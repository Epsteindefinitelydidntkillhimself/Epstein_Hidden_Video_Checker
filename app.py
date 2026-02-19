#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from scrape_epstein import NonPdfRow, ResultRow, convert_stage, scrape_one_page

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = BASE_DIR / "data.db"

DEFAULT_QUERY = "no images produced"


@contextmanager
def db_conn() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                mp4_filename TEXT NOT NULL DEFAULT '',
                file_url TEXT NOT NULL DEFAULT '',
                mp4_url TEXT NOT NULL DEFAULT '',
                source_page INTEGER DEFAULT 0,
                is_pdf INTEGER NOT NULL DEFAULT 0,
                viewed INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(filename, file_url, source_page)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_items_is_pdf ON items(is_pdf)
            """
        )
        conn.commit()


def upsert_pdf_rows(rows: List[ResultRow]) -> None:
    with db_conn() as conn:
        for row in rows:
            conn.execute(
                """
                INSERT INTO items(filename, mp4_filename, file_url, mp4_url, source_page, is_pdf)
                VALUES(?, ?, ?, ?, ?, 1)
                ON CONFLICT(filename, file_url, source_page) DO UPDATE SET
                    mp4_filename=excluded.mp4_filename,
                    mp4_url=excluded.mp4_url,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    row.pdf_filename,
                    row.mp4_filename,
                    row.pdf_url or "",
                    row.mp4_url or "",
                    row.source_page or 0,
                ),
            )
        conn.commit()


def upsert_non_pdf_rows(rows: List[NonPdfRow]) -> None:
    with db_conn() as conn:
        for row in rows:
            conn.execute(
                """
                INSERT INTO items(filename, mp4_filename, file_url, mp4_url, source_page, is_pdf)
                VALUES(?, '', ?, '', ?, 0)
                ON CONFLICT(filename, file_url, source_page) DO UPDATE SET
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    row.filename,
                    row.file_url or "",
                    row.source_page or 0,
                ),
            )
        conn.commit()


def read_items() -> List[Dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, filename, mp4_filename, file_url, mp4_url, source_page, is_pdf, viewed, notes, created_at, updated_at
            FROM items
            ORDER BY id ASC
            """
        ).fetchall()
        return [dict(r) for r in rows]


class ScrapeRequest(BaseModel):
    query: str = Field(default=DEFAULT_QUERY, min_length=1)


class ItemPatchRequest(BaseModel):
    viewed: Optional[bool] = None
    notes: Optional[str] = None


class ScrapeRuntimeState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.running = False
        self.paused = False
        self.error = ""
        self.query = DEFAULT_QUERY
        self.current_page = 0
        self.pages_processed = 0
        self.total_results = 0
        self.files_seen = 0
        self.non_pdf_seen = 0
        self.current_url = ""
        self.base_url = ""

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "running": self.running,
                "paused": self.paused,
                "error": self.error,
                "query": self.query,
                "current_page": self.current_page,
                "pages_processed": self.pages_processed,
                "total_results": self.total_results,
                "files_seen": self.files_seen,
                "non_pdf_seen": self.non_pdf_seen,
                "current_url": self.current_url,
            }

    def reset(self, query: str) -> None:
        with self.lock:
            self.running = True
            self.paused = False
            self.error = ""
            self.query = query
            self.current_page = 0
            self.pages_processed = 0
            self.total_results = 0
            self.files_seen = 0
            self.non_pdf_seen = 0
            self.current_url = ""
            self.base_url = ""

    def mark_done(self, error: str = "") -> None:
        with self.lock:
            self.running = False
            self.error = error


state = ScrapeRuntimeState()


def run_scrape_job(query: str) -> None:
    state.reset(query)
    page_no = 1
    blocked_pages = 0
    base_url = ""

    try:
        while True:
            with state.lock:
                state.current_page = page_no
            try:
                rows, non_pdf_rows, total, found_base, doj_url = scrape_one_page(
                    query=query,
                    page_index=page_no,
                    headed=False,
                    base_results_url=base_url,
                )
                base_url = found_base or base_url
                converted_rows = convert_stage(rows)

                upsert_pdf_rows(converted_rows)
                upsert_non_pdf_rows(non_pdf_rows)

                with state.lock:
                    state.base_url = base_url
                    state.current_url = doj_url
                    if total > 0:
                        state.total_results = total
                    state.pages_processed += 1
                    state.files_seen += len(converted_rows)
                    state.non_pdf_seen += len(non_pdf_rows)

                blocked_pages = 0
            except Exception as exc:
                blocked_pages += 1
                with state.lock:
                    state.current_url = f"Skipped page {page_no}: {exc}"
                if blocked_pages >= 5:
                    state.mark_done(error="Stopped after repeated blocked/empty pages.")
                    return

            snap = state.snapshot()
            if snap["total_results"] > 0:
                max_pages = max(1, (int(snap["total_results"]) + 9) // 10)
                if page_no >= max_pages:
                    state.mark_done()
                    return

            page_no += 1
            time.sleep(0.25)
    except Exception as exc:
        state.mark_done(error=str(exc))


app = FastAPI(title="Epstein Scraper API")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/")
def index() -> FileResponse:
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=500, detail="Missing static/index.html")
    return FileResponse(index_path)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/items")
def api_items() -> Dict[str, Any]:
    return {"items": read_items(), "state": state.snapshot()}


@app.post("/api/scrape")
def api_scrape(payload: ScrapeRequest) -> Dict[str, Any]:
    snap = state.snapshot()
    if snap["running"]:
        raise HTTPException(status_code=409, detail="Scrape is already running.")

    query = payload.query.strip() or DEFAULT_QUERY
    thread = threading.Thread(target=run_scrape_job, args=(query,), daemon=True)
    thread.start()
    return {"ok": True, "message": "Scrape started.", "query": query}


@app.patch("/api/items/{item_id}")
def api_patch_item(item_id: int, payload: ItemPatchRequest) -> Dict[str, Any]:
    if payload.viewed is None and payload.notes is None:
        raise HTTPException(status_code=400, detail="Nothing to update.")

    with db_conn() as conn:
        row = conn.execute("SELECT id, viewed, notes FROM items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Item not found.")

        viewed = int(payload.viewed) if payload.viewed is not None else int(row["viewed"])
        notes = payload.notes if payload.notes is not None else row["notes"]
        conn.execute(
            """
            UPDATE items
            SET viewed = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (viewed, notes, item_id),
        )
        conn.commit()

        updated = conn.execute(
            """
            SELECT id, filename, mp4_filename, file_url, mp4_url, source_page, is_pdf, viewed, notes, created_at, updated_at
            FROM items WHERE id = ?
            """,
            (item_id,),
        ).fetchone()

    return {"item": dict(updated)}
