#!/usr/bin/env python3
"""
One-shot scraper for DOJ Epstein Library search.

Install:
  pip install playwright
  playwright install
Run:
  python scrape_epstein.py

Optional tiny web UI:
  python scrape_epstein.py --serve
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
import urllib.request
from dataclasses import dataclass, asdict
from datetime import datetime, date
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

BASE_URL = "https://www.justice.gov"
SEARCH_URL = "https://www.justice.gov/epstein/search"
MULTIMEDIA_SEARCH_URL = "https://www.justice.gov/multimedia-search"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)
STATE_PATH = "doj_storage_state.json"


@dataclass
class ResultRow:
    index: int
    pdf_filename: str
    mp4_filename: str
    mp4_url: str
    source_page: int
    pdf_url: str


@dataclass
class NonPdfRow:
    filename: str
    source_page: int
    file_url: str


class ScrapeState:
    def __init__(self) -> None:
        self.lock = Lock()
        self.running = False
        self.finished = False
        self.error = ""
        self.phase = "idle"
        self.query = ""
        self.current = 0
        self.total = 0
        self.current_page_index = -1
        self.base_results_url = ""
        self.current_doj_url = ""
        self.rows: List[dict] = []
        self.non_pdf_rows: List[dict] = []
        self.paused = False
        self.pause_requested = False

    def reset_for_run(self, query: str) -> None:
        with self.lock:
            self.running = True
            self.finished = False
            self.error = ""
            self.phase = "retrieving"
            self.query = query
            self.current = 0
            self.total = 0
            self.current_page_index = -1
            self.base_results_url = ""
            self.current_doj_url = ""
            self.rows = []
            self.non_pdf_rows = []
            self.paused = False
            self.pause_requested = False

    def set_phase(self, phase: str) -> None:
        with self.lock:
            self.phase = phase

    def begin_run(self) -> None:
        with self.lock:
            self.running = True
            self.finished = False
            self.error = ""
            self.phase = "retrieving"
            self.paused = False
            self.pause_requested = False

    def request_pause(self) -> None:
        with self.lock:
            self.pause_requested = True

    def should_pause(self) -> bool:
        with self.lock:
            return self.pause_requested

    def mark_paused(self) -> None:
        with self.lock:
            self.running = False
            self.finished = False
            self.paused = True
            self.pause_requested = False
            self.phase = "paused"

    def set_counts(self, current: int, total: int) -> None:
        with self.lock:
            self.current = current
            self.total = total

    def add_row(self, row: ResultRow, total: int) -> None:
        with self.lock:
            self.rows.append(asdict(row))
            self.current = len(self.rows)
            if total > 0:
                self.total = total

    def set_rows(self, rows: List[ResultRow], phase: Optional[str] = None) -> None:
        with self.lock:
            self.rows = [asdict(r) for r in rows]
            self.current = len(rows)
            if phase:
                self.phase = phase

    def set_page_context(self, page_index: int, base_results_url: str, current_doj_url: str) -> None:
        with self.lock:
            self.current_page_index = page_index
            if base_results_url:
                self.base_results_url = base_results_url
            self.current_doj_url = current_doj_url

    def append_rows(self, rows: List[ResultRow], total: int) -> None:
        with self.lock:
            existing = {r.get("pdf_filename", "") for r in self.rows}
            for row in rows:
                if row.pdf_filename not in existing:
                    payload = asdict(row)
                    payload["index"] = len(self.rows) + 1
                    self.rows.append(payload)
                    existing.add(row.pdf_filename)
            self.current = len(self.rows)
            if total > 0:
                self.total = total

    def append_non_pdf_rows(self, rows: List[NonPdfRow]) -> None:
        with self.lock:
            existing = {(r.get("filename", ""), int(r.get("source_page", 0))) for r in self.non_pdf_rows}
            for row in rows:
                key = (row.filename, row.source_page)
                if key in existing:
                    continue
                self.non_pdf_rows.append(asdict(row))
                existing.add(key)

    def mark_done(self, error: str = "") -> None:
        with self.lock:
            self.running = False
            self.finished = True
            self.error = error
            if error:
                self.phase = "failed"
            else:
                self.phase = "done"

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "running": self.running,
                "finished": self.finished,
                "error": self.error,
                "phase": self.phase,
                "query": self.query,
                "current": self.current,
                "total": self.total,
                "remaining_count": max((self.total or self.current) - self.current, 0),
                "current_page_index": self.current_page_index,
                "base_results_url": self.base_results_url,
                "current_doj_url": self.current_doj_url,
                "paused": self.paused,
                "rows": list(self.rows),
                "non_pdf_rows": list(self.non_pdf_rows),
            }


class ProgressTracker:
    def __init__(self) -> None:
        self.lock = Lock()
        self.total = 0

    def set_total(self, total: int) -> None:
        with self.lock:
            if total > self.total:
                self.total = total

    def get_total(self) -> int:
        with self.lock:
            return self.total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape DOJ Epstein search results.")
    parser.add_argument("--query", default="no images produced", help="Search query phrase.")
    parser.add_argument("--out", default="results.csv", help="Output CSV path.")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode.")
    parser.add_argument("--serve", action="store_true", help="Run tiny web UI with Start button.")
    parser.add_argument("--host", default="127.0.0.1", help="Host for --serve mode.")
    parser.add_argument("--port", type=int, default=8000, help="Port for --serve mode.")
    return parser.parse_args()


def wait_stable(page: Page) -> None:
    page.wait_for_load_state("domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=7000)
    except PlaywrightTimeoutError:
        pass


def looks_like_homepage_fallback(page: Page) -> bool:
    try:
        canonical = ""
        canon_loc = page.locator("link[rel='canonical']")
        if canon_loc.count() > 0:
            canonical = (canon_loc.first.get_attribute("href") or "").strip().lower()
        title = (page.title() or "").lower()
        body = (page.locator("body").inner_text(timeout=1500) or "").lower()
        return (
            canonical == "https://www.justice.gov/"
            and "department of justice | homepage" in title
            and "search full epstein library" not in body
        )
    except Exception:
        return False


def is_in_header_or_nav(locator: Any) -> bool:
    try:
        return bool(
            locator.evaluate(
                """(el) => !!el.closest('header, .usa-header, .site-header, nav, .region-header')"""
            )
        )
    except Exception:
        return False


def click_age_gate_yes(page: Page) -> None:
    yes_locators = [
        page.locator("button:has-text('Yes')"),
        page.locator("button:has-text('YES')"),
        page.locator("a:has-text('Yes')"),
        page.locator("a:has-text('YES')"),
        page.get_by_role("button", name=re.compile(r"^yes$", re.I)),
        page.get_by_role("link", name=re.compile(r"^yes$", re.I)),
        page.get_by_text(re.compile(r"\byes\b", re.I)),
        page.locator("label:has-text('Yes')"),
        page.locator("input[type='submit'][value*='Yes' i]"),
    ]

    for loc in yes_locators:
        try:
            if loc.count() > 0:
                target = loc.first
                try:
                    target.scroll_into_view_if_needed(timeout=1000)
                except Exception:
                    pass
                target.click(timeout=3000, force=True)
                wait_stable(page)
                return
        except Exception:
            continue

    fallback = page.locator("text=/\\bYes\\b/i")
    if fallback.count() > 0:
        try:
            fallback.first.scroll_into_view_if_needed(timeout=1000)
        except Exception:
            pass
        fallback.first.click(force=True)
        wait_stable(page)


def is_age_gate_visible(page: Page) -> bool:
    try:
        body = (page.locator("body").inner_text(timeout=700) or "").lower()
        if "are you 18 years of age or older" in body:
            return True
    except Exception:
        pass

    checks = [
        page.locator("text=/Are you 18 years of age or older\\?/i"),
        page.get_by_role("button", name=re.compile(r"^yes$", re.I)),
        page.get_by_role("link", name=re.compile(r"^yes$", re.I)),
        page.locator("button:has-text('YES')"),
        page.locator("a:has-text('YES')"),
    ]
    for c in checks:
        try:
            if c.count() > 0 and c.first.is_visible(timeout=400):
                return True
        except Exception:
            continue
    return False


def ensure_age_gate_cleared(page: Page, max_attempts: int = 4) -> None:
    for _ in range(max_attempts):
        if not is_age_gate_visible(page):
            return
        click_age_gate_yes(page)
        wait_stable(page)
        # Wait for either gate to disappear or lower search/results UI to appear.
        for _ in range(8):
            if not is_age_gate_visible(page):
                return
            try:
                if find_library_search_input(page) is not None:
                    return
                if page_looks_like_results(page):
                    return
            except Exception:
                pass
            time.sleep(0.35)


def ensure_search_page_ready(page: Page) -> bool:
    # DOJ can intermittently return fallback content; retry a few entry routes.
    entry_urls = [SEARCH_URL, "https://www.justice.gov/epstein"]
    for _ in range(2):
        for url in entry_urls:
            try:
                page.goto(url, wait_until="domcontentloaded")
                wait_stable(page)
                ensure_age_gate_cleared(page)
                if url.endswith("/epstein"):
                    page.goto(SEARCH_URL, wait_until="domcontentloaded")
                    wait_stable(page)
                    ensure_age_gate_cleared(page)
                if find_library_search_input(page) is not None:
                    return True
            except Exception:
                continue
    return False


def find_library_search_input(page: Page):
    candidates = [
        page.locator("main input[placeholder*='Type to search' i]"),
        page.locator("main input[placeholder*='search' i]"),
        page.locator(".region-content input[placeholder*='Type to search' i]"),
        page.locator("form:has-text('Search Full Epstein Library') input[type='text']"),
        page.locator("input[placeholder*='Type to search' i]"),
        page.locator("input[placeholder*='search' i]"),
        page.locator("input[type='search']"),
        page.locator("input[type='text']"),
    ]

    for candidate in candidates:
        try:
            if candidate.count() == 0:
                continue
            field = candidate.first
            if is_in_header_or_nav(field):
                continue
            # Accept off-screen fields and scroll before interaction.
            try:
                field.scroll_into_view_if_needed(timeout=1200)
            except Exception:
                pass
            return field
        except Exception:
            continue

    return None


def submit_library_search(page: Page, search_input: Any) -> None:
    submit_candidates = [
        page.locator("main button:has-text('Search')"),
        page.locator("main input[type='submit'][value*='Search' i]"),
        page.locator(".region-content button:has-text('Search')"),
        page.locator("form:has(input[placeholder*='Type to search' i]) button:has-text('Search')"),
        page.locator("form:has(input[placeholder*='Type to search' i]) input[type='submit']"),
    ]
    for btn in submit_candidates:
        try:
            if btn.count() == 0:
                continue
            target = btn.first
            if target.is_visible(timeout=800) and not is_in_header_or_nav(target):
                target.click(timeout=3000)
                wait_stable(page)
                return
        except Exception:
            continue

    search_input.press("Enter")
    wait_stable(page)


def fill_and_submit_search(page: Page, query: str) -> None:
    ensure_age_gate_cleared(page)
    if looks_like_homepage_fallback(page) and not ensure_search_page_ready(page):
        raise RuntimeError(
            "DOJ returned homepage/fallback content instead of the Epstein search page."
        )

    for _ in range(4):
        input_count = page.locator("input[type='search'], input[type='text']").count()
        if input_count > 0:
            break
        time.sleep(0.7)
        wait_stable(page)

    search_input = find_library_search_input(page)
    if search_input is None:
        raise RuntimeError("Could not find Epstein Library (lower) search input.")

    search_input.click()
    search_input.fill("")
    search_input.fill(query)
    submit_library_search(page, search_input)


def get_result_links(page: Page):
    links = page.locator("main .view-content a, main .search-result a, main .views-row a")
    if links.count() == 0:
        links = page.locator("main a")
    try:
        links.first.wait_for(timeout=8000)
    except Exception:
        pass
    return links


def extract_pdf_filename(link_text: str, href: str) -> str:
    text_match = re.search(r"\b([A-Za-z0-9._-]+\.pdf)\b", link_text, flags=re.I)
    if text_match:
        return text_match.group(1)

    if href:
        base = href.split("?")[0].rstrip("/").rsplit("/", 1)[-1]
        if re.search(r"\.pdf$", base, flags=re.I):
            return base
    return ""


def parse_date_string(raw: str) -> Optional[date]:
    s = raw.strip()
    if not s:
        return None

    m = re.search(r"D:(\d{4})(\d{2})?(\d{2})?", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2) or "1")
        d = int(m.group(3) or "1")
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y", "%a, %d %b %Y %H:%M:%S %Z"]:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    return None


def extract_date_from_context(text: str) -> str:
    patterns = [
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            return match.group(0).strip()
    return ""


def oldest_date_string(candidates: List[str]) -> str:
    parsed: List[date] = []
    for c in candidates:
        d = parse_date_string(c)
        if d:
            parsed.append(d)
    if not parsed:
        return ""
    return min(parsed).isoformat()


def fetch_pdf_dates(pdf_url: str) -> List[str]:
    results: List[str] = []

    try:
        req = urllib.request.Request(pdf_url, headers={"User-Agent": UA}, method="HEAD")
        with urllib.request.urlopen(req, timeout=12) as resp:
            lm = resp.headers.get("Last-Modified", "").strip()
            if lm:
                results.append(lm)
    except Exception:
        pass

    chunks: List[bytes] = []
    for range_value in ["bytes=0-262143", "bytes=-262143"]:
        try:
            req = urllib.request.Request(
                pdf_url,
                headers={"User-Agent": UA, "Range": range_value},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                chunks.append(resp.read())
        except Exception:
            continue

    blob = b"\n".join(chunks)
    if blob:
        for m in re.finditer(rb"/(?:CreationDate|ModDate)\s*\(([^)]{4,80})\)", blob, flags=re.I):
            try:
                results.append(m.group(1).decode("latin-1", errors="ignore"))
            except Exception:
                continue

        for m in re.finditer(rb"\b(\d{4}-\d{2}-\d{2})\b", blob):
            try:
                results.append(m.group(1).decode("ascii", errors="ignore"))
            except Exception:
                continue

    return results


def extract_total_results(page: Page) -> int:
    candidates = [
        page.locator("text=/Showing\\s+\\d+\\s+to\\s+\\d+\\s+of\\s+[\\d,]+\\s+Results?/i"),
        page.locator("main").first,
    ]
    for loc in candidates:
        try:
            if loc.count() == 0:
                continue
            text = loc.first.inner_text(timeout=1200)
            m = re.search(r"of\s+([\d,]+)\s+Results?", text, flags=re.I)
            if m:
                return int(m.group(1).replace(",", ""))
        except Exception:
            continue
    return 0


def extract_rows_from_current_page(
    page: Page,
    seen: Dict[str, ResultRow],
    start_index: int,
) -> List[ResultRow]:
    new_rows: List[ResultRow] = []
    links = get_result_links(page)
    total = links.count()

    for i in range(total):
        link = links.nth(i)
        try:
            href = (link.get_attribute("href") or "").strip()
            link_text = (link.inner_text(timeout=1000) or "").strip()
        except Exception:
            continue

        pdf_filename = extract_pdf_filename(link_text, href)
        if not pdf_filename:
            continue

        if pdf_filename in seen:
            continue

        pdf_url = urljoin(BASE_URL, href) if href else ""

        row_date = ""
        row_context_selectors = [
            "xpath=ancestor::article[1]",
            "xpath=ancestor::li[1]",
            "xpath=ancestor::div[contains(@class,'views-row')][1]",
            "xpath=ancestor::div[contains(@class,'search-result')][1]",
            "xpath=ancestor::tr[1]",
            "xpath=ancestor::div[1]",
        ]
        for sel in row_context_selectors:
            try:
                ctx = link.locator(sel)
                if ctx.count() > 0:
                    ctx_text = ctx.first.inner_text(timeout=900)
                    row_date = extract_date_from_context(ctx_text)
                    if row_date:
                        break
            except Exception:
                continue

        metadata_dates: List[str] = []
        if pdf_url:
            metadata_dates = fetch_pdf_dates(pdf_url)

        final_date = oldest_date_string([row_date] + metadata_dates)
        row = ResultRow(
            index=start_index + len(new_rows) + 1,
            pdf_filename=pdf_filename,
            mp4_filename=re.sub(r"\.pdf$", ".mp4", pdf_filename, flags=re.I),
            mp4_url=re.sub(r"\.pdf(\?|$)", r".mp4\1", pdf_url, flags=re.I) if pdf_url else "",
            source_page=0,
            pdf_url=pdf_url,
        )
        seen[pdf_filename] = row
        new_rows.append(row)

    return new_rows


def signature_of_page(page: Page) -> str:
    links = get_result_links(page)
    count = links.count()
    names: List[str] = []
    # Build signature from result filenames only (avoid static sidebar/header links).
    for i in range(count):
        link = links.nth(i)
        try:
            href = (link.get_attribute("href") or "").strip()
            text = (link.inner_text(timeout=1000) or "").strip()
        except Exception:
            continue
        filename = extract_pdf_filename(text, href)
        if filename:
            names.append(filename.lower())
        if len(names) >= 8:
            break
    if names:
        return "|".join(names)
    # Fallback when no filenames are visible yet.
    return page.url


def current_page_marker(page: Page) -> str:
    selectors = [
        "main .pager__item--current",
        "main .pagination .active",
        "main [aria-current='page']",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                return (loc.first.inner_text(timeout=800) or "").strip()
        except Exception:
            continue
    return ""


def click_next_page(page: Page, current_sig: str, current_marker: str) -> bool:
    next_candidates = [
        page.locator("main a[rel='next']"),
        page.get_by_role("link", name=re.compile(r"^next$|next\s*›|›\s*next", re.I)),
        page.get_by_role("button", name=re.compile(r"^next$|next\s*›|›\s*next", re.I)),
        page.locator(".pager__item--next a, .pagination-next a"),
        page.locator("text=/^Next$/i"),
    ]

    for nxt in next_candidates:
        try:
            if nxt.count() > 0 and nxt.first.is_visible(timeout=1000):
                nxt.first.scroll_into_view_if_needed(timeout=1000)
                nxt.first.click(timeout=3000, force=True)
                wait_stable(page)
                if signature_of_page(page) != current_sig or current_page_marker(page) != current_marker:
                    return True
        except Exception:
            continue

    numeric_fallbacks = [
        "xpath=(//li[contains(@class,'pager__item--current')]/following-sibling::li//a)[1]",
        "xpath=(//li[contains(@class,'active')]/following-sibling::li//a)[1]",
        "xpath=(//a[@aria-current='page']/ancestor::*[self::li or self::a]/following::a[normalize-space()[number(.)=number(.)]][1])",
    ]

    for sel in numeric_fallbacks:
        try:
            candidate = page.locator(sel)
            if candidate.count() > 0 and candidate.first.is_visible(timeout=1000):
                candidate.first.scroll_into_view_if_needed(timeout=1000)
                candidate.first.click(timeout=3000, force=True)
                wait_stable(page)
                if signature_of_page(page) != current_sig or current_page_marker(page) != current_marker:
                    return True
        except Exception:
            continue

    return False


def goto_next_page_by_url(page: Page, current_sig: str) -> bool:
    # Drupal-style paging commonly uses ?page=<n>.
    try:
        parts = urlsplit(page.url)
        qs = dict(parse_qsl(parts.query, keep_blank_values=True))
        current_page = int(qs.get("page", "0"))
        qs["page"] = str(current_page + 1)
        next_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs), parts.fragment))
        page.goto(next_url, wait_until="domcontentloaded")
        wait_stable(page)
        ensure_age_gate_cleared(page)
        return signature_of_page(page) != current_sig
    except Exception:
        return False


def set_page_param(url: str, page_index: int) -> str:
    parts = urlsplit(url)
    qs = dict(parse_qsl(parts.query, keep_blank_values=True))
    qs["page"] = str(max(page_index, 1))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs), parts.fragment))


def remove_page_param(url: str) -> str:
    parts = urlsplit(url)
    qs = dict(parse_qsl(parts.query, keep_blank_values=True))
    qs.pop("page", None)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs), parts.fragment))


def build_direct_search_urls(query: str, page_index: int) -> List[str]:
    candidates = [
        (MULTIMEDIA_SEARCH_URL, {"keys": query, "page": str(page_index)}),
        {"search_api_fulltext": query, "page": str(page_index)},
        {"keys": query, "page": str(page_index)},
        {"query": query, "page": str(page_index)},
        {"search": query, "page": str(page_index)},
        {"q": query, "page": str(page_index)},
        {"s": query, "page": str(page_index)},
    ]
    urls: List[str] = []
    for c in candidates:
        if isinstance(c, tuple):
            base, params = c
            urls.append(f"{base}?{urlencode(params)}")
        else:
            urls.append(f"{SEARCH_URL}?{urlencode(c)}")
    return urls


def page_looks_like_results(page: Page) -> bool:
    try:
        if page.locator("text=/Showing\\s+\\d+\\s+to\\s+\\d+\\s+of\\s+[\\d,]+\\s+Results?/i").count() > 0:
            return True
    except Exception:
        pass
    try:
        links = get_result_links(page)
        return links.count() > 0
    except Exception:
        return False


def open_results_by_url_query(page: Page, query: str, page_index: int) -> Optional[str]:
    for url in build_direct_search_urls(query, page_index):
        try:
            page.goto(url, wait_until="domcontentloaded")
            wait_stable(page)
            ensure_age_gate_cleared(page)
            if is_age_gate_visible(page):
                continue
            if looks_like_homepage_fallback(page):
                continue
            if page_looks_like_results(page):
                return page.url
        except Exception:
            continue
    return None


def parse_multimedia_rows(payload: dict) -> tuple[List[ResultRow], List[NonPdfRow], int]:
    hits = payload.get("hits", {})
    total_obj = hits.get("total", {})
    total = int(total_obj.get("value", 0) or 0)
    items = hits.get("hits", []) or []

    rows: List[ResultRow] = []
    non_pdf_rows: List[NonPdfRow] = []
    seen_pdf: set[str] = set()
    seen_non_pdf: set[tuple[str, int]] = set()
    for item in items:
        src = item.get("_source", {}) or {}
        fields = src.get("fields", {}) or {}

        names = fields.get("ORIGIN_FILE_NAME") or src.get("ORIGIN_FILE_NAME") or []
        uris = fields.get("ORIGIN_FILE_URI") or src.get("ORIGIN_FILE_URI") or []

        if isinstance(names, str):
            names = [names]
        if isinstance(uris, str):
            uris = [uris]

        source_page = int(src.get("startPage", 0) or 0)
        filename = ""
        for n in names:
            m = re.search(r"\b([A-Za-z0-9._-]+\.[A-Za-z0-9]{2,5})\b", str(n), flags=re.I)
            if m:
                filename = m.group(1)
                break

        pdf_url = ""
        if uris:
            raw_uri = str(uris[0]).replace("\\/", "/")
            pdf_url = raw_uri if raw_uri.startswith("http") else urljoin(BASE_URL, raw_uri)
            if not filename:
                tail = raw_uri.split("?")[0].rstrip("/").rsplit("/", 1)[-1]
                if tail:
                    filename = tail

        if not filename:
            continue

        if not filename.lower().endswith(".pdf"):
            key = (filename, source_page)
            if key in seen_non_pdf:
                continue
            seen_non_pdf.add(key)
            non_pdf_rows.append(
                NonPdfRow(
                    filename=filename,
                    source_page=source_page,
                    file_url=pdf_url,
                )
            )
            continue

        if filename in seen_pdf:
            continue
        seen_pdf.add(filename)

        rows.append(
            ResultRow(
                index=len(rows) + 1,
                pdf_filename=filename,
                mp4_filename=re.sub(r"\.pdf$", ".mp4", filename, flags=re.I),
                mp4_url=re.sub(r"\.pdf(\?|$)", r".mp4\1", pdf_url, flags=re.I) if pdf_url else "",
                source_page=source_page,
                pdf_url=pdf_url,
            )
        )

    return rows, non_pdf_rows, total


def parse_json_from_page_text(page: Page) -> Optional[dict]:
    try:
        raw = (page.locator("body").inner_text(timeout=3000) or "").strip()
        if not raw:
            return None
        # JSON page may have optional prefix text; extract first object block.
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]
        return json.loads(raw)
    except Exception:
        return None


def recover_results_context(page: Page, query: str, target_page_index: int) -> bool:
    # Re-open search context after DOJ fallback/age-gate interruptions.
    try:
        page.goto(SEARCH_URL, wait_until="domcontentloaded")
        wait_stable(page)
        ensure_age_gate_cleared(page)
        fill_and_submit_search(page, query)

        base_results_url = page.url
        if target_page_index > 0:
            target_url = set_page_param(base_results_url, target_page_index)
            page.goto(target_url, wait_until="domcontentloaded")
            wait_stable(page)
            ensure_age_gate_cleared(page)
            # If gate/fallback interrupted again, retry one more time from search.
            if looks_like_homepage_fallback(page):
                page.goto(SEARCH_URL, wait_until="domcontentloaded")
                wait_stable(page)
                ensure_age_gate_cleared(page)
                fill_and_submit_search(page, query)
                base_results_url = page.url
                page.goto(set_page_param(base_results_url, target_page_index), wait_until="domcontentloaded")
                wait_stable(page)
                ensure_age_gate_cleared(page)
        return True
    except Exception:
        return False


def goto_page_by_index(page: Page, base_results_url: str, page_index: int) -> bool:
    # Navigate directly by URL page parameter and clear age gate if it flashes.
    target_url = set_page_param(base_results_url, page_index)
    for _ in range(3):
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            wait_stable(page)
            ensure_age_gate_cleared(page)
            if not looks_like_homepage_fallback(page):
                return True
        except Exception:
            continue
    return False


def scrape(
    query: str,
    headed: bool,
    on_new_row: Optional[Callable[[ResultRow, int], None]] = None,
    on_total: Optional[Callable[[int], None]] = None,
) -> List[ResultRow]:
    last_error = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=not headed,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        try:
            for entry_url in [SEARCH_URL, "https://www.justice.gov/epstein"]:
                context = browser.new_context(
                    locale="en-US",
                    timezone_id="America/New_York",
                    user_agent=UA,
                    extra_http_headers={
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                    viewport={"width": 1440, "height": 900},
                )
                context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
                )
                page = context.new_page()
                try:
                    page.goto(entry_url, wait_until="domcontentloaded")
                    wait_stable(page)

                    click_age_gate_yes(page)

                    if page.url.rstrip("/") == "https://www.justice.gov/epstein":
                        page.goto(SEARCH_URL, wait_until="domcontentloaded")
                        wait_stable(page)
                        click_age_gate_yes(page)

                    fill_and_submit_search(page, query)
                    base_results_url = page.url

                    total_results = extract_total_results(page)
                    if on_total:
                        on_total(total_results)

                    seen: Dict[str, ResultRow] = {}
                    page_index = 0
                    stalled_pages = 0
                    while True:
                        if not goto_page_by_index(page, base_results_url, page_index):
                            raise RuntimeError(f"Could not open results page index {page_index}.")

                        new_rows = extract_rows_from_current_page(page, seen, len(seen))
                        if on_new_row:
                            for row in new_rows:
                                on_new_row(row, total_results)
                        if new_rows:
                            stalled_pages = 0
                        else:
                            stalled_pages += 1

                        if total_results > 0 and len(seen) >= total_results:
                            break

                        # If no total available, stop after repeated empty pages.
                        if total_results == 0 and stalled_pages >= 3:
                            break

                        page_index += 1
                        time.sleep(0.25)

                    return list(seen.values())
                except Exception as exc:
                    last_error = str(exc)
                finally:
                    context.close()
        finally:
            browser.close()

    raise RuntimeError(last_error or "Scrape failed before results could be collected.")


def scrape_one_page(
    query: str,
    page_index: int,
    headed: bool,
    base_results_url: str = "",
) -> tuple[List[ResultRow], List[NonPdfRow], int, str, str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=not headed,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        try:
            context_kwargs: Dict[str, Any] = {
                "locale": "en-US",
                "timezone_id": "America/New_York",
                "user_agent": UA,
                "viewport": {"width": 1440, "height": 900},
            }
            if os.path.exists(STATE_PATH):
                context_kwargs["storage_state"] = STATE_PATH

            context = browser.new_context(
                **context_kwargs,
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.new_page()
            try:
                target_base = base_results_url or f"{MULTIMEDIA_SEARCH_URL}?{urlencode({'keys': query})}"
                loaded_url = set_page_param(target_base, page_index)

                def establish_session() -> None:
                    if not ensure_search_page_ready(page):
                        raise RuntimeError("Could not open Epstein search page to establish session.")
                    fill_and_submit_search(page, query)
                    wait_stable(page)
                    page.wait_for_timeout(500)

                # Always refresh session per page call; storage_state alone has been unreliable.
                establish_session()

                payload: Optional[dict] = None
                last_status = 0
                for _ in range(3):
                    # Primary: browser navigation (works in your environment even when request API gets 403).
                    try:
                        page.goto(loaded_url, wait_until="domcontentloaded")
                        wait_stable(page)
                        ensure_age_gate_cleared(page)
                        payload = parse_json_from_page_text(page)
                        if payload is not None:
                            break
                    except Exception:
                        pass

                    # Secondary fallback: request API.
                    try:
                        response = context.request.get(
                            loaded_url,
                            headers={"Accept": "application/json,text/plain,*/*", "User-Agent": UA},
                        )
                        last_status = response.status
                        if response.ok:
                            payload = response.json()
                            if payload is not None:
                                break
                    except Exception:
                        pass

                    # Re-establish session and try again.
                    establish_session()

                if payload is None:
                    # Final fallback: use normal HTML search results flow (no multimedia endpoint dependency).
                    if not ensure_search_page_ready(page):
                        raise RuntimeError(
                            f"PAGE_BLOCKED: unable to establish search page context (status {last_status or 403})."
                        )
                    fill_and_submit_search(page, query)
                    total_results = extract_total_results(page)

                    # Advance to requested page by UI/URL pagination from the HTML results page.
                    for _ in range(max(page_index - 1, 0)):
                        sig = signature_of_page(page)
                        marker = current_page_marker(page)
                        moved = click_next_page(page, sig, marker)
                        if not moved:
                            moved = goto_next_page_by_url(page, sig)
                        if not moved:
                            break
                        ensure_age_gate_cleared(page)
                        wait_stable(page)

                    seen_html: Dict[str, ResultRow] = {}
                    html_rows = extract_rows_from_current_page(page, seen_html, 0)
                    if not html_rows and total_results == 0:
                        raise RuntimeError(
                            f"PAGE_BLOCKED: no rows from HTML fallback (status {last_status or 403})."
                        )
                    try:
                        context.storage_state(path=STATE_PATH)
                    except Exception:
                        pass
                    return html_rows, [], total_results, remove_page_param(page.url), page.url

                rows, non_pdf_rows, total_results = parse_multimedia_rows(payload)
                if total_results == 0 and not rows:
                    raise RuntimeError("PAGE_BLOCKED: no rows from multimedia payload.")

                try:
                    context.storage_state(path=STATE_PATH)
                except Exception:
                    pass
                return rows, non_pdf_rows, total_results, target_base, loaded_url
            finally:
                context.close()
        finally:
            browser.close()


def convert_stage(rows: List[ResultRow], on_progress: Optional[Callable[[int, int], None]] = None) -> List[ResultRow]:
    converted: List[ResultRow] = []
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        converted.append(
            ResultRow(
                index=row.index,
                pdf_filename=row.pdf_filename,
                mp4_filename=re.sub(r"\.pdf$", ".mp4", row.pdf_filename, flags=re.I),
                mp4_url=re.sub(r"\.pdf(\?|$)", r".mp4\1", row.pdf_url, flags=re.I) if row.pdf_url else "",
                source_page=row.source_page,
                pdf_url=row.pdf_url,
            )
        )
        if on_progress:
            on_progress(idx, total)
    return converted


def write_csv(path: str, rows: List[ResultRow]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["pdf_filename", "mp4_filename", "page_number"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "pdf_filename": row.pdf_filename,
                    "mp4_filename": row.mp4_filename,
                    "page_number": row.source_page,
                }
            )


def rows_to_browser_html(rows: List[dict], non_pdf_rows: List[dict]) -> str:
    rows_json = json.dumps(rows)
    non_pdf_json = json.dumps(non_pdf_rows)
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Epstein Results</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; margin: 20px; color: #1b1f24; }}
    .controls {{ display: flex; gap: 8px; margin-bottom: 10px; align-items: center; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e8edf4; text-align: left; padding: 8px; }}
    th {{ background: #f2f5fa; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  </style>
</head>
<body>
  <h2>Converted Files</h2>
  <div class=\"controls\">
    <label for=\"sort\">Sort:</label>
    <select id=\"sort\">
      <option value=\"default\">Default (pulled order)</option>
      <option value=\"name\">Name</option>
    </select>
    <span id=\"count\"></span>
  </div>
  <table>
    <thead>
      <tr>
        <th>#</th>
        <th>PDF File (clickable)</th>
        <th>Converted .mp4 Name</th>
      </tr>
    </thead>
    <tbody id=\"rows\"></tbody>
  </table>
  <h3>Non-PDF Filenames</h3>
  <table>
    <thead>
      <tr>
        <th>Filename</th>
        <th>Source Page</th>
      </tr>
    </thead>
    <tbody id=\"nonPdfRows\"></tbody>
  </table>
  <script>
    const rows = {rows_json};
    const nonPdfRows = {non_pdf_json};
    const rowsEl = document.getElementById('rows');
    const nonPdfRowsEl = document.getElementById('nonPdfRows');
    const sortEl = document.getElementById('sort');
    const countEl = document.getElementById('count');

    function esc(s) {{
      return String(s || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }}

    function sortedRows(mode) {{
      const out = [...rows];
      if (mode === 'name') {{
        out.sort((a, b) => (a.mp4_filename || '').localeCompare(b.mp4_filename || ''));
      }} else {{
        out.sort((a, b) => (a.index || 0) - (b.index || 0));
      }}
      return out;
    }}

    function render() {{
      const mode = sortEl.value;
      const data = sortedRows(mode);
      countEl.textContent = `${{data.length}} files`;
      rowsEl.innerHTML = data.map((r) => `
        <tr>
          <td>${{r.index}}</td>
          <td class=\"mono\"><a href=\"${{esc(r.pdf_url)}}\" target=\"_blank\" rel=\"noopener\">${{esc(r.pdf_filename)}}</a></td>
          <td class=\"mono\"><a href=\"${{esc(r.mp4_url || '')}}\" target=\"_blank\" rel=\"noopener\">${{esc(r.mp4_filename)}}</a></td>
        </tr>
      `).join('');

      nonPdfRowsEl.innerHTML = nonPdfRows.map((r) => `
        <tr>
          <td class=\"mono\"><a href=\"${{esc(r.file_url || '')}}\" target=\"_blank\" rel=\"noopener\">${{esc(r.filename)}}</a></td>
          <td>${{esc(r.source_page || '')}}</td>
        </tr>
      `).join('');
    }}

    sortEl.addEventListener('change', render);
    render();
  </script>
</body>
</html>
"""


def rows_to_download_html(rows: List[dict], non_pdf_rows: List[dict]) -> str:
    lines = [
        "<!doctype html>",
        "<html lang=\"en\"><head><meta charset=\"utf-8\" /><title>Epstein Converted Files</title></head><body>",
        "<h2>Converted Files</h2>",
        "<ul>",
    ]
    for r in sorted(rows, key=lambda x: int(x.get("index", 0))):
        pdf_name = escape(str(r.get("pdf_filename", "")))
        mp4_name = escape(str(r.get("mp4_filename", "")))
        pdf_url = escape(str(r.get("pdf_url", "")))
        mp4_url = escape(str(r.get("mp4_url", "")))
        page_no = escape(str(r.get("source_page", "")))
        lines.append(f'<li><a href="{pdf_url}">{pdf_name}</a> | <a href="{mp4_url}">{mp4_name}</a> | page {page_no}</li>')
    lines.append("</ul>")
    lines.append("<h3>Non-PDF Filenames</h3>")
    lines.append("<ul>")
    for r in sorted(non_pdf_rows, key=lambda x: (int(x.get("source_page", 0)), str(x.get("filename", "")))):
        fname = escape(str(r.get("filename", "")))
        furl = escape(str(r.get("file_url", "")))
        page_no = escape(str(r.get("source_page", "")))
        lines.append(f'<li><a href="{furl}">{fname}</a> | page {page_no}</li>')
    lines.append("</ul></body></html>")
    return "\n".join(lines)


def make_handler(state: ScrapeState, headed: bool):
    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, payload: dict, code: int = 200) -> None:
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_html(self, html: str, code: int = 200, attachment_name: str = "") -> None:
            raw = html.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            if attachment_name:
                self.send_header("Content-Disposition", f'attachment; filename="{attachment_name}"')
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            if self.path.startswith("/status"):
                self._send_json(state.snapshot())
                return
            if self.path.startswith("/results"):
                snap = state.snapshot()
                self._send_html(rows_to_browser_html(snap["rows"], snap["non_pdf_rows"]))
                return
            if self.path.startswith("/download"):
                snap = state.snapshot()
                self._send_html(
                    rows_to_download_html(snap["rows"], snap["non_pdf_rows"]),
                    attachment_name="epstein_results.html",
                )
                return
            if self.path == "/" or self.path.startswith("/?"):
                self._send_html(INDEX_HTML)
                return
            self.send_error(HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:
            def write_current_csv() -> None:
                snap = state.snapshot()
                rows: List[ResultRow] = []
                for r in snap["rows"]:
                    rows.append(ResultRow(**r))
                write_csv("results.csv", rows)

            def run_pages(query: str, start_page: int, base_url: str) -> None:
                try:
                    state.begin_run()
                    current_base = base_url
                    page_no = max(1, start_page)
                    blocked_pages = 0
                    while True:
                        if state.should_pause():
                            state.mark_paused()
                            return
                        try:
                            rows, non_pdf_rows, total, found_base, doj_url = scrape_one_page(
                                query=query,
                                page_index=page_no,
                                headed=headed,
                                base_results_url=current_base,
                            )
                            current_base = found_base
                            state.set_page_context(page_no, current_base, doj_url)
                            state.set_phase("converting")
                            converted = convert_stage(rows)
                            state.append_rows(converted, total)
                            state.append_non_pdf_rows(non_pdf_rows)
                            write_current_csv()
                            blocked_pages = 0
                        except Exception as page_exc:
                            blocked_pages += 1
                            state.set_page_context(
                                page_no,
                                current_base,
                                f"Skipped page {page_no}: {page_exc}",
                            )
                            state.set_phase("retrieving")

                        snap_now = state.snapshot()
                        if snap_now["total"] > 0 and snap_now["current"] >= snap_now["total"]:
                            state.mark_done()
                            return
                        if snap_now["total"] > 0:
                            max_pages = max(1, (int(snap_now["total"]) + 9) // 10)
                            if page_no >= max_pages:
                                state.mark_done()
                                return
                        if blocked_pages >= 5:
                            state.mark_done(error="Stopped after repeated blocked/empty pages.")
                            return
                        page_no += 1
                        state.set_phase("retrieving")
                        time.sleep(0.25)

                except Exception as exc:
                    state.mark_done(error=str(exc))

            if self.path == "/pause":
                snap = state.snapshot()
                if not snap["running"]:
                    self._send_json({"ok": False, "message": "Not currently running."}, code=409)
                    return
                state.request_pause()
                self._send_json({"ok": True})
                return

            if self.path == "/resume":
                snap = state.snapshot()
                if snap["running"]:
                    self._send_json({"ok": False, "message": "Already running"}, code=409)
                    return
                if not snap["paused"]:
                    self._send_json({"ok": False, "message": "Nothing to resume."}, code=409)
                    return

                next_page = max(1, int(snap["current_page_index"]) + 1)
                Thread(
                    target=run_pages,
                    kwargs={
                        "query": snap["query"] or "no images produced",
                        "start_page": next_page,
                        "base_url": snap["base_results_url"],
                    },
                    daemon=True,
                ).start()
                self._send_json({"ok": True, "page": next_page})
                return

            if self.path != "/start":
                self.send_error(HTTPStatus.NOT_FOUND)
                return

            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8") if length > 0 else ""
            params = parse_qs(body)
            query = (params.get("query", ["no images produced"])[0] or "no images produced").strip()

            snap = state.snapshot()
            if snap["running"]:
                self._send_json({"ok": False, "message": "Already running"}, code=409)
                return

            state.reset_for_run(query)

            Thread(
                target=run_pages,
                kwargs={"query": query, "start_page": 1, "base_url": ""},
                daemon=True,
            ).start()
            self._send_json({"ok": True})

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return Handler


def run_server(host: str, port: int, headed: bool) -> None:
    state = ScrapeState()
    server = ThreadingHTTPServer((host, port), make_handler(state, headed))
    print(f"UI server running at http://{host}:{port}")
    print("Press Start in browser to run scraper.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


INDEX_HTML = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Epstein Scraper</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; margin: 24px; background: #f7f8fb; color: #1b1f24; }
    .card { background: #fff; border: 1px solid #dde2ea; border-radius: 10px; padding: 16px; max-width: 980px; }
    h1 { margin: 0 0 12px 0; font-size: 20px; }
    .controls { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
    input { padding: 8px 10px; border: 1px solid #cdd5df; border-radius: 8px; min-width: 280px; }
    button { padding: 8px 14px; border: 0; border-radius: 8px; background: #0b5fff; color: #fff; cursor: pointer; }
    button:disabled { opacity: .6; cursor: not-allowed; }
    .status { margin: 8px 0 10px 0; font-size: 14px; }
    .substatus { margin: 0 0 12px 0; color: #555; font-size: 13px; }
    .list-wrap { max-height: 310px; overflow-y: auto; border: 1px solid #e8edf4; border-radius: 8px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border-bottom: 1px solid #e8edf4; text-align: left; padding: 8px; }
    th { background: #f2f5fa; position: sticky; top: 0; z-index: 2; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .viewed-col { width: 78px; white-space: nowrap; }
    .notes-col { width: 520px; }
    .notes-input { width: 480px; max-width: 100%; }
    .note-tags { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 4px; }
    .note-tag { font-size: 11px; line-height: 1.2; padding: 2px 6px; border-radius: 999px; background: #e8eefc; color: #20407e; }
    .progress { width: 100%; height: 10px; border-radius: 999px; background: #e9edf5; overflow: hidden; margin-bottom: 10px; }
    .bar { height: 100%; width: 0%; background: linear-gradient(90deg, #0b5fff, #3ca4ff); transition: width 0.3s ease; }
    .spinner { display: inline-block; width: 12px; height: 12px; border: 2px solid #c7d6ef; border-top-color: #0b5fff; border-radius: 50%; animation: spin 0.8s linear infinite; margin-right: 6px; vertical-align: -1px; }
    .hidden { display: none; }
    .actions { margin-top: 12px; display: flex; gap: 8px; }
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
  </style>
</head>
<body>
  <div class=\"card\">
    <h1>Epstein Scraper Runner</h1>
    <div class=\"controls\">
      <input id=\"query\" value=\"no images produced\" />
      <button id=\"start\">Start</button>
      <button id=\"pause\" disabled>Pause</button>
      <button id=\"resume\" disabled>Resume</button>
    </div>

    <div class=\"status\" id=\"status\">Idle</div>
    <div class=\"substatus\" id=\"batchLabel\"></div>
    <div class=\"substatus\" id=\"dojUrl\"></div>
    <div class=\"substatus\" id=\"substatus\"></div>
    <div class=\"progress\"><div id=\"bar\" class=\"bar\"></div></div>

    <div class=\"list-wrap\">
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>PDF Filename</th>
            <th>.mp4 Filename</th>
            <th class=\"viewed-col\">Viewed?</th>
            <th class=\"notes-col\">Notes</th>
          </tr>
        </thead>
        <tbody id=\"rows\"></tbody>
      </table>
    </div>
    <datalist id=\"noteTags\"></datalist>

    <h3>Non-PDF Filenames</h3>
    <div class=\"list-wrap\">
      <table>
        <thead>
          <tr>
            <th>Filename</th>
            <th>Source Page</th>
          </tr>
        </thead>
        <tbody id=\"nonPdfRows\"></tbody>
      </table>
    </div>

    <div class=\"actions\" id=\"actions\">
      <button id=\"openBrowser\" disabled>Open in Browser</button>
    </div>
  </div>

  <script>
    const statusEl = document.getElementById('status');
    const batchLabelEl = document.getElementById('batchLabel');
    const dojUrlEl = document.getElementById('dojUrl');
    const substatusEl = document.getElementById('substatus');
    const rowsEl = document.getElementById('rows');
    const nonPdfRowsEl = document.getElementById('nonPdfRows');
    const startBtn = document.getElementById('start');
    const queryEl = document.getElementById('query');
    const barEl = document.getElementById('bar');
    const openBrowserBtn = document.getElementById('openBrowser');
    const pauseBtn = document.getElementById('pause');
    const resumeBtn = document.getElementById('resume');
    const noteTagsEl = document.getElementById('noteTags');
    const rowMeta = new Map();

    function esc(s) {
      return String(s || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }

    function rowKey(r) {
      return `${r.pdf_filename || ''}::${r.source_page || ''}`;
    }

    function tokenizeNotes(value) {
      return String(value || '')
        .split(',')
        .map((x) => x.trim())
        .filter((x) => x.length > 0);
    }

    function updateTagSuggestions() {
      const tags = new Set();
      rowMeta.forEach((meta) => {
        tokenizeNotes(meta.notes).forEach((tag) => tags.add(tag));
      });
      const options = Array.from(tags).sort((a, b) => a.localeCompare(b));
      noteTagsEl.innerHTML = options.map((tag) => `<option value="${esc(tag)}"></option>`).join('');
    }

    function renderTagChips(inputEl) {
      const key = inputEl.getAttribute('data-key') || '';
      const meta = rowMeta.get(key) || { viewed: false, notes: '' };
      const wrap = inputEl.parentElement.querySelector('[data-role=\"tags\"]');
      if (!wrap) return;
      const tags = tokenizeNotes(meta.notes);
      wrap.innerHTML = tags.map((tag) => `<span class=\"note-tag\">${esc(tag)}</span>`).join('');
    }

    function renderRows(rows) {
      rowsEl.innerHTML = rows.map((r) => `
        <tr>
          <td>${esc(r.index)}</td>
          <td class=\"mono\">${esc(r.pdf_filename)}</td>
          <td class=\"mono\"><a href=\"${esc(r.mp4_url || '')}\" target=\"_blank\" rel=\"noopener\">${esc(r.mp4_filename)}</a></td>
          <td class=\"viewed-col\"><input type=\"checkbox\" data-role=\"viewed\" data-key=\"${esc(rowKey(r))}\" /></td>
          <td class=\"notes-col\">
            <div class=\"note-tags\" data-role=\"tags\" data-key=\"${esc(rowKey(r))}\"></div>
            <input class=\"notes-input\" list=\"noteTags\" type=\"text\" data-role=\"notes\" data-key=\"${esc(rowKey(r))}\" />
          </td>
        </tr>
      `).join('');

      rowsEl.querySelectorAll('input[data-role=\"viewed\"]').forEach((el) => {
        const key = el.getAttribute('data-key') || '';
        const meta = rowMeta.get(key) || { viewed: false, notes: '' };
        el.checked = !!meta.viewed;
      });
      rowsEl.querySelectorAll('input[data-role=\"notes\"]').forEach((el) => {
        const key = el.getAttribute('data-key') || '';
        const meta = rowMeta.get(key) || { viewed: false, notes: '' };
        el.value = meta.notes || '';
        renderTagChips(el);
      });
      updateTagSuggestions();
    }

    function renderNonPdfRows(nonPdfRows) {
      nonPdfRowsEl.innerHTML = nonPdfRows.map((r) => `
        <tr>
          <td class=\"mono\"><a href=\"${esc(r.file_url || '')}\" target=\"_blank\" rel=\"noopener\">${esc(r.filename || '')}</a></td>
          <td>${esc(r.source_page || '')}</td>
        </tr>
      `).join('');
    }

    function percent(current, total) {
      if (!total || total <= 0) return 0;
      return Math.max(0, Math.min(100, Math.round((current / total) * 100)));
    }

    function setProgress(current, total) {
      barEl.style.width = `${percent(current, total)}%`;
    }

    function spinnerText(label) {
      return `<span class=\"spinner\"></span>${label}`;
    }

    async function poll() {
      try {
        const res = await fetch('/status');
        const data = await res.json();

        renderRows(data.rows || []);
        renderNonPdfRows(data.non_pdf_rows || []);
        const hasRows = (data.rows || []).length > 0;
        openBrowserBtn.disabled = !hasRows;
        pauseBtn.disabled = !data.running;
        resumeBtn.disabled = data.running || !data.paused;

        if (data.running) {
          startBtn.disabled = true;

          if (data.phase === 'converting') {
            statusEl.innerHTML = spinnerText('Converting to .mp4');
          } else {
            statusEl.innerHTML = spinnerText('Retrieving Files');
          }
          batchLabelEl.textContent = `Current page: ${data.current_page_index > 0 ? data.current_page_index : '-'}`;
          dojUrlEl.textContent = data.current_doj_url ? `DOJ URL: ${data.current_doj_url}` : '';

          const totalDisplay = data.total > 0 ? data.total : '?';
          const remaining = data.total > 0 ? data.remaining_count : '...';
          substatusEl.textContent = `Retrieved: ${data.current} / ${totalDisplay} | Remaining: ${remaining}`;
          setProgress(data.current, data.total || data.current || 1);
        } else if (data.paused) {
          statusEl.textContent = 'Paused';
          batchLabelEl.textContent = `Current page: ${data.current_page_index > 0 ? data.current_page_index : '-'}`;
          dojUrlEl.textContent = data.current_doj_url ? `DOJ URL: ${data.current_doj_url}` : '';
          const totalDisplay = data.total > 0 ? data.total : '?';
          substatusEl.textContent = `Retrieved: ${data.current}/${totalDisplay}. Press Resume to continue.`;
          setProgress(data.current, data.total || data.current || 1);
          startBtn.disabled = false;
        } else if (data.finished && data.error) {
          statusEl.textContent = `Failed: ${data.error}`;
          batchLabelEl.textContent = '';
          dojUrlEl.textContent = data.current_doj_url ? `DOJ URL: ${data.current_doj_url}` : '';
          substatusEl.textContent = '';
          startBtn.disabled = false;
        } else if (data.finished) {
          statusEl.textContent = `Complete. ${data.rows.length} files.`;
          batchLabelEl.textContent = `Current page: ${data.current_page_index > 0 ? data.current_page_index : '-'}`;
          dojUrlEl.textContent = data.current_doj_url ? `DOJ URL: ${data.current_doj_url}` : '';
          const totalDisplay = data.total > 0 ? data.total : '?';
          substatusEl.textContent = `Processed ${data.current}/${totalDisplay}. All pages complete.`;
          setProgress(data.rows.length, data.rows.length || 1);
          startBtn.disabled = false;
        } else {
          statusEl.textContent = 'Idle';
          batchLabelEl.textContent = '';
          dojUrlEl.textContent = '';
          substatusEl.textContent = '';
          startBtn.disabled = false;
          setProgress(0, 1);
        }
      } catch (err) {
        statusEl.textContent = `Status error: ${err}`;
      }
    }

    startBtn.addEventListener('click', async () => {
      window.focus();
      rowMeta.clear();
      updateTagSuggestions();
      const params = new URLSearchParams({ query: queryEl.value || 'no images produced' });
      const res = await fetch('/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: params.toString(),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({ message: 'start failed' }));
        statusEl.textContent = `Cannot start: ${data.message || 'unknown error'}`;
      }
      await poll();
    });

    rowsEl.addEventListener('change', (e) => {
      const t = e.target;
      if (!(t instanceof HTMLInputElement)) return;
      const role = t.getAttribute('data-role');
      const key = t.getAttribute('data-key') || '';
      if (!key) return;
      const meta = rowMeta.get(key) || { viewed: false, notes: '' };
      if (role === 'viewed') {
        meta.viewed = t.checked;
      }
      rowMeta.set(key, meta);
    });

    rowsEl.addEventListener('input', (e) => {
      const t = e.target;
      if (!(t instanceof HTMLInputElement)) return;
      const role = t.getAttribute('data-role');
      const key = t.getAttribute('data-key') || '';
      if (!key || role !== 'notes') return;
      const meta = rowMeta.get(key) || { viewed: false, notes: '' };
      meta.notes = t.value;
      rowMeta.set(key, meta);
      renderTagChips(t);
      updateTagSuggestions();
    });

    openBrowserBtn.addEventListener('click', () => {
      window.open('/results', '_blank', 'noopener');
    });

    pauseBtn.addEventListener('click', async () => {
      const res = await fetch('/pause', { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({ message: 'pause failed' }));
        statusEl.textContent = `Cannot pause: ${data.message || 'unknown error'}`;
      }
      await poll();
    });

    resumeBtn.addEventListener('click', async () => {
      const res = await fetch('/resume', { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({ message: 'resume failed' }));
        statusEl.textContent = `Cannot resume: ${data.message || 'unknown error'}`;
      }
      await poll();
    });

    setInterval(poll, 1000);
    poll();
  </script>
</body>
</html>
"""


def main() -> None:
    args = parse_args()

    if args.serve:
        run_server(host=args.host, port=args.port, headed=args.headed)
        return

    rows = scrape(args.query, args.headed)
    rows = convert_stage(rows)

    for row in rows:
        print(f"{row.mp4_filename}\t{row.source_page}")

    write_csv(args.out, rows)


if __name__ == "__main__":
    main()
