from __future__ import annotations

import hashlib
import re
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.energy.gov"
HEADERS = {"User-Agent": "RockQuant/0.2"}

MONTHS = (
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
)
DATE_RE = re.compile(rf"^({'|'.join(MONTHS)})\s+\d{{1,2}},\s+\d{{4}}$")


def canonical_key_from_url(url: str) -> str:
    path = urllib.parse.urlparse(url).path.rstrip("/").lower() or "/"
    return hashlib.sha256(f"doe|{path}".encode("utf-8")).hexdigest()


def parse_listing(html: str) -> List[Dict[str, str]]:
    """
    Parse DOE listing pages (div.views-row) and return items:
    published_date (ISO), title, url
    """
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []

    for row in soup.select("div.views-row"):
        date_raw: Optional[str] = None
        for s in row.stripped_strings:
            if DATE_RE.match(s):
                date_raw = s
                break
        if not date_raw:
            continue

        # Convert date to ISO
        published_date = datetime.strptime(date_raw, "%B %d, %Y").date().isoformat()

        # Find first relevant article link
        a = None
        for cand in row.find_all("a", href=True):
            href = cand["href"].strip()
            if href.startswith("/articles/") or href.startswith("/lpo/articles/"):
                a = cand
                break
        if not a:
            continue

        title = a.get_text(" ", strip=True)
        if not title:
            continue

        url = urllib.parse.urljoin(BASE_URL, a["href"].strip())
        items.append({"published_date": published_date, "title": title, "url": url})

    # De-dupe by url, preserve order
    seen = set()
    out: List[Dict[str, str]] = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        out.append(it)
    return out


def _normalize_feed_urls(feeds_cfg: Any) -> List[str]:
    """
    Supports:
      - list[str] of listing URLs
      - list[dict] with keys like list_url/url
    """
    urls: List[str] = []
    if not feeds_cfg:
        return urls

    for f in feeds_cfg:
        if isinstance(f, str):
            urls.append(f)
        elif isinstance(f, dict):
            u = f.get("list_url") or f.get("url")
            if u:
                urls.append(u)
    return urls


def run_pipeline(db_path: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bounded listing fetch + parse. Does NOT write to DB yet.
    (This is intentionally minimal to prevent Colab hangs.)
    """
    feeds_cfg = config.get("feeds") or [
        "https://www.energy.gov/lpo/listings/edf-news",
        "https://www.energy.gov/lpo/listings/lpo-press-releases",
    ]
    feed_urls = _normalize_feed_urls(feeds_cfg)

    max_pages = int(config.get("max_pages", 1))
    max_items_per_page = int(config.get("max_items_per_page", 0))  # 0 means no limit
    rate_limit_s = float(config.get("rate_limit_s", 0.25))
    timeout = config.get("timeout", (10, 30))  # (connect, read)
    max_retries = int(config.get("max_retries", 2))

    def fetch(session: requests.Session, url: str) -> requests.Response:
        backoff = 2
        for attempt in range(max_retries + 1):
            try:
                r = session.get(url, headers=HEADERS, timeout=timeout)
            except requests.RequestException:
                if attempt >= max_retries:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue

            # Retry only transient/throttle codes
            if r.status_code in (429, 502, 503, 504):
                if attempt >= max_retries:
                    r.raise_for_status()
                retry_after = r.headers.get("Retry-After")
                sleep_s = int(retry_after) if (retry_after and retry_after.isdigit()) else backoff
                time.sleep(sleep_s)
                backoff *= 2
                continue

            r.raise_for_status()
            return r

        raise RuntimeError("unreachable")

    total_items = 0
    session = requests.Session()

    for feed in feed_urls:
        for page in range(max_pages):
            page_url = feed if page == 0 else f"{feed}?page={page}"
            print(f"[fetch] {page_url}", flush=True)

            r = fetch(session, page_url)
            print(f"  status={r.status_code} bytes={len(r.text)}", flush=True)

            items = parse_listing(r.text)
            if max_items_per_page > 0:
                items = items[:max_items_per_page]

            print(f"  parsed_items={len(items)}", flush=True)
            total_items += len(items)

            if len(items) == 0:
                break

            time.sleep(rate_limit_s)

    return {"status": "ok", "items_parsed": total_items, "events": 0, "signals": 0}


