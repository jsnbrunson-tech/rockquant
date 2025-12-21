from __future__ import annotations

import hashlib
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.energy.gov"
HEADERS = {"User-Agent": "RockQuant/0.1"}

MONTHS = ("January","February","March","April","May","June","July","August","September","October","November","December")
DATE_RE = re.compile(rf"^({'|'.join(MONTHS)})\s+\d{{1,2}},\s+\d{{4}}$")


def _canonical_key(url: str) -> str:
    path = urllib.parse.urlparse(url).path.rstrip("/").lower() or "/"
    return hashlib.sha256(f"doe|{path}".encode("utf-8")).hexdigest()


def _parse_listing(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []
    for row in soup.select("div.views-row"):
        # find date
        date_raw = None
        for s in row.stripped_strings:
            if DATE_RE.match(s):
                date_raw = s
                break
        if not date_raw:
            continue

        # find first article link
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
        items.append({"published_date": date_raw, "title": title, "url": url})
    # de-dupe by url
    seen = set()
    out = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        out.append(it)
    return out


def run_pipeline(db_path: str, config: Dict[str, Any]) -> Dict[str, Any]:
    # Bounded and timed: should finish quickly
    feeds = config.get("feeds") or [
        "https://www.energy.gov/lpo/listings/edf-news",
        "https://www.energy.gov/lpo/listings/lpo-press-releases",
    ]
    max_pages = int(config.get("max_pages", 1))
    rate_limit_s = float(config.get("rate_limit_s", 0.25))

    total_items = 0
    for feed in feeds:
        for page in range(max_pages):
            url = feed if page == 0 else f"{feed}?page={page}"
            print(f"[fetch] {url}")
            r = requests.get(url, headers=HEADERS, timeout=(10, 30))
            print(f"  status={r.status_code} bytes={len(r.text)}")
            r.raise_for_status()
            items = _parse_listing(r.text)
            print(f"  parsed_items={len(items)}")
            total_items += len(items)
            if len(items) == 0:
                break
            time.sleep(rate_limit_s)

    return {"status": "ok", "items_parsed": total_items}

