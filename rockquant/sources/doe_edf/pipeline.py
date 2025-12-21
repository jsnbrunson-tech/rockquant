import hashlib
import sqlite3
import time
import re
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from .classify import classify_edf_subtype

BASE = "https://www.energy.gov"
HEADERS = {"User-Agent": "RockQuant/0.4"}

DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
)

def run_pipeline(db_path: str, config: dict) -> dict:
    feeds = config.get("feeds") or [
        "https://www.energy.gov/lpo/listings/edf-news",
        "https://www.energy.gov/lpo/listings/lpo-press-releases",
    ]
    max_items = int(config.get("max_items_per_page", 10))
    rate_limit_s = float(config.get("rate_limit_s", 0.25))

    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.OperationalError:
            pass

        cur = conn.cursor()

        items_parsed = 0
        new_events = 0
        skipped_no_link = 0
        skipped_no_date = 0

        # normalize feeds (strings or dicts)
        feed_urls = []
        for f in feeds:
            if isinstance(f, str):
                feed_urls.append((urlparse(f).path.split("/")[-1], f))
            elif isinstance(f, dict):
                feed_urls.append((f.get("name", "unknown"), f.get("list_url") or f.get("url")))

        for feed_name, feed_url in feed_urls:
            if not feed_url:
                continue

            r = requests.get(feed_url, headers=HEADERS, timeout=(10, 30))
            print(f"[fetch] {feed_url}\n  status={r.status_code} bytes={len(r.content)}", flush=True)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("div.views-row")[:max_items]
            print(f"  parsed_items={len(rows)}", flush=True)

            for row in rows:
                # strict article link selection
                article_href = None
                title = None
                for a in row.find_all("a", href=True):
                    href = a["href"].strip()
                    if href.startswith("/articles/") or href.startswith("/lpo/articles/"):
                        article_href = href
                        title = a.get_text(" ", strip=True)
                        break

                if not article_href:
                    skipped_no_link += 1
                    continue

                article_url = article_href if article_href.startswith("http") else (BASE + article_href)
                title = (title or "").strip() or article_url

                # exact date from row strings
                date_str = None
                for s in row.stripped_strings:
                    m = DATE_RE.search(s)
                    if m:
                        date_str = m.group(0)
                        break
                if not date_str:
                    skipped_no_date += 1
                    continue

                try:
                    event_date = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
                except ValueError:
                    skipped_no_date += 1
                    continue

                items_parsed += 1

                # upsert source_documents by article_url
                cur.execute("""
                    INSERT INTO source_documents (url, published_date, fetched_at)
                    VALUES (?, ?, datetime('now'))
                    ON CONFLICT(url) DO UPDATE SET
                      published_date = excluded.published_date,
                      fetched_at = excluded.fetched_at
                """, (article_url, event_date))

                source_doc_id = cur.execute(
                    "SELECT id FROM source_documents WHERE url = ?",
                    (article_url,)
                ).fetchone()[0]

                # canonical key = sha256("doe|" + normalized path)
                p = urlparse(article_url)
                url_path = (p.path.rstrip("/").lower() or "/")
                canonical_key = hashlib.sha256(f"doe|{url_path}".encode("utf-8")).hexdigest()

                existed = cur.execute(
                    "SELECT 1 FROM events WHERE canonical_event_key = ?",
                    (canonical_key,)
                ).fetchone()

                cur.execute("""
                    INSERT INTO events (
                      canonical_event_key, event_date, title, url,
                      event_type, event_subtype, source_doc_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(canonical_event_key) DO UPDATE SET
                      title = excluded.title,
                      event_date = excluded.event_date,
                      url = excluded.url,
                                          event_subtype = excluded.event_subtype
                """, (canonical_key, event_date, title, article_url, classify_edf_subtype(title), "news", source_doc_id))

                if not existed:
                    new_events += 1

                time.sleep(rate_limit_s)

        conn.commit()
        total_events = cur.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        print(f"  skipped: no_article_link={skipped_no_link}, no_date={skipped_no_date}", flush=True)

        return {"status": "ok", "items_parsed": items_parsed, "events": total_events, "new_events": new_events, "signals": 0}
    finally:
        conn.close()
