import hashlib
import sqlite3
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from .classify import classify_edf_subtype
from .signals import generate_signals
from rockquant.sources.fast41.classify import classify_fast41_subtype
from rockquant.sources.fast41.signals import generate_fast41_signals

BASE = "https://www.energy.gov"
HEADERS = {"User-Agent": "RockQuant/0.4"}

DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
)


def classify_subtype_for_feed(title: str, feed_url: str) -> str:
    netloc = urlparse(feed_url).netloc.lower()
    if "permitting.gov" in netloc:
        return classify_fast41_subtype(title)
    return classify_edf_subtype(title)

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
        # Ensure events.source_entity_id exists (idempotent, SQLite-safe)
        cols = [r[1] for r in cur.execute('PRAGMA table_info(events)').fetchall()]
        if 'source_entity_id' not in cols:
            cur.execute('ALTER TABLE events ADD COLUMN source_entity_id TEXT')
            conn.commit()

        items_parsed = 0
        new_events = 0
        skipped_no_link = 0
        skipped_no_date = 0

        # normalize feeds (strings or dicts)
        feed_urls = []
        for f in feeds:
            # Canonical source entity id by feed domain
            parsed_feed = urlparse(f)
            if 'permitting.gov' in parsed_feed.netloc.lower():
                source_entity_id = 'FAST41_COUN'
            elif 'energy.gov' in parsed_feed.netloc.lower() and '/lpo/' in f:
                source_entity_id = 'DOE_LPO'
            else:
                source_entity_id = parsed_feed.netloc.lower() or 'UNKNOWN'
            if isinstance(f, str):
                feed_urls.append((urlparse(f).path.split("/")[-1], f))
            elif isinstance(f, dict):
                feed_urls.append((f.get("name", "unknown"), f.get("list_url") or f.get("url")))

        for feed_name, feed_url in feed_urls:
            if not feed_url:
                continue

            last_err = None
            for attempt in range(1, 4):
                try:
                    # Increased timeouts: (connect, read)
                    last_err = None
                    for attempt in range(1, 4):
                        try:
                            # Increased timeouts: (connect, read)
                            r = requests.get(feed_url, headers=HEADERS, timeout=(20, 120))
                            break
                        except (requests.exceptions.Timeout, requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                            last_err = e
                            if attempt == 3:
                                raise
                            time.sleep(2 * attempt)
                    break
                except (requests.exceptions.Timeout, requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                    last_err = e
                    if attempt == 3:
                        raise
                    time.sleep(2 * attempt)
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
                    # Accept DOE LPO article paths and Permitting Council press-release paths
                    if href.startswith(("/articles/", "/lpo/articles/", "/newsroom/press-releases/")):
                        # Avoid capturing the listing page itself
                        if href.rstrip("/") == "/newsroom/press-releases":
                            continue
                        article_href = href
                        title = a.get_text(" ", strip=True)
                        break

                if not article_href:
                    skipped_no_link += 1
                    continue

                parsed_feed = urlparse(f)
                _base = f"{parsed_feed.scheme}://{parsed_feed.netloc}"
                article_url = article_href if article_href.startswith("http") else urljoin(_base, article_href)
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
                      event_type, event_subtype, source_doc_id, source_entity_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(canonical_event_key) DO UPDATE SET
                      title = excluded.title,
                      event_date = excluded.event_date,
                      url = excluded.url,
                source_entity_id = excluded.source_entity_id,
                                          event_subtype = excluded.event_subtype
            """, (canonical_key, event_date, title, article_url, "news", classify_subtype_for_feed(title, feed_url), source_doc_id, source_entity_id))
                if not existed:
                    new_events += 1

                time.sleep(rate_limit_s)

        conn.commit()
        total_events = cur.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        print(
            f"  skipped: no_article_link={skipped_no_link}, no_date={skipped_no_date}",
            flush=True,
        )

        # Generate signals from events
        signal_result = generate_signals(db_path)
        fast41_signal_result = generate_fast41_signals(db_path)
        signals_count = int(signal_result.get('signals', 0)) + int(fast41_signal_result.get('signals', 0))

        return {
            "status": "ok",
            "items_parsed": items_parsed,
            "events": total_events,
            "new_events": new_events,
            "signals": signals_count,
        }
    finally:
        conn.close()
