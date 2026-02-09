import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from email.mime.text import MIMEText
import smtplib
from typing import List, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


DB_PATH = "results.sqlite"

def safe_get(url: str, timeout: int = 60) -> requests.Response:
    # Retries help when sites are slow or flaky from GitHub runners
    last_err = None
    for _ in range(3):
        try:
            r = requests.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive",
                },
            )
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
    raise last_err

def load_config() -> Dict[str, Any]:
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)


def db_init(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            url TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            published_at TEXT,
            snippet TEXT,
            matched TEXT,
            score INTEGER NOT NULL,
            first_seen_at TEXT NOT NULL
        )
    """)
    conn.commit()


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def match_keywords(text: str, keywords: List[str]) -> Tuple[List[str], int]:
    t = text.lower()
    matched = []
    score = 0
    for kw in keywords:
        if kw.lower() in t:
            matched.append(kw)
            # simple weighting: more weight for "definitive agreement"
            if kw.lower() == "definitive agreement":
                score += 5
            elif kw.lower() in ("acquisition",):
                score += 3
            else:
                score += 2
    return matched, score


def upsert_new(conn: sqlite3.Connection, item: Dict[str, Any]) -> bool:
    # returns True if inserted (new)
    try:
        conn.execute(
            "INSERT INTO items(url, source, title, published_at, snippet, matched, score, first_seen_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (
                item["url"],
                item["source"],
                item["title"],
                item.get("published_at"),
                item.get("snippet"),
                ", ".join(item.get("matched", [])),
                int(item.get("score", 0)),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def fetch_businesswire(url: str) -> List[Dict[str, Any]]:
    r = safe_get(url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    items = []
    # BusinessWire pages change; this is a resilient approach:
    # gather article links that look like newsroom items
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = normalize_whitespace(a.get_text(" "))
        if not text or len(text) < 12:
            continue
        if "/news/home/" in href:
            full = href if href.startswith("http") else "https://www.businesswire.com" + href
            items.append({
                "source": "BusinessWire",
                "title": text,
                "url": full,
                "published_at": None,
                "snippet": None,
            })

    # de-dupe by url while preserving order
    seen = set()
    uniq = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        uniq.append(it)
    return uniq[:50]


def fetch_prnewswire(url: str) -> List[Dict[str, Any]]:
    r = safe_get(url, timeout=45)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    items = []
    # On PRNewswire list pages, release links are typically /news-releases/<slug>.html
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        title = normalize_whitespace(a.get_text(" "))
        if "/news-releases/" in href and href.endswith(".html") and len(title) > 12:
            full = href if href.startswith("http") else "https://www.prnewswire.com" + href
            items.append({
                "source": "PRNewswire",
                "title": title,
                "url": full,
                "published_at": None,
                "snippet": None,
            })

    seen = set()
    uniq = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        uniq.append(it)
    return uniq[:60]


def fetch_globenewswire_json(url: str) -> List[Dict[str, Any]]:
    r = safe_get(url, timeout=45)
    r.raise_for_status()
    data = r.json()

    items = []
    # JSON schema can vary; handle common fields
    # Often: data["Items"] or data itself is a list
    raw_items = data.get("Items") if isinstance(data, dict) else data
    if not raw_items:
        return items

    for it in raw_items:
        title = normalize_whitespace(it.get("Title") or it.get("title") or "")
        link = it.get("Url") or it.get("url") or it.get("Link") or it.get("link")
        if not title or not link:
            continue
        published = it.get("Published") or it.get("published") or it.get("PubDate") or it.get("pubDate")
        published_iso = None
        if published:
            try:
                published_iso = dtparser.parse(published).isoformat()
            except Exception:
                published_iso = None

        full = link if str(link).startswith("http") else "https://www.globenewswire.com" + str(link)
        snippet = normalize_whitespace(it.get("Teaser") or it.get("teaser") or it.get("Summary") or it.get("summary") or "")

        items.append({
            "source": "GlobeNewswire",
            "title": title,
            "url": full,
            "published_at": published_iso,
            "snippet": snippet or None,
        })

    return items[:60]


def build_digest(new_hits: List[Dict[str, Any]]) -> str:
    lines = []
    lines.append(f"Daily M&A Radar — {datetime.now().strftime('%Y-%m-%d')}")
    lines.append("")
    if not new_hits:
        lines.append("No new keyword matches today.")
        return "\n".join(lines)

    # sort by score then recency-ish (published_at string)
    new_hits.sort(key=lambda x: (x.get("score", 0), x.get("published_at") or ""), reverse=True)

    current_source = None
    for it in new_hits:
        if it["source"] != current_source:
            current_source = it["source"]
            lines.append("")
            lines.append(f"== {current_source} ==")

        matched = ", ".join(it.get("matched", []))
        pub = it.get("published_at") or ""
        lines.append(f"- {it['title']}")
        lines.append(f"  {it['url']}")
        if pub:
            lines.append(f"  Published: {pub}")
        if matched:
            lines.append(f"  Matched: {matched} (score {it.get('score',0)})")
        if it.get("snippet"):
            lines.append(f"  Snippet: {it['snippet']}")
    return "\n".join(lines)


def send_email(smtp_host: str, smtp_port: int, smtp_user: str, smtp_pass: str,
               mail_from: str, mail_to: str, subject: str, body: str) -> None:
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = mail_to

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def main() -> None:
    cfg = load_config()
    keywords = cfg["keywords"]

    conn = sqlite3.connect(DB_PATH)
    db_init(conn)

    all_items: List[Dict[str, Any]] = []

    sources = [
        ("BusinessWire", lambda: fetch_businesswire(cfg["sources"]["businesswire"])),
        ("PRNewswire", lambda: fetch_prnewswire(cfg["sources"]["prnewswire"])),
        ("GlobeNewswire", lambda: fetch_globenewswire_json(cfg["sources"]["globenewswire_json"])),
    ]

    for name, fn in sources:
        try:
            all_items += fn()
        except Exception as e:
            print(f"[WARN] {name} fetch failed: {e}")

    new_hits: List[Dict[str, Any]] = []
    for it in all_items:
        text = f"{it.get('title','')} {it.get('snippet','')}"
        matched, score = match_keywords(text, keywords)
        if not matched:
            continue

        it["matched"] = matched
        it["score"] = score

        if upsert_new(conn, it):
            new_hits.append(it)

    body = build_digest(new_hits)

    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    email_from = os.environ["EMAIL_FROM"]
    email_to = os.environ["EMAIL_TO"]

    subject = f"{cfg['email']['subject_prefix']} ({datetime.now().strftime('%Y-%m-%d')})"
    send_email(
        smtp_host=cfg["email"]["smtp_host"],
        smtp_port=int(cfg["email"]["smtp_port"]),
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        mail_from=email_from,
        mail_to=email_to,
        subject=subject,
        body=body,
    )


if __name__ == "__main__":
    main()
