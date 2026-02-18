import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

API_BASE = "https://api.quartr.com/public/v3"


def sanitize_slug(s: str, max_len: int = 80) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    return (s[:max_len].strip("-")) or "item"


def quartr_get(path: str, api_key: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers={"x-api-key": api_key}, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def list_transcript_documents_by_ticker(api_key: str, ticker: str, limit: int = 200) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    cursor = 0

    while True:
        resp = quartr_get(
            "/documents/transcripts",
            api_key,
            params={
                "tickers": ticker.upper(),
                "limit": limit,
                "cursor": cursor,
                "direction": "asc",
                "expand": "event",
            },
        )

        data = resp.get("data", [])
        if not isinstance(data, list) or not data:
            break

        all_items.extend(data)

        next_cursor = (resp.get("pagination") or {}).get("nextCursor")
        if next_cursor is None:
            break
        cursor = int(next_cursor)

    return all_items


def write_meta_files(ticker: str, items: List[Dict[str, Any]], base_dir: Optional[Path] = None) -> Path:
    """
    Writes:
      transcript_meta/<ticker>/_index.json
      transcript_meta/<ticker>/<event-title>_<doc-id>.json
    Returns the path to the index file.
    """
    base_dir = base_dir or Path.cwd()
    ticker_slug = sanitize_slug(ticker)

    out_dir = base_dir / "transcript_meta" / ticker_slug
    out_dir.mkdir(parents=True, exist_ok=True)

    # Per-document meta files (optional but useful)
    for item in items:
        doc_id = item.get("id")
        event = item.get("event") or {}
        title = event.get("title") or f"event_{item.get('eventId')}"
        safe_title = sanitize_slug(str(title), max_len=80)

        if not isinstance(doc_id, int):
            continue

        path = out_dir / f"{safe_title}_{doc_id}.json"
        path.write_text(json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")

    index_path = out_dir / "_index.json"
    index_path.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")

    return index_path


def build_meta_for_ticker(api_key: str, ticker: str, base_dir: Optional[Path] = None) -> Path:
    items = list_transcript_documents_by_ticker(api_key, ticker)
    return write_meta_files(ticker, items, base_dir=base_dir)


def main():
    api_key = os.environ.get("QUARTR_API_KEY")
    if not api_key:
        raise SystemExit("Set QUARTR_API_KEY (PowerShell: $env:QUARTR_API_KEY='...')")

    ticker = "AAPL"
    if len(sys.argv) >= 2:
        ticker = sys.argv[1].strip().upper()

    index_path = build_meta_for_ticker(api_key, ticker)
    print(f"Ticker: {ticker}")
    print(f"Index written: {index_path}")


if __name__ == "__main__":
    main()
