import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

import requests


def sanitize_filename(name: str, max_len: int = 140) -> str:
    name = name.strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "", name)  # Windows-illegal chars
    name = re.sub(r"\s+", " ", name)
    return (name[:max_len].rstrip()) or "transcript"


def download_json(url: str, api_key: str) -> Any:
    r = requests.get(
        url,
        headers={"x-api-key": api_key, "Accept": "application/json"},
        timeout=120,
        allow_redirects=True,
    )
    r.raise_for_status()
    return r.json()


def _join_text_array(arr: Any) -> str:
    if not isinstance(arr, list):
        return ""
    parts: list[str] = []
    for item in arr:
        if isinstance(item, dict):
            for k in ("text", "content", "value"):
                v = item.get(k)
                if isinstance(v, str) and v.strip():
                    parts.append(v.strip())
                    break
        elif isinstance(item, str) and item.strip():
            parts.append(item.strip())
    return "\n".join(parts).strip()


def _deep_collect_text_fields(obj: Any, max_chunks: int = 50000) -> list[str]:
    out: list[str] = []

    def rec(x: Any):
        if len(out) >= max_chunks:
            return
        if isinstance(x, dict):
            v = x.get("text")
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
            for vv in x.values():
                rec(vv)
        elif isinstance(x, list):
            for vv in x:
                rec(vv)

    rec(obj)

    cleaned: list[str] = []
    prev: Optional[str] = None
    for s in out:
        if s != prev:
            cleaned.append(s)
        prev = s
    return cleaned


def extract_text_from_raw_transcript(raw: Any) -> str:
    if isinstance(raw, dict):
        t = raw.get("transcript")
        if isinstance(t, dict):
            for k in ("text", "plainText", "content", "body"):
                v = t.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()

            for key in ("segments", "entries", "paragraphs", "turns", "speakerTurns", "items"):
                joined = _join_text_array(t.get(key))
                if joined:
                    return joined

        if isinstance(raw.get("text"), str) and raw["text"].strip():
            return raw["text"].strip()

        for key in ("segments", "entries", "paragraphs", "turns", "speakerTurns", "items"):
            joined = _join_text_array(raw.get(key))
            if joined:
                return joined

    chunks = _deep_collect_text_fields(raw)
    if chunks:
        return "\n".join(chunks).strip()

    raise RuntimeError("Could not extract transcript text from raw transcript JSON.")


def meta_obj_to_txt(api_key: str, ticker: str, meta_obj: dict, base_dir: Optional[Path] = None) -> Path:
    """
    Given ONE transcript-document metadata object (the items from _index.json),
    downloads its raw JSON and writes transcript_raw/<ticker>/<Title>_<id>.txt
    Returns the written txt path.
    """
    base_dir = base_dir or Path.cwd()

    file_url = meta_obj.get("fileUrl")
    if not file_url:
        raise RuntimeError("Metadata object missing fileUrl.")

    event = meta_obj.get("event") or {}
    title = event.get("title") or f"transcript_{meta_obj.get('id')}"
    safe_title = sanitize_filename(str(title))

    doc_id = meta_obj.get("id")
    suffix = f"_{doc_id}" if isinstance(doc_id, int) else ""

    out_dir = base_dir / "transcript_raw" / ticker.lower()
    out_dir.mkdir(parents=True, exist_ok=True)

    txt_path = out_dir / f"{safe_title}{suffix}.txt"

    raw = download_json(file_url, api_key)
    text = extract_text_from_raw_transcript(raw)

    txt_path.write_text(text + "\n", encoding="utf-8")
    return txt_path


def main():
    api_key = os.environ.get("QUARTR_API_KEY")
    if not api_key:
        raise SystemExit("Set QUARTR_API_KEY (PowerShell: $env:QUARTR_API_KEY='...')")

    if len(sys.argv) < 3:
        raise SystemExit("Usage: python meta_to_txt.py <ticker> <path-to-meta-json>")

    ticker = sys.argv[1].strip().upper()
    meta_path = Path(sys.argv[2]).expanduser().resolve()
    if not meta_path.exists():
        raise SystemExit(f"Meta JSON not found: {meta_path}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    # Support either direct object or {"data": {...}}
    meta_obj = meta["data"] if isinstance(meta.get("data"), dict) else meta

    txt_path = meta_obj_to_txt(api_key, ticker, meta_obj)
    print(f"Wrote: {txt_path}")


if __name__ == "__main__":
    main()
