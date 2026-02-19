import io
import re
from pathlib import Path
from typing import Optional, Tuple

import requests
from pypdf import PdfReader

API_BASE = "https://api.quartr.com/public/v3"


def sanitize_filename(name: str, max_len: int = 160) -> str:
    name = name.strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "", name)
    name = re.sub(r"\s+", " ", name)
    return (name[:max_len].rstrip()) or "slides"


def quartr_get(path: str, api_key: str, params: Optional[dict] = None) -> dict:
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers={"x-api-key": api_key}, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def list_slide_decks_for_event(api_key: str, event_id: int) -> list[dict]:
    resp = quartr_get(
        "/documents/slides",
        api_key,
        params={"eventIds": str(event_id), "expand": "event", "limit": 50, "direction": "asc"},
    )
    data = resp.get("data", [])
    return data if isinstance(data, list) else []


def _download_pdf_bytes(url: str, api_key: str) -> bytes:
    r = requests.get(
        url,
        headers={"x-api-key": api_key, "Accept": "application/pdf, application/octet-stream, */*"},
        timeout=180,
        allow_redirects=True,
    )
    r.raise_for_status()
    return r.content


def _normalize_one_line(txt: str) -> str:
    txt = (txt or "").replace("\r", "\n")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _pdf_bytes_to_page_lines(pdf_bytes: bytes) -> Tuple[list[str], dict]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    lines: list[str] = []
    non_empty = 0
    total_chars = 0

    for page in reader.pages:
        txt = _normalize_one_line(page.extract_text() or "")
        lines.append(txt)
        if txt:
            non_empty += 1
            total_chars += len(txt)

    metrics = {
        "pages": len(lines),
        "non_empty_pages": non_empty,
        "total_chars": total_chars,
        "coverage": (non_empty / len(lines)) if lines else 0.0,
    }
    return lines, metrics


def slide_deck_obj_to_txt(
    api_key: str,
    ticker: str,
    deck_obj: dict,
    base_dir: Optional[Path] = None,
    # Quality gate (tune if needed)
    min_coverage: float = 0.20,
    min_total_chars: int = 200,
) -> Path:
    """
    Direct text extraction only. If too sparse, raises RuntimeError.
    Writes one line per page to:
      slides_text/<ticker>/<event-title>_event_<eventId>_deck_<deckId>.txt
    """
    base_dir = base_dir or Path.cwd()

    file_url = deck_obj.get("fileUrl")
    if not isinstance(file_url, str) or not file_url:
        deck_id = deck_obj.get("id")
        if not isinstance(deck_id, int):
            raise RuntimeError("Slide deck missing fileUrl and id.")
        detail = quartr_get(f"/documents/slides/{deck_id}", api_key, params={"expand": "event"})
        deck_obj = detail.get("data") or {}
        file_url = deck_obj.get("fileUrl")
        if not isinstance(file_url, str) or not file_url:
            raise RuntimeError("Could not resolve slide deck fileUrl.")

    deck_id = deck_obj.get("id")
    event_id = deck_obj.get("eventId")
    event = deck_obj.get("event") or {}
    title = event.get("title") or f"event_{event_id}"
    safe_title = sanitize_filename(str(title))

    out_dir = base_dir / "slides_text" / ticker.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_title}_event_{event_id}_deck_{deck_id}.txt"

    pdf_bytes = _download_pdf_bytes(file_url, api_key)
    lines, metrics = _pdf_bytes_to_page_lines(pdf_bytes)

    if metrics["pages"] == 0:
        raise RuntimeError("Slide PDF had 0 pages.")
    if metrics["coverage"] < min_coverage or metrics["total_chars"] < min_total_chars:
        raise RuntimeError(
            f"Slide text extraction too sparse "
            f"(pages={metrics['pages']}, non_empty={metrics['non_empty_pages']}, "
            f"coverage={metrics['coverage']:.2f}, total_chars={metrics['total_chars']})."
        )

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path
