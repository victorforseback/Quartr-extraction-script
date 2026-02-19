import json
import os
from collections import defaultdict
from pathlib import Path

from get_meta import build_meta_for_ticker
from meta_to_txt import meta_obj_to_txt
from slides_to_txt import list_slide_decks_for_event, slide_deck_obj_to_txt


def choose_best(items: list[dict], key: str = "updatedAt") -> dict:
    def k(x):
        v = x.get(key)
        return v or ""
    return sorted(items, key=k, reverse=True)[0] if items else {}


def main():
    api_key = os.environ.get("QUARTR_API_KEY")
    if not api_key:
        raise SystemExit("Set QUARTR_API_KEY (PowerShell: $env:QUARTR_API_KEY='...')")

    companies = ["MSFT"]
    base_dir = Path.cwd()

    for ticker in companies:
        ticker = ticker.strip().upper()
        print(f"\n=== {ticker} ===")

        # Build transcript meta index (earnings-call filtered in get_meta.py)
        index_path = build_meta_for_ticker(api_key, ticker, base_dir=base_dir)
        items = json.loads(index_path.read_text(encoding="utf-8"))
        if not isinstance(items, list) or not items:
            print("No transcript metadata items found.")
            continue

        by_event = defaultdict(list)
        for it in items:
            eid = it.get("eventId")
            if isinstance(eid, int):
                by_event[eid].append(it)

        processed = 0
        skipped = 0

        for event_id, transcript_metas in by_event.items():
            decks = list_slide_decks_for_event(api_key, event_id)
            if not decks:
                skipped += 1
                print(f"Skip event {event_id}: no slide deck found.")
                continue

            transcript_meta = choose_best(transcript_metas, key="updatedAt")
            deck_meta = choose_best(decks, key="updatedAt")

            txt_path = None
            slides_txt_path = None

            try:
                # Write transcript text
                txt_path = meta_obj_to_txt(api_key, ticker, transcript_meta, base_dir=base_dir)

                # Write slides text (one line per page) WITHOUT saving PDFs
                slides_txt_path = slide_deck_obj_to_txt(api_key, ticker, deck_meta, base_dir=base_dir)

                processed += 1
                print(f"OK event {event_id}: {txt_path.name} + {slides_txt_path.name}")

            except Exception as e:
                # Optional cleanup to avoid partial outputs (stronger failsafe)
                if txt_path and txt_path.exists():
                    try:
                        txt_path.unlink()
                    except Exception:
                        pass
                if slides_txt_path and slides_txt_path.exists():
                    try:
                        slides_txt_path.unlink()
                    except Exception:
                        pass

                skipped += 1
                print(f"Skip event {event_id}: failed ({e})")

        print(f"Done {ticker}: processed={processed}, skipped={skipped}")


if __name__ == "__main__":
    main()
