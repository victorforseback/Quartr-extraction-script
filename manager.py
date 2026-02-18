import json
import os
from pathlib import Path

from get_meta import build_meta_for_ticker
from meta_to_txt import meta_obj_to_txt


def main():

    #-------------------------------------------------------

    # Put the tickers you want here:
    companies = ["AAPL"]  # e.g. ["AAPL", "MSFT", "NVDA"]

    #-------------------------------------------------------

    api_key = os.environ.get("QUARTR_API_KEY")
    if not api_key:
        raise SystemExit("Set QUARTR_API_KEY (PowerShell: $env:QUARTR_API_KEY='...')")
    
    base_dir = Path.cwd()

    for ticker in companies:
        ticker = ticker.strip().upper()
        print(f"\n=== {ticker} ===")

        # 1) Build metadata + index
        index_path = build_meta_for_ticker(api_key, ticker, base_dir=base_dir)
        print(f"Index: {index_path}")

        # 2) Load index and generate txt for each metadata object
        items = json.loads(index_path.read_text(encoding="utf-8"))
        if not isinstance(items, list) or not items:
            print("No transcript items found.")
            continue

        written = 0
        skipped = 0

        for meta_obj in items:
            try:
                txt_path = meta_obj_to_txt(api_key, ticker, meta_obj, base_dir=base_dir)
                written += 1
                print(f"Wrote: {txt_path.name}")
            except Exception as e:
                skipped += 1
                print(f"Skip (id={meta_obj.get('id')}): {e}")

        print(f"Done {ticker}: written={written}, skipped={skipped}")


if __name__ == "__main__":
    main()