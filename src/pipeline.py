"""Single entry point: raw DEEP pcap -> processed, analysis-ready data.

Run from the repo root:
    python src/pipeline.py             # reuse parsed parts if they exist
    python src/pipeline.py --reparse   # re-parse the raw pcap (~10 hours)
"""
import argparse
import time
from pathlib import Path

import data_access
import data_cleaning
import profile_data

ROOT = Path(__file__).resolve().parent.parent
INTERIM = ROOT / "data" / "interim" / "deep_20260911"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reparse", action="store_true", help="re-parse the raw pcap")
    args = ap.parse_args()
    t0 = time.time()

    print("== 1. ACQUIRE / PARSE ==")
    have_parts = any(INTERIM.glob("part_*.parquet"))
    if args.reparse or not have_parts:
        if not data_access.RAW.exists():
            raise SystemExit(
                f"Raw file not found: {data_access.RAW}\n"
                "Download it (see data/raw/README.md), or copy data/interim/ here,\n"
                "or skip the pipeline and open notebooks/phase2_analysis.ipynb,\n"
                "which only needs data/processed/."
            )
        data_access.main()
    else:
        print(f"parsed parts found in {INTERIM}, skipping (use --reparse to redo)")

    print("\n== 2. PROFILE ==")
    profile_data.main()

    print("\n== 3. CLEAN / TRANSFORM ==")
    data_cleaning.build_processed()

    print(f"\nPipeline finished in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()