from pathlib import Path
import polars as pl

ROOT = Path(__file__).resolve().parent.parent
PARTS = ROOT / "data" / "interim" / "deep_20260911" / "part_*.parquet"
KEY_COLS = ["timestamp","type","side","price","size","flags","trade_id"]

def main():
    lf = pl.scan_parquet(str(PARTS))
    quotes = lf.filter(pl.col("type") == "price_level_update")

    print("\n1. Rows by symbol and type")
    print(lf.group_by("symbol","type").agg(pl.len()).sort("symbol","type").collect())

    print("\n2. Flags on quote updates")
    print(quotes.group_by("flags").agg(pl.len()).sort("flags").collect())

    print("\n3. Zero-size updates per symbol")
    print(quotes.group_by("symbol").agg(pl.len().alias("updates"),
                                        (pl.col("size") == 0).sum().alias("size_zero"),
                                        ).sort("symbol").collect())

    print("\n4. Null counts per column")
    print(lf.null_count().collect())

    print("\n5. Duplicates")
    total = lf.select(pl.len()).collect().item()
    unique = lf.unique().select(pl.len()).collect().item()
    print(f"exact duplicate rows: {total - unique:,} of {total:,}")

    print("\n6. Backwards timestamps within each symbol (should be 0)")
    back = (pl.col("timestamp").diff().over("symbol") < pl.duration(microseconds=0))
    print(lf.group_by("symbol").agg(back.sum().alias("backwards")).sort("symbol").collect())

if __name__ == "__main__":
    main()