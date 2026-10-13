import polars as pl
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KEY_COLS = ["timestamp","type","side","price","size","flags","trade_id"]
PARTS = ROOT / "data" / "interim" / "deep_20260911" / "part_*.parquet"

def drop_consecutive_duplicates(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Drop a row identical to the previous row of the same symbol."""
    same_as_prev = pl.all_horizontal(
        [pl.col(c).eq_missing(pl.col(c).shift(1)).over("symbol") for c in KEY_COLS]
    )
    return lf.filter(~same_as_prev)

if __name__ == "__main__":
    lf = pl.scan_parquet(str(PARTS))
    before = lf.select(pl.len()).collect().item()
    after = drop_consecutive_duplicates(lf).select(pl.len()).collect().item()
    print(before - after)