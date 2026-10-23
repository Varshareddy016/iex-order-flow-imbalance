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

    pl.Config.set_tbl_cols(-1)          # show all columns, no "…"
    pl.Config.set_tbl_width_chars(220)

    print("\n7. Schema (column -> type)")
    for name, dtype in lf.collect_schema().items():
        print(f"  {name:<10} {dtype}")

    print("\n8. Cardinality (unique values per column; Polars counts null as one value)")
    print(f"total rows: {total:,}")
    cols = lf.collect_schema().names()
    print(lf.select([pl.col(c).n_unique().alias(c) for c in cols]).collect())

    print("\n9. Numeric columns: price and size, per symbol and type")
    nz = pl.col("size").filter(pl.col("size") > 0)   # size 0 = level removed, so leave it out of the typical size
    print(
        lf.group_by("symbol", "type")
          .agg(pl.col("price").min().alias("price_min"),
               pl.col("price").median().alias("price_med"),
               pl.col("price").max().alias("price_max"),
               nz.median().alias("size_med"),
               nz.quantile(0.99).alias("size_p99"),
               pl.col("size").max().alias("size_max"))
          .sort("symbol", "type").collect()
    )
    print(lf.select(pl.col("timestamp").min().alias("first"),
                    pl.col("timestamp").max().alias("last")).collect())

    print("\n10. Flags on trade reports")
    pl.Config.set_tbl_rows(-1)
    print(lf.filter(pl.col("type") == "trade_report")
            .group_by("flags").agg(pl.len()).sort("flags").collect())
    tr = lf.filter(pl.col("type") == "trade_report")
    print(tr.select(
        pl.len().alias("trades"),
        ((pl.col("flags") & 0x80) != 0).sum().alias("sweep_0x80"),
        ((pl.col("flags") & 0x40) != 0).sum().alias("extended_0x40"),
        ((pl.col("flags") & 0x20) != 0).sum().alias("oddlot_0x20"),
        ((pl.col("flags") & 0x10) != 0).sum().alias("tt_exempt_0x10"),
    ).collect())

if __name__ == "__main__":
    main()