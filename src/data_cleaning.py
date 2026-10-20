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

def add_session(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Label each row pre / regular / after using New York clock time."""
    t = pl.col("timestamp").dt.convert_time_zone("America/New_York").dt.time()
    return lf.with_columns(pl.when(t<pl.time(9,30)).then(pl.lit("pre"))
                           .when(t<pl.time(16,0)).then(pl.lit("regular"))
                           .otherwise(pl.lit("after"))
                           .alias("session"))

def add_usable_trade(lf: pl.LazyFrame) -> pl.LazyFrame:
     """Mark regular-session round-lot trades (extended-hours and odd-lot bits both clear)."""
     return lf.with_columns(
         (
             (pl.col("type") == "trade_report") &
             (pl.col("session") == "regular") &
             ((pl.col("flags") & 0x60) == 0)
         ).alias("usable_trade")
     )

def rebuild_book(quotes: pl.DataFrame) -> pl.DataFrame:
    """Replay price-level updates for ONE symbol (in file order) and record
    the best bid/ask after every update with flags == 1."""
    bids,asks = {},{}
    time_stamps,bid_prices,bid_sizes,ask_prices,ask_sizes = [],[],[],[],[]
    for ts , side , price , size , flags in quotes.select(
        "timestamp","side","price","size","flags"
    ).iter_rows():
        book = bids if side == "B" else asks
        if size == 0 :
            book.pop(price,None)
        else:
            book[price] = size
        if flags == 1 :
            bp = max(bids) if bids else None
            ap = min(asks) if asks else None
            time_stamps.append(ts)
            bid_prices.append(bp)
            bid_sizes.append(bids[bp] if bp is not None else None)
            ask_prices.append(ap)
            ask_sizes.append(asks[ap] if ap is not None else None)
    return pl.DataFrame(
        {"timestamp" : time_stamps , "bid_prices" : bid_prices , "bid_sizes" : bid_sizes,
         "ask_prices": ask_prices,"ask_sizes": ask_sizes},
         schema={"timestamp" : pl.Datetime("us","UTC"),"bid_prices":pl.Float64,
                 "bid_sizes" : pl.Int64,"ask_prices":pl.Float64,"ask_sizes":pl.Int64},
    )

# if __name__ == "__main__":
#     lf = pl.scan_parquet(str(PARTS))
#     before = lf.select(pl.len()).collect().item()
#     after = drop_consecutive_duplicates(lf).select(pl.len()).collect().item()
#     print(before - after)

#     print(add_session(lf).group_by("symbol", "session").agg(pl.len()).sort("symbol", "session").collect())

#     lf2 = add_usable_trade(add_session(lf))
#     print(lf2.filter(pl.col("type") == "trade_report").group_by("symbol").agg(
#         pl.len().alias("trades"),pl.col("usable_trade").sum().alias("usable"),
#     ).sort("symbol").collect())

# if __name__ == "__main__":
#     lf = pl.scan_parquet(str(PARTS))
#     spy = (lf.filter((pl.col("symbol") == "SPY") & (pl.col("type") == "price_level_update"))
#              .head(200_000).collect())
#     book = rebuild_book(spy)
#     print(book.head(8))
#     print(book.tail(5))
#     print("empty bid:", book["bid_prices"].is_null().sum(),
#           "| empty ask:", book["ask_prices"].is_null().sum(),
#           "| crossed:", (book["bid_prices"] >= book["ask_prices"]).sum())

if __name__ == "__main__":
    import time
    lf = pl.scan_parquet(str(PARTS))
    spy = lf.filter((pl.col("symbol") == "SPY") & (pl.col("type") == "price_level_update")).collect()
    t0 = time.time()
    book = rebuild_book(spy)
    print(len(spy), len(book), f"{time.time() - t0:.0f}s")
    print("empty bid:", book["bid_prices"].is_null().sum(),
          "| empty ask:", book["ask_prices"].is_null().sum(),
          "| crossed:", (book["bid_prices"] >= book["ask_prices"]).sum())