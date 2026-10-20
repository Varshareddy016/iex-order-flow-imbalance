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

def add_ofi(book: pl.DataFrame) -> pl.DataFrame:
    """Best-level order flow imbalance for each recorded state (Cont, Kukanov & Stoikov 2014)."""
    bp , bq = pl.col("bid_prices") , pl.col("bid_sizes")
    ap , aq = pl.col("ask_prices") , pl.col("ask_sizes")
    bp0, bq0 = bp.shift(1), bq.shift(1)
    ap0, aq0 = ap.shift(1), aq.shift(1)

    bid_part = pl.when(bp >= bp0).then(bq).otherwise(0) - pl.when(bp <= bp0).then(bq0).otherwise(0)
    ask_part = pl.when(ap <= ap0).then(aq).otherwise(0) - pl.when(ap >= ap0).then(aq0).otherwise(0)

    valid = pl.all_horizontal([e.is_not_null() for e in (bp, bq, ap, aq, bp0, bq0, ap0, aq0)])
    return book.with_columns(
        pl.when(valid).then(bid_part - ask_part).otherwise(None).alias("ofi")
    )
SYMBOLS = ["SPY", "QQQ", "IWM", "EMB", "VCSH"]
OUT = ROOT / "data" / "processed"

def build_processed():
    OUT.mkdir(parents=True, exist_ok=True)
    lf = drop_consecutive_duplicates(pl.scan_parquet(str(PARTS)))

    books = []
    for sym in SYMBOLS:
        q = lf.filter((pl.col("symbol") == sym) & (pl.col("type") == "price_level_update")).collect()
        books.append(add_ofi(rebuild_book(q)).with_columns(pl.lit(sym).alias("symbol")))
    book = add_session(pl.concat(books).lazy()).with_columns(
        ((pl.col("bid_prices") + pl.col("ask_prices")) / 2).alias("mid"),
        (pl.col("ask_prices") - pl.col("bid_prices")).alias("spread"),
    ).with_columns(pl.col("mid").diff().over("symbol").alias("dmid")).collect()
    book.write_parquet(OUT / "book_states.parquet")

    trades = add_usable_trade(add_session(lf.filter(pl.col("type") == "trade_report"))).collect()
    trades.write_parquet(OUT / "trades.parquet")
    print("book states:", len(book), "| trades:", len(trades))

if __name__ == "__main__":
    build_processed()