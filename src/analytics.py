"""First analytics on the processed data: OFI vs mid-price change, and OFI vs next-trade side.

Needs only data/processed/ (no raw pcap). Run from the repo root:
    python src/analytics.py

Writes results/*.csv with the same numbers as notebooks/phase2_analysis.ipynb.
"""
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import spearmanr, theilslopes

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
OUT = ROOT / "results"
SYMBOLS = ["SPY", "QQQ", "IWM", "EMB", "VCSH"]
WINDOWS = ["1s", "10s", "1m"]
SEED = 42

def before_close_minute(ts: pl.Expr) -> pl.Expr:
    """True before 15:59 New York time. The last minute has one far-away quote that makes the mid jump $11.80."""
    ny = ts.dt.convert_time_zone("America/New_York")
    return (ny.dt.hour() < 15) | ((ny.dt.hour() == 15) & (ny.dt.minute() < 59))

def window_fit(book: pl.DataFrame, sym: str, every: str, n: int = 5000) -> dict:
    """Sum OFI and take the mid change per window, then rank correlation and a Theil-Sen slope on a seeded sample."""
    d = (book.filter((pl.col("symbol") == sym) & (pl.col("session") == "regular"))
             .sort("timestamp")
             .group_by_dynamic("timestamp", every=every)
             .agg(pl.col("ofi").sum().alias("ofi_sum"), pl.col("mid").last().alias("mid_end"))
             .with_columns(pl.col("mid_end").diff().alias("dmid_w"))
             .drop_nulls(["ofi_sum", "dmid_w"])
             .filter(before_close_minute(pl.col("timestamp"))))
    x, y = d["ofi_sum"].to_numpy(), d["dmid_w"].to_numpy()
    idx = np.random.default_rng(SEED).choice(len(x), size=min(n, len(x)), replace=False)
    s, _, lo, hi = theilslopes(y[idx], x[idx])
    return dict(symbol=sym, window=every, n_windows=len(x),
                spearman=round(float(spearmanr(x, y)[0]), 3),
                share_dmid_zero=round(float((y == 0).mean()), 3),
                cents_per_1000_shares=round(s * 1000 * 100, 4),
                lo=round(lo * 1000 * 100, 4), hi=round(hi * 1000 * 100, 4))

def cumulative_book(book: pl.DataFrame) -> pl.DataFrame:
    """Running OFI per symbol, so OFI over any interval is just end minus start."""
    return (book.sort("timestamp")
                .with_columns(pl.col("ofi").fill_null(0).cum_sum().over("symbol").alias("cum_ofi"))
                .select("symbol", "timestamp", "cum_ofi", "mid"))

def label_trades(trades: pl.DataFrame, b: pl.DataFrame, window_ms: int) -> pl.DataFrame:
    """OFI over window_ms before each usable trade, and the trade side (+1 above the prior mid, -1 below, 0 at it)."""
    b_start = b.select("symbol", pl.col("timestamp").alias("ts_b"), pl.col("cum_ofi").alias("cum_start"))
    t = (trades.filter(pl.col("usable_trade"))
               .select("symbol", "timestamp", "price")
               .sort("timestamp")
               .with_columns((pl.col("timestamp") - pl.duration(milliseconds=window_ms)).alias("t_start")))
    return (t.join_asof(b, on="timestamp", by="symbol", strategy="backward", allow_exact_matches=False, check_sortedness=False)
             .rename({"cum_ofi": "cum_end", "mid": "mid_before"})
             .join_asof(b_start, left_on="t_start", right_on="ts_b", by="symbol", strategy="backward", check_sortedness=False)
             .with_columns((pl.col("cum_end") - pl.col("cum_start")).alias("ofi_prior"),
                           (pl.col("price") - pl.col("mid_before")).round(4).sign().alias("dir"))
             .drop_nulls(["ofi_prior", "dir"]))

def next_trade_table(trades: pl.DataFrame, d: pl.DataFrame) -> pl.DataFrame:
    """Hit rate of sign(OFI over the prior second) against the trade side, next to an always-guess-majority baseline."""
    n_usable = trades.filter(pl.col("usable_trade")).group_by("symbol").len().rename({"len": "n_usable"})
    sc = (d.filter(pl.col("dir") != 0)
           .with_columns((pl.col("ofi_prior").sign() == pl.col("dir")).alias("hit"))
           .group_by("symbol")
           .agg(pl.len().alias("n_classified"),
                (pl.col("dir") > 0).mean().alias("share_buys"),
                (pl.col("ofi_prior") == 0).mean().alias("share_ofi_zero"),
                pl.col("hit").filter(pl.col("ofi_prior") != 0).mean().alias("hit_rate"),
                pl.col("hit").filter(pl.col("ofi_prior") != 0).len().alias("n_scored")))
    return (n_usable.join(sc, on="symbol")
            .with_columns(pl.max_horizontal(pl.col("share_buys"), 1 - pl.col("share_buys")).alias("baseline"),
                          (1.96 * (pl.col("hit_rate") * (1 - pl.col("hit_rate")) / pl.col("n_scored")).sqrt()).alias("margin_95"))
            .with_columns(pl.col(pl.Float64).round(3))
            .sort("symbol"))

def shrink_and_gap_table(trades: pl.DataFrame, b: pl.DataFrame, d: pl.DataFrame) -> pl.DataFrame:
    """Hit rate as the OFI window shrinks toward the trade, then with a gap left before the trade (leakage check)."""
    rows = []
    for ms in [1000, 100, 10, 1]:
        s = label_trades(trades, b, ms).filter((pl.col("dir") != 0) & (pl.col("ofi_prior") != 0))
        rows.append(s.group_by("symbol").agg(pl.lit("window").alias("test"), pl.lit(ms).alias("ms"),
                                             (pl.col("ofi_prior").sign() == pl.col("dir")).mean().round(3).alias("hit_rate"),
                                             pl.len().alias("n")))
    # gap test: the window still starts 1 s before the trade but stops gap_ms early
    for gap in [10, 100, 500]:
        g = (d.select("symbol", "timestamp", "dir", "cum_start")
              .with_columns((pl.col("timestamp") - pl.duration(milliseconds=gap)).alias("t_end"))
              .sort("t_end")
              .join_asof(b.select("symbol", pl.col("timestamp").alias("t_end"), pl.col("cum_ofi").alias("cum_gap")),
                         on="t_end", by="symbol", strategy="backward", check_sortedness=False)
              .with_columns((pl.col("cum_gap") - pl.col("cum_start")).alias("ofi_gap"))
              .filter((pl.col("dir") != 0) & (pl.col("ofi_gap") != 0)))
        rows.append(g.group_by("symbol").agg(pl.lit("gap").alias("test"), pl.lit(gap).alias("ms"),
                                             (pl.col("ofi_gap").sign() == pl.col("dir")).mean().round(3).alias("hit_rate"),
                                             pl.len().alias("n")))
    return pl.concat(rows).with_columns(pl.col("ms").cast(pl.Int64)).sort("test", "symbol", "ms", descending=[True, False, False])

def main():
    OUT.mkdir(exist_ok=True)
    book = pl.read_parquet(PROCESSED / "book_states.parquet")
    trades = pl.read_parquet(PROCESSED / "trades.parquet")
    pl.Config.set_tbl_rows(30); pl.Config.set_tbl_cols(12)

    print("== OFI vs mid-price change, by symbol and window ==")
    fits = pl.DataFrame([window_fit(book, s, w) for s in SYMBOLS for w in WINDOWS])
    fits.write_csv(OUT / "ofi_vs_dmid_by_window.csv")
    print(fits)

    print("\n== Next-trade side vs prior 1 s OFI ==")
    b = cumulative_book(book)
    d = label_trades(trades, b, 1000)
    nxt = next_trade_table(trades, d)
    nxt.write_csv(OUT / "next_trade_hit_rate.csv")
    print(nxt)

    print("\n== Hit rate by window length and by gap before the trade ==")
    sg = shrink_and_gap_table(trades, b, d)
    sg.write_csv(OUT / "hit_rate_by_window_and_gap.csv")
    print(sg)

    print(f"\nwrote 3 tables to {OUT}")

if __name__ == "__main__":
    main()
