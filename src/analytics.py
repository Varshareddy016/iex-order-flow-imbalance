"""analytics.py: Phase 2 results for the IEX order-flow imbalance (OFI) project.

Reads the cleaned files in data/processed/ (book_states.parquet and trades.parquet)
and calculates every table we use in the report. It only calculates numbers. The
plots are made in notebooks/phase2_analysis.ipynb.

Run it with:  python src/analytics.py
It saves one CSV per table in results/.

What it does
- Cuts the regular session into windows (1 second, 10 seconds, 1 minute), adds up the
  OFI inside each window, and compares it with how much the mid-price changed from
  the previous window.
- Fits a Theil-Sen line (it is not thrown off by extreme points) through a random
  sample of 5,000 windows. The seed is fixed at 42, so the numbers are the same on
  every run.
- Drops 15:59 ET onward from the window analysis. At 15:59:59 one far-away ask made
  the mid jump by $11.80, which is a quote artifact, not a real price move.
- Next-trade test: a trade is a buy if its price is above the quote midpoint just
  before it, and a sell if it is below. Trades exactly at the midpoint are skipped.
  Only book states from before the trade are used (OFI over the previous second), so
  nothing from the future leaks in.
"""
from pathlib import Path
import sys

import numpy as np
import polars as pl
from scipy.stats import spearmanr, theilslopes

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
RESULTS = ROOT / "results"

SYMBOLS = ["SPY", "QQQ", "IWM", "EMB", "VCSH"]
WINDOWS = ["1s", "10s", "1m"]
SEED = 42                    # every random operation uses this seed
FIT_SAMPLE = 5000            # windows sampled for each Theil-Sen fit (it compares all pairs)
CLOSE_CUTOFF = (15, 59)      # drop 15:59:00 New York time onward
REGULAR_SECONDS = 23400      # 6.5 hours * 3600


#  loading
def load_processed():
    """Read the processed book states and trades. Raises a clear error if they are missing."""
    book_path = PROCESSED / "book_states.parquet"
    trades_path = PROCESSED / "trades.parquet"
    missing = [str(p) for p in (book_path, trades_path) if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Processed data not found: " + ", ".join(missing)
            + ". They are committed in the repo under data/processed/. "
            "If they are missing, run `python src/pipeline.py` (needs the raw file, "
            "see data/raw/README.md) or pull the repo again."
        )
    return pl.read_parquet(book_path), pl.read_parquet(trades_path)


def drop_closing_minute(df):
    """Keep only rows before 15:59 New York time. `df` needs a UTC `timestamp` column."""
    hour, minute = CLOSE_CUTOFF
    ny = pl.col("timestamp").dt.convert_time_zone("America/New_York")
    return df.filter((ny.dt.hour() < hour) | ((ny.dt.hour() == hour) & (ny.dt.minute() < minute)))


# OFI vs price change
def ofi_summary(book, symbol="SPY", clusters=(1080, 1500)):
    """Distribution facts for OFI per book update (regular session, before 15:59)."""
    s = drop_closing_minute(
        book.filter((pl.col("symbol") == symbol) & (pl.col("session") == "regular"))
    ).drop_nulls("ofi")
    o = s["ofi"].to_numpy()
    p1, p50, p99, p999 = np.percentile(o, [1, 50, 99, 99.9])
    out = dict(symbol=symbol, updates=len(o), share_zero=float((o == 0).mean()),
               p1=float(p1), p50=float(p50), p99=float(p99), p99_9=float(p999),
               min=float(o.min()), max=float(o.max()))
    for c in clusters:
        out[f"rows_at_abs_{c}"] = int((np.abs(o) == c).sum())
    return out


def make_windows(book, symbol, every, trim=True):
    """One row per window of the regular session: summed OFI and the change in the last mid.

    trim=True drops the closing minute (the default for every result in the report);
    trim=False keeps it, which the notebook uses to show the closing-minute artifact.
    """
    w = (book.filter((pl.col("symbol") == symbol) & (pl.col("session") == "regular"))
             .sort("timestamp")
             .group_by_dynamic("timestamp", every=every)
             .agg(pl.col("ofi").sum().alias("ofi_sum"), pl.col("mid").last().alias("mid_end"))
             .with_columns(pl.col("mid_end").diff().alias("dmid_w"))
             .drop_nulls(["ofi_sum", "dmid_w"]))
    return drop_closing_minute(w) if trim else w


def robust_fit(x, y, n=FIT_SAMPLE, seed=SEED):
    """Theil-Sen line through a random sample of n points (fixed seed -> same result every run)."""
    idx = np.random.default_rng(seed).choice(len(x), size=min(n, len(x)), replace=False)
    slope, intercept, lo, hi = theilslopes(y[idx], x[idx])
    return dict(slope=float(slope), intercept=float(intercept),
                slope_lo=float(lo), slope_hi=float(hi), n_fit=int(len(idx)))


def trim_effect(book, symbol="SPY", every="1s"):
    """Does dropping the closing minute change the result? Same fit with and without it."""
    rows = {}
    for label, trim in (("with_close", False), ("without_close", True)):
        w = make_windows(book, symbol, every, trim=trim)
        x, y = w["ofi_sum"].to_numpy(), w["dmid_w"].to_numpy()
        fit = robust_fit(x, y)
        rows[label] = dict(windows=len(x), spearman=float(spearmanr(x, y)[0]),
                           max_abs_dmid=float(np.abs(y).max()),
                           cents_per_1000_shares=fit["slope"] * 1000 * 100)
    return pl.DataFrame([dict(version=k, **v) for k, v in rows.items()])


def residual_summary(x, y, fit, bins=20):
    """Residuals of the robust line: percentiles, and the median residual in equal-count OFI bins."""
    resid = y - (fit["slope"] * x + fit["intercept"])
    pcts = np.percentile(resid, [1, 25, 50, 75, 99])
    pct_table = pl.DataFrame(dict(percentile=[1, 25, 50, 75, 99], residual=[float(v) for v in pcts]))
    edges = np.quantile(x, np.linspace(0, 1, bins + 1))
    b = np.digitize(x, edges[1:-1])
    ks = [k for k in range(bins) if (b == k).any()]
    bin_table = pl.DataFrame(dict(
        bin=ks,
        median_ofi=[float(np.median(x[b == k])) for k in ks],
        median_residual=[float(np.median(resid[b == k])) for k in ks],
    ))
    return resid, pct_table, bin_table


def window_table(book, symbols=SYMBOLS, windows=WINDOWS):
    """Rank correlation and robust slope for each symbol at each window length."""
    rows = []
    for sym in symbols:
        for every in windows:
            w = make_windows(book, sym, every)
            x, y = w["ofi_sum"].to_numpy(), w["dmid_w"].to_numpy()
            fit = robust_fit(x, y)
            rows.append(dict(
                symbol=sym, window=every, n_windows=len(x),
                spearman=round(float(spearmanr(x, y)[0]), 3),
                share_dmid_zero=round(float((y == 0).mean()), 3),
                cents_per_1000_shares=round(fit["slope"] * 1000 * 100, 4),
                lo=round(fit["slope_lo"] * 1000 * 100, 4),
                hi=round(fit["slope_hi"] * 1000 * 100, 4),
            ))
    return pl.DataFrame(rows)


# next-trade direction
def prepare_trades(book, trades, trim_close=False):
    """Build the pieces the next-trade tests share.

    b        book states with a running (cumulative) OFI per symbol, sorted by time
    b_start  the same running OFI, renamed so it can be looked up at a window start
    t        usable trades (regular session, round lot, not extended hours)

    trim_close=True also drops trades from the closing minute (15:59 onward). The default
    is False because the first version of the test did not do this; compare both.
    """
    b = (book.sort("timestamp")
             .with_columns(pl.col("ofi").fill_null(0).cum_sum().over("symbol").alias("cum_ofi"))
             .select("symbol", "timestamp", "cum_ofi", "mid"))
    b_start = b.select("symbol", pl.col("timestamp").alias("ts_b"), pl.col("cum_ofi").alias("cum_start"))
    t = (trades.filter(pl.col("usable_trade"))
               .select("symbol", "timestamp", "price")
               .sort("timestamp"))
    if trim_close:
        t = drop_closing_minute(t)
    return b, b_start, t


def label_trades(t, b, b_start, window_ms=1000):
    """One row per classified trade.

    dir        +1 buy (price above the midpoint just before the trade), -1 sell, 0 at the midpoint
    ofi_prior  OFI summed over the window_ms before the trade, using only states strictly before it
    """
    t2 = t.with_columns((pl.col("timestamp") - pl.duration(milliseconds=window_ms)).alias("t_start"))
    return (t2.join_asof(b, on="timestamp", by="symbol", strategy="backward", allow_exact_matches=False)
              .rename({"cum_ofi": "cum_end", "mid": "mid_before"})
              .join_asof(b_start, left_on="t_start", right_on="ts_b", by="symbol", strategy="backward")
              .with_columns((pl.col("cum_end") - pl.col("cum_start")).alias("ofi_prior"),
                            (pl.col("price") - pl.col("mid_before")).round(4).sign().alias("dir"))
              .drop_nulls(["ofi_prior", "dir"]))


def direction_table(t, d):
    """Hit rate of sign(OFI before the trade) vs trade direction, against the majority baseline."""
    n_usable = t.group_by("symbol").len().rename({"len": "n_usable"})
    sc = (d.filter(pl.col("dir") != 0)
            .with_columns((pl.col("ofi_prior").sign() == pl.col("dir")).alias("hit"))
            .group_by("symbol")
            .agg(pl.len().alias("n_classified"),
                 (pl.col("dir") > 0).mean().alias("share_buys"),
                 (pl.col("ofi_prior") == 0).mean().alias("share_ofi_zero"),
                 pl.col("hit").filter(pl.col("ofi_prior") != 0).mean().alias("hit_rate"),
                 pl.col("hit").filter(pl.col("ofi_prior") != 0).len().alias("n_scored")))
    return (n_usable.join(sc, on="symbol")
            .with_columns(
                pl.max_horizontal(pl.col("share_buys"), 1 - pl.col("share_buys")).alias("baseline"),
                (1.96 * (pl.col("hit_rate") * (1 - pl.col("hit_rate")) / pl.col("n_scored")).sqrt()).alias("margin_95"))
            .with_columns(pl.col(pl.Float64).round(3))
            .sort("symbol"))


def label_check(d, b):
    """Sanity check of the buy/sell labels: does the mid rise after buys and fall after sells?"""
    after = (d.select("symbol", "timestamp", "dir", "mid_before")
              .with_columns((pl.col("timestamp") + pl.duration(seconds=1)).alias("t_after"))
              .sort("t_after")
              .join_asof(b.select("symbol", pl.col("timestamp").alias("t_after"), pl.col("mid").alias("mid_after")),
                         on="t_after", by="symbol", strategy="backward")
              .with_columns((pl.col("mid_after") - pl.col("mid_before")).alias("dmid_after")))
    return (after.filter(pl.col("dir") != 0)
                 .group_by("symbol", "dir")
                 .agg(pl.col("dmid_after").mean().round(4).alias("avg_mid_change_next_1s"), pl.len())
                 .sort("symbol", "dir"))


def hit_by_window(t, b, b_start, lengths_ms=(1000, 100, 10, 1)):
    """Hit rate when the OFI window ending at the trade gets shorter (1 s down to 1 ms)."""
    out = None
    for ms in lengths_ms:
        d2 = (label_trades(t, b, b_start, window_ms=ms)
              .filter((pl.col("dir") != 0) & (pl.col("ofi_prior") != 0)))
        r = d2.group_by("symbol").agg(
            (pl.col("ofi_prior").sign() == pl.col("dir")).mean().round(3).alias(f"hit_{ms}ms"),
            pl.len().alias(f"n_{ms}ms"))
        out = r if out is None else out.join(r, on="symbol")
    return out.sort("symbol")


def hit_by_gap(d, b, gaps_ms=(10, 100, 500)):
    """Hit rate when the 1-second OFI window stops `gap` ms before the trade instead of at it.

    `d` must be the 1-second table from label_trades(..., window_ms=1000).
    """
    out = None
    for gap in gaps_ms:
        g = (d.select("symbol", "timestamp", "dir", "cum_start")
              .with_columns((pl.col("timestamp") - pl.duration(milliseconds=gap)).alias("t_end"))
              .sort("t_end")
              .join_asof(b.select("symbol", pl.col("timestamp").alias("t_end"), pl.col("cum_ofi").alias("cum_gap")),
                         on="t_end", by="symbol", strategy="backward")
              .with_columns((pl.col("cum_gap") - pl.col("cum_start")).alias("ofi_gap"))
              .filter((pl.col("dir") != 0) & (pl.col("ofi_gap") != 0)))
        r = g.group_by("symbol").agg(
            (pl.col("ofi_gap").sign() == pl.col("dir")).mean().round(3).alias(f"hit_gap{gap}ms"),
            pl.len().alias(f"n_gap{gap}ms"))
        out = r if out is None else out.join(r, on="symbol")
    return out.sort("symbol")


# quote activity and coverage
def quote_trade_table(book, trades):
    """Book states per executed trade, by symbol and session.

    Counts complete book states (flag = 1) and all trades, odd lots included, so the
    ratios differ a little from the raw quote-update ratios in the profiling step.
    """
    q = book.group_by("symbol", "session").agg(pl.len().alias("quotes"))
    tr = trades.group_by("symbol", "session").agg(pl.len().alias("trades"))
    return (q.join(tr, on=["symbol", "session"], how="left")
             .with_columns((pl.col("quotes") / pl.col("trades")).round(1).alias("quotes_per_trade"))
             .sort("symbol", "session"))


def activity_by_time(book):
    """Book states per 5 minutes across the day (New York time)."""
    ny = pl.col("timestamp").dt.convert_time_zone("America/New_York")
    return (book.with_columns((ny.dt.hour() + (ny.dt.minute() // 5) * 5 / 60).alias("hr"))
                .group_by("symbol", "hr").agg(pl.len().alias("n"))
                .sort("symbol", "hr"))


def coverage_tables(book):
    """Share of regular-session seconds with at least one book update: overall and by hour."""
    reg = (book.filter(pl.col("session") == "regular")
               .with_columns(pl.col("timestamp").dt.truncate("1s").alias("sec"),
                             pl.col("timestamp").dt.convert_time_zone("America/New_York").dt.hour().alias("hour")))
    overall = (reg.group_by("symbol").agg(pl.col("sec").n_unique().alias("secs"))
                  .with_columns((pl.col("secs") / REGULAR_SECONDS).round(3).alias("share_of_seconds"))
                  .sort("symbol"))
    # the first hour of the session (hour 9) is only 30 minutes long
    by_hour = (reg.group_by("symbol", "hour").agg(pl.col("sec").n_unique().alias("secs"))
                  .with_columns((pl.col("secs") / pl.when(pl.col("hour") == 9).then(1800).otherwise(3600)).alias("share"))
                  .sort("symbol", "hour"))
    return overall, by_hour


# run everything
def main(trim_close_trades=False):
    """Compute every table and save it to results/. Prints each one."""
    pl.Config.set_tbl_rows(40)
    pl.Config.set_tbl_cols(20)
    RESULTS.mkdir(exist_ok=True)
    book, trades = load_processed()

    def save(name, table):
        if not isinstance(table, pl.DataFrame):
            table = pl.DataFrame([table])
        table.write_csv(RESULTS / f"{name}.csv")
        print(f"\n=== {name} ===")
        print(table)

    # OFI vs price change
    save("spy_ofi_summary", ofi_summary(book, "SPY"))
    w = make_windows(book, "SPY", "1s")
    x, y = w["ofi_sum"].to_numpy(), w["dmid_w"].to_numpy()
    fit = robust_fit(x, y)
    save("spy_1s_fit", fit)
    save("spy_trim_effect", trim_effect(book))
    _, pct_table, bin_table = residual_summary(x, y, fit)
    save("spy_residual_percentiles", pct_table)
    save("spy_residual_bins", bin_table)
    save("window_table", window_table(book))

    # next-trade direction
    b, b_start, t = prepare_trades(book, trades, trim_close=trim_close_trades)
    d = label_trades(t, b, b_start, window_ms=1000)
    save("direction_table", direction_table(t, d))
    save("label_check", label_check(d, b))
    save("hit_by_window", hit_by_window(t, b, b_start))
    save("hit_by_gap", hit_by_gap(d, b))

    # quote activity and coverage
    save("quote_trade_table", quote_trade_table(book, trades))
    save("activity_by_time", activity_by_time(book))
    overall, by_hour = coverage_tables(book)
    save("coverage_overall", overall)
    save("coverage_by_hour", by_hour)
    print(f"\nSaved tables to {RESULTS}")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as err:
        print(f"ERROR: {err}")
        sys.exit(1)