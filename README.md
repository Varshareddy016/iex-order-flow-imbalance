# Order-Flow Imbalance Analysis on IEX Market Data

EAS 587 — Data Intensive Computing, Fall 2026

Team: Anvesh Sadam, Varshitha Gangadi, Hanu Varma Pinamaraju

## Overview

This project investigates whether short-term order-book imbalance (the
relative volume of resting buy-side vs. sell-side orders) predicts the
direction of the next executed trade on IEX Exchange, and whether that
relationship differs between high-activity and low-activity symbols.

Phase 2 uses one trading day (Sep 11, 2026) and five ETFs: SPY, QQQ and IWM
(high activity) and VCSH and EMB (low activity).

Full research plan: [`research_plan.md`](./research_plan.md)
Phase 2 report: [`phase2_report.md`](./phase2_report.md)

## Data

Primary dataset: IEX Historical DEEP data (full order-book depth feed).

- Source: https://iextrading.com/trading/market-data/#hist-download
- A single trading day (Sep 11, 2026) is 11 GB compressed, a raw binary
  packet capture (.pcap.gz) with 403,398,808 messages across ~8,000+ symbols.
- Measured in-memory footprint: ~453.5 bytes per message as Python objects,
  so the full day would need ~183 GB, far beyond a laptop. Parsing took
  ~10.2 hours (~11,000 messages/s) and kept 7.87M rows for the 5 symbols.
- Not committed to this repo due to size. See `data/raw/README.md` for
  download instructions. A small sample is in `data/samples/`.
- Download: <paste your Phase 1 download command here>. The pipeline expects
  the file at `data/raw/deep_20260911.pcap.gz`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Pipeline (Phase 2)

Single entry point. Run from the repo root:

```bash
python src/pipeline.py              # parse (skipped if parts exist) -> profile -> clean (~20 s)
python src/pipeline.py --reparse    # re-parse the raw pcap (~10 hours)
```

Stages: acquire/parse -> profile -> clean/transform -> processed data.

First analytics (needs only `data/processed/`, ~30 s):

```bash
python src/analytics.py             # writes results/*.csv
```

- `results/ofi_vs_dmid_by_window.csv`: rank correlation and Theil-Sen slope (seed 42, 5,000 windows) per symbol and window (1 s, 10 s, 1 min)
- `results/next_trade_hit_rate.csv`: next-trade side vs prior 1 s OFI, with the majority baseline
- `results/hit_rate_by_window_and_gap.csv`: hit rate as the window shrinks, and with a gap before the trade

Then open `notebooks/phase2_analysis.ipynb`. It loads `data/processed/` and
holds the EDA, first analytics and interpretation.

Code layout (`src/` builds the data, the notebook explores it):

- `src/data_access.py`: streams the pcap into Parquet parts (5 symbols, quotes and trades)
- `src/profile_data.py`: read-only profiling queries
- `src/data_cleaning.py`: duplicate removal, session labels, usable-trade flag, order book rebuild, OFI
- `src/pipeline.py`: single entry point that runs all of the above
- `src/analytics.py`: first analytics

Outputs in `data/processed/`:

- `book_states.parquet`: best bid/ask, OFI, mid-price, spread and session for every recorded book state
- `trades.parquet`: all trade reports with session and a `usable_trade` flag

Random seeds: 42 for the 5,000-window sample behind the Theil-Sen fit (notebook and `src/analytics.py`).

## Project Status

- [x] Phase 1: Dataset identified, verified, and research plan complete
- [ ] Phase 2: Data cleaning and exploratory analysis at scale (in progress)
- [ ] Phase 3: Distributed processing and modeling with Spark/Databricks

## Repository Structure

```
research_plan.md    — Phase 1 research plan
phase2_report.md    — Phase 2 report
data/
  samples/     — small representative samples
  raw/         — raw pcap (not committed)
  interim/     — parsed Parquet parts (not committed, rebuilt by the pipeline)
  processed/   — cleaned book states and trades (committed, 45 MB)
results/       — analytics tables (CSV), rebuilt by src/analytics.py
src/           — pipeline source code
notebooks/     — phase2_analysis.ipynb
```