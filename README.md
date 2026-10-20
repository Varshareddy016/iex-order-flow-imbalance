# Order-Flow Imbalance Analysis on IEX Market Data

EAS 587 — Data Intensive Computing, Fall 2026

Team: Anvesh Sadam, Varshitha Gangadi, Hanu Varma Pinamaraju

## Overview

This project investigates whether short-term order-book imbalance (the
relative volume of resting buy-side vs. sell-side orders) predicts the
direction of the next executed trade on IEX Exchange, and whether that
relationship differs between high-volume ETFs and thinly-traded equities.

Full research plan: [`research_plan.md`](./research_plan.md)

## Data

Primary dataset: IEX Historical DEEP data (full order-book depth feed).

- Source: https://iextrading.com/trading/market-data/#hist-download
- A single trading day is ~11.6 GB, raw binary packet capture (.pcap.gz),
  covering ~8,000+ symbols and tens of millions of messages.
- Verified in-memory footprint: ~453.5 bytes per message once parsed into
  Python objects; a verified lower bound of 15.5M+ messages for a single
  day already implies ~6.5 GB in memory, before order-book reconstruction
  state or features — see `research_plan.md` Section 5 for the full
  calculation.
- Not committed to this repo due to size. See `data/raw/README.md` for
  download instructions. Representative samples are in `data/samples/`.

## Setup

```bash
pip install -r requirements.txt
```

## Pipeline

- `src/data_access.py` — download and parse raw DEEP files
- `src/data_sampling.py` — export a readable sample to CSV
- `src/eda.py` — message-type counts and test-symbol detection

## Project Status

- [x] Phase 1: Dataset identified, verified, and research plan complete
- [ ] Phase 2: Data cleaning and exploratory analysis at scale
- [ ] Phase 3: Distributed processing and modeling with Spark/Databricks

## Repository Structure

```
research_plan.md — full Phase 1 research plan
data/
  samples/    — small representative data samples
  raw/        — download instructions for the full dataset (not committed)
  processed/  — cleaned/processed output (Phase 2+)
src/          — pipeline source code
```
