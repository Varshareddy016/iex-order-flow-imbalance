# From Raw Packets to Price Signals: A Scalable Pipeline for Order-Flow Imbalance Analysis on IEX Market Data

**Created By:** Anvesh Sadam, Varshitha Gangadi, Hanu Varma Pinamaraju <br>
**Version:** 1.0 <br>
**Target Community of Interest:** Quant researchers/traders, market makers, and market-microstructure academics interested in whether public order-book data predicts short-term price movement<br>
**Date Created / Last Updated:** 2026-09-16<br>
**GitHub Repository:** <https://github.com/find-me1/iex-order-flow-imbalance>

---

## Section 1: Research Goal

Most order-book activity never actually turns into a trade, in a sample of our own data, we found roughly 157 quote updates for every 1 executed trade, which raises a real question: does all that quote activity actually carry useful information about where price is about to move, or is most of it just noise? This project investigates whether short-term order-book imbalance (the relative volume of resting buy-side vs. sell-side orders) has measurable predictive power for the direction of the next executed trade on IEX Exchange, and whether that relationship differs across symbols with different liquidity profiles. Using a data-intensive pipeline built to parse, reconstruct, and analyze IEX's raw binary market-data captures (a single trading day of which exceeds 10GB and is impractical to process in full on a single machine), we will reconstruct order-book state over time for a representative set of symbols, compute flow-imbalance features, and statistically test their relationship to subsequent trade direction. By the end, we expect a quantified, evidence-based answer to whether, and how strongly, order-flow imbalance predicts short-term price direction on IEX, along with a reusable, scalable pipeline for order-book reconstruction that can be extended in later phases using Spark and lakehouse-style processing.

**Primary Research Question:**

Does short-term order-book imbalance predict the direction of the next executed trade on IEX, and does the strength of this relationship vary across symbols with different liquidity profiles (e.g., high-volume ETFs vs. thinly-traded equities)?

**Supporting Questions:**

1. How does the ratio of quote activity to actual trade executions vary across symbols and time-of-day (pre-market vs. regular hours), and what does that reveal about real vs. noise liquidity?
2. At what time horizon (1 second, 10 seconds, 1 minute) is any predictive relationship strongest, and does it decay in a predictable way?

## Section 2: Background and Motivation

IEX Exchange has a well-known origin story, it was built specifically to counter high-frequency trading firms that were exploiting tiny speed advantages to react to market signals microseconds before everyone else, a practice made famous by Michael Lewis's book Flash Boys. One of the clearest signals worth reacting to quickly is order-book imbalance, since a lopsided book, more buyers than sellers, or the reverse, tends to come right before a price move. That's exactly what made speed so valuable in the first place. It's also exactly why an exchange built to reduce speed advantages is such an interesting place to test whether this signal still holds.

This isn't a new idea in finance research, either. Cont, Kukanov, and Stoikov's widely-cited 2014 study, The Price Impact of Order Book Events, found that over short time intervals, price changes are mainly driven by order-flow imbalance, using NYSE data from 50 large-cap U.S. stocks, part of a broader line of market microstructure research looking at how the mechanics of trading, not just company fundamentals, drive short-term price movements. This has real practical stakes too: market makers use signals like this to manage the risk of trading against better-informed order flow, and quant trading firms use imbalance as a basic building-block signal in short-horizon strategies.

But this research, and most of what followed it, focused on large, highly liquid stocks on major exchanges like NYSE. That raises two real questions for us: does the same relationship hold on IEX, an exchange deliberately built to change trading dynamics, more than a decade later? And does the strength of that relationship depend on how liquid a stock actually is, comparing heavily-traded ETFs against stocks that barely trade at all? The original theory suggests it should, since the relationship's strength was tied to how deep the market is, but nobody's directly tested that as a controlled comparison in one pipeline.

This project takes on both questions directly. By building a pipeline that can parse IEX's full order-book depth feed at scale, reconstruct the order book, and test the imbalance-price relationship across a deliberate mix of high-volume ETFs and thinly-traded stocks, we're both replicating a foundational market microstructure finding on new data and extending it into a comparison nobody's really made before. Beyond the answer itself, we end up with a reusable pipeline for reconstructing order books from raw exchange data, a genuine data-intensive computing challenge given the scale and the messy, stateful nature of the data.

## Section 3: Research Objectives and Scope

**Objectives**

1. Build a working pipeline to download, parse, and decode IEX DEEP raw binary market data into structured, analyzable order-book events. Expected outcome: a reproducible parsing pipeline capable of processing a full trading day (10GB+) into structured records.
2. Reconstruct order-book state over time for a representative set of symbols (a mix of high-volume ETFs and thinly-traded equities), correctly handling known data-quality issues (test symbols, zero-size deletions, session boundaries). Expected outcome: clean, validated order-book snapshots suitable for feature computation.
3. Compute order-flow imbalance metrics at multiple time horizons and quantify the quote-to-trade ratio across symbols. Expected outcome: a feature set describing imbalance and liquidity characteristics per symbol.
4. Train and evaluate a machine learning classification model (logistic regression) predicting the direction of the next trade from order-flow imbalance features, comparing performance between high-volume ETFs and thinly-traded equities. Expected outcome: quantified evidence on whether, and how strongly, imbalance predicts direction, and whether the relationship differs by liquidity profile.
5. Scale the pipeline using distributed processing (Apache Spark) to process multiple days and a broader symbol set. Expected outcome: a pipeline that operates beyond a single machine's practical memory limits.

**In Scope**

- Parsing raw IEX DEEP data for a defined date range and symbol subset
- Cleaning and handling known data-quality issues (test symbols, zero-size deletions, session segmentation)
- Order-book reconstruction and imbalance feature computation
- Statistical testing of the imbalance vs. next-trade-direction relationship
- Comparison across liquidity tiers (high-volume ETFs vs. thinly-traded equities)
- Building a scalable, distributed version of the pipeline in Phase 3 using Spark

**Out of Scope**

- Actual live trading or execution of any strategy based on project findings
- Individual order-level data (DEEP provides aggregated size per price level only, not per-order detail)
- Multi-exchange comparison (this project is scoped to IEX only, not a direct NYSE/Nasdaq comparison)
- Predicting the magnitude of a price move (the project focuses on direction, not size, of the next move)
- Incorporating other asset classes or alternative data sources (e.g., news sentiment, insider-trading data) beyond what is noted as possible future enrichment
- Establishing causal mechanisms behind the imbalance-price relationship (the project tests predictive/statistical association, not causation)
- Building a real-time or live market-data processing system (this project analyzes historical DEEP data, made available T+1, not a live feed)

## Section 4: Prior Research and References

1. Cont, R., Kukanov, A., and Stoikov, S. (2014). The Price Impact of Order Book Events. Journal of Financial Econometrics, 12(1), 47-88. Studied the price impact of limit orders, market orders, and cancellations using NYSE Trade and Quote data for 50 large-cap U.S. stocks, finding a linear relationship between order-flow imbalance and short-term price changes, with a slope inversely proportional to market depth. This project tests whether the same relationship holds on a different, newer exchange (IEX) and adds an explicit, controlled comparison across liquidity tiers that was not the original study's central focus.
2. Gould, M. D., and Bonart, J. (2015/2016). Queue Imbalance as a One-Tick-Ahead Price Predictor in a Limit Order Book. Market Microstructure and Liquidity. Used logistic regression to test whether bid/ask queue imbalance predicts the direction of the next mid-price movement, using Nasdaq data for 10 liquid stocks, finding a strong, statistically significant relationship, especially for large-tick stocks. This project adopts a similar logistic-regression-based prediction approach, applied to IEX data, and extends the comparison to include genuinely thinly-traded equities alongside high-volume ETFs.
3. Zaharia, M. et al. (2016). Apache Spark: A Unified Engine for Big Data Processing. Communications of the ACM, 59(11), 56-65. Describes the Spark architecture and its unified approach to large-scale batch, streaming, and machine-learning workloads across a cluster of machines. This project uses Spark and its MLlib library in Phase 3 to scale order-book reconstruction and modeling beyond what a single machine can process, applying this architecture to a genuinely large, messy, real-world dataset.
4. IEX Group. IEX Historical Data and DEEP/TOPS Specification (ongoing). iextrading.com. The primary technical documentation describing the structure and semantics of the raw DEEP and TOPS data feeds used in this project, including message types, symbol conventions, and known test symbols. This project relies directly on this specification to correctly parse and interpret the raw data, and treats deviations from expected behavior, such as encountering IEX's own test symbols in the live feed, as a real data-quality issue to be handled explicitly.
5. Lewis, M. (2014). Flash Boys: A Wall Street Revolt. W. W. Norton & Company. A journalistic account of the founding of IEX, built specifically to counteract latency-based advantages exploited by high-frequency traders on other exchanges. This project uses this context to motivate why IEX is a meaningful and distinct venue to study, relative to the large exchanges such as NYSE used in prior academic order-book research.
6. Fama, E. F. (1970). Efficient Capital Markets: A Review of Theory and Empirical Work. The Journal of Finance, 25(2), 383-417. Established the Efficient Market Hypothesis, arguing that asset prices fully reflect all available public information. This project's core question, whether public order-book data still predicts price movement, is a direct, small-scale test of this theory's boundaries.
7. Tukey, J. W. (1977). Exploratory Data Analysis. Addison-Wesley. Established the foundational principles of exploratory data analysis this project follows in Phase 2 (distributions, outliers, sanity checks) before any formal modeling begins.

This list is expected to grow as Phase 2 and Phase 3 progress, particularly with additional references on statistical testing methodology and distributed order-book reconstruction techniques.

## Section 5: Supporting Data and Resources

**Primary Dataset: IEX Historical DEEP Data**

- Source: iextrading.com/trading/market-data (files served from a public Google Cloud Storage bucket at storage.googleapis.com/iex/data/feeds/)
- Intended use: primary big dataset for order-book reconstruction, imbalance feature computation, and price-direction prediction
- Access constraints: free, no registration required beyond accepting IEX's Historical Data Terms of Use; data is available T+1 (the previous trading day's file becomes available the next day); full DEEP history is available back to May 15, 2017. One thing we haven't fully nailed down yet is whether these terms allow us to share small data samples publicly in our GitHub repo, not just download them for our own use, so for now we're only including small, illustrative samples rather than full daily files, until that's confirmed.
- Verified scale: a single trading day (September 11, 2026) was directly downloaded and confirmed at 11.6 GB, covering roughly 8,000+ symbols and tens of millions of messages
- Format: raw binary packet captures (.pcap.gz) using IEX's proprietary transport protocol (IEX-TP), requiring specialized parsing rather than a standard tabular load

**In-Memory Footprint Estimate**

To directly test whether this dataset can be processed in memory on a single machine, we measured actual Python memory usage while parsing a real sample of the file. Parsing 100,000 messages from the DEEP feed consumed 43.24 MB of traced memory, or approximately 453.5 bytes per message once loaded as Python objects (each message becomes a dictionary carrying fields such as type, symbol, side, price, size, and timestamp, plus Python object overhead).

We also directly verified, through continuous processing of the actual file, that a single day's DEEP feed contains at least 15.5 million messages, confirmed after approximately 27 minutes of parsing at a measured throughput of about 9,700 messages per second, with processing still ongoing at that checkpoint. This is a documented lower bound, not the true full-day total, which is expected to be meaningfully higher.

Using this verified lower bound: 15,500,000 messages x 453.5 bytes per message is approximately 7.03 billion bytes, or approximately 6.5 GB, just to hold this partial sample's messages as Python objects in memory, before accounting for order-book reconstruction state, derived features, or the operating system and other running applications. Since the true full-day message count is expected to exceed this lower bound substantially, the actual in-memory footprint is expected to exceed the 8-16 GB of RAM typical of a team laptop, confirming that this dataset cannot be comfortably loaded and processed in memory in full on a single machine, consistent with the course's big-data requirement.

**Software and Tools**

- iex_parser (Python library) — decodes raw IEX pcap files into structured messages (DEEP_1_0 format)
- Python and the standard data science stack (pandas) — local cleaning and exploratory analysis in Phase 2
- Apache Spark and MLlib — distributed processing and predictive modeling in Phase 3
- Databricks Community Edition — the course's designated Spark platform
- GitHub — version control and the required project submission repository
- Direct HTTP download (curl) from the Google Cloud Storage bucket — used after the commonly referenced IEXTools downloader library was found to call a retired IEX API endpoint during Phase 1 access verification

**Computing Resources**

- Team laptops — initial parsing, cleaning, and small-scale exploratory analysis on sampled subsets
- Databricks Community Edition cluster — full-scale distributed processing in Phase 3
- Storage: every extra day of DEEP data adds roughly 10-12 GB, so local storage is naturally capped by how much disk space is available on our laptops. The local sampling strategy in Section 6 reflects that limit; anything beyond a small multi-day, multi-symbol set moves to Databricks in Phase 3, where storage isn't a constraint in the same way.

**Access Verification Already Performed**

During Phase 1, a full day's DEEP file (11.6 GB) was directly downloaded and parsed to confirm real accessibility. This surfaced the retired-API issue noted above (resolved via direct download), real message structure and schema, known data-quality issues (IEX's own test symbols ZIEXT, ZEXIT, and ZXIET appearing in the live feed; zero-size updates representing price-level deletions rather than errors), and a measured single-threaded parsing throughput of approximately 9,500 messages per second, motivating the need for distributed processing at full scale.

**Considered and Not Used**

The team considered supplementing this dataset with insider-trading disclosure data (e.g., openinsider.com, based on SEC Form 4 filings). This was not adopted: insider disclosures operate on a days-to-months timescale, incompatible with this project's second-to-minute prediction horizon, and do not apply to ETFs, which make up half of the project's liquidity-tier comparison.

## Section 6: Risks, Constraints, Assumptions, and Open Questions

**Risks**

1. Single-threaded parsing throughput was measured at approximately 9,500 messages per second, which makes processing multiple full days locally impractical before Phase 3's distributed infrastructure is in place. Impact: could delay the Phase 2 exploratory analysis timeline. Mitigation: Phase 2 will focus on a small, deliberately chosen symbol and day subset; full multi-day, multi-symbol processing is deferred to Phase 3 with Spark.
2. IEX represents a smaller share of total U.S. equity trading volume than NYSE or Nasdaq. Impact: findings may not generalize to the broader market, and the imbalance signal could differ on a smaller venue simply due to lower participation rather than a real difference in the underlying effect. Mitigation: results will be explicitly framed as IEX-specific findings, with this scope limitation stated directly in the final report.
3. IEX's DEEP feed shows only aggregated size at each price level, not individual orders. Impact: order-level behavior, such as individual order count or intent, cannot be analyzed, only the net picture at each price and side. Mitigation: the research question is scoped to what aggregated depth data can support, avoiding claims that would require order-level granularity.
4. Constructing prediction labels and imbalance features from time-series data creates a genuine risk of look-ahead bias, accidentally using information from after the prediction point when building features. Impact: could produce artificially inflated predictive performance that wouldn't hold up in a real setting. Mitigation: features will be computed using only data available strictly before each prediction point, with this boundary explicitly checked during Phase 2 validation.
5. Testing across multiple symbols and multiple candidate time horizons increases the risk of finding a significant relationship purely by chance. Impact: could lead to an overstated or spurious result. Mitigation: findings will be validated across a held-out set of days and symbols before being treated as real evidence, rather than relying on a single in-sample test.

**Constraints**

- Reliance on free-tier compute resources (team laptops and Databricks Community Edition) rather than paid cloud clusters, which caps how much data can realistically be processed at once and shapes the sampling strategy
- IEX's DEEP feed is only available from May 15, 2017 onward, bounding the historical depth of any longer-run analysis

**Assumptions**

- That the order-flow imbalance to price-direction relationship documented on NYSE (Cont et al., 2014) and Nasdaq (Gould and Bonart, 2015) will show at least a directionally similar pattern on IEX. This is the project's core hypothesis, so a negative result here is a valid finding, not a failure.
- That a representative sample of days and symbols will be sufficient to detect the relationship, if it exists, without requiring the full multi-year archive.
- That Databricks Community Edition's free-tier resource limits will be sufficient for the Phase 3 workload; this has not yet been tested at that scale.

**Open Questions**

- The exact set of symbols to use for the thinly-traded comparison group, which needs an objective, defensible liquidity threshold rather than an ad hoc choice
- The precise time horizon or horizons to test for prediction (for example, 1 second, 10 seconds, or 1 minute), to be determined empirically in Phase 2
- Whether Databricks Community Edition's compute limits are sufficient for the full Phase 3 scale, or whether further subsetting will be required
- How to label a trade where the price doesn't move at all (a tie), whether to count it as up, down, or exclude it from training entirely

## Section 7: Research Approach, Tasks, and Timeline

**Weeks 1-4 (detailed)**

**Week 1 (Sept 8-14): Getting started.** The course kicked off and the team came together, with lectures covering the fundamentals of big data.

**Week 2 (Sept 15-21): Finding and verifying the dataset.** We researched candidate datasets, then actually found, downloaded, and parsed IEX's DEEP market data ourselves. We confirmed its real scale (11.6 GB for a single day) and schema firsthand, and used all of it to write and submit the Phase 1 research plan and present it at the workshop on September 17.

**Week 3 (Sept 22-28): Building the foundation.** We'll set up the GitHub repository properly, turn our early parsing scripts into a clean, reusable module, and settle on an objective way to split symbols into high-volume and thinly-traded groups.

**Week 4 (Sept 29-Oct 5): First real cleaning and reconstruction.** We'll filter out IEX's own fake test symbols, correctly handle zero-size updates (which mean a price level was deleted, not that nothing happened), split the data by trading session, and reconstruct the order book end-to-end for a small test set of symbols and days, just to prove the whole pipeline works before scaling up.

**Weeks 5-8 (higher level)**

- Scale order-book reconstruction to the full initial symbol set (mix of high-volume ETFs and thinly-traded equities) across several days
- Compute order-flow imbalance features at multiple candidate time horizons
- Perform exploratory data analysis guided by Tukey's principles (distributions, outliers, sanity checks)
- Run initial statistical tests (logistic regression) of imbalance against next-trade direction on the local, sampled subset
- Check imbalance features for look-ahead bias, confirming each one only uses data available strictly before its prediction point
- Deliverable: first scalable analytics results, presented at the Phase 2 data-analysis presentation (October 14-16)

**Weeks 9-12 (higher level)**

- Port the pipeline to Apache Spark on Databricks Community Edition; scale to a broader symbol set and additional days
- Retrain and validate the predictive model at scale using Spark MLlib
- Compare results across liquidity tiers and refine the analysis (for example, testing how the signal decays across time horizons)
- Validate the trained model against a held-out set of days and symbols not used in training, guarding against a result that only looks significant by chance
- Package findings and prepare the final presentation
- Deliverable: final presentation, week of December 7

**Dependencies**

The Phase 2 presentation depends on completing cleaning and initial reconstruction (weeks 1-4). The final presentation depends on successfully porting the pipeline to Spark (weeks 9-10) before refinement and packaging can begin.
