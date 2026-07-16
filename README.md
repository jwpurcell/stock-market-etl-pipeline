# stock-market-etl-pipeline

A serverless ETL pipeline tracking daily stock market data for a set of equities (IBM, AAPL, MSFT), built on AWS SAM, Lambda, and EventBridge.

The pipeline fetches daily time series data from the Alpha Vantage API, flags significant price movements against a threshold, writes the enriched data to an S3 bucket partitioned Hive-style, supporting future querying and dashboard layer. 

Built as a portfolio project to demonstrate practical AWS serverless architecture and data engineering fundamentals.

**Status**: Tier 1 (core pipeline) complete and deployed.

## Architecture 

The pipeline runs automatically once daily via an EventBridge scheduled rule, which invokes the Lambda function to execute the following three stages:

**Extract**: fetches daily time series data for tracked equities (IBM, AAPL, MSFT) from the Alpha Vantage API, retrieving the API key securely from SSM Parameter Store.

**Transform**: Computes daily percentage change for the most recent trading day and flags moves exceeding a 3% threshold as significant, tagging the data in place. 

**Load**: Writes the enriched data to S3 using Hive-style partitioning (`symbol=/year=/month=/day=`), chosen to support future querying by symbol as the primary access pattern.

## Setup

```
# clone the repo 
git clone <repo_url>
cd stock-market-etl-pipeline

# Install Python dependencies (local testing)
python -m pip install -r fetch_stock_data/requirements.txt
python -m pip install boto3
python -m pip install -r tests/requirements.txt

# Configure AWS credentials 
aws configure 

# Store your Alpha Vantage API key securely in SSM Parameter Store
aws ssm put-parameter \
  --name "/alphavantage-api-key" \
  --value "YOUR_API_KEY_HERE" \
  --type "SecureString"

# Validate SAM template 
sam validate 

# Build the application  
sam build 

# Deploy (first time, guided)
sam deploy --guided 
```

```
# Set environment variables for local testing
export STOCK_SYMBOLS="IBM,AAPL,MSFT"
export BUCKET_NAME="stock-market-etl-data-<your-account-id>"
```


## Design

### Data/pipeline design

**3% significance threshold**

This threshold was chosen empirically rather than assumed. After deciding to tag (not filter) the daily data with a percentage change flag, I tested 2% and 3% cutoffs against real historical data for all three symbols tracked by default. At 2%, IBM alone flagged 35% of days, which was too broad to signal genuine volatility. At 3% all three symbols landed in a tighter more consistent 6-22% range, giving more meaningful "notable day" signalling across the board.

**Tag/annotate over filter**

Rather than filtering out the non-significant days, every fetched record is tagged in place with its computed percentage change and significance flag, preserving the full raw dataset. Filtering would permanently lose the ability to answer questions like "show me every day's price" or "recompute volatility with a different threshold" later, since discarded data can't be recovered retroactively. Tagging keeps that flexibility, future analysis, dashboards or Athena query can filter at query time however it needs. 

**Latest-day-only tagging, not full history**

Only the most recent day's record is tagged on each run, rather than recomputing percentage change across the full ~100-day history returned by Alpha Vantage each call. Historical days don't change, so retagging them daily would be a redundant computation with no new information gained. This does mean historical data sits untagged until a deliberate backfill step is run. A conscious tradeoff and a planned future improvement.


### Infrastructure/AWS design

**Hive-style partitioning, `symbol=` before `year=/month=/day=`**

Data is partitioned in Hive-style partitioning with `symbol=` as the first level, rather than leading with date. This was chosen as the pipeline's primary intended access pattern, tracking each stock's full history over time (e.g. "show me all of IBM's data") rather than cross-symbol daily snapshots. The ordering also sets up efficient prefix-based querying once Glue and Athena are added, without requiring any rework of existing data.

**US equities over ASX**

US equities (IBM, AAPL, MSFT) were chosen over ASX-listed stocks primarily for better Alpha Vantage API coverage and data reliability.

**SAM template parameter for tracking symbols, not hardcoded**

Tracked symbols are defined as a SAM parameter (`FetchRequestSymbols`), not hardcoded in `app.py`. This separates configuration from the application code, changing which stocks are tracked means updating a template and redeploying, rather than tinkering with application code and retesting pipeline logic.

**Environment variables sourced, rather than duplicated**

Runtime configuration (the S3 bucket name, tracked symbols) is passed to the Lambda via environment variables sourced directly from SAM template using `!Ref`, rather than duplicated as separate hardcoded values in `app.py`. This keeps a single source of truth, if the bucket name or symbol list ever changes, it only needs to change in one place, with no risk of the template and code being out of sync. 

### Error handling design 

**Per-symbol try/except, continue-on-failure loop**

Each symbol is fetched, transformed, and written inside its own `try`/`except` block, so one symbol's failure doesn't halt the entire run. This was validated for real during testing. One symbol can be rate-limited mid run and the others will still succeed and write to the bucket normally.

**Explicit validation for Alpha Vantage's "200 OK with error body" behaviour**

Alpha Vantage returns HTTP 200 even when rate-limited, with the actual error embedded in the JSON body instead of the status code, a failure mode standard `raise_for_status()` checks don't catch. This was discovered directly during testing, not from documentation, and required an explicit check for the expected `"Time Series (Daily)"` key, raising a proper exception when it's missing, so the failure surfaces through the same error-handling path as any other.

**Exceptions allowed to propagate with specific types**

`fetch_stock_data` doesn't catch its own exceptions, it lets specific types (`ConnectionError`, `HTTPError`, `JSONDecodeError`, etc.) propagate up to the caller rather than collapsing everything into a generic failure or a `None` return. This was a deliberate choice: a future retry wrapper needs to distinguish a transient network blip (worth retrying) from a bad API key, and that distinction is lost the moment errors get swallowed at the source

### Dashboard design 

**Streamlit over traditional BI platforms (Tableau, Power BI, Metabase)**

Considered a traditional BI platform for the final dashboard layer (Metabase specifically given its free, self-hosting option). BI platforms would model a more realistic hand off point of a queryable tool to non-technical stakeholders. Streamlit was chosen instead for this stage: it demonstrates full end-to-end technical ownership (building the interface, not just using a platform) and produces a freely hosted, instantly interactable link for a resume. The BI-handoff reasoning remains a deliberate tradeoff worth revisiting for future portfolio work.


### Monitoring design 

**CloudWatch alarm over DLQ**

A CloudWatch alarm was chosen over a Dead Letter Queue for monitoring failed runs. A DLQ only captures fully failed Lambda invocations, but the pipeline's per-symbol `try`/`except` already catches and logs individual failures (e.g. a rate-limited symbol) without crashing the whole run, so the most likely failure mode never reaches "total invocation failure". A CloudWatch alarm watching for `error`-level log entries covers both that partial-failure case and genuinely unexpected crashes, making it the broader, more useful choice given the pipeline's existing error handling. 

## Improvements / Roadmap

### In Progress: 

**Retry logic with exponential backoff of the Alpha Vantage call**

Alpha Vantage's free tier has aggressive rate limits. The current pipeline lets specific exception types propagate cleanly from `fetch_stock_data`, so a retry wrapper can distinguish between failures worth retrying from permanent ones (bad API key, invalid request) rather than retrying blindly.

**Data validation/schema check before writing to s3**

Currently, the pipeline validates for one specific failure mode (Alpha Vantage rate-limiting, detected via a missing `"Time Series (Daily)"` key). This will be extended into a general schema/validation check confirming the full shape and types of the fetched data (open/high/low/close/volume) before writing to S3, rather than validating only the top-level structure.

**Glue Crawler/Glue Data catalog + Athena workgroup for SQL queryability**

The raw JSON files in S3 are partitioned Hive-style specifically so they'd be automatically discoverable by a glue crawler without a rework. Once catalogued, Athena makes the full dataset SQL-queryable, feeding directly into the dashboard layer. 

### Planned: 

**Streamlit dashboard reading from Athena/S3 output**

An interactive dashboard hosted on Streamlit Community Cloud, reading from the pipeline's Athena/S3 output on a periodic refresh rather than live queries. This avoids public-facing credentials, and unbounded query costs as it's freely hosted.

**Cloudwatch alarm for failed runs**

An alarm watching for error-level entries in CloudWatch Logs, triggered by the pipeline's existing `logger.error()` calls. This surfaces partial failures (e.g. a rate-limited symbol) and any unhandled exceptions without needing to manually check logs after each run.
