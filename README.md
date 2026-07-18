# stock-market-etl-pipeline

A serverless ETL pipeline tracking daily stock market data for a set of equities (IBM, AAPL, MSFT), built on AWS SAM, Lambda, and EventBridge.

The pipeline fetches daily time series data from the Alpha Vantage API, flags significant price movements against a threshold, writes the enriched data to an S3 bucket partitioned Hive-style, supporting future querying and dashboard layer. 

Built as a portfolio project to demonstrate practical AWS serverless architecture and data engineering fundamentals.

**Status**: Tier 1 (core pipeline) and Tier 2 (resilience, transformation, queryability) complete and deployed. Tier 3 (dashboard, monitoring) planned, see Roadmap.

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

**One-file-per-day restructuring**

Originally, `write_to_s3` wrote the entire ~100-day Alpha Vantage response into a single file, meaning every day's partition redundantly contained all other day's data that didn't belong to it. This broke the correctness of the partition based querying (queries for a specific date would find that date duplicated across dozens of files, not clearly isolated in its own partition). `write to s3` and `apply_threshold` were changed so each S3 object holds exactly one day's record.

**Immediate backfill over deferred backfill**

The original roadmap deferred backfill as a future improvement, assuming raw historical data would already be sitting in the S3 bucket to reprocess later. The one-file-per-day restructuring removed that assumption (only the newest day is written, so a dashboard built on a few days of accumulated history wouldn't show anything meaningful). Backfill was moved from "deferred" to immediate: a one-time script (`scripts/backfill.py`, separate from lambda handler) fetches full history once per symbol and writes every day individually, giving the pipeline ~100 days of real, dashboard-ready history from day one rather than accumulating slowly. 

**Field-name cleaning in Transform, not Load**

Alpha Vantage's raw field names (`"1. open"`, `"2. high"`, etc.) are awkward, non-SQL-friendly identifiers. Rather than handling this in `write_to_s3`, renaming happens in `apply_threshold`, alongside the existing enrichment logic. This makes it a legitimate "Transform" step. `apply_threshold` builds a fresh, clean and enriched dict in its final, queryable form.

### Infrastructure/AWS design

**Hive-style partitioning, `symbol=` before `year=/month=/day=`**

Data is partitioned in Hive-style partitioning with `symbol=` as the first level, rather than leading with date. This was chosen as the pipeline's primary intended access pattern, tracking each stock's full history over time (e.g. "show me all of IBM's data") rather than cross-symbol daily snapshots. The ordering also sets up efficient prefix-based querying once Glue and Athena are added, without requiring any rework of existing data.

**US equities over ASX**

US equities (IBM, AAPL, MSFT) were chosen over ASX-listed stocks primarily for better Alpha Vantage API coverage and data reliability.

**SAM template parameter for tracking symbols, not hardcoded**

Tracked symbols are defined as a SAM parameter (`FetchRequestSymbols`), not hardcoded in `app.py`. This separates configuration from the application code, changing which stocks are tracked means updating a template and redeploying, rather than tinkering with application code and retesting pipeline logic.

**Environment variables sourced, rather than duplicated**

Runtime configuration (the S3 bucket name, tracked symbols) is passed to the Lambda via environment variables sourced directly from SAM template using `!Ref`, rather than duplicated as separate hardcoded values in `app.py`. This keeps a single source of truth, if the bucket name or symbol list ever changes, it only needs to change in one place, with no risk of the template and code being out of sync. 

**Explicit Glue Table over Crawler-managed schema**

The Crawler's auto-generated table correctly inferred types but preserved Alpha Vantage's raw, awkward column names. An explicit `AWS::Glue::Table` resource was added instead, with clean column names mapped to the raw JSON keys via the SerDe's `paths` parameter. The Crawler's `SchemaChangePolicy` was set to `LOG` (not `UPDATE_IN_DATABASE`), so it continues discovering new daily partitions automatically without ever overwriting the explicit schema on a future recrawl.

**Glue Crawler over partition projection**

Partition projection (calculating valid partitions mathematically) is more current AWS-recommended pattern for a predictably-growing daily partition scheme like this one. It was considered but deferred in favour of a Crawler, given the added CloudFormation complexity and risk of a subtle, hard to debug misconfiguration. The Crawler is scheduled daily, an hour after the pipeline's own run, to pick up each new partition.

**Athena Workgroup**

Unlike Lambda, S3, and SSM Parameter Store, AWS Glue Crawlers and Athena queries are not covered by an always-free tier. They're pay-as-you-go from the first run. At this project's data volume, the actual cost is a negligible fraction of a cent, but it's acknowledged to differ from the otherwise free-tier architecture.

### Error handling design 

**Per-symbol try/except, continue-on-failure loop**

Each symbol is fetched, transformed, and written inside its own `try`/`except` block, so one symbol's failure doesn't halt the entire run. This was validated for real during testing. One symbol can be rate-limited mid run and the others will still succeed and write to the bucket normally.

**Explicit validation for Alpha Vantage's "200 OK with error body" behaviour**

Alpha Vantage returns HTTP 200 even when rate-limited, with the actual error embedded in the JSON body instead of the status code, a failure mode standard `raise_for_status()` checks don't catch. This was discovered directly during testing, not from documentation, and required an explicit check for the expected `"Time Series (Daily)"` key, raising a proper exception when it's missing, so the failure surfaces through the same error-handling path as any other.

**Exceptions allowed to propagate with specific types**

`fetch_stock_data` doesn't catch its own exceptions, it lets specific types (`ConnectionError`, `HTTPError`, `JSONDecodeError`, etc.) propagate up to the caller rather than collapsing everything into a generic failure or a `None` return. This was a deliberate choice: a future retry wrapper needs to distinguish a transient network blip (worth retrying) from a bad API key, and that distinction is lost the moment errors get swallowed at the source

**Retry logic with exponential backoff**

Alpha Vantage's rate-limiting had daily quotas that can be exhausted mid build. `fetch_stock_data` already kept specific exception types (`ConnectionError`, `Timeout`, `HTTPError`, and a custom rate-limit `RequestException`) propagate cleanly rather than swallowing them, specifically so a wrapper could react intelligently rather than retrying blindly. `fetch_with_retry` retries only `ConnectionError`/`Timeout` (transient network failures) up to twice, with an exponential backoff (2s, then 4s). `HTTPError` and the rate-limit exceptions propagate immediately, unretried. 

**Field validation before writing to S3**

The existing rate-limit check only validated that the response contained `"Time Series (Daily)"` key. `fetch_stock_data` now also confirms that the most recent day's record contains all five required fields (open/high/low/close/volume) and that each value is genuinely numeric, raising a descriptive `RequestException` if not. Scoped to the most recent day only, consistent with the pipeline's latest-day-only philosophy elsewhere; full historical validation was addressed directly by the backfill script re-fetching and reprocessing every day from source, rather than needing separate retroactive validation logic.

**Production bug: dotted JSON keys and Glue SerDe**

After deploying the Glue table with clean column names, Athena queries returned populated values for `pct_change`/`significant_move` but empty values for every raw field (`open`, `high`, `low`, `close`, `volume`). The root cause: Alpha Vantage's raw keys contain literal periods (`"1. open"`), which the JSON SerDe's `paths` mapping appears to interpret as a nested-field separator rather than a literal key, silently failing the lookup. Rather than working around the SerDe's handling of dotted keys, the fix was to stop writing dotted keys to S3 at all, cleaning field names during Transform.

### Dashboard design 

**Streamlit over traditional BI platforms (Tableau, Power BI, Metabase)**

Considered a traditional BI platform for the final dashboard layer (Metabase specifically given its free, self-hosting option). BI platforms would model a more realistic hand off point of a queryable tool to non-technical stakeholders. Streamlit was chosen instead for this stage: it demonstrates full end-to-end technical ownership (building the interface, not just using a platform) and produces a freely hosted, instantly interactable link for a resume. The BI-handoff reasoning remains a deliberate tradeoff worth revisiting for future portfolio work.


### Monitoring design 

**CloudWatch alarm over DLQ**

The first production deploy silently failed for two days: `sam deploy --guided` hadn't correctly bundled the `requests` dependency into the deployment package, so every scheduled invocation failed with `Runtime.ImportModuleError` before any application code ran. Nothing surfaced automatically (it was only caught by manually checking S3). Fixed by running `sam build` explicitly before deployment, rather than relying on the guided deploy to handle it. This incident is the concrete motivation behind the alarm below.

A CloudWatch alarm was chosen over a Dead Letter Queue for monitoring failed runs. A DLQ only captures fully failed Lambda invocations, but the pipeline's per-symbol `try`/`except` already catches and logs individual failures (e.g. a rate-limited symbol) without crashing the whole run, so the most likely failure mode never reaches "total invocation failure". A CloudWatch alarm watching for `error`-level log entries covers both that partial-failure case and genuinely unexpected crashes, making it the broader, more useful choice given the pipeline's existing error handling. 

## Improvements / Roadmap

### Planned: 

**Streamlit dashboard reading from Athena/S3 output**

An interactive dashboard hosted on Streamlit Community Cloud, reading from the pipeline's Athena/S3 output on a periodic refresh rather than live queries. This avoids public-facing credentials, and unbounded query costs as it's freely hosted.

**Cloudwatch alarm for failed runs**

An alarm watching for error-level entries in CloudWatch Logs, triggered by the pipeline's existing `logger.error()` calls. This surfaces partial failures (e.g. a rate-limited symbol) and any unhandled exceptions without needing to manually check logs after each run.
