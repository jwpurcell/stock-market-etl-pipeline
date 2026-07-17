"""One-time backfill script

Fetches the full ~100 day history for each tracked symbol and writes
everyday as its own individually-partitioned S3 object, tagged with pct_change / significant_move
unlike the daily pipeline, which only tags and writes the most recent day.

Run manually, once, from a local machine with valid AWS credentials
and STOCK_SYMBOLS / BUCKET_NAME set. Not part of the deployed Lambda
"""
import sys
import os

# allow importing from fetch_stock_data/, since this script lives in script/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetch_stock_data"))

from app import fetch_stock_data, apply_threshold, write_to_s3, symbols_clean

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def backfill(symbol):
    """Backfills full history for a single symbol.

    Fetches once, then loops through every date in the response,
    tagging and writing each day individually.
    """
    data = fetch_stock_data(symbol)

    # loop over every date key in the time series, not just the most recent.
    for date in data["Time Series (Daily)"]:
        # tag this specific date (not the default "most recent" behaviour).
        tagged_data, target_date = apply_threshold(data, target_date=date)
        day_data = tagged_data["Time Series (Daily)"][target_date]

        s3_key = write_to_s3(symbol, target_date, day_data)
        logger.info(f"Backfilled {symbol} {target_date} -> {s3_key}")

if __name__ == "__main__":
    # run backfill for every symbol in the tracked list.
    for symbol in symbols_clean:
        backfill(symbol)