import os
import logging
import boto3
import time
import json
import requests


# Converts symbols from list to string
symbols_raw = os.environ["STOCK_SYMBOLS"]
symbols_clean = symbols_raw.split(",")

data_bucket = os.environ["BUCKET_NAME"]

# Fetch key parameter from SSM client
ssm_client = boto3.client('ssm')
response = ssm_client.get_parameter(
    Name="/alphavantage-api-key",
    WithDecryption=True
)
api_key = response["Parameter"]["Value"]

s3_client = boto3.client("s3")


logger = logging.getLogger(__name__)


def lambda_handler(event, context):
   
    results = {}
   
    for symbol in symbols_clean:
        try:
            data = fetch_with_retry(symbol)

            data, target_date = apply_threshold(data)

            day_data = data["Time Series (Daily)"][target_date]

            results[symbol] = write_to_s3(symbol, target_date, day_data)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for {symbol}: {e}")
        time.sleep(1)
        
    return results


def fetch_stock_data(symbol):
    """Fetch stock data request to Alpha Vantage with secure key
    
    Parameters
    ----------
    symbol: required
        The name of the equity of your choice
    
    Returns
    -------
    dict: returns raw (as-traded) daily time series of the global equity specified

    Raises
    ------
    exceptions: requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError, requests.exceptions.JSONDecodeError
    
    Also raises requests.exceptions.RequestException directly when:
    - The response is missing the expected "Time Series (Daily)" key (e.g., Alpha Vantage rate-limit response)
    - The most recent day's record is missing a required field (open/high/low/close/volume)
    - The most recent day's record contains a non-numeric value for a required field

    API doc: https://www.alphavantage.co/documentation/ 
    """

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key
        }
    
    r = requests.get('https://www.alphavantage.co/query', params=params)
    r.raise_for_status()
    stock_data = r.json()

    if "Time Series (Daily)" not in stock_data:
        raise requests.exceptions.RequestException(f"Unexpected response for {symbol}: {stock_data}")

    most_recent_date = max(stock_data["Time Series (Daily)"])
    latest_day_data = stock_data["Time Series (Daily)"][most_recent_date]

    required_fields = ["1. open", "2. high", "3. low", "4. close", "5. volume"]

    for field in required_fields:
        if field not in latest_day_data:
            raise requests.exceptions.RequestException(f"Missing field '{field}' for {symbol} on {most_recent_date}")
        try:
            float(latest_day_data[field])
        except ValueError:
            raise requests.exceptions.RequestException(f"Non-numeric value for '{field}' on {most_recent_date}: {latest_day_data[field]}")

    return stock_data

def fetch_with_retry(symbol):
    """Wraps fetch_stock_data with retry logic for transient network failures.

    Retries ConnectionError and Timeout up to 2 times, with a backoff (2s, then 4s). 
    All other exceptions (HTTPError, rate-limit detection) propogate immediately without retry.

    Parameters
    ----------
    symbol: required
        The name of the equity of your choice

    Returns
    -------
    dict: returns raw (as-traded) daily time series of the global equity specified,
    identical to fetch_stock_data's return value.

    Raises
    ------
    requests.exceptions.ConnectionError, requests.exceptions.Timeout:
        re-raises if all retry attempts are exhausted
    requests.exceptions.HTTPError, requests.exceptions.RequestException:
        propogate immediately, not retiredyes 
    """
    for attempt in range(3):
        try:
            result = fetch_stock_data(symbol)
            return result
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)

def apply_threshold(data, target_date=None):
    """Computes daily percentage change and significance flag for a given day.

    Parameters
    ----------
    data: dict
        The fetched stock data dict, containing "Time Series (Daily)".

    target_date: str, optional
        The specific date (YYYY-MM-DD) to tag.
        If not provided defaults to the most recent date in the data (used by the daily pipeline).
        Backfill passes this explicitly to tag every historical day.

    Returns
    -------
    tuple: (data, target_date)
        the enriched data dict and the data that was tagged.
    """

    # use the provided date if given (backfill), otherwise defualt to most recent date (daily pipeline)
    if target_date is None:
        target_date = max(data["Time Series (Daily)"])

    day_data = data["Time Series (Daily)"][target_date]
    daily_open = float(day_data["1. open"])
    daily_close = float(day_data["4. close"])

    daily_pct_change = (daily_close - daily_open) / daily_open * 100
    significant_move = abs(daily_pct_change) > 3

    day_data["pct_change"] = daily_pct_change
    day_data["significant_move"] = significant_move

    return data, target_date


def write_to_s3(symbol, target_date, day_data):
    """Writes a single days enriched record to S3 
    
    Parameters
    ----------
    symbol: str 
        Object key for which the PUT action was initiated.

    target_date: str
        the date (YYYY-MM-DD) this data represents

    day_data: dict 
        A single day's enriched record — the raw open/high/low/close/volume
        fields plus the computed pct_change and significant_move tags for 
        that specific date.

    Returns
    -------
    str:
        S3 key/path in which data was written 

    Raises
    ------
    exceptions: botocore.exceptions.ClientError
    """
    date_parts = target_date.split("-")
    s3_key = f"stock-data/symbol={symbol}/year={date_parts[0]}/month={date_parts[1]}/day={date_parts[2]}/data.json"
    json_data = json.dumps(day_data)
    s3_client.put_object(Bucket=data_bucket, Key=s3_key, Body=json_data)
    return s3_key
