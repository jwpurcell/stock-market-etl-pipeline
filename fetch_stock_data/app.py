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
            data = fetch_stock_data(symbol)

            data, most_recent_date = apply_threshold(data)

            results[symbol] = write_to_s3(symbol, most_recent_date, data)
            
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
    returns raw (as-traded) daily time series of the global equity specified: dict

    Raises
    ------
    exceptions: requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError, requests.exceptions.JSONDecodeError
    
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

    return stock_data

def apply_threshold(data):

    most_recent_date = max(data["Time Series (Daily)"])
    latest_day_data = data["Time Series (Daily)"][most_recent_date]
    daily_open = float(latest_day_data["1. open"])
    daily_close = float(latest_day_data["4. close"])

    daily_pct_change = (daily_close - daily_open) / daily_open * 100
    significant_move = abs(daily_pct_change) > 3

    latest_day_data["pct_change"] = daily_pct_change
    latest_day_data["significant_move"] = significant_move

    return data, most_recent_date


def write_to_s3(symbol, most_recent_date, data):
    """Writes the fetched and transformed data onto S3 bucket 
    
    Parameters
    ----------
    symbol: Object key for which the PUT action was initiated.

    most_recent_date: most recent date of the data when fetched
    
    data: the fetched and transformed stock data dict to be written

    Returns
    -------
    returns str: S3 key/path in which data was written 

    Raises
    ------
    exceptions: botocore.exceptions.ClientError
    """
    date_parts = most_recent_date.split("-")

    s3_key = f"stock-data/symbol={symbol}/year={date_parts[0]}/month={date_parts[1]}/day={date_parts[2]}/data.json"
    json_data = json.dumps(data)
    response = s3_client.put_object(Bucket=data_bucket, Key=s3_key, Body=json_data)

    return s3_key
