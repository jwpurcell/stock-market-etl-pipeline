import os
import logging
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

crawler_name = os.environ["CRAWLER_NAME"]
glue_client = boto3.client("glue")

def lambda_handler(event, context):
    """Starts the Glue Crawler in response to the main pipeline's success.

    Triggered by FetchStockDataFunction's OnSuccess Lambda destination,
    rather than a fixed schedule, this ensures the Crawler only runs once
    the day's new data has actually finished writing.

    Parameters
    ----------
    event: dict
        event payload. not used
    context: LambdaContext
        context object. not used.

    Returns
    -------
    dict: {"status": "crawler started", "crawler_name": str}

    Raises
    ------
    botocore.exceptions.ClientError: propagates if start_crawler fails
        (e.g the Crawler is already running)
    """
    
    glue_client.start_crawler(Name=crawler_name)
    logger.info(f"Started crawler: {crawler_name}")
    return {"status": "crawler started", "crawler_name": crawler_name}