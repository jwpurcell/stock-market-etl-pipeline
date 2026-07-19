import os
import logging 
import boto3
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bucket_name = os.environ["BUCKET_NAME"]
athena_workgroup = os.environ["ATHENA_WORKGROUP"]
glue_database = os.environ["GLUE_DATABASE"]

athena_client = boto3.client("athena")

from datetime import datetime, timezone

def lambda_handler(event, context):
    """Exports the full stock_data dataset to a new, date-stamped Parquet table.

    Triggered by the Glue Crawler's completion event (see CrawlerCompletionRule
    in template.yaml). Runs a CTAS query against the stock_data table, writing the
    result to a new table and S3 location named for today's date.

    Parameters
    ----------
    event: dict 
        EventBridge event payload from the Crawler completion rule. Not Used.
    
    context: LambdaContext
         Standard Lambda context object. Not Used.

    Returns
    -------
    dict: {"table_name": str, "s3_location": str}
        The name and S3 location of the newly created export table, on success.

    Raises
    ------
    Exception: raised if the CTAS query ends in a FAILED or CANCELLED state,
        with the reason reported by Athena included in the message.
    """
    today = datetime.now(timezone.utc).strftime("%Y%m%d")

    table_name = f"dashboard_export_{today}"
    s3_location = f"s3://{bucket_name}/dashboard-export/{today}/"

    query = f"""
    CREATE TABLE {table_name}
    WITH (format = 'PARQUET', external_location = '{s3_location}')
    AS SELECT * FROM {glue_database}.stock_data
    """

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": glue_database},
        WorkGroup=athena_workgroup
    )
    query_execution_id = response["QueryExecutionId"]
    logger.info(f"Started CTAS query {query_execution_id} for table {table_name}")

    while True:
        status_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status_response["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

        time.sleep(2)
    
    if state == "SUCCEEDED":
        logger.info(f"CTAS export succeeded: {table_name} at {s3_location}")
        return {"table_name": table_name, "s3_location": s3_location}
    else:
        reason = status_response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown reason")
        logger.error(f"CTAS export failed for {table_name}: state={state}, reason={reason}")
        raise Exception(f"CTAS query {query_execution_id} ended in state {state}: {reason}")