import streamlit as st
import boto3
import pandas as pd

BUCKET_NAME = st.secrets["BUCKET_NAME"]

s3_client = boto3.client(
    "s3",
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name="ap-southeast-2"
)

@st.cache_data(ttl=21600)
def load_latest_export():
    """Finds and loads the most recent dashboard export from S3."""
    response = s3_client.list_objects_v2(
        Bucket="BUCKET_NAME",
        Prefix="dashboard-export/",
        Delimiter="/"
    )
    prefixes = response["CommonPrefixes"]
    dates = [p["Prefix"].split("/")[1]for p in prefixes]
    latest_date = max(dates)

    files_response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=f"dashboard-export/{latest_date}/"
    )
    file_keys = [obj["Key"] for obj in files_response["Contents"]]

    s3_path = f"s3://{BUCKET_NAME}/{file_keys[0]}"
    return pd.read_parquet(s3_path)

df = load_latest_export()
st.write(df)