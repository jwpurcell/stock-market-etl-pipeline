import streamlit as st
import boto3
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt

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
        Bucket=BUCKET_NAME,
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

available_symbols = sorted(df["symbol"].unique())

df["date"] = pd.to_datetime(df["year"] + "-" + df["month"] + "-" + df["day"])

min_date = df["date"].min()
max_date = df["date"].max()


# section 1: controls (select symbol, select date range)
col1, col2 = st.columns(2)
with col1:
    selected_symbol = st.selectbox("Select Symbol", available_symbols)
with col2:
    date_range = st.slider(
        "Date Range",
        min_value=min_date.to_pydatetime(),
        max_value=max_date.to_pydatetime(),
        value=(min_date.to_pydatetime(), max_date.to_pydatetime())
    )

# filtering logic
filtered_df = df[
    (df["symbol"] == selected_symbol) &
    (df["date"] >= date_range[0]) &
    (df["date"] <= date_range[1])
]
filtered_df = filtered_df.sort_values("date")

# section 2: chart
for col in ["open", "high", "low", "close"]:
    filtered_df[col] = filtered_df[col].astype(float)

candlestick = go.Candlestick(
    x=filtered_df["date"],
    open=filtered_df["open"],
    high=filtered_df["high"],
    low=filtered_df["low"],
    close=filtered_df["close"]
)
fig = go.Figure(data=[candlestick])

# moving average
filtered_df["ma_20"] = filtered_df["close"].rolling(window=20).mean()

fig.add_trace(go.Scatter(
    x=filtered_df["date"],
    y=filtered_df["ma_20"],
    mode="lines",
    name="20-Day MA",
    line=dict(color="orange", width=2)
))

# significant-move scatter overlay
significant_days = filtered_df[filtered_df["significant_move"]]

fig.add_trace(go.Scatter(
    x=significant_days["date"],
    y=significant_days["close"],
    mode="markers",
    name="Significant Move",
    marker=dict(color="yellow", size=10, symbol="triangle-up")
))

fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=400
)
st.plotly_chart(fig)


max_gain = filtered_df["pct_change"].max()
max_drop = filtered_df["pct_change"].min()
volatility = filtered_df["pct_change"].std()

# section 3: metric cards
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Volatility", f"{volatility:.2f}%")
with col2: 
    st.metric("Max Single-Day Gain", f"{max_gain:.2f}%")
with col3:
    st.metric("Max Single-Day Drop", f"{max_drop:.2f}%")

# Row 4: significant-move table and kde histogram
col1, col2 = st.columns(2)

with col1:
    significant_moves = filtered_df[filtered_df["significant_move"] == True]
    st.subheader("Significant Move Days")
    st.dataframe(significant_moves[["date", "pct_change", "volume"]])

with col2:
    st.subheader("% Change Distribution")
    fig2, ax = plt.subplots()
    sns.histplot(filtered_df["pct_change"], kde=True, ax=ax)
    ax.set_xlabel("Daily % Change")
    st.pyplot(fig2)