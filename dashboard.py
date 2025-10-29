import streamlit as st
import pandas as pd
import plotly.express as px
import os

# === CONFIG ===
LOCAL_STREAM_PATH = r"E:\ShoppingCartAbandonment\cart_stream.txt"

# --- PAGE SETTINGS ---
st.set_page_config(page_title="Cart Abandonment Analytics", layout="wide")
st.title("🛒 Real-Time Shopping Cart Abandonment Detection Dashboard")

# --- REFRESH BUTTON ---
st.button("🔁 Refresh Data")

# --- LOAD STREAM FILE ---
if not os.path.exists(LOCAL_STREAM_PATH):
    st.warning("⚠ Waiting for data stream... (cart_stream.txt not found yet)")
    st.stop()

try:
    data = pd.read_json(LOCAL_STREAM_PATH, lines=True)
except Exception as e:
    st.warning(f"⚠ Unable to read data: {e}")
    st.stop()

if data.empty:
    st.info("📭 No data received yet. Wait for producer/consumer to start streaming.")
    st.stop()

# === ADD ABANDONMENT DETECTION ===
# In UCI dataset, cancelled invoices start with 'C' → cart abandoned
data['is_abandoned'] = data['InvoiceNo'].astype(str).str.startswith('C')
total_abandoned = data['is_abandoned'].sum()
total_orders = len(data)
abandon_rate = round((total_abandoned / total_orders) * 100, 2)

# === SIDEBAR METRICS ===
st.sidebar.header("📊 Live Metrics")
st.sidebar.metric("Total Records Streamed", total_orders)
st.sidebar.metric("Unique Customers", data['CustomerID'].nunique())
st.sidebar.metric("Unique Products", data['StockCode'].nunique())

# Highlight abandonment rate in red if it’s high
if abandon_rate > 20:
    st.sidebar.error(f"🛑 Abandonment Rate: {abandon_rate}%")
else:
    st.sidebar.success(f"✅ Abandonment Rate: {abandon_rate}%")

st.sidebar.metric("Abandoned Carts", total_abandoned)

# === MAIN DASHBOARD VISUALS ===
st.markdown("### 🧩 Customer & Product Insights")
col1, col2 = st.columns(2)

# Chart 1: Quantity Distribution
with col1:
    fig1 = px.histogram(
        data, x='Quantity',
        title='📦 Distribution of Items in Carts',
        color_discrete_sequence=['#4DB6AC']
    )
    fig1.update_layout(template="plotly_dark")
    st.plotly_chart(fig1, use_container_width=True)

# Chart 2: Top Products
with col2:
    top_products = (
        data.groupby('StockCode')['Quantity']
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    fig2 = px.bar(
        top_products, x='StockCode', y='Quantity',
        title='🏆 Top 10 Purchased/Added Products',
        color='Quantity',
        color_continuous_scale='Blues'
    )
    fig2.update_layout(template="plotly_dark")
    st.plotly_chart(fig2, use_container_width=True)

# Chart 3: Abandonment Trend Over Time
if 'InvoiceDate' in data.columns:
    data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'], errors='coerce')
    hourly_data = (
        data.groupby(data['InvoiceDate'].dt.hour)['is_abandoned']
        .mean()
        .reset_index(name='AbandonRate')
    )

    st.markdown("### ⏰ Hourly Abandonment Trend")
    fig3 = px.line(
        hourly_data, x='InvoiceDate', y='AbandonRate',
        title='Trend of Cart Abandonment (%)',
        markers=True, color_discrete_sequence=['#FF4C4C']
    )
    fig3.update_layout(template="plotly_dark", yaxis_title="Abandonment Rate (%)")
    st.plotly_chart(fig3, use_container_width=True)

# --- SUMMARY INSIGHTS ---
st.markdown("### 💡 Summary Insights")
most_added_product = data.groupby('StockCode')['Quantity'].sum().idxmax()
st.write(f"• Most added product: **{most_added_product}**")
st.write(f"• Total abandoned carts: **{total_abandoned}** out of {total_orders} ({abandon_rate}%)")
st.write("• Higher abandonment rates indicate customers are adding items but not completing checkout — a sign of potential drop-off.")
