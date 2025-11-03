# =========================
# dashboard.py (User Action Summary Added)
# =========================
import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
import os

# ======== CONFIG ========
st.set_page_config(page_title="Shopping Cart Abandonment Dashboard", layout="wide")
DATA_PATH = r"E:\proj\cart_stream.txt"  # where consumer writes events

st.title("🛒 Shopping Cart Abandonment Dashboard")
st.sidebar.header("📊 Live Metrics")

# ======== LOAD FUNCTION ========
@st.cache_data(ttl=5)
def load_data():
    if not os.path.exists(DATA_PATH):
        return pd.DataFrame(columns=["UserID", "ProductID", "Action", "Timestamp", "is_abandoned"])
    with open(DATA_PATH, "r") as f:
        lines = f.readlines()
    records = [json.loads(line.strip()) for line in lines if line.strip()]
    df = pd.DataFrame(records)

    if not df.empty:
        df["is_abandoned"] = df["is_abandoned"].astype(int)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    return df


# ======== DASHBOARD CONTENT ========
def render_dashboard():
    df = load_data()
    if df.empty:
        st.warning("No data yet. Waiting for events...")
        return

    # ----- METRICS -----
    total_users = df["UserID"].nunique()
    abandoned_users = df[df["is_abandoned"] == 1]["UserID"].nunique()
    abandonment_rate = (abandoned_users / total_users * 100) if total_users > 0 else 0

    # Sidebar metrics
    st.sidebar.metric("Total Users", total_users)
    st.sidebar.metric("Abandoned Carts (Users)", abandoned_users)
    st.sidebar.metric("Abandonment Rate (%)", round(abandonment_rate, 2))

    # ======== MAIN LAYOUT ========
    st.subheader("🧾 Recent Events")
    st.dataframe(df.tail(15), use_container_width=True)

    col1, col2 = st.columns(2)

    # ----- Action Distribution -----
    with col1:
        st.markdown("### 🛍️ Action Distribution")
        fig_action = px.histogram(
            df, x="Action", color="Action",
            title="User Actions", text_auto=True
        )
        st.plotly_chart(fig_action, use_container_width=True)

    # ----- Abandonment Over Time -----
    with col2:
        st.markdown("### 🚨 Abandonment Events (0 = Active, 1 = Abandoned)")
        fig_abd = px.scatter(
            df, x="Timestamp", y="is_abandoned",
            color="is_abandoned",
            color_continuous_scale=["#00CC96", "#EF553B"],
            title="Abandonment Over Time"
        )
        fig_abd.update_yaxes(tickvals=[0, 1])
        st.plotly_chart(fig_abd, use_container_width=True)

    # ======== USER ACTION SUMMARY ========
    st.markdown("### 🧍‍♂️ User Action Summary (All Actions per User)")
    action_summary = pd.pivot_table(
        df, index="UserID", columns="Action", values="ProductID", aggfunc="count", fill_value=0
    )
    action_summary["Total Actions"] = action_summary.sum(axis=1)
    action_summary = action_summary.reset_index().sort_values(by="Total Actions", ascending=False)

    st.dataframe(
    action_summary.reset_index(drop=True), 
    use_container_width=True,
    hide_index=True
)



# ======== MAIN LOOP ========
render_dashboard()
time.sleep(5)
st.rerun()
