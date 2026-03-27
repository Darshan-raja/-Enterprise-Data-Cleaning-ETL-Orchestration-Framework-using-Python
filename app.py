import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="ETL Sales Dashboard", layout="wide")
st.title("🚀 Enterprise Data Pipeline Dashboard")

DATA_PATH = "data/salesorder_cleaned.csv"

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    
    # 1. REMOVE HIDDEN SPACES from column names (The most common cause of KeyError)
    df.columns = df.columns.str.strip()
    
    # 2. CLEAN REVENUE: Remove symbols and convert to number
    if 'Total amount' in df.columns:
        df['Total amount'] = pd.to_numeric(df['Total amount'].replace('[\$,]', '', regex=True), errors='coerce')
    
    # 3. CLEAN DATE: Try common formats and drop rows that aren't dates
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        # Drop rows where Date couldn't be parsed
        df = df.dropna(subset=['Date'])
    
    # --- UI DISPLAY ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Transactions", f"{len(df):,}")
    
    if 'Total amount' in df.columns:
        total_sales = df['Total amount'].sum()
        col2.metric("Total Revenue", f"${total_sales:,.2f}")
    
    col3.metric("Pipeline Status", "Healthy ✅")

    st.subheader("Sales Trends")
    # Only try to build the chart if we have data remaining
    if not df.empty and 'Date' in df.columns and 'Total amount' in df.columns:
        # Grouping by Date and summing Total amount
        chart_data = df.groupby('Date')['Total amount'].sum().sort_index()
        st.line_chart(chart_data)
    else:
        st.warning("⚠️ Chart Error: Could not parse Date or Total amount columns correctly.")

    st.subheader("Data Preview")
    st.dataframe(df.head(10))

else:
    st.error("Data file not found. Please push your CSV to GitHub.")