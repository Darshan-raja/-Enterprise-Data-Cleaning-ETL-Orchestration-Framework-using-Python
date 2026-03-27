import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="ETL Sales Dashboard", layout="wide")

st.title("🚀 Enterprise Data Pipeline Dashboard")
st.write("This data is automatically cleaned and processed by **Apache Airflow**.")

# Path to your cleaned data
DATA_PATH = "data/salesorder_cleaned.csv"

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    
    # Simple Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rows", len(df))
    col2.metric("Total Sales", f"${df['TotalValue'].sum():,.2f}" if 'TotalValue' in df.columns else "N/A")
    col3.metric("Status", "Data Cleaned")

    # Data Table
    st.subheader("Cleaned Data Preview")
    st.dataframe(df.head(10))

    # Simple Chart
    if 'OrderDate' in df.columns:
        st.subheader("Sales Over Time")
        st.line_chart(df.set_index('OrderDate')['TotalValue'])
else:
    st.error(f"Waiting for Airflow... Could not find {DATA_PATH}. Run your DAG first!")