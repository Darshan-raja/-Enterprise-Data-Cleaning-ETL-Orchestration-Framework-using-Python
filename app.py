import streamlit as st
import pandas as pd
import os

st.title("📊 Enterprise Sales Dashboard")
st.write("Welcome to the final stage of our ETL Pipeline! Here is the cleaned data.")

# Using the EXACT file name from your screenshot!
file_path = "data/clean_salesorder.csv"

if os.path.exists(file_path):
    # Read the cleaned data
    df = pd.read_csv(file_path)
    
    # Show the data table
    st.subheader("Cleaned Dataset")
    st.dataframe(df)
    
    # Show a quick summary
    st.subheader("Data Summary")
    st.write(f"**Total Rows:** {len(df)}")
    st.write(f"**Total Columns:** {len(df.columns)}")

else:
    st.error(f"Could not find the data file at {file_path}. Make sure the Airflow DAG has run!")