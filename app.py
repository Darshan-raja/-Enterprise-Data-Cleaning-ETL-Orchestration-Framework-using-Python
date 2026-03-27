import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(page_title="Executive Sales Intelligence", page_icon="📊", layout="wide")

# Professional Blue KPI Styling
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    [data-testid="stMetricValue"] { color: #1E3A8A; font-size: 32px; font-weight: bold; }
    [data-testid="stMetric"] { 
        background-color: #ffffff; border-radius: 10px; 
        border: 1px solid #e0e0e0; box-shadow: 0 2px 4px rgba(0,0,0,0.05); 
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ Enterprise Sales Data Pipeline")

DATA_PATH = "data/salesorder_cleaned.csv"

@st.cache_data(ttl=600)
def load_and_standardize_data(path):
    if not os.path.exists(path):
        return None
    
    df = pd.read_csv(path)
    # 1. Clean column names (removes hidden spaces/newlines)
    df.columns = df.columns.str.strip()

    # 2. Dynamic Column Finder (Fuzzy Matching)
    def find_col(possible_names, df_cols):
        for name in possible_names:
            for col in df_cols:
                if name.lower() in col.lower():
                    return col
        return None

    date_key = find_col(['date', 'orderdate'], df.columns)
    amount_key = find_col(['total amount', 'totalvalue', 'totalprice'], df.columns)
    prod_key = find_col(['product', 'item'], df.columns)

    # 3. Transform and Rename to Standard Names
    if amount_key:
        df['Total amount'] = pd.to_numeric(df[amount_key].astype(str).replace('[\$,]', '', regex=True), errors='coerce')
    
    if date_key:
        df['Date'] = pd.to_datetime(df[date_key], errors='coerce')
        # Create Month for the monthly chart
        df['Month'] = df['Date'].dt.to_period('M').dt.to_timestamp()

    if prod_key:
        df = df.rename(columns={prod_key: 'Product'})

    # Only keep rows where critical data exists
    return df.dropna(subset=['Date', 'Total amount'])

# --- 3. EXECUTION ---
df = load_and_standardize_data(DATA_PATH)

if df is not None and not df.empty:
    # --- KPI ROW ---
    m1, m2, m3 = st.columns(3)
    m1.metric("Gross Revenue", f"${df['Total amount'].sum():,.2f}")
    m2.metric("Total Transactions", f"{len(df):,}")
    m3.metric("Pipeline Status", "Healthy ✅")

    st.divider()

    # --- CHARTS ROW ---
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("📈 Monthly Revenue Growth")
        # Aggregating by Month as requested
        monthly_sales = df.groupby('Month')['Total amount'].sum().reset_index().sort_values('Month')
        fig = px.line(monthly_sales, x='Month', y='Total amount', template="plotly_white", color_discrete_sequence=['#1E3A8A'])
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("📦 Product Distribution")
        if 'Product' in df.columns:
            top_prods = df.groupby('Product')['Total amount'].sum().nlargest(5).reset_index()
            fig_pie = px.pie(top_prods, values='Total amount', names='Product', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

    st.subheader("Data Preview")
    st.dataframe(df.head(10), use_container_width=True)
else:
    st.error("🚨 Data Error: Check your CSV file structure or run your Airflow DAG.")