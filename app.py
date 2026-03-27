import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Executive Sales Intelligence",
    page_icon="📊",
    layout="wide"
)

# Professional UI Styling (Clean, Modern, and Balanced)
st.markdown("""
    <style>
    .main { background-color: #f4f7f6; }
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 15px 20px;
        border-radius: 10px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.03);
    }
    [data-testid="stMetricValue"] {
        color: #1E3A8A; /* Deep Professional Blue */
        font-size: 32px;
        font-weight: 700;
    }
    [data-testid="stMetricLabel"] {
        color: #64748b;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ Enterprise Sales Data Pipeline")
st.caption("Status: **Operational** | Aggregation: **Monthly Business Review**")

DATA_PATH = "data/salesorder_cleaned.csv"

# --- 2. DATA ENGINE ---
@st.cache_data(ttl=600)
def load_and_clean_data(path):
    if not os.path.exists(path):
        return None
    
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    def find_column(targets, current_cols):
        for t in targets:
            for c in current_cols:
                if t.lower() in c.lower():
                    return c
        return None

    actual_date_col = find_column(['date', 'orderdate'], df.columns)
    actual_amount_col = find_column(['total amount', 'totalvalue', 'totalprice'], df.columns)
    actual_product_col = find_column(['product', 'item'], df.columns)

    if actual_amount_col:
        df['Revenue'] = pd.to_numeric(df[actual_amount_col].astype(str).replace('[\$,]', '', regex=True), errors='coerce')
    
    if actual_date_col:
        df['Date'] = pd.to_datetime(df[actual_date_col], errors='coerce')
        # Filter out bad dates and revenue
        df = df.dropna(subset=['Date', 'Revenue'])
        # Create Month-Year column for the chart
        df['Month'] = df['Date'].dt.to_period('M').dt.to_timestamp()
        
    if actual_product_col:
        df = df.rename(columns={actual_product_col: 'Product'})

    return df

# --- 3. DASHBOARD DISPLAY ---
df = load_and_clean_data(DATA_PATH)

if df is not None and not df.empty:
    # KPI Row
    total_rev = df['Revenue'].sum()
    total_trans = len(df)
    avg_order = df['Revenue'].mean()
    unique_prods = df['Product'].nunique() if 'Product' in df.columns else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Gross Revenue", f"${total_rev:,.0f}")
    m2.metric("Orders", f"{total_trans:,}")
    m3.metric("Avg. Order", f"${avg_order:,.2f}")
    m4.metric("Product Catalog", f"{unique_prods}")

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row
    col_chart, col_pie = st.columns([2, 1])

    with col_chart:
        st.subheader("📈 Monthly Revenue Growth")
        # Aggregating by Month for the professional trend
        monthly_trend = df.groupby('Month')['Revenue'].sum().reset_index().sort_values('Month')
        
        fig_line = px.area(monthly_trend, x='Month', y='Revenue', 
                          template="plotly_white", 
                          color_discrete_sequence=['#3B82F6']) # Modern Blue
        
        fig_line.update_layout(xaxis_title="", yaxis_title="Revenue ($)", margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig_line, use_container_width=True)

    with col_pie:
        st.subheader("📦 Revenue by Product")
        if 'Product' in df.columns:
            prod_data = df.groupby('Product')['Revenue'].sum().nlargest(6).reset_index()
            fig_pie = px.pie(prod_data, values='Revenue', names='Product', hole=0.6,
                            color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_pie.update_layout(margin=dict(l=0, r=0, t=20, b=0))
            st.plotly_chart(fig_pie, use_container_width=True)

    # Data Inspector
    with st.expander("🔍 System Audit Log"):
        st.dataframe(df[['Date', 'Product', 'Revenue']].sort_values('Date', ascending=False), use_container_width=True)

else:
    st.error("🚨 Production Data Unavailable. Run Airflow DAG to refresh CSV.")