#!/usr/bin/env python3
"""
🏦 Ethiopia Inter-Bank Settlement Dashboard - Demo Version
Works without HDFS/Kafka for Streamlit Cloud deployment
"""

import streamlit as st
import pandas as pd
import json
import random
from datetime import datetime
import plotly.express as px

st.set_page_config(
    page_title="🏦 NBE Settlement Pipeline",
    page_icon="🏦",
    layout="wide"
)

st.markdown("""
    <style>
    .main-header { font-size: 2.5rem; font-weight: bold; color: #1a237e; text-align: center; padding: 1rem; }
    .sub-header { font-size: 1.2rem; color: #455a64; text-align: center; padding-bottom: 1rem; }
    .metric-card { background: #f5f5f5; padding: 1rem; border-radius: 10px; text-align: center; }
    .metric-value { font-size: 2rem; font-weight: bold; color: #1a237e; }
    .metric-label { font-size: 0.9rem; color: #616161; }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🏦 Ethiopia Inter-Bank Settlement Pipeline</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">National Bank of Ethiopia · Real-Time Monitoring Dashboard</div>', unsafe_allow_html=True)

# Generate demo data
@st.cache_data
def generate_data():
    banks = ['CBE', 'Dashen', 'Awash', 'Bunna', 'Oromia', 'Wegagen', 'Ahadu']
    types = ['settlement', 'transfer', 'withdrawal', 'deposit', 'payment']
    
    data = []
    for i in range(100):
        source = random.choice(banks)
        dest = random.choice([b for b in banks if b != source])
        data.append({
            'transaction_id': f'TXN-{datetime.now().strftime("%Y%m%d")}-{random.randint(10000, 99999)}',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source_bank': source,
            'destination_bank': dest,
            'amount': round(random.uniform(100, 5000000), 2),
            'currency': 'ETB',
            'transaction_type': random.choice(types),
            'status': random.choices(['completed', 'pending', 'processing'], weights=[80, 10, 10])[0],
            'reference': f'REF-{random.randint(100000, 999999)}'
        })
    return pd.DataFrame(data)

df = generate_data()

# Metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="metric-value">{len(df)}</div>', unsafe_allow_html=True)
    st.markdown('<div class="metric-label">📊 Transactions</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="metric-value">{df[df["amount"] > 1000000].shape[0]}</div>', unsafe_allow_html=True)
    st.markdown('<div class="metric-label">🚨 Fraud Alerts</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="metric-value">{len(df["source_bank"].unique())}</div>', unsafe_allow_html=True)
    st.markdown('<div class="metric-label">🏦 Active Banks</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="metric-value">ETB {df["amount"].sum():,.0f}</div>', unsafe_allow_html=True)
    st.markdown('<div class="metric-label">💰 Total Amount</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Data Viewer
st.markdown("---")
st.markdown("## 📊 Transaction Viewer")

tab1, tab2 = st.tabs(["📋 All Transactions", "📈 Visualizations"])

with tab1:
    st.dataframe(df, use_container_width=True)

with tab2:
    col1, col2 = st.columns(2)
    with col1:
        fig = px.pie(df, names='source_bank', title='Transactions by Bank')
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.histogram(df, x='amount', nbins=20, title='Transaction Amount Distribution')
        st.plotly_chart(fig, use_container_width=True)

# Fraud Alerts
st.markdown("---")
st.markdown("## 🚨 Fraud Alerts")

fraud_df = df[df['amount'] > 1000000]
if not fraud_df.empty:
    st.dataframe(fraud_df, use_container_width=True)
else:
    st.info("No fraud alerts detected.")

# System Status
st.markdown("---")
st.markdown("## 🖥️ System Status")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 📁 HDFS")
    st.write("**Status:** ✅ Connected")
    st.write(f"**Files:** {random.randint(20, 50)} batch files")

with col2:
    st.markdown("### 🔄 Kafka")
    st.write("**Status:** ✅ Connected")
    st.write("**Topic:** `nbe_settlement_topic`")

with col3:
    st.markdown("### 📊 Pipeline")
    st.write("**Status:** ✅ Running")
    st.write(f"**Last Update:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
