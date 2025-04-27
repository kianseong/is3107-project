import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'mysql'),
    'user': os.getenv('MYSQL_USER', 'airflow'),
    'password': os.getenv('MYSQL_PASSWORD', 'airflow'),
    'database': os.getenv('DB_NAME', 'amazon_products'),
    'port': int(os.getenv('MYSQL_PORT', '3306'))
}

# Create database connection
engine = create_engine(
    f'mysql+pymysql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}'
)

# Page config
st.set_page_config(
    page_title="Amazon Products Analysis",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("Amazon Products Analysis Dashboard")

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Overview", "Category Analysis", "Price Analysis", "Rating Analysis", "Time Series"])

# Helper function to load data
def load_data(table_name):
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception as e:
        st.error(f"Error loading data from {table_name}: {str(e)}")
        return pd.DataFrame()

# Overview page
if page == "Overview":
    st.header("Overview")
    
    # Load data
    sales_data = load_data('sales_data')
    category_analysis = load_data('category_analysis')
    
    if not sales_data.empty:
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Products", len(sales_data))
        with col2:
            st.metric("Average Price", f"${sales_data['actual_price'].mean():.2f}")
        with col3:
            st.metric("Average Rating", f"{sales_data['ratings'].mean():.2f}")
        with col4:
            st.metric("Total Ratings", f"{sales_data['no_of_ratings'].sum():,.0f}")
        
        # Top categories
        st.subheader("Top Categories by Product Count")
        fig = px.bar(
            category_analysis.sort_values('product_count', ascending=False).head(10),
            x='main_category',
            y='product_count',
            title="Top 10 Categories"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAG first.")

# Category Analysis page
elif page == "Category Analysis":
    st.header("Category Analysis")
    
    # Load data
    category_analysis = load_data('category_analysis')
    
    if not category_analysis.empty:
        # Category distribution
        st.subheader("Category Distribution")
        fig = px.pie(
            category_analysis,
            values='product_count',
            names='main_category',
            title="Product Distribution by Category"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Category metrics
        st.subheader("Category Metrics")
        fig = px.scatter(
            category_analysis,
            x='avg_price',
            y='avg_rating',
            size='product_count',
            color='main_category',
            title="Price vs Rating by Category"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAG first.")

# Price Analysis page
elif page == "Price Analysis":
    st.header("Price Analysis")
    
    # Load data
    price_analysis = load_data('price_analysis')
    
    if not price_analysis.empty:
        # Price distribution
        st.subheader("Price Distribution")
        fig = px.bar(
            price_analysis,
            x='price_range',
            y='product_count',
            title="Product Count by Price Range"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Price vs Rating
        st.subheader("Price vs Rating")
        fig = px.scatter(
            price_analysis,
            x='price_range',
            y='avg_rating',
            size='total_ratings',
            title="Rating Distribution by Price Range"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAG first.")

# Rating Analysis page
elif page == "Rating Analysis":
    st.header("Rating Analysis")
    
    # Load data
    rating_analysis = load_data('rating_analysis')
    
    if not rating_analysis.empty:
        # Rating distribution
        st.subheader("Rating Distribution")
        fig = px.bar(
            rating_analysis,
            x='rating_range',
            y='product_count',
            title="Product Count by Rating Range"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Rating vs Price
        st.subheader("Rating vs Price")
        fig = px.scatter(
            rating_analysis,
            x='rating_range',
            y='avg_price',
            size='total_ratings',
            title="Price Distribution by Rating Range"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAG first.")

# Time Series page
elif page == "Time Series":
    st.header("Time Series Analysis")
    
    # Load data
    time_series = load_data('time_series_analysis')
    
    if not time_series.empty:
        # Time series metrics
        st.subheader("Time Series Metrics")
        
        # Create time series plot
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=time_series['analysis_date'],
            y=time_series['avg_price'],
            name="Average Price"
        ))
        fig.add_trace(go.Scatter(
            x=time_series['analysis_date'],
            y=time_series['avg_rating'],
            name="Average Rating"
        ))
        fig.update_layout(title="Price and Rating Trends Over Time")
        st.plotly_chart(fig, use_container_width=True)
        
        # Display metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Products", time_series['total_products'].iloc[-1])
            st.metric("Average Price", f"${time_series['avg_price'].iloc[-1]:.2f}")
        with col2:
            st.metric("Average Rating", f"{time_series['avg_rating'].iloc[-1]:.2f}")
            st.metric("Total Ratings", f"{time_series['total_ratings'].iloc[-1]:,.0f}")
    else:
        st.warning("No data available. Please run the Airflow DAG first.") 