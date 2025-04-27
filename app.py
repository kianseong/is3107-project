import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': 'mysql',  # Use the Docker service name
    'user': 'airflow',
    'password': 'airflow',
    'database': 'amazon_products',
    'port': 3306
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
page = st.sidebar.radio("Go to", ["Overview", "Category Analysis", "Price Analysis", "Rating Analysis"])

# Helper function to load data
def load_data(table_name):
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name}"))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
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
        
        # Category performance metrics
        st.subheader("Category Performance Metrics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.scatter(
                category_analysis,
                x='avg_price',
                y='avg_rating',
                size='product_count',
                color='main_category',
                hover_data=['sub_category', 'total_ratings', 'discount_percentage'],
                title="Price vs Rating by Category"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.scatter(
                category_analysis,
                x='avg_ratings_per_product',
                y='discount_percentage',
                size='product_count',
                color='main_category',
                hover_data=['sub_category', 'avg_price', 'total_ratings'],
                title="Ratings per Product vs Discount Percentage"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAGs first.")

# Category Analysis page
elif page == "Category Analysis":
    st.header("Category Analysis")
    
    # Load data
    category_analysis = load_data('category_analysis')
    
    if not category_analysis.empty:
        # Category distribution
        st.subheader("Category Distribution")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                category_analysis,
                values='product_count',
                names='main_category',
                title="Product Distribution by Category"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                category_analysis.sort_values('total_ratings', ascending=False).head(10),
                x='main_category',
                y='total_ratings',
                title="Top 10 Categories by Total Ratings"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Category metrics
        st.subheader("Category Metrics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.scatter(
                category_analysis,
                x='avg_price',
                y='avg_rating',
                size='product_count',
                color='main_category',
                hover_data=['sub_category', 'total_ratings', 'discount_percentage'],
                title="Price vs Rating by Category"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.scatter(
                category_analysis,
                x='price_std',
                y='rating_std',
                size='product_count',
                color='main_category',
                hover_data=['sub_category', 'avg_price', 'avg_rating'],
                title="Price Variability vs Rating Variability"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Additional metrics
        st.subheader("Additional Category Metrics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                category_analysis.sort_values('discount_percentage', ascending=False).head(10),
                x='main_category',
                y='discount_percentage',
                title="Top 10 Categories by Discount Percentage"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                category_analysis.sort_values('rating_price_ratio', ascending=False).head(10),
                x='main_category',
                y='rating_price_ratio',
                title="Top 10 Categories by Rating-Price Ratio"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAGs first.")

# Price Analysis page
elif page == "Price Analysis":
    st.header("Price Analysis")
    
    # Load data
    price_analysis = load_data('price_analysis')
    
    if not price_analysis.empty:
        # Price distribution
        st.subheader("Price Distribution")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                price_analysis,
                x='price_range',
                y='product_count',
                title="Product Count by Price Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                price_analysis,
                x='price_range',
                y='total_ratings',
                title="Total Ratings by Price Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Price metrics
        st.subheader("Price Metrics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.scatter(
                price_analysis,
                x='avg_price',
                y='avg_rating',
                size='product_count',
                color='price_range',
                hover_data=['total_ratings', 'discount_percentage'],
                title="Price vs Rating by Price Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.scatter(
                price_analysis,
                x='price_std',
                y='rating_std',
                size='product_count',
                color='price_range',
                hover_data=['avg_price', 'avg_rating'],
                title="Price Variability vs Rating Variability"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Additional metrics
        st.subheader("Additional Price Metrics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                price_analysis,
                x='price_range',
                y='discount_percentage',
                title="Discount Percentage by Price Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                price_analysis,
                x='price_range',
                y='avg_ratings_per_product',
                title="Average Ratings per Product by Price Range"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAGs first.")

# Rating Analysis page
elif page == "Rating Analysis":
    st.header("Rating Analysis")
    
    # Load data
    rating_analysis = load_data('rating_analysis')
    
    if not rating_analysis.empty:
        # Rating distribution
        st.subheader("Rating Distribution")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                rating_analysis,
                x='rating_range',
                y='product_count',
                title="Product Count by Rating Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                rating_analysis,
                x='rating_range',
                y='total_ratings',
                title="Total Ratings by Rating Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Rating metrics
        st.subheader("Rating Metrics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.scatter(
                rating_analysis,
                x='avg_price',
                y='total_ratings',
                size='product_count',
                color='rating_range',
                hover_data=['avg_ratings_per_product', 'discount_percentage'],
                title="Price vs Total Ratings by Rating Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.scatter(
                rating_analysis,
                x='price_std',
                y='avg_ratings_per_product',
                size='product_count',
                color='rating_range',
                hover_data=['avg_price', 'total_ratings'],
                title="Price Variability vs Ratings per Product"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Additional metrics
        st.subheader("Additional Rating Metrics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                rating_analysis,
                x='rating_range',
                y='discount_percentage',
                title="Discount Percentage by Rating Range"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                rating_analysis,
                x='rating_range',
                y='price_rating_ratio',
                title="Price-Rating Ratio by Rating Range"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available. Please run the Airflow DAGs first.") 