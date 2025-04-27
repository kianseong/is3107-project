from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'database': {
        'name': os.getenv('DB_NAME', 'amazon_products'),
        'table': os.getenv('DB_TABLE', 'sales_data')
    }
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "email": [os.getenv('AIRFLOW_EMAIL', 'airflow@example.com')],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": int(os.getenv('AIRFLOW_RETRIES', '3')),
    "retry_delay": timedelta(minutes=int(os.getenv('AIRFLOW_RETRY_DELAY', '10')))
}

@dag(
    dag_id='category_analysis',
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['project']
)
def category_analysis():
    @task
    def create_category_analysis() -> None:
        try:
            # Get database connection details from Airflow connection
            hook = MySqlHook(mysql_conn_id='amazon_products_mysql')
            conn = hook.get_connection('amazon_products_mysql')
            
            # Create engine with database name included
            engine = create_engine(
                f'mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{CONFIG["database"]["name"]}'
            )
            
            # Read the sales data
            with engine.connect() as connection:
                # Read sales data
                sales_data = pd.read_sql(f"SELECT * FROM {CONFIG['database']['table']}", connection)
                
                # Create category analysis table with more metrics
                category_analysis_sql = text("""
                CREATE TABLE IF NOT EXISTS category_analysis (
                    category_id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    main_category VARCHAR(100) NOT NULL,
                    sub_category VARCHAR(100) NOT NULL,
                    product_count INTEGER NOT NULL,
                    avg_price FLOAT NOT NULL,
                    min_price FLOAT NOT NULL,
                    max_price FLOAT NOT NULL,
                    price_std FLOAT NOT NULL,
                    avg_discount FLOAT NOT NULL,
                    max_discount FLOAT NOT NULL,
                    avg_rating FLOAT NOT NULL,
                    min_rating FLOAT NOT NULL,
                    max_rating FLOAT NOT NULL,
                    rating_std FLOAT NOT NULL,
                    total_ratings FLOAT NOT NULL,
                    avg_ratings_per_product FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                connection.execute(category_analysis_sql)
                
                # Calculate detailed category statistics
                category_stats = sales_data.groupby(['main_category', 'sub_category']).agg({
                    'id': 'count',
                    'actual_price': ['mean', 'min', 'max', 'std'],
                    'discount_price': ['mean', 'min', 'max'],
                    'ratings': ['mean', 'min', 'max', 'std'],
                    'no_of_ratings': ['sum', 'mean']
                }).reset_index()
                
                # Flatten the multi-level columns
                category_stats.columns = ['main_category', 'sub_category', 
                                       'product_count', 
                                       'avg_price', 'min_price', 'max_price', 'price_std',
                                       'avg_discount', 'min_discount', 'max_discount',
                                       'avg_rating', 'min_rating', 'max_rating', 'rating_std',
                                       'total_ratings', 'avg_ratings_per_product']
                
                # Add additional metrics
                category_stats['discount_percentage'] = ((category_stats['avg_discount'] / category_stats['avg_price']) * 100).round(2)
                category_stats['rating_price_ratio'] = (category_stats['avg_rating'] / category_stats['avg_price']).round(4)
                
                # Sort by product count
                category_stats = category_stats.sort_values('product_count', ascending=False)
                
                # Save to database
                category_stats.to_sql('category_analysis', connection, if_exists='replace', index=False)
                
                logger.info("Successfully created and populated category analysis table")
                
        except Exception as e:
            logger.error(f"Error creating category analysis: {str(e)}")
            raise

    # Define task dependencies
    create_category_analysis()

category_analysis_dag = category_analysis() 