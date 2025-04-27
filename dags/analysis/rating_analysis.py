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
    dag_id='rating_analysis',
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['project']
)
def rating_analysis():
    @task
    def create_rating_analysis() -> None:
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
                
                # Create rating analysis table with more metrics
                rating_analysis_sql = text("""
                CREATE TABLE IF NOT EXISTS rating_analysis (
                    rating_range_id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    rating_range VARCHAR(50) NOT NULL,
                    product_count INTEGER NOT NULL,
                    avg_price FLOAT NOT NULL,
                    min_price FLOAT NOT NULL,
                    max_price FLOAT NOT NULL,
                    price_std FLOAT NOT NULL,
                    total_ratings FLOAT NOT NULL,
                    avg_ratings_per_product FLOAT NOT NULL,
                    avg_discount FLOAT NOT NULL,
                    max_discount FLOAT NOT NULL,
                    discount_percentage FLOAT NOT NULL,
                    price_rating_ratio FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                connection.execute(rating_analysis_sql)
                
                # Create rating ranges
                sales_data['rating_range'] = pd.cut(sales_data['ratings'], 
                                                  bins=[0, 1, 2, 3, 4, 5],
                                                  labels=['0-1', '1-2', '2-3', '3-4', '4-5'])
                
                # Calculate detailed rating statistics
                rating_stats = sales_data.groupby('rating_range').agg({
                    'id': 'count',
                    'actual_price': ['mean', 'min', 'max', 'std'],
                    'no_of_ratings': ['sum', 'mean'],
                    'discount_price': ['mean', 'max']
                }).reset_index()
                
                # Flatten the multi-level columns
                rating_stats.columns = ['rating_range',
                                     'product_count',
                                     'avg_price', 'min_price', 'max_price', 'price_std',
                                     'total_ratings', 'avg_ratings_per_product',
                                     'avg_discount', 'max_discount']
                
                # Add additional metrics
                rating_stats['discount_percentage'] = ((rating_stats['avg_discount'] / rating_stats['avg_price']) * 100).round(2)
                
                # Calculate price_rating_ratio safely
                rating_stats['price_rating_ratio'] = rating_stats.apply(
                    lambda row: (row['avg_price'] / float(row['rating_range'].split('-')[0])) 
                    if float(row['rating_range'].split('-')[0]) != 0 
                    else row['avg_price'], 
                    axis=1
                ).round(4)
                
                # Replace infinity values with a large number
                rating_stats = rating_stats.replace([np.inf, -np.inf], 999999.0)
                
                # Sort by rating range
                rating_stats = rating_stats.sort_values('rating_range')
                
                # Save to database
                rating_stats.to_sql('rating_analysis', connection, if_exists='replace', index=False)
                
                logger.info("Successfully created and populated rating analysis table")
                
        except Exception as e:
            logger.error(f"Error creating rating analysis: {str(e)}")
            raise

    # Define task dependencies
    create_rating_analysis()

rating_analysis_dag = rating_analysis() 