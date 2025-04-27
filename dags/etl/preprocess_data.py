from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
import pandas as pd
import numpy as np

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'download_path': os.getenv('DOWNLOAD_PATH', '/opt/airflow/data'),
    'file_name': os.getenv('FILE_NAME', 'Amazon-Products.csv'),
    'invalid_ratings': ['nan', 'Get', 'FREE', '₹68.99', '₹65', '₹70', '₹100', '₹99', '₹2.99'],
    'currency_conversion_rate': float(os.getenv('CURRENCY_CONVERSION_RATE', '0.012'))
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
    dag_id='preprocess_data',
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['project']
)
def preprocess_data():
    @task
    def preprocess_dataset() -> str:
        try:
            csv_file_path = os.path.join(CONFIG['download_path'], CONFIG['file_name'])
            
            # Read data in chunks
            chunks = pd.read_csv(csv_file_path, chunksize=10000)
            processed_chunks = []
            
            for chunk in chunks:
                # Convert columns to appropriate types first
                chunk['name'] = chunk['name'].astype(str)
                chunk['main_category'] = chunk['main_category'].astype(str)
                chunk['sub_category'] = chunk['sub_category'].astype(str)
                chunk['ratings'] = chunk['ratings'].astype(str)
                chunk['no_of_ratings'] = chunk['no_of_ratings'].astype(str)
                chunk['discount_price'] = chunk['discount_price'].astype(str)
                chunk['actual_price'] = chunk['actual_price'].astype(str)
                
                # Drop duplicates first
                chunk = chunk.drop_duplicates(subset=['name', 'discount_price', 'actual_price', 'ratings', 'no_of_ratings'], keep="first")
                
                # Drop rows with missing values
                chunk.dropna(subset=['ratings', 'no_of_ratings', 'discount_price', 'actual_price'], inplace=True)
                
                # Drop invalid ratings
                for invalid_rating in CONFIG['invalid_ratings']:
                    chunk.drop(chunk[chunk['ratings'] == invalid_rating].index, inplace=True)
                
                # Process prices
                for price_col in ['discount_price', 'actual_price']:
                    chunk[price_col] = (chunk[price_col]
                                      .str.replace('₹', '', regex=False)
                                      .str.replace(',', '', regex=False)
                                      .astype(float)
                                      * CONFIG['currency_conversion_rate'])
                
                # Clean up ratings
                chunk['ratings'] = pd.to_numeric(chunk['ratings'], errors='coerce')
                chunk['no_of_ratings'] = chunk['no_of_ratings'].str.replace(',', '').astype(float)
                
                # Clean up text columns
                for col in ['name', 'main_category', 'sub_category']:
                    chunk[col] = chunk[col].str.strip()
                    chunk[col] = chunk[col].str.replace("'", "''")  # Escape single quotes
                
                # Add ID column
                chunk['id'] = range(1, len(chunk) + 1)
                
                processed_chunks.append(chunk)
            
            # Combine all processed chunks
            final_df = pd.concat(processed_chunks, ignore_index=True)
            
            # Drop any remaining rows with NaN values
            final_df = final_df.dropna()
            
            # Drop duplicates based on all columns except 'id'
            final_df = final_df.drop_duplicates(subset=['name', 'main_category', 'sub_category', 
                                                      'discount_price', 'actual_price', 
                                                      'ratings', 'no_of_ratings'])
            
            # Reset the ID column after dropping duplicates
            final_df['id'] = range(1, len(final_df) + 1)
            
            # Save the processed data
            final_df.to_csv(csv_file_path, index=False)
            
            logger.info(f"Successfully preprocessed data and saved to {csv_file_path}")
            return csv_file_path
            
        except Exception as e:
            logger.error(f"Error preprocessing data: {str(e)}")
            raise

    # Define task dependencies
    preprocess_dataset()

preprocess_data_dag = preprocess_data() 