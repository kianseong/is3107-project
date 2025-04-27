from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'dataset_name': os.getenv('KAGGLE_DATASET_NAME', 'lokeshparab/amazon-products-dataset'),
    'download_path': os.getenv('DOWNLOAD_PATH', '/opt/airflow/data'),
    'file_name': os.getenv('FILE_NAME', 'Amazon-Products.csv'),
    'relevant_columns': [
        "name", "main_category", "sub_category", 
        "discount_price", "actual_price", "ratings", "no_of_ratings"
    ]
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
    dag_id='download_dataset',
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['project']
)
def download_dataset():
    @task
    def download_kaggle_dataset() -> str:
        try:
            os.makedirs(CONFIG['download_path'], exist_ok=True)
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(CONFIG['dataset_name'], path=CONFIG['download_path'], unzip=True)

            csv_file_path = os.path.join(CONFIG['download_path'], CONFIG['file_name'])
            
            # Read and process in chunks to handle large files
            chunks = pd.read_csv(csv_file_path, chunksize=10000)
            processed_chunks = []
            
            for chunk in chunks:
                processed_chunk = chunk[CONFIG['relevant_columns']]
                processed_chunks.append(processed_chunk)
            
            # Combine all chunks
            extracted_df = pd.concat(processed_chunks, ignore_index=True)
            extracted_df.to_csv(csv_file_path, index=False)
            
            logger.info(f"Successfully downloaded and processed dataset to {csv_file_path}")
            return csv_file_path
            
        except Exception as e:
            logger.error(f"Error downloading dataset: {str(e)}")
            raise

    # Define task dependencies
    download_kaggle_dataset()

download_dataset_dag = download_dataset() 