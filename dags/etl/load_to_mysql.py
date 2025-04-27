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
    },
    'download_path': os.getenv('DOWNLOAD_PATH', '/opt/airflow/data'),
    'file_name': os.getenv('FILE_NAME', 'Amazon-Products.csv')
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
    dag_id='load_to_mysql',
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['project']
)
def load_to_mysql():
    @task
    def upload_to_mysql() -> None:
        try:
            hook = MySqlHook(mysql_conn_id='amazon_products_mysql')
            conn = hook.get_connection('amazon_products_mysql')
            
            engine = create_engine(
                f'mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{CONFIG["database"]["name"]}'
            )
            
            with engine.connect() as connection:
                # Database setup
                connection.execute(text(f"""
                CREATE DATABASE IF NOT EXISTS {CONFIG['database']['name']}
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci;
                """))
                
                connection.execute(text(f"GRANT ALL PRIVILEGES ON {CONFIG['database']['name']}.* TO 'airflow'@'%'"))
                connection.execute(text("FLUSH PRIVILEGES"))
                connection.execute(text(f"USE {CONFIG['database']['name']};"))
                
                # Table setup
                connection.execute(text(f"DROP TABLE IF EXISTS {CONFIG['database']['table']};"))
                connection.execute(text(f"""
                CREATE TABLE {CONFIG['database']['table']} (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(500) NOT NULL,
                    main_category VARCHAR(100) NOT NULL,
                    sub_category VARCHAR(100) NOT NULL,
                    discount_price FLOAT NOT NULL,
                    actual_price FLOAT NOT NULL,
                    ratings FLOAT NOT NULL,
                    no_of_ratings FLOAT NOT NULL
                );
                """))
                
                # Data preparation
                csv_file_path = os.path.join(CONFIG['download_path'], CONFIG['file_name'])
                df = pd.read_csv(csv_file_path)
                
                df = df.rename(columns={
                    'name': 'name',
                    'main_category': 'main_category',
                    'sub_category': 'sub_category',
                    'discount_price': 'discount_price',
                    'actual_price': 'actual_price',
                    'ratings': 'ratings',
                    'no_of_ratings': 'no_of_ratings'
                })
                
                # Data upload
                chunk_size = 10000
                total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
                
                for i, chunk in enumerate(np.array_split(df, total_chunks)):
                    chunk['ratings'] = chunk['ratings'].astype(float)
                    chunk['no_of_ratings'] = chunk['no_of_ratings'].astype(float)
                    chunk['discount_price'] = chunk['discount_price'].astype(float)
                    chunk['actual_price'] = chunk['actual_price'].astype(float)
                    
                    chunk.to_sql(
                        name=CONFIG['database']['table'],
                        con=connection,
                        if_exists='append',
                        index=False
                    )
            
        except Exception as e:
            logger.error(f"Error uploading to MySQL: {str(e)}")
            raise

    upload_to_mysql()

load_to_mysql_dag = load_to_mysql() 