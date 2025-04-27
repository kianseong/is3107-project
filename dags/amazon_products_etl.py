from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine, text
import pandas as pd
import json
import matplotlib.pyplot as plt
import numpy as np

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
    ],
    'invalid_ratings': ['nan', 'Get', 'FREE', '₹68.99', '₹65', '₹70', '₹100', '₹99', '₹2.99'],
    'currency_conversion_rate': float(os.getenv('CURRENCY_CONVERSION_RATE', '0.012')),
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
    dag_id='amazon_products_etl',
    default_args=default_args,
    schedule="@once",
    catchup=False,
    tags=['project']
)
def amazon_products_etl():
    @task
    def download_kaggle_dataset(dataset_name: str, download_path: str) -> str:
        try:
            os.makedirs(download_path, exist_ok=True)
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(dataset_name, path=download_path, unzip=True)

            csv_file_path = os.path.join(download_path, CONFIG['file_name'])
            
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

    @task
    def data_preprocessing(csv_file_path: str) -> str:
        try:
            # Read data in chunks
            chunks = pd.read_csv(csv_file_path, chunksize=10000)
            processed_chunks = []
            
            for chunk in chunks:
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

    @task
    def upload_to_mysql(csv_file_path: str) -> None:
        try:
            # Get database connection details from Airflow connection
            hook = MySqlHook(mysql_conn_id='amazon_products_mysql')
            conn = hook.get_connection('amazon_products_mysql')
            
            # Create engine with database name included
            engine = create_engine(
                f'mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{CONFIG["database"]["name"]}'
            )
            
            # Use a single connection for all operations
            with engine.connect() as connection:
                # Create database with proper permissions
                create_db_sql = text(f"""
                CREATE DATABASE IF NOT EXISTS {CONFIG['database']['name']}
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci;
                """)
                connection.execute(create_db_sql)
                logger.info(f"Created database {CONFIG['database']['name']}")
                
                # Grant permissions
                grant_sql = text(f"""
                GRANT ALL PRIVILEGES ON {CONFIG['database']['name']}.* TO 'airflow'@'%';
                """)
                connection.execute(grant_sql)
                
                # Flush privileges
                flush_sql = text("FLUSH PRIVILEGES;")
                connection.execute(flush_sql)
                logger.info("Granted permissions and flushed privileges")
                
                # Use the database
                connection.execute(text(f"USE {CONFIG['database']['name']};"))
                logger.info(f"Using database {CONFIG['database']['name']}")
                
                # Drop table if exists to ensure clean state
                drop_table_sql = text(f"DROP TABLE IF EXISTS {CONFIG['database']['table']};")
                connection.execute(drop_table_sql)
                logger.info(f"Dropped existing table {CONFIG['database']['table']}")
                
                # Create table with fully qualified name and matching column names
                create_table_sql = text(f"""
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
                """)
                connection.execute(create_table_sql)
                logger.info(f"Created table {CONFIG['database']['table']}")
                
                # Read and prepare data
                df = pd.read_csv(csv_file_path)
                logger.info(f"Read CSV file with {len(df)} rows")
                
                # Ensure column names match
                df = df.rename(columns={
                    'name': 'name',
                    'main_category': 'main_category',
                    'sub_category': 'sub_category',
                    'discount_price': 'discount_price',
                    'actual_price': 'actual_price',
                    'ratings': 'ratings',
                    'no_of_ratings': 'no_of_ratings'
                })
                
                # Verify table structure
                result = connection.execute(text(f"DESCRIBE {CONFIG['database']['table']}"))
                columns = [row[0] for row in result]
                logger.info(f"Table columns: {columns}")
                
                # Upload data in chunks
                chunk_size = 10000
                total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
                
                for i, chunk in enumerate(np.array_split(df, total_chunks)):
                    try:
                        # Log chunk information
                        logger.info(f"Processing chunk {i+1}/{total_chunks}")
                        logger.info(f"Chunk shape: {chunk.shape}")
                        
                        # Log data types
                        logger.info(f"Chunk data types:\n{chunk.dtypes}")
                        
                        # Log sample of problematic values
                        for col in ['ratings', 'no_of_ratings', 'discount_price', 'actual_price']:
                            invalid_values = chunk[chunk[col].isna()]
                            if not invalid_values.empty:
                                logger.warning(f"Found {len(invalid_values)} invalid values in {col}")
                                logger.warning(f"Sample of invalid rows:\n{invalid_values[['id', col]].head()}")
                        
                        # Ensure data types match
                        chunk['ratings'] = chunk['ratings'].astype(float)
                        chunk['no_of_ratings'] = chunk['no_of_ratings'].astype(float)
                        chunk['discount_price'] = chunk['discount_price'].astype(float)
                        chunk['actual_price'] = chunk['actual_price'].astype(float)
                        
                        # Log data ranges
                        logger.info(f"Data ranges for chunk {i+1}:")
                        for col in ['ratings', 'no_of_ratings', 'discount_price', 'actual_price']:
                            logger.info(f"{col}: min={chunk[col].min()}, max={chunk[col].max()}")
                        
                        # Insert data with ignore option
                        chunk.to_sql(
                            name=CONFIG['database']['table'],
                            con=connection,
                            if_exists='append',
                            index=False,
                            method='multi',
                            chunksize=1000
                        )
                        logger.info(f"Successfully uploaded chunk {i+1}/{total_chunks}")
                        
                    except Exception as chunk_error:
                        logger.error(f"Error processing chunk {i+1}: {str(chunk_error)}")
                        logger.error(f"Problematic chunk data:\n{chunk.head()}")
                        raise
            
            logger.info(f"Successfully uploaded data to MySQL database")
            
        except Exception as e:
            logger.error(f"Error uploading to MySQL: {str(e)}")
            raise

    @task
    def create_analysis_tables(csv_file_path: str) -> None:
        try:
            # Get database connection details from Airflow connection
            hook = MySqlHook(mysql_conn_id='amazon_products_mysql')
            conn = hook.get_connection('amazon_products_mysql')
            
            # Create engine with database name included
            engine = create_engine(
                f'mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{CONFIG["database"]["name"]}'
            )
            
            # Read the data
            df = pd.read_csv(csv_file_path)
            
            # Use a single connection for all operations
            with engine.connect() as connection:
                # Create category analysis table
                category_analysis_sql = text(f"""
                CREATE TABLE IF NOT EXISTS category_analysis (
                    category_id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    main_category VARCHAR(100) NOT NULL,
                    sub_category VARCHAR(100) NOT NULL,
                    product_count INTEGER NOT NULL,
                    avg_price FLOAT NOT NULL,
                    avg_discount FLOAT NOT NULL,
                    avg_rating FLOAT NOT NULL,
                    total_ratings FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                connection.execute(category_analysis_sql)
                
                # Create price analysis table
                price_analysis_sql = text(f"""
                CREATE TABLE IF NOT EXISTS price_analysis (
                    price_range_id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    price_range VARCHAR(50) NOT NULL,
                    product_count INTEGER NOT NULL,
                    avg_rating FLOAT NOT NULL,
                    total_ratings FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                connection.execute(price_analysis_sql)
                
                # Create rating analysis table
                rating_analysis_sql = text(f"""
                CREATE TABLE IF NOT EXISTS rating_analysis (
                    rating_range_id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    rating_range VARCHAR(50) NOT NULL,
                    product_count INTEGER NOT NULL,
                    avg_price FLOAT NOT NULL,
                    total_ratings FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                connection.execute(rating_analysis_sql)
                
                # Create time series analysis table
                time_series_sql = text(f"""
                CREATE TABLE IF NOT EXISTS time_series_analysis (
                    time_id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    analysis_date DATE NOT NULL,
                    total_products INTEGER NOT NULL,
                    avg_price FLOAT NOT NULL,
                    avg_rating FLOAT NOT NULL,
                    total_ratings FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                connection.execute(time_series_sql)
                
                # Populate category analysis
                category_stats = df.groupby(['main_category', 'sub_category']).agg({
                    'id': 'count',
                    'actual_price': 'mean',
                    'discount_price': 'mean',
                    'ratings': 'mean',
                    'no_of_ratings': 'sum'
                }).reset_index()
                
                category_stats.columns = ['main_category', 'sub_category', 'product_count', 
                                       'avg_price', 'avg_discount', 'avg_rating', 'total_ratings']
                
                category_stats.to_sql('category_analysis', connection, if_exists='replace', index=False)
                
                # Populate price analysis
                df['price_range'] = pd.cut(df['actual_price'], 
                                         bins=[0, 10, 50, 100, 500, 1000, float('inf')],
                                         labels=['0-10', '10-50', '50-100', '100-500', '500-1000', '1000+'])
                
                price_stats = df.groupby('price_range').agg({
                    'id': 'count',
                    'ratings': 'mean',
                    'no_of_ratings': 'sum'
                }).reset_index()
                
                price_stats.columns = ['price_range', 'product_count', 'avg_rating', 'total_ratings']
                price_stats.to_sql('price_analysis', connection, if_exists='replace', index=False)
                
                # Populate rating analysis
                df['rating_range'] = pd.cut(df['ratings'], 
                                          bins=[0, 1, 2, 3, 4, 5],
                                          labels=['0-1', '1-2', '2-3', '3-4', '4-5'])
                
                rating_stats = df.groupby('rating_range').agg({
                    'id': 'count',
                    'actual_price': 'mean',
                    'no_of_ratings': 'sum'
                }).reset_index()
                
                rating_stats.columns = ['rating_range', 'product_count', 'avg_price', 'total_ratings']
                rating_stats.to_sql('rating_analysis', connection, if_exists='replace', index=False)
                
                # Populate time series analysis
                current_date = pd.Timestamp.now().date()
                time_stats = pd.DataFrame({
                    'analysis_date': [current_date],
                    'total_products': [len(df)],
                    'avg_price': [df['actual_price'].mean()],
                    'avg_rating': [df['ratings'].mean()],
                    'total_ratings': [df['no_of_ratings'].sum()]
                })
                
                time_stats.to_sql('time_series_analysis', connection, if_exists='append', index=False)
                
                logger.info("Successfully created and populated analysis tables")
                
        except Exception as e:
            logger.error(f"Error creating analysis tables: {str(e)}")
            raise

    # Define task dependencies
    csv_file_path = download_kaggle_dataset(CONFIG['dataset_name'], CONFIG['download_path'])
    csv_file_path = data_preprocessing(csv_file_path)
    upload_to_mysql(csv_file_path)
    create_analysis_tables(csv_file_path)

amazon_products_dag = amazon_products_etl() 