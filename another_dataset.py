from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

import os
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine

import pandas as pd
import json
import matplotlib.pyplot as plt


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}


#@monthly
@dag(dag_id='another_dataset', default_args=default_args, schedule="@once", catchup=False, tags=['project'])
def test_taskflow_api_etl():
    @task
    def download_kaggle_dataset(dataset_name, download_path):
        os.makedirs(download_path, exist_ok=True)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset_name, path=download_path, unzip=True)

        file_name = "Amazon-Products.csv"

        # Combine the path
        csv_file_path = os.path.join(download_path, file_name)

        df = pd.read_csv(csv_file_path)

        # Select relevant columns
        relevant_columns = ["name", "main_category","sub_category", "discount_price", "actual_price", "ratings", "no_of_ratings"]
        extracted_df = df[relevant_columns]

        # Save the new DataFrame
        extracted_df.to_csv(csv_file_path, index=False)

        return csv_file_path

    @task
    def data_preprocessing(csv_file_path):
        df = pd.read_csv(csv_file_path)
        df['id'] = range(1, len(df) + 1)

        df.dropna(subset=['ratings', 'no_of_ratings', 'discount_price', 'actual_price'], inplace=True)
        
        #drop incorrect rating values
        df.drop(df[df['ratings'] == 'nan'].index, inplace=True)
        df.drop(df[df['ratings'] == 'Get'].index, inplace=True)
        df.drop(df[df['ratings'] == 'FREE'].index, inplace=True)
        df.drop(df[df['ratings'] == '₹68.99'].index, inplace=True)
        df.drop(df[df['ratings'] == '₹65'].index, inplace=True)
        df.drop(df[df['ratings'] == '₹70'].index, inplace=True)
        df.drop(df[df['ratings'] == '₹100'].index, inplace=True)
        df.drop(df[df['ratings'] == '₹99'].index, inplace=True)
        df.drop(df[df['ratings'] == '₹2.99'].index, inplace=True)
        df = df.dropna()

        df['discount_price'] = df['discount_price'].str.replace('₹', '', regex=False)  # Set regex=False for simple replacement
        df['discount_price'] = df['discount_price'].str.replace(',', '', regex=False)
        df['discount_price'] = df['discount_price'].astype(float)
        # convert to $ US dollar
        df['discount_price'] = df['discount_price']*0.012

        df['actual_price'] = df['actual_price'].str.replace('₹', '', regex=False)  # Set regex=False for simple replacement
        df['actual_price'] = df['actual_price'].str.replace(',', '', regex=False)
        df['actual_price'] = df['actual_price'].astype(float)
        df['actual_price'] = df['actual_price']*0.012

        df.to_csv(csv_file_path, index=False)

        return csv_file_path

    @task
    def upload_to_mysql(csv_file_path):
        create_database_sql = """
        CREATE DATABASE IF NOT EXISTS deproject;
        """
        # Use the Airflow PostgresHook to execute the SQL
        hook = MySqlHook(mysql_conn_id='mysql_deproject')
        hook.run(create_database_sql, autocommit=True)

        select_db_sql = "USE deproject;"
        hook.run(select_db_sql, autocommit=True)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS deproject.sales_raw_data2 (
            sales_id INTEGER PRIMARY KEY,
            Category VARCHAR(50) NOT NULL,
            SubCategory VARCHAR(50) NOT NULL,
            ProductName VARCHAR(250) NOT NULL,
            DiscountPrice FLOAT NOT NULL,
            ActualPrice FLOAT NOT NULL,
            Rating FLOAT NOT NULL,
            RatingCount INTEGER NOT NULL
        );
        """
        # Use the Airflow PostgresHook to execute the SQL
        hook = MySqlHook(mysql_conn_id='mysql_deproject')
        hook.run(create_table_sql, autocommit=True)

        engine = create_engine('mysql+pymysql://cc:spring2004@172.29.208.1:3306/deproject')
        print(engine)

        dataframe = pd.read_csv(csv_file_path)

        dataframe.to_sql(
            name='sales_raw_data2',  # Table name
            con=engine,  # SQLAlchemy engine
            if_exists='replace',  # Replace table if it exists
            index=False  # Do not include DataFrame index as a column
        )


    # task dependencies are defined in a straightforward way
    csv_file_path = download_kaggle_dataset("lokeshparab/amazon-products-dataset", "data")
    csv_file_path = data_preprocessing(csv_file_path)
    upload_to_mysql(csv_file_path)



test_etl_dag = test_taskflow_api_etl()