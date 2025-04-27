-- Create the airflow user if it doesn't exist
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow';

-- Grant all privileges to the airflow user
GRANT ALL PRIVILEGES ON *.* TO 'airflow'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS amazon_products
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- Grant specific privileges for the amazon_products database
GRANT ALL PRIVILEGES ON amazon_products.* TO 'airflow'@'%';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';
FLUSH PRIVILEGES; 