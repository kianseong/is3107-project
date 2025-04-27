# Amazon Products ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for Amazon products data using Apache Airflow and MySQL.

## Prerequisites

- Python 3.8+
- Apache Airflow
- MySQL Server
- Docker and Docker Compose (for containerized setup)

## Project Structure

```
.
├── dags/
│   └── etl/
│       ├── preprocess_data.py
│       └── load_to_mysql.py
├── data/
│   └── Amazon-Products.csv
├── .env
└── docker-compose.yml
```

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Create a `.env` file with the following variables:
```env
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_DATABASE=airflow
```

3. Set up MySQL connection in Airflow:
   - Go to Airflow UI > Admin > Connections
   - Add a new connection:
     - Conn Id: `amazon_products_mysql`
     - Conn Type: `MySQL`
     - Host: `your-mysql-host`
     - Schema: `amazon_products`
     - Login: `your-mysql-username`
     - Password: `your-mysql-password`
     - Port: `3306`

## Running the Pipeline

### Using Docker (Recommended)

1. Build and start the containers:
```bash
docker-compose up -d --build
```

2. Access Airflow UI at `http://localhost:8080`
   - Default credentials: airflow/airflow

3. Enable the DAGs:
   - Go to DAGs view
   - Enable `preprocess_data` and `load_to_mysql` DAGs

4. Trigger the DAGs:
   - Click on the DAG
   - Click "Trigger DAG" button
   - The pipeline will run in sequence:
     1. `preprocess_data`: Cleans and prepares the data
     2. `load_to_mysql`: Loads the processed data into MySQL

5. Access Streamlit UI at `http://localhost:8501`

## Monitoring

- Check DAG runs in Airflow UI
- View logs for each task
- Monitor MySQL database for loaded data

## Troubleshooting

1. Database Connection Issues:
   - Verify MySQL connection settings
   - Check if MySQL server is running
   - Ensure proper permissions are set

2. Data Processing Issues:
   - Check input CSV file format
   - Verify environment variables
   - Review task logs in Airflow UI

## License

This project is licensed under the MIT License - see the LICENSE file for details. 