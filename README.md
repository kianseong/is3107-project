# Amazon Products ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for Amazon products data using Apache Airflow. The pipeline downloads data from Kaggle, processes it, and loads it into a MySQL database.

## Prerequisites

- Docker
- Docker Compose
- Kaggle API credentials

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Create a `.env` file with your configuration:
```bash
cp .env.example .env
# Edit .env with your settings
```

3. Set up Kaggle API credentials:
```bash
# Create .kaggle directory
mkdir -p ~/.kaggle
# Copy your kaggle.json to ~/.kaggle/
```

4. Build and start the containers:
```bash
docker-compose up -d
```

5. Initialize the Airflow database:
```bash
docker-compose run airflow-webserver airflow db init
```

6. Create an Airflow user:
```bash
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

## Usage

1. Access the Airflow web interface at http://localhost:8080
2. Log in with the credentials you created
3. Enable the `amazon_products_etl` DAG
4. Trigger the DAG manually or wait for the scheduled run

## Project Structure

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
├── README.md
└── dags/
    └── amazon_products_etl.py
```

## Configuration

The following environment variables can be configured in the `.env` file:

- `AIRFLOW_EMAIL`: Email for Airflow notifications
- `AIRFLOW_RETRIES`: Number of retries for failed tasks
- `AIRFLOW_RETRY_DELAY`: Delay between retries in minutes
- `KAGGLE_DATASET_NAME`: Name of the Kaggle dataset
- `DOWNLOAD_PATH`: Path to store downloaded data
- `FILE_NAME`: Name of the CSV file
- `CURRENCY_CONVERSION_RATE`: Rate to convert prices to USD
- `DB_NAME`: MySQL database name
- `DB_TABLE`: MySQL table name

## Monitoring

- Airflow logs can be found in the `logs/` directory
- Database data is persisted in a Docker volume
- The Airflow web interface provides task status and logs

## Troubleshooting

1. If the DAG fails to start:
   - Check the Airflow logs
   - Verify Kaggle API credentials
   - Ensure MySQL is running

2. If data processing fails:
   - Check available disk space
   - Verify file permissions
   - Check MySQL connection settings

## License

This project is licensed under the MIT License - see the LICENSE file for details. 