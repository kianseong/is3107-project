from airflow import settings
from airflow.models import Connection
import os

def setup_mysql_connection():
    # Get connection details from environment variables or use defaults
    conn_id = 'amazon_products_mysql'
    conn_type = 'mysql'
    host = os.getenv('MYSQL_HOST', 'mysql')
    login = os.getenv('MYSQL_USER', 'airflow')
    password = os.getenv('MYSQL_PASSWORD', 'airflow')
    port = int(os.getenv('MYSQL_PORT', '3306'))

    # Create connection object
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port
    )

    # Get the session
    session = settings.Session()

    # Check if connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        # Update existing connection
        existing_conn.conn_type = conn_type
        existing_conn.host = host
        existing_conn.login = login
        existing_conn.password = password
        existing_conn.port = port
    else:
        # Add new connection
        session.add(conn)

    # Commit the changes
    session.commit()
    session.close()

if __name__ == "__main__":
    setup_mysql_connection() 