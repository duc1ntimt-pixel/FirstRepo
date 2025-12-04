# /opt/airflow/dags/test_postgres_connection.py

import psycopg2
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

from tasks.sql_tasks import get_PostgreSQL_conn_params
import logging
logger = logging.getLogger("airflow.task")

POSTGRES_CONFIG = get_PostgreSQL_conn_params()

# POSTGRES_CONFIG = {
#     "host": "192.xx.xx.117",
#     "port": 30079,
#     "dbname": "postgres_db",
#     "user": "postgres",
#     "password": "aiteam%xxx",
# }


with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
):

    @task()
    def test_connection():
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print("PostgreSQL version:", version)
            cur.close()
            conn.close()
            return "SUCCESS: Connected to PostgreSQL"
        except Exception as e:
            print("ERROR:", str(e))
            raise e

    test_connection()
