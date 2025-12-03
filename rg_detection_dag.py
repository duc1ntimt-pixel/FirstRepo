import sys, json, pendulum
sys.path.insert(0, "/opt/airflow/dags/repo")

from airflow import DAG
from airflow.decorators import task
from tasks.sql_tasks import ensure_results_table, get_conn_str, save_results_to_sql
from tasks.model_tasks.rg_detection.train import train_rg_model
from tasks.model_tasks.rg_detection.predict import predict_rg_model
from utils.logger import get_logger
import pandas as pd
import pyodbc

logger = get_logger(__name__)
conn_str = get_conn_str()
RESULTS_TABLE = "training_rg_results"

with DAG(
    dag_id="rg_detection_demo",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None, catchup=False,
    tags=["demo"],
) as dag:

    @task()
    def create_table():
        ensure_results_table(conn_str, RESULTS_TABLE)

    @task()
    def load_data():
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql("SELECT * FROM dbo.training_rg_data", conn)
        conn.close()
        logger.info(f"Loaded {len(df)} rows")
        return df.to_json(orient="records")

    @task()
    def train_task(df_json: str):
        df = pd.read_json(df_json)
        return train_rg_model(df)

    @task()
    def predict_task(model_info: dict, df_json: str):
        df = pd.read_json(df_json)
        return predict_rg_model(model_info['model_version'], df)

    @task()
    def save_task(results: list[dict]):
        save_results_to_sql(conn_str, RESULTS_TABLE, json.dumps(results))

    data = load_data()
    model_info = train_task(data)
    predictions = predict_task(model_info, data)
    save_task(predictions)

    create_table() >> data
