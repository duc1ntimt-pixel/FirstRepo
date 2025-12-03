# File: /opt/airflow/dags/rg_detection_dag.py
import sys
sys.path.insert(0, "/opt/airflow/dags/repo")

import pendulum
import json
from airflow import DAG
from airflow.decorators import task

# Import các hàm đã có
from tasks.sql_tasks import (
    save_results_to_sql,
    ensure_results_table,
    get_conn_str,
)
from tasks.model_tasks.rg_detection.train import train_rg_model
from tasks.model_tasks.rg_detection.predict import predict_rg_model
from pipelines.rg_detection import PIPELINE
from utils.logger import get_logger

logger = get_logger(__name__)
conn_str = get_conn_str()


# ==================== TaskFlow ====================
@task()
def create_results_table():
    ensure_results_table(
        conn_str=conn_str,
        table_name=PIPELINE["results_table"],
        schema_sql=PIPELINE["results_schema"],
        indexes=PIPELINE.get("results_indexes", [])
    )


@task()
def load_training_data():
    import pandas as pd
    import pyodbc

    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(PIPELINE["load_query"], conn)
    conn.close()
    logger.info(f"📦 Loaded {len(df)} rows")
    return df.to_json(orient="records")  # serialize as JSON


@task()
def train_task(training_data_json: str) -> dict:
    import pandas as pd
    df = pd.read_json(training_data_json)
    logger.info(f"Training RG model on {len(df):,} records")
    return train_rg_model(df)

@task()
def predict_task(model_info: dict, raw_data_json: str) -> list[dict]:
    import pandas as pd
    df = pd.read_json(raw_data_json)
    model_version = model_info["model_version"]
    logger.info(f"Running inference with {model_version} on {len(df):,} users")
    return predict_rg_model(model_version, df)

@task()
def add_risk_task(results: list[dict]) -> list[dict]:
    def map_risk_level(prob: float) -> str:
        if prob >= 0.8:
            return 'High'
        elif prob >= 0.5:
            return 'Medium'
        else:
            return 'Low'

    for row in results:
        row['risk_level'] = map_risk_level(row['probability'])
    return results


@task()
def save_results_task(results: list[dict]):
    save_results_to_sql(
        conn_str=conn_str,
        table_name=PIPELINE["results_table"],
        results_json=json.dumps(results)
    )


@task()
def log_success(model_info: dict):
    logger.info("✅ RG MODEL TRAINING & INFERENCE COMPLETED")
    logger.info(f"Model version: {model_info['model_version']}")


# ==================== DAG ====================
with DAG(
    dag_id="rg_detection_pipeline_v1",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # hoặc "@daily"
    catchup=False,
    tags=["rg", "responsible-gambling", "model-training"],
    default_args={"owner": "asdx-ai-team", "retries": 1},
) as dag:

    # DAG flow
    create_table = create_results_table()
    training_data = load_training_data()

    trained_model_info = train_task(training_data)
    inference_results = predict_task(trained_model_info, training_data)
    inference_with_risk = add_risk_task(inference_results)
    save_results_task(inference_with_risk)
    log_success(trained_model_info)

    # Dependencies
    create_table >> training_data
