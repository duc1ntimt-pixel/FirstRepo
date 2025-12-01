import os
import sys

sys.path.append("/opt/airflow/dags/repo/dags/tasks")
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from tasks.sql_tasks import check_sql_connection, load_data, save_results
from tasks.etl_tasks import run_etl_steps
from tasks.model_tasks import train_model
from tasks.api_tasks import trigger_api
from pipelines.llm_fraud_scoring import PIPELINE  # dictionary chứa query, endpoint, etl_steps
from utils.db import get_connection_string
conn_str = get_connection_string()

default_args = {
    "owner": "asdx-ai-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="asdx_llm_fraud_scoring_dag_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["llm", "fraud", "pipeline", "api"]
) as dag:

    conn_ok = check_sql_connection(conn_str)

    # # Step 1: Query SQL Azure
    # base_json = load_data(conn_str, PIPELINE["load_query"])

    # # Step 2: ETL Steps
    # expanded_json = run_etl_steps(base_json, PIPELINE.get("etl_steps"))

    # # Step 3: Run file Py (Clear Data > Training Model > Weights) main.py
    # # Step 4: Expose Model API (CI/CD Host Training Model via AKS) app.py
    # transformed_json = train_model(expanded_json)
    
    # # Step 5: Wait Model On-read => Call Model API in this step 4
    # api_result_json = trigger_api(transformed_json, PIPELINE["api_endpoint"])
    
    # # Step 6: Write results into SQL Azure (Inferences Model x Save Results)
    # save_results_task = save_results(conn_str, api_result_json, PIPELINE["results_table"])

    # # Dependencies
    # conn_ok >> base_json >> expanded_json >> api_result_json >> save_results_task
