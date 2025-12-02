# File: /opt/airflow/dags/rg_detection_dag.py
import sys
sys.path.insert(0, "/opt/airflow/dags/repo")

import pendulum
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

# Import các hàm bạn đã có
from tasks.sql_tasks import (
    load_data_from_sql,
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

# ==================== DAG ====================
with DAG(
    dag_id="rg_detection_pipeline_v1",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # hoặc "@daily"
    catchup=False,
    tags=["rg", "responsible-gambling", "model-training"],
    default_args={"owner": "asdx-ai-team", "retries": 1},
) as dag:

    # 1. Tạo bảng nếu chưa có
    ensure_table = PythonOperator(
        task_id="ensure_results_table",
        python_callable=ensure_results_table,
        op_kwargs={
            "conn_str": conn_str,
            "table_name": PIPELINE["results_table"],
            "schema_sql": PIPELINE["results_schema"],
            "indexes": PIPELINE.get("results_indexes", []),
        },
    )

    # 2. Load dữ liệu từ SQL
    load_data = PythonOperator(
        task_id="load_training_data",
        python_callable=load_data_from_sql,
        op_kwargs={"conn_str": conn_str, "query": PIPELINE["load_query"]},
    )

    # 3. Train model – dùng @task
    @task
    def train_task(training_data: list) -> dict:
        import pandas as pd
        df = pd.DataFrame(training_data)
        logger.info(f"Training RG model on {len(df):,} records")
        return train_rg_model(df)

    # 4. Inference – trả về list[dict]
    @task
    def predict_task(model_info: dict, raw_data: list) -> list[dict]:
        import pandas as pd
        df = pd.DataFrame(raw_data)
        model_type = model_info["model_type"]
        logger.info(f"Running inference with {model_type} on {len(df):,} users")
        return predict_rg_model(model_type, df)

    # 4b. Thêm risk_level
    @task
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

    # 5. Lưu kết quả vào SQL
    @task
    def save_results_task(results: list[dict]):
        save_results_to_sql(
            conn_str=conn_str,
            table_name=PIPELINE["results_table"],
            results_json=json.dumps(results)
        )

    # 6. Log model
    @task
    def log_success(model_info: dict):
        logger.info("RG MODEL TRAINING & INFERENCE COMPLETED")
        logger.info(f"Model version: {model_info['model_type']}")
        logger.info(f"F1 Score: {model_info.get('f1_score')}, AUC-PR: {model_info.get('auc_pr')}")

    # ==================== DAG flow ====================
    ensure_table >> load_data

    trained_model_info = train_task(load_data.output)
    inference_results = predict_task(trained_model_info, load_data.output)
    inference_results_with_risk = add_risk_task(inference_results)
    save_results_task(inference_results_with_risk)

    log_success(trained_model_info)
