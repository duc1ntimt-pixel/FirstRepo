# File: /opt/airflow/dags/rg_detection_dag.py

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

# Import các hàm bạn đã có
from tasks.sql_tasks import (
    load_data_from_sql,
    save_results_to_sql,
    ensure_results_table,
    get_connection_string,
)
from tasks.model_tasks.rg_detection.train import train_rg_model
from tasks.model_tasks.rg_detection.predict import predict_rg_model  # ← tên hàm đúng
from pipelines.rg_detection import PIPELINE
from utils.logger import get_logger

logger = get_logger(__name__)
conn_str = get_connection_string()

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

    # 3. Train model – dùng @task để dễ truyền DataFrame
    @task
    def train_task(training_data: list) -> dict:
        import pandas as pd
        df = pd.DataFrame(training_data)
        logger.info(f"Training RG model on {len(df):,} records")
        return train_rg_model(df)  # ← gọi đúng hàm trong train.py

    # 4. Inference lại dữ liệu (có thể dùng data mới sau này)
    @task
    def predict_task(model_info: dict, raw_data: list):
        import pandas as pd
        df = pd.DataFrame(raw_data)
        model_type = model_info["model_type"]
        logger.info(f"Running inference with {model_type} on {len(df):,} users")
        return predict_rg_model(model_type, df)  # ← trả về list[dict] để save

    # 5. Lưu kết quả vào SQL
    save_results = PythonOperator(
        task_id="save_results",
        python_callable=save_results_to_sql,
        op_kwargs={
            "conn_str": conn_str,
            "table_name": PIPELINE["results_table"],
        },
    )

    # ==================== Dependency ====================
    ensure_table >> load_data

    trained_model_info = train_task(load_data.output)
    inference_results = predict_task(trained_model_info, load_data.output)

    save_results.set_upstream(inference_results)

    # Optional: Log model mới
    @task
    def log_success(model_info: dict):
        logger.info("RG MODEL TRAINING & INFERENCE COMPLETED")
        logger.info(f"Model version: {model_info['model_type']}")
        logger.info(f"F1 Score: {model_info.get('f1_score')}, AUC-PR: {model_info.get('auc_pr')}")

    log_success(trained_model_info)