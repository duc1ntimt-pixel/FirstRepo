import os
import sys
sys.path.insert(0, "/opt/airflow/dags/repo/dags")

import json
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from sklearn.linear_model import LogisticRegression
import joblib
import requests

# -------------------------------
# Config
# -------------------------------
CSV_INPUT = "/opt/airflow/dags/data/input.csv"
CSV_OUTPUT_DIR = "/opt/airflow/dags/data/results/"
MODEL_DIR = "/opt/airflow/dags/models"
MODELS = ["fraud_v1", "fraud_v2", "aml_v1", "credit_v1", "risk_v1"]
API_ENDPOINT = "http://fastapi:8000/predict"

default_args = {
    "owner": "asdx-ai-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="multi_model_demo_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "multi-model"]
) as dag:

    # -------------------------------
    # Step 0: Ensure folders exist
    # -------------------------------
    os.makedirs(os.path.dirname(CSV_INPUT), exist_ok=True)
    os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
    os.makedirs(MODEL_DIR, exist_ok=True)

    # -------------------------------
    # Step 1: Load CSV
    # -------------------------------
    @task()
    def load_data(csv_path):
        if not os.path.exists(csv_path):
            df = pd.DataFrame({
                "feature1": [0.1, 0.2, 0.3],
                "feature2": [1.1, 1.2, 1.3],
                "label": [0, 1, 0],
                "case_name": ["case1", "case2", "case3"]
            })
            df.to_csv(csv_path, index=False)
        df = pd.read_csv(csv_path)
        return df.to_json(orient="records")

    # -------------------------------
    # Step 2: ETL
    # -------------------------------
    @task()
    def run_etl_steps(raw_json):
        df = pd.read_json(raw_json)
        # simple normalization
        df["feature1"] = df["feature1"] / df["feature1"].max()
        df["feature2"] = df["feature2"] / df["feature2"].max()
        return df.to_json(orient="records")

    # -------------------------------
    # Step 3: Train model
    # -------------------------------
    @task()
    def train_model(transformed_json, model_name):
        df = pd.read_json(transformed_json)
        X = df[["feature1", "feature2"]]
        y = df["label"]
        model = LogisticRegression()
        model.fit(X, y)
        model_path = os.path.join(MODEL_DIR, f"{model_name}.pkl")
        joblib.dump(model, model_path)
        return transformed_json

    # -------------------------------
    # Step 4: Call API (multi-model)
    # -------------------------------
    @task()
    def trigger_api(transformed_json, model_name):
        df = pd.read_json(transformed_json)
        payload = df.to_dict(orient="records")
        try:
            resp = requests.post(API_ENDPOINT, params={"model_type": model_name}, json=payload, timeout=10)
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

    # -------------------------------
    # Step 5: Save results CSV
    # -------------------------------
    @task()
    def save_results(results, model_name):
        df = pd.DataFrame(results)
        output_csv = os.path.join(CSV_OUTPUT_DIR, f"{model_name}_results.csv")
        df.to_csv(output_csv, index=False)
        return output_csv

    # -------------------------------
    # DAG execution loop for 5 models
    # -------------------------------
    data_json = load_data(CSV_INPUT)
    etl_json = run_etl_steps(data_json)

    for m in MODELS:
        transformed_json = train_model(etl_json, m)
        api_result = trigger_api(transformed_json, m)
        save_results(api_result, m)
