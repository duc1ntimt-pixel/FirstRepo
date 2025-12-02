# multi_model_demo_dag.py
import os
import sys
sys.path.insert(0, "/opt/airflow/dags/repo")

import json
import logging
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
CSV_INPUT       = "/mnt/models/data/input.csv"
CSV_OUTPUT_DIR  = "/mnt/models/data/results/"
MODEL_DIR       = "/mnt/models/models"
os.makedirs("/mnt/models/data", exist_ok=True)
os.makedirs("/mnt/models/data/results", exist_ok=True)
os.makedirs("/mnt/models/models", exist_ok=True)

MODELS = ["fraud_v1"]
API_ENDPOINT = "http://192.168.100.117:30082/predict"

# setup logging
logger = logging.getLogger("multi_model_demo_dag")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

default_args = {
    "owner": "ai-team",
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

    @task()
    def load_data(csv_path):
        logger.info(f"Step load_data - csv_path={csv_path}")
        if not os.path.exists(csv_path):
            df = pd.DataFrame({
                "feature1": [0.1, 0.2, 0.3],
                "feature2": [1.1, 1.2, 1.3],
                "label": [0, 1, 0],
                "case_name": ["case1", "case2", "case3"]
            })
            df.to_csv(csv_path, index=False)
            logger.info(f"Created sample CSV at {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded CSV with {len(df)} rows and columns {list(df.columns)}")
        return df.to_json(orient="records")

    @task()
    def run_etl_steps(raw_json):
        logger.info("Step run_etl_steps")
        df = pd.read_json(raw_json)
        logger.info(f"Before ETL: head=\n{df.head().to_dict(orient='records')}")
        # simple normalization
        df["feature1"] = df["feature1"] / df["feature1"].max() if df["feature1"].max() != 0 else df["feature1"]
        df["feature2"] = df["feature2"] / df["feature2"].max() if df["feature2"].max() != 0 else df["feature2"]
        logger.info(f"After ETL: head=\n{df.head().to_dict(orient='records')}")
        return df.to_json(orient="records")

    @task()
    def train_model(transformed_json, model_name):
        logger.info(f"Step train_model - model={model_name}")
        df = pd.read_json(transformed_json)
        X = df[["feature1", "feature2"]]
        y = df["label"]
        model = LogisticRegression()
        model.fit(X, y)
        model_path = os.path.join(MODEL_DIR, f"{model_name}.pkl")
        joblib.dump(model, model_path)
        logger.info(f"Saved model to {model_path}")
        return transformed_json

    @task()
    def trigger_api(transformed_json, model_name):
        logger.info(f"Step trigger_api - model={model_name}")
        df = pd.read_json(transformed_json)
        payload = df.to_dict(orient="records")  # a list of objects
        logger.info(f"Calling API {API_ENDPOINT} with {len(payload)} records (model_type={model_name})")
        try:
            resp = requests.post(API_ENDPOINT, params={"model_type": model_name}, json=payload, timeout=15)
            logger.info(f"API response status: {resp.status_code}")
            try:
                body = resp.json()
                logger.info(f"API response body (truncated): {str(body)[:1000]}")
            except Exception:
                body = {"error": f"non-json response: {resp.text}"}
                logger.warning("Response not JSON")
            if resp.status_code != 200:
                return {"error": f"Status {resp.status_code}", "body": body}
            return body
        except Exception as e:
            logger.exception("API call failed")
            return {"error": str(e)}

    @task()
    def save_results(results, model_name):
        logger.info(f"Step save_results - model={model_name}")
        # results could be dict with error or list of prediction dicts
        if isinstance(results, dict) and "error" in results:
            df = pd.DataFrame([results])
        else:
            # if API returns list of dicts
            try:
                df = pd.DataFrame(results)
            except Exception:
                df = pd.DataFrame([{"raw": str(results)}])
        output_csv = os.path.join(CSV_OUTPUT_DIR, f"{model_name}_results.csv")
        df.to_csv(output_csv, index=False)
        logger.info(f"Wrote results to {output_csv} (rows={len(df)})")
        return output_csv

    # pipeline
    data_json = load_data(CSV_INPUT)
    etl_json = run_etl_steps(data_json)

    for m in MODELS:
        transformed_json = train_model(etl_json, m)
        api_result = trigger_api(transformed_json, m)
        save_results(api_result, m)
