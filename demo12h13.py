# /opt/airflow/dags/test_postgres_connection.py

import psycopg2
from airflow import DAG
from airflow.decorators import task
import subprocess
import os
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from tasks.sql_tasks import get_PostgreSQL_conn_params
import logging
from sqlalchemy.engine import make_url
logger = logging.getLogger("airflow.task")
from tasks.model_tasks.rg_dv1.train import load_and_insert
POSTGRES_CONFIG = get_PostgreSQL_conn_params()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 4),
}

# V1.1
with DAG(
    dag_id="Demo",
    start_date=datetime(2023, 1, 1),
    schedule=None,
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
            print("PostgreSQL version:", POSTGRES_CONFIG)
            print("PostgreSQL version:", version)
            cur.close()
            conn.close()
            return "SUCCESS: Connected to PostgreSQL"
        except Exception as e:
            print("ERROR:", str(e))
            raise e

    @task
    def load_data_from_postgre():
    # Dùng đúng cái đã test thành công ở task đầu
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        
        try:
            # Dùng pandas + psycopg2 connection → 100% không lỗi cursor
            df_demo     = pd.read_sql("SELECT * FROM demographic",     conn)
            df_gambling = pd.read_sql("SELECT * FROM gambling",        conn)
            df_rg       = pd.read_sql("SELECT * FROM rg_information",  conn)

            print("LOAD THÀNH CÔNG 100% bằng psycopg2 + pandas!")
            print(f"demographic:     {df_demo.shape}")
            print(f"gambling:        {df_gambling.shape}")
            print(f"rg_information:  {df_rg.shape}")

        finally:
            conn.close()  # luôn đóng kết nối

        return df_demo, df_gambling, df_rg
    
    @task
    def trigger_gits():
        GIT_REPO  = Variable.get("GIT_REPO")
        LOCAL_DIR  = Variable.get("LOCAL_DIR")
        GIT_USER  = Variable.get("GIT_USER")
        GIT_EMAIL = Variable.get("GIT_EMAIL")
        GIT_TOKEN = Variable.get("GIT_TOKEN", default_var=None)

        # Xóa folder cũ nếu có
        if os.path.exists(LOCAL_DIR):
            subprocess.run(["rm", "-rf", LOCAL_DIR], check=True)

        subprocess.run(
            [
                "git",
                "-c",
                f'http.extraheader="AUTHORIZATION: Basic {GIT_TOKEN}"',
                "clone",
                GIT_REPO,
                LOCAL_DIR
            ],
            check=True
        )
        # Đường dẫn file deploy.md
        deploy_file = os.path.join(LOCAL_DIR, "deploy.md")

        # Nếu không có thì tạo mới
        if not os.path.exists(deploy_file):
            with open(deploy_file, "w") as f:
                f.write("# Deploy Log\n\n")

        # Thêm tag ngày giờ
        tag = datetime.now().strftime("Deploy at %Y-%m-%d %H:%M:%S\n")
        with open(deploy_file, "a") as f:
            f.write(tag)

        # Commit & Push
        subprocess.run(["git", "config", "user.name", GIT_USER], cwd=LOCAL_DIR, check=True)
        subprocess.run(["git", "config", "user.email", GIT_EMAIL], cwd=LOCAL_DIR, check=True)
        subprocess.run(["git", "add", "deploy.md"], cwd=LOCAL_DIR, check=True)
        subprocess.run(["git", "commit", "-m", f"Update deploy.md {tag.strip()}"], cwd=LOCAL_DIR, check=True)
        subprocess.run(["git", "push"], cwd=LOCAL_DIR, check=True)

    @task
    def wait_api():
        return ""
    @task
    def call_api():
        return ""
    
    @task 
    def save_data():
        return ""

    # t1 = test_connection()
    # t2 = load_data_from_postgre()
    trigger_gits()
    # t1 >> t2


