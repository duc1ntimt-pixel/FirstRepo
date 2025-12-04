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

# V1.2
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
        GIT_TOKEN = Variable.get("GIT_TOKEN")
        print(GIT_USER)
        print(GIT_EMAIL)
        print(f"[INFO] Start trigger_gits task")

        # Xóa folder cũ nếu có
        if os.path.exists(LOCAL_DIR):
            print(f"[INFO] Removing existing folder: {LOCAL_DIR}")
            subprocess.run(["rm", "-rf", LOCAL_DIR], check=True)
        else:
            print(f"[INFO] No existing folder, skip remove")

        # Clone repo

        print(f"[INFO] Cloning repo: {GIT_REPO} to {LOCAL_DIR}")
        cmd = f'git -c http.extraheader="AUTHORIZATION: Basic {GIT_TOKEN}" clone {GIT_REPO} {LOCAL_DIR}'
        print(f"[CMD] {cmd}")
        try:
            subprocess.run(cmd, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Clone failed: {e}")
            return

        if not os.path.exists(os.path.join(LOCAL_DIR, ".git")):
            print(f"[ERROR] Clone failed: .git folder not found")
            return

        print("[INFO] Clone completed")
        
        # Đường dẫn file deploy.md
        deploy_file = os.path.join(LOCAL_DIR, "deploy.md")

        # Nếu không có thì tạo mới
        if not os.path.exists(deploy_file):
            print(f"[INFO] Creating deploy.md")
            with open(deploy_file, "w") as f:
                f.write("# Deploy Log\n\n")
        else:
            print(f"[INFO] deploy.md exists, append log")

        files = os.listdir(LOCAL_DIR)
        if files:
            print(f"[INFO] Clone check OK, các file/folder trong repo:")
            for f in files:
                print(f" - {f}")
        else:
            print(f"[WARNING] Repo đã clone nhưng trống")

        # Thêm tag ngày giờ
        tag = datetime.now().strftime("Deploy at %Y-%m-%d %H:%M:%S\n")
        with open(deploy_file, "a") as f:
            f.write(tag)
        print(f"[INFO] Tag added to deploy.md: {tag.strip()}")

        # Commit & Push
        print(f"[INFO] Config git user")
        subprocess.run(["git", "config", "user.name", GIT_USER], cwd=LOCAL_DIR, check=True)
        subprocess.run(["git", "config", "user.email", GIT_EMAIL], cwd=LOCAL_DIR, check=True)

        print(f"[INFO] Adding deploy.md to git")
        subprocess.run(["git", "add", "deploy.md"], cwd=LOCAL_DIR, check=True)

        print(f"[INFO] Committing changes")
        subprocess.run(["git", "commit", "-m", f"Update deploy.md {tag.strip()}"], cwd=LOCAL_DIR, check=True)

        print(f"[INFO] Pushing changes to repo")
        try:
            # git -c http.extraheader="AUTHORIZATION: Basic SU1ULVNPRlRcaHV5bHQ6dzM2a3h6anJoZTczand0bGVtN3BkY2VmZ3hrazZ2M2g1ZXBremVrM2lqaHNhcDJmajVjYQ==" push origin main
            auth_header = f"AUTHORIZATION: Basic {GIT_TOKEN}"
            cmd = ["git", "-c", f'http.extraheader={auth_header}', "push", "push", "origin", "main", GIT_REPO]
            print(cmd)
            subprocess.run(cmd, cwd=LOCAL_DIR, check=True)
            print(f"[INFO] Push 1 try successfully")

            # push_cmd = f'git -c http.extraheader="AUTHORIZATION: Basic {GIT_TOKEN}" push {GIT_REPO}'
            # push_result = subprocess.run(push_cmd, shell=True, check=True, cwd=LOCAL_DIR)
            # print(f"[INFO] Push completed successfully")
            # print(f"[DEBUG] Push output: {push_result.stdout[:200]}...")
        except subprocess.CalledProcessError as e:

            print(f"[ERROR] Push failed: {e}")
            print(f"[ERROR] Push stderr: {e.stderr}")

        print(f"[INFO] Push completed")

    @task
    def wait_api():
        return ""
    @task
    def call_api():
        return ""
    
    @task 
    def save_data():
        return ""

  
    trigger_gits()


