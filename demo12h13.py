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
import requests
import json
from tasks.model_tasks.rg_dv1.train import load_and_insert
import numpy as np

def convert_numpy_to_python(d):
    for k, v in d.items():
        if isinstance(v, (np.integer, np.int64)):
            d[k] = int(v)
        elif isinstance(v, (np.floating, np.float64)):
            d[k] = float(v)
    return d

POSTGRES_CONFIG = get_PostgreSQL_conn_params()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 4),
}

# V1.3
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
    def load_data_from_postgre(dag_run=None):
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        user_id = dag_run.conf.get("user_id", 9822065)
        try:
            # Dùng pandas + psycopg2 connection → 100% không lỗi cursor
            df_demo = pd.read_sql("SELECT * FROM demographic WHERE user_id = %s", conn, params=(user_id,))
            df_gambling = pd.read_sql("SELECT * FROM gambling WHERE user_id = %s", conn, params=(user_id,))
            df_rg = pd.read_sql("SELECT * FROM rg_information WHERE user_id = %s", conn, params=(user_id,))

        finally:
            conn.close()  # luôn đóng kết nối

        if df_demo.empty:
            raise ValueError("❌ User not found in demographic table")
        # -----------------------------
        # 2. Convert dates
        # -----------------------------
        df_demo['registration_date'] = pd.to_datetime(df_demo['registration_date'], errors='coerce')
        df_gambling['date'] = pd.to_datetime(df_gambling['date'], errors='coerce')

        # -----------------------------
        # 3. RG label
        # -----------------------------
        df_demo['rg'] = 1 if not df_rg.empty else 0

        # -----------------------------
        # 4. Gambling aggregation
        # -----------------------------
        if df_gambling.empty:
            # if no gambling data, set zeros
            feature_dict = {
                "user_id": user_id,
                "country": df_demo['country'].iloc[0],
                "language": df_demo['language'].iloc[0],
                "gender": df_demo['gender'].iloc[0],
                "age": datetime.now().year - df_demo['birth_year'].iloc[0],
                "account_age_days": 0,
                "total_turnover": 0,
                "mean_turnover": 0,
                "total_hold": 0,
                "mean_hold": 0,
                "total_bets": 0,
                "mean_bets": 0,
                "days_active": 0,
                "days_since_last_bet": 0,
                "rolling_7d_std_turnover": 0,
                "rolling_7d_std_hold": 0,
                "rolling_7d_std_num_bets": 0,
                "rolling_30d_std_turnover": 0,
                "rolling_30d_std_hold": 0,
                "rolling_30d_std_num_bets": 0,
                "rolling_7d_mean_turnover": 0,
                "rolling_7d_mean_hold": 0,
                "rolling_7d_mean_num_bets": 0,
                "rolling_30d_mean_turnover": 0,
                "rolling_30d_mean_hold": 0,
                "rolling_30d_mean_num_bets": 0
            }
            return feature_dict

        # Continue if data exists
        df_gambling = df_gambling.sort_values("date")

        df_agg = df_gambling.agg({
            "turnover": ["sum", "mean"],
            "hold": ["sum", "mean"],
            "num_bets": ["sum", "mean"]
        })

        df_agg.columns = ["total_turnover", "mean_turnover",
                        "total_hold", "mean_hold",
                        "total_bets", "mean_bets"]

        first_bet = df_gambling["date"].min()
        last_bet = df_gambling["date"].max()
        latest_date = last_bet  # local feature only

        days_active = (last_bet - first_bet).days + 1
        days_since_last = (latest_date - last_bet).days

        # -----------------------------
        # 5. Rolling windows
        # -----------------------------
        df_rolling = df_gambling.set_index("date")[["turnover", "hold", "num_bets"]]

        def roll_feature(window):
            x = df_rolling.rolling(f"{window}D")
            return {
                f"rolling_{window}d_std_turnover": x["turnover"].std().mean(),
                f"rolling_{window}d_std_hold": x["hold"].std().mean(),
                f"rolling_{window}d_std_num_bets": x["num_bets"].std().mean(),
                f"rolling_{window}d_mean_turnover": x["turnover"].mean().mean(),
                f"rolling_{window}d_mean_hold": x["hold"].mean().mean(),
                f"rolling_{window}d_mean_num_bets": x["num_bets"].mean().mean(),
            }

        f7 = roll_feature(7)
        f30 = roll_feature(30)
        age = df_gambling['date'].max().year - df_demo['birth_year'].iloc[0]

        # -----------------------------
        # 6. Combine to final dict
        # -----------------------------
        feature_dict = {
            "user_id": user_id,
            "country": df_demo['country'].iloc[0],
            "language": df_demo['language'].iloc[0],
            "gender": df_demo['gender'].iloc[0],
            "age": age,
            "account_age_days": (latest_date - df_demo['registration_date'].iloc[0]).days,
            "total_turnover": df_agg["total_turnover"],
            "mean_turnover": df_agg["mean_turnover"],
            "total_hold": df_agg["total_hold"],
            "mean_hold": df_agg["mean_hold"],
            "total_bets": df_agg["total_bets"],
            "mean_bets": df_agg["mean_bets"],
            "days_active": days_active,
            "days_since_last_bet": days_since_last,
            **f7,
            **f30
        }

        return feature_dict
    @task
    def trigger_gits():
        GIT_REPO  = Variable.get("GIT_REPO")
        LOCAL_DIR  = Variable.get("LOCAL_DIR")
        GIT_USER  = Variable.get("GIT_USER")
        GIT_EMAIL = Variable.get("GIT_EMAIL")
        GIT_TOKEN = Variable.get("GIT_TOKEN")
        GIT_USER_PUSH = Variable.get("GIT_USER_PUSH")
        GIT_PASS_PUSH = Variable.get("GIT_PASS_PUSH")
        print(GIT_USER_PUSH)
        print(GIT_PASS_PUSH)
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

        os.environ["GIT_ASKPASS"] = "/bin/echo"
        os.environ["GIT_USERNAME"] = GIT_USER_PUSH
        os.environ["GIT_PASSWORD"] = GIT_PASS_PUSH
        subprocess.run(["git", "config", "credential.helper", "store"], cwd=LOCAL_DIR, check=True)

        # Push
        print(f"[INFO] Pushing changes to repo")
        try:
            subprocess.run(["git", "push", "origin", "main"], cwd=LOCAL_DIR, check=True)
            print(f"[INFO] Push completed successfully")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Push failed: {e}")

    @task
    def wait_api():
        return ""
    @task
    def call_api(feature_dict):
        url = "http://192.168.100.117:32053/predict"
        headers = {"accept": "application/json", "Content-Type": "application/json"}

        # Gọi API
        try:
            feature_dict = convert_numpy_to_python(feature_dict)

            response = requests.post(url, headers=headers, json=feature_dict)
            response.raise_for_status()
            result = response.json()
            print(f"[INFO] API response: {result}")
            return result
        except requests.RequestException as e:
            print(f"[ERROR] API call failed: {e}")
            return None
    
    @task
    def save_data(data):
        if data is None:
            print("[WARNING] No data to save, skipping task.")
            return "No data to save"
        # 2. Kết nối PostgreSQL
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()

        # 3. Chuẩn bị chèn dữ liệu
        cols = ', '.join(data.keys())
        vals_placeholders = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO user_analysis ({cols}) VALUES ({vals_placeholders})"

        # 4. Thực thi
        cur.execute(sql, list(data.values()))
        conn.commit()

        # 5. Đóng kết nối
        cur.close()
        conn.close()

        return f"Inserted user_id={data.get('user_id')} successfully"

    t1 = load_data_from_postgre()
    t2 = call_api(t1)
    t3 = save_data(t2)

t1 >> t2 >> t3

