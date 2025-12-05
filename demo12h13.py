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
import time

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
        # import psycopg2
        # import pandas as pd
        # import numpy as np
        # from datetime import datetime

        conn = psycopg2.connect(**POSTGRES_CONFIG)
        user_id = dag_run.conf.get("user_id", 7973162)

        try:
            df_demo = pd.read_sql("SELECT * FROM demographic WHERE user_id = %s", conn, params=(user_id,))
            df_gambling = pd.read_sql("SELECT * FROM gambling WHERE user_id = %s", conn, params=(user_id,))
            df_rg = pd.read_sql("SELECT * FROM rg_information WHERE user_id = %s", conn, params=(user_id,))
        finally:
            conn.close()

        # -------------------------------------------------------------------
        # 1. Validate demographic info
        # -------------------------------------------------------------------
        if df_demo.empty:
            raise ValueError("❌ demographic table missing user data")

        df_demo['registration_date'] = pd.to_datetime(df_demo['registration_date'], errors='coerce')

        country  = str(df_demo['country'].iloc[0])
        language = str(df_demo['language'].iloc[0])
        gender   = str(df_demo['gender'].iloc[0])

        birth_year = df_demo['birth_year'].iloc[0]
        birth_year = int(birth_year) if pd.notna(birth_year) else None
        age = datetime.now().year - birth_year if birth_year else 0

        reg_date = df_demo['registration_date'].iloc[0]

        # -------------------------------------------------------------------
        # If NO gambling data → return zero-feature vector
        # -------------------------------------------------------------------
        if df_gambling.empty:
            return {
                "user_id": int(user_id),
                "country": country,
                "language": language,
                "gender": gender,
                "age": age,
                "account_age_days": (datetime.now() - reg_date).days if pd.notna(reg_date) else 0,

                "total_turnover": 0.0,
                "mean_turnover": 0.0,
                "total_hold": 0.0,
                "mean_hold": 0.0,
                "total_bets": 0.0,
                "mean_bets": 0.0,

                "days_active": 0,
                "days_since_last_bet": 0,

                "rolling_7d_std_turnover": 0.0,
                "rolling_7d_std_hold": 0.0,
                "rolling_7d_std_num_bets": 0.0,
                "rolling_30d_std_turnover": 0.0,
                "rolling_30d_std_hold": 0.0,
                "rolling_30d_std_num_bets": 0.0,

                "rolling_7d_mean_turnover": 0.0,
                "rolling_7d_mean_hold": 0.0,
                "rolling_7d_mean_num_bets": 0.0,
                "rolling_30d_mean_turnover": 0.0,
                "rolling_30d_mean_hold": 0.0,
                "rolling_30d_mean_num_bets": 0.0,
            }

        # -------------------------------------------------------------------
        # 2. User HAS gambling activity → compute features
        # -------------------------------------------------------------------
        df_gambling['date'] = pd.to_datetime(df_gambling['date'], errors='coerce')
        df_gambling = df_gambling.sort_values("date")

        # ---------------------------------------
        # Safe Aggregation (NO KEY ERROR POSSIBLE)
        # ---------------------------------------
        df_agg = df_gambling.agg({
            "turnover": ["sum", "mean"],
            "hold": ["sum", "mean"],
            "num_bets": ["sum", "mean"]
        })

        df_agg.columns = ['_'.join(col).strip() for col in df_agg.columns.values]

        # Expected columns → rename mapping
        mapping = {
            "turnover_sum": "total_turnover",
            "turnover_mean": "mean_turnover",
            "hold_sum": "total_hold",
            "hold_mean": "mean_hold",
            "num_bets_sum": "total_bets",
            "num_bets_mean": "mean_bets",
        }

        # Prepare defaults
        agg_safe = {new: 0.0 for new in mapping.values()}

        # Fill what exists
        for old, new in mapping.items():
            if old in df_agg.columns:
                val = df_agg[old]
                # df_agg is a 1-row dataframe → extract scalar
                agg_safe[new] = float(val.iloc[0])

        # ---------------------------------------
        # Base temporal stats
        # ---------------------------------------
        first_bet = df_gambling["date"].min()
        last_bet  = df_gambling["date"].max()
        latest_date = last_bet

        days_active = int((last_bet - first_bet).days) + 1
        days_since_last_bet = 0  # latest_date == last_bet always

        # ---------------------------------------
        # Rolling window features
        # ---------------------------------------
        df_roll = df_gambling.set_index("date")[["turnover", "hold", "num_bets"]]

        def roll_feature(window):
            x = df_roll.rolling(f"{window}D")

            mean_turnover = x["turnover"].mean().mean()
            mean_hold     = x["hold"].mean().mean()
            mean_bets     = x["num_bets"].mean().mean()

            std_turnover = x["turnover"].std().mean()
            std_hold     = x["hold"].std().mean()
            std_bets     = x["num_bets"].std().mean()

            return {
                f"rolling_{window}d_std_turnover": 0.0 if pd.isna(std_turnover) else float(std_turnover),
                f"rolling_{window}d_std_hold":     0.0 if pd.isna(std_hold)     else float(std_hold),
                f"rolling_{window}d_std_num_bets": 0.0 if pd.isna(std_bets)     else float(std_bets),

                f"rolling_{window}d_mean_turnover": 0.0 if pd.isna(mean_turnover) else float(mean_turnover),
                f"rolling_{window}d_mean_hold":     0.0 if pd.isna(mean_hold)     else float(mean_hold),
                f"rolling_{window}d_mean_num_bets": 0.0 if pd.isna(mean_bets)     else float(mean_bets),
            }

        f7 = roll_feature(7)
        f30 = roll_feature(30)

        # -------------------------------------------------------------------
        # Build Final Feature Vector (NO MISSING FEATURES)
        # -------------------------------------------------------------------
        feature_dict = {
            "user_id": int(user_id),
            "country": country,
            "language": language,
            "gender": gender,
            "age": age,

            "account_age_days": (latest_date - reg_date).days if pd.notna(reg_date) else 0,

            "total_turnover": agg_safe["total_turnover"],
            "mean_turnover":  agg_safe["mean_turnover"],
            "total_hold":     agg_safe["total_hold"],
            "mean_hold":      agg_safe["mean_hold"],
            "total_bets":     agg_safe["total_bets"],
            "mean_bets":      agg_safe["mean_bets"],

            "days_active": days_active,
            "days_since_last_bet": days_since_last_bet,

            **f7,
            **f30,
        }

        return feature_dict
    
    @task
    def trigger_gits():
        import pexpect
        print(pexpect)
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

        deploy_file = os.path.join(LOCAL_DIR, "deploy.md")

        # create new
        if not os.path.exists(deploy_file):
            print(f"[INFO] Creating deploy.md")
            with open(deploy_file, "w") as f:
                f.write("# Deploy Log\n\n")
        else:
            print(f"[INFO] deploy.md exists, append log")

        files = os.listdir(LOCAL_DIR)
        if files:
            print(f"[INFO] file/folder in repo:")
            for f in files:
                print(f" - {f}")
        else:
            print(f"[WARNING] Repo cloned, sEmpty")

        # add Tad Datetime
        tag = datetime.now().strftime("Deploy at %Y-%m-%d %H:%M:%S\n")
        with open(deploy_file, "a") as f:
            f.write(tag)
        print(f"[INFO] Tag added to deploy.md: {tag.strip()}")

        # Commit & Push
        print(f"[INFO] Config git user")
        subprocess.run(["git", "config", "user.name", GIT_USER], cwd=LOCAL_DIR, check=True)
        subprocess.run(["git", "config", "user.email", GIT_EMAIL], cwd=LOCAL_DIR, check=True)

        print(f"[INFO] Adding deploy.md to git")
        remote_name = "origin"
        auth_repo = GIT_REPO.replace("https://", f"https://{GIT_USER}:{GIT_TOKEN}@")
        subprocess.run(["git", "remote", "remove", remote_name], cwd=LOCAL_DIR, check=False)  # delete if existed
        subprocess.run(["git", "remote", "add", remote_name, auth_repo], cwd=LOCAL_DIR, check=True)
        print(f"[INFO] Remote {remote_name} set to {auth_repo}")
        print("[INFO] Checking git remote -v")
        subprocess.run(["git", "remote", "-v"], cwd=LOCAL_DIR, check=True)
        subprocess.run(["git", "add", "deploy.md"], cwd=LOCAL_DIR, check=True)

        print(f"[INFO] Committing changes")
        subprocess.run(["git", "commit", "-m", f"Update deploy.md {tag.strip()}"], cwd=LOCAL_DIR, check=True)

        os.environ["GIT_ASKPASS"] = "/bin/echo"
        os.environ["GIT_USERNAME"] = GIT_USER_PUSH
        os.environ["GIT_PASSWORD"] = GIT_PASS_PUSH
        subprocess.run(["git", "config", "credential.helper", "store"], cwd=LOCAL_DIR, check=True)

        cmdpush = f'git -c http.extraheader="AUTHORIZATION: Basic {GIT_TOKEN}" push origin main'
        print(f"[CMD] {cmdpush}")
        try:
            subprocess.run(cmdpush, shell=True, check=True, cwd=LOCAL_DIR)
            print("[INFO] Push completed successfully")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Push failed: {e}")
            return
    @task
    def wait_api():
        url = "http://192.168.100.117:32054/"
        max_retries = 100          # max try 
        retry_delay = 5            # delay

        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "RG Prediction API is running." and data.get("model_loaded") == True:
                        print(f"[INFO] API is ready: {data}")
                        return data
                    else:
                        print(f"[INFO] API not ready yet: {data}, retrying in {retry_delay}s...")
                else:
                    print(f"[WARNING] HTTP {response.status_code}, retrying in {retry_delay}s...")
            except requests.exceptions.RequestException as e:
                print(f"[ERROR] Cannot reach API: {e}, retrying in {retry_delay}s...")

            time.sleep(retry_delay)

        raise Exception(f"API not ready after {max_retries} attempts")
    @task
    def call_api(feature_dict):
        url = "http://192.168.100.117:32054/predict"
        headers = {"accept": "application/json", "Content-Type": "application/json"}

        # call API
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
        # 2. Connect PostgreSQL
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()

        # 3. insert data
        cols = ', '.join(data.keys())
        vals_placeholders = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO user_analysis ({cols}) VALUES ({vals_placeholders})"

        # 4. execute
        cur.execute(sql, list(data.values()))
        conn.commit()

        # 5. Close connect
        cur.close()
        conn.close()

        return f"Inserted user_id={data.get('user_id')} successfully"

    t1 = load_data_from_postgre()
    t2 = trigger_gits()
    t3 = wait_api()
    t4 = call_api(t1)
    t5 = save_data(t4)

t1 >> t2 >> t3 >> t4 >> t5

