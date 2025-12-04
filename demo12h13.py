# /opt/airflow/dags/test_postgres_connection.py

import psycopg2
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from tasks.sql_tasks import get_PostgreSQL_conn_params
import logging
from sqlalchemy.engine import make_url

logger = logging.getLogger("airflow.task")
from tasks.model_tasks.rg_dv1.train import load_and_insert
POSTGRES_CONFIG = get_PostgreSQL_conn_params()

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
    def load_data_from_postgre1():
        url = make_url("postgresql+psycopg2://")  # base
        url = url.set(
            username=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password'], 
            host=POSTGRES_CONFIG['host'],
            port=POSTGRES_CONFIG['port'],
            database=POSTGRES_CONFIG['dbname']
        )
        engine = create_engine(url, future=False)

        with engine.connect() as conn:
            df_demo     = pd.read_sql_query("SELECT * FROM demographic",     conn)
            df_gambling = pd.read_sql_query("SELECT * FROM gambling",        conn)
            df_rg       = pd.read_sql_query("SELECT * FROM rg_information",  conn)

        print("df_demo shape:", df_demo.shape)
        print(df_demo.head())
        print("df_gambling shape:", df_gambling.shape)
        print(df_gambling.head())
        print("df_rg shape:", df_rg.shape)
        print(df_rg.head())

        return df_demo, df_gambling, df_rg
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
    def trigger_git():
        

        return ""
    @task
    def wait_api():
        return ""
    @task
    def call_api():
        return ""
    
    @task 
    def save_data():
        return ""

    t1 = test_connection()
    t2 = load_data_from_postgre()
    t1 >> t2


