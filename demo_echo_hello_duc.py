from airflow import DAG
from airflow.decorators import task
from datetime import datetime

default_args = {
    "owner": "simple-demo",
}

with DAG(
    dag_id="demo_echo_hello_v1",
    description="Simple one-step DAG that prints hello",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "simple"],
) as dag:

    @task()
    def say_hello():
        print("hello")

    say_hello()
