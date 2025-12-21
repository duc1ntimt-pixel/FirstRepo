from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello World! DAG chạy thành công rồi nè!")
    return "Done"

with DAG(
    dag_id="hello_world_simple",
    start_date=datetime(2025, 12, 21),
    schedule=None,              # Manual trigger
    catchup=False,
    tags=["test", "hello"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,
    )

    hello_task