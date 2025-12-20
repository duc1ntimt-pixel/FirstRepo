from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_function(ti):
    # Đẩy một giá trị đơn giản vào XCom
    return "Hello từ Task Push!"

def pull_function(ti):
    # Kéo giá trị từ Task trước về
    value = ti.xcom_pull(task_ids='push_task')
    print(f"Giá trị nhận được: {value}")
    if not value:
        raise ValueError("Không lấy được dữ liệu từ XCom!")

with DAG(
    dag_id="test_xcom_simple",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_function,
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_function,
    )

    push_task >> pull_task