from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime
import time

def wait_for_xcom(ti, task_id: str, timeout: int = 60):
    waited = 0
    while waited < timeout:
        app = ti.xcom_pull(task_ids=task_id)
        if app and 'metadata' in app and 'name' in app['metadata']:
            return app['metadata']['name']
        time.sleep(2)
        waited += 2
    raise ValueError(f"XCom from {task_id} not found after {timeout}s")

with DAG(
    dag_id="spark_pi_dynamic_sensor",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    spark_submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-jobs",
        application_file="spark-pi.yaml",
        do_xcom_push=True,
        get_logs=True,
    )

    wait_xcom = PythonOperator(
        task_id="wait_for_xcom",
        python_callable=wait_for_xcom,
        op_kwargs={"task_id": "spark_pi_submit", "timeout": 120},
    )

    spark_sensor = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="spark-jobs",
        application_name="{{ ti.xcom_pull(task_ids='wait_for_xcom') }}",
        poke_interval=10,
        timeout=600,
    )

    spark_submit >> wait_xcom >> spark_sensor
