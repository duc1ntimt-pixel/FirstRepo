from airflow import DAG
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

with DAG(
    dag_id="spark_sensor_test",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
) as dag:

    spark_sensor = SparkKubernetesSensor(
        task_id="spark_sensor_check",
        namespace="spark-jobs",
        application_name="spark-pi-test",  # tên SparkApplication test
        poke_interval=10,
        timeout=120,
    )
