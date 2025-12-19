from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

with DAG(
    dag_id="spark_pi_operator",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    spark_pi_submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-jobs",
        application_file="repo/spark-pi.yaml",
        do_xcom_push=True,
    )

    spark_pi_sensor = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="spark-jobs",
        application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
        poke_interval=10,
    )

    spark_pi_submit >> spark_pi_sensor
