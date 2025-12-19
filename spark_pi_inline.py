from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_pi_k8s",
    start_date=datetime(2025, 12, 18),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    spark_pi_task = SparkSubmitOperator(
        task_id="spark_pi",
        application="local:///opt/spark/examples/src/main/python/pi.py",
        conn_id="spark_default",
        executor_cores=1,
        executor_memory="512m",
        driver_cores=1,
        driver_memory="512m",
        name="spark-pi-job",
        verbose=True,
        conf={
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.container.image": "spark:latest",
            "spark.submit.deployMode": "cluster",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark"
        }
    )
