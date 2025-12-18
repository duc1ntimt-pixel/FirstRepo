from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG(
    dag_id="spark_pi_inline",
    start_date=datetime(2025, 12, 18),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_task = SparkKubernetesOperator(
        task_id="spark_pi",
        namespace="spark-jobs",  
        kubernetes_conn_id="kubernetes_default",
        application_file={
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": "spark-pi", "namespace": "spark-jobs"},
            "spec": {
                "type": "Python",
                "mode": "cluster",
                "image": "spark:latest",  
                "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
                "sparkVersion": "3.5.0",
                "restartPolicy": {"type": "Never"},
                "driver": {"cores": 1, "memory": "512m", "serviceAccount": "spark"},
                "executor": {"cores": 1, "memory": "512m", "instances": 1},
            },
        },
        do_xcom_push=True,
    )
