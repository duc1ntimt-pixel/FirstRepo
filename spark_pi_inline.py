from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from datetime import datetime
with DAG(
    dag_id="spark_pi_operator",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    spark_pi = SparkKubernetesOperator(
        task_id="spark_pi",
        namespace="spark-jobs",
        application_file=None,
        do_xcom_push=False,

        application={
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": "spark-pi-airflow",
                "namespace": "spark-jobs",
            },
            "spec": {
                "type": "Python",
                "mode": "cluster",
                "sparkVersion": "3.5.1",

                "image": "apache/spark:3.5.1",
                "imagePullPolicy": "IfNotPresent",

                "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",

                "driver": {
                    "cores": 1,
                    "memory": "512m",
                    "serviceAccount": "spark-driver",
                },

                "executor": {
                    "cores": 1,
                    "instances": 1,
                    "memory": "512m",
                },

                "restartPolicy": {
                    "type": "Never"
                }
            },
        },
    )
