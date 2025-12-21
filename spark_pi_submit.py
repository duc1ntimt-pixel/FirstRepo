from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

with DAG(
    dag_id="spark_pi_example_no_xcom",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    spark_submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-jobs",                  # Namespace chạy Spark job
        application_file="spark-pi.yaml",        # File YAML trong thư mục dags/
        do_xcom_push=False,                      # Tắt để tránh bug
        get_logs=True,                           # Vẫn lấy log driver pod hiển thị trong UI
        random_name_suffix=False,                # TẮT suffix random → name cố định, predictable
    )
    spark_submit