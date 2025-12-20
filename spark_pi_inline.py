from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

# Định nghĩa tên ứng dụng dùng chung (template)
# ts_nodash là biến chuẩn của Airflow: YYYYMMDDTHHMMSS
APP_NAME = "spark-pi-{{ ts_nodash | lower }}"

with DAG(
    dag_id="spark_pi_dynamic_no_xcom",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
) as dag:

    # 1. Submit SparkApplication
    spark_submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-jobs",
        application_file="spark-pi.yaml",
        # TẮT do_xcom_push để tránh lỗi sidecar
        do_xcom_push=False, 
    )

    # 2. Sensor dùng thẳng cái tên đã định nghĩa ở trên
    spark_sensor = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="spark-jobs",
        application_name=APP_NAME, # Sử dụng cùng template name
        poke_interval=10,
        timeout=600,
    )

    spark_submit >> spark_sensor