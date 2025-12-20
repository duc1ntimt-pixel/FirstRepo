from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

with DAG(
    dag_id="spark_pi_xcom_fixed",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
) as dag:

    # 1. Submit SparkApplication
    # SparkKubernetesOperator mặc định trả về nội dung của SparkApplication (JSON)
    spark_submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-jobs",
        application_file="spark-pi.yaml", 
        do_xcom_push=False, # Đẩy toàn bộ YAML/JSON kết quả vào DB
        get_logs=False, 
        random_name_suffix=False,
    )

    # 2. SparkKubernetesSensor
    # Dùng .output để lấy trực tiếp giá trị từ task trước
    spark_sensor = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="spark-jobs",
        # Cách lấy name an toàn trong Airflow 3
        application_name="spark-pi-{{ ts_nodash | lower }}",
        # application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
        poke_interval=10,
        timeout=600,
        mode="reschedule",
    )

    spark_submit >> spark_sensor