from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime
import time

# ===============================
# Python callable để lấy tên SparkApplication từ XCom
# ===============================
def get_spark_app_name(ti, submit_task_id: str):
    app = ti.xcom_pull(task_ids=submit_task_id)
    if app and 'metadata' in app and 'name' in app['metadata']:
        return app['metadata']['name']
    raise ValueError(f"XCom from {submit_task_id} not found or invalid")

# ===============================
# DAG
# ===============================
with DAG(
    dag_id="spark_pi_dynamic",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
    tags=["spark", "kubernetes"],
) as dag:

    # -------------------------------
    # 1. Submit SparkApplication
    # -------------------------------
    spark_submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-jobs",
        application_file="spark-pi.yaml",  # file YAML có template {{ ts_nodash }}
        do_xcom_push=True,                  # trả về metadata của SparkApplication
        get_logs=True,
    )

    # -------------------------------
    # 2. PythonOperator lấy tên SparkApplication từ XCom
    # -------------------------------
    fetch_app_name = PythonOperator(
        task_id="fetch_spark_app_name",
        python_callable=get_spark_app_name,
        op_kwargs={"submit_task_id": "spark_pi_submit"},
    )

    # -------------------------------
    # 3. SparkKubernetesSensor dùng tên lấy được từ XCom
    # -------------------------------
    spark_sensor = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="spark-jobs",
        application_name="{{ ti.xcom_pull(task_ids='fetch_spark_app_name') }}",
        poke_interval=10,
        timeout=600,
    )

    # -------------------------------
    # Dependencies
    # -------------------------------
    spark_submit >> fetch_app_name >> spark_sensor
