from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG(
    dag_id="demo20251209_spark",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "spark", "test"],
    # Nếu cần truyền user_id qua conf khi trigger
    # render_template_as_native_obj=True,
) as dag:

    test_connection_task = SparkKubernetesOperator(
        task_id="test_azure_sql_connection",
        namespace="spark-jobs",
        application_file="test_connection_spark.yaml",
        do_xcom_push=False,
        random_name_suffix=False,
        get_logs=True,                    # Quan trọng: thấy log đầy đủ trong Airflow UI
        startup_timeout_seconds=600,
    )

    hello_task = SparkKubernetesOperator(
        task_id="hello_spark_job",
        namespace="spark-jobs",
        application_file="hello_spark.yaml",
        do_xcom_push=False,
        random_name_suffix=False,
        get_logs=True,
        startup_timeout_seconds=600,
    )

    # Chạy tuần tự: test connection trước → hello sau
    test_connection_task >> hello_task