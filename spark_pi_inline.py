from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

with DAG(
    dag_id="spark_pi_xcom_debug",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False,
    tags=["spark", "debug"],
) as dag:

    # 1. Submit SparkApplication
    # Operator này sẽ tạo một "Launcher Pod" trên Airflow để gửi request đến Spark Operator
    spark_submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-jobs",
        application_file="spark-pi.yaml", 
        do_xcom_push=True,  # Kích hoạt XCom Sidecar
        get_logs=True,
        # Cấu hình để "cứu" sidecar nếu do thiếu tài nguyên hoặc lỗi định nghĩa pod
        executor_config={
            "pod_override": {
                "spec": {
                    "containers": [
                        {
                            "name": "base", # Container chính của Airflow Worker
                            "resources": {
                                "requests": {"cpu": "100m", "memory": "128Mi"},
                                "limits": {"cpu": "200m", "memory": "256Mi"}
                            }
                        }
                        # Airflow sẽ tự chèn container 'xcom-sidecar' vào đây
                    ]
                }
            }
        },
    )

    # 2. SparkKubernetesSensor
    # Lấy trực tiếp metadata['name'] từ XCom của task spark_pi_submit
    spark_sensor = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="spark-jobs",
        application_name="{{ (ti.xcom_pull(task_ids='spark_pi_submit'))['metadata']['name'] }}",
        poke_interval=10,
        timeout=600,
    )

    # Flow đơn giản: Submit xong thì Sensor chạy luôn
    spark_submit >> spark_sensor