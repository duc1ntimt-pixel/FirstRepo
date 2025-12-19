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
        # ĐỔI TÊN THAM SỐ Ở ĐÂY: application -> application_file
        application={
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                # Thêm lower để tránh lỗi Kubernetes đặt tên có chữ in hoa
                "name": "spark-pi-{{ ts_nodash | lower }}"
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
        do_xcom_push=True,
    )

    spark_pi_sensor = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="spark-jobs",
        # Lấy tên từ XCom trả về của task trước
        application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
        poke_interval=10,
    )

    spark_pi_submit >> spark_pi_sensor