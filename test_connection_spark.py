from pyspark.sql import SparkSession
import pyodbc
import os

spark = SparkSession.builder.appName("TestAzureSQLConnection").getOrCreate()

print("=" * 70)
print("TASK 1: TEST KẾT NỐI AZURE SQL TỪ SPARK JOB")
print("=" * 70)

def get_conn():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
        f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
        f"UID={os.getenv('AZURE_SQL_USERNAME')};"
        f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, timeout=30)

try:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT @@VERSION;")
    version = cur.fetchone()[0]
    print("✅ KẾT NỐI THÀNH CÔNG!")
    print(f"Azure SQL Server Version:\n{version.strip()}")
    cur.close()
    conn.close()
except Exception as e:
    print("❌ KẾT NỐI THẤT BẠI!")
    print(str(e))
    raise  # Để Spark job fail → Airflow task fail luôn

print("Task 1 hoàn thành thành công!")
spark.stop()