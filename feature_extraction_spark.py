from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, sum, mean, stddev, datediff, current_date, lit
from pyspark.sql.window import Window
import sys
import os

# Lấy user_id từ argument (Airflow truyền qua)
user_id = sys.argv[1]

spark = SparkSession.builder \
    .appName(f"FeatureExtraction-{user_id}") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Credentials từ env (mount từ secret như trước)
conn_str = (
    f"jdbc:sqlserver://{os.getenv('AZURE_SQL_SERVER')};"
    f"database={os.getenv('AZURE_SQL_DATABASE')};"
    f"user={os.getenv('AZURE_SQL_USERNAME')};"
    f"password={os.getenv('AZURE_SQL_PASSWORD')};"
    "encrypt=true;trustServerCertificate=false;"
)

# Đọc data
df_demo = spark.read.jdbc(url=conn_str, table="demographic", properties={"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}).filter(col("user_id") == user_id)
df_gambling = spark.read.jdbc(url=conn_str, table="gambling", properties={"driver": ...}).filter(col("user_id") == user_id)
df_rg = spark.read.jdbc(url=conn_str, table="rg_information", properties={"driver": ...}).filter(col("user_id") == user_id)

# Logic tính feature giống code Pandas của bạn (chuyển sang Spark SQL/DataFrame)
# ... (tính age, account_age_days, aggregations, rolling 7d/30d mean/std ...)

# Ví dụ một phần
feature_row = ...  # Tạo Row hoặc DataFrame 1 row với tất cả features

# Write vào table mới (ví dụ: user_features_temp)
feature_row.write.jdbc(url=conn_str, table="user_features_temp", mode="overwrite", properties={...})

spark.stop()