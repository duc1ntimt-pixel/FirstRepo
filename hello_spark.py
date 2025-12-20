from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

print("=" * 70)
print("TASK 2: HELLO FROM SPARK ON KUBERNETES!")
print("=" * 70)
print("HELLO FROM THE SECOND SPARK JOB!!! 🎉🎉🎉")
print(f"Spark version: {spark.version}")
print("Mọi thứ đang chạy tốt trong cluster Spark!")

# Tạo DataFrame nhỏ để thấy Spark thực sự chạy
data = [("Hello", "Airflow"), ("Hello", "Spark"), ("Hello", "Kubernetes")]
df = spark.createDataFrame(data, ["greeting", "to"])
df.show()

print("Task 2 hoàn thành thành công!")
spark.stop()