# Dùng image chính thức của Apache Spark (có sẵn PySpark)
FROM apache/spark:3.5.0

# Copy script Python của bạn vào thư mục work-dir (nơi Spark chạy mặc định)
COPY spark_pi.py /opt/spark/work-dir/spark_pi.py

# (Tùy chọn) Nếu cần thêm dependencies Python
# RUN pip install --no-cache-dir some-package

# User mặc định của image là 185 (spark user)
USER 185