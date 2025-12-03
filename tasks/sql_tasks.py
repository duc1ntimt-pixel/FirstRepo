from airflow.decorators import task
import pandas as pd
import json
import os, base64
from airflow.hooks.base import BaseHook
from utils.logger import get_logger
import pyodbc
from typing import Optional, Callable, Dict

logger = get_logger(__name__)
    
def get_conn_str() -> str:
    """
    Lấy connection string từ environment variables.
    Dùng cho các task chạy trên worker pod (KubernetesExecutor).
    """
    # Lấy từ env – nếu không có thì báo lỗi luôn, không để chạy fail lặng lẽ
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")
    server   = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    port = os.getenv("AZURE_SQL_PORT")
    database = os.getenv("AZURE_SQL_DATABASE")

    if not all([username, password, server, database]):
        missing = [k for k, v in {
            "AZURE_SQL_USERNAME": username,
            "AZURE_SQL_PASSWORD": password,
            "AZURE_SQL_SERVER": server,
            "AZURE_SQL_DATABASE": database
        }.items() if not v]
        raise AirflowException(f"Missing SQL env vars: {missing}")

    # Dùng đúng driver 18 (bạn đã cài rồi)
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server=tcp:{server},{port};"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=60;"
    )

    # Chỉ log thông tin không nhạy cảm
    logger.info(f"Connecting to SQL Server: {server}/{database} as {username}")
    logger.debug(f"Full conn_str (password masked): {conn_str.split('Pwd=')[0]}Pwd=***")

    return conn_str


@task()
def check_sql_connection(conn_str=None):
    conn = pyodbc.connect(conn_str)
    conn.close()
    print("✅ SQL connection ok")
    return True

@task()
def load_data_from_sql(conn_str, query):
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"📦 Loaded {len(df)} rows")
    return df.to_json(orient="records")
# tasks/sql_tasks.py → chỉ giữ phần này cho save_results_to_sql

@task
def save_results_to_sql(
    conn_str: str,
    results_json: str,
    table_name: str,
    override_func: Optional[Callable[[Dict], Dict]] = None
) -> None:
    """
    Lưu kết quả inference vào bảng đã được ensure từ trước.
    Không tạo bảng → chỉ INSERT → nhanh, sạch, an toàn.
    """
    data = json.loads(results_json)
    if not data:
        logger.warning("No inference results to save – skipping")
        return

    logger.info(f"Saving {len(data):,} inference records to {table_name}")

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Lấy danh sách cột từ record đầu tiên → dynamic insert
        sample_row = data[0]
        if override_func:
            sample_row = override_func(sample_row)  # áp dụng override nếu có

        columns = ", ".join(sample_row.keys())
        placeholders = ", ".join(["?" for _ in sample_row])
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Chuẩn bị batch
        batch = []
        batch_size = 1000

        for row in data:
            if override_func:
                row = override_func(row)
            batch.append([row[col] for col in sample_row.keys()])

            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                logger.debug(f"Inserted batch of {len(batch)} rows")
                batch.clear()

        # Insert phần còn lại
        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()

        cursor.close()
        conn.close()

        logger.info(f"Successfully saved {len(data):,} rows to {table_name}")

    except Exception as e:
        logger.error(f"Failed to save results to {table_name}: {e}")
        raise
def ensure_results_table(conn_str: str, table_name: str, schema_sql: str, indexes: list = None) -> None:
    """
    Tạo bảng + index nếu chưa tồn tại.
    """
    logger.info(f"Ensuring results table exists: {table_name}")

    full_sql = f"""
IF NOT EXISTS (SELECT * FROM sys.tables t 
               JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name + '.' + t.name = '{table_name}')
BEGIN
    CREATE TABLE {table_name} (
        {schema_sql}
    )
    PRINT 'Table {table_name} created'
END
ELSE
BEGIN
    PRINT 'Table {table_name} already exists'
END
"""

    if indexes:
        for idx in indexes:
            index_name = idx.split()[-1].replace("'", "''")  # escape nếu có '
            full_sql += f"""
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{index_name}' AND object_id = OBJECT_ID('{table_name}'))
BEGIN
    {idx}
    PRINT 'Index {index_name} created'
END
"""

    conn = None
    try:
        conn = pyodbc.connect(conn_str + ";TrustServerCertificate=yes;")
        cursor = conn.cursor()
        cursor.execute(full_sql)
        conn.commit()

        for row in cursor.messages:
            msg = row[1] if isinstance(row, tuple) else str(row)
            logger.info(msg)

        logger.info(f"Table {table_name} is ready")

    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        raise

    finally:
        if conn:
            conn.close()
