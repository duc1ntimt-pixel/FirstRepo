from airflow.decorators import task
import pyodbc
import pandas as pd
import json
import os, base64

sql_conn = base64.b64decode(os.environ['SQL_SERVER_CONN']).decode('utf-8')

def get_conn_str():
    # """Build connection string from environment variables."""
    # user = os.getenv("SQL_USERNAME")
    # pwd = os.getenv("SQL_PASSWORD")
    # server = os.getenv("SQL_SERVER", "sql-ml-ftai-dev.database.windows.net")
    # db = os.getenv("SQL_DATABASE", "mldb-dev")
    # driver = "{ODBC Driver 18 for SQL Server}"

    return (
        sql_conn
    )


@task()
def check_sql_connection(conn_str=None):
    conn = pyodbc.connect(conn_str)
    conn.close()
    print("✅ SQL connection ok")
    return True

@task()
def load_data(conn_str, query):
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"📦 Loaded {len(df)} rows")
    return df.to_json(orient="records")

@task()
def save_results(conn_str, results_json, results_table, override_func=None):
    data = json.loads(results_json)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(f"""
    IF OBJECT_ID('{results_table}', 'U') IS NULL
    CREATE TABLE {results_table} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        case_name NVARCHAR(255),
        status NVARCHAR(50),
        prediction_rg NVARCHAR(10),
        raw_response NVARCHAR(MAX),
        created_at DATETIME DEFAULT GETDATE()
    );
    """)

    insert_query = f"""
    INSERT INTO {results_table} (case_name,status,prediction_rg,raw_response)
    VALUES (?, ?, ?, ?)
    """

    for r in data:
        prediction = r.get("prediction_rg")
        if override_func:
            prediction = override_func(r)
        cursor.execute(insert_query,
                       r.get("case_name"),
                       r.get("status"),
                       str(prediction),
                       str(r.get("raw_response"))
                      )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"💾 Saved {len(data)} rows into {results_table}")
