import os

def get_connection_string():
    """
    Build SQL Server connection string from environment variables.
    Rancher/Azure sẽ inject ENV vào container → đảm bảo không push password vào Git.
    """

    SQL_USERNAME = os.getenv("SQL_USERNAME", "sqladmin")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "xx!")  # default dev (không dùng production)
    SQL_SERVER   = os.getenv("SQL_SERVER", "sql-ml-ftai-dev.database.windows.net")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "mldb-dev")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    return conn_str