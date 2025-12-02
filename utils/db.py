import os
import logging

logger = logging.getLogger("multi_model_demo_dag")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
def get_connection_string():
    """
    Build SQL Server connection string from environment variables.
    Rancher/Azure sẽ inject ENV vào container → đảm bảo không push password vào Git.
    """

    SQL_USERNAME = os.getenv("SQL_USERNAME", "sqladmin")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "xx!") 
    SQL_SERVER   = os.getenv("SQL_SERVER", "sql-ml-ftai-dev.database.windows.net")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "mldb-dev")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    return conn_str