# /opt/airflow/dags/rg_detection_demo_dag.py
import sys
sys.path.insert(0, "/opt/airflow/dags/repo")

import pyodbc
import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from tasks.sql_tasks import get_conn_str

import logging

logger = logging.getLogger("airflow.task")
CONN_STR = get_conn_str()
MODEL_DIR = "/mnt/models/models"
os.makedirs(MODEL_DIR, exist_ok=True)
RESULTS_TABLE = "dbo.rg_inference_results"

with DAG(
    dag_id="rg_detection_demoV3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rg", "demo"]
) as dag:

    @task()
    def load_training_data():
        query = "SELECT user_id, birth_year, registration_date, feature1, feature2, rg FROM dbo.training_rg_data"
        with pyodbc.connect(CONN_STR) as conn:
            df = pd.read_sql(query, conn)
        logger.info(f"Loaded {len(df)} training rows")
        # serialize to JSON để XCom-safe
        return df.to_json(orient="records")

    @task()
    def train_model_task(training_json: str):
        import pandas as pd
        from catboost import CatBoostClassifier
        import joblib
        import os
        from datetime import datetime
        from sklearn.preprocessing import StandardScaler
        from sklearn.compose import ColumnTransformer

        df = pd.read_json(training_json)
        df['age'] = datetime.now().year - pd.to_datetime(df['birth_year']).dt.year
        df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'])).dt.days
        df = df.fillna(0)

        X = df[['feature1','feature2','age','account_age_days']]
        y = df['rg'].astype(int)

        # chỉ chuẩn hóa numeric features
        numeric_features = X.columns.tolist()
        preprocessor = ColumnTransformer([('num', StandardScaler(), numeric_features)])
        X_proc = preprocessor.fit_transform(X)

        # train CatBoost
        scale_pos_weight = (len(y) - y.sum()) / y.sum() if y.sum() > 0 else 1.0
        model = CatBoostClassifier(
            iterations=50,
            learning_rate=0.1,
            depth=3,
            scale_pos_weight=scale_pos_weight,
            random_seed=42,
            verbose=False,
            eval_metric='F1'
        )
        model.fit(X_proc, y)

        ts = datetime.now().strftime("%Y%m%d_%H%M")
        model_path = os.path.join(MODEL_DIR, f"rg_model_{ts}.pkl")
        prep_path = os.path.join(MODEL_DIR, f"rg_preprocessor_{ts}.pkl")
        joblib.dump(model, model_path)
        joblib.dump(preprocessor, prep_path)

        logger.info(f"Model saved: {model_path}")
        logger.info(f"Preprocessor saved: {prep_path}")

        # return chỉ paths + timestamp → XCom-safe
        return {
            "model_ts": ts,
            "model_path": model_path,
            "prep_path": prep_path
        }

    @task()
    def inference_task(model_info: dict):
        import pandas as pd
        import joblib
        from datetime import datetime

        query = "SELECT user_id, birth_year, registration_date, feature1, feature2 FROM dbo.training_rg_data"
        with pyodbc.connect(CONN_STR) as conn:
            df = pd.read_sql(query, conn)

        df['age'] = datetime.now().year - pd.to_datetime(df['birth_year']).dt.year
        df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'])).dt.days

        model = joblib.load(model_info['model_path'])
        preprocessor = joblib.load(model_info['prep_path'])
        X = df[['feature1','feature2','age','account_age_days']]
        X_proc = preprocessor.transform(X)

        df['prediction'] = model.predict(X_proc)
        df['probability'] = model.predict_proba(X_proc)[:,1]

        def map_risk(p):
            if p>=0.8: return 'High'
            elif p>=0.5: return 'Medium'
            else: return 'Low'
        df['risk_level'] = df['probability'].apply(map_risk)

        df['model_type'] = model_info['model_ts']
        df['training_data_date'] = datetime.now().date()
        df['inference_date'] = datetime.now()

        logger.info(f"Inference completed for {len(df)} rows")
        return df.to_json(orient="records")  # JSON string → XCom-safe

    @task()
    def save_results_task(results_json: str):
        import pandas as pd
        import pyodbc
        df = pd.read_json(results_json)
        with pyodbc.connect(CONN_STR, autocommit=True) as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute(f"""
                    INSERT INTO {RESULTS_TABLE}
                    (user_id, model_type, prediction, probability, risk_level, inference_date, training_data_date)
                    VALUES (?,?,?,?,?,?,?)
                """, row.user_id, row.model_type, int(row.prediction), float(row.probability), row.risk_level, row.inference_date, row.training_data_date)
        logger.info(f"{len(df)} rows saved to {RESULTS_TABLE}")

    # ==================== DAG flow ====================
    data_json = load_training_data()
    model_info = train_model_task(data_json)
    results_json = inference_task(model_info)
    save_results_task(results_json)
