# /opt/airflow/dags/rg_detection_demo_dag.py
import sys
sys.path.insert(0, "/opt/airflow/dags/repo")

import os
import joblib
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import pendulum
from tasks.sql_tasks import get_conn_str

CONN_STR = get_conn_str()
MODEL_DIR = "/mnt/models/models"
os.makedirs(MODEL_DIR, exist_ok=True)
RESULTS_TABLE = "dbo.rg_inference_results"


# ==================== DAG Definition ====================
with DAG(
    dag_id="rg_detection_demo",
    start_date=pendulum.datetime(2025, 12, 4, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["rg", "demo", "catboost"]
) as dag:

    @task()
    def load_data_task():
        import pyodbc
        query = "SELECT user_id, birth_year, registration_date, feature1, feature2, rg FROM dbo.training_rg_data"
        with pyodbc.connect(CONN_STR) as conn:
            df = pd.read_sql(query, conn)
        print(f"Loaded {len(df)} rows")
        return df.to_json(orient='records')

    @task()
    def train_task(data_json: str):
        import pandas as pd
        from catboost import CatBoostClassifier
        from sklearn.preprocessing import StandardScaler
        from sklearn.compose import ColumnTransformer

        df = pd.read_json(data_json)
        df['age'] = datetime.now().year - pd.to_datetime(df['birth_year']).dt.year
        df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'])).dt.days
        df = df.fillna(0)

        X = df[['feature1','feature2','age','account_age_days']]
        y = df['rg'].astype(int)

        numeric_features = X.columns.tolist()
        preprocessor = ColumnTransformer([
            ('num', StandardScaler(), numeric_features)
        ])
        X_proc = preprocessor.fit_transform(X)

        scale_pos_weight = (len(y)-y.sum())/y.sum() if y.sum()>0 else 1.0
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
        print(f"Model saved: {model_path}")
        print(f"Preprocessor saved: {prep_path}")

        return {"model_ts": ts}

    @task()
    def inference_task(model_info: dict, data_json: str):
        import pandas as pd
        import joblib

        df = pd.read_json(data_json)
        model_ts = model_info["model_ts"]
        model_path = os.path.join(MODEL_DIR, f"rg_model_{model_ts}.pkl")
        prep_path = os.path.join(MODEL_DIR, f"rg_preprocessor_{model_ts}.pkl")
        model = joblib.load(model_path)
        preprocessor = joblib.load(prep_path)

        df['age'] = datetime.now().year - pd.to_datetime(df['birth_year']).dt.year
        df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'])).dt.days
        X = df[['feature1','feature2','age','account_age_days']]
        X_proc = preprocessor.transform(X)

        df['prediction'] = model.predict(X_proc)
        df['probability'] = model.predict_proba(X_proc)[:,1]

        def risk(prob):
            if prob>=0.8: return 'High'
            elif prob>=0.5: return 'Medium'
            else: return 'Low'
        df['risk_level'] = df['probability'].apply(risk)
        df['model_type'] = model_ts
        df['training_data_date'] = datetime.now().date()
        df['inference_date'] = datetime.now()

        return df[['user_id','model_type','prediction','probability','risk_level','inference_date','training_data_date']].to_json(orient='records')

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
                """, row.user_id, row.model_type, int(row.prediction), float(row.probability),
                     row.risk_level, row.inference_date, row.training_data_date)
        print(f"{len(df)} inference rows saved to {RESULTS_TABLE}")

    # ==================== DAG flow ====================
    data = load_data_task()
    model_info = train_task(data)
    results = inference_task(model_info, data)
    save_results_task(results)
