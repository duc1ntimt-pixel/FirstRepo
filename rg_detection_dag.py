# /opt/airflow/dags/rg_detection_demo.py
import sys
sys.path.insert(0, "/opt/airflow/dags/repo")

import pandas as pd
import pyodbc
import joblib
import os
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import f1_score, average_precision_score
from tasks.sql_tasks import get_conn_str

CONN_STR = get_conn_str()

MODEL_DIR = "/mnt/models/models"
os.makedirs(MODEL_DIR, exist_ok=True)

# Bảng kết quả
RESULTS_TABLE = "dbo.rg_inference_results"

# ==================== 1. Load data ====================
def load_training_data():
    query = "SELECT user_id, birth_year, registration_date, feature1, feature2, rg FROM dbo.training_rg_data"
    with pyodbc.connect(CONN_STR) as conn:
        df = pd.read_sql(query, conn)
    print(f"Loaded {len(df)} training rows")
    return df

# ==================== 2. Train model ====================
def train_model(df: pd.DataFrame):
    from catboost import CatBoostClassifier
    # Feature engineering
    df['age'] = datetime.now().year - pd.to_datetime(df['birth_year']).dt.year
    df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'])).dt.days
    df = df.fillna(0)
    
    X = df[['feature1','feature2','age','account_age_days']]
    y = df['rg'].astype(int)
    
    # Preprocessor
    numeric_features = X.columns.tolist()
    preprocessor = ColumnTransformer([
        ('num', StandardScaler(), numeric_features)
    ])
    
    X_proc = preprocessor.fit_transform(X)
    
    # Train CatBoost
    scale_pos_weight = (len(y) - y.sum()) / y.sum() if y.sum()>0 else 1.0
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
    
    # Save artifacts
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    model_path = os.path.join(MODEL_DIR, f"rg_model_{ts}.pkl")
    prep_path = os.path.join(MODEL_DIR, f"rg_preprocessor_{ts}.pkl")
    
    joblib.dump(model, model_path)
    joblib.dump(preprocessor, prep_path)
    print(f"Model saved: {model_path}")
    print(f"Preprocessor saved: {prep_path}")
    
    return model, preprocessor, ts

# ==================== 3. Inference ====================
def run_inference(model, preprocessor, model_ts):
    # Load new data (có thể từ cùng bảng training hoặc khác)
    query = "SELECT user_id, birth_year, registration_date, feature1, feature2 FROM dbo.training_rg_data"
    with pyodbc.connect(CONN_STR) as conn:
        df = pd.read_sql(query, conn)
    df['age'] = datetime.now().year - pd.to_datetime(df['birth_year']).dt.year
    df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'])).dt.days
    X = df[['feature1','feature2','age','account_age_days']]
    X_proc = preprocessor.transform(X)
    
    df['prediction'] = model.predict(X_proc)
    df['probability'] = model.predict_proba(X_proc)[:,1]
    
    # Map risk_level
    def risk(prob):
        if prob>=0.8: return 'High'
        elif prob>=0.5: return 'Medium'
        else: return 'Low'
    df['risk_level'] = df['probability'].apply(risk)
    
    df['model_type'] = model_ts
    df['training_data_date'] = datetime.now().date()
    df['inference_date'] = datetime.now()
    
    return df[['user_id','model_type','prediction','probability','risk_level','inference_date','training_data_date']]

# ==================== 4. Save results ====================
def save_results(df):
    with pyodbc.connect(CONN_STR, autocommit=True) as conn:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {RESULTS_TABLE} 
                (user_id, model_type, prediction, probability, risk_level, inference_date, training_data_date)
                VALUES (?,?,?,?,?,?,?)
            """, row.user_id, row.model_type, int(row.prediction), float(row.probability), row.risk_level, row.inference_date, row.training_data_date)
    print(f"{len(df)} inference rows saved to {RESULTS_TABLE}")

# ==================== 5. Run pipeline ====================
if __name__=="__main__":
    df_train = load_training_data()
    model, preprocessor, ts = train_model(df_train)
    df_infer = run_inference(model, preprocessor, ts)
    save_results(df_infer)
