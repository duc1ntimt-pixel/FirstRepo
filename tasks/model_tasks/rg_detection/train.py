# /opt/airflow/dags/repo/tasks/model_tasks/rg_detection/train.py

from utils.logger import get_logger
import pandas as pd
import joblib
import os
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import f1_score, average_precision_score
from catboost import CatBoostClassifier

logger = get_logger(__name__)

# ĐỔI LẠI ĐÚNG ĐƯỜNG DẪN MÀ FASTAPI ĐANG NHÌN
MODEL_DIR = "/mnt/models/models"   # Không có thư mục con /models nữa


def train_rg_model(df: pd.DataFrame) -> dict:
    if df.empty:
        raise ValueError("No data")

    # Feature engineering đơn giản
    df['age'] = datetime.now().year - pd.to_datetime(df['birth_year']).dt.year
    df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'])).dt.days
    df = df.fillna(0)

    X = df.drop(columns=['user_id','rg'])
    y = df['rg'].astype(int)

    # Preprocessing
    numeric_features = X.select_dtypes(include='number').columns.tolist()
    categorical_features = X.select_dtypes(exclude='number').columns.tolist()
    preprocessor = ColumnTransformer([
        ('num', StandardScaler(), numeric_features),
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features)
    ])

    X_processed = preprocessor.fit_transform(X)

    # Train model
    model = CatBoostClassifier(iterations=50, learning_rate=0.1, verbose=False)
    model.fit(X_processed, y)

    # Save artifacts
    os.makedirs(MODEL_DIR, exist_ok=True)
    version = f"demo_rg_{datetime.now().strftime('%H%M%S')}"
    model.save_model(os.path.join(MODEL_DIR, f"{version}.cbm"))
    joblib.dump(preprocessor, os.path.join(MODEL_DIR, f"{version}_prep.joblib"))

    return {"model_version": version, "rows": len(df), "status": "success"}
