import os
import pandas as pd
import joblib
from datetime import datetime
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, average_precision_score
from utils.logger import get_logger

logger = get_logger(__name__)
MODEL_DIR = "/mnt/models/models"

def train_rg_model(df: pd.DataFrame) -> dict:
    from catboost import CatBoostClassifier
    logger.info(f"Starting RG training with {len(df)} records")
    if df.empty:
        raise ValueError("No training data")

    df = df.copy()
    df['age'] = datetime.now().year - pd.to_datetime(df['birth_year'], errors='coerce').dt.year
    df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'], errors='coerce')).dt.days

    df = df.fillna({col: 0 for col in df.select_dtypes(include='number').columns})
    df = df.fillna({col: 'missing' for col in df.select_dtypes(include=['object', 'category']).columns})

    target = 'rg'
    X = df.drop(columns=['user_id', target], errors='ignore')
    y = df[target].astype(int)

    numeric_features = X.select_dtypes(include='number').columns.tolist()
    categorical_features = X.select_dtypes(exclude='number').columns.tolist()

    preprocessor = ColumnTransformer([
        ('num', StandardScaler(), numeric_features),
        ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features)
    ], remainder='passthrough')

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    X_train_proc = preprocessor.fit_transform(X_train)
    X_test_proc = preprocessor.transform(X_test)

    scale_pos_weight = (len(y_train) - y_train.sum()) / y_train.sum() if y_train.sum() > 0 else 1.0

    model = CatBoostClassifier(
        iterations=100, learning_rate=0.1, depth=4,
        scale_pos_weight=scale_pos_weight, random_seed=42,
        verbose=False, eval_metric='F1'
    )
    model.fit(X_train_proc, y_train)

    y_pred = model.predict(X_test_proc)
    y_proba = model.predict_proba(X_test_proc)[:, 1]

    f1 = f1_score(y_test, y_pred)
    auc_pr = average_precision_score(y_test, y_proba)

    os.makedirs(MODEL_DIR, exist_ok=True)
    model_version = f"rg_demo_v{datetime.now().strftime('%Y%m%d_%H%M')}"
    model_path = os.path.join(MODEL_DIR, f"{model_version}.cbm")
    prep_path = os.path.join(MODEL_DIR, f"{model_version}_prep.joblib")

    model.save_model(model_path)
    joblib.dump(preprocessor, prep_path)

    return {
        "model_version": model_version,
        "row_count": len(df),
        "f1_score": round(float(f1), 4),
        "auc_pr": round(float(auc_pr), 4)
    }
