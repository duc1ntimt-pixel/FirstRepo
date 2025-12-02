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
MODEL_DIR = "/mnt/models"   # Không có thư mục con /models nữa


def train_rg_model(df: pd.DataFrame) -> dict:
    """
    Train Responsible Gambling (RG) detection model from SQL data.
    Saves both model and preprocessor to shared volume.
    Returns metadata for downstream tasks.
    """
    logger.info(f"Starting RG model training with {len(df):,} records")

    if df.empty:
        logger.error("Received empty DataFrame – aborting training")
        raise ValueError("No training data")

    try:
        # ==================== Feature Engineering ====================
        logger.info("Performing feature engineering...")
        
        df = df.copy()  # avoid SettingWithCopyWarning
        
        # Age & account age
        df['age'] = datetime.now().year - pd.to_datetime(df['birth_year'], errors='coerce').dt.year
        df['account_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['registration_date'], errors='coerce')).dt.days
        
        # Fill missing values early (important for CatBoost + sklearn pipeline)
        df = df.fillna({
            col: 0 for col in df.select_dtypes(include='number').columns
        }).fillna({
            col: 'missing' for col in df.select_dtypes(include=['object', 'category']).columns
        })

        # Target column
        target = 'rg'
        if target not in df.columns:
            logger.error(f"Target column '{target}' not found in data")
            raise KeyError(f"Missing target column: {target}")

        X = df.drop(columns=['user_id', target], errors='ignore')
        y = df[target].astype(int)

        logger.info(f"Feature matrix shape: {X.shape}, Target distribution: {y.value_counts().to_dict()}")

        # ==================== Preprocessing Pipeline ====================
        numeric_features = X.select_dtypes(include='number').columns.tolist()
        categorical_features = X.select_dtypes(exclude='number').columns.tolist()

        logger.info(f"Numeric features ({len(numeric_features)}): {numeric_features[:10]}{'...' if len(numeric_features)>10 else ''}")
        logger.info(f"Categorical features ({len(categorical_features)}): {categorical_features[:10]}{'...' if len(categorical_features)>10 else ''}")

        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numeric_features),
                ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features)
            ],
            remainder='passthrough'
        )

        # ==================== Train / Test Split ====================
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        logger.info(f"Train set: {X_train.shape[0]} samples | Test set: {X_test.shape[0]} samples")

        X_train_processed = preprocessor.fit_transform(X_train)
        X_test_processed = preprocessor.transform(X_test)

        # ==================== Model Training ====================
        scale_pos_weight = (len(y_train) - y_train.sum()) / y_train.sum() if y_train.sum() > 0 else 1.0
        logger.info(f"Using scale_pos_weight = {scale_pos_weight:.2f} to handle class imbalance")

        model = CatBoostClassifier(
            iterations=500,
            learning_rate=0.08,
            depth=7,
            scale_pos_weight=scale_pos_weight,
            random_seed=42,
            verbose=False,
            eval_metric='F1'
        )

        logger.info("Training CatBoost model – training started")
        model.fit(X_train_processed, y_train)
        logger.info("Training completed")

        # ==================== Evaluation ====================
        y_pred = model.predict(X_test_processed)
        y_proba = model.predict_proba(X_test_processed)[:, 1]

        f1 = f1_score(y_test, y_pred)
        auc_pr = average_precision_score(y_test, y_proba)

        logger.info(f"Model evaluation – F1 Score: {f1:.4f} | AUC-PR: {auc_pr:.4f}")

        # ==================== Save Model & Preprocessor ====================
        model_version = f"rg_detection_v{datetime.now().strftime('%Y%m%d_%H%M')}"
        model_path = os.path.join(MODEL_DIR, f"{model_version}.joblib")
        prep_path = os.path.join(MODEL_DIR, f"{model_version}_preprocessor.joblib")

        os.makedirs(MODEL_DIR, exist_ok=True)

        joblib.dump(model, model_path)
        joblib.dump(preprocessor, prep_path)

        logger.info(f"Model artifacts saved")
        logger.info(f"   → Model       : {model_path}")
        logger.info(f"   → Preprocessor: {prep_path}")

        # ==================== Return Metadata ====================
        return {
            "model_type": model_version,
            "row_count": len(df),
            "f1_score": round(float(f1), 4),
            "auc_pr": round(float(auc_pr), 4),
            "status": "success",
            "saved_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.exception("RG model training failed")
        raise