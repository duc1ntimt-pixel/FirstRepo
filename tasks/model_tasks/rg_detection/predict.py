import joblib
import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)
MODEL_DIR = "/mnt/models/models"

def predict_rg_model(model_version: str, df: pd.DataFrame) -> list[dict]:
    from catboost import CatBoostClassifier
    if 'user_id' not in df.columns:
        raise KeyError("Input DataFrame must contain 'user_id' column")
    
    try:
        model_path = f"{MODEL_DIR}/{model_version}.cbm"
        prep_path = f"{MODEL_DIR}/{model_version}_prep.joblib"

        # Load model và preprocessor
        model = CatBoostClassifier()
        model.load_model(model_path)
        preprocessor = joblib.load(prep_path)
    except Exception as e:
        logger.exception(f"Failed to load model {model_version}")
        raise

    # Transform dữ liệu
    X = preprocessor.transform(df)
    df['rg_prediction'] = model.predict(X)
    df['rg_probability'] = model.predict_proba(X)[:, 1]

    # Mapping cột để save vào SQL
    df_out = df[['user_id', 'rg_prediction', 'rg_probability']].rename(
        columns={'rg_prediction': 'prediction', 'rg_probability': 'probability'}
    )
    return df_out.to_dict(orient='records')
