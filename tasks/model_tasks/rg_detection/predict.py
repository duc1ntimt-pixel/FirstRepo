import joblib
import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)
MODEL_DIR = "/mnt/models/models"

def predict_rg_model(model_type: str, df: pd.DataFrame) -> list[dict]:
    if 'user_id' not in df.columns:
        raise KeyError("Input DataFrame must contain 'user_id' column")
    
    try:
        model_path = f"{MODEL_DIR}/{model_type}.joblib"
        prep_path = f"{MODEL_DIR}/{model_type}_preprocessor.joblib"
        model = joblib.load(model_path)
        preprocessor = joblib.load(prep_path)
    except Exception as e:
        logger.exception(f"Failed to load model {model_type}")
        raise

    X = preprocessor.transform(df)
    df['rg_prediction'] = model.predict(X)
    df['rg_probability'] = model.predict_proba(X)[:, 1]

    # Mapping cột để save vào SQL
    df_out = df[['user_id', 'rg_prediction', 'rg_probability']].rename(
        columns={'rg_prediction': 'prediction', 'rg_probability': 'probability'}
    )
    return df_out.to_dict(orient='records')
