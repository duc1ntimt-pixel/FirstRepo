import pandas as pd
import joblib
from utils.logger import get_logger

logger = get_logger(__name__)
MODEL_DIR = "/mnt/models/models"

def predict_rg_model(model_version: str, df: pd.DataFrame) -> list[dict]:
    from catboost import CatBoostClassifier
    if 'user_id' not in df.columns:
        raise KeyError("Input DataFrame must contain 'user_id' column")

    model_path = f"{MODEL_DIR}/{model_version}.cbm"
    prep_path = f"{MODEL_DIR}/{model_version}_prep.joblib"

    model = CatBoostClassifier()
    model.load_model(model_path)
    preprocessor = joblib.load(prep_path)

    X_proc = preprocessor.transform(df)
    df['rg_prediction'] = model.predict(X_proc)
    df['rg_probability'] = model.predict_proba(X_proc)[:, 1]

    df_out = df[['user_id', 'rg_prediction', 'rg_probability']].rename(
        columns={'rg_prediction': 'prediction', 'rg_probability': 'probability'}
    )
    return df_out.to_dict(orient='records')
