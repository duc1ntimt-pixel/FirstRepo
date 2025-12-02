# tasks/model_tasks/rg_detection/predict.py
import joblib
import pandas as pd

MODEL_DIR = "/mnt/models/models"

def predict_rg_model(model_type: str, df: pd.DataFrame) -> pd.DataFrame:
    model_path = f"{MODEL_DIR}/{model_type}.joblib"
    prep_path = f"{MODEL_DIR}/{model_type}_preprocessor.joblib"

    model = joblib.load(model_path)
    preprocessor = joblib.load(prep_path)

    X = preprocessor.transform(df)
    df['rg_prediction'] = model.predict(X)
    df['rg_probability'] = model.predict_proba(X)[:, 1]

    return df[['user_id', 'rg_prediction', 'rg_probability']]