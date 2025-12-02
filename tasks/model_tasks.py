from airflow.decorators import task
import pandas as pd
import pickle
from sklearn.linear_model import LogisticRegression
import json

@task()
def train_model(transformed_json):
    df = pd.read_json(transformed_json)
    if "label" not in df.columns:
        print("⚠️ No label column, skipping training")
        return transformed_json

    X = df.drop("label", axis=1)
    y = df["label"]

    model = LogisticRegression()
    model.fit(X, y)

    with open("/opt/airflow/models/latest_model.pkl", "wb") as f:
        pickle.dump(model, f)
    print("✅ Model trained and saved")
    return transformed_json
