from airflow.decorators import task
import pandas as pd
import json

@task()
def run_etl_steps(raw_json, etl_steps=None):
    df = pd.read_json(raw_json)
    if etl_steps:
        for step in etl_steps:
            df = step(df)
    print(f"✅ ETL done, {len(df)} rows after transformation")
    return df.to_json(orient="records")
