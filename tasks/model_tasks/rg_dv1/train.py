import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime
from sklearn.pipeline import Pipeline
from catboost import CatBoostClassifier
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import classification_report, f1_score, average_precision_score
from sqlalchemy import create_engine
from tasks.sql_tasks import get_PostgreSQL_conn_str
from dotenv import load_dotenv
load_dotenv()

import logging
logger = logging.getLogger("airflow.task")

db_url =get_PostgreSQL_conn_str()
def load_data_from_csv():
    base = "/opt/airflow/dags/repo/tasks/model_tasks/rg_dv1/data"
    df_demo = pd.read_csv(base + 'demographic.csv').head(10)
    df_gambling = pd.read_csv(base + 'gambling.csv').head(10)
    df_rg = pd.read_csv(base + 'rg_information.csv').head(10)

    # df_demo = pd.read_csv(r'/opt/airflow/dags/repo/tasks/model_tasks/rg_dv1/data/demographic.csv')
    # df_gambling = pd.read_csv(r'/opt/airflow/dags/repo/tasks/model_tasks/rg_dv1/data/gambling.csv')
    # df_rg = pd.read_csv(r'/opt/airflow/dags/repo/tasks/model_tasks/rg_dv1/data/rg_information.csv')
    # df_demo, df_gambling, df_rg = df_demo.head(10), df_gambling.head(10), df_rg.head(10)
    return df_demo, df_gambling, df_rg

def load_data_from_postgre():
    
    engine = create_engine(db_url)

    df_demo = pd.read_sql("SELECT * FROM demographic", engine)
    df_gambling = pd.read_sql("SELECT * FROM gambling", engine)
    df_rg = pd.read_sql("SELECT * FROM rg_information", engine)

    return df_demo, df_gambling, df_rg

def insert_data_to_postgre(df: pd.DataFrame, table_name: str, if_exists="append"):
    try:
        engine = create_engine(db_url)

        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,   # append / replace / fail
            index=False,
            method="multi",
            chunksize=5000
        )

        print(f"Inserted {len(df)} rows into '{table_name}'")

    except Exception as e:
        print(f"Insert failed for '{table_name}': {e}")

def load_and_insert():
    print("Loading CSV files...")
    df_demo, df_gambling, df_rg = load_data_from_csv()

    print("Inserting to PostgreSQL...")
    insert_data_to_postgre(df_demo, "demographic")
    insert_data_to_postgre(df_gambling, "gambling")
    insert_data_to_postgre(df_rg, "rg_information")

    print("🎉 All tables inserted successfully.")

    df_demo, df_gambling, df_rg = load_data_from_postgre()
    print("\n=== demographic (df_demo) head ===")
    print(df_demo.head())

    print("\n=== gambling (df_gambling) head ===")
    print(df_gambling.head())

    print("\n=== rg_information (df_rg) head ===")
    print(df_rg.head())

def train():
    print("Loading data...")
    try:
        df_demo, df_gambling, df_rg = load_data_from_postgre()
    except Exception as e:
        print(f"Failed to load from CSV: {e}.")
        # Fallback logic or exit
        return

    print("Feature Engineering...")
    # Convert dates
    df_gambling['date'] = pd.to_datetime(df_gambling['date'])
    df_demo['registration_date'] = pd.to_datetime(df_demo['registration_date'])

    # Rolling Features
    df_gambling = df_gambling.sort_values(by=['user_id', 'date'])
    features_to_roll = ['turnover', 'hold', 'num_bets']

    # Base aggregation
    df_agg = df_gambling.groupby('user_id').agg(
        total_turnover=('turnover', 'sum'),
        mean_turnover=('turnover', 'mean'),
        total_hold=('hold', 'sum'),
        mean_hold=('hold', 'mean'),
        total_bets=('num_bets', 'sum'),
        mean_bets=('num_bets', 'mean'),
        first_bet_date=('date', 'min'),
        last_bet_date=('date', 'max')
    ).reset_index()

    df_agg['days_active'] = (df_agg['last_bet_date'] - df_agg['first_bet_date']).dt.days + 1
    latest_date = df_gambling['date'].max()
    df_agg['days_since_last_bet'] = (latest_date - df_agg['last_bet_date']).dt.days

    # Rolling windows
    windows = [7, 30]
    df_g_indexed = df_gambling.set_index('date').groupby('user_id')[features_to_roll]

    all_features = [df_agg]

    for window in windows:
        window_str = f'{window}D'
        df_roll = df_g_indexed.rolling(window_str)

        df_std = df_roll.std().rename(columns=lambda x: f'rolling_{window}d_std_{x}').groupby('user_id').mean()
        df_mean = df_roll.mean().rename(columns=lambda x: f'rolling_{window}d_mean_{x}').groupby('user_id').mean()

        all_features.extend([df_std, df_mean])

    df_features = all_features[0]
    for f in all_features[1:]:
        df_features = df_features.merge(f, on='user_id', how='left')

    # Merge with demographic
    rg_users = df_rg['user_id'].unique()
    df_demo['rg'] = df_demo['user_id'].apply(lambda x: 1 if x in rg_users else 0)

    df_model = df_demo.merge(df_features, on='user_id', how='left')

    # Fill NaNs
    df_model['age'] = datetime.now().year - df_model['birth_year']
    df_model['account_age_days'] = (latest_date - df_model['registration_date']).dt.days
    df_model = df_model.fillna(0)

    # Imbalance handling (Undersampling majority to 97/3 ratio as per notebook)
    df_minority = df_model[df_model['rg'] == 1]
    df_majority = df_model[df_model['rg'] == 0]

    # If we have enough data
    if len(df_minority) > 0:
        n_majority = int(len(df_minority) * (97/3))
        if len(df_majority) > n_majority:
            df_majority = df_majority.sample(n=n_majority, random_state=42)

        df_balanced = pd.concat([df_majority, df_minority]).sample(frac=1, random_state=42)
    else:
        df_balanced = df_model # Fallback

    # Prepare X, y
    target = 'rg'
    drop_cols = ['user_id', 'rg', 'birth_year', 'registration_date', 'first_deposit_date', 'first_bet_date', 'last_bet_date']
    X = df_balanced.drop(columns=drop_cols, errors='ignore')
    y = df_balanced[target]

    # Preprocessing Pipeline
    numeric_features = X.select_dtypes(include=np.number).columns.tolist()
    categorical_features = X.select_dtypes(exclude=np.number).columns.tolist()

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features)
        ])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # Fit Preprocessor
    X_train_processed = preprocessor.fit_transform(X_train)
    X_test_processed = preprocessor.transform(X_test)

    # Train CatBoost
    print("Training Model...")
    scale_pos_weight = len(y_train[y_train==0]) / len(y_train[y_train==1]) if len(y_train[y_train==1]) > 0 else 1

    model = CatBoostClassifier(
        iterations=200,
        learning_rate=0.1,
        depth=6,
        scale_pos_weight=scale_pos_weight,
        verbose=False
    )

    model.fit(X_train_processed, y_train)

    # Evaluate
    y_pred = model.predict(X_test_processed)
    y_proba = model.predict_proba(X_test_processed)[:, 1]

    f1 = f1_score(y_test, y_pred)
    auc_pr = average_precision_score(y_test, y_proba)

    print(f"F1 Score: {f1}")
    print(f"AUC-PR: {auc_pr}")

    # Save features list for API
    features_info = {
        'numeric_features': numeric_features,
        'categorical_features': categorical_features,
        'all_features': numeric_features + categorical_features,
        'best_threshold': 0.5 # Could be optimized
    }

    # Save Artifacts
    os.makedirs("artifacts", exist_ok=True)

    joblib.dump(model, "artifacts/catboost_rg_model.joblib")
    joblib.dump(preprocessor, "artifacts/preprocessor.joblib")
    joblib.dump(features_info, "artifacts/model_features.joblib")

    print("Training complete. Artifacts saved.")

# if __name__ == "__main__":
#     load_and_insert()
#     # train()