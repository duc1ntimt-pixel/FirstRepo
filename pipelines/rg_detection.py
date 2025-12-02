PIPELINE = {
    "model_name": "rg_detection",
    "load_query": "SELECT user_id, birth_year, registration_date, feature1, feature2, rg FROM training_rg_data",  # replace bằng query thật
    "results_table": "dbo.rg_inference_results",
    "results_schema": """
        inference_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
        user_id             BIGINT NOT NULL,
        model_type          VARCHAR(100) NOT NULL,
        prediction          INT NOT NULL,
        probability         FLOAT NOT NULL,
        risk_level          VARCHAR(20) NULL,
        inference_date      DATETIME2 DEFAULT GETUTCDATE(),
        training_data_date  DATE NULL,
        f1_score            FLOAT NULL,
        auc_pr              FLOAT NULL,
        processed_at        DATETIME2 DEFAULT GETUTCDATE()
    """,
    "results_indexes": [
        "CREATE INDEX IX_user_id ON dbo.rg_inference_results(user_id)",
        "CREATE INDEX IX_model_type ON dbo.rg_inference_results(model_type)",
        "CREATE INDEX IX_inference_date ON dbo.rg_inference_results(inference_date)"
    ],
    "api_endpoint": "http://192.168.100.117:30082/predict",
    "etl_steps": []
}
