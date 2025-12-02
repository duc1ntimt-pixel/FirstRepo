PIPELINE = {
    "model_name": "rg_detection",
    "load_query": "SELECT ... FROM training_rg_data ...",
    "results_table": "dbo.rg_inference_results",           # ← bảng riêng
    "results_schema": """
        inference_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
        user_id             BIGINT NOT NULL,
        model_type          VARCHAR(100) NOT NULL,
        prediction          INT NOT NULL,                    -- 0/1
        probability         FLOAT NOT NULL,
        risk_level          VARCHAR(20) NULL,                -- 'Low', 'Medium', 'High'
        inference_date      DATETIME2 DEFAULT GETUTCDATE(),
        training_data_date  DATE NULL,
        f1_score            FLOAT NULL,
        auc_pr              FLOAT NULL,
        processed_at        DATETIME2 DEFAULT GETUTCDATE()
    """,
    "results_indexes": [
        "CREATE INDEX IX_user_id ON dbo.rg_inference_results(user_id),
        CREATE INDEX IX_model_type ON dbo.rg_inference_results(model_type),
        CREATE INDEX IX_inference_date ON dbo.rg_inference_results(inference_date)
    ],
    "api_endpoint": "http://192.168.100.117:30082/predict",
    "etl_steps": [  # nếu cần xử lý thêm sau khi load từ SQL
        # ví dụ: đổi tên cột, fillna, tính age...
    ]
}