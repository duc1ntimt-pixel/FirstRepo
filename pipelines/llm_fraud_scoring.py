PIPELINE = {
    "load_query": "SELECT TOP 5 * FROM [dbo].[asdx-demo-db-llm_daily]",
    "api_endpoint": "http://xxx:8000/run",
    "results_table": "[dbo].[asdx-demo-db-llm_daily_results_as-is]",
    "etl_steps": [
        # Example ETL step: convert Date column to int
        lambda df: df.assign(Date=df["Date"].astype(int)),
        # Example ETL step: drop duplicate Customer_ID + Date
        lambda df: df.drop_duplicates(subset=["Date", "Customer_ID"])
    ],
    # Optional override logic (can be None)
    "override_logic": None
}
