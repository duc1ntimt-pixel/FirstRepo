from airflow.decorators import task
import requests
import json
import time

@task()
def trigger_api(json_data, endpoint, max_wait=600, interval=5):
    """
    json_data: JSON data from ETL
    endpoint: FastAPI prediction endpoint
    max_wait: max wait time in seconds (default 10 minutes)
    interval: time between retries
    """
    data = json.loads(json_data)
    results = []

    start = time.time()

    while True:
        try:
            response = requests.post(endpoint, json=data, timeout=10)
            rjson = response.json()

            # 🎯 CASE 1: Model chưa load → retry
            if isinstance(rjson, dict) and rjson.get("error") == "Model not loaded yet":
                print("⏳ Model not loaded yet → waiting...")
                
                if time.time() - start > max_wait:
                    raise Exception("⛔ Timeout waiting for model to load")

                time.sleep(interval)
                continue

            # 🎯 CASE 2: Predict OK
            response.raise_for_status()
            print("🚀 Prediction success!")
            results.append({
                "status": "success",
                "response": rjson
            })
            break

        except Exception as e:
            print(f"⚠️ API error: {e}")

            if time.time() - start > max_wait:
                results.append({
                    "status": "failed",
                    "error": str(e)
                })
                break

            print("⏳ Retrying...")
            time.sleep(interval)

    return json.dumps(results)
