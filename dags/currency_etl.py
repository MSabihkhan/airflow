
from datetime import datetime, timedelta, timezone

import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract(**context):
    url = "https://api.frankfurter.dev/v2/rates"

    params = {
        "base": "EUR",
        "quotes": "USD,GBP,JPY"
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )
    response.raise_for_status()

    data = response.json()

    context["ti"].xcom_push(
        key="raw_rates",
        value=data
    )

    print("Extracted:", data)

def transform(**context):
    data = context["ti"].xcom_pull(
        task_ids="extract",
        key="raw_rates"
    )

    if not data:
        raise ValueError("No exchange rates received")

    transformed = []

    for item in data:
        record = {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": float(item["rate"]),
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        transformed.append(record)

    context["ti"].xcom_push(
        key="transformed_rates",
        value=transformed
    )

    print("Transformed:", transformed)


def load(**context):
    records = context["ti"].xcom_pull(
        task_ids="transform",
        key="transformed_rates"
    )

    if not records:
        raise ValueError("No records to load")

    client = MongoClient(
        "mongodb://mongo:27017/",
        serverSelectionTimeoutMS=10000
    )

    try:
        db = client["currency_db"]
        collection = db["exchange_rates"]

        for record in records:
            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"]
                },
                {"$set": record},
                upsert=True
            )

        print("Successfully loaded", len(records), "records")
    finally:
        client.close()


with DAG(
    dag_id="currency_etl_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1)
    },
    tags=["ETL", "Currency", "MongoDB"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task
