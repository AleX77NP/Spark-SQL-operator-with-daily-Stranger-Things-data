from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
import requests
import json
from config.dag_config import default_args

BASE_URL = "https://strangerthings-quotes.vercel.app/api/quotes"

dag = DAG(dag_id="stranger_things_quotes", default_args=default_args, schedule="@daily")

quote = {}

response = requests.get(f"{BASE_URL}")
if response.status_code == 200:
    quote = response.json()[0]

query = f"INSERT INTO stranger_things.quotes \
(quote, author) VALUES ({quote['quote']}, {quote['author']})"

quotes_job = SparkSqlOperator(
    sql="SELECT * FROM stranger_things.quotes",
    master="local",
    task_id="quotes_job",
    dag=dag,
)
