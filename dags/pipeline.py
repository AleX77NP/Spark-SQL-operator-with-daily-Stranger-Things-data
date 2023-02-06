from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
import requests
from config.dag_config import default_args

BASE_URL = "https://strangerthings-quotes.vercel.app/api/quotes"

dag = DAG(dag_id="stranger_things_quotes", default_args=default_args, schedule="@daily")

query = "SELECT author from stranger_things.quotes"

# fetch new daily quote
response = requests.get(f"{BASE_URL}")
if response.status_code == 200:
    quote = response.json()[
        0
    ]  # if we get new quote, make query to insert it, otherwise retrieve authors
    query = f"""
        INSERT INTO stranger_things.quotes PARTITION(author="{quote['author']}") 
        VALUES ("{quote['quote']}")
    """

quotes_job = SparkSqlOperator(
    sql=query,
    master="local",
    task_id="quotes_job",
    dag=dag,
)

quotes_count_job = SparkSqlOperator(
    sql="SELECT COUNT(quote), author FROM stranger_things.quotes GROUP BY author",
    master="local",
    task_id="quotes_count_job",
    dag=dag,
)

quotes_job >> quotes_count_job
