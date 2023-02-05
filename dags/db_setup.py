from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from config.dag_config import default_args

dag = DAG(dag_id="db_setup", default_args=default_args, schedule="@once")

create_db_job = SparkSqlOperator(
    sql="CREATE DATABASE IF NOT EXISTS stranger_things",
    master="local",
    task_id="create_db_job",
    dag=dag,
)

create_table_job = SparkSqlOperator(
    sql="CREATE TABLE stranger_things.quotes (quote STRING) PARTITIONED BY(author STRING)",
    master="local",
    task_id="create_table_job",
    dag=dag,
)

create_db_job >> create_table_job
