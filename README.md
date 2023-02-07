# Daily pipeline with Apache Airflow's Spark SQL Operator

Spark SQL Operator is a really cool tool when used correctly. It makes writing Spark SQL 
queries against Hive Metastore look pretty and clean.

If you wanna learn more about Spark and Hive connection, I suggest reading this article:
https://medium.com/@sarfarazhussain211/metastore-in-apache-spark-9286097180a4

## Example

In this example we use Spark SQL operator to insert and read data on daily basis.
We will use Stranger Things API (that's right, Netflix and ST fans!) to fetch random quote per day and insert it in Hive Metastore. 

Every quote has two fileds - author and quote (text). Author can have more than one quote associated with him.

For this to work, we need two DAGs (you can find them in `dags/` directory):
1. `db_setup` - to create database and quotes table (this dag is triggered only once)
2. `pipeline` - to fetch quote, insert it and show how many quotes every author has

Notice `PARTITIONED BY` clause in our SQL statement for creating table (`db_setup.py`). This is a handy Hive feature that lets us choose how data is partitioned on the disk. In this case, we partition by author, since many quotes are bound to one author.

`CREATE TABLE stranger_things.quotes (quote STRING) PARTITIONED BY(author STRING)`

Our second DAG has two tasks. It fetches one quote from Stranger Things api and inserts it into database.
After that, we run another SQL query to get number of quotes for each author.

    `INSERT INTO stranger_things.quotes PARTITION(author="{quote['author']}") 
    VALUES ("{quote['quote']}")`


`SELECT COUNT(DISTINCT quote), author FROM stranger_things.quotes GROUP BY author`

Both of these queries are run using Spark SQL operator. With this, we don't have to worry about
complex configuration - we only need to pass SQL string that represents query and master field (local in this case).


And that's it!
If you go to Airflow and check DAG logs, you should see something like this:

```[2023-02-06, 14:51:41 UTC] {spark_sql.py:177} INFO - 1	Kali Prasad

[2023-02-06, 14:51:41 UTC] {spark_sql.py:177} INFO - 1	Eleven

[2023-02-06, 14:51:41 UTC] {spark_sql.py:177} INFO - 1	Lucas Sinclair

[2023-02-06, 14:51:41 UTC] {spark_sql.py:177} INFO - 1	Will Byers

[2023-02-06, 14:51:41 UTC] {spark_sql.py:177} INFO - 2	Dustin Henderson
```
