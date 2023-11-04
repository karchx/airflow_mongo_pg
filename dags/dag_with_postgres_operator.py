from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
      'owner': 'airflow',
      'retries': 5,
      'retry_delay': timedelta(minutes=5)
}

postgres_conn_id = 'warehouse_pg'

with DAG(
      dag_id='dag_with_postgres_operator_v01',
      default_args=default_args,
      start_date=datetime(2023, 11, 1)
) as dag:
    task1 = PostgresOperator(
            task_id='create_table_postgres_user',
            postgres_conn_id=postgres_conn_id,
            sql="""
                CREATE TABLE IF NOT EXISTS users(
                  _id VARCHAR,
                  userInformation VARCHAR,
                  username VARCHAR(20),
                  name VARCHAR(50),
                  lastname VARCHAR(50),
                  email VARCHAR(100),
                  primary key (_id, userInformation)
                );
                CREATE UNIQUE INDEX username_idx ON users (username);
                CREATE UNIQUE INDEX email_idx ON users (email);
            """
            )

    task2 = PostgresOperator(
            task_id='create_table_postgres_platforms',
            postgres_conn_id=postgres_conn_id,
            sql="""
                CREATE TABLE IF NOT EXISTS platforms(
                    id SERIAL PRIMARY KEY,
                    _id VARCHAR,
                    name VARCHAR(50),
                    email VARCHAR(100)
                );
            """
            )

    task3 = PostgresOperator(
            task_id="create_table_postgres_platforms_query",
            postgres_conn_id=postgres_conn_id,
            sql="""
                CREATE TABLE IF NOT EXISTS query_platform (
                    id SERIAL PRIMARY KEY,
                    platform_id INTEGER,
                    state VARCHAR(250),
                    profession VARCHAR,
                    city VARCHAR(100),
                    CONSTRAINT fk_platform FOREIGN KEY(platform_id) REFERENCES platforms(id)
                );
            """
            )

    task1 >> task2 >> task3
