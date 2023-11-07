from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine 
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import pandas as pd

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

def connection():
    hook = MongoHook(conn_id='mongo_conn')
    client = hook.get_conn()
    db = client['prod']
    return db

def get_data_mongo():
    # collection: users
    users_collection = connection().get_collection("users")
    users = users_collection.find()

    return list(users)

def parse_insert_data():
    user_data = get_data_mongo()

    for user in user_data:
        user['_id'] = str(user['_id'])
        if 'userInformation' in user:
            user['userInformation'] = str(user['userInformation'])
        else:
            user['userInformation'] = '' 

    columns = ['_id', 'userInformation', 'username', 'name', 'email', 'email']
    df = pd.DataFrame(list(user_data), columns=columns)

    # parameters = [tuple(row) for row in df.values]

    df.to_csv('/tmp/users.csv', index=False)

def migrate_data(path, db_table):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = create_engine(pg_hook.get_sqlalchemy_engine())
    #engine = create_engine("postgresql://airflow:airflow@postgres:5432/test", echo=True, future=True)
    df = pd.read_csv(path, index_col=False)

    print("<<<<<<<START MIGRATION>>>>>>>")
    df.to_sql(db_table, con=engine, if_exists='replace')
    print("<<<<<<<COMPLETE>>>>>>>")


default_args = {
      'owner': 'airflow',
      'on_failure_callback': on_failure_callback,
      'retries': 5,
      'retry_delay': timedelta(minutes=5)
}

postgres_conn_id = 'warehouse_pg'

with DAG(
    dag_id="dag_with_pg_mongo_data_v01",
    schedule_interval=None,
    start_date=datetime(2023,11,1),
    tags=["reports", "migration"],
    default_args=default_args
) as dag:

    t1 = PostgresOperator(
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
                CREATE UNIQUE INDEX IF NOT EXISTS username_idx ON users (username);
                CREATE UNIQUE INDEX IF NOT EXISTS email_idx ON users (email);
            """
            )

    t2 = PostgresOperator(
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

    t3 = PostgresOperator(
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

    t4 = PythonOperator(
        task_id="create_users_csv",
        python_callable=parse_insert_data,
        dag=dag
    )

    t5 = PythonOperator(
        task_id="insert_data_pg",
        python_callable=migrate_data,
        op_kwargs={
            'path': '/tmp/users.csv',
            'db_table': 'users'
        },
        dag=dag
    ) 

    t1 >> t2 >> t3 >> t4 >> t5
