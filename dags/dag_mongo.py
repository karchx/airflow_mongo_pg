from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
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

    parameters = [tuple(row) for row in df.values]
    return parameters
    
with DAG(
    dag_id="dag_load_data_mongo_v01",
    schedule_interval=None,
    start_date=datetime(2023,11,1),
    tags=["reports"],
    default_args={
        "owner": "airflow",
        "on_failure_callback": on_failure_callback
    }
) as dag:

    parameters = parse_insert_data()

    #sql = f"""INSERT INTO users (_id, userInformation, username, name, lastname, email) 
    #        VALUES {'(%s, %s, %s, %s, %s, %s)' * len(parameters)}
    #        """

    t1 = PostgresOperator(
         task_id="insert_data_into_pg",
         sql="""INSERT INTO users (_id, userInformation, username, name, lastname, email) 
             VALUES (%s, %s, %s, %s, %s, %s)
         """,
         postgres_conn_id = 'warehouse_pg',
         parameters=parameters[1]
    )

    t1
