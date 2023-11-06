from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
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

    df = pd.DataFrame(list(user_data))

    df.rename(columns={"_id": "_id", "userInformation": "userInformation", "username": "username", "name": "name", "lastname": "lastname", "email": "email"}, inplace=True)

    print(f"Data frame: {df}")

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
    t1 = PythonOperator(
        task_id="parse_insert_data",
        python_callable=parse_insert_data,
        #op_kwargs={"result": "1"},
        dag=dag
    )

    t1
