from airflow.decorators import dag, task
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import os
import csv
import datetime
import pandas as pd

from decimal import Decimal
from typing import Optional, Iterable, Dict, Callable
from datetime import date, datetime, timezone, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import task, get_current_context
from airflow.models import Variable

import airflow.providers.amazon.aws.hooks.s3 as s3
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import airflow.providers.postgres.hooks.postgres as postgres


# Define constants
HOOK_MSSQL_AMELIA_DB = 'mssql_dev'
HOOK_S3_G1_DATA_LAKE = 's3_staging'
S3_LOYALTY_DESTINATION_PATH = 'staging-data-loyalty/templace-data-destination'
S3_BUCKER_NAME = Variable.get('s3_data_lake')
TEMP_PATH = Variable.get('temp_path')
#settings = Variable.get("settings", deserialize_json=True)

# Get temp path in local server
#TEMP_PATH = Variable.get('temp_path') + 'loyalty_data_temp/'
TEMP_PATH = '/home/airflow/'

default_args = {
    #'owner': 'duytran',
    #'email': settings['email'],
    #'email_on_failure': True
}

#schedule_interval="0 0 3 1/1 * ? *"
# 0 0 3 1/1 * ? * --- chay vao 3h AM daily

@dag(default_args=default_args, schedule_interval="5 1,6,10,20 * * *", start_date=days_ago(1), catchup=False)
def payment_method_data_loyalty():

    # @task
    # def check_temp_path():
    #     print(f'Tempath: {TEMP_PATH}')
    #     if not os.path.exists(TEMP_PATH):
    #         os.mkdir(TEMP_PATH)

    #     files = os.listdir(TEMP_PATH)
    #     for f in files:
    #         print(f'Found temp file: {f}')
    #         os.remove(os.path.join(TEMP_PATH, f))

    @task
    def init_trans_date() -> Dict[str, date]:
        context = get_current_context()
        dag_run = context["dag_run"]
        print(f"Configuration JSON (Optional): {dag_run.conf}")

        today = datetime.now(timezone.utc) + timedelta(hours=7)
        try:
            p1 = dag_run.conf['trans_date']
            print("Try to get value from parameter: trans_date")

            # Try parse the value
            trans_date = datetime.strptime(p1, '%Y-%m-%d').date()
        except:
            print('Nothing, use default parameters')
            trans_date = today.date()
        print(f'Running time (UTC+7): {today}')
        print(f'trans_date: {trans_date}')

        return {
            'trans_date': f'{trans_date}'
        }

    # extract data
    @task
    def extract_from_sql_server(trans_date_str: str) -> Dict[str, str]:
        # parse trans_date object from parameter
        trans_date = datetime.strptime(trans_date_str, '%Y-%m-%d')
        print(f'trans_date: {trans_date}')

        # file name and it local path
        prefix_name = '{:02d}{:02d}{}'.format(trans_date.year, trans_date.month, trans_date.day)
        file_name = f"Payment_Method_"+prefix_name+'.csv'
        local_full_path = TEMP_PATH + file_name
       
        print("prefix_name : " + prefix_name)
        print("file_name : " + file_name)
        print("local_full_path : " + local_full_path)

        hook = mssql.MsSqlHook(HOOK_MSSQL_AMELIA_DB)
        sql_conn = hook.get_conn()
        query = f'dbVietjet_Data_Collection.dbo.rp_Loyalty_Payment_Method'

        # log the query to console
        print("Query rp_Loyalty_Payment_Method:"+query)
        # run query then export to parquet
        df = pd.read_sql(query, sql_conn)
        # export to csv
        df.to_csv(local_full_path)
        return {
            'file_name': f'{file_name}',
            'local_full_path': f'{local_full_path}',
            'prefix_name': f'{prefix_name}'
        }

    @task
    def upload_to_s3(file_name: str, local_full_path: str) -> str:
        s3_file = S3_LOYALTY_DESTINATION_PATH + f'/{file_name}'
        s3_uri = f's3://{S3_BUCKER_NAME}/{s3_file}'
        print(f's3_file: {s3_file}')
        print(f's3_uri: {s3_uri}')

        hook = s3.S3Hook(HOOK_S3_G1_DATA_LAKE)
        hook.load_file(local_full_path, s3_file,
                       bucket_name=S3_BUCKER_NAME, replace=True)
        return s3_uri

    ############ DAG FLOW ############
    trans_date = init_trans_date()
    local_file = extract_from_sql_server(trans_date['trans_date'])
    upload_to_s3(local_file['file_name'], local_file['local_full_path'])

dag = payment_method_data_loyalty()
