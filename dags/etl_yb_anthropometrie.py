import pandas as pd
import numpy as np
import duckdb
import http.client
from datetime import datetime, timedelta
import functions.kdrive_functions as func
import tempfile
import os

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook

# ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")


default_args = {
    'owner': 'MH',
    'retries': 0,
    # You can add more default arguments here as needed
}

# Define Postgres DB connection
postgres_conn = BaseHook.get_connection('postgres-sport-analytics-db')

# Initialize duckdb with postgres connector    
# cursor = duckdb.connect('/opt/airflow/data/mls.db')
cursor = duckdb.connect()
cursor.sql("INSTALL postgres;")
cursor.sql("LOAD postgres;")
cursor.sql(f"ATTACH 'dbname=youthbase user={postgres_conn.login} password={postgres_conn.password} host={postgres_conn.host}' AS postgres_db (TYPE POSTGRES);")

kdrive = func.kDrive()

@dag(
    dag_id="etl_yb_anthro",
    schedule=None,
    catchup=False,
    tags=['YB'],
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)
def LoadMeasurements():
    @task()
    def load_files():
        ids = kdrive.files_in_directory(4777)

        return ids

    @task()
    def get_files(load_files):
        file_ids = load_files
        print(load_files)
        df = pd.DataFrame()
        for id in file_ids:
            input_df = kdrive.read_files('excel', id)

            # Remove first frow which subtitle from dataframe
            input_df = input_df.loc[1:]

            # Unpivot Table to set set a row for a measure
            input_df = pd.melt(input_df, id_vars=input_df.columns[:6], value_vars=input_df.columns[6:])
            input_df.dropna(subset=['value'], inplace=True)
            input_df = input_df[input_df['value'] != 0]
            input_df = input_df[input_df['variable'] != 'Comment']
            input_df['value'] = input_df['value'].astype(float)
            input_df.columns = map(str.lower, input_df.columns)

            # Append raw DataFrame into final DataFrame
            df = pd.concat([df, input_df], ignore_index=True)
            # Filter only goalkeepers
            df = df[df['position'] == 'Goal']
            # convert birthday column to just date
            df['birthday'] = pd.to_datetime(df['birthday']).dt.date
            df.drop_duplicates(inplace=True)

        return df
    
    @task()
    def write_to_db(get_files):
        df = get_files
        table_name = 'postgres_db.anthrophometrie'
        cursor.sql(f"DELETE FROM {table_name};")
        cursor.sql(f"INSERT INTO {table_name} SELECT * FROM df;")
        # cursor.sql(f"DROP TABLE IF EXISTS {table_name};")
        # cursor.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df;")
        print(cursor.sql(f'SELECT count(*) AS total_zeilen FROM {table_name};'))

    get_file_ids = load_files()
    get_file_content = get_files(get_file_ids)
    save_to_db = write_to_db(get_file_content)
    
LoadMeasurements()