import pandas as pd
import numpy as np
import duckdb
import http.client
from datetime import datetime, timedelta
import functions.fbref_functions as func
import tempfile
import os

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

# ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")


default_args = {
    'owner': 'MH',
    'start_date': datetime(2024, 4, 25),
    'retries': 0
    # You can add more default arguments here as needed
}

@dag(
    dag_id="etl_euro2024",
    start_date=datetime(2023, 10, 2),
    schedule="0 5 * * 1",
    catchup=False,
    default_args=default_args,
)
def ProcessScores():
    # Define Postgres DB connection
    postgres_conn = BaseHook.get_connection('postgres_integrated')
    
    # Define drive connection
    drive_conn = BaseHook.get_connection('kdrive')
    headers = {
    'Authorization': 'Bearer ' + drive_conn.password,
    'Content-Type': 'application/octet-stream',
    }

    # Initialize duckdb with postgres connector    
    cursor = duckdb.connect()
    cursor.sql("INSTALL postgres;")
    cursor.sql("LOAD postgres;")
    cursor.sql(f"ATTACH 'dbname=football user={postgres_conn.login} password={postgres_conn.password} host={postgres_conn.host}' AS postgres_db (TYPE POSTGRES);")

    # Define competition url and name    
    competition_url = 'https://fbref.com/en/comps/676/history/European-Championship-Seasons'
    fb_stats = func.fbrefStats(competition_url)
    
    # 1. task to load data from the URL into a duckdb table
    @task()
    def extract_fixtures():

        df = fb_stats.get_fixtures()

        custom_file_name = 'euro2024_fixtures.csv'
        
        with tempfile.NamedTemporaryFile(mode='w', delete=True, prefix=custom_file_name) as temp:
            df.to_csv(temp.name, index=False)

            with open(temp.name, 'rb') as f:
                file = f.read()
            file_size = os.path.getsize(temp.name)

            print('Filename: ' + temp.name)
            print('Filesize: ' + str(file_size))

            conn = http.client.HTTPSConnection(drive_conn.host)
            conn.request("POST", f'{drive_conn.schema}/upload?total_size={file_size}&directory_id=5385&file_name={custom_file_name}&conflict=version', file, headers)
            res = conn.getresponse()
            data = res.read()
            print(data.decode("utf-8"))

        return df
    
    @task()
    def load_teams(extract_fixtures):
        df = extract_fixtures.loc[:8]
        teams = fb_stats.get_teams(df)

        table_name = 'postgres_db.teams'
        cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM teams;")        

    @task()
    def load_fixtures(extract_fixtures):
        df = extract_fixtures
        table_name = 'postgres_db.fixtures'
        cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df;")
        new_matches = cursor.sql(f"SELECT * FROM {table_name} WHERE inserted_timestamp = (SELECT max(inserted_timestamp) FROM {table_name});").df()
        print(cursor.sql('SELECT count(*) FROM new_matches;'))
        
        return df
    
    @task()
    def cleanse_scores(load_fixtures):
        scores_df = fb_stats.transform_scores(load_fixtures)

        table_name = 'postgres_db.scores'
        # cursor.sql("DROP TABLE IF EXISTS postgres_db.fixtures;")
        cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM scores_df;")

        return scores_df


    @task()
    def get_match_details(load_fixtures):
        df = load_fixtures
        match_details = fb_stats.get_match_details(df)

        return match_details

    @task()
    def load_gk_stats(get_match_details):
        gk_stats = get_match_details[1]
        
        table_name = 'postgres_db.goalkeeper_stats'
        #cursor.sql(f"SELECT * FROM {table_name} UNION SELECT * FROM gk_stats;")
        cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM gk_stats;")

        print(cursor.sql(f'SELECT count(*) AS total_zeilen FROM {table_name};'))
  
    @task()
    def load_shots(get_match_details):
        shots = get_match_details[0]

        table_name = 'postgres_db.shots'
        #cursor.sql(f"SELECT * FROM {table_name} UNION SELECT * FROM shots;")
        cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM shots;")

        print(cursor.sql(f'SELECT count(*) AS total_zeilen FROM {table_name};'))


    get_data = extract_fixtures()
    insert_teams = load_teams(get_data)
    insert_fixtures = load_fixtures(get_data)
    clean_scores = cleanse_scores(insert_fixtures)
    clean_matchdetails = get_match_details(insert_fixtures)
    insert_gk_stats = load_gk_stats(clean_matchdetails)
    insert_shots = load_shots(clean_matchdetails)

ProcessScores()