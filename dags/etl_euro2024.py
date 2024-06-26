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
    'owner': 'me',
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
    comp_name = 'Euro 2024'
    
    # 1. task to load data from the URL into a duckdb table
    @task()
    def extract_fixtures():

        df = func.get_fixtures(competition_url, comp_name)

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
            print(df.head())
            print(df.columns)

        return df

    @task()
    def cleanse_fixtures(extract_fixtures):
        scores_df = func.transform_scores(extract_fixtures)

        return scores_df

    @task()
    def load_fixtures(cleanse_fixtures):
        df = cleanse_fixtures
        cursor.sql("DROP TABLE IF EXISTS postgres_db.euro2024_fixtures;")
        cursor.sql("CREATE TABLE postgres_db.euro2024_fixtures AS SELECT * FROM df;")

        print(cursor.sql('SELECT * FROM df LIMIT 5;'))

    @task()
    def get_gk_stats(cleanse_fixtures):
        df = cleanse_fixtures
        match_details = func.get_match_details(df)[1]

        cursor.sql("CREATE TABLE postgres_db.euro2024_goalkeeper_stats AS SELECT * FROM match_details;")

        print(cursor.sql('SELECT * FROM match_details LIMIT 5;'))
  
    @task()
    def load_shots(cleanse_fixtures):
        df = cleanse_fixtures
        shots = func.get_match_details(df)[0]

        # Remove Added time from the Minute column 45/90 is the max
        shots['minute'] = [x[0] for x in shots['minute'].astype(str).str.split('+')]
        shots['minute'] = shots['minute'].astype(float).astype(int)

                # Remove Penalty note from Player and add to Notes column
        notes_list = []
        for i in range(len(df)):
            if df.loc[i]['Player'].rsplit("(")[-1] == 'pen)':
                notes_list.append('Penalty')
            else:
                notes_list.append(df.loc[i]['Notes'])

        df['Notes'] = notes_list
        df['Player'] = [x[0] for x in df['Player'].str.rsplit("(")] # Player

        shots = pd.concat([shots, df]).reset_index().drop(columns=['index'])

    custom_file_name = f'euro2024_shots_week_{week}.csv'


        with tempfile.NamedTemporaryFile(mode='w', delete=True, prefix=custom_file_name) as temp:
            shots.to_csv(temp.name, index=False)

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

    # @task()
    # def analyze_scores():

    # @task()
    # def analyze_scoring_opportunities():


    get_data = extract_fixtures()
    cls_scores = cleanse_fixtures(get_data)
    load_scores = load_fixtures(cls_scores)
    load_matchdetails = get_gk_stats(cls_scores)
    get_shots = load_shots(cls_scores)

ProcessScores()