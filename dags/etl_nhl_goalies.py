import pandas as pd
import numpy as np
import duckdb
import http.client
from datetime import datetime, timedelta
import functions.kdrive_functions as func
import tempfile
import os

from airflow.decorators import dag, task
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
cursor.sql(f"ATTACH 'dbname=nhl user={postgres_conn.login} password={postgres_conn.password} host={postgres_conn.host}' AS postgres_db (TYPE POSTGRES);")

kdrive = func.kDrive()

@dag(
    dag_id="etl_nhl_goalies",
    start_date=datetime(2024, 10, 9),
    schedule="0 18 * * *",
    tags=['Hockey'],
    default_args=default_args,
)
def SaveStatistics(): 
    # Define drive connection
    drive_conn = BaseHook.get_connection('kdrive')
    headers = {
    'Authorization': 'Bearer ' + drive_conn.password,
    'Content-Type': 'application/octet-stream',
    }

    file_directory = 5487

    # 1. task to load data from the URL into a duckdb table
    @task()
    def extract_stats_to_file():
        url = 'https://www.hockey-reference.com/leagues/NHL_2025_goalies.html'
        goalie_stats = pd.read_html(url)[0]
        goalie_stats.columns = [x[1] for x in goalie_stats.columns]
        goalie_stats = goalie_stats.iloc[:, :-5]

        current_date = datetime.now().date()
        custom_file_name = f'nhl_goalie_stats_{current_date}'

        kdrive.upload_files(filetype='parquet', df=goalie_stats, 
                            filename=custom_file_name, directory_id=file_directory)
        
    @task()
    def save_stats_to_db(get_data):
        ids = kdrive.files_in_directory(file_directory)
        goalie_stats = pd.DataFrame()
        cursor.sql("""
                   CREATE TEMP TABLE goalie_input (
                   player VARCHAR NOT NULL,
                   age INTEGER NOT NULL,
                   team VARCHAR NOT NULL,
                   games_played INTEGER NOT NULL,
                   games_started INTEGER NULL,
                   wins INTEGER NOT NULL,
                   losses INTEGER NOT NULL,
                   ties INTEGER NOT NULL,
                   goals_against INTEGER NOT NULL,
                   shots INTEGER NOT NULL,
                   saves INTEGER NOT NULL,
                   shutouts INTEGER NOT NULL,
                   minutes_played FLOAT NOT NULL
                   );
                   """)
        for id in ids:
            print(id)
            input_df = kdrive.read_files(filetype='parquet', file_id=id)
            input_df.columns = map(str.lower, input_df.columns)
            input_df.rename(columns={'tm': 'team', 'sa': 'shots', 'Test': 'test'}, inplace=True)
            cursor.sql("""
                       INSERT INTO goalie_input
                       SELECT CAST(player AS varchar) AS player,
                        CAST(age AS INTEGER) AS age,
                        CAST(team AS VARCHAR) AS team,
                        CAST(gp AS INTEGER) AS games_played,
                        CAST(gs AS INTEGER) AS games_started,
                        CAST(w AS INTEGER) AS wins,
                        CAST(l AS INTEGER) AS losses,
                        CAST("T/O" AS INTEGER) AS ties,
                        CAST(ga AS INTEGER) goals_against,
                        CAST(shots AS INTEGER) AS shots,
                        CAST(sv AS INTEGER) AS saves,
                        CAST(so AS INTEGER) shutouts,
                        CAST(SPLIT_PART(min, ':', 1) AS FLOAT) + CAST(CAST(right(min, 2) AS integer) / 60 AS FLOAT) AS minutes_played
                       FROM input_df 
                       WHERE player NOT IN ('Player', 'League Average');
                       """)
                
        #     goalie_stats = pd.concat([goalie_stats, input_df])

        table_name = 'postgres_db.goalie_stats'
        # goalie_stats = goalie_stats[['player', 'age', 'team', 'gp', 'gs', 'w', 'l', 'T/O', 'ga', 'shots', 'sv', 'so', 'min']].copy()
        # goalie_stats.drop_duplicates(inplace=True)

        cursor.sql(f"DELETE FROM {table_name};")
        cursor.sql(f"INSERT INTO {table_name} SELECT DISTINCT * FROM goalie_input;")
        # cursor.sql(f"DROP TABLE {table_name};")
        # cursor.sql(f"CREATE TABLE {table_name} AS SELECT * FROM goalie_input;")
        # print(f'Full replaced goalie_stats with {len(goalie_stats)} rows')

    get_data = extract_stats_to_file()
    write_full = save_stats_to_db(get_data)

SaveStatistics()