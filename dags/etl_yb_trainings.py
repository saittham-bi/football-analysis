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

@dag(
    dag_id="etl_yb_trainings",
    schedule=None,
    catchup=False,
    tags=['YB'],
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)

def LoadTrainings(): 
    kdrive = func.kDrive()
    # 1. task to load data from the URL into a duckdb table
    @task()
    def load_goalkeeper_dimension():
        keepers = kdrive.read_files('csv', 5474)
        keepers['Player Name'] = keepers['Vorname'] + ' ' + keepers['Name']
        keepers['Initialen'] = keepers['Vorname'].astype(str).str[0] + keepers['Name'].astype(str).str[0]
        keepers['Geburtsdatum'] = pd.to_datetime(keepers['Geburtsdatum'], dayfirst=True)

        table_name = 'postgres_db.goalies'
        # cursor.sql(f"DROP TABLE IF EXISTS {table_name};")
        # cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM keepers;")
        cursor.sql(f"DELETE FROM {table_name};")
        cursor.sql(f"INSERT INTO {table_name} SELECT * FROM keepers;")
        row_amount = cursor.sql(f"SELECT count(*) FROM {table_name};")
        
        print(f'Table {table_name} replaced with new row count: {row_amount} ')


    @task()
    def load_trainingskill():
        skills = kdrive.read_files('csv', 5476)
        skills.columns = skills.iloc[2, :8].tolist() + (skills.iloc[0, 8:] + ',' + skills.iloc[1, 8:]).tolist()
        skills = skills.iloc[3:]
        melt_df = skills.melt(id_vars=skills.columns[:8], value_vars=skills.iloc[8:]) \
                    .dropna(subset=['value']) \
                    .reset_index()

        melt_df.columns = melt_df.columns.str.replace(" ", "_")
        melt_df[['verhalten', 'entscheid']] = melt_df['variable'].str.split(',', expand=True)
        melt_df['Datum'] = pd.to_datetime(melt_df.Training_Start_Datetime, dayfirst=True).dt.date
        melt_df['Zeit'] = pd.to_datetime(melt_df.Training_Start_Datetime, dayfirst=True).dt.time
        cursor.sql("DROP TABLE IF EXISTS skills; CREATE TEMPORARY TABLE skills AS SELECT * FROM melt_df;")

        th_skills = cursor.sql("""SELECT sps.Index
           ,sps.Player AS Keeper
           ,CAST(sps.Datum AS DATE) AS Datum
           ,CAST(sps.Zeit AS TIME) AS Zeit
           ,sps.Training_Team_Name AS Trainingsteam
           ,sps.Training_Text AS Titel
           ,sps.Training_Unit_Short_Text AS Übung
           ,sps.Training_Unit_Description AS Notizen
           ,sps.Training_Unit_Intensity_Text AS Intensität
           ,sps.verhalten AS Kategorie
           ,sps.entscheid AS Spielsituation
           ,ent.entscheid AS Verhalten
           ,CAST(sps.value AS INT) AS Dauer
           FROM skills sps
           LEFT JOIN (SELECT * FROM skills WHERE verhalten = 'Verhalten') ent 
           ON sps.Player = ent.Player
           AND sps.Training_Start_Datetime = ent.Training_Start_Datetime
           AND sps.Training_Unit_Short_Text = ent.Training_Unit_Short_Text
           WHERE sps.verhalten = 'DEF'
           
           UNION ALL
           
           SELECT sps.Index
           ,sps.Player AS Keeper
           ,CAST(sps.Datum AS DATE) AS Datum
           ,CAST(sps.Zeit AS TIME) AS Zeit
           ,sps.Training_Team_Name AS Trainingsteam
           ,sps.Training_Text AS Titel
           ,sps.Training_Unit_Short_Text AS Übung
           ,sps.Training_Unit_Description AS Notizen
           ,sps.Training_Unit_Intensity_Text AS Intensität
           ,sps.verhalten AS Kategorie
           ,sps.entscheid AS Spielsituation
           ,'Spielaufbau' AS Verhalten
           ,CAST(sps.value AS INT) AS Dauer
           FROM skills sps
           WHERE sps.verhalten = 'OFF';           
           """).to_df()

        table_name = 'postgres_db.trainings'
        # cursor.sql(f"DROP TABLE IF EXISTS {table_name};")
        # cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM th_skills;")
        cursor.sql(f"DELETE FROM {table_name};")
        cursor.sql(f"INSERT INTO {table_name} SELECT * FROM th_skills;")
        row_amount = cursor.sql(f"SELECT count(*) FROM {table_name};")
        
        print(f'Table {table_name} replaced with new row count: {row_amount}')

    @task()
    def load_presence():
        presence = kdrive.read_files('csv', 5475)
        presence.columns = presence.columns.str.replace(" ", "_")
        presence.columns = presence.columns.str.replace(":", "")
        presence['Stufe'] = [x[-1] for x in presence['Team'].str.split(', ')]
        presence['Datum'] = pd.to_datetime(presence.Training_Start_Datetime).dt.date
        presence['Zeit'] = pd.to_datetime(presence.Training_Start_Datetime).dt.time

        th_presence = cursor.sql("""
            SELECT Person AS Keeper
            ,Position AS Position
            ,Team AS Team
            ,Stufe AS Stufe
            ,Training_Text AS Titel
            ,Training_Team AS Trainingsteam
            ,CAST(Datum AS DATE) AS Datum
            ,CAST(Zeit AS TIME) AS Zeit
            ,Abscence_Reason AS Abwesenheitsgrund
            ,CAST(Unnamed_7 AS INT) AS Anwesend
            FROM presence;
           """).to_df()
        
        table_name = 'postgres_db.anwesenheiten'
        # cursor.sql(f"DROP TABLE IF EXISTS {table_name};")
        # cursor.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM th_presence;")
        cursor.sql(f"DELETE FROM {table_name};")
        cursor.sql(f"INSERT INTO {table_name} SELECT * FROM th_presence;")
        row_amount = cursor.sql(f"SELECT count(*) FROM {table_name};")
        
        print(f'Table {table_name} replaced with new row count: {row_amount}') 
    
    # @task()
    # def call_snowflake_sprocs():
    #     SQLExecuteQueryOperator(
    #     task_id="call_sproc1", conn_id='postgres-sport-analytics-db', sql="grant_role_youth_base.sql"
    #     )
    # @task.bash
    # def run_command_from_script() -> str:
    #     return "include/grant_role.sh"


    get_keepers = load_goalkeeper_dimension()
    get_trainings = load_trainingskill()
    get_presence = load_presence()
    # run_script = run_command_from_script()
    
LoadTrainings()