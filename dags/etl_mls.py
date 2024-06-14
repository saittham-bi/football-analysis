# import pandas as pd
# import numpy as np
# import duckdb
# import http.client
# from datetime import datetime, timedelta
# import functions.fbref_functions as func
# import tempfile
# import os

# from airflow.decorators import dag, task
# from airflow.hooks.base import BaseHook

# # ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")


# default_args = {
#     'owner': 'sa_postgres',
#     'start_date': datetime(2024, 4, 25),
#     'retries': 0
#     # You can add more default arguments here as needed
# }

# # Define Postgres DB connection
# postgres_conn = BaseHook.get_connection('postgres_default')

# # Initialize duckdb with postgres connector    
# cursor = duckdb.connect('/opt/airflow/data/mls.db')
# cursor.sql("INSTALL postgres;")
# cursor.sql("LOAD postgres;")
# cursor.sql(f"ATTACH 'dbname=football user={postgres_conn.login} password={postgres_conn.password} host={postgres_conn.host}' AS postgres_db (TYPE POSTGRES);")

# @dag(
#     dag_id="etl_mls",
#     start_date=datetime(2023, 10, 2),
#     schedule="0 5 * * 1",
#     catchup=False,
#     default_args=default_args,
# )
# def ProcessScores(): 
#     # Define drive connection
#     drive_conn = BaseHook.get_connection('kdrive')
#     headers = {
#     'Authorization': 'Bearer ' + drive_conn.password,
#     'Content-Type': 'application/octet-stream',
#     }


#     # Define competition url and name    
#     competition_url = 'https://fbref.com/en/comps/22/history/Major-League-Soccer-Seasons'
#     comp_name = 'Major League Soccer'
    
#     # 1. task to load data from the URL into a duckdb table
#     @task()
#     def extract_fixtures():

#         df = func.get_fixtures(competition_url, comp_name)

#         custom_file_name = 'mls_fixtures.csv'
        
#         with tempfile.NamedTemporaryFile(mode='w', delete=True, prefix=custom_file_name) as temp:
#             df.to_csv(temp.name, index=False)

#             with open(temp.name, 'rb') as f:
#                 file = f.read()
#             file_size = os.path.getsize(temp.name)

#             print('Filename: ' + temp.name)
#             print('Filesize: ' + str(file_size))

#             conn = http.client.HTTPSConnection(drive_conn.host)
#             conn.request("POST", f'{drive_conn.schema}/upload?total_size={file_size}&directory_id=4465&file_name={custom_file_name}&conflict=version', file, headers)
#             res = conn.getresponse()
#             data = res.read()
#             print(data.decode("utf-8"))

#         return df

#     @task()
#     def cleanse_fixtures(extract_fixtures):
#         scores_df = func.transform_scores(extract_fixtures)

#         return scores_df
  
#     @task()
#     def load_fixtures(cleanse_fixtures):
#         df = cleanse_fixtures
#         cursor.sql("DROP TABLE IF EXISTS postgres_db.mls_fixtures;")
#         cursor.sql("CREATE TABLE postgres_db.mls_fixtures AS SELECT * FROM df;")

#         print(cursor.sql('SELECT * FROM df LIMIT 5;'))

#     get_data = extract_fixtures()
#     cls_scores = cleanse_fixtures(get_data)
#     load_scores = load_fixtures(cls_scores)


# ProcessScores()

# # cursor.sql('DETACH postgres_db')
# # cursor.close()