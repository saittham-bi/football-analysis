# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta
# from sqlalchemy import create_engine
# import duckdb

# from airflow.decorators import dag, task
# from airflow.hooks.base import BaseHook

# default_args = {
#     'owner': 'sa_postgres',
#     'start_date': datetime(2024, 4, 25),
#     # You can add more default arguments here as needed
# }

# @dag(
#     dag_id="db_test",
#     default_args=default_args,
#     #schedule="0 4 * * 1",
#     #catchup=False,
#     #default_args={"retries": 0},
# )
# def ETL():
#     # postgres_conn = BaseHook.get_connection('postgres_default')

#     @task()
#     def create_df():
#         df = pd.DataFrame([['Manuel Neuer', 32, 44.0, 28], ['Lukas Hradecky', 30, 28.9, 21], ['Kevin Trapp', 32, 42.4, 44]], 
#                           columns=['keeper_name', 'games_played', 'xg', 'goals'])      
#         #return df
    
#         cursor = duckdb.connect('/opt/airflow/data/my_duckdb.db')
#         cursor.sql("INSTALL postgres;")
#         cursor.sql("LOAD postgres;")
#         cursor.sql(f"ATTACH 'dbname=airflow user={postgres_conn.login} password={postgres_conn.password} host={postgres_conn.host}' AS postgres_db (TYPE POSTGRES);")
#         # print(cursor.sql('SELECT * FROM df;').to_df())
#         # cursor.sql("CREATE TABLE IF NOT EXISTS postgres_db.ducktable (id INTEGER, name VARCHAR);")
#         # cursor.sql("CREATE TABLE IF NOT EXISTS postgres_db.goalies ")
#         cursor.sql("INSERT INTO postgres_db.goalies SELECT * FROM df")
#         cursor.close()
#         # try:
#         #     postgres_hook = PostgresHook(postgres_conn_id="postgres_default", database='football')
#         #     conn = postgres_hook.get_conn()
#         #     engine = create_engine(conn)
#         #     df.to_sql('goalies', con=conn, if_exists='replace', index=False)
#         #     conn.commit()
#         #     return 0
#         # except Exception as e:
#         #     return 1

#     #table_creation = create_tables.set_upstream(create_table)
#     start = create_df()
#     #upload_scores = load_matches_to_db.set_upstream(start)
#     # upload_data = load_data_task.set_upstream(start)

# ETL()


