import os
import psycopg2  
from db_handler import create_connection
from data_handler import execute_posthook_statements
import etl_handler

def execute():
     
     db_session = create_connection()
     etl_handler.update_etl_watermark(db_session,'stg_kaggle_spotify_tracks',0)
     etl_handler.update_etl_watermark(db_session,'stg_kaggle_spotify_users',1)
     execute_posthook_statements(db_session,'SQL_Commands')
     db_session.close()
