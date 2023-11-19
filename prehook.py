import data_handler
import lookups
import db_handler
import os
from data_handler import return_create_statement_from_dataframe,execute_prehook_statements,execute_sql_script_from_config
import pandas as pd
from db_handler import create_connection,execute_query, close_connection
from data_handler import insert_statements_from_dataframe,extract_data_into_df,create_etl_watermark_table,insert_data_in_batches, record_etl_watermark

from lookups import FileType
from import_from_google_drive import download_and_convert_to_dataframe

def execute():
    db_session = create_connection()
    execute_prehook_statements(db_session, 'SQL_Commands')
    tracks_file_id = '1EDDNjMY23piFi8zCGaySGvlRcbUUqhKc'
    users_file_id = '157Ka9S8JMwEjYdvxzLTwMQzv1TH_aUvM'
    df_tracks = download_and_convert_to_dataframe(tracks_file_id)
    df_users = download_and_convert_to_dataframe(users_file_id)
    insert_statements1 = insert_statements_from_dataframe(df_users.head(200), 'musicschema', 'stg_kaggle_spotify_users')
    insert_statements2 = insert_statements_from_dataframe(df_tracks.head(500), 'musicschema', 'stg_kaggle_spotify_tracks')

    with db_session.cursor() as cursor:
        for statement in insert_statements1:
            cursor.execute(statement)
        for statement in insert_statements2:
            cursor.execute(statement)

    db_session.commit()





