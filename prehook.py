import data_handler
import lookups
import db_handler
import os
from data_handler import execute_prehook_statements,execute_sql_script_from_config
import pandas as pd
from db_handler import create_connection,execute_query, close_connection
from data_handler import insert_statements_from_dataframe,extract_data_into_df
from lookups import FileType
from import_from_google_drive import download_and_convert_to_dataframe
from etl_handler import update_etl_watermark,return_etl_watermark,insert_initial_etl_watermark

def execute():
   
    db_session = create_connection()
    execute_sql_script_from_config('SQL_Commands/V0_prehook_Create_Schema.sql','config.json')
    execute_sql_script_from_config('SQL_Commands/V4_prehook_Create_ETL_watermark_table.sql','config.json')
 
    last_inserted_track_id = return_etl_watermark(db_session, 'stg_kaggle_spotify_tracks')
    last_inserted_user_id = return_etl_watermark(db_session, 'stg_kaggle_spotify_users')
 
    execute_prehook_statements(db_session, 'SQL_Commands')
 
    tracks_file_id = '1EDDNjMY23piFi8zCGaySGvlRcbUUqhKc'
    users_file_id = '157Ka9S8JMwEjYdvxzLTwMQzv1TH_aUvM'
    genre_mapping_id='1IhGIN2uYoLHuKuaECb1Xd6-8gCjBOtdd'
    df_tracks = download_and_convert_to_dataframe(tracks_file_id)
    df_users = download_and_convert_to_dataframe(users_file_id)
    #df_genre_mappings = download_and_convert_to_dataframe(genre_mapping_id)
 
    insert_initial_etl_watermark(db_session, 'stg_kaggle_spotify_users')
    insert_initial_etl_watermark(db_session, 'stg_kaggle_spotify_tracks')

    insert_statements_tracks = insert_statements_from_dataframe(df_tracks, 'musicschema', 'stg_kaggle_spotify_tracks')
    insert_statements_users = insert_statements_from_dataframe(df_users, 'musicschema', 'stg_kaggle_spotify_users')
    #insert_statements_genres = insert_statements_from_dataframe(df_users, 'musicschema', 'genre_mapping')
    with db_session.cursor() as cursor:
        for statement in insert_statements_tracks:

            print(f"Statement: {statement}")
            try:
                track_id = int(statement.split('VALUES')[1].split(',')[0].strip(" '()"))
                print(f"Extracted Track ID: {track_id}")
            except ValueError as e:
                print(f"Error extracting Track ID: {e}")
                track_id = -1  
 
            if track_id > last_inserted_track_id:
                cursor.execute(statement)
 
        latest_track_id = df_tracks['track_id'].max()
        update_etl_watermark(db_session, 'stg_kaggle_spotify_tracks', latest_track_id)
 
        for statement in insert_statements_users:
            user_id = int(statement.split('VALUES')[1].split(',')[0].strip(" '()"))
 
            if user_id > last_inserted_user_id:
                cursor.execute(statement)
 
        latest_user_id = df_users['id'].max()
        update_etl_watermark(db_session, 'stg_kaggle_spotify_users', latest_user_id)
        
        # for statement in insert_statements_genres:
        #     cursor.execute(statement)
    db_session.commit()
 