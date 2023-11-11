import data_handler
import lookups
import db_handler

from data_handler import return_create_statement_from_dataframe
import pandas as pd
from db_handler import create_connection,execute_query, close_connection
from data_handler import insert_statements_from_dataframe,extract_data_into_df,create_etl_watermark_table,insert_data_in_batches
from lookups import FileType

db_session = create_connection()

import os
import googleapiclient
import google.oauth2


# try to load them in google drive and use a common link between the team.
def return_csv_list():
    csv_list = []
    csv_list.append('C:\\dataproject\\songs_dataset.csv')
    csv_list.append('')
    return csv_list

# def read_csv_files():
#     # Replace with the path to your JSON key file.
#       KEY_FILE = 'path/to/your/credentials.json'
#       SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# # Create a service client
#       creds = None
#       creds = service_account.Credentials.from_service_account_file(KEY_FILE, scopes=SCOPES)

# # Build the Google Drive API service
#       drive_service = build('drive', 'v3', credentials=creds)

# # Replace with the file IDs of your CSV files
#       file_id_1 = 'your_csv_file_id_1'
#       file_id_2 = 'your_csv_file_id_2'

# # Download the CSV files
#       file_path_1 = 'local_path_to_save_csv1.csv'
#       file_path_2 = 'local_path_to_save_csv2.csv'

#       request = drive_service.files().export_media(fileId=file_id_1, mimeType='text/csv')
#       fh = open(file_path_1, 'wb')
#       downloader = MediaIoBaseDownload(fh, request)
#       done = False
# while done is False:
#     status, done = downloader.next_chunk()
#     print(f"Download {int(status.progress() * 100)}%")

# request = drive_service.files().export_media(fileId=file_id_2, mimeType='text/csv')
# fh = open(file_path_2, 'wb')
# downloader = MediaIoBaseDownload(fh, request)
# done = False
# while done is False:
#     status, done = downloader.next_chunk()
#     print(f"Download {int(status.progress() * 100)}%")




def execute_prehook_sql_statements(db_session):
    # read sql file from SQL_Commands folder
    file_content = None
    item = 'V2_prehook_create_schema.sql'
    if item.split('_')[1] == 'prehook':
        db_handler.execute_query(db_session, file_content)


    

def create_staging_tables(csv_list, staging_source_name, staging_schema_name, staging_table_name):
    # recommendation: using an underscore statement like: music_schema.
    for csv_item in csv_list:
        # try to dynamically load the source.
        table_name = f"stg_k{staging_source_name}_{staging_table_name}"
        # Ensure that the csv_item contains at least one '/'
        if '/' in csv_item:
            staging_table_name = csv_item.split(
                '/')[-1].replace('.csv', '').lower()
        else:
            # Handle the case where there is no '/' in the csv_item
            # You can provide a default value or raise an exception as needed
            staging_table_name = "default_value"  # Replace with your default value
        stg_df = data_handler.extract_data_into_df(
            lookups.FileType.CSV, csv_item)
        create_stmnt = data_handler.return_create_statement_from_dataframe(
            stg_df, {staging_schema_name}, table_name)
        
def execute():
    pass
    

def create_dimension_tables(stg_users_dataframe,stg_songs_dataframe):
    dim_users_data = stg_users_dataframe[["age", "gender", "fav_music_genre", "music_time_slot", "music_lis_frequency", "music_satisfaction"]]
    dim_songs_data = stg_songs_dataframe[["track_id", "track_name", "track_artist", "playlist_genre"]]
    dim_genre_data = stg_songs_dataframe[["playlist_genre"]]

    dim_users_table_name = "Dim_users"
    dim_songs_table_name = "Dim_songs"
    dim_genre_table_name = "Dim_genre"
    schema = 'musicschema'

    execute_query(conn=db_session, query=return_create_statement_from_dataframe(
    dim_users_data, schema, dim_users_table_name))

    execute_query(conn=db_session, query=return_create_statement_from_dataframe(
    dim_songs_data, schema, dim_songs_table_name))

    execute_query(conn=db_session, query=return_create_statement_from_dataframe(
    dim_genre_data, schema, dim_genre_table_name))


def create_fact_tables(stg_users_dataframe,stg_songs_dataframe):
    schema = 'musicschema'
    fact_user_genre_data = stg_users_dataframe[["age", "gender", "fav_music_genre"]]
    fact_user_genre_data["total_users"] = 1
    fact_user_genre_data = fact_user_genre_data.groupby(["age", "gender", "fav_music_genre"]).sum().reset_index()

    fact_song_genre_data = stg_songs_dataframe[["track_name", "track_artist", "playlist_genre"]]
    fact_song_genre_data["total_songs"] = 1
    fact_song_genre_data = fact_song_genre_data.groupby(["track_name", "track_artist", "playlist_genre"]).sum().reset_index()

    fact_user_genre_table_name = "Fact_user_genre"
    fact_song_genre_table_name = "Fact_song_genre"

    execute_query(conn=db_session, query=return_create_statement_from_dataframe(
        fact_user_genre_data, schema, fact_user_genre_table_name))

    execute_query(conn=db_session, query=return_create_statement_from_dataframe(
        fact_song_genre_data, schema, fact_song_genre_table_name))

'''
agg_genre_data = stg_songs_dataframe[["playlist_genre"]]
agg_genre_data["total_songs"] = 1
agg_genre_data = agg_genre_data.groupby("playlist_genre").sum().reset_index()

agg_dim_genre_data = stg_songs_dataframe[["playlist_genre"]]
agg_dim_genre_data = agg_dim_genre_data.drop_duplicates()

agg_total_songs_per_genre_table_name = "agg_total_songs_per_genre"
agg_dim_genre_table_name = "agg_dim_genre"


execute_query(conn=db_session, query=return_create_statement_from_dataframe(
    agg_genre_data, schema, agg_total_songs_per_genre_table_name))'''