import data_handler
import lookups
import db_handler
import os
from data_handler import return_create_statement_from_dataframe,execute_prehook_statements,execute_sql_script_from_config
import pandas as pd
from db_handler import create_connection,execute_query, close_connection
from data_handler import insert_statements_from_dataframe,extract_data_into_df,create_etl_watermark_table,insert_data_in_batches
from lookups import FileType

def return_excel_list():
    csv_list = []
    csv_list.append('C:\\dataproject\\songs_dataset.xlsx')
    csv_list.append('C:\\dataproject\\users_dataset.xlsx')
    csv_list.append('')
    return csv_list


def execute():
    db_session = create_connection()
    execute_prehook_statements(db_session, 'SQL_Commands')
    # execute_sql_script_from_config('SQL_Commands/prehook_create_stg_kaggle_spotify_tracks.sql')
    # execute_sql_script_from_config('SQL_Commands/prehook_create_stg_kaggle_spotify_users.sql')
    df = pd.read_excel("C:\\dataproject\\users_dataset.xlsx")
    df1 = pd.read_excel("C:\\dataproject\\songs_dataset.xlsx")
    insert_statements1 = insert_statements_from_dataframe(df, 'musicschema', 'stg_kaggle_spotify_users')
    insert_statements2 = insert_statements_from_dataframe(df1, 'musicschema', 'stg_kaggle_spotify_tracks')

    with db_session.cursor() as cursor:
        for statement in insert_statements1:
            cursor.execute(statement)
        for statement in insert_statements2:
            cursor.execute(statement)

    db_session.commit()





