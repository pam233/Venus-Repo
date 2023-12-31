import pandas as pd
import psycopg2
from lookups import FileType, ErrorHandling, HandledType
from db_handler import create_connection,close_connection,execute_query
import os
from datetime import time
import json
from datetime import datetime
from sqlalchemy import create_engine


db_url = "postgresql://postgres:Xup_am5ql@localhost:5432/MusicDatabase"

# Create an SQLAlchemy engine
engine = create_engine(db_url)

con=create_connection()


def extract_data_into_df(data_type, data_path):
    data_frame = None
    try:
        if data_type == FileType.CSV:
            data_frame = pd.read_csv(data_path)
        elif data_type == FileType.EXCEL:
            data_frame = pd.read_excel(data_path)
        elif data_type == FileType.PostgreSQL:
            pass
    except Exception as e:
        suffix = ErrorHandling.extract_data_info_df.value
        prefix = str(e)
    return data_frame




def execute_sql_script_from_config(sql_script_path, config_path='config.json'):
    try:
        with open(config_path) as config_file:
            config_data = json.load(config_file)

        conn = psycopg2.connect(
            dbname=config_data['database'],
            user=config_data['user'],
            password=config_data['password'],
            host=config_data['host'],
            port=config_data['port']
        )

        cursor = conn.cursor()

        with open(sql_script_path, 'r') as sql_file:
            sql_script = sql_file.read()

        cursor.execute(sql_script)
        conn.commit()
        conn.close()

        print("SQL script executed successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error executing SQL script: {error}")


def execute_prehook_statements(db_session, directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql") and "_prehook" in file:
                file_path = os.path.join(root, file)
                query = None
                print(file)

                with open(file_path, "r") as f:
                    query = f.read()
                execute_query(db_session, query)
                db_session.commit()

def execute_posthook_statements(db_session, directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql") and "_posthook" in file:
                file_path = os.path.join(root, file)
                query = None
                print(file)

                with open(file_path, "r") as f:
                    query = f.read()
                execute_query(db_session, query)
                db_session.commit()

def execute_hook_statements(db_session, directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql") and "_hook" in file:
                file_path = os.path.join(root, file)
                query = None
                print(file)

                with open(file_path, "r") as f:
                    query = f.read()
                execute_query(db_session, query)
                db_session.commit()

def insert_statements_from_dataframe(dataframe, schema_name, table_name):
    conn = create_connection()

    insert_statements = []
    for _, row in dataframe.iterrows():
        formatted_row = []

        for value in row:
            if pd.notna(value):
                if isinstance(value, str):
                    formatted_value = value.replace("'", "''")
                    formatted_row.append(f"'{formatted_value}'")
                elif isinstance(value, pd.Timestamp):
                    formatted_row.append(f"'{value}'")
                else:
                    formatted_row.append(str(value))
            else:
                formatted_row.append('NULL')

        columns = ', '.join(dataframe.columns)
        values = ', '.join(formatted_row)
        insert_statement = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values});"
        insert_statements.append(insert_statement)
    return insert_statements


def execute_query(conn, query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            print("SQL script executed successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error executing SQL script: {error}")




