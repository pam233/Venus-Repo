import pandas as pd
import psycopg2
# remove any imported object that is not being used.
from lookups import FileType, ErrorHandling, HandledType
# use 1 import of db_handler not multiple
from db_handler import create_connection,close_connection,execute_query
import os
from datetime import time
import psycopg2
import json
from datetime import datetime

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
        # log_message(suffix, prefix)
    return data_frame



def list_all_files(file_directory):
    # return list of query content into a list
    pass


def return_create_statement_from_dataframe(dataframe, schema_name, table_name):
    type_mapping = {
        'Int64': 'INT',
        'float64': 'FLOAT',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP'
    }
    fields = []
    for column, dtype in dataframe.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        fields.append('"{column}" {sql_type}'.format(
            column=column, sql_type=sql_type))

    create_table_statement = "CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n".format(
        schema_name=schema_name, table_name=table_name)
    # ID SERIAL PRIMARY KEY (this is optional)
    # if you have an ID in the table, you don't need this.
    create_table_statement += 'ID SERIAL PRIMARY KEY, \n'
    create_table_statement += ',\n'.join(fields)
    create_table_statement += "\n);"
    return create_table_statement

def execute_sql_script_from_config(sql_script_path, config_path='config.json'):
    try:
        # Load database connection details from config.json
        with open(config_path) as config_file:
            config_data = json.load(config_file)

        conn = psycopg2.connect(
            dbname=config_data['database'],
            user=config_data['user'],
            password=config_data['password'],
            host=config_data['host'],
            port=config_data['port']
        )

        # Create a cursor object
        cursor = conn.cursor()

        # Read the SQL file
        with open(sql_script_path, 'r') as sql_file:
            sql_script = sql_file.read()

        # Execute the SQL script
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

        record_etl_watermark(conn, schema_name, "etl_watermark", table_name)

    return insert_statements


def create_etl_watermark_table(conn, sql_file_name):
    try:
        with conn.cursor() as cursor:
            with open(sql_file_name, 'r') as sql_file:
                create_table_query = sql_file.read()
            execute_query(conn, create_table_query)
            # don't print -- log them in file.
            print("ETL watermark table created successfully.")
    except (Exception, psycopg2.Error) as error:
        # don't print -- log them in file.
        print(f"Error creating ETL watermark table: {error}")


# def record_etl_watermark(conn, schema_name, watermark_table_name, table_name):
#     try:
#         with conn.cursor() as cursor:
#             etl_watermark_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#             insert_query = f"INSERT INTO {schema_name}.{watermark_table_name} (table_name, last_update_timestamp) " \
#                            f"VALUES ('{table_name}', TIMESTAMP '{etl_watermark_timestamp}') " \
#                            f"ON CONFLICT (table_name) DO UPDATE SET last_update_timestamp = TIMESTAMP '{etl_watermark_timestamp}';"
#             execute_query(conn, insert_query)
#             print(f"ETL watermark timestamp recorded for table: {table_name}")
#     except (Exception, psycopg2.Error) as error:
#         print(f"Error recording ETL watermark timestamp for table {table_name}: {error}")

def record_etl_watermark(conn, schema_name, watermark_table_name, table_name):
    try:
        with conn.cursor() as cursor:
            etl_watermark_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insert_query = f"""
                INSERT INTO {schema_name}.{watermark_table_name} (table_name, last_update_timestamp)
                VALUES ('{table_name}', TO_TIMESTAMP('{etl_watermark_timestamp}', 'YYYY-MM-DD HH24:MI:SS'))
                ON CONFLICT (table_name) DO UPDATE 
                SET last_update_timestamp = TO_TIMESTAMP('{etl_watermark_timestamp}', 'YYYY-MM-DD HH24:MI:SS');
            """
            execute_query(conn, insert_query)
            print(f"ETL watermark timestamp recorded for table: {table_name}")
    except (Exception, psycopg2.Error) as error:
        print(f"Error recording ETL watermark timestamp for table {table_name}: {error}")

def execute_query(conn, query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            print("SQL script executed successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error executing SQL script: {error}")




def insert_data_in_batches(data, schema_name, table_name, batch_size, watermark_table_name):
    if data is None:
        # don't print -- log them in file.
        print("Error: Data is None.")
        return
    
    conn = create_connection()
    
    if conn is None:
        return
    
    try:
        with conn.cursor() as cursor:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                insert_statements = insert_statements_from_dataframe(batch, schema_name, table_name)
                
                with conn:
                    with conn.cursor() as cursor:
                        for statement in insert_statements:
                            execute_query(conn, statement)
                    
                    # After inserting the batch, record the ETL watermark timestamp
                    record_etl_watermark(conn, schema_name, watermark_table_name,table_name)
        
        conn.commit()
        print("Data inserted successfully in batches.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting data in batches: {error}")
    finally:
        if conn:
            conn.close()