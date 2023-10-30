import pandas as pd
import psycopg2
from lookups import FileType, ErrorHandling, HandledType
import db_handler
from db_handler import execute_query
import os


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
        # log_message(suffix,prefix)
    finally:
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
    create_table_statement += 'ID SERIAL PRIMARY KEY, \n'
    create_table_statement += ',\n'.join(fields)
    create_table_statement += "\n);"
    return create_table_statement


def execute_prehook_statements(db_session, directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql") and "_prehook" in file:
                file_path = os.path.join(root, file)
                query = None
                print(file)

                with open(file_path, "r") as f:
                    query = f.read()
                db_handler.execute_query(db_session, query)
                db_session.commit()


def insert_statements_from_dataframe(dataframe, schema_name, table_name):
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


# Use the function to get insert queries for each row in the dataframe
