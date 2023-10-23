import pandas as pd
import psycopg2
from lookups import FileType,ErrorHandling,HandledType
import re

def extract_data_into_df(data_type,data_path):
    data_frame=None
    try:
        if data_type==FileType.CSV:
           data_frame=pd.read_csv(data_path)
        elif data_type==FileType.EXCEL:
             data_frame=pd.read_excel(data_path)
        elif data_type==FileType.PostgreSQL:
             pass
    except Exception as e:
        suffix=ErrorHandling.extract_data_info_df.value
        prefix=str(e)
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



# def insert_statement_from_dataframe(dataframe, schema_name, table_name):
#     columns=','.join(dataframe.columns)
#     for index,row in dataframe.iterrows():
#         values_list=[]
#         for val in row.values:
#             val_type=str(type(val))
#             if val_type==HandledType.TIMESTAMP.value:
#                 values_list.append(str(val))
#             elif val_type==HandledType.STRING.value:
#                 values_list.append(f"'{val}")
#             elif val_type==HandledType.LIST.value:
#                 val_item=';'.join(val)
#                 values_list.append(f"'{val_item}")
#             else:
#                 values_list.append(str(val))
#         values=', '.join(values_list)
#         insert_statement=f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values});" 
#         return insert_statement

def insert_statement_from_dataframe(dataframe, schema_name, table_name):
    columns = ', '.join(dataframe.columns)
    rows = []

    for _, row in dataframe.iterrows():
        formatted_row = []

        for value in row:
            if pd.notna(value):
                if isinstance(value, str):
                    formatted_row.append(f"'{value}'")
                elif isinstance(value, pd.Timestamp):
                    formatted_row.append(f"'{value}'")
                else:
                    formatted_row.append(str(value))
            else:
                formatted_row.append('NULL')

        rows.append(', '.join(formatted_row))


    values = ', '.join(rows).replace("'", "' '")

    insert_statement = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values});"

    return insert_statement



