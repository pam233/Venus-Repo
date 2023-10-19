import pandas as pd
import psycopg2


def create_table_from_df(dataframe, schema_name, table_name, connection_string):
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

    try:
        conn = psycopg2.connect(config.json)
        cursor = conn.cursor()
        cursor.execute(create_table_statement)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {str(e)}")


data = {
    "ID": [1, 2, 3],
    "Name": ["Alice", "Bob", "Charlie"],
    "Age": [25, 30, 22]
}

your_dataframe = pd.DataFrame(data)


schema_name = "public"
table_name = "tryouttable"
connection_string = "config.json"

# Call the create_table_from_df function
create_table_from_df(your_dataframe, schema_name,
                     table_name, connection_string)
