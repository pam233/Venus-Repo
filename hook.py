import os
import psycopg2  
from db_handler import create_connection
# Function to establish a database connection


# Function to execute SQL code from a file
def execute_sql_file(connection, file_path):
    with open(file_path, 'r') as file:
        sql_code = file.read()
        with connection.cursor() as cursor:
            cursor.execute(sql_code)
        connection.commit()

# Main ETL function
def execute_etl(sql_folder):
    connection = create_connection()

    # Execute your SQL files in the desired order
    execute_sql_file(connection, os.path.join(sql_folder, 'SQL_Commands'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V2_hook_create_dim_users_table.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V4_hook_create_dim_songs.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V5_hook_create_fct_user_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V6_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join(sql_folder, 'V3_hook_create_dim_genre.sql'))

    # Add more files as needed

    connection.close()

