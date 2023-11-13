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
def execute():
    connection = create_connection()

    # Execute your SQL files in the desired order
    execute_sql_file(connection, os.path.join('SQL_Commands\V2_hook_create_dim_users_table.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V3_hook_create_dim_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V4_hook_create_dim_songs.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V5_hook_create_fct_user_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V6_hook_fct_average_listen_time_per_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V7_hook_create_agg_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V8_hook_create_fct_age_distribution_per_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V9_hook_create_fct_average_listen_time_per_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V10_hook_create_fct_song_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V11_hook_create_agg_songs_per_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\v12_hook_create_agg_user_per_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V13_hook_create_agg_genre.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V14_hook_create_agg_release_date.sql'))
    execute_sql_file(connection, os.path.join('SQL_Commands\V15_hook_create_agg_playlist.sql'))

    connection.close()

