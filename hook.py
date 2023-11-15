import os
import psycopg2  
from db_handler import create_connection
from data_handler import execute_hook_statements
# Function to establish a database connection




# Main ETL function
def execute():
    connection = create_connection()
    execute_hook_statements(connection,'SQL_Commands')
    connection.close()

