import os
import psycopg2  
from db_handler import create_connection
from data_handler import execute_posthook_statements

def execute():
     connection = create_connection()
     execute_posthook_statements(connection,'SQL_Commands')
     connection.close()
