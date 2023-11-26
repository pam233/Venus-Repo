import os
import psycopg2  
from db_handler import create_connection
from data_handler import execute_posthook_statements
import etl_handler

def execute():
     
     db_session = create_connection()
     etl_handler.update_or_insert_etl_watermark(db_session)

     execute_posthook_statements(db_session,'SQL_Commands')
    # etl_handler.update_etl_watermark()
     db_session.close()
