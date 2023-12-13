import pandas as pd
from db_handler import create_connection,execute_query, close_connection
from data_handler import insert_statements_from_dataframe,extract_data_into_df
from data_handler import execute_sql_script_from_config
from lookups import FileType
import prehook
from prehook import execute
import hook
import posthook
from import_from_google_drive import download_and_convert_to_dataframe
from etl_handler import insert_initial_etl_watermark,update_etl_watermark,return_etl_watermark


db_session = create_connection()
def etl():
    print("Running ETL process...")
   
    prehook.execute()
    hook.execute()
    posthook.execute()
   
    print("ETL process completed.")
 
