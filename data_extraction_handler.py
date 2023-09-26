import pandas as pd
import psycopg2
from lookups import FileType,ErrorHandling
from data_logging import return_error_log

def extract_data_into_df(data_type,data_path):
    data_frame=None
    try:
        if data_type==FileType.CSV:
           data_frame=pd.read_csv(data_path)
        elif data_type==FileType.EXCEL:
             data_frame=pd.read_excel(data_path)
        elif data_type==FileType.PostgreSQL:
             pass
        elif data_type==FileType.mongoDB:
             pass
    except Exception as e:
        suffix=ErrorHandling.extract_data_info_df.value
        prefix=str(e)
        return_error_log(suffix,prefix)
    finally:
        return data_frame
    
def list_all_files(file_directory):
    pass
