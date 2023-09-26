from enum import Enum

class FileType(Enum):
    CSV= ".csv"
    EXCEL =" .xlsx"
    TEXT =" .txt"
    PostgreSQL ="postegreSQL"
    mongoDB= "mongo"

class ErrorHandling(Enum):
    extract_data_info_df="extract_data_info_df"


    