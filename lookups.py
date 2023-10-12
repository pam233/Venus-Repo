from enum import Enum
import logging

class FileType(Enum):
    CSV= ".csv"
    EXCEL =" .xlsx"
    TEXT =" .txt"
    PostgreSQL ="postegreSQL"
    mongoDB= "mongo"

class ErrorHandling(Enum):
    extract_data_info_df="extract_data_info_df"

class LogLevels:
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
   