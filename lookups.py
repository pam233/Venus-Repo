from enum import Enum

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

class HandledType(Enum):
  TIMESTAMP = "<class 'pandas.libs.tslibs.timestamps.Timestamp>"
  INTEGER="<class 'int>"
  STRING = "<class 'str'>"
  FLOAT ="<class 'float'>"
  LIST="<class 'list'>"
   
class DBData(Enum):
    SCHEMA = 'music_schema'
    SOURCE_KAGGLE = 'kaggle'
    