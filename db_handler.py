import psycopg2
import logging
from logging_handler import Logger
from lookups import LogLevels
import json

local_logger = Logger(log_file='app.log')

def load_config(filename='config.json'):
    try:
        with open(filename, 'r') as file:
            config = json.load(file)
            return config
    except FileNotFoundError:
        local_logger.log_message(LogLevels.ERROR,'Config file not found: %s' % filename)
        return None
    except json.JSONDecodeError:
        local_logger.log_message(LogLevels.ERROR,'Error decoding JSON from config file: %s' % filename)
        return None

def create_connection():
    config = load_config()
    if config is None:
        return None

    try:
        conn = psycopg2.connect(
            database=config['database'],
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port']
        )
        
        local_logger.log_message(LogLevels.INFO,'Successfully connected to the PostgreSQL database')        
        return conn
    except (Exception, psycopg2.Error) as error:
        local_logger.log_message(LogLevels.ERROR,'Error connecting to the PostgreSQL database: %s' % error)
        return None

def execute_query(conn, query):
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        # result = cursor.fetchall()  # Fetch all results
        conn.commit()
        #local_logger.set_log_level(logging.INFO)
        local_logger.log_message(LogLevels.DEBUG,'Query executed successfully: %s' % query)
        # local_logger.log_message(LogLevels.INFO,'Query result: %s' % json.dumps(result, indent=2))  # Log the result
        # return result  # Return the result for further processing if needed
    except (Exception, psycopg2.Error) as error:
        if cursor:
            conn.rollback()
        
        local_logger.log_message(LogLevels.ERROR,'Error executing query: %s' % error)
        local_logger.log_message(LogLevels.INFO,query)
        return None
    finally:
        if cursor:
            cursor.close()





def close_connection(conn):
    if conn is not None:
        conn.close()
        local_logger.log_message(LogLevels.INFO,'PostgreSQL connection closed')

def try_connection():
    try:
        connection = create_connection()
        query = "SELECT 1 ;"
        execute_query(connection, query)
    except Exception as e:
        
        local_logger.log_message(LogLevels.ERROR,'An error occurred: %s' % str(e))
    finally:
        close_connection(connection)
