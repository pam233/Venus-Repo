import psycopg2
import logging
import json

logging.basicConfig(filename='app.log', level=logging.INFO)


def load_config(filename='config.json'):
    try:
        with open(filename, 'r') as file:
            config = json.load(file)
            return config
    except FileNotFoundError:
        logging.error('Config file not found: %s', filename)
        return None
    except json.JSONDecodeError:
        logging.error('Error decoding JSON from config file: %s', filename)
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
        logging.info('Successfully connected to the PostgreSQL database')
        return conn
    except (Exception, psycopg2.Error) as error:
        logging.error(
            'Error connecting to the PostgreSQL database: %s', error)
        return None


def execute_query(conn, query):
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()  # Fetch all results
        conn.commit()
        logging.info('Query executed successfully: %s', query)
        logging.info('Query result: %s', json.dumps(result, indent=2))  # Log the result
        return result  # Return the result for further processing if needed
    except (Exception, psycopg2.Error) as error:
        if cursor:
            conn.rollback()
        logging.error('Error executing query: %s', error)
        return None
    finally:
        if cursor:
            cursor.close()



def close_connection(conn):
    if conn is not None:
        conn.close()
        logging.info('PostgreSQL connection closed')


def try_connection():
    try:
        connection = create_connection()
        query = "SELECT * FROM dim_track;"
        execute_query(connection, query)
    except Exception as e:
        logging.error('An error occurred: %s', str(e))
    finally:
        close_connection(connection)


try_connection()