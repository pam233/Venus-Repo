import json
from datetime import datetime
import psycopg2
from db_handler import create_connection,execute_query, close_connection

def filter_data_by_year(data, target_year):
    filtered_data = [record for record in data if get_year_from_date(record['track_album_release_date']) == target_year]
    return filtered_data

def get_year_from_date(date_str):
    date_obj = datetime.strptime(date_str, '%m/%d/%Y').date()
    return date_obj.year

def save_watermark(connection, cursor, last_processed_id, table_name):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute(f"INSERT INTO etl_watermark (table_name, last_processed_id, last_processed_timestamp) VALUES (%s, %s, %s)",
                   (table_name, last_processed_id, timestamp))
    connection.commit()

def read_watermark(connection, cursor, table_name):
    cursor.execute("SELECT last_processed_id, last_processed_timestamp FROM etl_watermark WHERE table_name = %s ORDER BY last_processed_id DESC LIMIT 1", (table_name,))
    result = cursor.fetchone()
    return result

def parse_date(date_str):
    return datetime.strptime(date_str, '%m/%d/%Y').date()

def etl_process(connection, cursor, table_name, filter_year_range):
    last_processed_record = read_watermark(connection, cursor, table_name)
    last_processed_id = last_processed_record[0] if last_processed_record else 0

    start_year, end_year = filter_year_range
    cursor.execute(f"SELECT * FROM {table_name} WHERE id > {last_processed_id} AND get_year_from_date(track_album_release_date) BETWEEN {start_year} AND {end_year} ")
    extracted_data = cursor.fetchall()

    if extracted_data:
        transformed_data = [(record[0], record[1].upper(), parse_date(record[2])) for record in extracted_data]

        cursor.executemany("INSERT INTO destination_table (id, transformed_value, release_date) VALUES (%s, %s, %s)", transformed_data)

        last_processed_id = max(record[0] for record in extracted_data)
        save_watermark(connection, cursor, last_processed_id, table_name)

connection = create_connection()
cursor = connection.cursor()

current_year = datetime.now().year
year_ranges = [(start_year, start_year + 10) for start_year in range(1970, current_year, 10)]

for start_year, end_year in year_ranges:
    etl_process(connection, cursor, 'stg_kaggle_spotify_tracks', (start_year, end_year))

connection.close()
