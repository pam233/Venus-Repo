

import db_handler
import data_handler
from datetime import datetime

def insert_initial_etl_watermark(db_session, table_name):
    insert_query = """
        INSERT INTO musicschema.etl_watermark (table_name, etl_last_id)
        VALUES (%s, %s);
    """
 
    with db_session.cursor() as cursor:
        cursor.execute(insert_query, (table_name, 0))
 
    db_session.commit()
 
def return_etl_watermark(db_session, table_name):
    query = """
        SELECT 
            etl_last_id
        FROM musicschema.etl_watermark 
        WHERE table_name = %s
    """
    with db_session.cursor() as cursor:
        cursor.execute(query, (table_name,))
 
        etl_return = cursor.fetchall()
 
    print(f"ETL Return for table '{table_name}': {etl_return}")
    if not etl_return:
        print("No ETL record found. Returning 0.")
        return 0
    return_etl_field = etl_return[0][0]
    return int(return_etl_field)
 
def update_etl_watermark(db_session, table_name, last_id):
    update_query = """
        UPDATE musicschema.etl_watermark
        SET etl_last_id = %s
        WHERE table_name = %s;
    """
    with db_session.cursor() as cursor:
        cursor.execute(update_query, (int(last_id), table_name))
 
    print(f"ETL watermark updated for table '{table_name}' with last_id = {last_id}.")
    db_session.commit()


