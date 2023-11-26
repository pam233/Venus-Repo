import db_handler
import data_handler
from datetime import datetime



def return_etl_watermark(db_session):
    # returns all timestamps for the updates
    query = """
        SELECT 
            last_update_timestamp
        FROM musicschema.etl_watermark
    """
    etl_return = data_handler.return_query_as_dataframe(db_session, query)
    
    # Check if the DataFrame is empty
    if etl_return.empty:
        return ['01/01/1900']  
    return_etl_field = etl_return['last_update_timestamp'].tolist()
    
    return return_etl_field




def update_or_insert_etl_watermark(db_session):
    query = "SELECT COUNT(1) AS total_records FROM musicschema.etl_watermark"
    return_data = data_handler.return_query_as_dataframe(db_session, query)
    total_records = return_data['total_records'].iloc[0]
    if total_records > 0:
        query = f"""UPDATE musicschema.etl_watermark SET last_update_timestamp = '{datetime.now()}'"""
    else:
        query = f""" INSERT INTO musicschema.etl_watermark (last_update_timestamp) VALUES ('{datetime.now()}')"""
    db_handler.execute_query(db_session, query)
    db_session.commit()
