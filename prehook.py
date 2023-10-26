import data_handler 
import lookups
import db_handler


def return_csv_list():
    csv_list = []
    csv_list.append('')
    csv_list.append('')
    csv_list.append('')
    csv_list.append('')
    csv_list.append('')
    return csv_list


def execute_prehook_sql_statements(db_session):
    # read sql file from SQL_Commands folder
    file_content = None
    item = 'V2_prehook_create_schema.sql'
    if item.split('_')[1] == 'prehook':
        db_handler.execute_query(db_session, file_content)



def create_staging_tables(csv_list):
    for csv_item in csv_list:
        schema_name = 'dw_reporting'
        table_name = f"stg_kaggle_{staging_table_name}"
        stg_df = data_handler.extract_data_into_df(lookups.FileType.CSV, csv_item)
        staging_table_name = csv_item.split('/')[len( csv_item.split('/'))].replace('.csv','').tolower()
        create_stmnt = data_handler.return_create_statement_from_dataframe(stg_df,schema_name,table_name)



        
        


