import requests
import pandas as pd
import json
from database_connection_with_logging_handler import create_connection


def load_data_into_dataframe(file_path, file_type, db_connection=None):
    db_connection = create_connection()
    supported_file_types = ['api', 'json', 'csv', 'excel']

    if file_type not in supported_file_types:
        print("Unsupported type!")
        return -1

    if file_type == 'api':
        response = requests.get(file_path)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print(
                f'Failed to retrieve data from the API. Status code: {response.status_code}')
            return None

    elif file_type == 'json':
        with open(file_path, 'r') as file:
            data = json.load(file)
        return pd.DataFrame(data)

    elif file_type == 'csv':
        return pd.read_csv(file_path)

    elif file_type == 'excel':
        return pd.read_excel(file_path)
