import pandas as pd
import requests
from io import BytesIO

def download_and_convert_to_dataframe(file_id):
    export_link = f'https://drive.google.com/uc?id={file_id}'
    
    with requests.Session() as session:
        response = session.get(export_link, headers={'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'})

    if response.status_code == 200:
        print(response.headers.get('Content-Type'))  # Print Content-Type for debugging
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
        return df
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
