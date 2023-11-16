import pandas as pd
import requests
from io import BytesIO

def download_and_convert_to_dataframe(file_id):
    export_link = f'https://drive.google.com/uc?id={file_id}'
    
    # Make a request to download the file
    response = requests.get(export_link)

    if response.status_code == 200:
        # Convert the content to a Pandas DataFrame
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
        return df
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

# file_id = '1EDDNjMY23piFi8zCGaySGvlRcbUUqhKc'
# df = download_and_convert_to_dataframe(file_id)

# if df is not None:
#     print(df)
# else:
#     print("Error: DataFrame not created.")
