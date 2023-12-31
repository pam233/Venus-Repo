import pandas as pd
import re

# Function to remove non-UTF-8 characters from text


def clean_text(text):
    non_utf8_pattern = re.compile('[^\x00-\x7F]+')
    cleaned_text = re.sub(non_utf8_pattern, '', text)
    return cleaned_text

# Function to clean all text columns in a DataFrame


def clean_data_from_utf8(df):
    # Apply the clean_text function to all text columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(clean_text)
    return df


def load_data_csv(file_path):
    return pd.read_csv(file_path)


def explore_data(data):
    print(data.head())
    print(data.describe())
    print(data.isnull().sum())


def remove_special_characters(dataframe, column):
    regex_pattern = r'[@_!#$%^&*()<>?/\|}{~:]Ã¶©¼‰ã‚¹ãƒ‘ã‚²ãƒƒãƒ†ã‚£ãƒ¼'
    rows_to_remove = []

    for index, row in df.iterrows():
        if any(char in regex_pattern for char in row['text_column']):
            rows_to_remove.append(index)

    df = df.drop(rows_to_remove)


def preprocess_data(data):
    data = data.dropna()
    data = data.drop_duplicates()
    data['release_date'] = pd.to_datetime(data['release_date'])
    return data

