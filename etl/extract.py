import pandas as pd

def extract_data(source, file_path=None, last_extract_date=None):
    # Example function to extract data from a source
    if source == 'csv':
        if last_extract_date:
            data = pd.read_csv(file_path)
            data['load_date'] = pd.to_datetime(data['load_date'])
            new_data = data[data['load_date'] > last_extract_date]
            return new_data
        else:
            return pd.read_csv(file_path)
        
    elif source == 'database':
        # Logic to extract data from a database
        pass

    return data

def stage_data(data, destination_path):
    data.to_parquet(destination_path, index=False)