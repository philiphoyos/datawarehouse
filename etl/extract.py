import pandas as pd
from datetime import datetime
from config.logging_config import logger

def extract_data(source, transaction_date='load_data', file_path=None, last_extract_date=None):
    # Example function to extract data from a source
    if source == 'csv':
        logger.info(f"Processing {source} in {file_path}, last_extract_date: {last_extract_date}")
        if last_extract_date:
            data = pd.read_csv(file_path, sep='\t', lineterminator='\r')

            if transaction_date in data.columns and transaction_date is not None:
                data[transaction_date] = pd.to_datetime(data[transaction_date])
            else:
                data['import_date'] = pd.to_datetime(pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
                transaction_date = 'import_date'
            new_data = data[data[transaction_date] > last_extract_date]
            return new_data
        else:
            return pd.read_csv(file_path)
        
    elif source == 'database':
        # Logic to extract data from a database
        pass

    # return data

def stage_data(data, destination_path):
    data.to_parquet(destination_path, index=False)