import pandas as pd

def load_data(new_data, warehouse_file, destination='parquet'):
    if destination == 'parquet':
        # data.to_parquet('path/to/parquet', index=False)
        try:
            existing_data = pd.read_parquet(warehouse_file)
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        except FileNotFoundError:
            combined_data = new_data
            
        combined_data.to_parquet(warehouse_file, index=False)

    
    elif destination == 'database':
        # Logic to load data into a database
        pass
