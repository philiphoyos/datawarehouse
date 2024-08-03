import pandas as pd
import uuid

def transform_customers(data):
    data['surrogate_key'] = [str(uuid.uuid4()) for _ in range(len(data))]
    data['start_date'] = pd.to_datetime('today')
    data['end_date'] = pd.NaT
    return data
