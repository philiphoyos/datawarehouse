import pandas as pd

def transform_data(new_data, some_unique_identifier=None, existing_data=None):
    if existing_data !=None:
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
    
    else:
        combined_data = new_data

    if some_unique_identifier != None:
        # Sort by a relevant column and remove duplicates (keeping the latest records)
        combined_data.sort_values(by=[some_unique_identifier], inplace=True)
        combined_data.drop_duplicates(subset=[some_unique_identifier], keep='last', inplace=True)

    return combined_data
