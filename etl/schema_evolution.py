import pandas as pd
import json
import pyarrow.parquet as pq
import pyarrow as pa
import os

def get_current_schema(metadata_path):
    with open(metadata_path, 'r') as file:
        return json.load(file)

def update_metadata(metadata, metadata_path):
    with open(metadata_path, 'w') as file:
        json.dump(metadata, file, indent=4)

def apply_schema_evolution(existing_schema, new_schema):
    existing_columns = set(existing_schema.keys())
    new_columns = set(new_schema.keys())
    
    added_columns = new_columns - existing_columns
    removed_columns = existing_columns - new_columns
    
    return added_columns, removed_columns

def alter_parquet_schema(parquet_file_path, added_columns):
    if not added_columns:
        return
    
    table = pq.read_table(parquet_file_path)
    for column in added_columns:
        table = table.append_column(column, pa.array([None] * table.num_rows, pa.string()))
    pq.write_table(table, parquet_file_path)

    # Possible replace with  
    # table = pq.read_table(parquet_file_path)
    # new_fields = table.schema.append(pa.schema([(col, pa.string()) for col in added_columns]))

    # new_table = pa.Table.from_arrays([table.column(i) for i in range(table.num_columns)] + [pa.array([])] * len(added_columns), schema=new_fields)
    # pq.write_table(new_table, parquet_file_path)
    

# # Example usage
# metadata_path = 'metadata/products_metadata.json'
# parquet_file_path = 'warehouse/product_dim.parquet'

# # New schema with an additional column
# new_schema = {
#     "product_id": "int",
#     "product_name": "string",
#     "category": "string",
#     "price": "float",
#     "product_description": "string",  # New column
#     "start_date": "date",
#     "end_date": "date"
# }

# # Load existing schema from metadata
# current_metadata = get_current_schema(metadata_path)
# existing_schema = {field['name']: field['type'] for field in current_metadata['fields']}

# # Detect schema changes
# added_columns, removed_columns = apply_schema_evolution(existing_schema, new_schema)

# # Apply schema changes to Parquet file
# alter_parquet_schema(parquet_file_path, added_columns)

# # Update metadata with new schema
# current_metadata['fields'].append({"name": "product_description", "type": "string"})
# update_metadata(current_metadata, metadata_path)
