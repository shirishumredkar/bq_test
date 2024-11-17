import json
import pandas as pd
import argparse
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set up BigQuery client
bq_client = bigquery.Client()

def load_json_from_bq(project_id, dataset_id, table_id, column_name, period):
    query = f"""
    SELECT {column_name}, period
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE period = '{period}'
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    data = []
    for row in results:
        content = json.loads(row[column_name])
        content['period'] = row['period'].isoformat()  # Convert period to string
        data.append(content)
    return data

def flatten_json(nested_json):
    """
    Flatten JSON structure dynamically without hardcoding columns.
    """
    flat_records = []
    period = nested_json.pop('period', None)
    for emp in nested_json["emp_table"]:
        flattened_record = {}
        flattened_record['period'] = period
        for key, value in emp.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, dict):
                        for sub_sub_key, sub_sub_value in sub_value.items():
                            flattened_record[f"{key}_{sub_key}_{sub_sub_key}"] = sub_sub_value
                    else:
                        flattened_record[f"{key}_{sub_key}"] = sub_value
            elif isinstance(value, list):
                flattened_record[key] = json.dumps(value)  # Store list as JSON string
            else:
                flattened_record[key] = value
        flat_records.append(flattened_record)
    return flat_records

def detect_schema(flat_data):
    df = pd.DataFrame(flat_data)
    schema = []
    for column, dtype in zip(df.columns, df.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            field_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(column, field_type))
    return schema

def create_or_update_table(dataset_id, table_id, flat_data):
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Check if the table exists
    try:
        table = bq_client.get_table(table_ref)
        print(f"Table {table_id} exists. Checking for schema updates...")
        update_table_schema(table, flat_data)
    except NotFound:
        # If it does not exist, create the table
        print(f"Table {table_id} not found. Creating table and loading data...")
        schema = detect_schema(flat_data)
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)
        print(f"Created table {table_id} in dataset {dataset_id}.")
        load_data_to_bq(dataset_id, table_id, flat_data)

def update_table_schema(table, flat_data):
    current_schema = {field.name: field for field in table.schema}
    new_fields = detect_schema(flat_data)
    for new_field in new_fields:
        if new_field.name not in current_schema:
            print(f"Adding new field: {new_field.name}")
            current_schema[new_field.name] = new_field
    # Update table schema with new fields
    table.schema = list(current_schema.values())
    bq_client.update_table(table, ["schema"])
    print(f"Updated table schema for {table.table_id}.")
    append_data_to_bq(table.dataset_id, table.table_id, flat_data)

def append_data_to_bq(dataset_id, table_id, flat_data):
    df = pd.DataFrame(flat_data)
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = bq_client.load_table_from_dataframe(df, f"{dataset_id}.{table_id}", job_config=job_config)
    job.result()
    print(f"Appended data into {dataset_id}.{table_id}.")

def load_data_to_bq(dataset_id, table_id, flat_data):
    df = pd.DataFrame(flat_data)
    job = bq_client.load_table_from_dataframe(df, f"{dataset_id}.{table_id}")
    job.result()
    print(f"Loaded data into {dataset_id}.{table_id}.")

# Main function
def main():
    parser = argparse.ArgumentParser(description='Load and flatten JSON data from BigQuery.')
    parser.add_argument('--project_id', type=str, required=True, help='GCP project ID')
    parser.add_argument('--dataset_id', type=str, required=True, help='BigQuery dataset ID')
    parser.add_argument('--source_table_id', type=str, required=True, help='BigQuery source table ID')
    parser.add_argument('--target_table_id', type=str, required=True, help='BigQuery target table ID')
    parser.add_argument('--column_name', type=str, required=True, help='Column name containing JSON data')
    parser.add_argument('--period', type=str, required=True, help='Period date to filter data')

    args = parser.parse_args()

    # Load JSON data from BigQuery
    json_data = load_json_from_bq(args.project_id, args.dataset_id, args.source_table_id, args.column_name, args.period)

    # Flatten JSON data row-by-row
    flat_data = []
    for record in json_data:
        flat_data.extend(flatten_json(record))

    # Create or update the BigQuery table
    create_or_update_table(args.dataset_id, args.target_table_id, flat_data)

if __name__ == '__main__':
    main()
