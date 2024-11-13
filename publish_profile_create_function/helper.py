from google.cloud import bigquery
from datetime import datetime
import pytz

bangkok_tz = pytz.timezone('Asia/Bangkok')

def handle_max_retries(grade_error_data):
    client = bigquery.Client()
    dataset_id = "slp_grading_stg"
    table_id = "grade_no_profiles_error"
    errors_table = client.dataset(dataset_id).table(table_id)

    # Check if the table exists, and create if it does not
    try:
        client.get_table(errors_table)
        print(f"Table {table_id} already exists.")
    except Exception:
        print(f"Table {table_id} does not exist. Creating table...")
        # Define the schema for the grade_no_profiles_error table
        schema = [
            bigquery.SchemaField("iden_no", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("x_correlation_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("iden_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("iden_subtype", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("birth_date", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("profile_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("max_retry_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("force_create", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE")
        ]

        # Create the table with the defined schema
        table = bigquery.Table(errors_table, schema=schema)
        table = client.create_table(table)
        print(f"Table {table_id} created.")

    # Prepare rows for insertion with adjusted values
    error_rows = [
        {
            "iden_no": row["iden_no"],
            "event_id": row["event_id"],
            "event_name": row["event_name"],
            "x_correlation_id": row["x_correlation_id"],
            "iden_type": row["iden_type"],
            "iden_subtype": row["iden_subtype"],
            "title": row["title"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "birth_date": row["birth_date"],
            "profile_status": "CREATION_FAILED",
            "max_retry_count": 5,
            "force_create": row["force_create"],  # Directly from source, expects field to be present
            "created_at": datetime.now(bangkok_tz).isoformat(), 
            "updated_at": datetime.now(bangkok_tz).isoformat()
        }
        for row in grade_error_data
    ]

    # Insert rows into grade_no_profiles_error table
    errors = client.insert_rows_json(errors_table, error_rows)
    if errors:
        print("Errors occurred during insertion:", errors)
    else:
        print(f"Inserted {len(error_rows)} records into error table.")