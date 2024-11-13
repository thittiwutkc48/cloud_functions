from google.cloud import bigquery
from datetime import datetime
import pytz

bangkok_tz = pytz.timezone('Asia/Bangkok')

def handle_max_retries(grade_error_data):
    client = bigquery.Client()
    dataset_id = "slp_grading_stg"
    table_id = "grade_no_profiles_error"
    errors_table = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(errors_table)
        print(f"Table {table_id} already exists.")
    except Exception:
        print(f"Table {table_id} does not exist. Creating table...")
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

        table = bigquery.Table(errors_table, schema=schema)
        table = client.create_table(table)
        print(f"Table {table_id} created.")

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

    existing_iden_nos = [row['iden_no'] for row in error_rows]
    check_query = f"""
        SELECT iden_no
        FROM `{dataset_id}.{table_id}`
        WHERE iden_no IN UNNEST(@iden_nos)
    """

    query_params = [bigquery.ArrayQueryParameter("iden_nos", "STRING", existing_iden_nos)]
    existing_rows = client.query(check_query, job_config=bigquery.QueryJobConfig(query_parameters=query_params)).result()

    existing_iden_nos_in_table = set(row["iden_no"] for row in existing_rows)

    filtered_error_rows = [row for row in error_rows if row["iden_no"] not in existing_iden_nos_in_table]

    if filtered_error_rows:
        errors = client.insert_rows_json(errors_table, filtered_error_rows)
        if errors:
            print("Errors occurred during insertion:", errors)
        else:
            print(f"Inserted {len(filtered_error_rows)} records into {table_id} table.")
    else:
        print(f"No new records to insert. All iden_no values already exist in the {table_id} table.")