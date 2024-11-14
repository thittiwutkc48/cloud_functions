from google.cloud import bigquery
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv

load_dotenv()

bangkok_tz = pytz.timezone('Asia/Bangkok')

stg_dataset = os.getenv('STG_DATASET')
error_table=os.getenv('NO_PROFILE_ERROR_TABLE')

def handle_max_retries(grade_error_data):
    client = bigquery.Client()
    table = client.dataset(stg_dataset).table(error_table)

    try:
        client.get_table(table)
        print(f"Table {error_table} already exists.")
    except Exception:
        print(f"Table {error_table} does not exist. Creating table...")
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

        table = bigquery.Table(table, schema=schema)
        table = client.create_table(table)
        print(f"Table {error_table} created.")

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
            "force_create": row["force_create"],  
            "created_at": datetime.now(bangkok_tz).isoformat(), 
            "updated_at": datetime.now(bangkok_tz).isoformat()
        }
        for row in grade_error_data
    ]

    check_query = f"""
        SELECT iden_no, event_id
        FROM `{stg_dataset}.{error_table}`
        WHERE iden_no IN UNNEST(@iden_list) AND event_id IN UNNEST(@event_ids)
    """

    iden_ids = [row["iden_no"] for row in error_rows]
    event_ids = [row["event_id"] for row in error_rows]

    query_params = [
        bigquery.ArrayQueryParameter("iden_list", "STRING", iden_ids),
        bigquery.ArrayQueryParameter("event_ids", "STRING", event_ids)
    ]

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    existing_rows = client.query(check_query, job_config=job_config).result()

    existing_pairs_in_table = set((row["iden_no"], row["event_id"]) for row in existing_rows)

    filtered_error_rows = [
        row for row in error_rows
        if (row["iden_no"], row["event_id"]) not in existing_pairs_in_table
    ]

    if filtered_error_rows:
        errors = client.insert_rows_json(error_table, filtered_error_rows)
        if errors:
            print("Errors occurred during insertion:", errors)
        else:
            print(f"Inserted {len(filtered_error_rows)} records into {error_table}.")
    else:
        print("No new records to insert. All matching iden_no and event_id pairs already exist in the table.")