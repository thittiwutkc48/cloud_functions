import functions_framework
from google.cloud import bigquery, pubsub_v1
import json
from datetime import datetime
import pytz
from helper import handle_max_retries

bangkok_tz = pytz.timezone('Asia/Bangkok')

client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = "projects/single-loyalty-platform/topics/create-profile-topic"

@functions_framework.http
def publish_profile_create(request):
    request_json = request.get_json(silent=True)
    event_name = request_json["event_name"]

    if event_name == "COMPAREGRADE" :
        print("Event:COMPAREGRADE")
        stg_dataset_name = "slp_grading_stg"
        pf_dataset_name = "slp_profile_lz"
        pf_table_name = "profile_identifiers"
        grade_table_name = "grade_no_profiles"
        grade_error_data = []
        merge_query = f"""
        MERGE INTO {stg_dataset_name}.{grade_table_name} AS TARGET
        USING (
            SELECT grade.iden_no , profile_status , updated_at
            FROM {stg_dataset_name}.{grade_table_name} grade
            LEFT JOIN {pf_dataset_name}.{pf_table_name} idx USING (iden_no)
            WHERE idx.iden_no IS NOT NULL
        ) AS SOURCE
        ON TARGET.iden_no = SOURCE.iden_no
        WHEN MATCHED THEN
        UPDATE SET
            TARGET.profile_status = 'CREATION_SUCCESS',
            TARGET.updated_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR)
        """
        
        client.query(merge_query).result()
        print("MERGE query executed successfully.")
        
        select_query = f"""
        SELECT * FROM {stg_dataset_name}.{grade_table_name}
        WHERE profile_status = 'CREATION_PENDING' AND retry_count <= 5
        """
        results = client.query(select_query).result()

        for row in results:
            force_create = row.force_create
            retry_count = row.retry_count
            iden_no = row.iden_no

            if force_create is False :
                event_data = {
                    "event_id": str(row.event_id),
                    "event_name": "profile.create",
                    "event_timestamp": datetime.now(bangkok_tz).isoformat(),
                    "x_correlation_id": str(row.x_correlation_id),
                    "event_parameter": {
                        "profile_value": row.iden_no,
                        "profile_type": row.iden_type
                    }
                }
            elif force_create is True :
                event_data = {
                    "event_id": str(row.event_id),  
                    "event_name": "profile.force_create",
                    "event_timestamp": datetime.now(bangkok_tz).isoformat(), 
                    "x_correlation_id": str(row.x_correlation_id),  
                    "event_parameter": {
                        "profile_value": row.iden_no,
                        "profile_type": row.iden_type,
                        "profile_subtype": row.iden_subtype,
                        "profile_force_create_info": {
                            "title": row.title,          
                            "first_name": row.first_name,    
                            "last_name": row.last_name, 
                            "birth_date": row.birth_date
                        }
                    }
                }
        
            try:
                if retry_count == 5:
                    grade_error_data.append(row)
                
                else :
                    future = publisher.publish(topic_path, json.dumps(event_data).encode("utf-8"))
                    future.result() 
                    print(f"Published message for iden_no {iden_no}")

            except Exception as e:
                print(f"Error publishing message for iden_no {iden_no}: {e}")


        update_query = f"""
            UPDATE {stg_dataset_name}.{grade_table_name}
            SET retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP()
            WHERE profile_status = 'CREATION_PENDING' AND retry_count < 5
        """
        client.query(update_query).result()
        print("UPDATE query executed successfully.")

        if grade_error_data :
            handle_max_retries(grade_error_data)
        
    return "Success"