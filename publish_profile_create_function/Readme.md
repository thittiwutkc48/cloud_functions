How to Depoly on GCP
Use ls find directory you are stay 
Use cd stay on main.py level

Cloud Functions
gcloud functions deploy publish_profile_create_function \
  --gen2 \
  --region=asia-southeast1 \
  --runtime=python310 \
  --source=. \
  --entry-point=publish_profile_create_function \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512MB \
  --timeout=540s

Deploy with variable
gcloud functions deploy publish_profile_create_function \
  --gen2 \
  --region=asia-southeast1 \
  --runtime=python310 \
  --source=. \
  --entry-point=publish_profile_create_function \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512MB \
  --timeout=540s \
  --set-env-vars STG_DATASET=slp_grading_stg,PROFILE_DATASET=slp_profile_lz,PROFILE_TABLE=profile_identifiers,NO_PROFILE_TABLE=grade_no_profiles,NO_PROFILE_ERROR_TABLE=grade_no_profiles_error


Cloud Scheduler
gcloud scheduler jobs create http compare-grade-1200 \
  --schedule="0 12 23 * *" \
  --uri="https://asia-southeast1-single-loyalty-platform.cloudfunctions.net/publish_profile_create_function" \
  --http-method=POST \
  --message-body='{"event_name":"COMPAREGRADE"}' \
  --time-zone="Asia/Bangkok" \
  --location="asia-southeast1" \
  --headers="Content-Type=application/json"


