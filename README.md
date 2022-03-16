# Sample exercise Cloud Function

**Problem Statement -**

Write a cloud function that processes and upload the files into BigQuery along with an additional audit field - ingestion_date<br>
Note: Cloud function should trigger on arrival of file in bucket

**Solution -**

Step 1 : Create a temporary csv file with an additional field "ingestion_date" - *create_csv_with_ingest_time*<br>
Step 2 : Load this temporary csv file to bigquery using standard functions - *load_csv_to_bigQuery*<br>
Step 3 : Orchestrate the above two steps on receiving a file in GCS - *file_finalized*<br>

**Test Cases -**

1. Handle empty CSV file
2. Handle CSV files with incosistant number of columns
3. Assert number of rows in CSV file is equal to the number of rows inserted in bigquery

**Create Cloud Function -**

> gcloud functions deploy GCS_to_BQ_load --env-vars-file env.yaml \
--entry-point file_finalized --runtime python37 --trigger-resource my-project-1548066830003-data \
--trigger-event google.storage.object.finalize

**Run Test Script -**

> python3 -m unittest test_gcs_to_bq

