# import functions_framework
# from google.cloud import storage
# from google.cloud import bigquery
# import json
# import pandas as pd

# def clean_data(entities):
#     cleaned_data = []
#     for entity in entities:
#         partners = ", ".join([partner["name"] for partner in entity["partners"]])
#         course_info = {
#             "type": entity["__typename"].replace("DiscoveryCollections_", ""),
#             "id": entity["id"],
#             "name": entity["name"],
#             "slug": entity["slug"],
#             "url": entity["url"],
#             "partners": partners,
#             "difficulty": entity["difficultyLevel"],
#             "coursera_plus": "Yes" if entity["isPartOfCourseraPlus"] else "No",
#             "image_url": entity["imageUrl"],
#             "course_count": entity.get("courseCount", "")  # Only for specializations
#         }
#         cleaned_data.append(course_info)
#     return cleaned_data

# @functions_framework.cloud_event
# def load_to_bigquery(cloud_event):
#     """Cloud Function triggered by GCS event to load data into BigQuery."""
#     # Get GCS event data
#     data = cloud_event.data
#     bucket_name = data["bucket"]
#     file_name = data["name"]

#     # Download file from GCS using default ADC
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(file_name)
#     json_content = blob.download_as_string().decode("utf-8")
#     entities = json.loads(json_content)

#     # Clean data and convert to DataFrame
#     cleaned_data = clean_data(entities)
#     df = pd.DataFrame(cleaned_data)

    # # BigQuery configuration
    # project_id = "vital-cathode-454012-k0"  # Replace with your project ID
    # dataset_id = "ETL_pipeline_kere"    # Replace with your dataset ID
    # table_id = "coursera_courses"

#     bq_client = bigquery.Client(project=project_id)
#     table_ref = f"{project_id}.{dataset_id}.{table_id}"

#     # Define schema for BigQuery
#     schema = [
#         bigquery.SchemaField("type", "STRING"),
#         bigquery.SchemaField("id", "STRING"),
#         bigquery.SchemaField("name", "STRING"),
#         bigquery.SchemaField("slug", "STRING"),
#         bigquery.SchemaField("url", "STRING"),
#         bigquery.SchemaField("partners", "STRING"),
#         bigquery.SchemaField("difficulty", "STRING"),
#         bigquery.SchemaField("coursera_plus", "STRING"),
#         bigquery.SchemaField("image_url", "STRING"),
#         bigquery.SchemaField("course_count", "STRING")
#     ]

#     # Load to BigQuery using default ADC
#     job_config = bigquery.LoadJobConfig(
#         schema=schema,
#         write_disposition="WRITE_TRUNCATE",  # Overwrite table
#         source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
#     )
#     df.to_gbq(table_ref, project_id=project_id, if_exists="replace", table_schema=schema)

#     return f"Loaded {file_name} into {table_ref}", 200


import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import json
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def clean_data(entities):
    cleaned_data = []
    for entity in entities:
        partners = ", ".join([partner["name"] for partner in entity["partners"]])
        course_info = {
            "type": entity["__typename"].replace("DiscoveryCollections_", ""),
            "id": entity["id"],
            "name": entity["name"],
            "slug": entity["slug"],
            "url": entity["url"],
            "partners": partners,
            "difficulty": entity["difficultyLevel"],
            "coursera_plus": "Yes" if entity["isPartOfCourseraPlus"] else "No",
            "image_url": entity["imageUrl"],
            "course_count": entity.get("courseCount", "")
        }
        cleaned_data.append(course_info)
    return cleaned_data

def load_data(bucket_name, file_name):
    """Helper function to load data from GCS to BigQuery."""
    logging.info("Loading data: bucket=%s, file=%s", bucket_name, file_name)

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        json_content = blob.download_as_string().decode("utf-8")
        entities = json.loads(json_content)
    except Exception as e:
        logging.error("Failed to download or parse GCS file: %s", str(e))
        return "Failed to download or parse GCS file", 500

    cleaned_data = clean_data(entities)
    df = pd.DataFrame(cleaned_data)

    # BigQuery configuration
    project_id = "vital-cathode-454012-k0"  # Replace with your project ID
    dataset_id = "ETL_pipeline_kere"    # Replace with your dataset ID
    table_id = "coursera_courses"

    try:
        bq_client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("slug", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("partners", "STRING"),
            bigquery.SchemaField("difficulty", "STRING"),
            bigquery.SchemaField("coursera_plus", "STRING"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("course_count", "STRING")
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        df.to_gbq(table_ref, project_id=project_id, if_exists="replace", table_schema=schema)
        logging.info("Loaded %s into %s", file_name, table_ref)
    except Exception as e:
        logging.error("Failed to load data into BigQuery: %s", str(e))
        return "Failed to load data into BigQuery", 500

    return f"Loaded {file_name} into {table_ref}", 200

@functions_framework.cloud_event
def load_to_bigquery(cloud_event):
    """Cloud Function triggered by GCS event to load data into BigQuery."""
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    return load_data(bucket_name, file_name)

@functions_framework.http
def load_to_bigquery_http(request):
    """HTTP Cloud Function to load data into BigQuery (for Cloud Scheduler)."""
    content_type = request.headers.get("Content-Type")
    if content_type != "application/json":
        logging.error("Unsupported Content-Type: %s", content_type)
        return "Unsupported Media Type: Content-Type must be application/json", 415

    # Log the raw request body for debugging
    raw_body = request.get_data(as_text=True)
    logging.info("Raw request body: %s", raw_body)

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            logging.error("Failed to parse JSON: %s", raw_body)
            return "Failed to parse JSON", 400
        if "data" not in request_json:
            logging.error("Missing 'data' key in payload: %s", request_json)
            return "Missing 'data' key in payload", 400
        data = request_json["data"]
        if "bucket" not in data or "name" not in data:
            logging.error("Missing 'bucket' or 'name' in data: %s", data)
            return "Missing 'bucket' or 'name' in data", 400
        bucket_name = data["bucket"]
        file_name = data["name"]
        return load_data(bucket_name, file_name)
    except Exception as e:
        logging.error("Error processing HTTP request: %s", str(e))
        return "Error processing request", 500