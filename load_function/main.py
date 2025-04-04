# import functions_framework
# from google.cloud import storage
# from google.cloud import bigquery
# import json
# import pandas as pd
# import logging

# logging.basicConfig(level=logging.INFO)

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
#             "course_count": entity.get("courseCount", "")
#         }
#         cleaned_data.append(course_info)
#     return cleaned_data

# def load_data(bucket_name, file_name):
#     """Helper function to load data from GCS to BigQuery."""
#     logging.info("Loading data: bucket=%s, file=%s", bucket_name, file_name)

#     try:
#         storage_client = storage.Client()
#         bucket = storage_client.bucket(bucket_name)
#         blob = bucket.blob(file_name)
#         json_content = blob.download_as_string().decode("utf-8")
#         entities = json.loads(json_content)
#     except Exception as e:
#         logging.error("Failed to download or parse GCS file: %s", str(e))
#         return "Failed to download or parse GCS file", 500

#     cleaned_data = clean_data(entities)
#     df = pd.DataFrame(cleaned_data)

#     # BigQuery configuration
#     project_id = "vital-cathode-454012-k0"
#     dataset_id = "ETL_pipeline_kere"
#     table_id = "coursera_courses"

#     try:
#         bq_client = bigquery.Client(project=project_id)
#         table_ref = f"{project_id}.{dataset_id}.{table_id}"

#         schema = [
#             bigquery.SchemaField("type", "STRING"),
#             bigquery.SchemaField("id", "STRING"),
#             bigquery.SchemaField("name", "STRING"),
#             bigquery.SchemaField("slug", "STRING"),
#             bigquery.SchemaField("url", "STRING"),
#             bigquery.SchemaField("partners", "STRING"),
#             bigquery.SchemaField("difficulty", "STRING"),
#             bigquery.SchemaField("coursera_plus", "STRING"),
#             bigquery.SchemaField("image_url", "STRING"),
#             bigquery.SchemaField("course_count", "STRING")
#         ]

#         job_config = bigquery.LoadJobConfig(
#             schema=schema,
#             write_disposition="WRITE_TRUNCATE",
#             source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
#         )
#         df.to_gbq(table_ref, project_id=project_id, if_exists="replace", table_schema=schema)
#         logging.info("Loaded %s into %s", file_name, table_ref)
#     except Exception as e:
#         logging.error("Failed to load data into BigQuery: %s", str(e))
#         return "Failed to load data into BigQuery", 500

#     return f"Loaded {file_name} into {table_ref}", 200

# @functions_framework.cloud_event
# def load_to_bigquery(cloud_event):
#     """Cloud Function triggered by GCS event to load data into BigQuery."""
#     logging.info("Received Cloud Event: %s", cloud_event)
#     data = cloud_event.data
#     bucket_name = data["bucket"]
#     file_name = data["name"]
#     # Only process the specific file we care about
#     if file_name != "zambara/kere/coursera_courses.json":
#         logging.info("Ignoring file: %s", file_name)
#         return "Ignoring file", 200
#     return load_data(bucket_name, file_name)

# @functions_framework.http
# def load_to_bigquery_http(request):
#     """HTTP Cloud Function to load data into BigQuery (for manual testing)."""
#     content_type = request.headers.get("Content-Type")
#     if content_type != "application/json":
#         logging.error("Unsupported Content-Type: %s", content_type)
#         return "Unsupported Media Type: Content-Type must be application/json", 415

#     raw_body = request.get_data(as_text=True)
#     logging.info("Raw request body: %s", raw_body)

#     try:
#         request_json = request.get_json(silent=True)
#         if not request_json:
#             logging.error("Failed to parse JSON: %s", raw_body)
#             return "Failed to parse JSON", 400
#         if "data" not in request_json:
#             logging.error("Missing 'data' key in payload: %s", request_json)
#             return "Missing 'data' key in payload", 400
#         data = request_json["data"]
#         if "bucket" not in data or "name" not in data:
#             logging.error("Missing 'bucket' or 'name' in data: %s", data)
#             return "Missing 'bucket' or 'name' in data", 400
#         bucket_name = data["bucket"]
#         file_name = data["name"]
#         return load_data(bucket_name, file_name)
#     except Exception as e:
#         logging.error("Error processing HTTP request: %s", str(e))
#         return "Error processing request", 500


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
        try:
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
                "course_count": str(entity.get("courseCount", ""))
            }
            cleaned_data.append(course_info)
        except KeyError as e:
            logging.error("Missing key in entity: %s, entity: %s", str(e), entity)
            continue
    return cleaned_data

def load_to_bigquery(gcs_uri, dataset_id, table_id, write_disposition="WRITE_TRUNCATE"):
    """Load data from GCS to BigQuery."""
    logging.info("Loading data from GCS URI: %s", gcs_uri)

    # Download and parse the GCS file
    try:
        uri_parts = gcs_uri.replace("gs://", "").split("/", 1)
        bucket_name = uri_parts[0]
        file_name = uri_parts[1]
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        json_content = blob.download_as_string().decode("utf-8")
        entities = json.loads(json_content)
        logging.info("Successfully downloaded and parsed GCS file: %d entities", len(entities))
    except Exception as e:
        logging.error("Failed to download or parse GCS file: %s", str(e))
        return f"Failed to download or parse GCS file: {str(e)}", 500

    # Clean the data
    cleaned_data = clean_data(entities)
    if not cleaned_data:
        logging.error("No valid data after cleaning")
        return "No valid data after cleaning", 500
    df = pd.DataFrame(cleaned_data)
    logging.info("Cleaned data: %d rows, columns: %s", len(df), df.columns.tolist())

    # BigQuery configuration
    project_id = "vital-cathode-454012-k0"
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
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        df.to_gbq(table_ref, project_id=project_id, if_exists="replace", table_schema=schema)
        logging.info("Loaded %s into %s", file_name, table_ref)

        # Verify the number of rows loaded
        table = bq_client.get_table(table_ref)
        logging.info("Loaded %d rows into BigQuery table %s", table.num_rows, table_ref)
        return table.num_rows
    except Exception as e:
        logging.error("Failed to load data into BigQuery: %s", str(e))
        return f"Failed to load data into BigQuery: {str(e)}", 500

@functions_framework.http
def gcs_to_bigquery(request):
    """HTTP Cloud Function to load data from GCS to BigQuery."""
    # Parse request parameters
    request_json = request.get_json(silent=True)

    if not request_json or "gcs_uri" not in request_json:
        logging.error("Missing 'gcs_uri' parameter in request")
        return json.dumps({
            "status": "error",
            "message": "Missing 'gcs_uri' parameter"
        }), 400

    gcs_uri = request_json["gcs_uri"]
    dataset_id = request_json.get("dataset_id", "ETL_pipeline_kere")
    table_id = request_json.get("table_id", "coursera_courses")
    write_disposition = request_json.get("write_disposition", "WRITE_TRUNCATE")

    # Load to BigQuery
    logging.info("Loading data from %s to BigQuery table %s.%s", gcs_uri, dataset_id, table_id)
    try:
        rows_loaded = load_to_bigquery(gcs_uri, dataset_id, table_id, write_disposition)
        if isinstance(rows_loaded, str):  # Error occurred
            return json.dumps({
                "status": "error",
                "message": rows_loaded
            }), 500
        logging.info("Loaded %d rows to BigQuery table %s.%s", rows_loaded, dataset_id, table_id)
        return json.dumps({
            "status": "success",
            "message": f"Loaded {rows_loaded} rows to BigQuery table {dataset_id}.{table_id}",
            "rows_loaded": rows_loaded
        }), 200
    except Exception as e:
        logging.error("Failed to load data to BigQuery: %s", str(e))
        return json.dumps({
            "status": "error",
            "message": f"Failed to load data to BigQuery: {str(e)}"
        }), 500