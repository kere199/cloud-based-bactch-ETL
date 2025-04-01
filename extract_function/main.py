import functions_framework
import requests
import json
from google.cloud import storage
import logging
import datetime

logging.basicConfig(level=logging.INFO)

ENDPOINT = "https://www.coursera.org/graphql-gateway?opname=DiscoveryCollections"
HEADERS = {
    "content-type": "application/json",
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_`7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "origin": "https://www.coursera.org",
    "referer": "https://www.coursera.org/search?query=AI",
    "apollographql-client-name": "search-v2",
    "apollographql-client-version": "7632fa15d8fa40347c6e478bba89b2bbc90f70a7",
}

PAYLOAD = {
    "operationName": "DiscoveryCollections",
    "variables": {"contextType": "PAGE", "contextId": "search-zero-state"},
    "query": "query DiscoveryCollections($contextType: String!, $contextId: String!, $passThroughParameters: [DiscoveryCollections_PassThroughParameter!]) { DiscoveryCollections { queryCollections(input: {contextType: $contextType, contextId: $contextId, passThroughParameters: $passThroughParameters}) { ...DiscoveryCollections_DiscoveryCollection __typename } __typename } } fragment DiscoveryCollections_DiscoveryCollection on DiscoveryCollections_productCollection { __typename id label linkedCollectionPageMetadata { url __typename } entities { ...DiscoveryCollections_DiscoveryEntity __typename } } fragment DiscoveryCollections_DiscoveryEntity on DiscoveryCollections_learningProduct { __typename id slug name url partnerIds imageUrl partners { ...DiscoveryCollections_DiscoveryCollectionsPartner __typename } ... on DiscoveryCollections_specialization { courseCount difficultyLevel isPartOfCourseraPlus productCard { ...DiscoveryCollections_ProductCard __typename } } ... on DiscoveryCollections_professionalCertificate { difficultyLevel isPartOfCourseraPlus productCard { ...DiscoveryCollections_ProductCard __typename } } } fragment DiscoveryCollections_DiscoveryCollectionsPartner on DiscoveryCollections_partner { id name logo __typename } fragment DiscoveryCollections_ProductCard on ProductCard_ProductCard { id marketingProductType productTypeAttributes { ... on ProductCard_Specialization { isPathwayContent __typename } __typename } __typename }"
}

def fetch_graphql_data():
    try:
        logging.info("Sending request to Coursera GraphQL endpoint")
        response = requests.post(ENDPOINT, headers=HEADERS, json=PAYLOAD)
        response.raise_for_status()
        data = response.json()
        logging.info("Response received: %s", json.dumps(data, indent=2))
        if "errors" in data:
            logging.error("GraphQL errors: %s", json.dumps(data["errors"]))
            return None
        if not data.get("data") or not data["data"].get("DiscoveryCollections"):
            logging.error("Unexpected response structure: %s", json.dumps(data))
            return None
        return data["data"]["DiscoveryCollections"]["queryCollections"][0]["entities"]
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch Coursera data: %s", str(e))
        return None

@functions_framework.http
def extract_to_gcs(request):
    """HTTP Cloud Function to scrape Coursera data, upload to GCS, and trigger load to BigQuery."""
    entities = fetch_graphql_data()
    if not entities:
        logging.error("No data fetched from Coursera")
        return json.dumps({
            "status": "error",
            "message": "Failed to fetch data from Coursera"
        }), 500

    bucket_name = "zambara"
    # Add a timestamp to the filename to create a unique file
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    destination_path = f"zambara/kere/coursera_courses_{timestamp}.json"

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_string(json.dumps(entities, indent=4), content_type="application/json")
        logging.info("Uploaded data to gs://%s/%s", bucket_name, destination_path)
    except Exception as e:
        logging.error("Failed to upload to GCS: %s", str(e))
        return json.dumps({
            "status": "error",
            "message": f"Failed to upload to GCS: {str(e)}"
        }), 500

    gcs_uri = f"gs://{bucket_name}/{destination_path}"

    # Trigger the load_function (gcs_to_bigquery) via HTTP
    load_function_url = "https://etl-kere-load-82546987242.us-central1.run.app"  # Replace with your load_function URL
    load_payload = {
        "gcs_uri": gcs_uri,
        "dataset_id": "ETL_pipeline_kere",
        "table_id": "coursera_courses",
        "write_disposition": "WRITE_TRUNCATE"
    }

    try:
        # Use the same service account credentials for authentication
        # In Cloud Run, the default service account is used automatically
        response = requests.post(
            load_function_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {get_id_token()}"
            },
            json=load_payload
        )
        response.raise_for_status()
        load_response = response.json()
        logging.info("Triggered load_function: %s", load_response)
        return json.dumps({
            "status": "success",
            "message": "Extracted Coursera data, uploaded to GCS, and triggered load to BigQuery",
            "gcs_uri": gcs_uri,
            "load_response": load_response
        }), 200
    except requests.exceptions.RequestException as e:
        logging.error("Failed to trigger load_function: %s", str(e))
        return json.dumps({
            "status": "error",
            "message": f"Failed to trigger load_function: {str(e)}"
        }), 500

def get_id_token():
    """Get an ID token for authentication."""
    import google.auth
    import google.auth.transport.requests
    creds, _ = google.auth.default()
    request = google.auth.transport.requests.Request()
    creds.refresh(request)
    return creds.id_token

# import functions_framework
# import requests
# import json
# from google.cloud import storage
# import logging

# logging.basicConfig(level=logging.INFO)

# ENDPOINT = "https://www.coursera.org/graphql-gateway?opname=DiscoveryCollections"
# HEADERS = {
#     "content-type": "application/json",
#     "accept": "application/json",
#     "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
#     "origin": "https://www.coursera.org",
#     "referer": "https://www.coursera.org/search?query=AI",
#     "apollographql-client-name": "search-v2",
#     "apollographql-client-version": "7632fa15d8fa40347c6e478bba89b2bbc90f70a7",
# }

# PAYLOAD = {
#     "operationName": "DiscoveryCollections",
#     "variables": {"contextType": "PAGE", "contextId": "search-zero-state"},
#     "query": "query DiscoveryCollections($contextType: String!, $contextId: String!, $passThroughParameters: [DiscoveryCollections_PassThroughParameter!]) { DiscoveryCollections { queryCollections(input: {contextType: $contextType, contextId: $contextId, passThroughParameters: $passThroughParameters}) { ...DiscoveryCollections_DiscoveryCollection __typename } __typename } } fragment DiscoveryCollections_DiscoveryCollection on DiscoveryCollections_productCollection { __typename id label linkedCollectionPageMetadata { url __typename } entities { ...DiscoveryCollections_DiscoveryEntity __typename } } fragment DiscoveryCollections_DiscoveryEntity on DiscoveryCollections_learningProduct { __typename id slug name url partnerIds imageUrl partners { ...DiscoveryCollections_DiscoveryCollectionsPartner __typename } ... on DiscoveryCollections_specialization { courseCount difficultyLevel isPartOfCourseraPlus productCard { ...DiscoveryCollections_ProductCard __typename } } ... on DiscoveryCollections_professionalCertificate { difficultyLevel isPartOfCourseraPlus productCard { ...DiscoveryCollections_ProductCard __typename } } } fragment DiscoveryCollections_DiscoveryCollectionsPartner on DiscoveryCollections_partner { id name logo __typename } fragment DiscoveryCollections_ProductCard on ProductCard_ProductCard { id marketingProductType productTypeAttributes { ... on ProductCard_Specialization { isPathwayContent __typename } __typename } __typename }"
# }

# def fetch_graphql_data():
#     try:
#         logging.info("Sending request to Coursera GraphQL endpoint")
#         response = requests.post(ENDPOINT, headers=HEADERS, json=PAYLOAD)
#         response.raise_for_status()
#         data = response.json()
#         logging.info("Response received: %s", json.dumps(data, indent=2))
#         if "errors" in data:
#             logging.error("GraphQL errors: %s", json.dumps(data["errors"]))
#             return None
#         if not data.get("data") or not data["data"].get("DiscoveryCollections"):
#             logging.error("Unexpected response structure: %s", json.dumps(data))
#             return None
#         return data["data"]["DiscoveryCollections"]["queryCollections"][0]["entities"]
#     except requests.exceptions.RequestException as e:
#         logging.error("Failed to fetch Coursera data: %s", str(e))
#         return None

# @functions_framework.http
# def extract_to_gcs(request):
#     """HTTP Cloud Function to scrape Coursera data and upload to GCS."""
#     entities = fetch_graphql_data()
#     if not entities:
#         logging.error("No data fetched from Coursera")
#         return "Failed to fetch data from Coursera", 500

#     bucket_name = "zambara"
#     destination_path = "zambara/kere/coursera_courses.json"

#     try:
#         storage_client = storage.Client()
#         bucket = storage_client.bucket(bucket_name)
#         blob = bucket.blob(destination_path)
#         blob.upload_from_string(json.dumps(entities, indent=4), content_type="application/json")
#         logging.info("Uploaded data to gs://%s/%s", bucket_name, destination_path)
#     except Exception as e:
#         logging.error("Failed to upload to GCS: %s", str(e))
#         return "Failed to upload to GCS", 500

#     return f"Uploaded Coursera data to gs://{bucket_name}/{destination_path}", 200