# import functions_framework
# import requests
# import json
# from google.cloud import storage

# # GraphQL endpoint and headers
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

# # GraphQL query and variables
# PAYLOAD = {
#     "operationName": "DiscoveryCollections",
#     "variables": {"contextType": "PAGE", "contextId": "search-zero-state"},
#     "query": """
#     query DiscoveryCollections($contextType: String!, $contextId: String!, $passThroughParameters: [DiscoveryCollections_PassThroughParameter!]) {
#       DiscoveryCollections {
#         queryCollections(input: {contextType: $contextType, contextId: $contextId, passThroughParameters: $passThroughParameters}) {
#           ...DiscoveryCollections_DiscoveryCollection
#           __typename
#         }
#         __typename
#       }
#     }
#     fragment DiscoveryCollections_DiscoveryCollection on DiscoveryCollections_productCollection {
#       __typename
#       id
#       label
#       linkedCollectionPageMetadata { url __typename }
#       entities { ...DiscoveryCollections_DiscoveryEntity __typename }
#     }
#     fragment DiscoveryCollections_DiscoveryEntity on DiscoveryCollections_learningProduct {
#       __typename
#       id
#       slug
#       name
#       url
#       partnerIds
#       imageUrl
#       partners { ...DiscoveryCollections_DiscoveryCollectionsPartner __typename }
#       ... on DiscoveryCollections_specialization { courseCount difficultyLevel isPartOfCourseraPlus productCard { ...DiscoveryCollections_ProductCard __typename } }
#       ... on DiscoveryCollections_professionalCertificate { difficultyLevel isPartOfCourseraPlus productCard { ...DiscoveryCollections_ProductCard __typename } }
#     }
#     fragment DiscoveryCollections_DiscoveryCollectionsPartner on DiscoveryCollections_partner { id name logo __typename }
#     fragment DiscoveryCollections_ProductCard on ProductCard_ProductCard { id marketingProductType productTypeAttributes { ... on ProductCard_Specialization { isPathwayContent __typename } __typename } __typename }
#     """
# }

# def fetch_graphql_data():
#     try:
#         response = requests.post(ENDPOINT, headers=HEADERS, json=PAYLOAD)
#         response.raise_for_status()
#         data = response.json()
#         return data["data"]["DiscoveryCollections"]["queryCollections"][0]["entities"]
#     except requests.exceptions.RequestException as e:
#         print(f"Error fetching data: {e}")
#         return None

# @functions_framework.http
# def extract_to_gcs(request):
#     """HTTP Cloud Function to scrape Coursera data and upload to GCS."""
#     # Fetch data from Coursera
#     entities = fetch_graphql_data()
#     if not entities:
#         return "Failed to fetch data", 500

#     # Configuration
#     bucket_name = "zambara"  # Your bucket name
#     destination_path = "zambara/kere/coursera_courses.json"

#     # Upload to GCS using default ADC
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_path)
#     blob.upload_from_string(json.dumps(entities, indent=4), content_type="application/json")

#     return f"Uploaded Coursera data to gs://{bucket_name}/{destination_path}", 200

import functions_framework
import requests
import json
from google.cloud import storage
import logging

logging.basicConfig(level=logging.INFO)

ENDPOINT = "https://www.coursera.org/graphql-gateway?opname=DiscoveryCollections"
HEADERS = {
    "content-type": "application/json",
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
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
    """HTTP Cloud Function to scrape Coursera data and upload to GCS."""
    entities = fetch_graphql_data()
    if not entities:
        logging.error("No data fetched from Coursera")
        return "Failed to fetch data from Coursera", 500

    bucket_name = "zambara"
    destination_path = "zambara/kere/coursera_courses.json"

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_string(json.dumps(entities, indent=4), content_type="application/json")
        logging.info("Uploaded data to gs://%s/%s", bucket_name, destination_path)
    except Exception as e:
        logging.error("Failed to upload to GCS: %s", str(e))
        return "Failed to upload to GCS", 500

    return f"Uploaded Coursera data to gs://{bucket_name}/{destination_path}", 200