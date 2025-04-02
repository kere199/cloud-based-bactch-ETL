Project Overview
The Coursera ETL Pipeline automates the process of collecting course data from Coursera, transforming it into a structured format, and loading it into BigQuery for analysis. The pipeline consists of two main components:

Extract Function (extract_to_gcs): Scrapes course data from Coursera’s GraphQL API, uploads the data as a JSON file to GCS, and triggers the load function.
Load Function (gcs_to_bigquery): Downloads the JSON file from GCS, transforms the data, and loads it into a BigQuery table.
The pipeline is triggered daily by a single Cloud Scheduler job, which calls the extract_function. The extract_function then directly triggers the load_function via an HTTP request, ensuring a seamless end-to-end process.

Architecture
The pipeline uses the following Google Cloud services:

Cloud Scheduler: Triggers the extract_function daily at midnight UTC.
Cloud Run: Hosts the extract_function and load_function as serverless functions.
Google Cloud Storage (GCS): Stores the scraped Coursera data as JSON files.
BigQuery: Stores the transformed data in a table for analysis.
Workflow
Cloud Scheduler triggers the extract_function (etl-kere) daily at midnight UTC.
The extract_function:
Scrapes course data from Coursera’s GraphQL API.
Uploads the data as a timestamped JSON file to GCS (e.g., gs://zambara/zambara/kere/coursera_courses_20250401_123456.json).
Triggers the load_function by making an HTTP POST request with the GCS URI.
The load_function:
Downloads the JSON file from GCS.
Transforms the data (e.g., joins partner names, standardizes fields).
Loads the data into the coursera_courses table in BigQuery.
