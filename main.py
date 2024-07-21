import base64
import functions_framework

import datetime
import os

import requests
from google.cloud import bigquery

class Config:
    dataset_id = os.environ.get("dataset_id")
    table_name = os.environ.get("table_name")
    url = os.environ.get("url")


def get_data(url: str) -> dict:
    """Get data via REST API call using requests package

    Args:
        url (str): The URL of target endpoint.
    
    Returns:
        Dictionary of API response
    """
    response = requests.get(url)
    return response.json()

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def insert_data(cloud_event):
    """Background Cloud Function to be triggered by Pub/Sub.
    """
    client = bigquery.Client()
    dataset_ref = client.dataset(Config.dataset_id)

    raw = get_data(Config.url)
    record = [(
        raw["time"]["updatedISO"], 
        raw["bpi"]["THB"]["rate_float"], 
        raw["bpi"]["USD"]["rate_float"]
        )]
    
    table_ref = dataset_ref.table(Config.table_name)
    table = client.get_table(table_ref)
    result = client.insert_rows(table, record)
    return result
