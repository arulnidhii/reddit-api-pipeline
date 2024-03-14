from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.operators.azure_container_instances import AzureContainerInstancesOperator


with DAG(
    dag_id="reddit_data_extraction",
    start_date=datetime(2024, 3, 14),
    schedule_interval="@daily",
    tags=["reddit", "data_extraction"],
) as dag:

    extract_data = AzureContainerInstancesOperator(
        task_id="extract_reddit_data",
        image_uri="redditapi.azurecr.io/reddit_extractor:latest", 
        resource_group="Reddit-api-arul",  
        region="UK South",              
    )
