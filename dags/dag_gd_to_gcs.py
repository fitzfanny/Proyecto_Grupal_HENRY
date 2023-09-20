from airflow.models import DAG

from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator


CREDENTIALS_FILE = '../Credentials/fiery-protocol-399500-d5690a50fe03.json'
GCPCONN = "google_cloud_default"
MY_BUCKET_NAME = 'data-lake'
HENRY_PROJECT = 'fiery-protocol-399500'


default_args = {
        'owner':'Tinmar Andrade',
        'start_date':datetime(2023,9,20),
        'email':['tinmar96@gmail.com',],
        'email_on_failure':True,
        
    }


@dag(
    'gd_to_gcs',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    tags=['HENRY','Proyecto Final','Proyecto en Equipo']
    )


def gd_to_gcs():
    
    create_bucket = GCSCreateBucketOperator(
		bucket_name = MY_BUCKET_NAME,
		location = 'us-east1',
		project_id = HENRY_PROJECT,
		gcp_conn_id="google_cloud_default"
	)

	extract_load = GoogleDriveToGCSOperator(
		bucket_name=MY_BUCKET_NAME,
		object_name="None",
		file_name=MY_FILE_NAME,
		folder_id=MY_FOLDER_ID,
		drive_id="None",
		gcp_conn_id="google_cloud_default",
		impersonation_chain="None",
		)

dag = gd_to_gcs()