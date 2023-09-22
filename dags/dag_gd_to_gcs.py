from airflow.decorators import dag

from datetime import datetime

from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator


CREDENTIALS_FILE = '../Credentials/fiery-protocol-399500-d5690a50fe03.json'
GCPCONN = "google_cloud_default"
MY_BUCKET_NAME = 'data-lake'
HENRY_PROJECT = 'fiery-protocol-399500'


default_args = {
        'owner':'Tinmar Andrade',
        'start_date':datetime(2023,9,20),
        'email':['tinmar96@gmail.com','jozz.rom@gmail.com'],
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
        task_id = 'create_bucket',
		bucket_name = MY_BUCKET_NAME,
		location = 'us-east1',
		project_id = HENRY_PROJECT,
		gcp_conn_id= GCPCONN
		)

	# Carga de datos Yelp
	MY_FOLDER_ID = '1TI-SsMnZsNP6t930olEEWbBQdo_yuIZF' # Folder de Yelp
	for MY_FILE_NAME in ['user.parquet','tip.json','review.json','business.pkl']:
		extract_load_yelp = GoogleDriveToGCSOperator(
        	task_id = 'extract_load_yelp',
			bucket_name=MY_BUCKET_NAME,
			object_name=MY_FILE_NAME,
			file_name=MY_FILE_NAME,
			folder_id=MY_FOLDER_ID,
			drive_id="None",
			gcp_conn_id=GCPCONN
			)

	# Cargar datos de Maps Metadata
	MY_FOLDER_ID = '1olnuKLjT8W2QnCUUwh8uDuTTKVZyxQ0Z'
	for MY_FILE_NAME in range(1,12):
		extract_load_maps_meta = GoogleDriveToGCSOperator(
			task_id = 'extract_load_maps_meta',
			bucket_name=MY_BUCKET_NAME,
			object_name=f'metadata_{MY_FILE_NAME}.json',
			file_name=f'{MY_FILE_NAME}.json',
			folder_id=MY_FOLDER_ID,
			drive_id="None",
			gcp_conn_id=GCPCONN
			)

	# Cargar datos de Maps Estados

	for OBJECT_NAME,MY_FOLDER_ID in zip(['review-New_York','review-California','review-Texas','review-Colorado','review-Georgia'],
                            	['18HYLDXcKg-cC1CT9vkRUCgea04cNpV33','1Jrbjt-0hnLCvecfrnMwGu1jYZSxElJll','1zq12pojMW2zeGgts0lHFSf_pF1L_4UWr','1IlUZJZxOyRiIWo3G6BqW1Thu2kXYKMFX','1MuPznes6CebS6gyWPVU-kR4EVKKLY4l3']):
		
		if OBJECT_NAME == 'review-New_York':
			for MY_FILE_NAME in range(1,19):
				extract_load_maps_newyork = GoogleDriveToGCSOperator(
					task_id = 'extract_load_maps_newyork',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					drive_id="None",
					gcp_conn_id=GCPCONN
					)

		if OBJECT_NAME == 'review-California':
			for MY_FILE_NAME in range(1,19):
				extract_load_maps_california = GoogleDriveToGCSOperator(
					task_id = 'extract_load_maps_california',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					drive_id="None",
					gcp_conn_id=GCPCONN
					)

		if OBJECT_NAME == 'review-Texas':
			for MY_FILE_NAME in range(1,17):
				extract_load_maps_texas = GoogleDriveToGCSOperator(
					task_id = 'extract_load_maps_texas',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					drive_id="None",
					gcp_conn_id=GCPCONN
					)

		if OBJECT_NAME == 'review-Colorado':
			for MY_FILE_NAME in range(1,17):
				extract_load_maps_colorado = GoogleDriveToGCSOperator(
					task_id = 'extract_load_maps_colorado',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					drive_id="None",
					gcp_conn_id=GCPCONN
					)

		if OBJECT_NAME == 'review-Georgia':
			for MY_FILE_NAME in range(1,14):
				extract_load_maps_georgia = GoogleDriveToGCSOperator(
        			task_id = 'extract_load_maps_georgia',
					bucket_name=MY_BUCKET_NAME,
					object_name=f'{OBJECT_NAME[7:]}_{MY_FILE_NAME}.json',
					file_name=f'{MY_FILE_NAME}.json',
					folder_id=MY_FOLDER_ID,
					drive_id="None",
					gcp_conn_id=GCPCONN
					)
    
    

dag = gd_to_gcs()