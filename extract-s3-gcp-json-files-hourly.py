# Importação de bibliotecas
import os
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceGCSToGCSOperator

# Conexões 
AWS_SOURCE_SYNC_ZONE_PATH = 'landing'
GCP_DEST_SYNC_ZONE_PATH = 'gs://caminho-bucket/'
GCP_PROCESSING_BUCKET_PATH = 'processing-s3-zone'
GCP_LANDING_BUCKET_PATH = 'landing-s3-zone'

# Variáveis
default_args = {
    'owner': 'sarah braga',
    'retries': 1,
    'retry_delay': 0
}
current_date = datetime.now()
get_year = current_date.year
get_month = current_date.month
get_day = current_date.day
get_hour = current_date.hour

#------------------------------------------------------------#
# Descrição: Declaração da dag
#------------------------------------------------------------#
@dag(
    dag_id='extract-s3-gcp-json-files-hourly',
    start_date=datetime(2023, 2, 9),
    max_active_runs=1,
    schedule_interval=timedelta(hours=1),
    default_args=default_args,
    catchup=False, # Quando falso, esse parametro não inicia dags extras caso o start date esteja atrasado
    tags=['hourly', 'ingestion', 's3', 'gcs'] # Facilitam a identificação da dag
)

#------------------------------------------------------------#
# Descrição: Controla o fluxo de execução das tarefas da dag
#------------------------------------------------------------#
def ingest_data():

    # Dummy para marcar início da dag (opcional)
    init = DummyOperator(
        task_id='init'
    )

    # Transfere dados do s3 para gcs
    # Caso tenha mais de 1 prefixo para passar, pode utilizar também o dynamic task mapping
    sync_s3_to_gcs_user_json_files = S3ToGCCSOperator(
        task_id='sync_s3_to_gcs_user_json_files'
        bucket=AWS_SOURCE_SYNC_ZONE_PATH, # bucket de origem dos dados
        prefix='user/', # caminho da pasta
        dest_gcs=GCP_DEST_SYNC_ZONE_PATH,
        replace=False, # substitui os arquivos
        gzip = False # trazer arquivos comprimidos também
    )

    # Cria o bucket caso não exista
    # Se existir só ignora
    create_bucket = GCSCreateBucketOperator(
        task_id='create_bucket',
        bucket_name=GCP_PROCESSING_BUCKET_PATH,
        storage_class='REGIONAL',
        location='US-EAST1'
    )

    # Copiar os dados de 1 storage para outro
    copy_files_processing_zone = CloudDataTransferServiceGCSToGCSOperator (
        task_id='copy_files_processing_zone',
        source_bucket=GCP_LANDING_BUCKET_PATH,
        source_path='s3togcs/user/',
        object_conditions={'includePrefixes':f'user_{get_year}_{get_month}_{get_day}_{get_hour}'}, # Traz tudo que está daquela hora para frente
        transfer_options={'overwriteObjectsAlreadyExistingSink':True, 'overwriteWhen': 'NEVER'}, # Não faz overwriting
        destination_bucket=GCP_PROCESSING_BUCKET_PATH,
        destination_path='user/'
    )

    # Dummy para marcar fim da dag (opcional)
    finish = DummyOperator(
        task_id='finish'
    )

    # Fluxo da tarefa a ser criada
    init >> sync_s3_to_gcs_user_json_files >> create_bucket >> copy_files_processing_zone >> finish

# Inicialização da dag
dag = ingest_data()
