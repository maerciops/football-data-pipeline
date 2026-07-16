import pendulum
import boto3
import os
import glob

from airflow import DAG
from airflow.decorators import task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_DATA_PATH = '/home/dev/football-data-pipeline/data'
AIRFLOW_DATA_PATH = '/opt/airflow/football-data'

with DAG(
    dag_id="data_extraction_fbref",
    description="Extract football data from FBref using Docker",
    schedule_interval="@weekly",
    start_date=pendulum.datetime(2026, 4, 25, tz="America/Sao_Paulo"),
    catchup=False,
) as dag:
    
    extract_data = DockerOperator(
        task_id="extract_data",
        image="football-data-pipeline-extract-data:latest",
        api_version="auto",
        auto_remove='force',
        docker_url="unix://var/run/docker.sock", 
        network_mode="football-data-pipeline_default",
        shm_size=536870912,  #512MB
        environment={
            'TZ': 'America/Sao_Paulo',
            'OUTPUT_DIR': '/app/data',
            'APP_BASE_DIR': '/app',
            'FLARESOLVERR_URL': 'http://flaresolverr:8191/v1',
        },
        mounts=[
            Mount(
                source=HOST_DATA_PATH, 
                target='/app/data', 
                type='bind'
            )
        ]
    )

    @task()
    def upload_to_s3():
        import re

        S3_BUCKET = os.environ['S3_BUCKET']
        S3_PREFIX = os.environ.get('S3_PREFIX', 'football/raw')

        s3 = boto3.client('s3')

        search_path = os.path.join(AIRFLOW_DATA_PATH, "dados_brasileirao", "*.parquet")
        files = glob.glob(search_path)

        if not files:
            raise ValueError(f"Nenhum arquivo encontrado em {AIRFLOW_DATA_PATH}")

        uploaded = []

        for filepath in files:
            filename = os.path.basename(filepath)
            match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)

            if match:
                data_arquivo = match.group(1)
                ano, mes, dia = data_arquivo.split('-')
                
                s3_key = f"{S3_PREFIX}/ano={ano}/mes={mes}/dia={dia}/{filename}"
            else:
                s3_key = f"{S3_PREFIX}/unpartitioned/{filename}"

            s3.upload_file(filepath, S3_BUCKET, s3_key)
            uploaded.append(s3_key)

        print(f"{len(uploaded)} arquivos enviados para s3://{S3_BUCKET}/{S3_PREFIX}/")
        return uploaded
    
    extract_data >> upload_to_s3()