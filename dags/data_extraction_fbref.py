import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_DATA_PATH = '/home/dev/football-data-pipeline/data'

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