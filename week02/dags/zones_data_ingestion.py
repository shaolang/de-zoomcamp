from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import ingest_script as i

URL = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
FOUT = '/opt/airflow/zone_lookup.csv'
TABLE_NAME = 'zones'

dag_params = dict(
    dag_id='Zone_data_ingestion',
    catchup=True,
    is_paused_upon_creation=True,
    schedule_interval='0 6 2 1 *',
    start_date=datetime(2022, 1, 2))


with DAG(**dag_params) as dag:
    download = BashOperator(
        task_id='download_zone_data',
        bash_command=f'curl -sSLf {URL} -o {FOUT}'
    )

    ingest = PythonOperator(
        task_id='ingest_zone_data',
        python_callable=i.ingest_callable,
        op_kwargs=dict(
            user='airflow',
            password='airflow',
            host='postgres',
            port=5432,
            db='airflow',
            table_name=TABLE_NAME,
            csv_file=FOUT,
            datetime_cols=[])
    )

    download >> ingest

