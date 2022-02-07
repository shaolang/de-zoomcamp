from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import ingest_script as i

URL_TEMPLATE = (
    'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_'
    '{{ dag_run.logical_date.strftime("%Y-%m") }}.csv')
FOUT_TEMPLATE = '/opt/airflow/fhv-{{ dag_run.logical_date.strftime("%Y-%m") }}.csv'
TABLE_NAME_TEMPLATE = 'fhv_{{ dag_run.logical_date.strftime("%Y_%m") }}'

dag_params = dict(
    dag_id='FHV_data_ingestion',
    catchup=True,
    is_paused_upon_creation=True,
    schedule_interval='0 6 2 * *',
    start_date=datetime(2019, 1, 1))


with DAG(**dag_params) as dag:
    download = BashOperator(
        task_id='download_fhv_data',
        bash_command=f'curl -sSLf {URL_TEMPLATE} -o {FOUT_TEMPLATE}'
    )

    ingest = PythonOperator(
        task_id='ingest_fhv_data',
        python_callable=i.ingest_callable,
        op_kwargs=dict(
            user='airflow',
            password='airflow',
            host='postgres',
            port=5432,
            db='airflow',
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=FOUT_TEMPLATE,
            datetime_cols=['pickup_datetime', 'dropoff_datetime'])
    )

    download >> ingest
