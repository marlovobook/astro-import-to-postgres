
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import csv
from io import StringIO
import os

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Create Connection used in .copy_expert
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='postgres',
    port='5432'
    )
def _read_parquet(ds):

    # Check file
    ds_date = f"{ds}"
    print(ds_date) #The DAG run’s logical date as YYYY-MM-DD #2023-01-01
    

    # Set the directory containing parquet files
    data_directory = "/usr/local/airflow/include/data_sample"
    
    # Get list of files to process
    files = [file for file in os.listdir(data_directory) if file.endswith('.parquet') and ds in file]
    
    # # Read all parquet files into a single DataFrame
    dfs = [pd.read_parquet(os.path.join(data_directory, file), use_threads=True) for file in files]
    df_concat = pd.concat(dfs, ignore_index=True)


    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(df_concat.values)
    sio.seek(0)
    with conn.cursor() as c:
        c.copy_expert(
            sql="""
            COPY dbo.sampledata_datawow (
                department_name,
                sensor_serial,
                create_at,
                product_name,
                product_expire 
            ) FROM STDIN WITH CSV""",
            file=sio
        )
        conn.commit()



default_args = {
    'owner' : 'BOOK',
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'catchup' : False,
    ######## w8 previous task ##########
    'wait_for_downstream' : False,
    'depends_on_past': False
    ######## w8 previous task ##########
}


# Define your DAG
with DAG(
    dag_id='import_to_postgres_s',
    default_args=default_args,
    description='Incrementally Copy file from source',
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1), 
    end_date=datetime(2023, 1, 31)

)as dags:
    
    start = DummyOperator(task_id="start")

    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema_on_postgres',
        conn_id="postgres",
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS dbo;
            CREATE TABLE IF NOT EXISTS dbo.sampledata_datawow (
            
                department_name VARCHAR(100),
                sensor_serial VARCHAR(100),
                create_at TIMESTAMP,
                product_name VARCHAR(100),
                product_expire TIMESTAMP
            );
            
        
        """,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=_read_parquet

    )
     
    end = DummyOperator(task_id='end')


start >> create_schema  >> load_to_postgres >> end

