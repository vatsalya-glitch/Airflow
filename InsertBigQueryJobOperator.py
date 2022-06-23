from datetime import datetime
import os
from airflow import models
from airflow.models import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
import json

# constant vars
PROJECT_ID = os.environ.get("GCP_PROJECT")
resource_sa = "service_account"
location="us-east4"

sql_folder_path = f"sql-files"

owner_name = f"vatsalya"

default_dag_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'project_id': PROJECT_ID
}


with models.DAG(
        'BigQueryInsertJobOperator',
        schedule_interval=@monthly,
        default_args=default_dag_args,
        template_searchpath = os.path.join(os.environ.get('DAGS_FOLDER'),sql_folder_path),
        user_defined_macros= {'data_set':  'dataset','table_name': 'table'},
        ) as dag:
            task_1= BigQueryInsertJobOperator(
                    task_id = 'call_sql',
                    impersonation_chain = resource_sa, # SA Airflow uses to impersonate while interacting with BQ                
                    configuration = {
                        'labels' : {
                            'owner': owner_name 
                        },
                        'query' : {
                            'query' :"{% include 'BqJobInsertOperator.sql'%}", # referencing the file via templates
                            'useLegacySql' : False # uses standard SQL as dialect.                        
                        }
                    }
            )   
task_1
