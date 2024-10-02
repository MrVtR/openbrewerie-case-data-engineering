from __future__ import annotations
import json
import textwrap
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import requests
import json
import pandas as pd
import os
import shutil

with DAG(
    "tutorial_dag",
    default_args={"retries": 2},
    description="DAG tutorial",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    
    def bronze_dag(**kwargs):
        first_iteraction = False
        cont_pag = 0
        #Pegando todas as cervejarias dos EUA, que é a grande maioria dos dados
        while True:
            x = requests.get(f'https://api.openbrewerydb.org/v1/breweries?by_country=united%20states&per_page=250&page={cont_pag}')
            response = json.loads(x.text)
            cont_pag+=1

            if first_iteraction==False:
                df = pd.DataFrame.from_dict(response)
                first_iteraction = True
            else:
                if(len(response)>0):
                    df = pd.concat([df, pd.DataFrame.from_dict(response)])
                else:
                    break

        df.to_csv("./data/bronze/api_raw_data.csv",index=False)
    
    bronze_dag_task = PythonOperator(
        task_id="bronze_dag",
        python_callable=bronze_dag,
    )

    start_task = DummyOperator(task_id='start_task', dag=dag)

    end_task = DummyOperator(task_id='end_task', dag=dag)
    start_task >> bronze_dag_task >> end_task