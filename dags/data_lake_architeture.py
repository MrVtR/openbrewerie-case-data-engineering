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

    def silver_dag(**kwargs):
        df = pd.read_csv("./data/bronze/api_raw_data.csv")
        print(df.head())

        print(len(df['country'].unique()),df['country'].unique())
        df['country'] = df['country'].apply(lambda x: x.strip())
        print(len(df['country'].unique()),df['country'].unique(),end='\n\n')

        print(len(df['state'].unique()),df['state'].unique(),end='\n\n')
        df['state'] = df['state'].apply(lambda x: x.strip())
        df['state'] = df['state'].apply(lambda x: x.title())
        print(len(df['state'].unique()),df['state'].unique())

        df.to_parquet("./data/silver",partition_cols=["country","state"])
        pass

    def gold_dag(**kwargs):
        df = pd.read_parquet("./data/silver")
        df2 = df.groupby(["state","brewery_type"]).brewery_type.agg(brewery_count=("count"))
        df2.to_csv("./data/gold/aggregate.csv")

    bronze_dag_task = PythonOperator(
        task_id="bronze_dag",
        python_callable=bronze_dag,
    )
    silver_dag_task = PythonOperator(
        task_id="silver_dag",
        python_callable=silver_dag,
    )
    gold_dag_task = PythonOperator(
        task_id="gold_dag",
        python_callable=gold_dag,
    )

    start_task = DummyOperator(task_id='start_task', dag=dag)

    end_task = DummyOperator(task_id='end_task', dag=dag)
    start_task >> bronze_dag_task >> silver_dag_task >> gold_dag_task >> end_task