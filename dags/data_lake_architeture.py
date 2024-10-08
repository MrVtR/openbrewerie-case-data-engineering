from __future__ import annotations
import json
import textwrap
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta

import requests
import json
import pandas as pd

def task_failure_or_retry_alert(context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    log_data = {
        'task_id':[task_instance.task_id],
        'dag_id':[dag_run.dag_id],
        'state':[task_instance.state],
        'try_number':[task_instance.try_number],
        'max_tries':[task_instance.max_tries],
        'execution_date':[task_instance.execution_date],
        'log_url':[task_instance.log_url]
    }

    try:
        df_all_logs = pd.read_csv("./custom_logs/all_logs.csv")
        df_current_log = pd.DataFrame(log_data)
        df_all_logs = pd.concat([df_all_logs, df_current_log], ignore_index=True)
    except:
        df_all_logs = pd.DataFrame(log_data)
    finally:
        df_all_logs.to_csv('./custom_logs/all_logs.csv',index=False)

    print("Log do erro customizado salvo com sucesso na pasta custom_logs")

# Define a sample Python function to raise an error
def fail_task():
    raise ValueError("This task will fail and trigger a retry.")

with DAG(
    "ab_imbev_breweries",
    default_args={"retries": 2,'retry_delay': timedelta(minutes=3)},
    description="DAG tutorial",
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2024, 10, 6, tz='America/Sao_Paulo'),
    end_date=pendulum.datetime(2024, 10, 30, tz='America/Sao_Paulo'),
    max_active_tasks=1,
    catchup=False,
    on_failure_callback=task_failure_or_retry_alert, 
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
        on_retry_callback=task_failure_or_retry_alert,
    )
    silver_dag_task = PythonOperator(
        task_id="silver_dag",
        python_callable=silver_dag,
        on_retry_callback=task_failure_or_retry_alert,
    )
    gold_dag_task = PythonOperator(
        task_id="gold_dag",
        python_callable=gold_dag,
        on_retry_callback=task_failure_or_retry_alert,
    )

    start_task = DummyOperator(task_id='start_task', dag=dag)

    end_task = DummyOperator(task_id='end_task', dag=dag)
    start_task >> bronze_dag_task >> silver_dag_task >> gold_dag_task >> end_task