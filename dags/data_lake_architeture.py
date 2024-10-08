from __future__ import annotations
import json
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
import requests
import pandas as pd
import time 
import os

#Função para registrar erros e retentativas de tarefas em um arquivo CSV.
def task_failure_or_retry_alert(context):
    # Obtém informações da DAG Run e Task Instance
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    # Cria um dicionário com os dados de log
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
        # Tenta ler o arquivo de logs existente
        df_all_logs = pd.read_csv("./custom_logs/all_logs.csv")
        # Converte os dados de log atuais em um DataFrame
        df_current_log = pd.DataFrame(log_data)
        # Concatena os logs existentes com os logs atuais
        df_all_logs = pd.concat([df_all_logs, df_current_log], ignore_index=True)
    except:
        # Cria um novo DataFrame se o arquivo não existir
        df_all_logs = pd.DataFrame(log_data)
    finally:
        # Salva o DataFrame no arquivo CSV
        os.makedirs('custom_logs', exist_ok=True)
        df_all_logs.to_csv('./custom_logs/all_logs.csv',index=False)

    print("Log do erro customizado salvo com sucesso na pasta custom_logs")

def function_tt():
    raise ValueError("Erro personalizado")
with DAG(
    "ab_inbev_breweries",
    default_args={"retries": 2,'retry_delay': timedelta(minutes=3)},
    description="Processo seletivo da Ab-Inbev 2024",
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2024, 10, 6, tz='America/Sao_Paulo'),
    end_date=pendulum.datetime(2024, 10, 30, tz='America/Sao_Paulo'),
    max_active_tasks=1,
    catchup=False,
    on_failure_callback=task_failure_or_retry_alert, 
) as dag:
    
    #Coleta dados da API Open Brewery DB e salva em formato CSV na camada Bronze.
    def bronze_dag(**kwargs):
        first_iteraction = False
        cont_pag = 0
        #Pegando todas as cervejarias dos EUA, que é a grande maioria dos dados
        while True:
            # Faz uma requisição GET para a API
            x = requests.get(f'https://api.openbrewerydb.org/v1/breweries?by_country=united%20states&per_page=250&page={cont_pag}')
            if x.status_code == 200:
                # Converte a resposta da API de JSON para um dicionário Python
                response = json.loads(x.text)
                # Incrementa o contador de página
                cont_pag+=1
                # Primeira iteração: cria um DataFrame com a resposta da API
                if first_iteraction==False:
                    df = pd.DataFrame.from_dict(response)
                    first_iteraction = True
                # Iterações subsequentes: concatena os dados ao DataFrame existente
                else:
                    if(len(response)>0):
                        df = pd.concat([df, pd.DataFrame.from_dict(response)])
                    # Sai do loop quando não houver mais dados na resposta da API
                    else:
                        break
            else:
                time.sleep(10)
        
        # Salva os dados brutos em um arquivo CSV na camada Bronze
        os.makedirs('data/bronze', exist_ok=True)
        df.to_csv("./data/bronze/api_raw_data.csv",index=False)

    #Processa os dados da camada Bronze, realiza limpeza e salva em formato Parquet na camada Silver.
    def silver_dag(**kwargs):
        # Lê os dados brutos da camada Bronze
        df = pd.read_csv("./data/bronze/api_raw_data.csv")
        # Imprime o cabeçalho do DataFrame
        print(df.head())

        # Imprime o número de valores únicos e os valores únicos da coluna 'country'
        print(len(df['country'].unique()),df['country'].unique())
        # Remove espaços em branco do início e fim dos valores na coluna 'country'
        df['country'] = df['country'].apply(lambda x: x.strip())
        # Imprime o número de valores únicos e os valores únicos da coluna 'country' após a limpeza, haverá uma diferença na quantia de dados únicos
        print(len(df['country'].unique()),df['country'].unique(),end='\n\n')

        # Imprime o número de valores únicos e os valores únicos da coluna 'state'
        print(len(df['state'].unique()),df['state'].unique(),end='\n\n')
        # Remove espaços em branco do início e fim dos valores na coluna 'state'
        df['state'] = df['state'].apply(lambda x: x.strip())
        # Define a primeira letra de cada palavra como maiúscula na coluna 'state'
        df['state'] = df['state'].apply(lambda x: x.title())
        # Imprime o número de valores únicos e os valores únicos da coluna 'state' após a limpeza, haverá uma diferença na quantia de dados únicos
        print(len(df['state'].unique()),df['state'].unique())

        # Salva os dados processados em formato Parquet na camada Silver, particionando por 'country' e 'state'
        os.makedirs('data/silver', exist_ok=True)
        df.to_parquet("./data/silver",partition_cols=["country","state"])
        pass

    #Agrega os dados da camada Silver e salva em formato CSV na camada Gold.
    def gold_dag(**kwargs):
        # Lê os dados processados da camada Silver
        df = pd.read_parquet("./data/silver")
        # Agrupa os dados por 'state' e 'brewery_type' e conta o número de cervejarias por tipo
        df2 = df.groupby(["state","brewery_type"]).brewery_type.agg(brewery_count=("count"))
        # Salva os dados agregados em um arquivo CSV na camada Gold
        os.makedirs('data/gold', exist_ok=True)
        df2.to_csv("./data/gold/aggregate.csv")
    
    # Cria uma tarefa PythonOperator para a função bronze_dag
    bronze_dag_task = PythonOperator(
        task_id="bronze_dag",
        python_callable=function_tt,
        on_retry_callback=task_failure_or_retry_alert,
    )
    # Cria uma tarefa PythonOperator para a função silver_dag
    silver_dag_task = PythonOperator(
        task_id="silver_dag",
        python_callable=silver_dag,
        on_retry_callback=task_failure_or_retry_alert,
    )
    # Cria uma tarefa PythonOperator para a função gold_dag
    gold_dag_task = PythonOperator(
        task_id="gold_dag",
        python_callable=gold_dag,
        on_retry_callback=task_failure_or_retry_alert,
    )

    # Cria uma tarefa DummyOperator para marcar o início da DAG
    start_task = DummyOperator(task_id='start_task', dag=dag)
    # Cria uma tarefa DummyOperator para marcar o final da DAG
    end_task = DummyOperator(task_id='end_task', dag=dag)
    # Define a ordem de execução das tarefas na DAG
    start_task >> bronze_dag_task >> silver_dag_task >> gold_dag_task >> end_task
