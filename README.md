# OpenBrewerie Case Data Engineering

[ENGLISH README](https://github.com/MrVtR/openbrewerie-case-data-engineering/blob/main/README_ENG.md)

## Conceito do projeto
Este projeto implementa uma pipeline de dados utilizando Apache Airflow para coletar, processar e armazenar informações sobre cervejarias dos Estados Unidos, extraídas da API Open Brewery DB. O fluxo de dados segue uma arquitetura de camadas: Bronze, Silver e Gold, onde os dados são gradualmente limpos, transformados e agregados para facilitar a análise.

A finalidade deste projeto é demonstrar habilidades em engenharia de dados, incluindo a criação de pipelines escaláveis para manipulação de grandes volumes de dados com foco na integridade e processamento eficiente.

## Pré-requisitos e recursos utilizados
O projeto foi implementado utilizando as seguintes tecnologias e bibliotecas:

1. **Estrutura de pastas do projeto**

  ```
  .
  ├── assets/
  │   └── error_detection.jpg
  ├── custom_logs/
  │   └── all_logs.csv
  ├── dags/
  │   └── data_lake_architeture.py
  ├── data/
  │   ├── bronze
  │   ├── silver
  │   └── gold
  ├── .env
  └── docker-compose.yml
  ```

2. **Linguagens e Ferramentas**:
   - Python 3.12
   - Apache Airflow 2.10.2
   - Docker e Docker compose

3. **Bibliotecas Externas**:
   - pendulum: Usado para manipulação de datas
   - requests: Para fazer chamadas HTTP à API Open Brewery DB
   - pandas: Para manipulação e processamento de dados
   - airflow: Para criar e gerenciar os DAGs

4. **Links de Referências**:
   - [Open Brewery DB API](https://www.openbrewerydb.org/)
   
## Passo a passo
Abaixo, o passo a passo para a implementação e execução deste projeto:

### **Ferramenta de Orquestração**:
- Para as configurações da DAG no airflow, **incluindo tratamento de scheduling, retries e erros da tarefa**, utilizei os seguintes parâmetros:
```py
    # Quantas tentativas serão feitas em caso de falha da tarefa e o intervalo entre cada tentativa
    default_args={"retries": 2,'retry_delay': timedelta(minutes=3)},
    # De quanto em quanto tempo a tarefa será acionada automaticamente
    schedule=timedelta(days=1),
    # Data de ínicio dos agendamentos, obedecendo meu timezone local
    start_date=pendulum.datetime(2024, 10, 6, tz='America/Sao_Paulo'),
    # Data de fim dos agendamentos, obedecendo meu timezone local
    end_date=pendulum.datetime(2024, 10, 30, tz='America/Sao_Paulo'),
    # Define o número máximo de tarefas que podem ser executadas simultaneamente.
    max_active_tasks=1,
    # Grante que o Airflow não vai executar as tarefas "perdidas" (ou atrasadas) de datas anteriores ao start_date. 
    catchup=False,
    # Função que será acionada para registrar logs customizados em caso de falha da tarefa
    on_failure_callback=task_failure_or_retry_alert, 
```

### **Arquitetura do Data Lake**:
  **Todas as etapas desta seção podem ser integradas facilmente com a Google Cloud, integrando o Cloud Storage no código, para salvar os dados usando um serviço de Data Lake.**
  - **Bronze Layer**:
    - A DAG coleta dados da API Open Brewery DB, especificamente sobre cervejarias dos Estados Unidos, garantindo pegar todas as páginas de dados disponíveis.
    - Os dados brutos são salvos no formato CSV na camada "Bronze" para garantir que os dados originais estejam preservados.
   
  - **Silver Layer**:
    - Os dados brutos são processados e limpos, incluindo a remoção de espaços em branco e a normalização de valores como os estados.
    - Os dados limpos são salvos no formato Parquet e particionados por país e estado na camada "Silver".

  - **Gold Layer**:
    - Os dados da camada "Silver" são agregados, contando o número de cervejarias por estado e tipo de cervejaria.
    - Os resultados finais são armazenados no formato CSV na camada "Gold".

### **Conteinerização**
  - Foi utilizado o Docker Desktop + Docker Compose no Windows
  - A ferramenta Airflow foi a principal do projeto, servindo de Orquestrador, sendo utilizado na porta 8080.
  - De forma auxiliar, coloquei no arquivo docker-compose uma instância do Jupyter Notebook para ser possível testar os códigos de forma mais rápida durante o desenvolvimento do projeto, usado na porta 8888.

### **Monitoramento/Alertas**:
  - Foi criado uma função para lidar com tarefas que entram em um estado de falha/retry, essa função **gera um log customizado** em formato csv para o usuário ter uma visão mais organizada das tarefas que deram algum erro.
  - A mesma função pode ser integrada com ferramentas mensageiras e também com a Cloud, **para deixar o projeto simples de ser testado pelo avaliador**, essas ferramentas não foram utilizadas no projeto.
    - Ferramentas mensageiras recomendadas:
      - Slack (**Necessário configuração de Webhook**)
      - Google Chat (**Necessário configuração de Webhook**)
      - Email (**Necessário configuração SMTP**)
    - Para a Google Cloud é possível utilizar tanto o **Cloud Storage** com a integração da biblioteca dele em python quanto o **Big Query** com a bibliotecas pandas_gbq.
  - Outros tipos de funções de monitoramento também podem ser criadas com as integrações acima, na parte de Data Quality, destaco problemas com dados nulos e eventuais glitchs de dados, cujo estes estão presentes em algumas linhas da API quando vemos todos os países disponiveis nas requisições. Um Exemplo abaixo de um glitch que se encontra na base de dados da OpenBrewerie:
    ![Detecção de erro](https://github.com/MrVtR/openbrewerie-case-data-engineering/blob/main/assets/error_detection.jpg)
    - Para corrigir este tipo de erro, podemos partir para uma abordagem matemática utilizando a [**similaridade de cossenos**](https://nishtahir.com/fuzzy-string-matching-using-cosine-similarity/), comparando a string com problemas com uma lista de cidades/estados de um determinado país, que são as colunas mais comuns que observei de ter esse tipo de comportamento, embora possa ser visto em outras colunas também. Ao encontrar a string com a maior similaridade, ela será substituída pela string correta.

## Instalação
Para instalar e executar o projeto:

1. Clone o repositório do projeto.
2. Configure o Docker em sua máquina, uma recomendação é instalar o [Docker Desktop](https://www.docker.com/products/docker-desktop/) para ter uma interface gráfica disponível.
3. Utilize os seguintes comandos na pasta principal do projeto obtida no passo 1:
    ```
      docker compose up airflow-init
      docker compose up  
     ```
4. Após isso, o uso do Airflow será liberado no endereço **localhost:8080**. Para se autenticar, coloque **airflow** como usuário e **airflow** como senha.
5. Caso queira utilizar o Jupyter Notebook para algum tipo de debug, ele será habilitado na porta 8888, seu endereço completo com o token de autenticação será exibido no terminal durante a inicialização do container.

## Execução
1. Acesse a interface web do Airflow no endereço **localhost:8080**.
2. Realize a autenticação destacada na seção de Instalação acima.
3. Ative a DAG "ab_inbev_breweries" caso não esteja ativada.
4. Execute as tarefas individualmente ou a DAG completa, seguindo o fluxo de dados desde a camada Bronze até a Gold.
5. Os dados criados estarão localizados na pasta **data** do projeto e os dados de logs customizados estarão na pasta **custom_logs**.

## Autor
* [Vítor Ribeiro](https://github.com/MrVtR) - Desenvolvedor do Pipeline de Dados - Engenheiro de Dados/ML Engineer

## Demais anotações e referências
- [Documentação do Airflow](https://airflow.apache.org/docs/)
- [Documentação do Pandas](https://pandas.pydata.org/docs/)
- [Documentação da API](https://www.openbrewerydb.org/documentation/)

## Imagens/screenshots
- Simulação de tarefa, forçando um erro para engatilhar a função de tratamento de erros
![Simulação de Tarefa](https://github.com/MrVtR/openbrewerie-case-data-engineering/blob/main/assets/simulacao.jpg)
