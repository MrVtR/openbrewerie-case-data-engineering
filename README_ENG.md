# OpenBrewerie Case Data Engineering

## Project Concept
This project implements a data pipeline using Apache Airflow to collect, process, and store information about breweries in the United States, extracted from the Open Brewery DB API. The data flow follows a layered architecture: Bronze, Silver, and Gold, where the data is gradually cleaned, transformed, and aggregated to facilitate analysis.

The purpose of this project is to demonstrate data engineering skills, including the creation of scalable pipelines for handling large volumes of data, focusing on data integrity and efficient processing.

## Prerequisites and Resources Used
The project was implemented using the following technologies and libraries:

1. **Project Folder Structure**

  ```
  .
  ├── assets/
  │   └── error_detection.jpg
  ├── custom_logs/
  │   └── all_logs.csv
  ├── dags/
  │   └── data_lake_architecture.py
  ├── data/
  │   ├── bronze
  │   ├── silver
  │   └── gold
  ├── .env
  └── docker-compose.yml
  ```

2. **Languages and Tools**:
   - Python 3.12
   - Apache Airflow 2.10.2
   - Docker and Docker Compose

3. **External Libraries**:
   - pendulum: Used for date manipulation
   - requests: For making HTTP calls to the Open Brewery DB API
   - pandas: For data manipulation and processing
   - airflow: To create and manage DAGs

4. **Reference Links**:
   - [Open Brewery DB API](https://www.openbrewerydb.org/)
   
## Step-by-Step
Below is the step-by-step process for implementing and running this project:

### **Orchestration Tool**:
- For DAG configurations in Airflow, **including scheduling, retries, and task error handling**, the following parameters were used:
```py
    # Number of retry attempts if a task fails and the interval between retries
    default_args={"retries": 2,'retry_delay': timedelta(minutes=3)},
    # How often the task will be automatically triggered
    schedule=timedelta(days=1),
    # Start date of the schedule, respecting the local timezone
    start_date=pendulum.datetime(2024, 10, 6, tz='America/Sao_Paulo'),
    # End date of the schedule, respecting the local timezone
    end_date=pendulum.datetime(2024, 10, 30, tz='America/Sao_Paulo'),
    # Defines the maximum number of tasks that can run simultaneously
    max_active_tasks=1,
    # Ensures that Airflow won't run "missed" tasks from dates before the start_date
    catchup=False,
    # Function that will be triggered to log custom errors in case of task failure
    on_failure_callback=task_failure_or_retry_alert, 
```

### **Data Lake Architecture**:
  **All steps in this section can be easily integrated with Google Cloud by incorporating Cloud Storage into the code to save the data using a Data Lake service.**
  - **Bronze Layer**:
    - The DAG collects data from the Open Brewery DB API, specifically about breweries in the United States, ensuring all pages of data are fetched.
    - Raw data is saved in CSV format in the "Bronze" layer to ensure that the original data is preserved.
   
  - **Silver Layer**:
    - The raw data is processed and cleaned, including removing blank spaces and normalizing values like states.
    - The cleaned data is saved in Parquet format and partitioned by country and state in the "Silver" layer.

  - **Gold Layer**:
    - Data from the "Silver" layer is aggregated, counting the number of breweries by state and brewery type.
    - The final results are stored in CSV format in the "Gold" layer.

### **Containerization**
  - Docker Desktop + Docker Compose was used on Windows.
  - Airflow was the main tool in the project, acting as the orchestrator, running on port 8080.
  - Additionally, a Jupyter Notebook instance was included in the Docker Compose file to allow quick testing of the code during the project development, running on port 8888.

### **Monitoring/Alerts**:
  - A function was created to handle tasks that enter a failure/retry state, and this function **generates a custom log** in CSV format, allowing users to have an organized view of tasks that encountered an error.
  - The same function can be integrated with messaging tools and the Cloud, but **to keep the project simple for evaluation**, these tools were not used in the project.
    - Recommended messaging tools:
      - Slack (**Webhook configuration required**)
      - Google Chat (**Webhook configuration required**)
      - Email (**SMTP configuration required**)
    - For Google Cloud, you can use either **Cloud Storage** by integrating its Python library or **BigQuery** using the `pandas_gbq` library.
  - Other types of monitoring functions can also be created with the above integrations. For Data Quality, I highlight issues with null data and occasional data glitches, which are present in some rows of the API when retrieving all available countries. Below is an example of a glitch found in the OpenBrewery database:
    ![Error Detection](https://github.com/MrVtR/openbrewerie-case-data-engineering/blob/main/assets/error_detection.jpg)
    - To fix such an error, we can use a mathematical approach by applying [**cosine similarity**](https://nishtahir.com/fuzzy-string-matching-using-cosine-similarity/), comparing the problematic string with a list of cities/states from a particular country. The string with the highest similarity will replace the incorrect string.

## Installation
To install and run the project:

1. Clone the project repository.
2. Set up Docker on your machine. I recommend installing [Docker Desktop](https://www.docker.com/products/docker-desktop/) to have a graphical interface available.
3. Run the following commands in the main project folder from step 1:
    ```
      docker compose up airflow-init
      docker compose up  
     ```
4. After that, Airflow will be accessible at **localhost:8080**. Use **airflow** as both username and password to log in.
5. If you want to use Jupyter Notebook for debugging, it will be enabled on port 8888. Its full address, including the authentication token, will be displayed in the terminal during container startup.

## Execution
1. Access the Airflow web interface at **localhost:8080**.
2. Log in using the credentials mentioned in the Installation section.
3. Activate the DAG "ab_inbev_breweries" if it is not already activated.
4. Run the individual tasks or the entire DAG, following the data flow from the Bronze layer to the Gold layer.
5. The created data will be located in the project's **data** folder, and custom log data will be in the **custom_logs** folder.

## Author
* [Vítor Ribeiro](https://github.com/MrVtR) - Data Pipeline Developer - Data Engineer/ML Engineer

## Additional Notes and References
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

## Images/screenshots
![DAG Example](https://github.com/seuprojeto/imagens/dag_exemplo.png)
