from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import json
import pandas as pd
import psycopg2
import numpy as np

# Defin argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir la función para obtener los datos de COVID-19
def get_covid_data():
    url = 'https://api.covidtracking.com/v1/us/daily.json'
    response = requests.get(url)
    data = response.json()
    return data

# Definir la función para procesar los datos y crear el DataFrame limpio
def process_covid_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_covid_data_task')
    columnas = ['date', 'positive', 'death', 'positiveIncrease', 'deathIncrease', 'totalTestResults', 'hospitalizedCurrently', 'recovered', 'total', 'totalTestResultsIncrease']
    datos = [{columna: registro[columna] for columna in columnas if columna in registro} for registro in data]
    df = pd.DataFrame(datos)
    nombres_columnas = {
        'date': 'submission_date',
        'positive': 'tot_cases',
        'death': 'tot_death',
        'positiveIncrease': 'new_case',
        'deathIncrease': 'new_death',
        'totalTestResults': 'total_test_results',
        'hospitalizedCurrently': 'hospitalized_currently',
        'recovered': 'recovered',
        'total': 'total',
        'totalTestResultsIncrease': 'total_test_results_increase'
    }
    df = df.rename(columns=nombres_columnas)

    # Eliminar filas con valores cero, NaN, None o nulos
    columns_to_check = df.columns[1:]
    df_cleaned = df.dropna(subset=columns_to_check, how='all')
    df_cleaned = df_cleaned.replace({0: np.nan, 'None': np.nan, None: np.nan})
    df_cleaned = df_cleaned.dropna(how='all', subset=columns_to_check)

    # Eliminar duplicados del DataFrame
    df_cleaned = df_cleaned.drop_duplicates()

    # Guardar el DataFrame limpio como variable XCom para usarlo en otra tarea
    ti.xcom_push(key='cleaned_df', value=df_cleaned)

# Definir la función para insertar los datos en la tabla de Redshift
def insert_into_redshift(**kwargs):
    ti = kwargs['ti']
    df_cleaned = ti.xcom_pull(key='cleaned_df', task_ids='process_covid_data_task')

    if df_cleaned is None or df_cleaned.empty:
        raise ValueError("DataFrame limpio no encontrado o vacío en XCom.")

    # Conexión a Amazon Redshift
    host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    port = 5439
    database = 'data-engineer-database'
    user = 'fgmartinez87_coderhouse'
    password = '7c92hMs3M1'  # Ver contraseña en la entrega

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Crear la tabla en Redshift con diststyle even y sortkeys
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS covid_data (
        submission_date INT,
        tot_cases INT,
        tot_death INT,
        new_case INT,
        new_death INT,
        total_test_results INT,
        hospitalized_currently INT,
        recovered INT,
        total INT,
        total_test_results_increase INT
    )
    DISTSTYLE EVEN
    SORTKEY (submission_date);
    '''
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()

    # Insertar los datos en la tabla covid_data
    insert_query = 'INSERT INTO covid_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    data_to_insert = [tuple(row) for row in df_cleaned.values]
    with conn.cursor() as cur:
        cur.executemany(insert_query, data_to_insert)
        conn.commit()

# Definir la función para eliminar duplicados en la tabla de Redshift
def remove_duplicates_from_redshift(**kwargs):
    # Conexión a Amazon Redshift
    host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    port = 5439
    database = 'data-engineer-database'
    user = 'fgmartinez87_coderhouse'
    password = '7c92hMs3M1'  # Ver contraseña en la entrega

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    with conn.cursor() as cur:
        # Query para eliminar duplicados utilizando una tabla temporal en memoria
        cur.execute("CREATE TEMP TABLE covid_data_temp AS SELECT DISTINCT * FROM covid_data;")
        cur.execute("TRUNCATE TABLE covid_data;")
        cur.execute("INSERT INTO covid_data SELECT * FROM covid_data_temp;")
        cur.execute("DROP TABLE covid_data_temp;")
        conn.commit()

# Crear el DAG
with DAG('covid_data_dag', 
         default_args=default_args, 
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:

    # Definir las tareas como PythonOperators
    get_covid_data_task = PythonOperator(
        task_id='get_covid_data_task',
        python_callable=get_covid_data
    )

    process_covid_data_task = PythonOperator(
        task_id='process_covid_data_task',
        python_callable=process_covid_data,
        provide_context=True,  # Pasar el contexto, incluido 'ti'
    )

    insert_into_redshift_task = PythonOperator(
        task_id='insert_into_redshift_task',
        python_callable=insert_into_redshift,
        provide_context=True,  # Pasar el contexto, incluido 'ti'
    )

    remove_duplicates_from_redshift_task = PythonOperator(
        task_id='remove_duplicates_from_redshift_task',
        python_callable=remove_duplicates_from_redshift,
        provide_context=True,  # Pasar el contexto, incluido 'ti'
    )

    # Definir las dependencias entre tareas
    get_covid_data_task >> process_covid_data_task >> insert_into_redshift_task >> remove_duplicates_from_redshift_task 