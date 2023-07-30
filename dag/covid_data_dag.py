from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import requests
import json
import pandas as pd
import psycopg2
import numpy as np

# Definir la función para enviar una alerta en caso de falla del DAG
def send_failure_alert(context):
    subject = "ALERTA: Falla en el DAG covid_data_dag"
    message = "El DAG covid_data_dag ha fallado en su ejecución."
    email_operator = EmailOperator(
        task_id='send_failure_email_task',
        to='fgmartinez87@gmail.com',  # Reemplaza con el correo electrónico destinatario
        subject=subject,
        html_content=message,
        dag=dag
    )
    email_operator.execute(context=context)

# Defin argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,  # Habilitar el envío de alertas por correo electrónico en caso de falla
    'email': 'fgmartinez87@gmail.com',  # Reemplaza con el correo electrónico destinatario para recibir alertas
    'on_failure_callback': send_failure_alert,  # Función para enviar alerta en caso de falla
}

# Definir la función para obtener los datos de COVID-19 y enviar una alerta si no hay resultados
def get_covid_data(**kwargs):
    url = 'https://api.covidtracking.com/v1/us/daily.json'
    response = requests.get(url)
    data = response.json()
    
    if data is None or len(data) == 0:
        # Enviar alerta por correo electrónico
        subject = "ALERTA: No se pudieron obtener datos de COVID-19"
        message = "No se encontraron datos de COVID-19 en la API."
        email_operator = EmailOperator(
            task_id='send_email_task',
            to='fgmartinez87@gmail.com',  # Reemplaza con el correo electrónico destinatario
            subject=subject,
            html_content=message,
            dag=dag
        )
        email_operator.execute(context=kwargs)
        raise ValueError("No se pudieron obtener datos de COVID-19.")
    
    return data

# Definir la función para procesar los datos y crear el DataFrame limpio
def process_covid_data(data):
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

    # Eliminar filas con todos sus valores cero, NaN, None o nulos
    columns_to_check = df.columns[1:]
    df_cleaned = df.dropna(subset=columns_to_check, how='all')
    df_cleaned = df_cleaned.replace({0: np.nan, 'None': np.nan, None: np.nan})
    df_cleaned = df_cleaned.dropna(how='all', subset=columns_to_check)
    
    # Eliminar columnas con todos sus valores como NaN, None o nulos
    df_cleaned = df_cleaned.dropna(axis='columns', how='all')

    # Calcular la columna new_hospitalized que representa la diferencia de hospitalized_currently del día y el día anterior
    df_cleaned['new_hospitalized'] = df_cleaned['hospitalized_currently'].diff()

    # Calcular las columnas tot_death_ratio y new_death_ratio
    df_cleaned['tot_death_ratio'] = df_cleaned['tot_death'] / df_cleaned['tot_cases']
    df_cleaned['new_death_ratio'] = df_cleaned['new_death'] / df_cleaned['new_case']

    # Guardar el DataFrame limpio en un archivo CSV
    output_file = '/usr/local/airflow/data/covid_data_cleaned.csv'  # Ruta donde se guardará el archivo
    df_cleaned.to_csv(output_file, index=False)

    return output_file

# Definir la función para enviar una alerta si se superan los límites
def check_thresholds_and_send_alert(file_path):
    df_cleaned = pd.read_csv(file_path)

    def send_alert_email(subject, message):
        email_operator = EmailOperator(
            task_id='send_alert_email_task',
            to='fgmartinez87@gmail.com',  # Reemplaza con el correo electrónico destinatario
            subject=subject,
            html_content=message,
            dag=dag
        )
        email_operator.execute(context={})

    # Verificar si la variable new_death supera los 5000
    if df_cleaned['new_death'].max() > 5000:
        subject = f"ALERTA: Variable new_death supera los 5000"
        message = f"La variable new_death ha superado los 5000 el día ({df_cleaned.loc[df_cleaned['new_death'].idxmax(), 'submission_date']})."
        send_alert_email(subject, message)

    # Verificar si el tot_death_ratio supera los 0.2
    if df_cleaned['tot_death_ratio'].max() > 0.2:
        subject = f"ALERTA: Tot_death_ratio superó el 20% "
        message = f"El tot_death_ratio ha superado el 20% el día ({df_cleaned.loc[df_cleaned['tot_death_ratio'].idxmax(), 'submission_date']})"
        send_alert_email(subject, message)

# Definir la función para insertar los datos en la tabla de Redshift
def insert_into_redshift(file_path):
    df_cleaned = pd.read_csv(file_path)

    if df_cleaned is None or df_cleaned.empty:
        raise ValueError("DataFrame limpio no encontrado o vacío en XCom.")

    # Agregar una columna con la hora actual
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_cleaned['insertion_time'] = current_time

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

    # Crear la tabla en Redshift con diststyle even y sortkeys. Creo la columna insertion_time con la hora a la que se produce el insert
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
        total_test_results_increase INT,
        insertion_time TIMESTAMP
    )
    DISTSTYLE EVEN
    SORTKEY (submission_date);
    '''
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()

    # Insertar los datos en la tabla covid_data
    insert_query = 'INSERT INTO covid_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    data_to_insert = [tuple(row) for row in df_cleaned.values]
    with conn.cursor() as cur:
        cur.executemany(insert_query, data_to_insert)
        conn.commit()

# Definir la función para eliminar duplicados en la tabla de Redshift
def remove_duplicates_from_redshift():
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

    # Definir la tarea para obtener datos de COVID-19 y enviar una alerta en caso de falla
    get_covid_data_task = PythonOperator(
        task_id='get_covid_data_task',
        python_callable=get_covid_data,
        provide_context=True,
    )

    # Definir la tarea para procesar los datos y crear el DataFrame limpio
    process_covid_data_task = PythonOperator(
        task_id='process_covid_data_task',
        python_callable=process_covid_data,
        provide_context=True,
    )

    # Definir la tarea para enviar una alerta si se superan los límites
    check_thresholds_and_send_alert_task = PythonOperator(
        task_id='check_thresholds_and_send_alert_task',
        python_callable=check_thresholds_and_send_alert,
        provide_context=True,
    )

    # Definir la tarea para insertar los datos en la tabla de Redshift
    insert_into_redshift_task = PythonOperator(
        task_id='insert_into_redshift_task',
        python_callable=insert_into_redshift,
        provide_context=True,
    )

    # Definir la tarea para eliminar duplicados en la tabla de Redshift
    remove_duplicates_from_redshift_task = PythonOperator(
        task_id='remove_duplicates_from_redshift_task',
        python_callable=remove_duplicates_from_redshift,
    )

    # Definir las dependencias entre tareas
    get_covid_data_task >> process_covid_data_task >> insert_into_redshift_task >> remove_duplicates_from_redshift_task >> check_thresholds_and_send_alert_task
