FROM puckel/docker-airflow

COPY ./dag/ /usr/local/airflow/dags

# Crear el directorio donde se guardarán los datos procesados
RUN mkdir -p /usr/local/airflow/data
RUN chown airflow: /usr/local/airflow/data

EXPOSE 8080

CMD ["webserver"]