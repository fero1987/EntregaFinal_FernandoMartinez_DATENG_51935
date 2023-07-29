FROM puckel/docker-airflow

COPY ./dag/ /usr/local/airflow/dags

EXPOSE 8080

CMD ["webserver"]