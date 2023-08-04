## Entregable 4 Fernando Martinez

1) Como primer paso obtenemos la siguiente imagen de Docker Hub a nuestra máquina local (https://hub.docker.com/r/puckel/docker-airflow) mediante el comando **docker pull puckel/docker-airflow** 

2) Creo la carpeta "dag" y la carpeta "data" dentro del repositorio. En la carpeta "dag" guardo el script con el nombre covid_data_dag.py. La carpeta "data" se utilizará para guardar el Dataframe.

3) Creo el Dockerfile con el siguiente script.

<img width="412" alt="image" src="https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/10aee5bd-0468-4dea-8274-dfddeee96f81">

4) Creo el container con el comando **docker build -t imagen1 .**

<img width="517" alt="image" src="https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/6e02d83e-5d74-4176-a6cd-d47abdb80cf2">

5) Corro el container con el comando **docker run -d -p 8080:8080 imagen1**

<img width="536" alt="image" src="https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/44c96870-9647-4019-ad42-2b3ab03f5a9d">

6) Configuro gmail para poder conectarse a Airflow y enviar alertas por mail. Para ello voy a la siguiente [opción](https://myaccount.google.com/apppasswords?rapt=AEjHL4OW1Yc9EGv4-imaBzNgJ35rPY_TU3WY50K3s7ZiOs-azV6__w84ZGdXsvfWaoq7w_oKP-i5y0Se0Fpo0SAt0Lsf5cad2Q) dentro de gmail para configurar la contraseña de las aplicaciones. Esta contraseña se utilizará a la hora de configurar la variable SMTP_PASSWORD dentro de Airflow.

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/f9c31b43-183e-4592-8a83-f7ef6b62fa65)

7) Habilito el acceso IMAP dentro de gmail.

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/f1ee9fef-988a-4d75-92c6-28e7fda7e3aa)

8) Entro al puerto 8080 y accedo a Airflow.
   
![image](https://github.com/fero1987/Curso-DE-CoderHouse/assets/50931047/39446052-4d3e-4b1a-8612-b2e3485fa90b)

7) Accedo al menu Variables dentro de Admin y creo las variables SMTP_EMAIL_FROM, SMTP_EMAIL_TO Y SMTP_PASSWORD (con la contraseña del paso 6) que se van a utilizar para poder enviar las alertas por mail.

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/9559c20e-6ab0-4885-a58c-6903aa5d1b8d)

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/3f1ba787-c7f8-4f12-9b1b-7ac4a2513e9c)

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/e3a1e523-c06f-4010-baad-000b60925bb4)

8) Inicio la ejecución del DAG.

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/853f6cda-62b8-4480-a76b-a93fa49ecd40)

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/2371dbb2-b99e-4094-ab5f-2e5b926b3397)

9) Vemos que se han ejecutado todas las tareas con éxito.

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/a1bec25e-1b0c-44dc-ac6b-3f8ad60b1421)

10) Al entrar a gmail, vemos que han llegado las alertas de forma automática:

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/fd75dea7-9e5b-49fd-9e22-ca0021396064)

![image](https://github.com/fero1987/EntregaFinal_FernandoMartinez_DATENG_51935/assets/50931047/2437fd1c-0054-425b-b9a1-61c15ed964cf)




 
