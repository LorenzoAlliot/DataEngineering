FROM apache/airflow:2.7.0


# Copia el archivo requirements.txt al directorio de trabajo
COPY requirements.txt .

# Instala las dependencias
RUN pip install -r requirements.txt

# Copia el script ProyectoFinal-Alliot.py al directorio /opt/airflow/dags
COPY ProyectoFinal-Alliot.py /opt/airflow/dags/

# Copia el directorio common al directorio /opt/airflow/proyecto
COPY common /opt/airflow/common/

# Copia el directorio config al directorio /opt/airflow/proyecto
COPY config /opt/airflow/config/


RUN airflow db init && \
airflow users create \
          --username admin \
          --password admin \
          --firstname FIRST_NAME \
          --lastname LAST_NAME \
          --role Admin \
          --email admin@example.org

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"


ENTRYPOINT airflow scheduler & airflow webserver