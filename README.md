# Proyecto final DataEngineering
![Python](https://img.shields.io/badge/Python-blue?logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-E25A1C?logo=apache%20spark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apache%20airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Redshift](https://img.shields.io/badge/Redshift-E71A2A?logo=amazon%20redshift&logoColor=white)

## Abstract
**Los objetivos de este proyecto son:**
* Obtener datos de una API 
* Transformarlos y crear DataFrames
* Conectar a una BBDD en Redshift
* Crear las tablas necesarias y poblarlas.
* Generar contenedores en Docker para su ejecución
* Programar su ejecución mediante DAGs en AirFlow

**El tema del proyecto es:**
Las estadísticas de los equipos argentinos que jueguen distintos torneos locales que sean del año 2023.

El objetivo de la extracción de la API es obtener las estadísticas de los equipos argentinos en los torneos locales que sean del año 2023.

**Las tablas son:**
* countrys: contiene todos los paises que figuran en la API
* leagues: se listan todas las ligas que contiene la API, las actualizaciones se dan cuando hay una liga nueva, sino dentro del ID de liga se van acumulando las distintas temporadas (season)
* teams: son los distintos equipos que participan en las ligas y contiene distintos datos descriptivos de los mimsos.
* venues: son los estadios de los correspondientes equipos
* standings: son las estadisticas de los equipos en las ligas, se actualiza luego de cada partido

La API es consumida desde https://api-sports.io/ especificamente se utilizará la que contiene datos sobre fútbol.

La API es pública pero con la limitación de un máximo de 100 llamadas por día y 10 por minuto

En este link se puede ver la documentación https://www.api-football.com/documentation-v3#section/Introduction

Especificamente se utilizarán los endpoints:
* /league: que según la documentación se actualiza varias veces al día
* /standings: que según la documentación se actualiza cada hora.
* /teams: ue según la documentación se actualiza varias veces a la semana

## Distribución de archivos
**Los archivos a tener en cuenta son:**
* `dockerfile/`: Archivo para crear la imagen necesaria para utilizar Airflow.
* `commons/` : Carpeta con los archivos .py que contienen las funciones y transformaciones para llamar desde los DAGs.
* `config/` : Carpeta con el archivo que contiene las credenciales y su script con la funcion que lo parsea.
* ProyectoFinal-Alliot.py : DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
* requierements.txt : archivo con las librerias utilizadas para instalar en la imagen de docker.

## Instrucciones de uso
Para ejecutar este pipeline deberás tener instalado:
* Docker
* Python

### Pasos para ejecutar pipeline
* Primero debes abrir docker y asegurarte de que esté funcionando correctamente.
* Clona este repositorio en tu máquina local:
```bash
git clone https://github.com/LorenzoAlliot/DataEngineering.git
```
* Abre la terminal en tu PC y escribe este comando para posicionarse en el directorio del Dockerfile
```bash
cd [ruta_del_Dockerfile]
```
* Una vez en posicionados en el directorio ejecuta el comando:
```bash
docekr build . -t [nombre_de_imagen]
```
Esto creará la imagen de docker
* Una vez creada la imagen procedemos a crear el contenedor con el siguiente comando:
```bash
docker run --rm -d -p 8080:8080 [nombre_de_imagen]
```
* Una vez ejecutado esto ir a  https://localhost:8080/
* Se debería abrir la interfaz de AirFlow
![image](https://github.com/LorenzoAlliot/DataEngineering/assets/113041882/7a45d642-ea1a-477f-b655-c00d4ed5f6e3)
* Las credenciales son:
  * username: admin
  * password: admin

Desde aquí podrás administrar los DAGs

### Puntos a tener en cuenta
El DAG esta configurando para enviar mails en caso de ejecucion correcta o error, recomiendo modificar el script email y modificar la linea 18 y 33
```bash
x.sendmail('lorenzoalliot@gmail.com', 'mail@destinatario', message)
```
