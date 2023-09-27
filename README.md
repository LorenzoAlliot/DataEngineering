# Proyecto final DataEngineering

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
*   /league: que según la documentación se actualiza varias veces al día
*   /standings: que según la documentación se actualiza cada hora.
*   /teams: ue según la documentación se actualiza varias veces a la semana

## Distribución de archivos
**Los archivos a tener en cuenta son:**
dockerfile/: Archivo para crear la imagen necesaria para utilizar Airflow.
* commons/: Carpeta con los archivos .py que contienen las funciones y transformaciones para llamar desde los DAGs.
* config/: Carpeta con el archivo que contiene las credenciales y su script con la funcion que lo parsea.
* ProyectoFinal-Alliot.py: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
* requierements.txt: archivo con las librerias utilizadas para instalar en la imagen de docker.
