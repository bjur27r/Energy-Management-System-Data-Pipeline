# Abastracto Energy-Management-System-Data-Pipeline

#License Iracesa SL



## Motivación:
Primera fase de un proyecto orientado a la gestión inteligente de redes energéticas offgrid ó en generación distribuida. Para ello se ha diseñado una arquitectura  lambda para la ingesta y procesamiento  en streaming y batch  de todas las señales recibidas desde los inversores que gestionan  las redes energéticas  de  cada instalación(Sector Residencial/Agropecuario), entre las distintas fuentes de generación( para este proyeto solar) y consumos (baterias, y red consumo).

Para enriquecer  el proyecto, se recurren a  otras fuentes de datos  tales como Aemet, Omie con el fin de optimizar el coste energético desde la previsión  haciendo uso de modelos de aprendizaje automático. Principalmente en esta fase se cconstruyen modelos  que permiten predecir la generación solar, la previsión de consumo,  así como el precio de la energia adquirida en red.


## Estructura Directorios:

### EM-AUTOCONSMUM: Arquitectura Lambda para procesamiento de procesamiento
Descripción:Alberga el diseño de la arquitectura lambda , con, a) una capa en streaming, que permite persistir los estados  de cada instalación  en Cassandra, y activar la , b)capa batch con el fin de estimar los consumos y generaciónes 24H en función de los histróricos y previsión meteo respectivamente.c)Query, Persistencia en Cassandra visualizada con Apache Zeppelin. 

Tecnologias:
-Spark 1.6,Core y Streaming,Mllib, interprete Scala.
-IDE:Intellij, con gestor de dependencias en Maven.
-Persistencia, Cassandra 3.0.1, HDFS.


### ETL: Fuentes de ingesta , tratamiento y limpieza de datos, conexiones API
Descripción:, Ingesta transformación de las distintas fuentes de datos. 
-Python, Jupyter.
-Conectores API Aemet y Omie.

### PoolService: Servicio para la consulta de la predicción del precio del PVPC.
Construcción de modelo de la previsión del precio del PVPC parametrizado con el histórico de señeales meteo de más de 50 estaciones metereologicas, extendidas por todo el territorio. Se eligen este input dada la dependencia de del actual modelo enerético a las fuentes de generación renovables,demostrando como son capaces de explicar en gran medida la varianza del PVPC. 
-Tenologias: Python,Tensor Flow y Flask.Persistencia disco.
-Algoritmo, Perceptron  Multicapa otimizado con gradiente descendente.

Mas detalles en proxima presentación.

