# Generador de cifras control

El generador de cifras control es una herramienta el cual obtiene datos estadísticos de las tablas en impala  a partir de un grupo de esquemas declarados en un catalogo de datos.

[![N|Solid](https://i.ibb.co/jyfYb2M/Diagrama-arquitectura.png)](https://nodesource.com/products/nsolid)

Para poder ejecutar el programa y realizar pruebas sobre datos dummy en necesario tener instalado los siguientes módulos.
-	Ibis-framework
-	Framework-spark(librerías pyspark)
-	mysql-connector-java.jar


Pasos para descargar y ejecutar el artefacto.
```sh
debian@host$ git clone https://github.com/ArturoGonzalezBecerril/cifras_control.git
debian@host$ cd EstadisticoDeFtes
debian@host$ ./spark-submmit ServicioImpala.py --jars mysql-connector-java.jar
```

