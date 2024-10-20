# Desarrollo de Trabajo

* [Una Breve Introducción a Spark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_01_development.md#una-breve-introducción-a-spark)
* [Laboratorio 1: Ejecutar Sesión Interactiva de PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_01_development.md#laboratorio-1-ejecutar-sesión-interactiva-de-pyspark)
* [Laboratorio 2: Usando Iceberg con PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_01_development.md#laboratorio-2-usando-iceberg-con-pyspark)
* [Resumen](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_01_development.md#resumen)
* [Enlaces y Recursos Útiles](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_01_development.md#enlaces-y-recursos-útiles)

### Una Breve Introducción a Spark

Apache Spark es un sistema de procesamiento distribuido y de código abierto utilizado para cargas de trabajo de grandes datos. Ha ganado una popularidad extrema como el motor preferido para el análisis interactivo de datos y el despliegue de pipelines de Ingeniería de Datos y Aprendizaje Automático en producción a gran escala.

En CDE, puedes usar Spark para explorar datos de manera interactiva a través de las Sesiones de CDE o desplegar pipelines de ingeniería de datos por lotes a través de los Trabajos de CDE.

### Laboratorio 1: Ejecutar Sesión Interactiva de PySpark

Navega a la Página de Inicio de CDE y lanza una Sesión de PySpark. Mantén la configuración por defecto.

![alt text](../../img/part1-cdesession-1.png)

Una vez que la Sesión esté lista, abre la pestaña "Interactuar" para ingresar tu código.

![alt text](../../img/part1-cdesession-2.png)

Puedes copiar y pegar código de las instrucciones en el cuaderno haciendo clic en el ícono en la parte superior derecha de la celda de código.

![alt text](../../img/part1-cdesession-3.png)

Copia la siguiente celda en el cuaderno. Antes de ejecutarla, asegúrate de haber editado la variable "username" con tu usuario asignado.

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "s3a://goes-se-sandbox01/data"
username = "user002"
```

![alt text](../../img/part1-cdesession-4.png)

No se requieren más ediciones de código. Continúa ejecutando cada fragmento de código a continuación en celdas separadas en el cuaderno.

```
### CARGAR ARCHIVO DE TRANSACCIONES HISTÓRICAS DESDE EL ALMACENAMIENTO EN LA NUBE
transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

```
### CREAR FUNCIÓN DE PYTHON PARA APLANAR ESTRUCTURAS ANIDADAS EN DATAFRAME DE PYSPARK
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(F.col(prefix + elem.name).alias(prefix + elem.name))
    return result
```

```
### EJECUTAR FUNCIÓN DE PYTHON PARA APLANAR ESTRUCTURAS ANIDADAS Y VALIDAR NUEVO ESQUEMA
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()
```

```
### RENOMBRAR COLUMNAS
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")
```

```
### CONVERTIR TIPOS DE COLUMNAS DE STRING A TIPO APROPIADO
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))
```

```
### CALCULAR EL PROMEDIO Y MEDIANA DEL MONTO DE TRANSACCIONES CON TARJETA DE CRÉDITO
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Promedio del Monto de Transacción: ", transactionsAmountMean)
print("Mediana del Monto de Transacción: ", transactionsAmountMedian)
```

```
### CREAR VISTA TEMPORAL DE SPARK DESDE DATAFRAME
transactionsDf.createOrReplaceTempView("trx")
spark.sql("SELECT * FROM trx LIMIT 10").show()
```

```
### CALCULAR EL MONTO PROMEDIO DE TRANSACCIONES POR MES
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
```

```
### CALCULAR EL MONTO PROMEDIO DE TRANSACCIONES POR DÍA DE LA SEMANA
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
```

```
### CALCULAR NÚMERO DE TRANSACCIONES POR TARJETA DE CRÉDITO
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
```

```
### CARGAR DATOS PII DE CLIENTES DESDE EL ALMACENAMIENTO EN LA NUBE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii".format(storageLocation, username))
piiDf.show()
piiDf.printSchema()
```

```
### CONVERTIR LAT Y LON A TIPO FLOAT Y CREAR VISTA TEMPORAL
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))
piiDf.createOrReplaceTempView("cust_info")
```

```
### SELECCIONAR LOS 100 PRINCIPALES CLIENTES CON MÚLTIPLES TARJETAS DE CRÉDITO ORDENADOS POR NÚMERO DE TARJETAS DE CRÉDITO DE MAYOR A MENOR
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()
```

```
### SELECCIONAR LAS 100 TARJETAS DE CRÉDITO PRINCIPALES CON MÚLTIPLES NOMBRES ORDENADAS DE MAYOR A MENOR
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()
```

```
# SELECCIONAR LOS 25 PRINCIPALES CLIENTES CON MÚLTIPLES DIRECCIONES ORDENADOS DE MAYOR A MENOR
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()
```

```
### UNIR DATASETS Y COMPARAR COORDENADAS DE PROPIETARIO DE TARJETA DE CRÉDITO CON COORDENADAS DE TRANSACCIÓN
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM cust_info i INNER JOIN trx r
          ON i.credit_card_number == r.credit_card_number;""")
joinDf.show()
```

```
### CREAR UDF DE PYSPARK PARA CALCULAR DISTANCIA ENTRE TRANSACCIÓN Y UBICACIÓN DEL HOGAR
distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from

_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))
```

```
### SELECCIONAR CLIENTES CUYA TRANSACCIÓN OCURRIÓ A MÁS DE 100 MILLAS DEL HOGAR
distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
```

### Laboratorio 2: Usando Iceberg con PySpark

#### Iceberg Merge Into

Crear tabla Iceberg de Transacciones:

```
spark.sql("CREATE DATABASE IF NOT EXISTS SPARK_CATALOG.HOL_DB_{}".format(username))

transactionsDf.writeTo("SPARK_CATALOG.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

Cargar nuevo lote de transacciones en Vista Temporal:

```
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_1".format(storageLocation, username))
trxBatchDf.createOrReplaceTempView("trx_batch")
```

Ejemplo de sintaxis de Merge Into:

```
MERGE INTO prod.db.target t   -- una tabla objetivo
USING (SELECT ...) s          -- las actualizaciones fuente
ON t.id = s.id                -- condición para encontrar actualizaciones para filas objetivo
WHEN MATCHED AND s.op = 'delete' THEN DELETE -- actualizaciones
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET t.count = t.count + 1
WHEN NOT MATCHED THEN INSERT *
```

Ejecutar MERGE INTO para cargar nuevo lote en la tabla de Transacciones:

Comando SQL de Spark:

```
# CONTADORES ANTES DEL MERGE POR TIPO DE TRANSACCIÓN:
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()

# OPERACIÓN MERGE
spark.sql("""MERGE INTO spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} t   
USING (SELECT * FROM trx_batch) s          
ON t.credit_card_number = s.credit_card_number               
WHEN MATCHED AND t.transaction_amount < 1000 AND t.transaction_currency != "CHF" THEN UPDATE SET t.transaction_type = "invalid"
WHEN NOT MATCHED THEN INSERT *""".format(username))

# CONTADORES DESPUÉS DEL MERGE:
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()
```

#### Iceberg Time Travel / Lectura Incremental

Ahora que has agregado datos a la tabla de transacciones, puedes realizar operaciones de Iceberg Time Travel.

```
# HISTORIA DE LA TABLA ICEBERG (MUESTRA CADA SNAPSHOT Y TIMESTAMP)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.history".format(username)).show()

# SNAPSHOTS DE LA TABLA ICEBERG (ÚTIL PARA CONSULTAS INCREMENTALES Y TIME TRAVEL)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.snapshots".format(username)).show()

# AÑADIR SEGUNDO LOTE DE DATOS
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_2".format(storageLocation, username))
trxBatchDf.writeTo("spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").append()

# ALMACENAR IDS DE SNAPSHOTS PRIMERO Y ÚLTIMO DESDE LA TABLA SNAPSHOTS
snapshots_df = spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.snapshots;".format(username))
```

```
last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
second_snapshot = snapshots_df.select("snapshot_id").collect()[1][0]

incReadDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", second_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}".format(username))

print("Informe Incremental:")
incReadDf.show()
```

### Resumen

El lago de datos en CDP simplifica la analítica avanzada sobre todos los datos con una plataforma unificada para datos estructurados y no estructurados y servicios de datos integrados para habilitar cualquier caso de uso analítico desde ML, BI hasta análisis de flujo y análisis en tiempo real. Apache Iceberg es el secreto del lago de datos abierto.

Apache Iceberg es un formato de tabla abierto diseñado para cargas de trabajo analíticas grandes. Soporta evolución de esquema, particionamiento oculto, evolución del diseño de particiones y viaje en el tiempo. Cada cambio en la tabla crea un snapshot de Iceberg, lo que ayuda a resolver problemas de concurrencia y permite a los lectores escanear un estado estable de la tabla cada vez.

Iceberg se presta bien a una variedad de casos de uso, incluyendo Analítica de Lago de Datos, pipelines de Ingeniería de Datos y cumplimiento normativo con aspectos específicos de regulaciones como GDPR (Reglamento General de Protección de Datos) y CCPA (Ley de Privacidad del Consumidor de California) que requieren la capacidad de eliminar datos de clientes a solicitud.

Los Clústeres Virtuales CDE proporcionan soporte nativo para Iceberg. Los usuarios pueden ejecutar cargas de trabajo de Spark e interactuar con sus tablas de Iceberg a través de declaraciones SQL. La Capa de Metadatos de Iceberg rastrea versiones de tablas Iceberg a través de Snapshots y proporciona Tablas de Metadatos con snapshot y otra información útil. En este Laboratorio utilizamos Iceberg para acceder al conjunto de datos de transacciones con tarjeta de crédito en un momento específico.

En esta sección primero exploraste dos conjuntos de datos de manera interactiva con las sesiones interactivas de CDE. Esta función te permitió ejecutar consultas ad-hoc sobre grandes datos estructurados y no estructurados, y prototipar código de Aplicaciones Spark para ejecución por lotes.

Luego, utilizaste Iceberg Merge Into y Time Travel para primero actualizar de manera eficiente tus datos, y luego consultar tus datos a través de la dimensión temporal. Estos son solo dos ejemplos simples de cómo la Analítica de Lago de Datos con Iceberg te permite implementar pipelines de ingeniería de datos flexibles.

### Enlaces y Recursos Útiles

Si tienes curiosidad por aprender más sobre las funciones anteriores en el contexto de casos de uso más avanzados, por favor visita las siguientes referencias:

* [Apache Iceberg en la Plataforma de Datos de Cloudera](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html)
* [Explorando la Arquitectura de Iceberg](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Uso de Apache Iceberg en la Ingeniería de Datos de Cloudera](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)
* [Importación y Migración de Tabla Iceberg en Spark 3](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-iceberg-import-migrate-table.html)
* [Introducción a Iceberg y Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
* [Sintaxis SQL de Iceberg](https://iceberg.apache.org/docs/latest/spark-queries/)
