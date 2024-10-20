# Sviluppo del Job Spark

* [Una Breve Introduzione a Spark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#a-brief-introduction-to-spark)
* [Laboratorio 1: Esegui una Sessione Interattiva PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#lab-1-run-pyspark-interactive-session)
* [Laboratorio 2: Utilizzare Icberg con PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#lab-2-using-icberg-with-pyspark)
* [Riassunto](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#summary)
* [Link e Risorse Utili](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#useful-links-and-resources-1)

### Una Breve Introduzione a Spark

Apache Spark è un sistema di elaborazione distribuito open-source utilizzato per carichi di lavoro di big data. Ha guadagnato una popolarità estrema come motore per l'analisi interattiva dei dati e il dispiegamento di pipeline di Ingegneria dei Dati e Machine Learning in produzione su larga scala.

In CDE puoi utilizzare Spark per esplorare i dati in modo interattivo tramite le Sessioni CDE o distribuire pipeline di ingegneria dei dati batch tramite i Lavori CDE.


### Laboratorio 1: Esegui una Sessione Interattiva PySpark

Naviga alla Pagina Principale di CDE e avvia una Sessione PySpark. Lascia intatti i valori predefiniti.

![alt text](../../img/part1-cdesession-1.png)

Una volta che la Sessione è pronta, apri la scheda "Interact" per inserire il tuo codice.

![alt text](../../img/part1-cdesession-2.png)

Puoi copiare e incollare il codice dalle istruzioni nel notebook facendo clic sull'icona in alto a destra della cella di codice.

![alt text](../../img/part1-cdesession-3.png)

Copia la cella seguente nel notebook. Prima di eseguirla, assicurati di aver modificato la variabile "username" con il tuo utente assegnato.

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "s3a://goes-se-sandbox01/data"
username = "user002"
```

![alt text](../../img/part1-cdesession-4.png)

Non sono necessarie ulteriori modifiche al codice. Continua a eseguire ogni frammento di codice di seguito in celle separate nel notebook.

```
### CARICA IL FILE DI TRANSAZIONI STORICHE DAL CLOUD STORAGE
transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

```
### CREA UNA FUNZIONE PYTHON PER APPiATTIRE I DATAFRAME NESTED STRUCTS DI PYSPARK
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
### ESEGUI LA FUNZIONE PYTHON PER APPiATTIRE I NESTED STRUCTS E VALIDA IL NUOVO SCHEMA
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()
```

```
### RINOMINA LE COLONNE
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")
```

```
### CASTA I TIPI DI COLONNA DA STRINGA AL TIPO APPROPRIATO
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))
```

```
### CALCOLA LA MEDIA E LA MEDIANA DELL'IMPORTO DELLA TRANSAZIONE CON CARTA DI CREDITO
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Transaction Amount Mean: ", transactionsAmountMean)
print("Transaction Amount Median: ", transactionsAmountMedian)
```

```
### CREA UNA VISTA TEMPORANEA SPARK DAL DATAFRAME
transactionsDf.createOrReplaceTempView("trx")
spark.sql("SELECT * FROM trx LIMIT 10").show()
```

```
### CALCOLA L'IMPORTO MEDIO DELLA TRANSAZIONE PER MESE
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
```

```
### CALCOLA L'IMPORTO MEDIO DELLA TRANSAZIONE PER GIORNO DELLA SETTIMANA
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
```

```
### CALCOLA IL NUMERO DI TRANSAZIONI PER CARTA DI CREDITO
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
```

```
### CARICA I DATI PII DEI CLIENTI DAL CLOUD STORAGE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii".format(storageLocation, username))
piiDf.show()
piiDf.printSchema()
```

```
### CASTA LAT E LON A TIPO FLOAT E CREA UNA VISTA TEMPORANEA
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))
piiDf.createOrReplaceTempView("cust_info")
```

```
### SELEZIONA I PRIMI 100 CLIENTI CON PIÙ CARTE DI CREDITO ORDINATI PER NUMERO DI CARTE DAL PIÙ ALTO AL PIÙ BASSO
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()
```

```
### SELEZIONA LE PRIME 100 CARTE DI CREDITO CON PIÙ NOMI ORDINATI DAL PIÙ ALTO AL PIÙ BASSO
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()
```

```
# SELEZIONA I PRIMI 25 CLIENTI CON PIÙ INDIRIZZI ORDINATI DAL PIÙ ALTO AL PIÙ BASSO
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()
```

```
### UNISCI I DATASET E COMPARA LE COORDINATE DEL PROPRIETARIO DELLA CARTA DI CREDITO CON LE COORDINATE DELLA TRANSAZIONE
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM cust_info i INNER JOIN trx r
          ON i.credit_card_number == r.credit_card_number;""")
joinDf.show()
```

```
### CREA UNA UDF PYSPARK PER CALCOLARE LA DISTANZA TRA LA TRANSAZIONE E LE LOCALITÀ DOMICILIARI
distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",


                                                                            "address_latitude", "address_longitude")))
```

```
### SELEZIONA I CLIENTI LE CUI TRANSAZIONI SONO AVVENUTE A PIÙ DI 100 MIGLIA DA CASA
distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
```


### Laboratorio 2: Utilizzare Icberg con PySpark

#### Iceberg Merge Into

Crea la tabella Iceberg delle Transazioni:

```
spark.sql("CREATE DATABASE IF NOT EXISTS SPARK_CATALOG.HOL_DB_{}".format(username))

transactionsDf.writeTo("SPARK_CATALOG.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

Carica un Nuovo Batch di Transazioni nella Vista Temp:

```
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_1".format(storageLocation, username))
trxBatchDf.createOrReplaceTempView("trx_batch")
```

Sintassi Esempio di Merge Into:

```
MERGE INTO prod.db.target t   -- una tabella di destinazione
USING (SELECT ...) s          -- gli aggiornamenti della sorgente
ON t.id = s.id                -- condizione per trovare aggiornamenti per le righe di destinazione
WHEN MATCHED AND s.op = 'delete' THEN DELETE -- aggiornamenti
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET t.count = t.count + 1
WHEN NOT MATCHED THEN INSERT *
```

Esegui MERGE INTO per caricare un nuovo batch nella tabella delle Transazioni:

Comando Spark SQL:

```
# CONTEGGI PRE-MERGE PER TIPO DI TRANSAZIONE:
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()

# OPERAZIONE DI MERGE
spark.sql("""MERGE INTO spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} t   
USING (SELECT * FROM trx_batch) s          
ON t.credit_card_number = s.credit_card_number               
WHEN MATCHED AND t.transaction_amount < 1000 AND t.transaction_currency != "CHF" THEN UPDATE SET t.transaction_type = "invalid"
WHEN NOT MATCHED THEN INSERT *""".format(username))

# CONTEGGIO POST-MERGE:
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()
```

#### Iceberg Time Travel / Lettura Incrementale

Ora che hai aggiunto dati alla tabella delle transazioni, puoi eseguire operazioni di Time Travel con Iceberg.

```
# STORIA DELLA TABELLA ICEBERG (MOSTRA OGNI SNAPSHOT E TIMESTAMP)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.history".format(username)).show()

# SNAPSHOT DELLA TABELLA ICEBERG (UTILE PER LE QUERY INCREMENTALI E TIME TRAVEL)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.snapshots".format(username)).show()

# AGGIUNGI UN SECONDO BATCH DI DATI
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_2".format(storageLocation, username))
trxBatchDf.writeTo("spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").append()

# MEMORIZZA I PRIMI E GLI ULTIMI ID SNAPSHOT DALLA TABELLA DEGLI SNAPSHOT
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

print("Incremental Report:")
incReadDf.show()
```

### Riassunto

Il data lakehouse su CDP semplifica l'analisi avanzata su tutti i dati con una piattaforma unificata per dati strutturati e non strutturati e servizi dati integrati per abilitare qualsiasi caso d'uso analitico, dal ML, BI all'analisi in streaming e in tempo reale. Apache Iceberg è il segreto dell'aperto lakehouse.

Apache Iceberg è un formato di tabella aperto progettato per carichi di lavoro analitici di grandi dimensioni. Supporta l'evoluzione dello schema, la partizione nascosta, l'evoluzione del layout di partizione e il time travel. Ogni cambiamento della tabella crea uno snapshot di Iceberg, questo aiuta a risolvere i problemi di concorrenza e consente ai lettori di esaminare uno stato stabile della tabella ogni volta.

Iceberg si presta bene a una varietà di casi d'uso, tra cui Lakehouse Analytics, pipeline di Ingegneria dei Dati e conformità normativa con aspetti specifici delle normative come GDPR (Regolamento Generale sulla Protezione dei Dati) e CCPA (California Consumer Privacy Act) che richiedono di poter eliminare i dati dei clienti su richiesta.

I Cluster Virtuali CDE forniscono supporto nativo per Iceberg. Gli utenti possono eseguire carichi di lavoro Spark e interagire con le loro tabelle Iceberg tramite dichiarazioni SQL. Il Livello di Metadati di Iceberg tiene traccia delle versioni della tabella Iceberg tramite Snapshots e fornisce Tabelle di Metadati con informazioni su snapshot e altre informazioni utili. In questo Laboratorio abbiamo utilizzato Iceberg per accedere al dataset delle transazioni con carta di credito a un determinato timestamp.

In questa sezione hai prima esplorato due dataset in modo interattivo con le sessioni interattive di CDE. Questa funzione ti ha permesso di eseguire query ad-hoc su dati grandi, strutturati e non strutturati, e di prototipare codice Spark Application per l'esecuzione batch.

Poi, hai sfruttato Apache Iceberg Merge Into e Time Travel per prima cosa eseguire efficientemente l'upsert dei tuoi dati e poi interrogare i tuoi dati attraverso la dimensione temporale. Questi sono solo due semplici esempi di come le analisi Iceberg Lakehouse ti permettano di implementare pipeline di ingegneria dei dati flessibili.

### Link e Risorse Utili

Se sei curioso di saperne di più su queste funzionalità nel contesto di casi d'uso più avanzati, visita i seguenti riferimenti:

* [Apache Iceberg nella Cloudera Data Platform](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html)
* [Esplorare l'Architettura di Iceberg](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Utilizzare Apache Iceberg in Cloudera Data Engineering](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)
* [Importazione e Migrazione di Tabelle Iceberg in Spark 3](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-iceberg-import-migrate-table.html)
* [Introduzione a Iceberg e Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
* [Sintassi SQL di Iceberg](https://iceberg.apache.org/docs/latest/spark-queries/)
