# Développement du Spark Job

* [Une Brève Introduction à Spark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_01_development.md#une-brève-introduction-à-spark)
* [Laboratoire 1 : Exécuter une Session Interactive PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_01_development.md#laboratoire-1-exécuter-une-session-interactive-pyspark)
* [Laboratoire 2 : Utiliser Iceberg avec PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_01_development.md#laboratoire-2-utiliser-iceberg-avec-pyspark)
* [Résumé](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_01_development.md#résumé)
* [Liens et Ressources Utiles](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_01_development.md#liens-et-ressources-utiles)

### Une Brève Introduction à Spark

Apache Spark est un système de traitement distribué open-source utilisé pour les charges de job de big data. Il a acquis une extrême popularité en tant que moteur de choix pour l'analyse interactive des données et le déploiement de pipelines d'Ingénierie de Données en Production et d'

Apprentissage Machine à grande échelle.

Dans CDE, vous pouvez utiliser Spark pour explorer les données de manière interactive via les Sessions CDE ou déployer des pipelines d'ingénierie de données en batch via les Jobs CDE.

### Laboratoire 1 : Exécuter une Session Interactive PySpark

Accédez à la Page d'Accueil CDE et lancez une Session PySpark. Laissez les paramètres par défaut intacts.

![alt text](../../img/part1-cdesession-1.png)

Une fois la Session prête, ouvrez l'onglet "Interact" pour entrer votre code.

![alt text](../../img/part1-cdesession-2.png)

Vous pouvez copier et coller du code à partir des instructions dans le notebook en cliquant sur l'icône en haut à droite de la cellule de code.

![alt text](../../img/part1-cdesession-3.png)

Copiez la cellule suivante dans le notebook. Avant de l'exécuter, assurez-vous d'avoir modifié la variable "username" avec votre utilisateur assigné.

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "s3a://goes-se-sandbox01/data"
username = "user002"
```

![alt text](../../img/part1-cdesession-4.png)

Aucune autre modification de code n'est nécessaire. Continuez à exécuter chaque extrait de code ci-dessous dans des cellules séparées du notebook.

```
### CHARGER LE FICHIER DES TRANSACTIONS HISTORIQUES À PARTIR DU STOCKAGE CLOUD
transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

```
### CRÉER UNE FONCTION PYTHON POUR APLANIR LES STRUCTURES NESTED DU DATAFRAME PYSPARK
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
### EXÉCUTER LA FONCTION PYTHON POUR APLANIR LES STRUCTURES NESTED ET VALIDER LE NOUVEAU SCHÉMA
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()
```

```
### RENOMMER LES COLONNES
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")
```

```
### CASTING DES TYPES DE COLONNES DE STRING AU TYPE APPROPRIÉ
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))
```

```
### CALCULER LA MOYENNE ET LA MÉDIANE DES MONTANTS DES TRANSACTIONS PAR CARTE DE CRÉDIT
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Moyenne du Montant des Transactions : ", transactionsAmountMean)
print("Médiane du Montant des Transactions : ", transactionsAmountMedian)
```

```
### CRÉER UNE VUE TEMPORAIRE SPARK À PARTIR DU DATAFRAME
transactionsDf.createOrReplaceTempView("trx")
spark.sql("SELECT * FROM trx LIMIT 10").show()
```

```
### CALCULER LE MONTANT MOYEN DES TRANSACTIONS PAR MOIS
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
```

```
### CALCULER LE MONTANT MOYEN DES TRANSACTIONS PAR JOUR DE LA SEMAINE
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
```

```
### CALCULER LE NOMBRE DE TRANSACTIONS PAR CARTE DE CRÉDIT
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
```

```
### CHARGER LES DONNÉES PII DES CLIENTS À PARTIR DU STOCKAGE CLOUD
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii".format(storageLocation, username))
piiDf.show()
piiDf.printSchema()
```

```
### CAST LAT LON AU TYPE FLOAT ET CRÉER UNE VUE TEMPORAIRE
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))
piiDf.createOrReplaceTempView("cust_info")
```

```
### SÉLECTIONNER LES 100 PRINCIPAUX CLIENTS AVEC PLUSIEURS CARTES DE CRÉDIT TRIE PAR NOMBRE DE CARTES DE CRÉDIT DU PLUS ÉLEVÉ AU PLUS BAS
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()
```

```
### SÉLECTIONNER LES 100 CARTES DE CRÉDIT PRINCIPALES AVEC PLUSIEURS NOMS TRIE DU PLUS ÉLEVÉ AU PLUS BAS
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()
```

```
# SÉLECTIONNER LES 25 CLIENTS AVEC PLUSIEURS ADRESSES TRIE DU PLUS ÉLEVÉ AU PLUS BAS
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()
```

```
### JOINDRE LES DATASETS ET COMPARER LES COORDONNÉES DU PROPRIÉTAIRE DE CARTE DE CRÉDIT AVEC LES COORDONNÉES DE TRANSACTION
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM cust_info i INNER JOIN trx r
          ON i.credit_card_number == r.credit_card_number;""")
joinDf.show()
```

```
### CRÉER UNE UDF PYSPARK POUR CALCULER LA DISTANCE ENTRE LES TRANSACTIONS ET LES LOCATIONS DOMICILIAIRES
distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))
```

```
### SÉLECTIONNER LES CLIENTS DONT LA TRANSACTION S'EST PRODUITE À PLUS DE 100 MILES DU DOMICILE
distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
```

### Laboratoire 2 : Utiliser Iceberg avec PySpark

#### Iceberg Merge Into

Créer Table Iceberg des Transactions:

```
spark.sql("CREATE DATABASE IF NOT EXISTS SPARK_CATALOG.HOL_DB_{}".format(username))

transactionsDf.writeTo("SPARK_CATALOG.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

Charger Nouveau Lot de Transactions dans Vue Temporaire:

```
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_1".format(storageLocation, username))
trxBatchDf.createOrReplaceTempView("trx_batch")
```

Exemple de Syntaxe Merge Into:

```
MERGE INTO prod.db.target t   -- une table cible
USING (SELECT ...) s          -- les mises à jour sources
ON t.id = s.id                -- condition pour trouver les mises à jour pour les lignes cibles
WHEN MATCHED AND s.op = 'delete' THEN DELETE -- mises à jour
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET

 t.count = t.count + 1
WHEN NOT MATCHED THEN INSERT *
```

Exécuter MERGE INTO pour charger le nouveau lot dans la table des Transactions:

Commande SQL Spark:

```
# COMPTEURS AVANT LE MERGE PAR TYPE DE TRANSACTION :
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()

# OPÉRATION MERGE
spark.sql("""MERGE INTO spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} t   
USING (SELECT * FROM trx_batch) s          
ON t.credit_card_number = s.credit_card_number               
WHEN MATCHED AND t.transaction_amount < 1000 AND t.transaction_currency != "CHF" THEN UPDATE SET t.transaction_type = "invalid"
WHEN NOT MATCHED THEN INSERT *""".format(username))

# COMPTEURS APRÈS LE MERGE :
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()
```

#### Iceberg Time Travel / Lecture Incrémentielle

Maintenant que vous avez ajouté des données à la table des transactions, vous pouvez effectuer des opérations de Iceberg Time Travel.

```
# HISTORIQUE DE LA TABLE ICEBERG (AFFICHE CHAQUE SNAPSHOT ET TIMESTAMP)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.history".format(username)).show()

# SNAPSHOTS DE LA TABLE ICEBERG (UTILE POUR LES REQUÊTES INCRÉMENTIELLES ET LE TIME TRAVEL)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.snapshots".format(username)).show()

# AJOUTER UN DEUXIÈME LOT DE DONNÉES
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_2".format(storageLocation, username))
trxBatchDf.writeTo("spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").append()

# STOCKER LES IDS DES SNAPSHOTS PRÉCÉDENT ET ACTUEL À PARTIR DE LA TABLE SNAPSHOTS
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

print("Rapport Incrémentiel :")
incReadDf.show()
```

### Résumé

Le lac de données dans CDP simplifie l'analyse avancée sur toutes les données avec une plateforme unifiée pour les données structurées et non structurées et des services de données intégrés pour permettre tout cas d'utilisation analytique allant de ML, BI jusqu'à l'analyse en flux et l'analyse en temps réel. Apache Iceberg est le secret du lac de données ouvert.

Apache Iceberg est un format de table ouvert conçu pour les charges de job analytiques volumineuses. Il prend en charge l'évolution du schéma, le partitionnement caché, l'évolution du design de partitions et le voyage dans le temps. Chaque changement dans la table crée un snapshot d'Iceberg, ce qui aide à résoudre les problèmes de concurrence et permet aux lecteurs de scanner un état stable de la table à chaque fois.

Iceberg est bien adapté à une variété de cas d'utilisation, y compris l'Analyse de Lac de Données, les pipelines d'Ingénierie de Données et la conformité réglementaire avec des aspects spécifiques des régulations comme le GDPR (Règlement Général sur la Protection des Données) et le CCPA (Loi sur la Protection de la Vie Privée des Consommateurs) qui exigent la capacité de supprimer des données de clients sur demande.

Les Clusters Virtuels CDE fournissent un support natif pour Iceberg. Les utilisateurs peuvent exécuter des charges de job de Spark et interagir avec leurs tables Iceberg via des déclarations SQL. La Couche de Métadonnées d'Iceberg suit les versions des tables Iceberg via des Snapshots et fournit des Tables de Métadonnées avec snapshot et autres informations utiles. Dans ce Laboratoire, nous avons utilisé Iceberg pour accéder à l'ensemble de données des transactions par carte de crédit à un moment donné.

Dans cette section, vous avez d'abord exploré deux ensembles de données de manière interactive avec les sessions interactives de CDE. Cette fonctionnalité vous a permis d'exécuter des requêtes ad-hoc sur de grandes données structurées et non structurées, et de prototyper du code d'Applications Spark pour une exécution par lot.

Ensuite, vous avez utilisé Iceberg Merge Into et Time Travel pour d'abord mettre à jour de manière efficace vos données, puis interroger vos données à travers la dimension temporelle. Ce ne sont que deux exemples simples de la manière dont l'Analyse de Lac de Données avec Iceberg vous permet de mettre en œuvre des pipelines d'ingénierie de données flexibles.

### Liens et Ressources Utiles

Si vous êtes curieux d'en savoir plus sur les fonctionnalités précédentes dans le contexte de cas d'utilisation plus avancés, veuillez visiter les références suivantes :

* [Apache Iceberg sur la Plateforme de Données Cloudera](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html)
* [Exploration de l'Architecture d'Iceberg](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Utilisation d'Apache Iceberg dans l'Ingénierie des Données Cloudera](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)
* [Importation et Migration de Table Iceberg dans Spark 3](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-iceberg-import-migrate-table.html)
* [Introduction à Iceberg et Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
* [Syntaxe SQL d'Iceberg](https://iceberg.apache.org/docs/latest/spark-queries/)
