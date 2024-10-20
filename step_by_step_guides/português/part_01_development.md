Here is the translation of the text into Portuguese:

# Desenvolvimento do Trabalho

* [Uma Breve Introdução ao Spark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#a-brief-introduction-to-spark)
* [Laboratório 1: Executar uma Sessão Interativa PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#lab-1-run-pyspark-interactive-session)
* [Laboratório 2: Usar Iceberg com PySpark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#lab-2-using-iceberg-with-pyspark)
* [Resumo](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#summary)
* [Links e Recursos Úteis](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_01_development.md#useful-links-and-resources-1)

### Uma Breve Introdução ao Spark

Apache Spark é um sistema de processamento distribuído open-source usado para cargas de trabalho de big data. Ganhou uma popularidade extrema como motor para análise interativa de dados e para a implementação de pipelines de Engenharia de Dados e Machine Learning em produção em larga escala.

No CDE você pode usar o Spark para explorar dados de forma interativa por meio das Sessões CDE ou distribuir pipelines de engenharia de dados em batch através dos Trabalhos CDE.


### Laboratório 1: Executar uma Sessão Interativa PySpark

Navegue até a Página Principal do CDE e inicie uma Sessão PySpark. Mantenha os valores padrão inalterados.

![alt text](../../img/part1-cdesession-1.png)

Uma vez que a Sessão esteja pronta, abra a aba "Interact" para inserir seu código.

![alt text](../../img/part1-cdesession-2.png)

Você pode copiar e colar o código das instruções no notebook clicando no ícone no canto superior direito da célula de código.

![alt text](../../img/part1-cdesession-3.png)

Cole a célula abaixo no notebook. Antes de executá-la, certifique-se de ter alterado a variável "username" para o seu usuário designado.

```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "s3a://goes-se-sandbox01/data"
username = "user002"
```

![alt text](../../img/part1-cdesession-4.png)

Não são necessárias mais modificações no código. Continue executando cada fragmento de código abaixo em células separadas no notebook.

```
### CARREGUE O ARQUIVO DE TRANSAÇÕES HISTÓRICAS DO ARMAZENAMENTO EM NUVEM
transactionsDf = spark.read.json("{0}/mkthol/trans/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

```
### CRIE UMA FUNÇÃO PYTHON PARA DESENROLAR DATAFRAMES NESTED STRUCTS DO PYSPARK
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
### EXECUTE A FUNÇÃO PYTHON PARA DESENROLAR OS NESTED STRUCTS E VALIDE O NOVO ESQUEMA
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()
```

```
### RENOMEIE AS COLUNAS
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")
```

```
### CAST AS TIPOS DE COLUNA DE STRING PARA O TIPO APROPRIADO
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))
```

```
### CALCULE A MÉDIA E A MEDIANA DO VALOR DA TRANSAÇÃO COM CARTÃO DE CRÉDITO
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Transaction Amount Mean: ", transactionsAmountMean)
print("Transaction Amount Median: ", transactionsAmountMedian)
```

```
### CRIE UMA VISÃO TEMPORÁRIA SPARK A PARTIR DO DATAFRAME
transactionsDf.createOrReplaceTempView("trx")
spark.sql("SELECT * FROM trx LIMIT 10").show()
```

```
### CALCULE O VALOR MÉDIO DA TRANSAÇÃO POR MÊS
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
```

```
### CALCULE O VALOR MÉDIO DA TRANSAÇÃO POR DIA DA SEMANA
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
```

```
### CALCULE O NÚMERO DE TRANSAÇÕES POR NÚMERO DO CARTÃO DE CRÉDITO
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
```

```
### CARREGUE OS DADOS PII DOS CLIENTES DO ARMAZENAMENTO EM NUVEM
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/mkthol/pii/{1}/pii".format(storageLocation, username))
piiDf.show()
piiDf.printSchema()
```

```
### CAST LAT E LON PARA O TIPO FLOAT E CRIE UMA VISÃO TEMPORÁRIA
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))
piiDf.createOrReplaceTempView("cust_info")
```

```
### SELECIONE OS 100 PRINCIPAIS CLIENTES COM MAIS CARTÕES DE CRÉDITO ORDENADOS PELO NÚMERO DE CARTÕES DO MAIS ALTO PARA O MAIS BAIXO
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()
```

```
### SELECIONE OS 100 PRINCIPAIS CARTÕES DE CRÉDITO COM MAIS NOMES ORDENADOS DO MAIS ALTO PARA O MAIS BAIXO
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()
```

```
# SELECIONE OS 25 PRINCIPAIS CLIENTES COM MAIS ENDEREÇOS ORDENADOS DO MAIS ALTO PARA O MAIS BAIXO
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()
```

```
### UNA OS DATASETS E COMPARE AS COORDENADAS DO DONO DO CARTÃO DE CRÉDITO COM AS COORDENADAS DA TRANSAÇÃO
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM cust_info i INNER JOIN trx r
          ON i.credit_card_number == r.credit_card_number;""")
joinDf.show()
```

```
### CRIE UMA UDF PYSPARK PARA CALCULAR A DISTÂNCIA ENTRE A TRANSAÇÃO E OS LOCAIS RESIDENCIAIS
distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))
```

```
### SELECIONE OS CLIENTES CUJAS TRANSAÇÕES OCORRERAM A MAIS DE 100 MILHAS DE CASA
distanceDf.filter(distanceDf

.trx_dist_from_home > 100).show()
```

### Laboratório 2: Usar Iceberg com PySpark

#### Iceberg Merge Into

Crie a tabela Iceberg das Transações:

```
spark.sql("CREATE DATABASE IF NOT EXISTS SPARK_CATALOG.HOL_DB_{}".format(username))

transactionsDf.writeTo("SPARK_CATALOG.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

Carregue um Novo Batch de Transações na Visualização Temporária:

```
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_1".format(storageLocation, username))
trxBatchDf.createOrReplaceTempView("trx_batch")
```

Exemplo de Sintaxe de Merge Into:

```
MERGE INTO prod.db.target t   -- uma tabela de destino
USING (SELECT ...) s          -- os updates da fonte
ON t.id = s.id                -- condição para encontrar atualizações para as linhas de destino
WHEN MATCHED AND s.op = 'delete' THEN DELETE -- atualizações
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET t.count = t.count + 1
WHEN NOT MATCHED THEN INSERT *
```

Execute MERGE INTO para carregar um novo batch na tabela das Transações:

Comando Spark SQL:

```
# CONTAGENS PRÉ-MERGE POR TIPO DE TRANSAÇÃO:
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()

# OPERAÇÃO DE MERGE
spark.sql("""MERGE INTO spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} t   
USING (SELECT * FROM trx_batch) s          
ON t.credit_card_number = s.credit_card_number               
WHEN MATCHED AND t.transaction_amount < 1000 AND t.transaction_currency != "CHF" THEN UPDATE SET t.transaction_type = "invalid"
WHEN NOT MATCHED THEN INSERT *""".format(username))

# CONTAGEM PÓS-MERGE:
spark.sql("""SELECT TRANSACTION_TYPE, COUNT(*) FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0} GROUP BY TRANSACTION_TYPE""".format(username)).show()
```

#### Iceberg Time Travel / Leitura Incremental

Agora que você adicionou dados à tabela das transações, você pode executar operações de Time Travel com Iceberg.

```
# HISTÓRIA DA TABELA ICEBERG (MOSTRA CADA SNAPSHOT E TIMESTAMP)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.history".format(username)).show()

# SNAPSHOT DA TABELA ICEBERG (ÚTIL PARA CONSULTAS INCREMENTAIS E TIME TRAVEL)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}.snapshots".format(username)).show()

# ADICIONE UM SEGUNDO BATCH DE DADOS
trxBatchDf = spark.read.schema("credit_card_number string, credit_card_provider string, event_ts timestamp, latitude double, longitude double, transaction_amount long, transaction_currency string, transaction_type string").json("{0}/mkthol/trans/{1}/trx_batch_2".format(storageLocation, username))
trxBatchDf.writeTo("spark_catalog.HOL_DB_{0}.TRANSACTIONS_{0}".format(username)).using("iceberg").append()

# ARMAZENE O PRIMEIRO E O ÚLTIMO ID SNAPSHOT DA TABELA DOS SNAPSHOTS
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

print("Relatório Incremental:")
incReadDf.show()
```

### Resumo

O data lakehouse no CDP simplifica a análise avançada em todos os dados com uma plataforma unificada para dados estruturados e não estruturados e serviços de dados integrados para habilitar qualquer caso de uso analítico, desde ML, BI até análise em streaming e em tempo real. Apache Iceberg é o segredo do lakehouse aberto.

Apache Iceberg é um formato de tabela aberto projetado para cargas de trabalho analíticos de grandes dimensões. Suporta evolução de esquema, particionamento oculto, evolução do layout de particionamento e time travel. Cada mudança na tabela cria um snapshot do Iceberg, o que ajuda a resolver problemas de concorrência e permite que os leitores examinem um estado estável da tabela sempre que necessário.

Iceberg é bem adequado para uma variedade de casos de uso, incluindo Análise de Lakehouse, pipelines de Engenharia de Dados e conformidade normativa com aspectos específicos de regulamentações como LGPD (Lei Geral de Proteção de dados), GDPR (Regulamento Geral sobre a Proteção de Dados) e CCPA (California Consumer Privacy Act) que exigem a capacidade de eliminar dados de clientes sob demanda.

Os Clusters Virtuais CDE fornecem suporte nativo para Iceberg. Os usuários podem executar cargas de trabalho Spark e interagir com suas tabelas Iceberg por meio de declarações SQL. O Nível de Metadados do Iceberg rastreia as versões da tabela Iceberg por meio de Snapshots e fornece Tabelas de Metadados com informações sobre snapshots e outras informações úteis. Neste Laboratório, usamos o Iceberg para acessar o dataset das transações com cartão de crédito em um timestamp específico.

Nesta seção, você primeiro explorou dois datasets de forma interativa com as sessões interativas do CDE. Essa funcionalidade permitiu que você executasse consultas ad-hoc sobre dados grandes, estruturados e não estruturados, e prototipasse código Spark Application para execução em batch.

Em seguida, você utilizou o Apache Iceberg Merge Into e Time Travel para, primeiro, executar eficientemente o upsert de seus dados e depois consultar seus dados através da dimensão temporal. Estes são apenas dois exemplos simples de como as análises Iceberg Lakehouse permitem implementar pipelines de engenharia de dados flexíveis.

### Links e Recursos Úteis

Se você está curioso para saber mais sobre essas funcionalidades no contexto de casos de uso mais avançados, visite os seguintes recursos:

* [Apache Iceberg na Cloudera Data Platform](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html)
* [Explorar a Arquitetura do Iceberg](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Usando Apache Iceberg no Cloudera Data Engineering](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)
* [Importação e Migração de Tabelas Iceberg no Spark 3](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-iceberg-import-migrate-table.html)
* [Introdução ao Iceberg e Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
* [Sintaxe SQL do Iceberg](https://iceberg.apache.org/docs/latest/spark-queries/)
