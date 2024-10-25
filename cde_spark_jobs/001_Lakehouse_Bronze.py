#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys, random, os, json, random, configparser
from utils import *

spark = SparkSession \
    .builder \
    .appName("TELCO LAKEHOUSE BRONZE LAYER") \
    .getOrCreate()

username = sys.argv[1]
print("PySpark Runtime Arg: ", username)

storageLocation=sys.argv[2]
print("Storage Location: ", storageLocation)

### RECREATE DATABASE AND TRX TABLE
#spark.sql("DROP DATABASE IF EXISTS SPARK_CATALOG.TELCO_DB_{} CASCADE".format(username))
#spark.sql("CREATE DATABASE IF NOT EXISTS SPARK_CATALOG.TELCO_DB_{}".format(username))

#---------------------------------------------------
#               CREATE BRONZE TABLES
#---------------------------------------------------

INTEREST = '{0}/telco/demo/{1}/interest'.format(storageLocation, username)
PRODUCT_SUBSCRIPTION = '{0}/telco/demo/{1}/productsubscription'.format(storageLocation, username)
SVA_SUBSCRIPTION = '{0}/telco/demo/{1}/svasub'.format(storageLocation, username)
ATENDIMENTO = '{0}/telco/demo/{1}/atendimento'.format(storageLocation, username)
NAVIGATION = "{0}/telco/demo/{1}/navigation".format(storageLocation, username)

### LOAD INTEREST DATA FROM CLOUD STORAGE
interestDf = spark.read.parquet(INTEREST)
interestDf.printSchema()

### LOAD PRODUCT DATA SUBSCRIPTION FROM CLOUD STORAGE
productSubscriptionDf = spark.read.parquet(PRODUCT_SUBSCRIPTION)
productSubscriptionDf.printSchema()

### LOAD INTEREST DATA FROM CLOUD STORAGE
svaSubDf = spark.read.parquet(SVA_SUBSCRIPTION)
svaSubDf.printSchema()

### LOAD INTEREST DATA FROM CLOUD STORAGE
atendimentoDf = spark.read.parquet(ATENDIMENTO)
atendimentoDf.printSchema()

### LOAD ANTENNA DATA FROM CLOUD STORAGE
navigationDf = spark.read.parquet(NAVIGATION)
navigationDf.printSchema()

### RENAME MULTIPLE COLUMNS
#cols = [col for col in transactionsDf.columns if col.startswith("transaction")]
#new_cols = [col.split(".")[1] for col in cols]
#transactionsDf = renameMultipleColumns(transactionsDf, cols, new_cols)

### CAST TYPES
#cols = ["transaction_amount", "latitude", "longitude"]
#transactionsDf = castMultipleColumns(transactionsDf, cols)
#transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))

## CREATE BRONZE DB & TABLES IF NOT EXISTS
print("CREATE TELCO DB: TELCO_MEDALLION")
spark.sql("CREATE DATABASE IF NOT EXISTS TELCO_MEDALLION")

"""print("REMOVE PRIOR TABLE BRANCHES IF THEY EXIST")
try:
    spark.sql("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.PRODUCTS_SILVER DROP BRANCH ingestion_branch")
    print("REMOVE INGESTION BRANCH FROM PRODUCTS_SILVER SUCCESSFUL")
except:
    print("REMOVE INGESTION BRANCH FROM PRODUCTS_SILVER UNSUCCESSFUL")
    pass

try:
    spark.sql("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.FACTS_SILVER DROP BRANCH ingestion_branch")
    print("REMOVE INGESTION BRANCH FROM FACTS_SILVER SUCCESSFUL")
except:
    print("REMOVE INGESTION BRANCH FROM FACTS_SILVER UNSUCCESSFUL")
    pass"""

print("ATENDIMENTO BRONZE")
try:
    atendimentoDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.ATENDIMENTO_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        partitionedBy(F.months("dtprazofinalanatel")). \
        using("iceberg"). \
        create()
    print("CREATED ATENDIMENTO BRONZE")
except Exception as e:
    print("ATENDIMENTO BRONZE ALREADY EXISTS")
    print('\n')
    print(f'caught {type(e)}: e')
    print("PERFORMING APPEND TO ATENDIMENTO BRONZE INSTEAD")
    print('\n')
    atendimentoDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.ATENDIMENTO_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        partitionedBy(F.months("dtprazofinalanatel")). \
        using("iceberg"). \
        append()
    print(e)

print("PRODUCT SUBSCRIPTION BRONZE")
try:
    productSubscriptionDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.PRODUCT_SUBSCRIPTION_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        create()
    print("CREATED PRODUCT SUBSCRIPTION BRONZE")
except Exception as e:
    print("PRODUCT SUBSCRIPTION BRONZE ALREADY EXISTS")
    print('\n')
    print(f'caught {type(e)}: e')
    print("PERFORMING APPEND TO PRODUCT SUBSCRIPTION BRONZE INSTEAD")
    print('\n')
    productSubscriptionDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.PRODUCT_SUBSCRIPTION_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        append()
    print(e)

print("SVA SUBSCRIPTION BRONZE")
try:
    svaSubDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.SVA_SUBSCRIPTION_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        create()
    print("CREATED SVA SUBSCRIPTION BRONZE")
except Exception as e:
    print("SVA SUBSCRIPTION BRONZE ALREADY EXISTS")
    print('\n')
    print(f'caught {type(e)}: e')
    print("PERFORMING APPEND TO SVA SUBSCRIPTION BRONZE INSTEAD")
    print('\n')
    svaSubDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.SVA_SUBSCRIPTION_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        append()
    print(e)

print("INTEREST BRONZE")
try:
    interestDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.INTEREST_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        create()
    print("CREATED INTEREST BRONZE")
except Exception as e:
    print("INTEREST BRONZE ALREADY EXISTS")
    print('\n')
    print(f'caught {type(e)}: e')
    print("PERFORMING APPEND TO INTEREST BRONZE INSTEAD")
    print('\n')
    interestDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.INTEREST_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        append()
    print(e)

print("NAVIGATION BRONZE")
try:
    navigationDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.NAVIGATION_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        create()
    print("CREATED NAVIGATION BRONZE")
except Exception as e:
    print("NAVIGATION BRONZE ALREADY EXISTS")
    print('\n')
    print(f'caught {type(e)}: e')
    print("PERFORMING APPEND TO NAVIGATION BRONZE INSTEAD")
    print('\n')
    navigationDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.NAVIGATION_BRONZE"). \
        tableProperty("write.format.default", "parquet"). \
        tableProperty("write.spark.fanout.enabled", "true"). \
        using("iceberg"). \
        append()
    print(e)
