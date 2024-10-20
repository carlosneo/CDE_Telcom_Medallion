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
    .appName("TELCO GOLD LAYER") \
    .getOrCreate()

storageLocation='abfs://data@telefonicabrstor661f42a0.dfs.core.windows.net'
print("Storage Location from Config File: ", storageLocation)

username = sys.argv[1]
print("PySpark Runtime Arg: ", sys.argv[1])


#-----------------------------------------------------
#               JOIN INCREMENTAL READ WITH CUST INFO
#-----------------------------------------------------

### CREATE GOLD TABLES

#### PRODUCT NAMES COUNT
productNamesGoldDf = spark.sql("SELECT PRODUCTID, PRODUCTNAME, COUNT(*) FROM SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER GROUP BY PRODUCTID, PRODUCTNAME")
productNamesGoldDf.writeTo("SPARK_CATALOG.DEFAULT.PRODUCT_NAMES_GOLD").using("iceberg").createOrReplace()

#### COUNT USERS PER PRODUCT
countUsersPerProductGoldDf = spark.sql("SELECT PRODUCTID, COUNT(USER_ID) FROM SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER")
countUsersPerProductGoldDf.writeTo("SPARK_CATALOG.DEFAULT.COUNT_USERS_PER_PRODUCT_GOLD").using("iceberg").createOrReplace()

#### AVERAGE GROSS VALUE PER PRODUCT
avgValPerProductGoldDf = spark.sql("SELECT PRODUCTID, AVG(GROSSVALUE) FROM SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER")
avgValPerProductGoldDf.writeTo("SPARK_CATALOG.DEFAULT.AVG_VALUE_PER_PRODUCT_GOLD").using("iceberg").createOrReplace()

#### MONITORING
monitoringKeyFieldsGoldDf = spark.sql("SELECT NU_TLFN, PRODUCTID, NRPROTOCOLO, INTERESSE FROM SPARK_CATALOG.DEFAULT.FACTS_SILVER")
monitoringKeyFieldsGoldDf = monitoringKeyFieldsGoldDf.dropDuplicates()
monitoringKeyFieldsGoldDf.writeTo("SPARK_CATALOG.DEFAULT.MONITORING_KEY_FIELDS_GOLD").using("iceberg").createOrReplace()
