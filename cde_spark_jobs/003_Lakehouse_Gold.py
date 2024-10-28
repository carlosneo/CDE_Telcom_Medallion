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

username = sys.argv[1]
print("PySpark Runtime Arg: ", username)

storageLocation=sys.argv[2]
print("Storage Location: ", storageLocation)

#-----------------------------------------------------
#               JOIN INCREMENTAL READ WITH CUST INFO
#-----------------------------------------------------

### CREATE GOLD TABLES

#### PRODUCT NAMES COUNT
productNamesGoldDf = spark.sql("SELECT PRODUCTNAME, COUNT(*) FROM SPARK_CATALOG.TELCO_MEDALLION.PRODUCTS_SILVER GROUP BY PRODUCTNAME")
productNamesGoldDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.PRODUCT_NAMES_GOLD").using("iceberg").createOrReplace()

#### COUNT USERS PER PRODUCT
countUsersPerProductGoldDf = spark.sql("SELECT PRODUCTID, COUNT(USER_ID) FROM SPARK_CATALOG.TELCO_MEDALLION.PRODUCTS_SILVER GROUP BY PRODUCTID")
countUsersPerProductGoldDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.COUNT_USERS_PER_PRODUCT_GOLD").using("iceberg").createOrReplace()

#### AVERAGE GROSS VALUE PER PRODUCT
avgValPerProductGoldDf = spark.sql("SELECT PRODUCTID, AVG(GROSSVALUE) FROM SPARK_CATALOG.TELCO_MEDALLION.PRODUCTS_SILVER GROUP BY PRODUCTID")
avgValPerProductGoldDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.AVG_VALUE_PER_PRODUCT_GOLD").using("iceberg").createOrReplace()

#### MONITORING
monitoringKeyFieldsGoldDf = spark.sql("SELECT IDATENDIMENTO, QTINSISTENCIA, NU_TLFN, DT_PRMR_ATCV_LNHA, PRODUCTID, NRPROTOCOLO, INTERESSE \
                                        FROM SPARK_CATALOG.TELCO_MEDALLION.FACTS_SILVER")
monitoringKeyFieldsGoldDf = monitoringKeyFieldsGoldDf.dropDuplicates()
monitoringKeyFieldsGoldDf.writeTo("SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD")\
                            .partitionedBy(F.months("dt_prmr_atcv_lnha")) \
                            .tableProperty("write.spark.fanout.enabled", "true") \
                            .using("iceberg").createOrReplace()

#---------------------------------------------------
#               ICEBERG SCHEMA EVOLUTION
#---------------------------------------------------

# DROP COLUMNS
print("SCHEMA EVOL: EXECUTING ICEBERG DROP NRPROTOCOLO COLUMN ON MONITORING_KEY_FIELDS_GOLD TABLE:")
print("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD DROP COLUMN NRPROTOCOLO")
spark.sql("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD DROP COLUMN NRPROTOCOLO")

# RENAME COLUMN
print("SCHEMA EVOL: EXECUTING ICEBERG RENAME INTERESSE COLUMN ON MONITORING_KEY_FIELDS_GOLD TABLE:")
print("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD RENAME COLUMN INTERESSE TO INTER")
spark.sql("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD RENAME COLUMN INTERESSE TO INTER")

# CAST COLUMN TO BIGINT
print("SCHEMA EVOL: EXECUTING ICEBERG TYPE CONVERSION ON QTINSISTENCIA MONITORING_KEY_FIELDS_GOLD TABLE:")
print("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD ALTER COLUMN QTINSISTENCIA TYPE BIGINT")
spark.sql("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD ALTER COLUMN QTINSISTENCIA TYPE BIGINT")

#---------------------------------------------------
#               ICEBERG PARTITION EVOLUTION
#---------------------------------------------------
#dt_prmr_atcv_lnha
print("PARTITION EVOL: REPLACE PARTITION FIELD MONTH WITH FIELD DAY ON ATENDIMENTO BRONZE TABLE\n")

print("\nATENDIMENTO BRONZE TABLE PARTITIONS AFTER ALTER PARTITION STATEMENT: ")
spark.sql("SELECT * FROM SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD.PARTITIONS").show()

print("\nALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD DROP PARTITION FIELD MONTHS(DT_PRMR_ATCV_LNHA)")
spark.sql("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD DROP PARTITION FIELD MONTHS(DT_PRMR_ATCV_LNHA)")

print("\nALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD ADD PARTITION FIELD DAYS(DT_PRMR_ATCV_LNHA)")
spark.sql("ALTER TABLE SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD ADD PARTITION FIELD DAYS(DT_PRMR_ATCV_LNHA)")

print("\ATENDIMENTO BRONZE TABLE PARTITIONS AFTER ALTER PARTITION STATEMENT: ")
spark.sql("SELECT * FROM SPARK_CATALOG.TELCO_MEDALLION.MONITORING_KEY_FIELDS_GOLD.PARTITIONS").show()
