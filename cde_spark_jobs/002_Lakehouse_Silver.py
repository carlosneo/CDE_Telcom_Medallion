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
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

spark = SparkSession \
    .builder \
    .appName("TELCO LAKEHOUSE SILVER LAYER") \
    .getOrCreate()

storageLocation='abfs://data@telefonicabrstor661f42a0.dfs.core.windows.net'
print("Storage Location from Config File: ", storageLocation)

username = sys.argv[1]
print("PySpark Runtime Arg: ", sys.argv[1])


#---------------------------------------------------
#               INCREMENTAL READ ON BRONZE LAYER
#---------------------------------------------------

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.ATENDIMENTO_BRONZE.history").show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.ATENDIMENTO_BRONZE.snapshots").show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE FOR ATENDIMENTO_BRONZE
snapshots_df = spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.ATENDIMENTO_BRONZE.snapshots;")

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
second_snapshot = snapshots_df.select("snapshot_id").collect()[0][0]

incReadAtendimentoDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", second_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("SPARK_CATALOG.DEFAULT.ATENDIMENTO_BRONZE")

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.PRODUCT_SUBSCRIPTION_BRONZE.history").show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.PRODUCT_SUBSCRIPTION_BRONZE.snapshots").show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE FOR PRODUCT_SUBSCRIPTION_BRONZE
snapshots_df = spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.PRODUCT_SUBSCRIPTION_BRONZE.snapshots;")

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
second_snapshot = snapshots_df.select("snapshot_id").collect()[0][0]

incReadProdSubDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", second_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("SPARK_CATALOG.DEFAULT.PRODUCT_SUBSCRIPTION_BRONZE")

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.SVA_SUBSCRIPTION_BRONZE.history").show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.SVA_SUBSCRIPTION_BRONZE.snapshots").show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE FOR SVA SUBSCRIPTION
snapshots_df = spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.SVA_SUBSCRIPTION_BRONZE.snapshots;".format(username))

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
second_snapshot = snapshots_df.select("snapshot_id").collect()[0][0]

incReadSvaSubDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", second_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("SPARK_CATALOG.DEFAULT.SVA_SUBSCRIPTION_BRONZE")

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.INTEREST_BRONZE.history").show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.INTEREST_BRONZE.snapshots").show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE FOR INTEREST_BRONZE
snapshots_df = spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.INTEREST_BRONZE.snapshots;".format(username))

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
second_snapshot = snapshots_df.select("snapshot_id").collect()[0][0]

incReadInterestDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", second_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("SPARK_CATALOG.DEFAULT.INTEREST_BRONZE")

#---------------------------------------------------
#               JOIN INCREMENTAL READS
#---------------------------------------------------

## PREPARING INCREMENTAL READ FOR SILVER TABLE: PRODUCTS

incProductsDf = incReadSvaSubDf.join(incReadProdSubDf, \
                                        incReadSvaSubDf.productid == incReadProdSubDf.id_prdt, \
                                        "inner")

## PREPARING INCREMENTAL READ FOR SILVER TABLE: FACT

incFactsDf = incReadSvaSubDf.join(incReadProdSubDf, incReadSvaSubDf.productid == incReadProdSubDf.id_prdt, "inner")\
                            .join(incReadAtendimentoDf, incReadAtendimentoDf.msisdn == incReadProdSubDf.nu_tlfn, "inner").drop(incReadAtendimentoDf.msisdn)\
                            .join(incReadInterestDf, incReadInterestDf.msisdn == incReadProdSubDf.nu_tlfn, "inner").drop(incReadInterestDf.msisdn)

columns = ["nu_tlfn", "nu_doct", "user_id", "dt_prmr_atcv_lnha", "dt_dstv_lnha", "ds_prdt", \
            "ds_plno", "uf", "no_lgrd", "no_imovel", "no_brro", "msisdn", \
            "productid", "productname", "servicetype", "idatendimento", "cpf", "dtabertura", "qtinsistencia", \
            "nivel", "nrprotocolo", "interesse"]

incFactsDf = incFactsDf.select(*columns)

#---------------------------------------------------
#               VALIDATE BATCH DATA IN BRANCH
#---------------------------------------------------

# validate the data quality of the sales data with great-expectations

geIncProductsDf = SparkDFDataset(incProductsDf)

geIncProductsDfValidation = geIncProductsDf.expect_column_max_to_be_between(column="COMPANY", min_value=0, max_value=1000000000)

print(f"VALIDATION RESULTS FOR PRODUCTS BATCH DATA:\n{geIncProductsDfValidation}\n")
assert geIncProductsDfValidation.success, \
    "VALIDATION FOR PRODUCTS TABLE UNSUCCESSFUL: MAX VALUE OUTSIDE SPECIFIED RANGE."

geIncFactsDf = SparkDFDataset(incFactsDf)

geIncFactsDfValidation = geIncFactsDf.expect_column_max_to_be_between(column="NIVEL", min_value=0, max_value=100000)

print(f"VALIDATION RESULTS FOR FACTS BATCH DATA:\n{geIncFactsDfValidation}\n")
assert geIncFactsDfValidation.success, \
    "VALIDATION FOR FACTS TABLE UNSUCCESSFUL: MAX VALUE OUTSIDE SPECIFIED RANGE."

# create temp views after data quality tests have been passed

incProductsDf.createOrReplaceTempView("INCPRODUCTS")
incFactsDf.createOrReplaceTempView("INCFACTS")


#---------------------------------------------------
#               CREATE SILVER LAYER TABLES
#---------------------------------------------------

### CREATE SILVER LAYER TABLES FACT TABLE
# Spark SQL Command:
spark.sql("""
CREATE TABLE IF NOT EXISTS SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER (
  `nu_tlfn` STRING,
  `nu_doct` STRING,
  `user_id` STRING,
  `id_prdt` INT,
  `dt_prmr_atcv_lnha` TIMESTAMP,
  `dt_dstv_lnha` TIMESTAMP,
  `ds_plno` STRING,
  `id_estd_lnha` INT,
  `cd_ddd` STRING,
  `UF` STRING,
  `NO_LGRD` STRING,
  `NO_IMOVEL` STRING,
  `NO_BRRO` STRING,
  `NU_CEP` STRING,
  `NO_MNCO` STRING,
  `CD_IBGE_MNCO` STRING,
  `ID_DISP_XDSL` INT,
  `ID_DISP_FTTC` INT,
  `ID_DISP_FTTH` INT,
  `FL_PLNO_DSCN` INT,
  `FL_DEBT_AUTM` INT,
  `FL_CNTA_ONLN` INT,
  `FL_PLNO_TTLR` INT,
  `NU_IMEI_APRL` STRING,
  `DS_MODL_ORIG_APRL` STRING,
  `FL_VIVO_TOTAL` INT,
  `DT_TRCA_APRL` TIMESTAMP,
  `DT_INI_PLNO` TIMESTAMP,
  `MSISDN` STRING,
  `PRODUCTID` STRING,
  `DATACONTRATACAO` STRING,
  `PRODUCTNAME` STRING,
  `PROTOCOL_NUMBER` STRING,
  `SPNAME` STRING,
  `SUBSCRIBESTATE` STRING,
  `PRODUCTACCOUNTINGGROUP` STRING,
  `CLIENT` STRING,
  `SERVICETYPE` STRING,
  `TPLINHA` STRING,
  `GROSSVALUE` INT,
  `COMPANY` INT,
  `TAXPIS` DECIMAL,
  `TAXCOFINS` DECIMAL,
  `TAXISS` DECIMAL,
  `DISCOUNTVALUE` STRING
  )
USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS SPARK_CATALOG.DEFAULT.FACTS_SILVER (
  `nu_tlfn` STRING,
  `nu_doct` STRING,
  `user_id` STRING,
  `dt_prmr_atcv_lnha` TIMESTAMP,
  `dt_dstv_lnha` TIMESTAMP,
  `ds_prdt` STRING,
  `ds_plno` STRING,
  `UF` STRING,
  `NO_LGRD` STRING,
  `NO_IMOVEL` STRING,
  `NO_BRRO` STRING,
  `MSISDN` STRING,
  `PRODUCTID` STRING,
  `PRODUCTNAME` STRING,
  `SERVICETYPE` STRING,
  `IDATENDIMENTO` STRING,
  `CPF` STRING,
  `DTABERTURA` TIMESTAMP,
  `QTINSISTENCIA` INT,
  `NIVEL` STRING,
  `NRPROTOCOLO` INT,
  `INTERESSE` STRING
  )
USING iceberg;
""")

#---------------------------------------------------
#               CREATE SILVER LAYER BRANCHES
#---------------------------------------------------

print("PRODUCTS SILVER")
try:
    # CREATE TABLE BRANCH: PRODUCTS
    spark.sql("ALTER TABLE SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER CREATE BRANCH ingestion_branch")
    print("PRODUCTS SILVER BRANCH CREATED")
    # MERGE OPERATION: PRODUCTS
    spark.sql("""MERGE INTO SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER.ingestion_branch t
                    USING (SELECT * FROM INCPRODUCTS) s
                    ON t.productid = s.productid
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *""")
    print("MERGE INTO TARGET TABLE COMPLETED")
except Exception as e:
    print("PRODUCTS SILVER IS EMPTY - BRANCH CANNOT BE CREATED")
    print('\n')
    print(f'caught {type(e)}: e')
    print(e)
    print("PERFORMING MERGE INTO TARGET TABLE INSTEAD")
    print('\n')

    # MERGE OPERATION: PRODUCTS
    spark.sql("""MERGE INTO SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER t
                    USING (SELECT * FROM INCPRODUCTS) s
                    ON t.productid = s.productid
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *""")


print("FACTS SILVER")
try:
    # CREATE TABLE BRANCH: PRODUCTS
    spark.sql("ALTER TABLE SPARK_CATALOG.DEFAULT.FACTS_SILVER CREATE BRANCH ingestion_branch")
    print("PRODUCTS SILVER BRANCH CREATED")
    # MERGE OPERATION: PRODUCTS
    spark.sql("""MERGE INTO SPARK_CATALOG.DEFAULT.FACTS_SILVER.ingestion_branch t
                    USING (SELECT * FROM INCFACTS) s
                    ON t.nu_tlfn = s.nu_tlfn
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *""")
    print("MERGE INTO TARGET TABLE COMPLETED")
except Exception as e:
    print("PRODUCTS SILVER IS EMPTY - BRANCH CANNOT BE CREATED")
    print('\n')
    print(f'caught {type(e)}: e')
    print(e)
    print("PERFORMING MERGE INTO TARGET TABLE INSTEAD")
    print('\n')
    # MERGE OPERATION: PRODUCTS
    spark.sql("""MERGE INTO SPARK_CATALOG.DEFAULT.FACTS_SILVER t
                    USING (SELECT * FROM INCFACTS) s
                    ON t.nu_tlfn = s.nu_tlfn
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *""")


#---------------------------------------------------
#               MERGE TRANSACTIONS WITH HIST
#---------------------------------------------------

# PRODUCTS SILVER

### PRE-MERGE COUNTS BY TRANSACTION TYPE:
spark.sql("""SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER""").show()

### MERGE INGESTION BRANCH INTO MAIN TABLE BRANCH

#The cherrypick_snapshot procedure creates a new snapshot incorporating the changes from another snapshot in a metadata-only operation
#(no new datafiles are created). To run the cherrypick_snapshot procedure you need to provide two parameters:
#the name of the table you’re updating as well as the ID of the snapshot the table should be updated based on.
#This transaction will return the snapshot IDs before and after the cherry-pick operation as source_snapshot_id and current_snapshot_id.
#we will use the cherrypick operation to commit the changes to the table which were staged in the 'ing_branch' branch up until now.

# SHOW PAST BRANCH SNAPSHOT ID'S
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER.refs;").show()

try:
    # SAVE THE SNAPSHOT ID CORRESPONDING TO THE CREATED BRANCH
    productsBranchSnapshotId = spark.sql("SELECT snapshot_id FROM SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER.refs WHERE NAME == 'ingestion_branch';").collect()[0][0]
    # USE THE PROCEDURE TO CHERRY-PICK THE SNAPSHOT
    # THIS IMPLICITLY SETS THE CURRENT TABLE STATE TO THE STATE DEFINED BY THE CHOSEN PRIOR SNAPSHOT ID
    spark.sql("CALL spark_catalog.system.cherrypick_snapshot('SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER',{})".format(productsBranchSnapshotId))
    # VALIDATE THE CHANGES
    # THE TABLE ROW COUNT IN THE CURRENT TABLE STATE REFLECTS THE APPEND OPERATION - IT PREVIOSULY ONLY DID BY SELECTING THE BRANCH
    spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.PRODUCTS_SILVER;").show()
except Exception as e:
    print("PRODUCTS SILVER WAS EMPTY - BRANCH COULD NOT BE CREATED OR MERGED")
    print('\n')
    print(f'caught {type(e)}: e')
    print(e)


# FACTS SILVER

### PRE-MERGE COUNTS BY TRANSACTION TYPE:
spark.sql("""SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.FACTS_SILVER""").show()

# SHOW PAST BRANCH SNAPSHOT ID'S
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.FACTS_SILVER.refs;").show()

try:
    # SAVE THE SNAPSHOT ID CORRESPONDING TO THE CREATED BRANCH
    factsBranchSnapshotId = spark.sql("SELECT snapshot_id FROM SPARK_CATALOG.DEFAULT.FACTS_SILVER.refs WHERE NAME == 'ingestion_branch';").collect()[0][0]
    # USE THE PROCEDURE TO CHERRY-PICK THE SNAPSHOT
    # THIS IMPLICITLY SETS THE CURRENT TABLE STATE TO THE STATE DEFINED BY THE CHOSEN PRIOR SNAPSHOT ID
    spark.sql("CALL spark_catalog.system.cherrypick_snapshot('SPARK_CATALOG.DEFAULT.FACTS_SILVER',{})".format(factsBranchSnapshotId))
    # VALIDATE THE CHANGES
    # THE TABLE ROW COUNT IN THE CURRENT TABLE STATE REFLECTS THE APPEND OPERATION - IT PREVIOSULY ONLY DID BY SELECTING THE BRANCH
    spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.FACTS_SILVER;").show()
except Exception as e:
    print("FACTS SILVER WAS EMPTY - BRANCH COULD BE CREATED OR MERGED")
    print('\n')
    print(f'caught {type(e)}: e')
    print(e)
