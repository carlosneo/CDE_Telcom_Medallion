#****************************************************************************
# (C) Cloudera, Inc. 2020-2025
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

from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys, random, os, json, random, configparser

## CDE PROPERTIES

def parseProperties():
    """
    Method to parse total number of HOL participants argument
    """
    try:
        print("PARSING JOB ARGUMENTS...")
        maxParticipants = sys.argv[1]
    except Exception as e:
        print("READING JOB ARG UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return maxParticipants


def createSparkSession():
    """
    Method to create an Iceberg Spark Session
    """

    try:
        spark = SparkSession \
            .builder \
            .appName("PURGE TABLES SESSION") \
            .getOrCreate()
    except Exception as e:
        print("LAUNCHING SPARK SESSION UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return spark


def dropTable(spark, tbl):
    """
    Method to drop table for provided user
    """

    try:
        print("DROP TABLE PURGE: {}".format(tbl))
        spark.sql("DROP TABLE IF EXISTS SPARK_CATALOG.TELCO_MEDALLION.{} PURGE".format(tbl))
    except Exception as e:
        print("DROP {} TABLE UNSUCCESSFUL".format(tbl))
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def main():

    maxParticipants = parseProperties()

    spark = createSparkSession()

    for i in range(int(maxParticipants)):
        if i+1 < 10:
            username = "user00" + str(i+1)
        elif i+1 > 9 and i+1 < 99:
            username = "user0" + str(i+1)
        elif i+1 > 99:
            username = "user" + str(i+1)

        print("PROCESSING USER {}...\n".format(username))
        print("\nDROP TABLE ATENDIMENTO_BRONZE")
        dropTable(spark, "ATENDIMENTO_BRONZE")
        print("\nDROP TABLE PRODUCT_SUBSCRIPTION_BRONZE")
        dropTable(spark, "PRODUCT_SUBSCRIPTION_BRONZE")
        print("\nDROP TABLE SVA_SUBSCRIPTION_BRONZE")
        dropTable(spark, "SVA_SUBSCRIPTION_BRONZE")
        print("\nDROP TABLE INTEREST_BRONZE")
        dropTable(spark, "INTEREST_BRONZE")

if __name__ == "__main__":
    main()
