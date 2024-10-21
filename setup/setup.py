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

from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from utils import *
from datetime import datetime
import sys, random, os, json, random, configparser

## CDE PROPERTIES

def parseProperties():
    """
    Method to parse total number of HOL participants argument
    """
    try:
        print("PARSING JOB ARGUMENTS...")
        maxParticipants = sys.argv[1]
        storageLocation = sys.argv[2]
    except Exception as e:
        print("READING JOB ARG UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return maxParticipants, storageLocation


def createSparkSession():
    """
    Method to create an Iceberg Spark Session
    """

    try:
        spark = SparkSession \
            .builder \
            .appName("TELCO LOAD") \
            .getOrCreate()
    except Exception as e:
        print("LAUNCHING SPARK SESSION UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return spark


def createInvoicingData(spark):
    """
    Method to create Invoicing dataframe using the dbldatagen and Faker frameworks
    """

    try:
        print("CREATING DF...\n")
        dg = TelcoDataGen(spark)
        invoiceDf = dg.invoicingDataGen()
    except Exception as e:
        print("CREATING DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return invoiceDf


def createNavigationData(spark):
    """
    Method to create Navigation dataframe using the dbldatagen and Faker frameworks
    """

    try:
        print("CREATING DF...\n")
        dg = TelcoDataGen(spark)
        navigationDf = dg.navigationDataGen()
    except Exception as e:
        print("CREATING DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return navigationDf


def createAtendimentoData(spark):
    """
    Method to create Atendimento dataframe using the dbldatagen and Faker frameworks
    """

    try:
        print("CREATING ATENDIMENTO DF...\n")
        dg = TelcoDataGen(spark)
        atendimentoDf = dg.atendimentoDataGen()
    except Exception as e:
        print("CREATING ATENDIMENTO DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return atendimentoDf


def createAntennaData(spark):
    """
    Method to create Atendimento dataframe using the dbldatagen and Faker frameworks
    """

    try:
        print("CREATING DF...\n")
        dg = TelcoDataGen(spark)
        antennaDf = dg.antennaDataGen()
    except Exception as e:
        print("CREATING DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return antennaDf


def createProductSubscriptionData(spark):
    """
    Method to create ProductSubscription dataframe using the dbldatagen and Faker frameworks
    """

    try:
        print("CREATING DF...\n")
        dg = TelcoDataGen(spark)
        productSubscriptionDf = dg.productSubscriptionDataGen()
    except Exception as e:
        print("CREATING DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return productSubscriptionDf


def createInterestData(spark):
    """
    Method to create interest dataframe using the dbldatagen and Faker frameworks
    """

    try:
        print("CREATING DF...\n")
        dg = TelcoDataGen(spark)
        interestDf = dg.interestDataGen()
    except Exception as e:
        print("CREATING DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return interestDf


def createSvaSubscriptionData(spark):
    """
    Method to create svaSubscription dataframe using the dbldatagen and Faker frameworks
    """

    try:
        print("CREATING DF...\n")
        dg = TelcoDataGen(spark)
        svaSubDf = dg.svaSubscriptionDataGen()
    except Exception as e:
        print("CREATING DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)

    return svaSubDf


def saveInvoiceData(invoiceDf, storageLocation, username):
    """
    Method to save Invoice data to Cloud Storage in parquet format
    """

    print("SAVING INVOICE TO PARQUET IN CLOUD STORAGE...\n")

    try:
        invoiceDf. \
            write. \
            format("parquet"). \
            mode("overwrite"). \
            save("{0}/telco/demo/{1}/invoice".format(storageLocation, username))
    except Exception as e:
        print("SAVING INVOICE DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def saveNavigationData(navigationDf, storageLocation, username):
    """
    Method to save Navigation to Cloud Storage in Parquet format
    """

    print("SAVING NAVIGATION DATA TO PARQUET IN CLOUD STORAGE...\n")

    try:
        navigationDf. \
            write. \
            format("parquet"). \
            mode("overwrite"). \
            save("{0}/telco/demo/{1}/navigation".format(storageLocation, username))
    except Exception as e:
        print("SAVING NAVIGATION DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def saveAtendimentoData(atendimentoDf, storageLocation, username):
    """
    Method to save Atendimento Data to Cloud Storage in Json format
    """

    print("SAVING ATENDIMENTO TO PARQUET IN CLOUD STORAGE...\n")

    try:
        atendimentoDf. \
            write. \
            format("parquet"). \
            mode("overwrite"). \
            save("{0}/telco/demo/{1}/atendimento".format(storageLocation, username))
    except Exception as e:
        print("SAVING ATENDIMENTO UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def saveAntennaData(antennaDf, storageLocation, username):
    """
    Method to save Antenna Data to Cloud Storage
    """

    print("SAVING ANTENNA DF TO PARQUET IN CLOUD STORAGE...\n")

    try:
        antennaDf \
            .write. \
            mode('overwrite') \
            .options(header='True') \
            .parquet("{0}/telco/demo/{1}/antenna".format(storageLocation, username))
    except Exception as e:
        print("SAVING ANTENNA DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def saveProductSubscriptionData(productSubscriptionDf, storageLocation, username):
    """
    Method to save Product Subscription Data to Cloud Storage in csv format
    """

    print("SAVING PRODUCT SUB DF TO PARQUET IN CLOUD STORAGE...\n")

    try:
        productSubscriptionDf \
            .write. \
            mode('overwrite') \
            .options(header='True') \
            .parquet("{0}/telco/demo/{1}/productsubscription".format(storageLocation, username))
    except Exception as e:
        print("SAVING PRODUCT SUBSCRIPTION DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def saveInterestData(interestDf, storageLocation, username):
    """
    Method to save Interest Data to Cloud Storage in parquet format
    """

    print("SAVING INTEREST DF TO PARQUET IN CLOUD STORAGE...\n")

    try:
        interestDf \
            .write. \
            mode('overwrite') \
            .options(header='True') \
            .parquet("{0}/telco/demo/{1}/interest".format(storageLocation, username))
    except Exception as e:
        print("SAVING INTEREST DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def saveSvaSubData(svaSubDf, storageLocation, username):
    """
    Method to save SVA SUB Data to Cloud Storage in parquet format
    """

    print("SAVING SVA SUB DF TO PARQUET IN CLOUD STORAGE...\n")

    try:
        svaSubDf \
            .write. \
            mode('overwrite') \
            .options(header='True') \
            .parquet("{0}/telco/demo/{1}/svasub".format(storageLocation, username))
    except Exception as e:
        print("SAVING SVA SUB DATA UNSUCCESSFUL")
        print('\n')
        print(f'caught {type(e)}: e')
        print(e)


def main():

    maxParticipants, storageLocation = parseProperties()

    spark = createSparkSession()

    for i in range(int(maxParticipants)):
        if i+1 < 10:
            username = "user00" + str(i+1)
        elif i+1 > 9 and i+1 < 99:
            username = "user0" + str(i+1)
        elif i+1 > 99:
            username = "user" + str(i+1)

        print("PROCESSING USER {}...\n".format(username))

        #invoiceDf = createInvoicingData(spark)
        #saveInvoiceData(invoiceDf, storageLocation, username)

        #navigationDf = createNavigationData(spark)
        #saveNavigationData(navigationDf, storageLocation, username)

        atendimentoDf = createAtendimentoData(spark)
        saveAtendimentoData(atendimentoDf, storageLocation, username)

        #antennaDf = createAntennaData(spark)
        #saveAntennaData(antennaDf, storageLocation, username)

        productSubscriptionDf = createProductSubscriptionData(spark)
        saveProductSubscriptionData(productSubscriptionDf, storageLocation, username)

        interestDf = createInterestData(spark)
        saveInterestData(interestDf, storageLocation, username)

        svaSubDf = createSvaSubscriptionData(spark)
        saveSvaSubData(svaSubDf, storageLocation, username)

if __name__ == "__main__":
    main()
