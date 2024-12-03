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

import os
import numpy as np
import pandas as pd
from datetime import datetime
import dbldatagen as dg
import dbldatagen.distributions as dist
from dbldatagen import FakerTextFactory, DataGenerator, fakerText
from faker.providers import bank, credit_card, currency
from faker import Faker
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField

class TelcoDataGen:

    '''Class to Generate Telco Data'''

    def __init__(self, spark):
        self.spark = spark

    def invoicingDataGen(self, shuffle_partitions_requested = 20, partitions_requested = 20, data_rows = 35000000):

        # setup use of Faker
        fake = Faker()

        # Define a function to generate random plan descriptions
        def generate_plan_description():
            plans = [
                "Plano Basico", 
                "Plano Premium", 
                "Plano Corporativo", 
                "Plano Familia", 
                "Plano Ilimitado", 
                "Plano de Dados", 
                "Plano de Voz", 
                "Plano 3G"
                "Plano 4G", 
                "Plano 5G", 
                "Plano Especial",
                "Plano Fixo",
                "Plano Movel",
                "Plano Empresarial", 
                "Plano 100GB", 
                "Plano 500GB"
            ]
            return fake.random_element(plans)
        
        def generate_ds_prdt():
            g_prdt_contratado = [
                "Pre", 
                "Pos"
            ]
            return fake.random_element(g_prdt_contratado)
        
        def generate_no_lgrd():
            no_lgrd = [
                "Avenida", 
                "Rua",
                "Alameda"
            ]
            return fake.random_element(no_lgrd)

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("nu_doct", "string", minValue=74567891195, maxValue=14567891195999, uniqueValues=35000000, step=1)
                    .withColumn("dt_ciclo_rcrg", "timestamp", begin="2019-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("ds_prdt", "string", value=generate_ds_prdt)
                    .withColumn("ds_plno", "string", value=generate_plan_description)
                    .withColumn("cd_ddd", "int", minValue=1, maxValue=99, random=True)
                    .withColumn("uf", "string", minValue=1, maxValue=99)
                    .withColumn("no_lgrd", "string", value=generate_no_lgrd)
                    .withColumn("no_imovel", "string", minValue=12345667890, maxValue=92345667890, random=True)
                    .withColumn("no_brro", "string", minValue=74567891195, maxValue=14567891195999)
                    .withColumn("nu_cep", "string", minValue=1123456789098765432, maxValue=9923456789098765432)
                    .withColumn("no_mnco", "string", minValue=74567891195, maxValue=14567891195999)
                    .withColumn("cb_ibge_mnco", "string", minValue=1234567, maxValue=9234567, random=True)
                    .withColumn("dt_vncmt", "timestamp", begin="2019-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("vl_ftra", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("vl_rcrg", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("ds_cnl_rcrg", "decimal", minValue=74567891195, maxValue=14567891195999)
                    .withColumn("dt_pgto", "timestamp", begin="2019-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("user_id", "string", minValue=74567891195999, maxValue=14567891195999)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("uf", df["uf"].cast("string"))

        return df

    def navigationDataGen(self, shuffle_partitions_requested = 200, partitions_requested = 2000, data_rows = 470000000000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("nr_tlfn", "string", minValue=1, maxValue=9002341230, step=1) #text=FakerTextUS("credit_card_number")
                    .withColumn("ds_ip", "string", minValue=12345678123, maxValue=9234567890123)
                    .withColumn("cd_imei", "string", minValue=1234567012345, maxValue=9234567890125)
                    .withColumn("cd_cgi", "string", minValue=123456734567890, maxValue=9934567890123456)
                    .withColumn("dt_ini_nvgc", "int", minValue=0, maxValue=669345, random=True)
                    .withColumn("dt_fim_nvgc", "int", minValue=0, maxValue=493843, random=True)
                    .withColumn("ds_host", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("ds_ptcl", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("ds_sbpt", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("qtdd_byte_tfgd", "int", minValue=1, maxValue=300000, random=True)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df

    def atendimentoDataGen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 1500000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("msisdn", "string", minValue=1, maxValue=9234567890123, step=1)
                    .withColumn("cpf", "string", minValue=1234, maxValue=923456234)
                    .withColumn("dtabertura", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dtprazofinalinterno", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("qtinsistencia", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("inalarme", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("icanal", "string", minValue=1111, maxValue=9999)
                    .withColumn("dtultimaalteracao", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("nivel", "string", minValue=11111, maxValue=99999)
                    .withColumn("dtprazofinalanatel", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("qthorasprazoatendimento", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("dsobservacao", minValue=15678, maxValue=999999998)
                    .withColumn("nrprotocolo", "int", minValue=1, maxValue=300000, random=True)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df

    def antennaDataGen(self, shuffle_partitions_requested = 200, partitions_requested = 500, data_rows = 13000000000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("msisdn", "string", minValue=1, maxValue=9234567890123, step=1)
                    .withColumn("tx_uplink", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("rx_downlink", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("qt_volume", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("dt_start_time", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dt_end_time", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("qt_duration", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("nu_served_imei", "string", minValue=12345, maxValue=9234512399)
                    .withColumn("cd_cgi", "string", minValue=123890891, maxValue=99999867891)
                    .withColumn("cd_pais", "string", minValue=11, maxValue=99)
                    .withColumn("cd_area", "string", minValue=11, maxValue=99)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df

    def productSubscriptionDataGen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 2000000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("nu_tlfn", "string", minValue=1, maxValue=9234567890123, step=1)
                    .withColumn("nu_doct", "string", minValue=74567891195, maxValue=14567891195999, uniqueValues=35000000, step=1)
                    .withColumn("user_id", "string", minValue=90123, maxValue=92340123)
                    .withColumn("id_prdt", "int", minValue=1, maxValue=923457890, step=1)
                    .withColumn("dt_prmr_atcv_lnha", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dt_dstv_lnha", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("ds_prdt", "string", minValue=12348905, maxValue=9345612399)
                    .withColumn("ds_plno", "string", minValue=12345678, maxValue=5678909234)
                    .withColumn("id_estd_lnha", "int", minValue=243, maxValue=493843, random=True)
                    .withColumn("cd_ddd", minValue=243, maxValue=493843, random=True)
                    .withColumn("uf", "string", minValue=243, maxValue=493843, random=True)
                    .withColumn("no_lgrd", "string", minValue=1234693, maxValue=923452399)
                    .withColumn("no_imovel", "string", minValue=1234569, maxValue=1234789)
                    .withColumn("no_brro", "string", minValue=12323445, maxValue=12393456)
                    .withColumn("nu_cep", "string", minValue=12347345, maxValue=923456399)
                    .withColumn("no_mnco", "string", minValue=1234567812, maxValue=925672399)
                    .withColumn("cd_ibge_mnco", "string", minValue=1234567, maxValue=9999999)
                    .withColumn("id_disp_xdsl", "int", minValue=111, maxValue=9999999)
                    .withColumn("id_disp_fttc", "int", minValue=111, maxValue=9999999)
                    .withColumn("id_disp_ftth", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_plno_dscn", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_debt_autm", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_cnta_onln", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_plno_ttlr", "int", minValue=111, maxValue=9999999)
                    .withColumn("nu_imei_aprl", "string", minValue=19993, maxValue=99999999)
                    .withColumn("ds_modl_orig_aprl", "string", minValue=36195, maxValue=99999999, random=True, randomSeed=4)
                    .withColumn("fl_vivo_total", "int", minValue=1, maxValue=36197999, random=True, randomSeed=4)
                    .withColumn("dt_trca_aprl", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dt_ini_plno", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df

    def svaSubscriptionDataGen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 1000000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("msisdn", "string", minValue=1, maxValue=9234567890123, step=1)
                    .withColumn("productid", "string", minValue=1, maxValue=923457890, step=1)
                    .withColumn("datacontratacao", "string", minValue=1, maxValue=92571231)
                    .withColumn("productname", "string", minValue=1, maxValue=92347031)
                    .withColumn("protocol_number", "string", minValue=1, maxValue=927989023)
                    .withColumn("spname", "string", minValue=1, maxValue=92345891231)
                    .withColumn("subscribestate", "string", minValue=1, maxValue=92378231)
                    .withColumn("productaccountinggroup", "string", minValue=1, maxValue=92341231)
                    .withColumn("client", "string", minValue=1, maxValue=23456)
                    .withColumn("servicetype", "string", minValue=1, maxValue=9234901231)
                    .withColumn("tplinha", "string", minValue=1, maxValue=9238901231)
                    .withColumn("grossvalue", "int", minValue=1, maxValue=99999999)
                    .withColumn("company", "int", minValue=1, maxValue=99999999)
                    .withColumn("taxpis", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("taxcofins", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("taxiss", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("discountvalue", "string", minValue=1211, maxValue=2345600)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df
