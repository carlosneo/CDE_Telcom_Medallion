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
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField

class TelcoDataGen:

    '''Class to Generate Banking Data'''

    def __init__(self, spark):
        self.spark = spark

    def invoicingDataGen(self, shuffle_partitions_requested = 5, partitions_requested = 5, data_rows = 2000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("nu_doct", "string", minValue=74567891195999, maxValue=14567891195999, uniqueValues=2000, step=1)
                    .withColumn("dt_ciclo_rcrg", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("ds_prdt", "string", minValue=1234567891195999, maxValue=9234567891195999)
                    .withColumn("ds_plno", "string", minValue=123456789119599123456789119599123456789119599, maxValue=9234567891195991234567891195991234567891195990)
                    .withColumn("cd_ddd", "int", minValue=1, maxValue=990000, random=True)
                    .withColumn("uf", "string", minValue=1, maxValue=99)
                    .withColumn("no_lgrd", "string", minValue=123456789119599123456789119599123456789119599, maxValue=9234567891195991234567891195991234567891195990)
                    .withColumn("no_imovel", "string", minValue=12345667890, maxValue=92345667890, random=True)
                    .withColumn("no_brro", "string", minValue=12345678911959912345678911959912345945839483948394836789119599, maxValue=923456783232332232323232233291195991234567891195991234567891195990)
                    .withColumn("nu_cep", "string", minValue=1123456789098765432, maxValue=9923456789098765432)
                    .withColumn("no_mnco", "string", minValue=12345678911959912345678911959912345945839483948394836789119599, maxValue=923456783232332232323232233291195991234567891195991234567891195990)
                    .withColumn("cb_ibge_mnco", "string", minValue=1234567, maxValue=9234567, random=True)
                    .withColumn("dt_vncmt", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("vl_ftra", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("vl_rcrg", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("ds_cnl_rcrg", "decimal", minValue=12345678911959912345678911959912345945839483948394836789119599, maxValue=923456783232332232323232233291195991234567891195991234567891195990)
                    .withColumn("dt_pgto", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("user_id", "string", minValue=74567891195999, maxValue=14567891195999)

        df = fakerDataspec.build()
        #df = df.withColumn("uf", df["uf"].cast("string"))

        return df

    def navigationDataGen(self, shuffle_partitions_requested = 1, partitions_requested = 1, data_rows = 100):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("nu_tlfn", "string", minValue=1234567890123, maxValue=9234567890123) #text=FakerTextUS("credit_card_number")
                    .withColumn("ds_ip", "string", minValue=123456789012312345678901231234567890123, maxValue=9234567890123923456789012392345678901239234567890123)
                    .withColumn("cd_imei", "string", minValue=123456789012345, maxValue=923456789012345)
                    .withColumn("cd_cgi", "string", minValue=12345678901234567890, maxValue=99345678901234567890)
                    .withColumn("dt_ini_nvgc", "int", minValue=0, maxValue=669345, random=True)
                    .withColumn("dt_fim_nvgc", "int", minValue=0, maxValue=493843, random=True)
                    .withColumn("ds_host", "string", minValue=123456789012312345678901231234567890123123456789012312345678901231234567890123123456789012312345678901231234567890123, maxValue=923456789012392345678901239234567890123923456789012392345678901239234567890123923456789012392345678901239234567890123923456789012392345678901239234567890123)
                    .withColumn("ds_ptcl", "string", minValue=123456789012312345678901231234567890123123456789012312345678901231234567890123123456789012312345678901231234567890123, maxValue=923456789012392345678901239234567890123923456789012392345678901239234567890123923456789012392345678901239234567890123923456789012392345678901239234567890123)
                    .withColumn("ds_sbpt", "string", minValue=123456789012312345678901231234567890123123456789012312345678901231234567890123123456789012312345678901231234567890123, maxValue=923456789012392345678901239234567890123923456789012392345678901239234567890123923456789012392345678901239234567890123923456789012392345678901239234567890123)
                    .withColumn("qtdd_byte_tfgd", "int", minValue=1, maxValue=300000, random=True)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df


    def atendimentoDataGen(self, shuffle_partitions_requested = 5, partitions_requested = 5, data_rows = 2000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("msisdn", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("cpf", "string", minValue=12345678901234, maxValue=92345678901234)
                    .withColumn("dtabertura", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dtprazofinalinterno", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("qtinsistencia", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("inalarme", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("icanal", "string", minValue=1111, maxValue=9999)
                    .withColumn("dtultimaalteracao", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("nivel", "string", minValue=11111, maxValue=99999)
                    .withColumn("dtprazofinalanatel", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("qthorasprazoatendimento", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("dsobservacao", minValue=12345678901234123456789012341234567890123412345678901234, maxValue=999999999912345678901234123456789012341234567890123412340000)
                    .withColumn("nrprotocolo", "int", minValue=1, maxValue=300000, random=True)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df


    def antennaDataGen(self, shuffle_partitions_requested = 5, partitions_requested = 5, data_rows = 2000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("msisdn", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("tx_uplink", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("rx_downlink", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("qt_volume", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("dt_start_time", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dt_end_time", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("qt_duration", "int", minValue=1, maxValue=300000, random=True)
                    .withColumn("nu_served_imei", "string", minValue=123456789012345, maxValue=923456789012399)
                    .withColumn("cd_cgi", "string", minValue=12345678901234567891, maxValue=99999678901234567891)
                    .withColumn("cd_pais", "string", minValue=11, maxValue=99)
                    .withColumn("cd_area", "string", minValue=11, maxValue=99)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df


    def productSubscriptionDataGen(self, shuffle_partitions_requested = 5, partitions_requested = 5, data_rows = 2000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("nu_tlfn", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("nu_doct", "string", minValue=12345678901234, maxValue=92345678901234)
                    .withColumn("user_id", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("id_prdt", "int", minValue=243, maxValue=493843, random=True)
                    .withColumn("dt_prmr_atcv_lnha", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dt_dstv_lnha", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("ds_prdt", "string", minValue=123456789012345, maxValue=923456789012399)
                    .withColumn("ds_plno", "string", minValue=123456789012345123456789012345123456789012345, maxValue=923456789012399923456789012399923456789012399)
                    .withColumn("id_estd_lnha", "int", minValue=243, maxValue=493843, random=True)
                    .withColumn("cd_ddd", minValue=243, maxValue=493843, random=True)
                    .withColumn("uf", "string", minValue=243, maxValue=493843, random=True)
                    .withColumn("no_lgrd", "string", minValue=123456789012345123456789012345123456789012345, maxValue=923456789012399923456789012399923456789012399)
                    .withColumn("no_imovel", "string", minValue=123456789, maxValue=123456789)
                    .withColumn("no_brro", "string", minValue=123456789012345123456789012345123456789012345, maxValue=923456789012399923456789012399923456789012399)
                    .withColumn("nu_cep", "string", minValue=123456789012345123456789012345123456789012345, maxValue=923456789012399923456789012399923456789012399)
                    .withColumn("no_mnco", "string", minValue=123456789012345123456789012345123456789012345, maxValue=923456789012399923456789012399923456789012399)
                    .withColumn("cd_ibge_mnco", "string", minValue=1234567, maxValue=9999999)
                    .withColumn("id_disp_xdsl", "int", minValue=111, maxValue=9999999)
                    .withColumn("id_disp_fttc", "int", minValue=111, maxValue=9999999)
                    .withColumn("id_disp_ftth", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_plno_dscn", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_debt_autm", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_cnta_onln", "int", minValue=111, maxValue=9999999)
                    .withColumn("fl_plno_ttlr", "int", minValue=111, maxValue=9999999)
                    .withColumn("nu_imei_aprl", "string", minValue=36745678911959993674567891195999, maxValue=9999999999999999974567891197999)
                    .withColumn("ds_modl_orig_aprl", "string", minValue=36745678911959993674567891195999, maxValue=9999999999999999974567891197999, random=True, randomSeed=4)
                    .withColumn("fl_vivo_total", "int", minValue=1, maxValue=3674567891197999, random=True, randomSeed=4)
                    .withColumn("dt_trca_aprl", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    .withColumn("dt_ini_plno", "timestamp", begin="1900-01-01 01:00:00",end="2023-12-31 23:59:00",interval="1 second", random=True)
                    )

        df = fakerDataspec.build()
        df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df


    def interestDataGen(self, shuffle_partitions_requested = 1, partitions_requested = 1, data_rows = 150):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("msisdn", "string", minValue=1234567890123, maxValue=9234567890123) #text=FakerTextUS("credit_card_number")
                    .withColumn("interesse", "string", minValue=1234567890123123456789012312345678901231234567890123, maxValue=9234567890123923456789012392345678901239234567890123)
                    .withColumn("nvl_interesse", "string", minValue=1234567890123, maxValue=9234567890123)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))
        #df = df.dropDuplicates(['credit_card_number', 'credit_card_provider'])

        return df


    def svaSubscriptionDataGen(self, shuffle_partitions_requested = 5, partitions_requested = 5, data_rows = 2000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("msisdn", "string", minValue=1234567890123, maxValue=9234567890123)
                    .withColumn("productid", "string", minValue=1234567890, maxValue=9234567890)
                    .withColumn("datacontratacao", "string", minValue=12345678901231, maxValue=92345678901231)
                    .withColumn("productname", "string", minValue=123456789012311234567890123112345678901231, maxValue=923456789012319234567890123192345678901231)
                    .withColumn("protocol_number", "string", minValue=12345678901123, maxValue=92345679890123)
                    .withColumn("spname", "string", minValue=123456789012311234567890123112345678901231, maxValue=923456789012319234567890123192345678901231)
                    .withColumn("subscribestate", "string", minValue=12345678901231, maxValue=92345678901231)
                    .withColumn("productaccountinggroup", "string", minValue=123456789012311234567890123112345678901231, maxValue=923456789012319234567890123192345678901231)
                    .withColumn("client", "string", minValue=12345, maxValue=23456)
                    .withColumn("servicetype", "string", minValue=12345678901231, maxValue=92345678901231)
                    .withColumn("tplinha", "string", minValue=12345678901231, maxValue=92345678901231)
                    .withColumn("grossvalue", "int", minValue=1, maxValue=9999999999)
                    .withColumn("company", "int", minValue=1, maxValue=9999999999)
                    .withColumn("taxpis", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("taxcofins", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("taxiss", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("discountvalue", "string", minValue=123451111, maxValue=234560000)
                    )

        df = fakerDataspec.build()
        #df = df.withColumn("credit_card_number", df["credit_card_number"].cast("string"))

        return df
