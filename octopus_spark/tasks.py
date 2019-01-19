import django
django.setup()

import time
from celery.task import task
from octopus_spark import models
from pyspark.sql import SparkSession
from pyspark import StorageLevel

# from octopus_spark.oct.sparkinit import sparkinit
#
# sparkinit()
# print("app_init")
sparkSession = SparkSession.builder.getOrCreate()

@task
def _do_kground_work(name):
    print(name)
    time.sleep(5)

@task
def bg_extrData(extrSystem, extrTable):
    # views.bg_extrData(extrSystem, extrTable)
    # sparkSession = SparkSession.builder.getOrCreate()
    # 从模型表中读取配置信息
    get_SrcDB = models.SrcDB.objects.all().get(src_sys=extrSystem.upper())
    databaseType = get_SrcDB.src_type.upper()
    if databaseType == "ORACLE":
        url_str = "jdbc:oracle:thin:" + get_SrcDB.src_user + "/" + get_SrcDB.src_pwd \
            + "@" + get_SrcDB.src_ip + ":" + get_SrcDB.src_port + ":" + get_SrcDB.src_instance
    elif databaseType == "MYSQL":
        pass
    elif databaseType == "TERADATA":
        pass
    elif databaseType == "SQLSERVER":
        pass
    get_SrcTab = models.Extr.objects.all().get(src_sys=extrSystem.upper(), src_tab_nm = extrTable.upper())
    extrConditon = get_SrcTab.extr_condition
    print("####Table Extracting#### Table Name:" + extrTable + " Condition:" + extrConditon)
    orasql = "(select * from " + extrTable +" where " + extrConditon + ") tab"
    sparkDF = sparkSession.read.jdbc(url_str, orasql)
    sparkDF.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).collect()
    sparkDF.createOrReplaceTempView(extrSystem + "_" + extrTable)
    print("####Table Extracted#### Table Name:" + extrTable + " Condition:" + extrConditon)

