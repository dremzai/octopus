from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
# from pyspark import StorageLevel
# from octopus_spark import models
# import pandas as pd
# import json
"""用于SPARKSESSION初始化"""
def sparkconf():
    pass

def sparkinit():
    ssConf = SparkConf()
    ssConf.setAppName("octopus")
    #ssConf.setMaster('local')
    ssConf.setMaster('spark://hdp5:7077')
    ssConf.set('spark.scheduler.mode', 'FAIR')
    ssConf.set('spark.scheduler.allocation.file', '/home/oct/fairscheduler.xml')
    ssConf.set('spark.executor.memory', '4g')
    ssConf.set('spark.deiver.memory', '8g')
    ssConf.set('spark.cores.max', '48')
    ssConf.set('spark.executor.cores', '3')
    ssConf.set('spark.rdd.compress', 'true')
    ssConf.set('spark.debug.maxToStringFields', '200')
    ssConf.set('spark.sql.shuffle.partitions', '32')
    ssConf.set('spark.default.parallelism', '32')
    ssConf.set('spark.driver.maxResultSize', '10g')
    # ssConf.set('spark.num-executors', '20') # yarn
    # ssConf.set('spark.logConf', True)
    print(ssConf.getAll())
    sparkSession = SparkSession.builder.enableHiveSupport().config(conf=ssConf).getOrCreate()
    print(sparkSession)
    # sparkSession.sql("show tables").show()

    # get_DefDim = models.DefDim.objects.values("dim_cd", "dim_nm", "f_dim_cd", "dim_lv")
    # sparkdf = sparkSession.createDataFrame(pd.read_json(json.dumps(list(get_DefDim))))
    # sparkdf.persist(storageLevel=StorageLevel.MEMORY_ONLY)
    # sparkdf.createOrReplaceTempView("DefDim")



# Spark Properties
# Name	Value
# spark.app.id	local-1530839300558
# spark.app.name	octopus
# spark.cores.max	40
# spark.driver.host	doo-PC
# spark.driver.port	2532
# spark.executor.cores	4
# spark.executor.id	driver
# spark.executor.memory	2g
# spark.logConf	True
# spark.master	local
# spark.rdd.compress	True
# spark.scheduler.mode	FIFO
# spark.serializer.objectStreamReset	100
# spark.submit.deployMode	client
# spark.ui.showConsoleProgress	true
