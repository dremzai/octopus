from django.shortcuts import render, HttpResponse
from pyspark.sql import SparkSession, functions
from pyspark import StorageLevel
from octopus_spark import models
from elasticsearch import Elasticsearch
import pandas as pd
import json, copy, requests, hdfs, time , os
from octopus_spark.oct.sparkinit import sparkinit

# 初始化SPARKSESSION
sparkinit()
sparkSession = SparkSession.builder.getOrCreate()

# 文件分区数量
repNum = 32  # 目前环境是16个Executors，这个可以放入配置表中进行配置 @@@@

# ES配置
es_nodes = "hdp5"
es_xpack_sql_url = "http://" + es_nodes +":9200/_xpack/sql?format=json"
es_nos = "3"
es_client = Elasticsearch(hosts=[es_nodes])


# HDFS配置
hdfs_dir = "hdfs://hdp5:9000"
extractData_dir = "/user/hive/warehouse/sdata.db/" # 贴源目录名称
indexData_dir = "/user/hive/warehouse/index.db/" # 指标目录名称
tempData_dir = "ptemp" # 指标目录名称
hdfs_client = hdfs.Client("http://hdp5:50070")

# 注册临时表
def hdfsTabReg():
    print("####注册临时表#### 开始")
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "extr") # 指定任务池
    # 贴源表注册
    hdfs_tables = []
    for root, dir, files in hdfs_client.walk("/" + extractData_dir):
        if len(files) != 0:
            if files[0] == "_SUCCESS":
                tmp = root.split("/")
                print(tmp)
                if len(tmp) == 5:
                    tName = tmp[1] + "_" + tmp[2] + "_" + tmp[3] + "_" + tmp[4]
                    tDir = "/" + tmp[1] + "/" + tmp[2] + "/" + tmp[3] + "/" + tmp[4]
                    readPath = hdfs_dir + tDir + "/*"  # 拼接hdfs路径
                    sparkDF = sparkSession.read.option("pushdown", "true").load(readPath);  # 加载parquet
                    sparkDF.createOrReplaceTempView(tName)
                    print(tName)
                    tName = tmp[1] + "_" + tmp[2] + "_" + tmp[3]
                    tDir = "/" + tmp[1] + "/" + tmp[2] + "/" + tmp[3]
                    readPath = hdfs_dir + tDir + "/*"  # 拼接hdfs路径
                    sparkDF = sparkSession.read.option("pushdown", "true").load(readPath);  # 加载parquet
                    sparkDF.createOrReplaceTempView(tName)
                    print(tName)
                if len(tmp) == 4:
                    tName = tmp[1] + "_" + tmp[2] + "_" + tmp[3]
                    tDir = "/" + tmp[1] + "/" + tmp[2] + "/" + tmp[3]
                    readPath = hdfs_dir + tDir + "/*"  # 拼接hdfs路径
                    sparkDF = sparkSession.read.option("pushdown", "true").load(readPath);  # 加载parquet
                    sparkDF.createOrReplaceTempView(tName)
                    print(tName)
                if tName not in hdfs_tables:
                    hdfs_tables.append(tName)
    # 指标表注册
    hdfs_tables = []
    for root, dir, files in hdfs_client.walk("/" + indexData_dir):
        if len(files) != 0:
            if files[0] == "_SUCCESS":
                tmp = root.split("/")
                tName = tmp[1] + "_" + tmp[2]
                tDir = "/" + tmp[1] + "/" + tmp[2]
                if tName not in hdfs_tables:
                    hdfs_tables.append(tName)
                    readPath = hdfs_dir + tDir + "/*"  # 拼接hdfs路径
                    sparkDF = sparkSession.read.option("pushdown", "true").load(readPath);  # 加载parquet
                    sparkDF.createOrReplaceTempView(tName)
                    print(tName)
    print("####注册临时表#### 结束")
# hdfsTabReg() # 项目启动时，将parquet文件注册为表，供spark查询

# 测试函数
def test(request, aaa):
    data = {
        "query": "SELECT * from ind_depo_0001 order by 1"
    }
    url = "http://localhost:9200/_xpack/sql?format=json"
    header = {"Content-Type": "application/json"}
    response = requests.post(url,headers=header , data=json.dumps(data))
    print(response.json())
    print(response)
    bbb = response.json()
    return render(request, "test.html", {"data": bbb})
# 搜索引擎页面
def search(request):
    inputSql = ""
    if request.method == "POST":
        inputSql = request.POST.get("getsql", None).replace(";", "")
    key_words = inputSql #request.GET.get('q', '')
    response = es_client.search(
        index='ind_depo_0001',
        body={
            "query": {
                    "multi_match": {
                        "query": key_words,
                        "fields": ["CURR", "AREA"]
                    }
            },
            "from": 0,
            "size": 10
            }
    )
    print(response)
    total_nums = response["hits"]["total"]
    hit_list = []
    for hit in response["hits"]["hits"]:
        hit_dict = {}
        hit_dict["score"] = hit["_score"]
        hit_list.append(hit_dict)
    return render(request, "result.html", {"all_hits": hit_list,
                                           "key_words": key_words,
                                           "total_nums": total_nums})

# 数据中刷新维度表
def refreshDefDim(request):
    get_DefDim = models.DefDim.objects.values("dim_cd", "dim_nm", "f_dim_cd", "dim_lv")
    sparkDF = sparkSession.createDataFrame(pd.read_json(json.dumps(list(get_DefDim))))
    sparkDF.persist(storageLevel=StorageLevel.MEMORY_ONLY).collect()
    sparkDF.createOrReplaceTempView("DefDim") # 维度表名称
    return HttpResponse(sparkDF)

# 默认index页返回函数
def index(request):
    extr_list = models.Extr.objects.all()
    return render(request, "index.html", {"data":extr_list})

# 通过前台url获取sql语句，在sparksql中执行后返回json格式数据给前台。
def execUrlSql(request, urlSql):
    dataview = "[" + ','.join(sparkSession.sql(urlSql).toJSON().collect()) + "]"
    # return render(request, "test.html", {"data":dataview})
    return HttpResponse("<h4>执行SQL:</h4></br>"
                        + urlSql + "</br>"
                        + "<h4>返回结果JSON格式:</h4></br>"
                        + dataview)

# 通过前台页面输入python，执行结果
def execInputPythonCmd(request):
    startTm = time.time()
    if request.META.get('HTTP_X_FORWARDED_FOR'):
        ip = request.META['HTTP_X_FORWARDED_FOR']
    else:
        ip = request.META['REMOTE_ADDR']
    fileName = ip + "-" + str(startTm)
    inputCmd = ""
    dataview = []
    if request.method == "POST":
        inputCmd = request.POST.get("getcmd", None)
        wfile = "wfile = open('tmppycmd/" + fileName + "','w')"
        cfile = "wfile.close()"
        inputCmdExec = wfile + "\n" + inputCmd + "\n" + cfile
        exec(inputCmdExec)
        opFile = open("tmppycmd/" + fileName, "r")
        dataview = opFile.readlines()
    # try:
    #     if request.method == "POST":
    #         inputCmd = request.POST.get("getsql", None).replace(";","")
    #         exec(inputCmd)
    # except:
    #     dataview = "查询错误:" + inputCmd
    endTm = time.time()
    costTm = str(round(endTm - startTm, 2))
    return render(request, "pythoncmd.html", {"inputCmd":inputCmd, "data":json.dumps(dataview,ensure_ascii=False)})

# 通过前台页面输入框获取sql语句，在sparksql中执行后返回json格式数据给前台页面
def execInputSql(request):
    print("####request.body:\n", request.body)
    print("####request.POST:\n", request.POST)
    startTm = time.time()
    if request.META.get('HTTP_X_FORWARDED_FOR'):
        ip = request.META['HTTP_X_FORWARDED_FOR']
    else:
        ip = request.META['REMOTE_ADDR']
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "sql")
    dataview = []
    # tmpdataview = ""
    inputSql = ""
    if (request.method == "POST") & ("runsql" in request.POST):
        # inputSql = request.POST.get("getsql", None).replace(";", "")
        inputSql = request.POST.get("getsql", None)
        # print(sparkSession.sql(inputSql).toJSON().collect())
        # dataview = "[" + ','.join(sparkSession.sql(inputSql).toJSON().collect()) + "]"
        for i in inputSql.split(";"):
            # resultDF = sparkSession.sql(i)
            # tmpdataview += ','.join(resultDF.toJSON().collect())
            resultDF = sparkSession.sql(i).toJSON().collect()
        dataview = "[" + ','.join(resultDF) + "]"
        # dataview = "[" + tmpdataview + "]"
    if (request.method == "POST") & ("savesql" in request.POST):
        inputSql = request.POST.get("getsql", None)
        fileName = ip + ""
    # try:
    #     if request.method == "POST":
    #         inputSql = request.POST.get("getsql", None).replace(";","")
    #         dataview = "[" + ','.join(sparkSession.sql(inputSql).toJSON().collect()) + "]"
    # except:
    #     dataview = "查询错误:" + inputSql
    endTm = time.time()
    costTm = str(round(endTm - startTm, 2))
    print("####用时####" + costTm + "s")
    return render(request, "sparksql.html", {"getsql":inputSql, "data":dataview})

# 通过前台页面输入框获取sql语句，在ES中执行后返回json格式数据给前台页面
def execInputESSql(request):
    startTm = time.time()
    # 这里有两种处理方式：1、POST给ES;2、转成dataframe用SparkSql，ES可以支持pushdown
    # 用SparkSql会支持更多的SQL语句，但是需要对SQL进行解析
    dataview = []
    inputSql = ""
    if request.method == "POST":
        inputSql = request.POST.get("getsql", None).replace(";", "")
        data = {
            "query": inputSql
        }
        print(data)
        header = {"Content-Type": "application/json"}
        response = requests.post(es_xpack_sql_url, headers=header , data=json.dumps(data))
        print(response)
        dataview = response.json()
    endTm = time.time()
    costTm = str(round(endTm - startTm, 2))
    print("####用时####" + costTm + "s")
    return render(request, "test.html", {"getsql":inputSql, "data": json.dumps(dataview)})

# 数据抽取函数,可传入限制条件
def extrDataCond(request,extrSystem,extrTable,extrConditon):
    get_SrcDB = models.SrcDB.objects.all().get(src_sys=extrSystem) # 配置库中获取数据源配置
    databaseType = get_SrcDB.src_type.upper() # 获取数据源数据库类型
    # 拼接jdbc连接串
    if databaseType == "ORACLE":
        url_str = "jdbc:oracle:thin:" + get_SrcDB.src_user + "/" + get_SrcDB.src_pwd \
            + "@" + get_SrcDB.src_ip + ":" + get_SrcDB.src_port + ":" + get_SrcDB.src_instance
    elif databaseType == "MYSQL":
        pass
    elif databaseType == "TERADATA":
        pass
    elif databaseType == "SQLSERVER":
        pass
    print("####Table Extracting#### Table Name:" + extrTable + " Condition:" + extrConditon)
    orasql = "(select * from " + extrTable +" where " + extrConditon + ") tab"
    sparkDF = sparkSession.read.jdbc(url_str, orasql)
    sparkDF.createOrReplaceTempView(extrSystem+"_"+extrTable)
    print("####Table Extracted#### Table Name:" + extrTable + " Condition:" + extrConditon)
    return HttpResponse("<h4>抽取数据库类型:</h4></br>"
                        + databaseType
                        + "</br><h4>完成建表:</h4></br>"
                        +extrSystem+"_"+extrTable)

# 数据抽取函数
# Parquet文件可以考虑，获取数据落入HDFS，上层再用SPARK，可以pushdown
def extrData(request,extrSystem, extrTable, extrDate = "30001231"):
    startTm = time.time()
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "extr") # 指定任务池
    get_SrcDB = models.SrcDB.objects.all().get(src_sys=extrSystem.upper()) # 配置库中获取数据源配置
    get_SrcTab = models.Extr.objects.all()\
        .get(src_sys=extrSystem.upper(), src_tab_nm = extrTable.upper()) # 根据源及表名，取出配置的抽取条件
    if extrDate == "30001231":
        extrDataDt = get_SrcTab.extr_data_dt  # 数据日期
    else:
        extrDataDt = extrDate
    extrDataDt10 = extrDataDt[:4] + "-" + extrDataDt[4:6] + "-" + extrDataDt[6:8]
    parquetDir = hdfs_dir + extractData_dir + extrSystem.lower() + "_" + extrTable.lower() + "/ROW_DATE=" + extrDataDt10  # 拼接hdfs路径
    partitionDir = extractData_dir + extrSystem.lower() + "_" + extrTable.lower() + "/ROW_DATE=" + extrDataDt10  # 拼接hdfs路径
    tName = "sdata." + extrSystem.lower() + "_" + extrTable.lower()
    # 重复抽取判断
    tDir = []
    tDir.append(partitionDir.upper())
    checkDir = []
    for root, dir, files in hdfs_client.walk(extractData_dir):
        checkDir.append(root.upper())
    # print(tDir)
    # print(checkDir)
    # print(len(list((set(checkDir).union(set(tDir)))^(set(checkDir)^set(tDir)))))
    if len(list((set(checkDir).union(set(tDir)))^(set(checkDir)^set(tDir)))) != 0:
        return HttpResponse("</br><h4>此日期数据已经存在，无需重复抽取！</h4></br>"
                                   # + parquetDir
                                   + "</br><h4>如需重复抽取，请删除分区，执行：</h4></br>"
                                   + "alter table " + tName + " drop partition (ROW_DATE='" + extrDataDt10 + "')"
                                   )
    extrConditon = get_SrcTab.extr_condition\
        .replace("${DATA_DT}",extrDataDt10)\
        .replace("${TAB_DT}", "_" + extrDataDt)  # 抽取条件,替换占位符
    print("####Table Extracting#### Table Name:" + extrTable + " Condition:" + extrConditon)
    databaseType = get_SrcDB.src_type.upper()  # 获取数据源数据库类型
    extrPart = int(get_SrcDB.src_part) # 并行数
    if databaseType == "ORACLE":
        # 依据数据源不同的数据库，拼接JDBC连接串
        url_str = "jdbc:oracle:thin:" + get_SrcDB.src_user + "/" + get_SrcDB.src_pwd \
            + "@" + get_SrcDB.src_ip + ":" + get_SrcDB.src_port + ":" + get_SrcDB.src_instance
        pushsql = "(select tmp.*,'"+ extrDataDt10 +"' as row_date from " + extrTable + " tmp where " + extrConditon + ") tab"  # 拼接SQL语句
        print(pushsql)
        partNum = extrPart # 抽取并行数，JDBC Session数，这个可以考虑放入配置表中，根据数据源获取@@@@
        partList = []
        # 生成分片参数
        for i in range(0, partNum):
            # 注意Oracle这样分片会存在最大上限为64，26*2 + 2，a~Z + /
            # 理想情况是大于等于重分区数量，这样就可以避免shuffle
            partList.append("mod(ascii(substr(rowid,-1))," + str(partNum) + ") = " + str(i))
        sparkDF = sparkSession.read.option("fetchsize", 10000).jdbc(url_str, pushsql, predicates = partList)
            # .option("fetchsize", 500000)\
        descTab = sparkDF.dtypes
        cTabSql = "CREATE TABLE IF NOT EXISTS " + tName + " ("
        for i in descTab:
            if i[0].upper() != "ROW_DATE":
                cTabSql += i[0].upper() + " " + i[1] + ","
        cTabSql = cTabSql[:-1]
        cTabSql += ") PARTITIONED BY (ROW_DATE STRING COMMENT '抽取日期') STORED AS PARQUET"
        print("cTabSql: " + cTabSql)
        sparkSession.sql(cTabSql)
        cPartSql = "ALTER TABLE " + tName + " ADD PARTITION (ROW_DATE='" + extrDataDt10 + "')"
        print("cPartSql:" + cPartSql)
        sparkSession.sql(cPartSql)
        # 将字段名称统一转成大写
        for rc in sparkDF.columns:
            sparkDF = sparkDF.withColumnRenamed(rc,rc.upper())
    elif databaseType == "MYSQL":
        pass
    elif databaseType == "TERADATA":
        url_str = "jdbc:teradata://" + get_SrcDB.src_ip + "/" \
                  + "CLIENT_CHARSET=EUC_CN,TMODE=TERA,CHARSET=ASCII,LOB_SUPPORT=off" \
                  + ",user=apppub_qry,password=apppub_qry"
        partNum = extrPart  # 抽取并行数
        pushsql = "(SELECT tmp.*,'"+ extrDataDt10 +"' as row_date from " \
                  + extrSystem.upper() + "." + extrTable + " tmp where " + extrConditon + ") tab"  # 拼接SQL语句
        print(pushsql)
        partList = []
        # 生成分片参数
        for i in range(0, partNum):
            partList.append("OCTPARTNUM - OCTPARTNUM/" + str(partNum) + "*" + str(partNum) + "=" + str(i))
        sparkDF = sparkSession.read.option("driver","com.teradata.jdbc.TeraDriver")\
            .option("fetchsize", 10000) \
            .jdbc(url_str, pushsql)
        descTab = sparkDF.dtypes
        cTabSql = "CREATE TABLE IF NOT EXISTS " + tName + " ("
        for i in descTab:
            if i[0].upper() != "ROW_DATE":
                cTabSql += i[0].upper() + " " + i[1] + ","
        cTabSql = cTabSql[:-1]
        cTabSql += ") PARTITIONED BY (ROW_DATE STRING COMMENT '抽取日期') STORED AS PARQUET"
        print("cTabSql: " + cTabSql)
        sparkSession.sql(cTabSql)
        cPartSql = "ALTER TABLE " + tName + " ADD PARTITION (ROW_DATE='" + extrDataDt10 + "')"
        print("cPartSql:" + cPartSql)
        sparkSession.sql(cPartSql)
        # 将字段名称统一转成大写
        for rc in sparkDF.columns:
            sparkDF = sparkDF.withColumnRenamed(rc,rc.upper())
    elif databaseType == "SQLSERVER":
        pass
    # coalesce可用于合并分区，这里做重分区处理，如果抽取的并行数大于spark默认的并行数，可以用coalesce，避免shuffle。
    if repNum > partNum:
        sparkDF.repartition(repNum).write.parquet(parquetDir, "overwrite");
    else:
        sparkDF.coalesce(repNum).write.parquet(parquetDir, "overwrite");
    extrCnt = str(sparkDF.count())  # 仅做展现，可放入日志中 @@@@
    sparkDF.unpersist() # 释放掉DF，阻断DAG
    print("####Table Extracted#### Table Name:" + extrTable + " Condition:" + extrConditon)
    sparkSession.catalog.refreshTable(tName)
    endTm = time.time()
    costTm = str(round(endTm - startTm, 2))
    print("####用时####" + costTm + "s")
    get_SrcTab.extr_cnt = extrCnt
    get_SrcTab.extr_data_dt = extrDataDt
    get_SrcTab.save()
    return HttpResponse("<h4>抽取数据库类型:</h4></br>"
                        + databaseType
                        + "</br><h4>落地表名称:</h4></br>"
                        + tName
                        + "</br><h4>记录条数:</h4></br>"
                        + extrCnt
                        + "</br><h4>并行数量:</h4></br>"
                        + str(partNum)
                        + "</br><h4>HDFS路径:</h4></br>"
                        + parquetDir
                        + "</br><h4>耗时:</h4></br>"
                        + costTm)

        #.option("oracle.jdbc.ReadTimeout", 6000000)\
        #.option("oracle.net.CONNECT_TIMEOUT", 600000)\
# 指标加工:
def indProc(request, indNo ,indDate = "30001231"):
    startTm = time.time()
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "proc") # 指定任务池
    # 取出已配置用于加工的SQL语句，按照层级进行排序
    getProcind = models.DefIndCalc.objects.all().filter(ind_no=indNo.upper()).order_by("calc_lvl")
    execSql = getProcind.values("ind_proc", "tmp_tab")
    # 取出指标定义中，是否聚合标志
    getAggFlag = models.DefInd.objects.all().filter(ind_no=indNo.upper())
    AggFlag = getAggFlag.values("ind_prop").first()["ind_prop"]
    if AggFlag == "+":
        AggFlagPrt = "Auto Aggregate"
    else:
        AggFlagPrt = "Manual Aggregate"
    print("####Index Processing#### Index No:" + indNo.upper() + " Aggregate Type:" + AggFlagPrt)
    getDataDt = models.DefInd.objects.all().filter(ind_no=indNo.upper())
    if indDate == "30001231":
        indexDataDt = getAggFlag.values("ind_data_dt").first()["ind_data_dt"]
    else:
        indexDataDt = indDate
    # 重复加工判断
    """
    tDir = []
    tDir.append("/" + indexData_dir + "/" + indNo.lower() + "/" + indexDataDt)
    checkDir = []
    for root, dir, files in hdfs_client.walk("/" + indexData_dir):
        checkDir.append(root)
    if len(list((set(checkDir).union(set(tDir))) ^ (set(checkDir) ^ set(tDir)))) != 0:
        return HttpResponse("</br><h4>此日期指标已经存在，无需重复加工！</h4></br>"
                            + tDir[0]
                            + "</br><h4>如需重复加工，删除该日期文件</h4></br>"
                            )
    """
    indexDataDt10 = indexDataDt[:4] + "-" + indexDataDt[4:6] + "-" + indexDataDt[6:8]
    partitionDir = indexData_dir + indNo.lower() + "/index_date=" + indexDataDt10  # 拼接hdfs路径
    tDir = []
    tDir.append(partitionDir.upper())
    checkDir = []
    for root, dir, files in hdfs_client.walk(indexData_dir):
        checkDir.append(root.upper())
    print(tDir)
    print(checkDir)
    print(len(list((set(checkDir).union(set(tDir)))^(set(checkDir)^set(tDir)))))
    if len(list((set(checkDir).union(set(tDir)))^(set(checkDir)^set(tDir)))) != 0:
        return HttpResponse("</br><h4>此日期指标已经存在，无需重复加工！</h4></br>"
                            # + tDir[0]
                            + "</br><h4>如需重复加工，删除该日期文件</h4></br>"
                            + "alter table index." + indNo.lower() + " drop partition (INDEX_DATE='" + indexDataDt10 + "')"
                            )
    # 逐层执行SQL语句，建立临时表，临时表在内存中，考虑：1是否在内存保存，2应该改成动态的DF名称，最终表unpersist@@@@
    lpCnt = 0
    tNameList = []
    for i in execSql:
        lpCnt += 1
        nameDF = "spark" + str(lpCnt)
        execSql = i["ind_proc"]\
            .replace("${DATA_DT}", indexDataDt10) \
            .replace("${TAB_DT}", "_" + indexDataDt)
        print(execSql)
        locals()[nameDF] = sparkSession.sql(execSql)
        locals()[nameDF].persist(storageLevel=StorageLevel.MEMORY_AND_DISK)  #需要再想一想！
        tName = i["tmp_tab"]
        locals()[nameDF].createOrReplaceTempView(tName)  #过程表是否需要保留？再想想
        locals()[nameDF].count() # action操作
        tNameList.append(i["tmp_tab"])
        print("####Index Processing#### Index No:" + indNo.upper() + " execSql loop " + str(lpCnt))
    print("####Index Processing#### Index No:" + indNo.upper() + " exit execSql loop")
    # parquet写入
    # parquetDir = hdfs_dir + "/" + indexData_dir + "/" + tName.lower() + "/" + indexDataDt # hdfs路径
    # locals()[nameDF].write.parquet(parquetDir,"overwrite"); # 写入parquet，直接覆盖
    locals()[nameDF].write.saveAsTable("index." + tName,format="parquet",mode="append",partitionBy="index_date")
    # 删表操作
    for tN in tNameList:
        # dropSql = "drop table " + tN
        # sparkSession.sql(dropSql)
        sparkSession.catalog.dropTempView(tN) # the view will also be uncached
        print("####Index Processing#### Index No:" + indNo.upper() + " drop tables " + tN)
    print("####Index Processing#### Index No:" + indNo.upper() + " exit tables drop")
    # 从内存中删除
    # for j in range(1,lpCnt+1):
    #     nameDF = "spark" + str(j)
    #     locals()[nameDF].unpersist()
    #     print("####Index Processing#### Index No:" + indNo.upper() + " unpersist dataframes " + nameDF)
    # print("####Index Processing#### Index No:" + indNo.upper() + " exit unpersist dataframes")
    # print("####Index Processing#### Index No:" + indNo.upper() + " exit execSql loop")
    # 内存？需要想一想@@@@
    parquetDir = hdfs_dir + indexData_dir + tName.lower() + "/index_date=" + indexDataDt10  # 拼接hdfs路径
    sparkDF = sparkSession.read.option("pushdown","true").load(parquetDir); # 已pushdown的形式读取文件
    # 维度是否聚合进行判断
    if AggFlag != "+": # 非聚合
        print("####Index Processed#### Index No:" + indNo.upper() + " Aggregate Type:" + AggFlagPrt)
    if AggFlag == "+" : # 聚合
        # 取出维度表
        get_DefDim = models.DefDim.objects.values("dim_no", "dim_cd", "dim_nm", "f_dim_cd", "dim_lv")
        # 维度表转成dataframe，放入内存中
        dimDF = sparkSession.createDataFrame(pd.read_json(json.dumps(list(get_DefDim)))) \
            .persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        dimDFCnt = str(dimDF.count()) # action
        print("####start loop#### dimDF count:" + dimDFCnt)
        listCol = sparkDF.columns # 获取DF列名称
        #  循环DF的列
        for i in listCol:
            #  判断列是否需要汇总，此处仅根据维度表配置进行判断，需要修改成指标维度关系表判断@@@@
            if dimDF.filter(dimDF.dim_no == i).filter(dimDF.f_dim_cd == "+").count() > 0 :
                # 进入维度层级循环，从最大层级至最小层级依次关联
                for j in range(int(dimDF.filter(dimDF.dim_no == i).agg(functions.max("dim_lv").alias("mlv"))
                                           .toJSON().first().split(":")[1].split("}")[0]), 0, -1):
                    # 动态命名中间过程dataframe名称
                    nameDF = "spark" + str(i) + str(j)
                    # 将文件中数据加载至dataframe中，注意每次循环该文件内容会发生变化
                    locals()[nameDF] = sparkSession.read.option("pushdown","true").load(parquetDir);
                    print("####Index Processing#### Index No:" + indNo.upper()
                          + " Aggregate Type:" + AggFlagPrt + " Stage dim:" + str(i) + " lvl:" + str(j))
                    # 复制列名称的list
                    listColNew = copy.deepcopy(listCol)
                    listColNew[listCol.index(i)] = "f_dim_cd" # 替换列名称，用于关联使用
                    # 建立维度表临时dataframe，替换列名称，用于关联使用
                    tempDF = dimDF.filter(dimDF.dim_no == i).filter(dimDF.dim_lv == j).withColumnRenamed("dim_cd", i)
                    # 与维度临时表进行内连接操作，仅取出该层级下维度的数据
                    unionDF = locals()[nameDF].join(tempDF, i).select(listColNew).withColumnRenamed("f_dim_cd", i)
                    # 将关联后的数据进行聚合操作
                    unionDF = unionDF\
                        .groupBy(listCol[0: len(listCol) - 1 ])\
                        .agg(functions.sum(listCol[-1]).alias(listCol[-1]))
                    # 将处理后的数据加入parquet文件中，注意append方式
                    unionDF.write.parquet(parquetDir,"append");
                    unionDF.unpersist() # 释放dataframe，阻断DAG
                    # print("#######################################")
        print("####exit loop####")
        dimDF.unpersist()
    sparkDF.unpersist() # 将SQL阶段的dataframe释放掉
    # 从文件中读取数据，转换为dataframe
    # readPath = hdfs_dir + "/" + indexData_dir + "/" + tName.lower() + "/*"  # 拼接hdfs路径
    # sparkDF = sparkSession.read.option("pushdown","true").load(readPath);
    # 将字段名称统一转成小写
    # for rc in sparkDF.columns:
    #     sparkDF = sparkDF.withColumnRenamed(rc,rc.lower())
    # sparkDF.createOrReplaceTempView(indexData_dir + "_" + tName) # 注册表
    """
    # 将结果数据写入ES
    esIndexName = tName.lower() # index名称，表名转小写
    sparkDFType = sparkDF.dtypes # 获取dataframe的数据字典
    transDict = {"timestamp":"keyword","string":"keyword","decimal":"double"} # 字段类型转换字典
    transStr = lambda x: '"' + x[0] + '":{' + '"type":"' + transDict[x[1].split("(")[0]] + '"}' # 类型转换拼接函数
    # 拼接mapping字符串，json格式
    mapStr = ""
    for m in sparkDFType:
        mapStr += transStr(m) + ","
    mapStr = mapStr[:-1]
    mapStr = '{"properties":{' + mapStr.lower() + '}}'
    header = {"Content-Type": "application/json"} # http头定义
    # print(esIndexName)
    print("http://" + es_nodes +":9200/" + esIndexName)
    requests.delete("http://" + es_nodes +":9200/" + esIndexName, headers=header) # 删除index
    # requests.put("http://" + es_nodes +":9200/" + esIndexName , headers=header) # 新建index
    setStr = '{"settings":{"number_of_replicas":"0","number_of_shards":"' + es_nos + '"}}' # 定义副本数量为0
    requests.put("http://" + es_nodes +":9200/" + esIndexName
                 , headers=header, data=setStr) # 配置index setting
    # print(mapStr)
    requests.post("http://" + es_nodes +":9200/" + esIndexName + "/" + esIndexName + "/_mapping"
                  , headers=header, data=mapStr) # 新建type，定义mapping
    # setStr = '{"index.refresh_interval":"120"}' # 定义刷新时间
    # requests.put("http://" + es_nodes +":9200/" + esIndexName + "/" + esIndexName +"/_settings"
    #              , headers=header, data=setStr) # 配置type setting
    # 转化字段类型，用于ES读取数据
    for L in sparkDF.dtypes:
        if (L[1].find("decimal") == 0):
            sparkDF = sparkDF.withColumn(L[0], sparkDF[L[0]].cast("Double"))
        if (L[1].find("timestamp") == 0):
            sparkDF = sparkDF.withColumn(L[0], sparkDF[L[0]].cast("string"))
    # 写入ES，注意这里index名称和type名称是一致的，并行处理时需要考虑拆成不同的type @@@@
    sparkDF.write.mode("append")\
        .format("org.elasticsearch.spark.sql")\
        .option("es.resource", esIndexName + "/" + esIndexName)\
        .option("es.nodes", es_nodes)\
        .save()
    # 重新设置副本数量为 1
    setStr = '{"number_of_replicas":"1"}'
    requests.put("http://" + es_nodes +":9200/" + esIndexName +"/_settings", headers=header, data=setStr)
    print("####Index Processed#### Index No:" + indNo.upper() + " Aggregate Type:" + AggFlagPrt)
    """
    """
    # 循环写入不同的type中
    print("####开始写入ES####")
    totalCnt = sparkDF.count()
    print("####写入ES记录总数#### " + str(totalCnt))
    splitNum = 4000000
    if round(totalCnt / splitNum) % 2 == 0:
        totalSp = round(totalCnt / splitNum) + 1
    else:
        totalSp = round(totalCnt / splitNum) + 0
    print("####写入ES分片总数#### " + str(totalSp))
    for esSp in range(1, totalSp + 1):
        print("####写入ES写入进度#### " + str(esSp) + "/" + str(totalSp))
        esIndexNameSp = esIndexName + "_" + str(esSp)
        requests.delete("http://" + es_nodes + ":9200/" + esIndexNameSp, headers=header)  # 删除index
        requests.put("http://" + es_nodes + ":9200/" + esIndexNameSp, headers=header)  # 新建index
        setStr = '{"number_of_replicas":"0"}'  # 定义副本数量为0
        requests.put("http://" + es_nodes +":9200/" + esIndexNameSp +"/_settings"
                     , headers=header, data=setStr) # 配置index setting
        requests.post("http://" + es_nodes +":9200/" + esIndexNameSp + "/" + esIndexNameSp + "/_mapping"
                      , headers=header, data=mapStr) # 新建type，定义mapping
        setStr = '{"index.refresh_interval":"120"}'  # 定义刷新时间
        requests.put("http://" + es_nodes +":9200/" + esIndexNameSp + "/" + esIndexNameSp +"/_settings"
                     , headers=header, data=setStr) # 配置type setting
        wtDF = "wtDF" + str(esSp)
        locals()[wtDF] = sparkDF.limit(splitNum)
        locals()[wtDF].repartition(32).write.mode("append").format("org.elasticsearch.spark.sql")\
            .option("es.resource", esIndexNameSp + "/" + esIndexNameSp)\
            .option("es.nodes", es_nodes)\
            .save()
        sparkDF = sparkDF.subtract(locals()[wtDF])
        locals()[wtDF].unpersist()
        writeESCnt = str(requests.get("http://" + es_nodes +":9200/" + esIndexName + "*/_count"
                                      , headers=header).json()['count'])
        print("####完成写入ES记录数：" + writeESCnt)
    """
    endTm = time.time()
    costTm = str(round(endTm - startTm, 2))
    print("####用时####" + costTm + "s")
    # return HttpResponse(execSql)
    return HttpResponse("</br><h4>指标加工完成，耗时：</h4></br>"
                        + costTm
                        )
"""
def indProc(request,indNo):
    header = {"Content-Type": "application/json"}
    #  末层数据通过SQL的方式实现逻辑，最终结果在dataframe里
    get_Procind = models.DefIndCalc.objects.all().filter(ind_no=indNo.upper()).order_by("calc_lvl")
    execSql = get_Procind.values("ind_proc", "tmp_tab")
    get_AggFlag = models.DefInd.objects.all().filter(ind_no=indNo.upper())
    AggFlag = get_AggFlag.values("ind_prop").first()["ind_prop"]
    if AggFlag == "+":
        AggFlagPrt = "Auto Aggregate"
    else:
        AggFlagPrt = "Manual Aggregate"
    print("####Index Processing#### Index No:" + indNo.upper() + " Aggregate Type:" + AggFlagPrt)
    for i in execSql:
        sparkDF = sparkSession.sql(i["ind_proc"])
        sparkDF.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)  #需要再想一想！
        tabNm = i["tmp_tab"]
        sparkDF.createOrReplaceTempView(tabNm)  #过程表是否需要保留？再想想
        sparkDF.count()
    if AggFlag != "+":
        print("####Index Processed#### Index No:" + indNo.upper() + " Aggregate Type:" + AggFlagPrt)
    if AggFlag == "+" :
        listCol = sparkDF.columns
        #  读取维度表，实现父节点的汇总处理
        get_DefDim = models.DefDim.objects.values("dim_no", "dim_cd", "dim_nm", "f_dim_cd", "dim_lv")
        dimDF = sparkSession.createDataFrame(pd.read_json(json.dumps(list(get_DefDim)))) \
            .persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        print("####start loop#### dimDF count:" + str(dimDF.count()))
        listColNew = copy.deepcopy(listCol)
        listJoinCol = []
        listAggCol = []
        #  外层循环遍历列
        for i in listCol:
            #  取出需要汇总的维度
            if dimDF.filter(dimDF.dim_no == i).filter(dimDF.f_dim_cd == "+").count() > 0 :
                listJoinCol.append(str(i))
                loopCnt = 0
                listLoopCol = []
                listLoopCol.append(str(i))
                for j in range(int(dimDF.filter(dimDF.dim_no == i).agg(functions.max("dim_lv").alias("mlv"))
                                           .toJSON().first().split(":")[1].split("}")[0]), 0, -1):
                    print("####Index Processing#### Index No:" + indNo.upper()
                          + " Aggregate Type:" + AggFlagPrt + " Stage dim:" + str(i) + " lvl:" + str(j))
                    newCol = str(i) + "_lvl_" + str(j)
                    listLoopCol.append(newCol)
                    if loopCnt  == 0:
                        joinCol = i
                    else:
                        joinCol = lastCol
                    tempDF = dimDF.filter(dimDF.dim_no == i).filter(dimDF.dim_lv == j)\
                        .withColumnRenamed("dim_cd", joinCol)\
                        .withColumnRenamed("f_dim_cd", newCol)
                    if loopCnt == 0:
                        joinDF = tempDF.select(listLoopCol)
                    else:
                        joinDF = joinDF.join(tempDF, joinCol).select(listLoopCol)
                    lastCol = newCol
                    loopCnt += 1
                    listLpJoinCol = joinDF.columns
                cnt = 1
                for k in listLpJoinCol[1:]:
                    listColNew.insert(listColNew.index(i) + cnt, k)
                    cnt += 1    
                listAggCol.append(listLpJoinCol)
                sparkDF = sparkDF.join(joinDF, i, "left").select(listColNew)
            else:
                listAggCol.append(str(i).split())         
        for rc in sparkDF.columns:
            sparkDF = sparkDF.withColumnRenamed(rc,rc.lower())
        sparkDF.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        sparkDF.createOrReplaceTempView(tabNm)
        sparkDF.count()
        # 保留至ES 
        esIndexName = tabNm.lower() 
        header = {"Content-Type": "application/json"}
        requests.delete("http://" + es_nodes +":9200/" + esIndexName, headers=header)
        requests.put("http://" + es_nodes +":9200/" + esIndexName, headers=header)
        #transDict = {"timestamp":"date","string":"keyword","decimal":"double"}
        transDict = {"timestamp":"keyword","string":"keyword","decimal":"double"}
        sparkDFType = sparkDF.dtypes
        transStr = lambda x: '"' + x[0] + '":{' + '"type":"' + transDict[x[1].split("(")[0]] + '"}'
        mapStr = ""
        for m in sparkDFType:
            mapStr += transStr(m) + ","
            #if m[1] == "timestamp":
            #    mapStr = mapStr[:-2] + ',"format":"yyyy-MM-dd HH:mm:ss"},'
        mapStr = mapStr[:-1]
        mapStr = '{"properties":{' + mapStr + '}}'
        requests.post("http://" + es_nodes +":9200/" + esIndexName + "/" + esIndexName + "/_mapping", headers=header, data=mapStr)
        print("####DATA####" + mapStr )
        print("####POST####" + "http://" + es_nodes +":9200/" + esIndexName + "/" + esIndexName + "/_mapping")
        for L in sparkDF.dtypes:
            if (L[1].find("decimal") == 0):
                sparkDF = sparkDF.withColumn(L[0], sparkDF[L[0]].cast("Double")) 
            if (L[1].find("timestamp") == 0):
                sparkDF = sparkDF.withColumn(L[0], sparkDF[L[0]].cast("string")) 
        sparkDF.write.mode("append").format("org.elasticsearch.spark.sql")\
            .option("es.resource", esIndexName + "/" + esIndexName)\
            .option("es.nodes", es_nodes) \
            .save()
    print("####Index Processed#### Index No:" + indNo.upper() + " Aggregate Type:" + AggFlagPrt)
    writeESCnt = str(requests.get("http://" + es_nodes +":9200/" + esIndexName + "/_count", headers=header).json()['count'])
    print("####完成写入ES记录数：" + writeESCnt)
    return HttpResponse(execSql)
"""

    # append: Append contents of this DataFrame to existing data.
    # overwrite: Overwrite existing data.
    # error or errorifexists: Throw an exception if data already exists.
    # ignore: Silently ignore this operation if data already exists.

    #
# NONE
# 默认配置（不缓存）
# DISK_ONLY
# 数据缓存到磁盘, 特点读写特别慢，内存占用比较少
# DISK_ONLY_2
# 数据缓存到磁盘两份，特点读写比较慢（比DISK_ONLY读写快，稳定性好）
# MEMORY_ONLY
# 数据缓存到内存和cache()
# 功能之一，读写最快但是内存消耗比较大
# MEMORY_ONLY_2
# 数据缓存到内存，并且缓存两份，特点读写速度快内存消耗很大，稳定性比较好，适用于集群不稳定，缓存的数据计算过程比较复杂的情况
# MEMORY_ONLY_SER
# 数据缓存到内存并序列化，一般可以配合kyro一起使用，读写过程需要序列化和反序列化，读写速度比MeMory_only慢，但是数列化后的数据占用内存比较少
# MEMORY_ONLY_SER_2
# 数据序列化后存两份到内存，读写过程同上，特点内存占用量较大，适用于不太稳定的集群
# MEMORY_AND_DISK
# 数据缓存到内存，内存不够溢写到磁盘，一般情况这个使用的比较多一点，是读写性能和数据空间的平衡点
# MEMORY_AND_DISK_2
# 数据缓存两份到内存，内存不够溢写到磁盘，一般情况这个使用的比较多一点，是读写性能和数据空间的平衡点
# MEMORY_AND_DISK_SER
# 数据序列化后缓存到内存，内存不够溢写到磁盘
# MEMORY_AND_DISK_SER_2数据序列化后缓存2份到内存，内存不够溢写到磁盘
#
# OFF_HEAP
# 使用对外内存缓存数据可以配合tachyon一款使用



# def bg_extrData(extrSystem, extrTable):
#     sparkSession = SparkSession.builder.getOrCreate()
#     # 从模型表中读取配置信息
#     get_SrcDB = models.SrcDB.objects.all().get(src_sys=extrSystem.upper())
#     databaseType = get_SrcDB.src_type.upper()
#     if databaseType == "ORACLE":
#         url_str = "jdbc:oracle:thin:" + get_SrcDB.src_user + "/" + get_SrcDB.src_pwd \
#             + "@" + get_SrcDB.src_ip + ":" + get_SrcDB.src_port + ":" + get_SrcDB.src_instance
#     elif databaseType == "MYSQL":
#         pass
#     elif databaseType == "TERADATA":
#         pass
#     elif databaseType == "SQLSERVER":
#         pass
#     get_SrcTab = models.Extr.objects.all().get(src_sys=extrSystem.upper(), src_tab_nm = extrTable.upper())
#     extrConditon = get_SrcTab.extr_condition
#     print("####Table Extracting#### Table Name:" + extrTable + " Condition:" + extrConditon)
#     orasql = "(select * from " + extrTable +" where " + extrConditon + ") tab"
#     sparkDF = sparkSession.read.jdbc(url_str, orasql)
#     sparkDF.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).collect()
#     sparkDF.createOrReplaceTempView(extrSystem + "_" + extrTable)
#     print("####Table Extracted#### Table Name:" + extrTable + " Condition:" + extrConditon)
#


