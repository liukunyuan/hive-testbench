#!/usr/bin/python -u
# -*- coding: utf-8 -*-
import json

import multiprocessing
import os
import time
import commands
import traceback

import datetime
import random
import threading
import socket
import uuid
import subprocess
from multiprocessing import Pool


from random import sample
import sys
reload(sys)
sys.setdefaultencoding('utf8')



def get_ts(dtime):

    return int((time.mktime(dtime.timetuple())+dtime.microsecond/1000000.0))


def exeCmd(cmd,retryTimes=0):
    print(cmd)
    code, output = commands.getstatusoutput(cmd)
    return code,output


def testsql(filepath,sparkSql,filedir):
    try:
        filepath = filepath.replace("\n", "")
        lineArr = filepath.split(".")
        sqlIndex = lineArr[0]
        print(sqlIndex)
        cmd = sparkSql +"liukunyuan_tpc_ds_"+sqlIndex+ " -f " + filedir + filepath
        print(cmd)
        code, output = exeCmd(cmd)
        print(output)
        if (code != 0):
            print("测试失败:" + filepath)
            return
        outputArr = output.split("\n")
        startTime=""
        endTime = ""
        # for line in outputArr:
        #     print(line )
        #     if("Time taken" in line):
        #         lineArr = line.split(" ")
        #         startTime="20"+lineArr[0]+" "+lineArr[1]
        #         break
        # print(startTime)
        # newoutputArr=list(reversed(outputArr))
        # for line in newoutputArr:
        #     print(line )
        #     if("Time taken" in line):
        #         lineArr = line.split(" ")
        #         endTime="20"+lineArr[0]+" "+lineArr[1]
        #         break
        # print(endTime)

        # startTime=datetime.datetime.strptime(startTime, "%Y/%m/%d %H:%M:%S")
        # endTime=datetime.datetime.strptime(endTime, "%Y/%m/%d %H:%M:%S")
        # costTime = get_ts(endTime) - get_ts(startTime)


        costTime=""
        newoutputArr=list(reversed(outputArr))
        for line in newoutputArr:
            print(line )
            if("Time taken" in line):
                costTime=line.split("Time taken: ")[1].split(",")[0]
                break



        print("\nsql:%s,耗时:%s\n"%(filepath,costTime))
    except Exception as e:
        print(e)





def main(sparkVersion,concurrentsize,dbname):
    sparkSql=" "
    if("spark"==sparkVersion):
        sparkSql = "spark-sql  --hivevar DB="+dbname+"  -i settings/init2.sql --queue default  --conf spark.sql.crossJoin.enabled=true  --executor-memory 3G  --conf spark.dynamicAllocation.maxExecutors=5   --name  "
    # else:
        # sparkSql = "/usr/hdp/3.1.4.0-315/spark3/bin/spark-sql   --conf spark.sql.hive.convertMetastoreOrc=false  --conf spark.sql.hive.metastore.version=2.3.9   --hivevar DB="+dbname+"  -i settings/init2.sql  --queue default   --conf spark.sql.crossJoin.enabled=true  --executor-memory 3G  --conf spark.dynamicAllocation.maxExecutors=5   --name  "

    pool = multiprocessing.Pool(int(concurrentsize))

    filedir="spark-queries-tpcds/"
    filearr=os.listdir(filedir)


    for filepath in filearr:

        # testsql(filepath,sparkSql,filedir)
        pool.apply_async(testsql,
                         (filepath,sparkSql,filedir,))

    pool.close()
    pool.join()

if __name__ == "__main__":
    sparkVersion=sys.argv[1]
    concurrentsize=sys.argv[2]
    dbname=sys.argv[3]

    main(sparkVersion,concurrentsize,dbname)

