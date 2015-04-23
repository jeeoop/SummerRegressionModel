#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 05 18:50:50 2015

@author: Ken Ouyang
"""
##  =============================================  ##
##  add lib by current file location
import os, sys, subprocess, shlex
from datetime import date,datetime,timedelta
import happybase

prodCode = sys.argv[1]

##  =============================================  ##
##  add UDFs
pyGlobalLib = '/app/' + prodCode + '/lib/pythonUDFs/'
sys.path.append(pyGlobalLib)
from pybase_Func import getPath, printTS, log_N_email
from Oracle_Func import oraclePass, oracleDt

##  =============================================  ##
##  Global Parameters (filepath, logpath, folder):
emailListFile = '/app/' + prodCode + '/lib/txt/DWDS_BIGDATA_maillist.txt'
inputDataPath = '/data/stage/smt_usage/'

with open(emailListFile, 'r') as elist:
    distributionEmail = elist.read().strip()

pyLocalLib = getPath(__file__,-1,'lib')
scriptPath = getPath(__file__,-1,'scripts/python')
logPath = getPath(__file__,-1,'log')
controlPath = getPath(__file__,-1,'control')
tdyDt = date.today()
ydyDtStr = (tdyDt-timedelta(1)).strftime("%Y-%m-%d")


logFile = logPath + 'wf_anomaly_detection_' + ydyDtStr + '_' + printTS('%H%M%S') + '.log'
controlFile = controlPath + 'controlFile.txt'

HDFSDataPath = '/bgdrtl' + prodCode + '/appdata/anomaly_detection/'
tempHbaseFolder = '/bgdrtl' + prodCode + '/appdata/anomaly_detection/_pyMrHbaseLoading'
tempWeatherFolder = '/bgdrtl' + prodCode + '/appdata/anomaly_detection/_temp24_1'
tempWeatherFolder2 = '/bgdrtl' + prodCode + '/appdata/anomaly_detection/_temp24_2'
#hiveLogPath = '/datalake/' + prodCode + '/etl/hive/SMT_Usage/Load_Log/'

##  =============================================  ##
##  /////////////////////////////////////////////  ##
##  =============================================  ##


ydy = date.today()-timedelta(1)
enddt = ydy.strftime('%Y-%m-%d')
startdt = (ydy-timedelta(15)).strftime('%Y-%m-%d')
chYear = str(ydy.year)

##  =============================================  ##
##  distributed computing:
def pProcess(commands):
    import subprocess,shlex
    processes = []
    outErrChk = []
    for comm in commands:
        proc = subprocess.Popen(shlex.split(comm),stdout=subprocess.PIPE)
        processes.append(proc)
    for p in processes:
        stdout_value = p.communicate()[0]
        outErrChk.append(stdout_value)
    return outErrChk

allNodes = ['ittestbat@172.25.10.62','ittestbat@172.25.10.243','ittestbat@172.25.10.245','ittestbat@172.25.10.246','ittestbat@172.25.10.247',\
            'ittestbat@172.25.10.248','ittestbat@172.25.10.249','ittestbat@172.25.10.250','ittestbat@172.25.10.251','ittestbat@172.25.10.252']

##  --------------  ##
commands1 = map(lambda node: 'ssh ' + node + ' mkdir -p /home/ittestbat/_pyParallelTemp_SumReg/', allNodes)
errChk1 = pProcess(commands1)

##  --------------  ##
commands2 = map(lambda node: 'scp /home/ittestbat/temp/get_ESI_list.py ' + node + ':/home/ittestbat/_pyParallelTemp_SumReg/', allNodes)
errChk2 = pProcess(commands2)

##  --------------  ##

ydystring = '2013-07-01'
commands3 = map(lambda node: 'ssh ' + node + ' python /home/ittestbat/_pyParallelTemp_SumReg/get_ESI_list.py ' +' '\
         + ydystring +' '+ str(allNodes.index(node)), allNodes)
errChk3 = pProcess(commands3)

##  --------------  ##
commands4 = map(lambda node: 'ssh ' + node + ' rm -r /home/ittestbat/_pyParallelTemp_SumReg/', allNodes)
errChk4 = pProcess(commands4)



controlP = '1'
commands3 = map(lambda node: 'ssh ' + node + ' python /home/ittestbat/temp/get_ESI_list.py '+ \
            controlP +' '+ ydystring +' '+ str(allNodes.index(node)), allNodes)

errChk3 = pProcess(commands3)


python /home/ittestbat/temp/Extract10Day.v5.py 1 2015-03-27 0

ssh ittestbat@172.25.10.62 mkdir -p /home/ittestbat/_pyParallelTemp_SumReg/
scp /home/ittestbat/temp/get_ESI_list.py ittestbat@172.25.10.62:/home/ittestbat/_pyParallelTemp_SumReg/
ssh ittestbat@172.25.10.62  python /home/ittestbat/_pyParallelTemp_SumReg/get_ESI_list.py 2014-07-01 0




##  --------------  ##
##  --------------  ##
dateString = '2014-10-01'

hivePartitionPath = '/bgdrtlqa/appdata/summer_regression/hv_l_3p_hly/eff_dt=' + dateString + '/'

comm_rm_hiveOut = ['hadoop fs -rm -r -skipTrash ' + hivePartitionPath]
errChk5 = pProcess(comm_rm_hiveOut)
##  --------------  ##
comm_hs_10part = []
for part in range(0,10):
    inputPath = '/bgdrtlqa/appdata/summer_regression/py_s_esi_list/eff_dt=' + dateString + '/part=' + str(part)
    hs_part = "hadoop jar /opt/cloudera/parcels/CDH-5.1.3-1.cdh5.1.3.p0.12/lib/hadoop-mapreduce/hadoop-streaming-2.3.0-cdh5.1.3.jar -D mapred.reduce.tasks=0 -D stream.map.input.field.separator=',' -D stream.map.output.field.separator=',' -D mapred.textoutputformat.separator=',' -input "\
    + inputPath + ' -output ' + hivePartitionPath + 'part=' + str(part) + ' -file /app/qa/HVAC/summer_regression/scripts/python/extract_sumReg_input_mapper.py -mapper  extract_sumReg_input_mapper.py'
    comm_hs_10part.append(hs_part)

errChk6 = pProcess(comm_hs_10part)









