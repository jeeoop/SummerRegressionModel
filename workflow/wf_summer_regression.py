#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 10:46:23 2015

@author: Ken Ouyang
"""
##  =============================================  ##
##  add lib by current file location
import sys, subprocess,shlex
from datetime import date,datetime,timedelta
import happybase

prodCode = sys.argv[1]
ydystring = sys.argv[2]
prodCode2 = 'test'

##  =============================================  ##
##  add UDFs
pyGlobalLib = '/app/'+prodCode+'/lib/pythonUDFs/'
sys.path.append(pyGlobalLib)
from pybase_Func import pProcess,log_N_email, getPath, printTS, dtft

##  =============================================  ##
##  Global Parameters (filepath, logpath, folder):
emailListFile = '/app/'+prodCode+'/lib/txt/DWDS_BIGDATA_maillist.txt'

#with open(emailListFile, 'r') as elist:
#    distributionEmail = elist.read().strip()
distributionEmail = 'yongjian.ouyang@nrg.com;jiayun.zhao@nrg.com'


allNodes = ['it'+prodCode2+'bat@172.25.10.62', 'it'+prodCode2+'bat@172.25.10.243', 'it'+prodCode2+'bat@172.25.10.245',\
            'it'+prodCode2+'bat@172.25.10.246','it'+prodCode2+'bat@172.25.10.247', 'it'+prodCode2+'bat@172.25.10.248',\
            'it'+prodCode2+'bat@172.25.10.249','it'+prodCode2+'bat@172.25.10.250', 'it'+prodCode2+'bat@172.25.10.251',\
            'it'+prodCode2+'bat@172.25.10.252']

pyLocalLib = getPath(__file__,-1,'lib')
hiveScriptPath = getPath(__file__,-1,'scripts/hive')
pyScriptPath = getPath(__file__,-1,'scripts/python')
logPath = getPath(__file__,-1,'log')
controlPath = getPath(__file__,-1,'control')
tdyDt = date.today()
tdyDtStr = tdyDt.strftime("%Y-%m-%d")
ydyDtStr = (tdyDt-timedelta(1)).strftime("%Y-%m-%d")
logFile = logPath + 'wf_summer_regression_' + tdyDtStr + '_' + printTS('%H%M%S') + '.log'

##  =============================================  ##
if __name__ == "__main__":
    error_code = 'n'
    subject_title = 'Summer Regression Job'
    startT = datetime.now()
    ##  -----------------  ##
    ##  check whether data has been updated for the last date of last month.
    MDL_DT = (dtft(ydystring)-timedelta(1)).strftime("%Y-%m-%d")
    pool = happybase.ConnectionPool(size=2, host='hbasemaster')
    with pool.connection() as conn:
        h_smt = conn.table('ETL_ESI_SMT_G4')
        h_tem = conn.table('ETL_WEATHER_INFO_24')
        uCol = 'r:24-' + ''.join(ydystring.split('-')[1:])
        tCol = 'r:tem-' + ''.join(ydystring.split('-')[1:])
        prefixStr = '7-'+ydystring.split('-')[0]
        checkU,checkT = [],[]
        idx,jdx = 0, 0
        hbTime = datetime.now()
        try:
            for uKey, uData in h_smt.scan(row_prefix = prefixStr, columns=[uCol], limit=10):
                print uKey
                if (datetime.now()-hbTime).seconds > 5:
                    break
        except:
            error_code = 'e'
            log_N_email(logFile, 'No usage information update. Job aborted.', 'send', error_code, subject_title, distributionEmail)
        try:
            hbTime = datetime.now()
            for tKey, tData in h_tem.scan(row_prefix = prefixStr, columns=[tCol], limit=10):
                print tKey
                if (datetime.now()-hbTime).seconds > 5:
                    break
        except:
            error_code = 'e'
            log_N_email(logFile, 'No weather information update. Job aborted.', 'send', error_code, subject_title, distributionEmail)    

##  =============================================  ##    
    log_N_email(logFile, 'Starting extracting ESI list from HBase.', 'hold', error_code,'','')
    localTempFolder = '/home/it'+prodCode2+'bat/_pyParallelTemp_sumReg/'
    hive_ESI_list_Path = '/bgdrtl'+prodCode+'/appdata/summer_regression/py_s_esi_list/eff_dt='
    commands1 = map(lambda node: 'ssh ' + node + ' mkdir -p ' + localTempFolder, allNodes)
    output1,errChk1 = pProcess(commands1)
    ##  --------------  ##
    commands2 = map(lambda node: 'scp ' + pyScriptPath+'get_ESI_list.py ' + node + ':' + localTempFolder, allNodes)
    output2,errChk2 = pProcess(commands2)
    ##  --------------  ##
    commands3_0 = ['hadoop fs -rm -r -skipTrash ' + hive_ESI_list_Path + ydystring ]
    output3_0,errChk3_0 = pProcess(commands3_0)

    commands3_1 = map(lambda node: 'ssh ' + node + ' python ' + localTempFolder + 'get_ESI_list.py ' +' '\
         + ydystring +' '+ str(allNodes.index(node)), allNodes)
    output3_1,errChk3_1 = pProcess(commands3_1)
    ##  --------------  ##
    commands4 = map(lambda node: 'ssh ' + node + ' rm -r ' + localTempFolder, allNodes)
    output4,errChk4 = pProcess(commands4)
##  =============================================  ##
##  Get the latest model effective date
##  echo "Get latest model effective date"
    if (1 in errChk1) or (1 in errChk2) or (1 in errChk3_1) or (1 in errChk4):
        error_code = 'e'
        log_N_email(logFile, 'ESI list extraction failed. Job aborted.', 'send', error_code, subject_title, distributionEmail)
    else:
        log_N_email(logFile, 'Starting extracting data from HBase.', 'hold', error_code,'','')
        hivePartitionPath = '/bgdrtlqa/appdata/summer_regression/hv_l_3p_hly/eff_dt=' + ydystring + '/'
        comm_rm_hiveOut = ['hadoop fs -rm -r -skipTrash ' + hivePartitionPath]
        output5, errChk5 = pProcess(comm_rm_hiveOut)
        comm_hs_10part = []
        for part in range(0,10):
            inputPath = '/bgdrtlqa/appdata/summer_regression/py_s_esi_list/eff_dt=' + ydystring + '/part=' + str(part)
            hs_part = "hadoop jar /opt/cloudera/parcels/CDH-5.1.3-1.cdh5.1.3.p0.12/lib/hadoop-mapreduce/hadoop-streaming-2.3.0-cdh5.1.3.jar -D mapred.reduce.tasks=0 -D stream.map.input.field.separator=',' -D stream.map.output.field.separator=',' -D mapred.textoutputformat.separator=',' -input "\
                + inputPath + ' -output ' + hivePartitionPath + 'part=' + str(part) + ' -file  ' + pyScriptPath + 'extract_sumReg_input_mapper.py -mapper  extract_sumReg_input_mapper.py'
            comm_hs_10part.append(hs_part)
        output6, errChk6 = pProcess(comm_hs_10part)
##  =============================================  ##
##  Starting regression
        if 1 in errChk6:
            error_code = 'e'
            log_N_email(logFile, 'Data extraction failed. Job aborted.', 'send', error_code, subject_title, distributionEmail)
        else:
            log_N_email(logFile, 'Starting regression calculation.', 'hold', error_code, subject_title, distributionEmail)        
            RUN_YEAR = ydystring.split('-')[0]
            RUN_MONTH = ydystring.split('-')[1]
            RUN_DATE = ydystring.split('-')[2]
            comm_sumReg_hive = 'hive -f ' + hiveScriptPath \
                + 'wf_3p_hly_lr.hql -hiveconf RUN_YEAR="' + RUN_YEAR \
                + '" -hiveconf RUN_MONTH="' + RUN_MONTH \
                + '" -hiveconf RUN_DATE="' + RUN_DATE \
                + '" -hiveconf REPLICATION="10" -hiveconf CON_LVL="0.90" -hiveconf MODE="' + prodCode + '"'
            proc_sumReg_hive = subprocess.Popen(shlex.split(comm_sumReg_hive),stdout=subprocess.PIPE)
            outp_sumReg_hive = proc_sumReg_hive.communicate()[0]

##  =============================================  ## 
            if proc_sumReg_hive.returncode != 0:
                error_code = 'e'
                log_N_email(logFile, 'Hive Summer-Regression job failed. Job aborted.', 'send', error_code, subject_title, distributionEmail)
            else:
                totalT = datetime.now()-startT
                timepass = round(totalT.seconds/60.0, 2)
                textOut_status_end = 'Job Completed! Total Time Used: '+ str(timepass) + ' minutes.\n'
                log_N_email(logFile, textOut_status_end, 'send', error_code, subject_title, distributionEmail)