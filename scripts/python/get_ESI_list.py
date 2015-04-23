#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 05 18:50:50 2015

@author: Ken Ouyang
"""

import happybase
import sys
from datetime import datetime,timedelta
import csv
import subprocess

def twoD(string):
    if len(string)==1: return '0'+string
    else: return string

def dtft(indt):
    return datetime.strptime(indt, '%Y-%m-%d').date()

##  =====================================  ##
rgNum = sys.argv[2]
ydystring = sys.argv[1]
hr2305 = range(0,5)+range(22,24)

projectName = 'sumReg_get_list'
pool1 = happybase.ConnectionPool(size=3, host='172.25.10.78')
nmRk = ydystring +'_'+ projectName
hivePath = '/bgdrtlqa/appdata/summer_regression/py_s_esi_list/eff_dt='
outputFile = '/home/ittestbat/_pyParallelTemp_sumReg/sumReg_ESI_list_'+ rgNum +'.csv'

cutMonth = dtft(ydystring).month
cutYear = dtft(ydystring).year

##  =====================================  ##
def activeCust(idt,odt,strtDt,endDt):
    deltaDay = 0
    try:
        mvi,mvo,dt1,dt2 = dtft(idt),dtft(odt),dtft(strtDt),dtft(endDt)
        if mvo > dt1 and mvo <= dt2:
            if mvi < dt1:
                deltaDay = (mvo-dt1).days
            else:
                deltaDay = (mvo-mvi).days
        elif mvo > dt2:
            if mvi < dt1:
                deltaDay = (dt2-dt1).days+1
            elif mvi > dt1 and mvi <= dt2:
                deltaDay = (dt2-mvi).days+1
    except:
        pass
    return deltaDay

##  --------------------------------------  ##
def extractFromHbase_1Yr(rgidx):
    idx = 0
    jdx = 0
    with open(outputFile,'wb') as tocsv:
        writer=csv.writer(tocsv, delimiter=',',lineterminator='\n',)
        with pool1.connection() as conn1:
            h_cid = conn1.table('ETL_CID_HIST')
            h_esi = conn1.table('ETL_ESI_HIST')
            statusChk = conn1.table('Parallel_Process_Check_Status')
            for rowKey, rowData in h_cid.scan(row_prefix=rgidx, columns=['i:ESI_ID','i:ACTUAL_MVI_DT','i:ACTUAL_MVO_DT','i:ENRLMNT_STAT_CD','i:CUST_TYPE_CD'], batch_size=200):
                if rowData['i:ENRLMNT_STAT_CD'] in ['01', '05', '06', '24', '95', '96']:
                    if rowData['i:CUST_TYPE_CD'] in ['RS', 'RSLGT', 'RSREG']:
                        delDt = activeCust(rowData['i:ACTUAL_MVI_DT'][:10],rowData['i:ACTUAL_MVO_DT'][:10],startdt,enddt)
                        if delDt > 100:
                            esi = rowData['i:ESI_ID']
                            if len(esi) > 10:
                                esiRk = esi[-6] +'-'+ esi
                                smtRk = esi[-6] +'-'+ chYear +'-'+ esi
                                wthid = h_esi.row(row=esiRk, columns=['i:WTHR_ZONE_ID'])['i:WTHR_ZONE_ID']
                                if wthid != 'null':
                                    fOut = [wthid, smtRk, chCol]
                                    writer.writerow(fOut)
                if jdx == 5000:
                    tColNm = 'Status:' + rgidx
                    statusChk.put(nmRk, {tColNm:str(idx)})
                    print idx
                    jdx = 0
                idx += 1
                jdx += 1
    return idx


##  --------------------------------------  ##
def extractFromHbase_OverYr(rgidx):
    idx = 0
    jdx = 0
    with open(outputFile,'wb') as tocsv:
        writer=csv.writer(tocsv, delimiter=',',lineterminator='\n',)
        with pool1.connection() as conn1:
            h_cid = conn1.table('ETL_CID_HIST')
            h_esi = conn1.table('ETL_ESI_HIST')
            statusChk = conn1.table('Parallel_Process_Check_Status')
            for rowKey, rowData in h_cid.scan(row_prefix=rgidx, columns=['i:ESI_ID','i:ACTUAL_MVI_DT','i:ACTUAL_MVO_DT','i:ENRLMNT_STAT_CD','i:CUST_TYPE_CD'], batch_size=200):
                if rowData['i:ENRLMNT_STAT_CD'] in ['01', '05', '06', '24', '95', '96']:
                    if rowData['i:CUST_TYPE_CD'] in ['RS', 'RSLGT', 'RSREG']:
                        delDt0 = activeCust(rowData['i:ACTUAL_MVI_DT'],rowData['i:ACTUAL_MVO_DT'],startdt,cutdt0)
                        delDt1 = activeCust(rowData['i:ACTUAL_MVI_DT'],rowData['i:ACTUAL_MVO_DT'],cutdt1,enddt)
                        delDt = delDt0 + delDt1
                        if delDt > 100:
                            esi = rowData['i:ESI_ID']
                            if len(esi) > 10:
                                esiRk = esi[-6] +'-'+ esi
                                smtRk0 = esi[-6] +'-'+ chYear[0] +'-'+ esi
                                smtRk1 = esi[-6] +'-'+ chYear[1] +'-'+ esi
                                wthid = h_esi.row(row=esiRk, columns=['i:WTHR_ZONE_ID'])['i:WTHR_ZONE_ID']
                                if wthid != 'null':
                                    smtRk = smtRk0 +'|'+ smtRk1
                                    chCol = chCol_0 +'|' + chCol_1
                                    fOut = [wthid, smtRk, chCol]
                                    writer.writerow(fOut)
                if jdx == 5000:
                    tColNm = 'Status:' + rgidx
                    statusChk.put(nmRk, {tColNm:str(idx)})
                    print idx
                    jdx = 0
                idx += 1
                jdx += 1
    return idx

##  =============================================  ##
##  main
if __name__ == "__main__":
    startT = datetime.now()
    if cutMonth in [1,2,3,4,5,6]:
        chYear = str(cutYear-1)
        startdt = str(cutYear-1) +'-06-01'
        enddt = str(cutYear-1) +'-09-30'
        cutdt0,cutdt1 = 'na','na'
        chCol = 'm6+m7+m8+m9'
        counts = extractFromHbase_1Yr(rgNum)
    elif cutMonth in [7,8,9]:
        chYear = [str(cutYear-1),str(cutYear)]
        startdt = str(cutYear-1) + dtft(ydystring).strftime('-%m-%d')
        cutdt0 = str(cutYear-1) +'-09-30'
        cutdt1 = str(cutYear) +'-06-01'
        enddt = (dtft(ydystring)-timedelta(1)).strftime('%Y-%m-%d')
        if cutMonth == 7:
            chCol_0 = 'm7+m8+m9'
            chCol_1 = 'm6'
        elif cutMonth == 8:
            chCol_0 = 'm8+m9'
            chCol_1 = 'm6+m7'
        elif cutMonth == 9:
            chCol_0 = 'm9'
            chCol_1 = 'm6+m7+m8'
        counts = extractFromHbase_OverYr(rgNum)
    elif cutMonth in [10,11,12]:
        chYear = str(cutYear)
        startdt = str(cutYear) +'-06-01'
        enddt = str(cutYear) +'-09-30'
        cutdt0,cutdt1 = 'na','na'
        chCol = 'm6+m7+m8+m9'
        counts = extractFromHbase_1Yr(rgNum) 
    totalT = datetime.now()-startT
    timepass = totalT.seconds/60.0
    print 'completed %d counts, time used: %.2f minutes.' % (counts, timepass)
    tConn = happybase.Connection(host='172.25.10.78')
    statusT = tConn.table('Parallel_Process_Check_Status')
    tColNm = 'Status:' + rgNum
    statusT.put(nmRk, {tColNm: 'Completed. See log file for detail.'})
    tConn.close()
    eff_dt = ydystring
    outhivePath = hivePath + eff_dt + '/part=' + rgNum
    mkdir1 = 'hadoop fs -mkdir -p ' + outhivePath
    copyFromLocal1 = 'hadoop fs -copyFromLocal ' + outputFile +' '+ outhivePath
    if counts > 1300000:
        print 'Start moving file to HDFS'
        subprocess.call(mkdir1, shell=True)
        erChk1 = subprocess.call(copyFromLocal1, shell=True)
        if erChk1 == 0:
            print 'Moving Completed'
        else:
            print 'ERROR'
    else:
        print 'ERROR'