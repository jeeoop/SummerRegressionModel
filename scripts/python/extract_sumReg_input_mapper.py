#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 08 18:50:50 2015

@author: Ken Ouyang
"""

import happybase
from datetime import datetime
import sys

def twoD(string):
    if len(string)==1: return '0'+string
    else: return string

def dtft(indt):
    return datetime.strptime(indt, '%Y-%m-%d').date()

def fixAry(in1,in2):
    ary1,ary2 = [],[]
    tck,uck = 0,0
    for i,j in zip(in1,in2):
        try:
            if float(j)>71:
                tck += 1
                if float(i)>0:
                    uck += 1
                    ary1.append(i)
                    ary2.append(j)
        except:
            pass
    try:
        if uck*1.00/tck > 0.8 and uck>20:
            chk=0
        else:
            chk=1
    except:
        chk=1
    arystr1,arystr2 = '|'.join(ary1),'|'.join(ary2)
    return (arystr1,arystr2,chk, str(tck),str(uck))

def get24ColNm(chosenMonth):
    array1 = map(lambda x: 'r:24-'+x, chosenMonth)
    array2 = map(lambda x: 'r:tem-'+x, chosenMonth)
    array3 = map(lambda x: 'r:i-'+x, chosenMonth)
    return array1,array2,array3

def read_input(file):
    for line in file:
        line = line.strip()
        yield line

def main():
    allText = read_input(sys.stdin)
    pool = happybase.ConnectionPool(size=2, host='hbasemaster')
    with pool.connection() as conn:
        h_smt = conn.table('ETL_ESI_SMT_G4')
        h_tem = conn.table('ETL_WEATHER_INFO_24')
        for line in allText:
            rds = line.split(',')
            wthid,esiRks,terms = rds[0],rds[1],rds[2]
            if len(esiRks.split('|')) == 1:
                smtRk = esiRks
                esi = esiRks.split('-')[2]
                temRk = wthid +'-'+ esiRks.split('-')[1]
                exec 'chCol = ' + terms
                get24, get24t, get24i = get24ColNm(chCol)
                smtRow = h_smt.row(row=smtRk, columns=get24)
                temRow = h_tem.row(row=temRk, columns=get24t)
                infRow = h_smt.row(row=smtRk, columns=get24i)
                smtAry, temAry = [], []
                if len(smtRow)*1.00/len(temRow) > 0.8:
                    for uKey in smtRow:
                        keyCol = uKey.split('-')[1]
                        iKey = 'r:i-' + keyCol
                        tKey = 'r:tem-' + keyCol
                        checkRds = infRow[iKey].split('|')
                        if checkRds[0] == 'nrg' and checkRds[2] == 'A': # and (checkRds[3]=='A' or checkRds[4]=='A'):
                            uString = '|'.join([smtRow[uKey].split('|')[i] for i in hr2305])
                            tString = '|'.join([temRow[tKey].split('|')[i] for i in hr2305])
                            smtAry.append(uString)
                            temAry.append(tString)
                    smtStr1 = '|'.join(smtAry)
                    temStr1 = '|'.join(temAry)
                    smtOutStr,temOutStr,chcount, ucount, tcount = fixAry(smtStr1.split('|'),temStr1.split('|'))
                    if chcount == 0:
                        print '%s,%s,%s,%s,%s' % (esi,tcount,ucount,smtOutStr,temOutStr)
            elif len(esiRks.split('|')) == 2:
                smtRk_0,smtRk_1 = esiRks.split('|')[0],esiRks.split('|')[1]
                term_0, term_1 = terms.split('|')
                esi = smtRk_0.split('-')[2]
                temRk_0 = wthid +'-'+ smtRk_0.split('-')[1]
                temRk_1 = wthid +'-'+ smtRk_1.split('-')[1]
                exec 'chCol_0 = ' + term_0
                exec 'chCol_1 = ' + term_1
                get24_0, get24t_0, get24i_0 = get24ColNm(chCol_0)
                get24_1, get24t_1, get24i_1 = get24ColNm(chCol_1)
                smtRow_0 = h_smt.row(row=smtRk_0, columns=get24_0)
                smtRow_1 = h_smt.row(row=smtRk_1, columns=get24_1)
                temRow_0 = h_tem.row(row=temRk_0, columns=get24t_0)
                temRow_1 = h_tem.row(row=temRk_1, columns=get24t_1)
                infRow_0 = h_smt.row(row=smtRk_0, columns=get24i_0)
                infRow_1 = h_smt.row(row=smtRk_1, columns=get24i_1)
                smtRow = smtRow_0.copy()
                smtRow.update(smtRow_1)
                temRow = temRow_0.copy()
                temRow.update(temRow_1)
                infRow = infRow_0.copy()
                infRow.update(infRow_1)
                smtAry, temAry = [], []
                if len(smtRow)*1.00/len(temRow) > 0.8:
                    for uKey in smtRow:
                        keyCol = uKey.split('-')[1]
                        iKey = 'r:i-' + keyCol
                        tKey = 'r:tem-' + keyCol
                        checkRds = infRow[iKey].split('|')
                        if checkRds[0] == 'nrg' and checkRds[2] == 'A': # and (checkRds[3]=='A' or checkRds[4]=='A'):
                            uString = '|'.join([smtRow[uKey].split('|')[i] for i in hr2305])
                            tString = '|'.join([temRow[tKey].split('|')[i] for i in hr2305])
                            smtAry.append(uString)
                            temAry.append(tString)
                    smtStr1 = '|'.join(smtAry)
                    temStr1 = '|'.join(temAry)
                    smtOutStr,temOutStr,chcount, ucount, tcount = fixAry(smtStr1.split('|'),temStr1.split('|'))
                    if chcount == 0:
                        print '%s,%s,%s,%s,%s' % (esi,tcount,ucount,smtOutStr,temOutStr)

##  =============================================  ##
##  main
if __name__ == "__main__":
    hr2305 = range(0,5)+range(22,24)
    l30 = range(1,31)
    l31 = range(1,32)
    m6 =  [ '06'+twoD(str(i)) for i in l30 ]
    m7 =  [ '07'+twoD(str(i)) for i in l31 ]
    m8 =  [ '08'+twoD(str(i)) for i in l31 ]
    m9 =  [ '09'+twoD(str(i)) for i in l30 ]
    main()