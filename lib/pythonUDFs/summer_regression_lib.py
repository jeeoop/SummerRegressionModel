#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 07 14:30:20 2015

@author: Ken Ouyang
"""

def activeCust(idt,odt,strtDt,endDt):
    deltaDay = 0
    try:
        mvi,mvo,dt1,dt2 = dtft(idt),dtft(odt),dtft(strtDt),dtft(endDt)
        if mvo > dt1 and mvo < dt2:
            if mvi < dt1:
                deltaDay = (mvo-dt1).days
            else:
                deltaDay = (mvo-mvi).days
        elif mvo > dt2:
            if mvi < dt1:
                deltaDay = (dt2-dt1).days
            elif mvi>dt1 and mvi<dt2:
                deltaDay = (dt2-mvi).days
    except:
        pass
    return deltaDay
