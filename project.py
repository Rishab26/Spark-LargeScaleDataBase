#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:00:09 2020

@author: ufac001
"""

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta
from itertools import chain

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return datetime.timezone(datetime.timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=timezone.utc)
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

# Q1: replace pass with your code
def extract_email_network(rdd):
    rdd_mail = rdd.map(lambda x: Parser().parsestr(x))
    filter_none = lambda x: list(filter(None.__ne__,x))
    rdd_full_email_tuples = rdd_mail.map(lambda x :(x.get('From') ,filter_none([x.get('To')]+[x.get('Cc')]+[x.get('Bcc')]),date_to_dt(x.get('Date'))))
    flattened_list = lambda x : list(chain.from_iterable(ele.split(",") for ele in x))
    val_by_vec = lambda x: ((x[0],j,x[2]) for j in flattened_list(x[1]))
    rdd_email_triples= rdd_full_email_tuples.flatMap(lambda x: val_by_vec(x))
    replace_n = lambda x : (x[0].replace("\n",""),x[1].replace("\n",""),x[2])
    replace_t = lambda x : (x[0].replace("\t",""),x[1].replace("\t",""),x[2])
    replace_sp = lambda x : (x[0].replace(" ",""),x[1].replace(" ",""),x[2])
    rdd_email_triples_filtered = rdd_email_triples.map(lambda x :replace_n(x)).map(lambda x :replace_t(x)).map(lambda x :replace_sp(x))
    regex = "((\w+[\.\-])+(\w*[a-zA-Z]\w*))@enron.com"
    valid_email = lambda s: re.compile(regex).fullmatch(s) != None
    rdd_email_triples_enron = rdd_email_triples_filtered.filter(lambda x: x[0]!=x[1]).filter(lambda x:(valid_email(x[0]),valid_email(x[1]),x[2]))
    distinct_triples = rdd_email_triples_enron.distinct()
    return distinct_triples
# Q2: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    timestamp = lambda x: x.replace(tzinfo=timezone.utc)
    if drange!= None:
        t1_t = rdd.map(lambda x : (x[0],x[1],x[2]))
        condition_apply = lambda x :((x[0],x[1],x[2]) if drange[0] <= x[2] <= drange[1] else None)
        t1_withact = rdd.filter(lambda x : condition_apply(x))
        t1 = t1_withact.map(lambda x :((x[0],x[1]),1)).reduceByKey(lambda x,y: x+y)
        rdd_q2 = t1.map(lambda x : (x[0][0],x[0][1],x[1])) 
    else :
        t1 = rdd.map(lambda x :((x[0],x[1]),1)).reduceByKey(lambda x,y: x+y)
        rdd_q2 = t1.map(lambda x : (x[0][0],x[0][1],x[1]))
    return rdd_q2

# Q3.1: replace pass with your code
def get_out_degrees(rdd):
    rdd_q3_1 = rdd.map(lambda x : (x[0],x[2])).reduceByKey(lambda x,y:x+y)
    rdd_q3_2 = rdd.map(lambda x :(x[1],0))
    rdd_q3_union = rdd_q3_2.union(rdd_q3_1)
    rdd_q3 = rdd_q3_union.reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortBy(lambda x: x,False)
    return rdd_q3
    

# Q3.2: replace pass with your code         
def get_in_degrees(rdd):
    rdd_q3_1 = rdd.map(lambda x : (x[1],x[2])).reduceByKey(lambda x,y:x+y)
    rdd_q3_2 = rdd.map(lambda x :(x[0],0))
    rdd_q3_union = rdd_q3_2.union(rdd_q3_1)
    rdd_q3 = rdd_q3_union.reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortBy(lambda x: x,False)
    return rdd_q3

# Q4.1: replace pass with your code            
def get_out_degree_dist(rdd):
    rdd_q4_1 = rdd.map(lambda x : (x[0],x[2])).reduceByKey(lambda x,y:x+y)
    rdd_q4_2 = rdd.map(lambda x :(x[1],0))
    rdd_q4_union = rdd_q4_2.union(rdd_q4_1)
    rdd_q4 = rdd_q4_union.reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x,True)
    return rdd_q4

# Q4.2: replace pass with your code
def get_in_degree_dist(rdd):
    rdd_q4_1 = rdd.map(lambda x : (x[1],x[2])).reduceByKey(lambda x,y:x+y)
    rdd_q4_2 = rdd.map(lambda x :(x[0],0))
    rdd_q4_union = rdd_q4_2.union(rdd_q4_1)
    rdd_q4 = rdd_q4_union.reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x,True)
    return rdd_q4
