__author__ = 'elliott'
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from config import Config
import threading
import logging
import time
import traceback
import pymongo
import json
from pymongo import MongoClient
import calendar
import zlib
from datetime import datetime
logging.basicConfig(level=logging.INFO)
dbclient = MongoClient(Config.DATABASE_URL)
db = dbclient['instagram']
job_collection = db['job']
p1 = job_collection.find_one({"lat":22.305,"lng":114.155})
etime = p1["etime"]
ctime = p1["ctime"]
p1["etime"] = p1["ctime"]-86400*15
ctime = p1["ctime"]-86400*15
job_collection.save(p1)
dif = (ctime-etime)/32
for i in range(32):
    r = {'ctime':ctime, 'etime':ctime-dif,'lat':22.305,'lng':114.155, 'stime':ctime, 'ongoing':1}
    ctime = ctime - dif
    job_collection.insert_one(r)
