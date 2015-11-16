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
from datetime import datetime
logging.basicConfig(level=logging.INFO)


class InstagramImageCrawler(threading.Thread):

    def __init__(self, manager, latitude, longitude, start_time, access_token):
        super(InstagramImageCrawler, self).__init__()
        self.manager = manager
        self.access_token = access_token
        self.lat = latitude
        self.lng =longitude
        self.stime = start_time


    def run(self):
        try:
            api = InstagramAPI(access_token=self.access_token, client_secret=Config.CLIENT_SECRET, client_id=Config.CLIENT_ID)
            result = api.media_search(lat=self.lat, lng=self.lng, max_timestamp=self.stime, count=1000, distance=4990)
            current_time = 0
            logging.info("Server return %d item from (%s,%s)"%(len(result),self.lat,self.lng))

            for media in result:
                caption = ""
                tags = ""
                latitude = ""
                longitude = ""
                location_id = ""
                if media.caption:
                    caption = media.caption.text
                if media.location:
                    location_id = str(media.location.id)
                    latitude = str(media.location.point.latitude)
                    longitude = str(media.location.point.longitude)
                for tag in media.tags:
                    tags = tags + tag.name
                    tags = tags + " "

                raw_json = json.dumps(media.raw)
                current_time = calendar.timegm(media.created_time.timetuple())
                image_record = {"json":raw_json, "image_id": media.id, "created_time": media.created_time, "caption":caption, "location_id":location_id, "longitude":longitude, "latitude":latitude, "tags":tags, "user_id":media.user.id, "user_name":media.user.username}
                user_record = {"user_id":media.user.id, "user_name":media.user.username}
                self.manager.image_collection.update_one({"image_id": media.id},{"$set":image_record}, upsert= True)
                self.manager.user_collection.update_one({"user_id":media.user.id}, {"$set":user_record} , upsert=True)
            if len(result) == 0:
                current_time = self.stime - 86400*5

            job = self.manager.job_collection.find_one({"lat":self.lat, "lng":self.lng})
            job['ctime'] = current_time
            self.manager.job_collection.save(job)
            logging.info("Job done! Updating Current Time %s %s for (%s,%s)"%(current_time, datetime.utcfromtimestamp(float(current_time)), self.lat, self.lng))
        except:
            traceback.print_exc()

class InstagramManager:

    def __init__(self):
        self.dbclient = None
        self.db = None
        self.job_collection = None
        self.image_collection = None
        self.user_collection = None
        self.accessTokenPools = {}

    def check_access_tokens(self):

        for accessToken in Config.ACCESS_TOKENS:
            try:
                api = InstagramAPI(access_token=accessToken, client_secret=Config.CLIENT_SECRET, client_id=Config.CLIENT_ID)
                api.user_recent_media(user_id="self", count=1)
                logging.info("Access Token %s is valid, remaining: %s" % (accessToken, api.x_ratelimit_remaining))
                self.accessTokenPools[accessToken] = int(api.x_ratelimit_remaining)
            except InstagramAPIError as e:
                logging.warning("Ran out of quota for this access token:%s" % accessToken)

    def retrieve_access_token(self):
        max_limit = 0
        proposed_accessToken = ''
        for accessToken in self.accessTokenPools:
            currentLimit = self.accessTokenPools[accessToken]
            if currentLimit > max_limit:
                max_limit = currentLimit
                proposed_accessToken = accessToken
        if proposed_accessToken != '':
            self.accessTokenPools[proposed_accessToken] = max_limit - 1

        return proposed_accessToken

    def remaining_access_token(self):
        sum = 0
        for accessToken in self.accessTokenPools:
            currentLimit = self.accessTokenPools[accessToken]
            sum += currentLimit
        return sum

    def init_database(self):
        self.dbclient = MongoClient(Config.DATABASE_URL)
        self.db = self.dbclient['instagram']
        self.job_collection = self.db['job']
        self.image_collection = self.db['image']
        self.user_collection = self.db['user']
        if self.job_collection.count() == 0:
            logging.warning("Empty Database. Will build up the table")
            self.buildup_database()
        else:
            logging.info("Database connection ready!")

    def buildup_database(self):
        for (lat, lng) in Config.COORDINATES:
            stime = int(time.time())-86400
            etime = stime - 86400*365
            job = {"lat":lat, "lng":lng, "stime":stime, "ctime":stime, "etime":etime, "ongoing":1}
            self.job_collection.insert_one(job)
        self.user_collection.create_index([('user_id', pymongo.ASCENDING)], unique=True)
        #self.user_collection.create_index([('user_id', pymongo.ASCENDING)], unique=True)
        self.image_collection.create_index([('image_id', pymongo.ASCENDING)], unique=True)

    def schedule(self):
        threads = []
        access_token = ''
        for job in self.job_collection.find({"ongoing": 1}):
            if job['ctime'] <= job['etime']:
                job['ongoing'] = 0
                self.job_collection.save(job)
                continue
            while True:
                access_token = self.retrieve_access_token()
                if access_token == '':
                    logging.warning("Not enough access_tokens! Waiting")
                    time.sleep(60*5)
                    self.check_access_tokens()
                else:
                    break
            craw_thread = InstagramImageCrawler(self,job['lat'],job['lng'],job['ctime'],access_token)
            craw_thread.start()
            threads.append(craw_thread)
            logging.info("Schedule A Task for the following attributes: lat:%s, lng:%s, timestamp:%s, accesstoken:%s"%(job['lat'],job['lng'],job['ctime'],access_token))

        while True:
            finished = True
            time.sleep(3)
            for t in threads:
                if t.is_alive():
                    finished = False
            if finished:
                break
            else:
                logging.debug("Some thread are still running!")







c = InstagramManager()
c.check_access_tokens()
c.init_database()
while True:
    c.schedule()
    logging.info("All tasks done! Wait for next shot! Remaining Shots:%d"%c.remaining_access_token())
    time.sleep(1)