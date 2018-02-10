import os
import MySQLdb
import datetime
import time
import json
# import pandas as pd
import threading
import pika


class Utility:
   ConfigFilePath = "Config.json"
   mode = 0
   debug = 0

   # is there a need to pass 'db' in the init ? because it is getting initialised in the init_config func
   def __init__(self, debug=0, log_file=None, db=None):
       self.db = None
       self.log_file = None
       self.logmode = "File"
       self.unprocessed = None
       self.processed = None
       self.sleep_time = None
       self.mode = None
       self.rekognition_collection_name = None
       self.AWS_ACCESS_KEY = None
       self.AWS_ACCESS_SECRET_KEY = None
       self.bucket_name = None
       self.face_match_threshold = None
       self.bucket_location = None
       # no need to pass db as param
       self.init_config(log_file, db)
       self.debug = debug
       self.multi_face_coll_flag = None
       self.sleepTime_addToColl = None
       if self.logmode == "Queue":
         print "Initialising Queue Logging"
         self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
         self.channel=self.connection.channel()
         #self.channel.exchange_declare('LogMessages',type='direct')
       else:
         self.connection = None
         self.channel = None

   def loginfo(self,msg):
       if self.logmode == "File":
           self.loginfofile(msg)
       elif self.logmode == "Queue":
           self.loginfoqueue(msg)
     
   def loginfoqueue(self,msg):
    print msg
    RoutingKey = "FaceRecog"
    if threading.currentThread().getName() is not None:
        thread_name = threading.currentThread().getName()
    else:
        thread_name = "MainP"
    CurrentDT = datetime.datetime.now()
    CurrentDTStr = CurrentDT.strftime("%d-%m-%y %H:%M:%S")
    msg = CurrentDTStr + ": " + str(thread_name) + ": " + msg

    print "****" + RoutingKey + ": " + msg
    self.channel.basic_publish(exchange='LogMessages' , routing_key= RoutingKey, body = msg)
    #print "...."
    #print "Msg sent: " + RoutingKey

   def loginfofile(self,msg):
       if self.mode == 0:
           print msg
       elif self.mode == 1 and self.debug == 1:
           CurrentDT = datetime.datetime.now()
           CurrentDTStr = CurrentDT.strftime("%d-%m-%y %H:%M:%S")
           # l_thread = threading.currentThread()
           # print l_thread
           if threading.currentThread().getName() is not None:
               thread_name = threading.currentThread().getName()
           else:
               thread_name = "MainP"
           msg = CurrentDTStr + ": " + str(thread_name) + ": " + msg +"\n"
           lock = threading.Lock()
           # print lock, lock.acquire()
           while not lock.acquire():
               l_msg = "waiting to acquire lock on file for 100 ms"
               print l_msg
               self.loginfo(l_msg)
               time.sleep(1)
           # print("XXXXXXX lock acquried by the {} XXXXXXX".format(thread_name))
           if self.debug:
               fo = open(self.log_file, "a")
               # print "File opened successfully..."
               fo.write(msg)
           # print "msg printing done"
           if self.debug:
               try:
                   fo.flush()
                   os.fsync(fo.fileno())
                   fo.close()
                   # print "File closed successfully..."
               except:
                   print "error in closing the log file"
           lock.release()
           # print("XXXX lock released by the {} XXXXXXX".format(thread_name))
           # msg = CurrentDTStr + ":" +  msg
           # fo = open(self.logFile,'a')
           # print >>fo,msg + ""
           # fo.flush()
           # os.fsync(fo.fileno())
           # fo.close()

   def init_config(self, log_file, db=None):  # no need db param
       # print os.path.abspath(__file__)

       try:
           config_json = json.loads(open(self.ConfigFilePath).read())
           # change the DB params & keep it configurable
           # self.db = MySQLdb.connect(host=config_json["_mysql_indigo_dev"]["host"],
           #                           user=config_json["_mysql_indigo_dev"]["user"],
           #                           passwd=config_json["_mysql_indigo_dev"]["passwd"],
           #                           db=config_json["_mysql_indigo_dev"]["db"])
           if db is None:
            self.db = MySQLdb.connect(host=config_json["_mysql_indigo_dev"]["host"],
                                     user=config_json["_mysql_indigo_dev"]["user"],
                                     passwd=config_json["_mysql_indigo_dev"]["passwd"],
                                     db=config_json["_mysql_indigo_dev"]["db"])
           else:
            self.db = MySQLdb.connect(host=config_json["_mysql_big_data"]["host"],
                                     user=config_json["_mysql_big_data"]["user"],
                                     passwd=config_json["_mysql_big_data"]["passwd"],
                                     db=config_json["_mysql_big_data"]["db"])
           self.unprocessed = config_json['_unprocessed']
           self.processed = config_json['_processed']
           self.sleep_time = config_json['_sleep_time']
           self.logmode = config_json['_logmode']
           self.rekognition_collection_name = config_json['rekognition_collection_name']
           self.AWS_ACCESS_KEY = config_json['AWS']['AWS_ACCESS_KEY']
           self.AWS_ACCESS_SECRET_KEY = config_json['AWS']['AWS_ACCESS_SECRET_KEY']
           self.bucket_name = config_json['AWS']['bucket_name']
           self.bucket_location = config_json['AWS']['bucket_location']
           self.multi_face_coll_flag = config_json['multi_face_coll_flag']
           self.sleepTime_addToColl = config_json['sleepTime_addToColl']
           self.face_match_threshold = config_json['face_match_threshold']

           if log_file:
               self.log_file = str(os.path.join(config_json["_logs"], log_file))
               self.mode = 1
           else:
               self.mode = 0
       except Exception as e:
           msg = "error reading config file" + str(e)
           self.loginfo(msg)
           print("Exception inside the init_config func of the utility class: {}".format(e))

   def query_database(self, sql):
       # type: (object) -> object
       """
       :rtype: rows and rows count
       """
       cursor = self.db.cursor()
       try:
           cursor.execute(sql)
           rows = cursor.fetchall()
           no_of_rows = cursor.rowcount
           message = "Read Query: SUCCESS, Count({})".format(no_of_rows)
           message = "Read Query: '" + sql + "', SUCCESS, Count({})".format(no_of_rows)
           self.loginfo(message)
           return rows, no_of_rows
       except Exception as e:
           message = "Update Query: '" + sql + "', FAILED. Exception : " + str(e)
           self.loginfo(message)
           return None, 0  # added zero

   def update_database(self, sql):
       cursor = self.db.cursor()
       try:
           cursor.execute(sql)
           self.db.commit()
           message = "Update Query: '" + sql + "', SUCCESS"
           self.loginfo(message)
           return True
       except Exception as e:
           message = "Update Query: '" + sql + "', FAILED. Exception : " + str(e)
           self.loginfo(message)
           self.db.rollback()
           return False

"""
   def query_database_df(self, sql):
       cursor = self.db.cursor()
       self.loginfo("Read Query: " + sql)
       try:
           df = pd.read_sql(sql, con=self.db)
           return df
       except Exception as e:
           self.loginfo("Error in executing sql " + str(e))
           return None

   def get_unique_uuid(self):
       sql = "Select UUID();"
       self.loginfo("Query: " + sql)
       try:
           df = pd.read_sql(sql, con=self.db)
           return df.get_value(0, 'UUID()', takeable=False)
       except Exception as e:
           self.loginfo("Error in executing sql" + str(e))
           self.db.rollback()
           return None

   def check_params(self, request, param, man):
       if request.POST.get(param):
           return request.POST.get(param)
       elif man == True:
           pass
       else:
           return False


if __name__ == "__main__":
   utility = Utility("Cleanup.txt")
   utility = Utility()
   utility.get_unique_uuid()
"""

