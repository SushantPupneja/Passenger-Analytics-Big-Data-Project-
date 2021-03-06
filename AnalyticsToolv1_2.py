from datetime import datetime
import re
import pandas as pd
import json
import os
from sqlalchemy import create_engine
from Utility import Utility
import threading
import time
from decorators import run_time_decorator
from numpy import *


# Pax Flow Analyzer Implementation:----
class PaxFlow(object):
    def __init__(self, pax_key, pax_enter_date, pax_enter_time, pax_exit_time, location, location_type):
        self.pax_flow = {"key": pax_key, "enter_date": pax_enter_date, "exit_time": pax_exit_time,
                         "enter_time": pax_enter_time, "location": location,
                         "location_type": location_type}

    def set(self, pax_key, pax_value):
        self.pax_flow[pax_key] = pax_value

    def get(self, pax_info_key):
        return self.pax_flow[pax_info_key]

    @run_time_decorator
    def add_pax_flow(self, utility):
        sql = "INSERT INTO PaxFlowAnalyzer ('PaxKey', 'PaxDate', 'PaxLocation', 'PaxLocationType', " \
              "'PaxEnterTime', 'PaxExitTime') VALUES ('{}', '{}', '{}', '{}')" \
            .format(self.pax_flow["key"], self.pax_flow["enter_date"], self.pax_flow["location"],
                    self.pax_flow["location_type"], self.pax_flow["enter_time"], self.pax_flow["exit_time"])
        print sql
        flag = utility.update_database(sql)
        print flag
        if flag:
            print "passenger flow captured in database"
        else:
            print "passenger flow not captured in database"
        return None


columns = ['date', 'time', 'image_key', 'face_id', 'image_type', 'gender', 'age', 'matched_grp_key', 'matched_grp_emotion',
           'matched_grp_logdate', 'similarity', 'location', 'device_type', 'emotion', 'psngr_pnr', 'psngr_flight', 'flight_name', 'destination',
           'time_grp', 'age_grp', 'queue_wait_time', 'valid_status']

# update locations from database

# location mapping object
locations = {"ICam003": "Karachi Bakery", "ICam021": "Karachi Bakery", "ICam011": "Queue"}


# debug function
def log(s):
    if debug:
        print(s)


def create_image_name(date, time, device_id, image_type):
    # image_key = "F1ICam012_Grp_2017-06-03_19:01:41.632899.jpg"
    return "_".join(["F1", device_id, image_type, date, (time + ".jpg")])


# insert received row in the BIG_DATA table

@run_time_decorator
def insert_table(row):
    """
        This function will create a row against a conversion, footfall, queue_wait_time.
    """
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)

    df_ = pd.DataFrame(columns=columns)
    df_.loc[0] = row
    # utility.loginfofile(str(df_))
    # sql = "INSERT INTO BIG_DATA (date, time, image_key, face_id, type, gender, age, matched_grp_key, similarity, location, emotion, psngr_pnr, psngr_flight, flight_name, destination, time_grp, age_grp, valid_status) values {};".format((str(row[0]), str(row[1]), row[2], row[3], row[4], row[5], str(row[6]), row[7], str(row[8]), row[9], row[10], row[11], row[12], row[13],row[14], row[15], str(row[16]), row[17]))
    try:
        # print sql
        # cursor.execute(sql)
        df_.to_sql("BIG_DATA", con=engine, flavor=None, schema=None, if_exists='append', index=False, index_label=None,
                   chunksize=None, dtype=None)
        utility.loginfofile("row inserted in big_data")
    except Exception, e:
        utility.loginfofile("issue occurred while updating big data table " + str(e))
        # rollback of the transaction into the database
        return False


@run_time_decorator
# get age group
def find_age_grp(i):
    age = i
    compare_age = float(age) / 10 - age / 10
    if compare_age > 0.5:
        age_grp = ((age + 5) / 10) * 10
    elif compare_age < 0.5:
        age_grp = (age / 10) * 10
    else:
        age_grp = age
    # age_grp = 0
    # if i in range(1, 6):
    #     age_grp = 5
    # elif i in range(6, 11):
    #     age_grp = 10
    # elif i in range(11, 16):
    #     age_grp = 15
    # elif i in range(16, 21):
    #     age_grp = 20
    # elif i in range(21, 26):
    #     age_grp = 25
    # elif i in range(26, 31):
    #     age_grp = 30
    # elif i in range(31, 36):
    #     age_grp = 35
    # elif i in range(36, 41):
    #     age_grp = 40
    # elif i in range(41, 46):
    #     age_grp = 45
    # elif i in range(46, 51):
    #     age_grp = 50
    # elif i in range(51, 56):
    #     age_grp = 55
    # elif i in range(56, 61):
    #     age_grp = 60
    # elif i in range(61, 66):
    #     age_grp = 65
    # elif i in range(66, 71):
    #     age_grp = 70
    # elif i in range(71, 76):
    #     age_grp = 75
    # elif i in range(76, 81):
    #     age_grp = 80
    # elif i in range(81, 86):
    #     age_grp = 85
    # elif i in range(86, 91):
    #     age_grp = 90
    # elif i in range(91, 96):
    #     age_grp = 95
    # elif i in range(96, 101):
    #     age_grp = 100

    return age_grp


# update status to database for analytic check
def check_analytic_true(img_id):
    while True:
        try:
            utility = Utility(log_file="big_data_process_log_file.txt", debug=1)
            print("connected to mysql db")
            break
        except:
            print("mysql server gone....... trying again")

    sql = "Update AP_ImageData SET status = {} where ID = '{}';".format(5, img_id)

    flag = utility.update_database(sql)

    if flag:
        utility.loginfofile("Quitting thread after updating status- " + str(threading.currentThread().getName()))
        return True
    else:
        utility.loginfofile("Database query Failed while updating status for thread " + str(threading.currentThread().getName()))
        return False


# process individual images for pax flow at the airport


@run_time_decorator
def pax_flow_analyzer(img_date, img_time, image_key, matched_image_list):
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)
    utility.loginfo("Initializing ......... PaxFlowAnalyzer")
    pax_location, pax_location_type, pax_key, pax_date, pax_sha_enter_time, pax_sha_exit_time = "SHA", "SHA", \
                                                                                                image_key, img_date, img_time, img_time

    # passenger must be at sha
    pax_sha = PaxFlow(pax_key, pax_date, str(pax_sha_enter_time), str(pax_sha_exit_time), pax_location,
                      pax_location_type)
    # PaxFlow("F1XXCdf0000", "2018-01-23", "13:00:00", "21:00:00", "sha1", "sha")
    pax_sha.add_pax_flow(utility)
    del pax_sha

    if len(matched_image_list) == 0:
        return None

    for pax in matched_image_list:
        pax_similarity, pax_location, pax_location_type, pax_location_time = pax[2], pax[3], pax[5], pax[7].time()
        utility.loginfo("passenger({}) found at {}, type: {}".format(pax_key, pax_location, pax_location_type,
                                                                     str(pax_location_time)))

        # if pax_location_time == "footfall":
        try:
            print("passenger({}) found at {}, type: {}, capture time: {}".format(pax_key, pax_location,
                                                                                 pax_location_type,
                                                                                 str(pax_location_time)))
            # passenger location changes , break passenger flow.
            if pax_location != pax_flow.get("location"):
                # add passenger flow in the database
                pax_flow.add_pax_flow(utility)
                # update passenger flow for new location
                print("updating passenger flow for ({}) new location({})........., type: {}".format(pax_key,
                                                                                                    pax_location,
                                                                                                    pax_location_type))
                pax_flow.set("location", pax_location)
                pax_flow.set("location_type", pax_location_type)
                pax_flow.set("enter_time", str(pax_location_time))
                pax_flow.set("exit_time", str(pax_location_time))
                continue

            pax_exit_time = datetime.strptime(pax_flow.get("exit_time"), '%H:%M:%S').time()

            if pax_location_time > pax_exit_time:
                pax_flow.set("exit_time", str(pax_location_time))

        except NameError:
            utility.loginfo("creating object for passenger({}) found at {}, type:{}".format(pax_key, pax_location,
                                                                                            pax_location_type))
            pax_flow = PaxFlow(pax_key, str(pax_date), str(pax_location_time), str(pax_location_time), pax_location,
                               pax_location_type)
    # add current passenger flow in the db
    pax_flow.add_pax_flow(utility)
    del pax_flow
    return None


@run_time_decorator
# process individual images for conversion and footfall
def process_footfall_conversion(log_id, date, time, image_key, face_id, image_type, gender, age, emotion, psngr_pnr,
                                psngr_flight, flight_name, destination, time_grp, age_grp, processed_images):
    # create an object for Utility
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)

    utility.loginfofile("Initializing thread- " + threading.currentThread().getName() + ",type-footfall-conversion")

    # create 2D matrix for to track billing and footfall across locations ####['location', footfall_flag, billing_flag, deviceId]
    pax_tracking = array([])
    for image in processed_images:
        matched_grp_key, similarity, devid, valid_status, device_type, location, matched_grp_logdate, matched_grp_emotion = image[1], image[2][:6], image[3], image[4], \
                                                                        image[5], image[8], image[7], "NA"

        if device_type == "queue_wait":
            continue

        if location not in map(lambda x: x[0], pax_tracking):
            # location not exists in pax_tracking
            append(pax_tracking, [location, 0, 0, devid], 0)
        for dev in pax_tracking:
            if dev[0] != location:
                continue
            if dev[1] == 1 and dev[2] == 1:
                break
            if device_type == "footfall":
                dev[1] = 1
            elif device_type == "billing":
                dev[2] = 1

        if valid_status is None:
            valid_status = "NA"
        else:
            valid_status = str(valid_status)

        queue_wait_time = "NA"
        # create a row for data frame
        row = [str(date), str(time), image_key, face_id, image_type, gender, str(age), matched_grp_key, matched_grp_emotion, matched_grp_logdate,
               str(similarity), location, device_type, emotion, psngr_pnr, psngr_flight, flight_name, destination,
               time_grp, str(age_grp), queue_wait_time, valid_status]

        utility.loginfofile(str(row))

        # add row in data fram  e/database
        # insert_excel(row)
        print("inserting row in BG for {}".format(device_type))
        insert_table(row)

    for tracked_device in pax_tracking:
        location, footfall_flag, billing_flag, device_id = tracked_device[0], tracked_device[1], tracked_device[2], tracked_device[3]
        if not footfall_flag and billing_flag:
            utility.loginfofile("conversion found without footfall at location:{}".format(location))
            matched_grp_key, similarity, valid_status, device_type, queue_wait_time, matched_grp_emotion, matched_grp_logdate = create_image_name(str(date),
                                                                                                        str(time),
                                                                                                        device_id,
                                                                                                        'grp'), \
                                                                                      threshold_similarity, "NA", \
                                                                                      "footfall", "NA", "NA", datetime.strptime(str(date)+" "+ str(time), "%Y-%m-%d %H:%M:%S")

            row = [str(date), str(time), image_key, face_id, image_type, gender, str(age), matched_grp_key, matched_grp_emotion, matched_grp_logdate,
                   str(similarity), location, device_type, emotion, psngr_pnr, psngr_flight, flight_name, destination,
                   time_grp, str(age_grp), queue_wait_time, valid_status]
            insert_table(row)

    return True


@run_time_decorator
def process_queue_wait_time(log_id, date, time, image_key, face_id, image_type, gender, age, emotion, psngr_pnr,
                            psngr_flight, flight_name, destination, time_grp, age_grp, processed_images):
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)

    utility.loginfofile("Initializing thread- " + threading.currentThread().getName() + ", type-queue_wait")

    # queue_row_data = {"matched_grp_key":"", "similarity":"", "img_logdate":""}
    for processed_image in processed_images:
        if processed_image[5] != "queue_wait":
            print("checking queue: {} found !!!!!!!!!".format(processed_image[5]))
            continue
        # check individual logdate must be greater group logdate
        print("checking queue: {} found !!!!!!!!!".format(processed_image[5]))
        utility.loginfofile("passenger found at queue")
        device_type, ind_logdate, grp_logdate, similarity = processed_image[5], processed_image[6], processed_image[7], \
                                                            processed_image[2]

        if ind_logdate < grp_logdate:
            print("Alert!!!!!!!!!!...............passenger found at queue after sha (sha:{}, queue:{})".format(
                str(ind_logdate), str(grp_logdate)))
            continue

        matched_grp_key, matched_devid, valid_status, matched_grp_emotion, matched_grp_logdate = processed_image[1], processed_image[3], "NA", "NA", grp_logdate
        queue_wait_time = str((ind_logdate - grp_logdate).total_seconds())
        try:
            location = locations[matched_devid]
        except:
            location = ""
        # now add row in the table
        row = [str(date), str(time), image_key, face_id, image_type, gender, str(age), matched_grp_key, matched_grp_emotion, matched_grp_logdate,
               str(similarity)[0:9], location, device_type, emotion, psngr_pnr, psngr_flight, flight_name, destination,
               time_grp, str(age_grp), queue_wait_time, valid_status]

        # insert_excel(row)
        insert_table(row)

    return None


# process individual passengers
@run_time_decorator
def process_individual(image_key, image_type, log_id, log_date):
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)
    # get meta info of the passenger from AP_ImageData_Info
    sql = "SELECT ImageKey , faceId , gender , age_High, age_Low , emotions , location, " \
          "logDate, type , psngr_pnr , psngr_flight, device_type FROM AP_ImageData_Info WHERE ImageKey = '{}' " \
          "ORDER BY LogDate;".format(image_key)

    images, no_of_rows = utility.query_database(sql)

    if no_of_rows == 0:
        # in case of no row found, no face found for the image, case should be reported as no face images not
        # has status 4 in AP_ImageData.
        utility.loginfofile("ALERT......!!!!!!!!!!!!!! No face/unprocessed image found " + str(image_key))
        # no face found in the image , image/passenger is not valid no entry in BIG_DATA required
        # update status with 5 in AP_ImageDate
        log("no face found in the image")
        check_analytic_true(log_id)
        return None

    # meta information for face of the passenger at SHA
    utility.loginfofile("Number of faces found in an image " + str(no_of_rows) + "for " + str(image_key))
    log("Number of faces found in an image " + str(no_of_rows) + "for " + str(image_key))
    # for ideal cases there would be only one face or a face with max area will be processed
    # but in cases multiple faces found , all faces will act as an individual passenger entry for big_data
    for img in images:
        image_key, face_id, gender, age_high, age_low, all_emotions, devid, ind_log_date, ind_image_type, psngr_pnr, psngr_flight, device_type = \
            img[0], img[1], img[2], img[3], img[4], img[5], img[6], img[7], img[8], img[9], img[10], img[11]

        # in case logdate at ap_imagedata not matches with logdate at ap_imagedata_info
        if log_date != ind_log_date or image_type != ind_image_type:
            utility.loginfo("ALERT..........!!!!!!!! Data redundancy error {}:{}, {}:{}".format(log_date, ind_log_date,
                                                                                                image_type, ind_image_type))
        img_date, img_time = ind_log_date.date(), ind_log_date.time()

        if all_emotions:
            emotion = re.findall(r"[\w^.]+", all_emotions)[0]
        else:
            emotion = "NA"

        if age_high and age_low:
            age = int((int(age_high + age_low)) / 2)
            age_grp = find_age_grp(age)  # age belongs to which group
        else:
            age, age_grp = "NA", "NA"

        if image_type == "Ind":

            if psngr_flight:
                utility.loginfofile("accessing Flight details for passenger")
                flight_name = psngr_flight[:2]
                sql = "SELECT (SELECT FI1.Description from Airlines_Info FI1 where FI1.Airline2LC='%s') as flightName,(SELECT AI.City_Desc FROM Airports_Info AI where Airport_3LC=FI.Destination) as City_Desc from  Flights_Info FI where FI.FLNO1='%s' OR FI.FLNO2='%s' OR FI.FLNO3='%s';" % (
                    str(flight_name), str(psngr_flight), str(psngr_flight), str(psngr_flight))
                # accessing flight details for ind images
                flight_details, no_of_rows = utility.query_database(sql)
                if no_of_rows > 0:
                    flight_name, destination = flight_details[0][0], flight_details[0][1]
                else:
                    flight_name, destination = "NA", "NA"
            else:
                psngr_pnr, psngr_flight, flight_name, destination = "NA", "NA", "NA", "NA"

            if img_time != "NA":
                time_grp = str(img_time)[:2]
            else:
                time_grp = "NA"

            sql = "SELECT IndKey, GrpKey, Similarity, MatchDevId, valid_status, device_type, IndLogDate, GrpLogDate, DeviceLocation FROM CL_ImageMap WHERE IndKey LIKE '{}%' and valid_status = {} ORDER BY GrpLogDate".format(
                image_key[0:99], 1)

            utility.loginfofile("searching...... matches for ind image")
            processed_images, no_of_rows = utility.query_database(sql)

            if no_of_rows == 0:
                # no match found for the passenger at any of the locations, add passenger to the data collections,
                # continue for next passenger
                utility.loginfofile(" no matches found for image {}".format(no_of_rows, image_key))
                location, matched_grp_key, similarity, queue_wait_time, valid_status, matched_grp_emotion, matched_grp_logdate = "NA", "NA", "NA", "NA", "NA", "NA", datetime.strptime(str(img_date)+" "+ str(img_time), "%Y-%m-%d %H:%M:%S")
                time_grp = str(img_time)[:2] + ":00:00"
                row = [str(img_date), str(img_time), image_key, face_id, image_type, gender, str(age),
                       matched_grp_key, matched_grp_emotion, matched_grp_logdate, str(similarity), location, device_type, emotion, psngr_pnr,
                       psngr_flight, flight_name, destination, time_grp, str(age_grp), queue_wait_time,
                       valid_status]

                utility.loginfofile(str(row))
                # add row in data frame/database
                t1 = threading.Thread(name="Thread_for_log_id-" + str(log_id), target=insert_table(row))
                t1.start()
                check_analytic_true(log_id)
                continue

            if pax_flow_analyze_process == "True":
                pax_flow_analyzer(img_date, img_time, image_key, processed_images)

            if footfall_conversion_process == "True":
                process_footfall_conversion(log_id, img_date, img_time, image_key, face_id, image_type, gender,
                                            age, emotion, psngr_pnr, psngr_flight, flight_name, destination,
                                            time_grp, age_grp, processed_images)

            if queue_wait_process == "True":
                process_queue_wait_time(log_id, img_date, img_time, image_key, face_id, image_type, gender, age, emotion, psngr_pnr, psngr_flight, flight_name, destination, time_grp, age_grp, processed_images)
    check_analytic_true(log_id)
    return None


# initiate the process with datetime range:----
def gp_process(from_date=None, to_date=None):
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)

    if from_date is not None or to_date is not None:
        sql1 = "SELECT Distinct Date(LogDate) FROM AP_ImageData WHERE status = '{}' and logDate BETWEEN '{}' AND '{}' ORDER \
                    BY LogDate DESC;".format(4, from_date, to_date)
    else:
        sql1 = "SELECT Distinct Date(LogDate) FROM AP_ImageData WHERE status = '{}';".format(4)
    # fetch all dates to process images
    dates, no_of_rows = utility.query_database(sql1)
    # add log into file
    utility.loginfofile(str(dates))

    if not no_of_rows:
        utility.loginfofile("no unprocessed data found")
        return None

    del utility
    for date in dates:
        utility = Utility(log_file="big_data_process_log_file.txt", debug=1)
        utility.loginfofile("starting processing data for date " + str(date[0]))
        # get characteristics for image from AP_ImageData_Info
        sql = "SELECT ImageKey, type , LogDate, ID FROM AP_ImageData WHERE status = '{}' and Date(LogDate) = '{}' " \
              "and Type = '{}' ORDER BY LogDate DESC;".format(4, date[0], 'Ind')
        # get all records from AP_ImageData with status 4
        un_pro_images, no_of_rows = utility.query_database(sql)

        if no_of_rows == 0:
            # in case no records found to process (status = 4) for the date, continue for next date available
            utility.loginfofile("No new records found to process for date".format(str(date)))
            continue
        # records found(status=4) to process
        utility.loginfofile(" '{}' images found to process for date {}".format(no_of_rows, str(date[0])))
        for un_pro_img in un_pro_images:
            image_key, image_type, log_date, log_id = un_pro_img[0], un_pro_img[1], un_pro_img[2], un_pro_img[3]
            utility.loginfofile("processing image with image_key, type ('{}', '{}')".format(image_key, image_type))
            # check for thread count
            # print ("Active-Thread: " + str(threading.activeCount()))
            while threading.activeCount() > 10:
                print ("thread memory is full.........  Waiting for free memory going to sleep....")
                # utility.loginfofile("Waiting for free thread going to sleep.......")
                time.sleep(1)

            # process individual passenger in thread

            t1 = threading.Thread(name="Thread for passenger({}), log-id: {}-".format(image_key, log_id),
                                  target=process_individual, args=(image_key, image_type, log_id, log_date))
            t1.start()

        del utility



            # image_type == "Grp":
            #         log("passing by group images")
            #         check_analytic_true(log_id)
            #         pass


# location, matched_grp_key, similarity, psngr_pnr, psngr_flight, flight_name, destination, queue_wait_time, valid_status = "NA","NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA"
#                     time_grp = str(img_time)[:2] + ":00:00"
#                     # create a row for data frame
#                     row = [str(img_date), str(img_time), image_key, face_id, image_type, gender, str(age), matched_grp_key,
#                                str(similarity), location, device_type, emotion, psngr_pnr, psngr_flight, flight_name, destination,
#                                time_grp, str(age_grp), queue_wait_time, valid_status]

#                     utility.loginfofile(str(row))
#                     # add row in data frame/database
#                     t2 = threading.Thread(name="Thread_for_log_id-" + str(log_id), target=insert_table(row),)
#                     t2.start()
#                     utility.loginfofile("Initializing thread- " + threading.currentThread().getName() + ", type-grp image")
# ----------- init --------------

# read settings for process
f_path = os.path.abspath("Config.json")
print f_path


if f_path:

    with open(f_path) as read_file:
        list_ = json.load(read_file)  # read configurations from file

        to_date = list_["big_data_config"]["to_date"]
        from_date = list_["big_data_config"]["from_date"]
        threshold_similarity = list_["big_data_config"]["threshold_similarity"]
        footfall_conversion_process = list_["big_data_config"]["footfall_conversion_process"]
        queue_wait_process = list_["big_data_config"]["queue_wait_process"]
        pax_flow_analyze_process = list_["big_data_config"]["pax_flow_analyzer"]
        # database credentials
        big_data_host = list_["_mysql_big_data"]["host"]
        big_data_user = list_["_mysql_big_data"]["user"]
        big_data_passwd = list_["_mysql_big_data"]["passwd"]
        big_data_db = list_["_mysql_big_data"]["db"]

        debug = True

        try:
            # create a database connection for processing using sqlalchmey
            engine = create_engine(
                'mysql://{}:{}@{}/{}'.format(big_data_db, big_data_user, big_data_host, big_data_passwd))
        except:
            log("Error 2: Not able to connect to BIG DATA database")

        # try:
        #     writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
        # except Exception, e:
        #     log("Error 1: Not able to create writer for excel {}".format(str(e)))

        if from_date or to_date:
            print ("big data init with args")
            gp_process(from_date, to_date)
        else:
            print ("gp_process init without args")
            gp_process()

else:
    print("Not able to read settings")