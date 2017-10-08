import MySQLdb
from datetime import datetime
import re
import pandas as pd
import json
import os
from sqlalchemy import create_engine
from Utility import Utility
import threading
import time

# host = "192.168.1.200"
host = "aitat2.ckfsniqh1gly.us-west-2.rds.amazonaws.com"
user = "IndigoDev"
db = "IndigoDev"
passwd = "IndigoDev"

# create a database connection for processing using sqlalchmey
engine = create_engine('mysql://GMRAnalytics:GMRAnalytics@aitat2.ckfsniqh1gly.us-west-2.rds.amazonaws.com/GMRAnalytics')

# create a database connection for processing using MYSQLdb
db = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)

cursor = db.cursor()

columns = ['date', 'time', 'image_key', 'face_id', 'image_type', 'gender', 'age', 'matched_grp_key', 'similarity', 'location', 'emotion', 'psngr_pnr', 'psngr_flight', 'flight_name', 'destination', 'time_grp', 'age_grp', 'valid_status']

#location mapping object
locations = {"ICam011": "Tifin Express", "ICam012": "Karachi Bakery"}

def insert_table(row, utility):
    df_ = pd.DataFrame(columns=columns)
    df_.loc[0] = row
    # utility.loginfofile(str(df_))
    # sql = "INSERT INTO BIG_DATA (date, time, image_key, face_id, type, gender, age, matched_grp_key, similarity, location, emotion, psngr_pnr, psngr_flight, flight_name, destination, time_grp, age_grp, valid_status) values {};".format((str(row[0]), str(row[1]), row[2], row[3], row[4], row[5], str(row[6]), row[7], str(row[8]), row[9], row[10], row[11], row[12], row[13],row[14], row[15], str(row[16]), row[17]))
    try:
        # print sql
        # cursor.execute(sql)
        df_.to_sql("BIG_DATA", con=engine, flavor=None, schema=None, if_exists='append', index=False, index_label=None, chunksize=None, dtype=None)
        utility.loginfofile("row inserted in big_data")
        return True
    except Exception, e:
        utility.loginfofile("issue occurred while updating big data table " + str(e))
        return False


# get age group


def find_age_grp(i):
    age_grp = 0
    if i in range(1, 6): age_grp = 5
    elif i in range(6, 11): age_grp = 10
    elif i in range(11, 16): age_grp = 15
    elif i in range(16, 21): age_grp = 20
    elif i in range(21, 26): age_grp = 25
    elif i in range(26, 31): age_grp = 30
    elif i in range(31, 36): age_grp = 35
    elif i in range(36, 41): age_grp = 40
    elif i in range(41, 46): age_grp = 45
    elif i in range(46, 51): age_grp = 50
    elif i in range(51, 56): age_grp = 55
    elif i in range(56, 61): age_grp = 60
    elif i in range(61, 66): age_grp = 65
    elif i in range(66, 71): age_grp = 70
    elif i in range(71, 76): age_grp = 75
    elif i in range(76, 81): age_grp = 80
    elif i in range(81, 86): age_grp = 85
    elif i in range(86, 91): age_grp = 90
    elif i in range(91, 96): age_grp = 95
    elif i in range(96, 101): age_grp = 100

    return age_grp

# update status to database for analytic check


def check_analytic_true(img_id, utility):
    sql = "Update AP_ImageData SET status = {} where ID = '{}';".format(5, img_id)
    flag = utility.update_database(sql)
    if flag:
        utility.loginfofile("Quitting thread after updating status- " + str(threading.currentThread().getName()))
        return True
    else:
        utility.loginfofile("Database query Failed while updating status for thread " + str(threading.currentThread().getName()) + str(e))
        return False

# process individual images


def process_ind_image(image_key, log_id):
    # create an object for Utility
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)

    utility.loginfofile("Initializing thread- " + threading.currentThread().getName() + ",type-ind")
    # search for individual records in AP_ImageData_Info
    sql = "SELECT ImageKey , faceId , gender , age_High, age_Low , emotions , location, " \
          "logDate, type , psngr_pnr , psngr_flight FROM AP_ImageData_Info WHERE ImageKey = '{}' ORDER BY LogDate;".format(image_key)

    ind_images, no_of_rows = utility.query_database(sql)

    try:
        if no_of_rows > 0:
            # get characteristics for each individual person
            for ind_img in ind_images:
                process_image = True
                image_type = ind_img[8]
                if image_type == "Ind":

                    image_key = ind_img[0]
                    face_id = ind_img[1]
                    gender = ind_img[2]
                    logDate = ind_img[7]

                    if logDate is not None:
                        date = logDate.date()
                        time = logDate.time()
                        process_image = True
                    else:
                        utility.loginfofile("DATETIME ERROR....... incorrect logdate")
                        process_image = False

                    all_emotions = ind_img[5]

                    if all_emotions or all_emotions is None:
                        emotion = re.findall(r"[\w^.]+", all_emotions)[0]
                    else:
                        emotion = "NA"

                    if (ind_img[3] and ind_img[4]) or (ind_img[3] is None or ind_img[4] is None) :
                        age = int((int(ind_img[3]) + int(ind_img[4])) / 2)
                        age_grp = find_age_grp(age)
                    else:
                        age = "NA"
                        age_grp = "NA"

                    # get flight details for each individual person

                    psngr_pnr = ind_img[9]
                    psngr_flight = ind_img[10]
                    if process_image:
                        if psngr_flight:
                            utility.loginfofile("accessing Flight details for passenger")
                            flight_name = psngr_flight[:2]

                            sql = "SELECT (SELECT FI1.Description from Airlines_Info FI1 where FI1.Airline2LC='%s') as flightName,(SELECT AI.City_Desc FROM Airports_Info AI where Airport_3LC=FI.Destination) as City_Desc from  Flights_Info FI where FI.FLNO1='%s' OR FI.FLNO2='%s' OR FI.FLNO3='%s';" % (
                                str(flight_name), str(psngr_flight), str(psngr_flight), str(psngr_flight))

                            # accessing flight details for ind images
                            flight_details, no_of_rows = utility.query_database(sql)

                            if no_of_rows > 0:
                                flight_name = flight_details[0][0]
                                destination = flight_details[0][1]
                            else:
                                flight_name = "NA"
                                destination = "NA"

                        else:
                            flight_name = "NA"
                            destination = "NA"

                        image_type = ind_img[8]
                        if time != "NA":
                            time_grp = str(time)[:2]
                        else:
                            time_grp = "NA"

                        # search for matches in CL_ImageMap

                        sql = "SELECT IndKey, GrpKey, Similarity, MatchDevId, valid_status FROM CL_ImageMap WHERE IndKey = '{}'".format(image_key)

                        utility.loginfofile("searching...... matches for ind image")

                        processed_images, no_of_rows = utility.query_database(sql)

                        # if matches found for the image
                        if no_of_rows > 0:
                            for image in processed_images:
                                matched_grp_key = image[1]
                                similarity = image[2][:6]
                                devid = image[3]
                                # location mapping
                                location = locations[devid]
                                valid_status = image[4]

                                if valid_status is None:
                                    valid_status = "NA"
                                else:
                                    valid_status = str(valid_status)
                                # create a row for data frame

                                row = [date, time, image_key, face_id, image_type, gender, age, matched_grp_key,
                                       similarity, location, emotion, psngr_pnr, psngr_flight, flight_name, destination,
                                       time_grp, age_grp, valid_status]
                                utility.loginfofile(str(row))

                                # add row in data frame/database
                                flag = insert_table(row, utility)
                                # df_.loc[len(df_)] = row

                    # if no match found for individual
                        else:
                            utility.loginfofile("No match found for the image")
                            matched_grp_key = "NA"
                            similarity = "NA"
                            location = "NA"
                            valid_status = "NA"

                            # create a row for data frame

                            row = [date, time, image_key, face_id, image_type, gender, age, matched_grp_key,
                                   similarity, location, emotion, psngr_pnr, psngr_flight, flight_name, destination,
                                   time_grp, age_grp, valid_status]

                            utility.loginfofile(str(row))
                            # add row in data frame/database
                            flag = insert_table(row, utility)
                            # df_.loc[len(df_)] = row
                            # flag = True

                else:
                    utility.loginfofile("Error Image_Type in AP_ImageData_Info for ind image")
        else:
            utility.loginfofile("no face/unprocessed image found " + str(image_key))

        # update status = 5 in AP_ImageData
        check_analytic_true(log_id, utility)
        return True
    except Exception, e:
        utility.loginfofile("Individual Image Main process interruption " + str(e))
        return False

# process group images

def process_grp_image(image_key, log_id):
    # create an object for Utility
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)

    utility.loginfofile("Initializing thread- " + str(threading.currentThread().getName()) + ",type-grp")

    sql = "SELECT ImageKey , faceId , gender , age_High, age_Low , emotions , location, " \
          "logDate, type FROM AP_ImageData_Info WHERE ImageKey = '{}' ORDER BY LogDate;".format(image_key)
    grp_images, no_of_rows = utility.query_database(sql)

    try:
        if no_of_rows > 0:
            utility.loginfofile("Number of faces found in grp image " + str(no_of_rows))
            for grp_img in grp_images:

                image_key = grp_img[0]
                face_id = grp_img[1]
                gender = grp_img[2]
                devid = grp_img[6]
                # location mapping
                location = locations[devid]

                date = grp_img[7].date()
                time = grp_img[7].time()
                all_emotions = grp_img[5]

                if all_emotions:
                    emotion = re.findall(r"[\w^.]+", all_emotions)[0]
                else:
                    emotion = "NA"

                if grp_img[3] and grp_img[4]:
                    age = int((int(grp_img[3]) + int(grp_img[4])) / 2)
                    age_grp = find_age_grp(age)
                else:
                    age = "NA"
                    age_grp = "NA"

                # data enrichment for group images

                matched_grp_key = "NA"
                similarity = "NA"
                psngr_pnr = "NA"
                psngr_flight = "NA"
                flight_name = "NA"
                destination = "NA"
                valid_status = "NA"

                image_type = grp_img[8]
                time_grp = str(time)[:2] + ":00:00"

                # create a row for data frame
                row = [str(date), str(time), image_key, face_id, image_type, gender, str(age), matched_grp_key,
                               str(similarity), location, emotion, psngr_pnr, psngr_flight, flight_name, destination,
                               time_grp, str(age_grp), valid_status]

                utility.loginfofile(str(row))
                # add row in data frame/database
                flag = insert_table(row, utility)
                # df_.loc[len(df_)] = row
        else:
            utility.loginfofile("No face/unprocessed grp image found " + str(image_key))
        # update status with 5 in AP_ImageDate
        check_analytic_true(log_id, utility)
        return True
    except Exception, e:
        utility.loginfofile("Grp Image process interruption" + str(e))


# initiate the process with datetime range:----
def gp_process(from_date=None, to_date=None):
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1)

    if from_date is not None or to_date is not None:
        sql1 = "SELECT Distinct Date(LogDate) FROM AP_ImageData WHERE logDate BETWEEN '{}' AND '{}' ORDER \
                     BY LogDate DESC;".format(from_date, to_date)
    else:
        sql1 = "SELECT Distinct Date(LogDate) FROM AP_ImageData;"
    # fetch all dates to process images
    dates, no_of_rows = utility.query_database(sql1)
    # add log into file
    utility.loginfofile(str(dates))

    for date in dates:

        utility.loginfofile("starting processing data for date " + str(date[0]))

        sql = "SELECT ImageKey, type , LogDate, ID FROM AP_ImageData WHERE status = '{}' and Date(LogDate) = '{}' " \
              "ORDER BY LogDate DESC;".format(4, date[0])
        # get all records from AP_ImageData with status 4 to add in BigData
        images, no_of_rows = utility.query_database(sql)

        if no_of_rows > 0:
            # records found to add in the bigData
            utility.loginfofile(" '{}' images found to process for date {}".format(no_of_rows, str(date[0])))

            for img in images:
                image_key = img[0]
                image_type = img[1]
                log_id = img[3]

                utility.loginfofile("found image with type '{}'".format(image_type))

                # check for thread count
                print ("Active-Thread: " + str(threading.activeCount()))
                while threading.activeCount() > 10:
                    print ("thread memory is full.........  Waiting for free memory going to sleep....")
                    # utility.loginfofile("Waiting for free thread going to sleep.......")
                    time.sleep(1)

                # get characteristics for image from AP_ImageData_Info
                if image_type == "Ind":
                    t1 = threading.Thread(name="Thread_for_log_id-" + str(log_id), target=process_ind_image,
                                          args=(image_key, log_id))
                    t1.start()
                elif image_type == "Grp":
                    t2 = threading.Thread(name="Thread_for_log_id-" + str(log_id), target=process_grp_image,
                                          args=(image_key, log_id))
                    t2.start()
                # else:
                #     check_flag = False

        else:
            utility.loginfofile("No new records found for big data")

    # print df_
    # writer = pd.ExcelWriter('Big_Data_Analytics_GMR_New.xlsx', engine='xlsxwriter')
    #
    # df_.to_excel(writer, index=True)
    # writer.save()


# ----------- init --------------


# read settings for process
f_path = os.path.abspath("Config.json")
print f_path

if f_path:

    with open(f_path) as read_file:
        list_ = json.load(read_file)          # read configurations from file
        print list_

        to_date = list_["big_data_config"]["to_date"]
        from_date = list_["big_data_config"]["from_date"]

        if from_date or to_date:
            print ("big data init with args")
            gp_process(from_date, to_date)
        else:
            print ("gp_process init without args")
            gp_process()

else:
    print("Not able to read settings")

# if __name__ == "__main__":
#     #Footfall_shops(fromDate,toDate)
#     init_big_data_process(fromDate,toDate)



# BIG_DATA sql:

# CREATE TABLE BIG_DATA (id int(11) PRIMARY KEY auto_increment, date date, time time, image_key VARCHAR(100), face_id VARCHAR(100), type VARCHAR(25), gender VARCHAR(25), age VARCHAR(25), matched_grp_key VARCHAR(100), similarity VARCHAR(10), location VARCHAR(25), emotion VARCHAR(25), psngr_pnr VARCHAR(25), psngr_flight VARCHAR(25), flight_name VARCHAR(25), destination VARCHAR(25), time_grp VARCHAR(10), age_grp VARCHAR(10), valid_status VARCHAR(25))
