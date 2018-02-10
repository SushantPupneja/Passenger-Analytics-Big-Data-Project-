from random import randint
from datetime import datetime, timedelta
from Utility import Utility
import uuid
from decorators import run_time_decorator

device_list = {'billing': ['ICam007', 'ICam008', 'ICam009', 'ICam012'], 'sha': ['ICam001', 'ICam002', 'ICam003'],
               'footfall': ['ICam004', 'ICam005', 'ICam006', 'ICam011'], 'queue': ['ICam010']}

device_mapping = {'Karachi Bakery': ['ICam007', 'ICam004'], 'Tiffin Express': ['ICam008', 'ICam005'],
                  'KFC': ['ICam009', 'ICam006'], 'Shopper Stop': ['ICam011', 'ICam012']}

flag = [True, False]
gender_list = ["Male", "Female"]
emotion_list = ["HAPPY", "ANGRY", "SAD", "SURPRISED", "CONFUSED", "DISGUSTED", "CALM"]


@run_time_decorator
def get_device_list():
    """

    :return: list of devices registered for data generation
    """
    all_device_list = []
    for device_type in device_list:
        print device_type
        devices = device_list[device_type]
        for device in devices:
            print device
            all_device_list.append(device)
    return all_device_list


@run_time_decorator
def random_number(inrange):
    """
    :param inrange:
    :return: an random integer within the passed range, [20,52]
    """
    return randint(inrange[0], inrange[1]-1)


@run_time_decorator
def create_image_name(date, time, device_id, image_type="None", psngr_pnr="None", psngr_flight="None"):
    """
    :param date:
    :param time:
    :param device_id:
    :param image_type:
    :param psngr_pnr:
    :param psngr_flight:
    :return:image_key = "F1ICam012_Grp_N65E68_D45K89_2017-06-03_19:01:41.632899.jpg"
    """
    return "_".join(["F1", device_id, image_type, psngr_pnr, psngr_flight, date, (time+".jpg")])


@run_time_decorator
def location_finder(device_id):
    for i in device_mapping.keys():
        if device_id in device_mapping[i]:
            return i
    return "NA"


@run_time_decorator
# get the device mapper initialized
def create_device_mapper():
    """
    :return: dictionary of device mapping
    """
    utility = Utility(log_file="data_gen_process_log_file.txt", debug=1)
    sql = "SELECT DeviceName, DeviceType, DeviceLocation FROM DeviceMapper"
    mapped_device_list, device_count = utility.query_database(sql)
    if device_count == 0:
        print("no device registered in the database")
        return None
    for mapped_device in mapped_device_list:
        print mapped_device
    return None


@run_time_decorator
def flight_details(flight_info=None):
    """

    :param flight_info:
    :return: pnr details
    """
    if flight_info is None:
        print("flight/destination required in args")
    randnum = str(uuid.uuid4())
    create_pnr = flight_info[0]+randnum[-5:].upper()
    return create_pnr


@run_time_decorator
def get_emotions():
    """

    :return: meta information of emotions for a face in an image
    """
    emotions = "{}:{} {}:{} {}:{}".format(emotion_list[random_number([0, len(emotion_list)])],
                                          random_number([100, 150]) / 4.0,
                                          emotion_list[random_number([0, len(emotion_list)])],
                                          random_number([50, 100]) / 4.0,
                                          emotion_list[random_number([0, len(emotion_list)])],
                                          random_number([10, 50]) / 4.0)
    return emotions


@run_time_decorator
def get_psngr_flight_info(airline, airport):
    """

    :param airline:
    :param airport:
    :return: passenger flight number
    """
    sql_get_airport_code = "SELECT Airport_3LC FROM Airports_Info where City_Desc = '{}'".format(airport)
    utility = Utility(log_file="data_gen_process_log_file.txt", debug=1)
    airport_code, airport_count = utility.query_database(sql_get_airport_code)
    if airport_count == 0:
        print("'{}': airport not served".format(airport))
        return None
    sql_get_airline_code = "SELECT Airline2LC FROM Airlines_Info where Description = '{}'".format(airline)
    airline_code, airline_count = utility.query_database(sql_get_airline_code)
    if airline_count == 0:
        print("'{};: airline service not available".format(airline))
        return None

    sql_get_flight_code = "SELECT FLNO3 FROM Flights_Info where Destination='{}' and FLNO3 LIKE '{}%'".format(airport_code[0][0],
                                                                                                              airline_code[0][0])
    psngr_flight, flight_count = utility.query_database(sql_get_flight_code)
    if flight_count == 0:
        print("{} not served to {}".format(airline, airport))
        return None
    return psngr_flight[0][0]


@run_time_decorator
def create_data(date, process_start_time, end_time, airline, destination):
    """

    :param date:
    :param process_start_time:
    :param end_time:
    :param airline:
    :param destination:
    :return: generate and dump data
    """
    # device name list
    utility = Utility(log_file="data_gen_process_log_file.txt", debug=1)
    print(airline, destination, process_start_time, end_time)
    # prepare flight details
    psngr_flight, start_time = get_psngr_flight_info(airline, destination), process_start_time
    if psngr_flight is None:
        return None
    print("{} is ready to take off for {}, on boarding passengers..........".format(airline, destination))
    reg_device_list = get_device_list()
    sha_devices = device_list['sha']

    # round robin selection of sha
    def get_device():
        for device in sha_devices:
            yield device

    device_gen_obj = get_device()
    while 4 > 3:
        if start_time >= end_time:
            break

        # create passenger at sha's using round robin
        try:
            device_id = device_gen_obj.next()
        except StopIteration:
            print("generation exception catched")
            device_gen_obj = get_device()
            device_id = device_gen_obj.next()

        device_type = "SHA"
        date, time = date, start_time.time()
        print device_id, date, time, device_type
        image_type, face_id, psngr_pnr, status = "Ind", "", flight_details(destination), 4
        image_key = create_image_name(str(date), str(time), device_id, image_type, psngr_pnr, psngr_flight)
        image_name = image_key[3:]
        print image_key, image_name
        # dump data in AP_ImageData(add passenger in the database)
        ap_image_info_row = [image_name, image_key, str(start_time), image_type, device_id, device_type, status]
        print ap_image_info_row
        sql = "INSERT INTO AP_ImageData (ImageName, ImageKey, LogDate, Type, DevId, status, device_type) VALUES ('{}', " \
              "'{}', '{}', '{}', '{}', '{}', '{}')".format(image_name, image_key, str(start_time),
                                                           image_type, device_id, status, device_type)
        # print sql
        utility.update_database(sql)

        # characteristics info
        age_low = random_number([15, 30])
        age_high = random_number([30, 70])
        emotions = get_emotions()
        gender, time_grp = gender_list[random_number([0, len(gender_list)])], str(time)[:2]
        print age_low, age_high, gender, emotions, time_grp
        # dump meta info in AP_ImageData_Info
        sql = "INSERT INTO AP_ImageData_Info (ImageKey, gender, age_High, age_Low, emotions, type, " \
              "psngr_pnr, psngr_flight, LogDate, location, device_type) VALUES ('{}', '{}', '{}', '{}', '{}', '{}'" \
              ", '{}', '{}', '{}', '{}', '{}' )".format(image_key, gender, age_high,
                                                        age_low, emotions, image_type,
                                                        psngr_pnr, psngr_flight,
                                                        str(start_time), device_id,
                                                        device_type)
        # print sql
        utility.update_database(sql)
        grp_matched_time = start_time
        # start_time += timedelta(minutes=5)

        print reg_device_list
        # find group images for the passenger
        for reg_device in reg_device_list:
            # minutes = random_number([1, 10])
            # grp_matched_time += timedelta(minutes=minutes)
            # generate matches for individual passengers.
            # create grp image against different locations:-
            if not flag[random_number([0, 1])]: # flag true means grp image is captured
                continue
            print reg_device
            matched_device_id, matched_device_type = "", ""
            reg_device_ind = False
            for device_type in device_list:
                if device_type == "sha":
                    # check if reg_device is of sha as well in that case please ignore the reg_device
                    if reg_device in device_list[device_type]:
                        reg_device_ind = True
                        break
                if reg_device in device_list[device_type]:
                    matched_device_id = reg_device
                    matched_device_type = device_type
                    break

            if reg_device_ind:
                continue
            minutes = random_number([3, 15])
            if matched_device_type == "queue":
                old_grp_matched_time = grp_matched_time
                grp_matched_time = process_start_time - timedelta(minutes=minutes)
            else:
                grp_matched_time += timedelta(minutes=minutes)
            matched_image_type, matched_date, matched_time = "Grp", date, grp_matched_time.time()

            matched_image_key = create_image_name(str(date), str(grp_matched_time), matched_device_id, matched_image_type)
            matched_image_name = matched_image_key[3:]
            # add group in ap_image_data
            sql_grp_img = "INSERT INTO AP_ImageData (ImageName, ImageKey, LogDate, Type, DevId, status, device_type) " \
                          "VALUES ('{}', " \
                          "'{}', '{}', '{}', '{}', '{}', '{}')".format(matched_image_name, matched_image_key,
                                                                       str(grp_matched_time),
                                                                       matched_image_type,
                                                                       matched_device_id,
                                                                       status, matched_device_type)
            # print sql_grp_img
            utility.update_database(sql_grp_img)
            # add meta info of group image in ap_image_data
            # dump meta info in AP_ImageData_Info
            grp_image_emotions = get_emotions()
            sql_grp_img_meta = "INSERT INTO AP_ImageData_Info (ImageKey, gender, age_High, age_Low, emotions, " \
                               "type, psngr_pnr, psngr_flight, LogDate, location, device_type) " \
                               "VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}' )".format\
                (matched_image_key, gender, age_high, age_low, grp_image_emotions,
                 matched_image_type, "None", "None", str(grp_matched_time), matched_device_id,matched_device_type)
            print sql_grp_img_meta
            # utility.update_database(sql_grp_img_meta)
            # add matches in CL_ImageMap
            similarity = str(random_number([75, 100]))
            matched_device_location = ""
            for location in device_mapping:
                if reg_device in device_mapping[location]:
                    matched_device_location = location
            sql_grp_img_match = "INSERT INTO CL_ImageMap (IndKey, GrpKey, Similarity, GrpLogDate, IndLogDate, " \
                                "MatchDevId, device_type, DeviceLocation) VALUES ('{}', '{}', '{}', '{}', '{}', " \
                                "'{}', '{}', '{}')".format(image_key, matched_image_key, similarity, grp_matched_time,
                                                           start_time, matched_device_id, matched_device_type,
                                                           matched_device_location)
            # print sql_grp_img_match
            utility.query_database(sql_grp_img_match)
            if matched_device_type == "queue":
                grp_matched_time = old_grp_matched_time
        start_time += timedelta(minutes=5)

# start making data
# create_device_mapper()
create_data(datetime.now().date(), datetime.now()-timedelta(hours=2), datetime.now(), "Indigo", "Ahmedabad")
# print get_psngr_flight_info("Emirates", "Chandigarh")
# print get_device_list()