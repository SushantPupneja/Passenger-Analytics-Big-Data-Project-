from Utility import Utility
import os
import json
import xlsxwriter
import pandas as pd


def insert_excel(row):
    """
        This function will add a row in excel
    """
    #load the excel, if exists
    try:
        df_ = pd.read_excel('sample_big_data.xlsx', sheet=1)
        df_.loc[len(df_)] = row
    except:
        print("excel not exists, creating one")
        df_ = pd.DataFrame(columns=columns)
        df_.loc[0] = row
    #initiate excel writer to write altered frame in excel
    writer = pd.ExcelWriter('sample_big_data.xlsx', engine='xlsxwriter')
    try:
        df_.to_excel(writer,'Sheet1')
        writer.save()
    except:
        print("excel found/created not able to append row") 

location_list_footfall = {}

def insert_data():
    #insert data in CR_Dataset
    values = ""
    column_names = set(location_list_footfall.keys())
    for location_footfall in location_list_footfall:
        values += location_list_footfall[location_footfall]
    print column_names, values

    # sql = "INSERT INTO CR_Reports {} VALUES {}".format(column_names)


def generate_report(unprocessed_data_list):
    
    utility = Utility(log_file="big_data_process_log_file.txt", debug=1, db='Analytics')

    for passenger_key in unprocessed_data_list:

        # sql = "SELECT location, similarity, count(*) FROM BIG_DATA WHERE image_key = '{}' and device_type = '{}' GROUP BY location;".format(image_key,'footfall')
        # matched_location_list, matched_location_list_count = utility.query_database(sql)

        # select rows where image_key is passenger_key and device_type is "footfall"
        df_filtered = df_[(df_.similarity >= 80) & (df_.device_type == "footfall") & (df_.image_key == passenger_key)]
        print len(df_filtered)
        continue

        if not matched_location_list_count:
            print("passenger not found at any of the shops")
            continue

        for matched_location in matched_location_list_count:
            similarity, location =  matched_location[0], matched_location[1]

            if int(similarity) < 85:
                continue

            location = "".join(location.split(" "))

            try:
                location_list_footfall[location] += 1
            except:
                print("appending new location in list")
                location_list_footfall[location] = 1

        #insert row into the database   #cursor.execute('SELECT last_insert_id()')
        print(location_list_footfall)
    insert_data()

def main():
    utility = Utility(log_file='big_data_process_log_file.txt', debug=1, db='Analytics')
    
    sql = "SELECT ReportID FROM CR_Reports WHERE {}".format(filter_cr_reports)
    print sql
 
    report_list, report_list_count = utility.query_database(sql)

    print report_list, report_list_count

    if report_list_count:
        print("report already processed")
        return None

    # sql_1 = "SELECT distinct(image_key) FROM BIG_DATA WHERE {}".format(filter_big_data)
    # print sql_1
    # unprocessed_data_list, unprocessed_data_list_count = utility.query_database(sql_1)
    # print unprocessed_data_list_count
    try:
        df_ = pd.read_excel('sample_big_data.xlsx', sheet=1)
    except Exception, e:
        print(e)
        print("no file found to extract data")

    #select unique image keys from the frame
    ind_passengers = df_.image_key.unique()
    if len(ind_passengers):
        generate_report(ind_passengers)
        
    # if unprocessed_data_list_count:
    #     generate_report(unprocessed_data_list)
# read settings for process
f_path = os.path.abspath("Config.json")
print f_path

if f_path:
    with open(f_path) as read_file:
        list_ = json.load(read_file)          # read configurations from file
        # database credentials
        big_data_host = list_["_mysql_big_data"]["host"]
        big_data_user = list_["_mysql_big_data"]["user"]
        big_data_passwd = list_["_mysql_big_data"]["passwd"]
        big_data_db = list_["_mysql_big_data"]["db"]

        # read filters
        shop_filter = list_["report_filters"]["shop_filter"]
        flight_filter = list_["report_filters"]["flight_filter"]
        destination_filter = list_["report_filters"]["destination_filter"]

        #create filter
        if shop_filter:
            filter_big_data = "location = '{}'".format(shop_filter)
            filter_cr_reports = "shop_filter = '{}'".format(shop_filter)

        if flight_filter:
            filter_big_data += "and flight_name = '{}'".format(flight_filter)
            filter_cr_reports += "and flight_filter = '{}'".format(flight_filter)


        if destination_filter:
            filter_big_data += "and destination = '{}'".format(destination_filter)
            filter_cr_reports += "and destination_filter = '{}'".format(destination_filter)

        print filter_big_data, filter_cr_reports
        threshold_similarity = list_["big_data_config"]["threshold_similarity"]
        debug = True

        if shop_filter:
            main()
        else:
            print("mandatory filters not available")
        # try:
        #     # create a database connection for processing using sqlalchmey
        #     engine = create_engine('mysql://{}:{}@{}/{}'.format(big_data_db, big_data_user, big_data_host, big_data_passwd))
        #     Session = sessionmaker(engine)
        #     session = Session()
        # except:
        #     print("Error 2: Not able to connect to BIG DATA database")

else:
    print("Not able to read database credentials")


# CREATE TABLE CR_Reports (ReportID int(11) auto_increment PRIMARY KEY, ShopFilter VARCHAR(50), FlightFilter VARCHAR(50), DestinationFilter VARCHAR(50), DataSetId INT NOT NULL) ENGINE=INNODB;
# CREATE TABLE CR_DataSet (DataSetId int(11) AUTO_INCREMENT PRIMARY KEY, KarachiBakery int(11) default 0, TifinExpress int(11) default 0, KFC int(11)) ENGINE=INNODB;
# ALTER TABLE CR_Reports ADD CONSTRAINT DataSetId FOREIGN KEY (DataSetId) REFERENCES CR_DataSet (DataSetId) ON DELETE CASCADE;

#CREATE TABLE DeviceMapper (id int auto_increment PRIMARY KEY, DeviceName VARCHAR(50) NOT NULL, DeviceType VARCHAR(50) NOT NULL, DeviceLocation VARCHAR(50) NOT NULL);