import requests
import logging as log
import random
from flask import Flask, request, jsonify
from flask_restful import Api
from cassandra.cluster import Cluster
from timeit import default_timer as timer
from datetime import timedelta

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
api = Api(app)

##################################################
def getComponentURL(n, ip_add):
    url_string = "http://"
    if n == "2":
        url_string = url_string + ip_add + "/lgr/predict"
    elif n == "3":
        url_string = url_string + ip_add + "/lgr/predict" #"/svm/predict"
    elif n == "4":
        url_string = url_string + ip_add + "/lgr/predict" #"/analytics"
    else:
        msg = "DataLoader Error getComponentURL: Unable to find the service component, no Service component found with code: " + n
        log.info(msg)
        print(msg)
    return url_string

#################################################
class DataLoader:
    KEYSPACE=0
    cluster=0
    session=0
    rec_cnt=100
    def __init__(self):
        self.KEYSPACE = "ccproj_db"
        self.cluster = Cluster(['10.176.67.91'])
        self.session = self.cluster.connect()
        self.session.set_keyspace(self.KEYSPACE)

    def validation(self, usr_name):
        result = 0
        qry = "SELECT COUNT(*) FROM " + usr_name + ";"
        try:
            stat = self.session.prepare(qry)
            x = self.session.execute(stat)
            for row in x:
                result = row.count
        except:
            result = -1
            log.info("DataLoader validation: No Table found in Cassandra database.")
            print("DataLoader validation: No Table found in Cassandra database.")
        return result

    def employee_create_schema(self, usr_name):
        status = 0
        stat1 = "CREATE TABLE IF NOT EXISTS " + usr_name + """( 
                                            UU_ID UUID PRIMARY KEY, 
                                            EMP_ID DECIMAL, 
                                            DEPT_TYPE TEXT, 
                                            GENDER TEXT, 
                                            RACE   TEXT, 
                                            DAY_OF_WEEK TEXT, 
                                            CHECKIN_DATETIME TEXT, 
                                            DURATION TEXT);"""
        try:
            cre_tbl = self.session.prepare(stat1)
            self.session.execute(cre_tbl)
        except:
            status = -1
            self.session.shutdown()
        return status

    def employee_data_gen(self, usr_name):
        # define constants
        status = 0
        dept1 = ["1", "2", "3", "4", "5", "6"]
        gender1 = ["male", "female"]
        race1 = ["1", "2", "3", "4", "5", "6"]
        day1 = ['MON', "TUE", 'WED', 'THU', "FRI"]
        checkin1 = ["8:00", "8:30", "9:00",
                    "9:30", "10:00", "10:30",
                    "11:00", "11:30", "12:00",
                    "12:30", "13:00", "13:30", "14:00"]
        duration1 = ["1", "2", "3", "4", "5", "6", "7", "8"]
        # Insert query format
        ins_stat = "INSERT INTO " + usr_name + """(UU_ID, EMP_ID, DEPT_TYPE, GENDER, RACE, DAY_OF_WEEK, CHECKIN_DATETIME, DURATION)
                                                VALUES (now(), ?, ?, ?, ?, ?, ?, ?)"""
        try:
            insert_q = self.session.prepare(ins_stat)
            # generate data
            for i in range(1, self.rec_cnt + 1):
                empid = i
                dept = random.choice(dept1)
                gender = random.choice(gender1)
                race = random.choice(race1)
                day = random.choice(day1)
                checkin = random.choice(checkin1)
                duration = random.choice(duration1)
                self.session.execute(insert_q, [empid, dept, gender, race, day, checkin, duration])
        except:
            status = -1
            self.session.shutdown()
        return status

    def employee_append_data(self, usr_name, data):
        # define constants
        status = 0
        empid = data["emp_id"]
        dept1 = data["dept_type"]
        gender1 = data["gender"]
        race1 = data["race"]
        day1 = data["day_of_week"]
        checkin1 = data["checkin_datetime"]
        duration1 = data["duration"]
        # Insert query format
        ins_stat = "INSERT INTO " + usr_name + """(UU_ID, EMP_ID, DEPT_TYPE, GENDER, RACE, DAY_OF_WEEK, CHECKIN_DATETIME, DURATION)
                                                VALUES (now(), ?, ?, ?, ?, ?, ?, ?)"""
        try:
            insert_q = self.session.prepare(ins_stat)
            #generate data
            self.session.execute(insert_q, [empid, dept1, gender1, race1, day1, checkin1, duration1])
        except:
            status = -1
            self.session.shutdown()
        return status

    def hospital_create_schema(self, usr_name):
        status = 0
        stat1 = "CREATE TABLE IF NOT EXISTS " + usr_name + """( 
                                            uu_id UUID PRIMARY KEY, 
                                            hadm_id DECIMAL, 
                                            hospital_expire_flag DECIMAL, 
                                            insurance DECIMAL, 
                                            total_time_icu   DECIMAL, 
                                            num_in_icu DECIMAL, 
                                            amount DECIMAL, 
                                            rate DECIMAL, 
                                            total_items DECIMAL, 
                                            value DECIMAL, 
                                            dilution_value DECIMAL, 
                                            abnormal_count DECIMAL,
                                            item_distinct_abnormal DECIMAL,
                                            checkin_time TEXT);"""
        try:
            cre_tbl = self.session.prepare(stat1)
            self.session.execute(cre_tbl)
        except:
            status = -1
            self.session.shutdown()
        return status

    # hospital Data generator
    def hospital_data_gen(self, usr_name):
        # define constants
        status = 0
        hospital_expire_flag1 = [1, 2]
        insurance1 = [1, 2, 3, 4, 5]
        checkin_time1 = ["00:00", "00:30", "01:00", "01:30", "02:00", "02:30", "03:00", "03:30", "04:00", "04:30", "05:00", "05:30", "06:00",
                         "06:30","07:00", "07:30", "08:00", "08:30", "09:00", "09:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30"]
        # Insert query format
        ins_stat = "INSERT INTO " + usr_name + """ (uu_id, hadm_id, hospital_expire_flag, insurance, total_time_icu, num_in_icu,
                                                               amount, rate, total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_time)
                                                               VALUES (now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        try:
            insert_q = self.session.prepare(ins_stat)
        #generate data
            for i in range(1000, self.rec_cnt + 1000):
                hadm_id = i
                hospital_expire_flag = random.choice(hospital_expire_flag1)
                insurance = random.choice(insurance1)
                total_time_icu = random.uniform(0.002428259, 1)
                num_in_icu = random.uniform(0.142857143, 1)
                amount = random.uniform(-0.013434843, 1)
                rate = random.uniform(0, 1)
                total_items = random.uniform(0.093117409, 1)
                value = random.uniform(0.000110609, 1)
                dilution_value = random.uniform(0, 1)
                abnormal_count = random.uniform(0.001908852, 1)
                item_distinct_abnormal = random.uniform(0.059405941, 1)
                checkin_time = random.choice(checkin_time1)
                self.session.execute(insert_q,
                             [hadm_id, hospital_expire_flag, insurance, total_time_icu, num_in_icu, amount, rate,
                             total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_time])
        except:
            status = -1
            self.session.shutdown()
        return status

    def hospital_append_data(self, usr_name, data):
        # define constants
        status = 0
        # Insert query format
        ins_stat = "INSERT INTO " + usr_name + """ (uu_id, hadm_id, hospital_expire_flag, insurance, total_time_icu, num_in_icu,
                                                                       amount, rate, total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_time)
                                                                       VALUES (now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        try:
            insert_q = self.session.prepare(ins_stat)
            #generate data
            hadm_id = data["hadm_id"]
            hospital_expire_flag = data["hospital_expire_flag"]
            insurance = data["insurance"]
            total_time_icu = data["total_time_icu"]
            num_in_icu = data["num_in_icu"]
            amount = data["amount"]
            rate = data["rate"]
            total_items = data["total_items"]
            value = data["value"]
            dilution_value = data["dilution_value"]
            abnormal_count = data["abnormal_count"]
            item_distinct_abnormal = data["item_distinct_abnormal"]
            checkin_time = data["checkin_time"]
            self.session.execute(insert_q,
                             [hadm_id, hospital_expire_flag, insurance, total_time_icu, num_in_icu, amount, rate,
                             total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_time])
        except:
            status = -1
            self.session.shutdown()
        return status

    def __del__(self):
        self.session.shutdown()
        print("Object Destroyed")

class DLError(Exception):
    """Custom exception class to be thrown when local error occurs."""
    def __init__(self, message, status, payload=None):
        self.message = message
        self.status = status
        self.payload = payload

@app.errorhandler(DLError)
def handle_bad_request(error):
    """Catch BadRequest exception globally, serialize into JSON, and respond with 400."""
    payload = dict(error.payload or ())
    payload['status'] = error.status
    payload['message'] = error.message
    return jsonify(payload), error.status

@app.route("/dataloader", methods=['POST', 'GET'])
def dataloader():
    start = timer()
    #get request parameters
    req = request.get_json()
    workflow = req["workflow"]
    comp_name = req["client_name"]
    order = list(req["workflow_specification"])
    ip_list = req["ips"]
    res = 0
    status1 = status2 = -1
    #start workflow
    log.info("DataLoader: Starting workflow: " + workflow)
    db1 = DataLoader()
    #Check if company name is already exist if yes then get records count
    res = db1.validation(comp_name)
    if res <= 0:
        res = db1.rec_cnt
        log.info("DataLoader: Creating a new table for the client: " + comp_name)
        print("DataLoader: Creating a new table for the client: " + comp_name)
        #Workflow check - Hospital flow or Employee flow and switch to the corresponding block of code
        if (workflow.upper() == "EMPLOYEE"):
            status1 = db1.employee_create_schema(comp_name)
            if status1 == 0:
                log.info("DataLoader: A new Table created successfully: " + db1.KEYSPACE + "." + comp_name)
                print("DataLoader: A new Table created successfully: " + db1.KEYSPACE + "." + comp_name)
                log.info("DataLoader: Progressing initial data loading.......")
                print("DataLoader: Progressing initial data loading.......")
                status2 = db1.employee_data_gen(comp_name)
                if status2 == 0:
                    log.info("DataLoader: Data Loaded Successfully. Total Number of records: " + str(res))
                    print("DataLoader: Data Loaded Successfully. Total Number of records: " + str(res))
                else:
                    log.info("DataLoader ERROR: Failed in Data generation, No records added.")
                    print("DataLoader ERROR: Failed in Data generation, No records added.")
                    raise DLError('DataLoader ERROR - Failed in Data generation, No records added.', 400, { 'ext': 1 })
                # end of if status2 == 0:
            else:
                log.info("DataLoader ERROR: Cassandra failed to create a new table.")
                print("DataLoader Error: Cassandra failed to create a new table.")
                raise DLError('DataLoader ERROR - Cassandra failed to create a new table.', 404, { 'ext': 1 })
            # end of if status1 == 0:
        elif (workflow.upper() == "HOSPITAL"):
            status1 = db1.hospital_create_schema(comp_name)
            if status1 == 0:
                log.info("DataLoader: A new Table created successfully: " + db1.KEYSPACE + "." + comp_name)
                print("DataLoader: A new Table created successfully: " + db1.KEYSPACE + "." + comp_name)
                log.info("DataLoader: Progressing initial data loading.......")
                print("DataLoader: Progressing initial data loading.......")
                status2 = db1.hospital_data_gen(comp_name)
                if status2 == 0:
                    log.info("DataLoader: Data Loaded Successfully. Total Number of records: " + str(res))
                    print("DataLoader: Data Loaded Successfully. Total Number of records: " + str(res))
                else:
                    log.info("DataLoader ERROR: Failed in Data generation, No records added.")
                    print("DataLoader ERROR: Failed in Data generation, No records added.")
                    raise DLError('DataLoader ERROR - Failed in Data generation, No records added.', 400, {'ext': 1})
                # end of if status2 == 0:
            else:
                log.info("DataLoader ERROR: Cassandra failed to create a new table.")
                print("DataLoader Error: Cassandra failed to create a new table.")
                raise DLError('DataLoader ERROR - Cassandra failed to create a new table.', 404, {'ext': 1})
            # end of if status1 == 0:
        else:
            log.info("DataLoader ERROR: Wrong Workflow Name in Request: " + workflow)
            print("DataLoader ERROR: Wrong Workflow Name in Request: " + workflow)
            raise DLError('DataLoader ERROR - Wrong Workflow Name in Request', 404, {'ext': 1})
        #end of if (workflow.upper() == "EMPLOYEE"):
    else:
        log.info("DataLoader: User/Company name already active: " + comp_name)
        print("DataLoader: User/Company name already active: " + comp_name)
    # end of if res <= 0:
    db1.session.shutdown()
    end = timer()
    t = str(timedelta(seconds=end - start))
    dataloader = "start_time:" + str(start) + ", end_time:" + str(end) + ", elapsed_time:" + str(t) + ", records_added:" + str(res)
    raise DLError(dataloader, 200, {'ext': 0})

@app.route("/dataflow_append", methods=['POST', 'GET'])
def dataflow_append():
    start = timer()
    # get request parameters
    req = request.get_json()
    workflow = req["workflow"]
    comp_name = req["client_name"]
    order = list(req["workflow_specification"])
    ip_list = req["ips"]
    data = req["data"]
    res = 0
    status = -1
    # start workflow
    log.info("DataLoader: Starting workflow: " + workflow)
    db1 = DataLoader()
    # Workflow check - Hospital flow or Employee flow and switch to the corresponding block of code
    if (workflow.upper() == "EMPLOYEE"):
        log.info("DataLoader: Appending new data to the table.......")
        print("DataLoader: Appending new data to the table.......")
        status = db1.employee_append_data(comp_name, data)
        if status == 0:
            res = db1.validation(comp_name)
            log.info("DataLoader: new record added Successfully. Total Number of records in table : " + str(res))
            print("DataLoader: new record added Successfully. Total Number of records in table : " + str(res))
        else:
            log.info("DataLoader ERROR: Failed in data addition, No new records added.")
            print("DataLoader ERROR: Failed in data addition, No new records added.")
            raise DLError('DataLoader ERROR - Failed in data addition, No new records added.', 400, {'ext': 1})
        # end of if status == 0:

    elif (workflow.upper() == "HOSPITAL"):
        log.info("DataLoader: Appending new data to the table.......")
        print("DataLoader: Appending new data to the table.......")
        status = db1.hospital_append_data(comp_name, data)
        if status == 0:
            res = db1.validation(comp_name)
            log.info("DataLoader: new record added Successfully. Total Number of records in table : " + str(res))
            print("DataLoader: new record added Successfully. Total Number of records in table : " + str(res))
        else:
            log.info("DataLoader ERROR: Failed in data addition, No new records added.")
            print("DataLoader ERROR: Failed in data addition, No new records added.")
            raise DLError('DataLoader ERROR - Failed in data addition, No new records added.', 400, {'ext': 1})
        # end of if status == 0:
    else:
        log.info("DataLoader ERROR: Wrong Workflow Name in Request: " + workflow)
        print("DataLoader ERROR: Wrong Workflow Name in Request: " + workflow)
        raise DLError('DataLoader ERROR - Wrong Workflow Name in Request', 404, {'ext': 1})
        # end of if (workflow.upper() == "EMPLOYEE"):
    db1.session.shutdown()
    end = timer()
    t = str(timedelta(seconds=end - start))
    msg = "start_time:" + str(start) + ", end_time:" + str(end) + ", elapsed_time:" + str(t) + ", records_added:" + str(res)
    db1.session.shutdown()
    end = timer()
    t = str(timedelta(seconds=end - start))

    #Prepare forward request Json
    for_req = {}
    analytics = [{"start_time": start, "end_time": end, "elapsed_time": t, "record_count": res}]
    for_req["request"] = req
    for_req["analytics"] = analytics
    log.info("DataLoader: Forwarding a request to the next component.......")
    print("DataLoader: Forwarding a request to the next component.......")
    print(for_req)
    # redirect
    for i in order[1]:
        print("order " + str(i))
        FWD_URL = getComponentURL(str(i), ip_list[str(i)])
        if FWD_URL != "http://":
            requests.post(FWD_URL, headers={'content-type': 'application/json'}, json=for_req)
            log.info("DataLoader is forwarding a request to the next component:")
            log.info(FWD_URL)
            print("DataLoader is forwarding a request to the next component:")
            print(FWD_URL)
    raise DLError(msg, 200, {'ext': 0})

#api.add_resource(employee_dataloader, "/employee_dataloader")

if __name__ == "__main__":
  app.run(debug=True, host="0.0.0.0", port=303)
