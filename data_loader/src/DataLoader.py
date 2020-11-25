import requests, sys, os
import logging as log, traceback
import random
from flask import Flask, request, jsonify
from flask_restful import Api
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
import time

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
api = Api(app)
log.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
#HashMap
emp_dict = {}
hosp_dict = {}
first_call_result= {}
##################################################
def updateHashMap(wf, comp, order, ip_list):
    h_key = {"order": order, "ips": ip_list}
    if (wf.upper() == "EMPLOYEE"):
        emp_dict[comp] = h_key
    if (wf.upper() == "HOSPITAL"):
        hosp_dict[comp] = h_key

def getComponentURL(n, ip_add):
    url_string = "http://"
    if n == "2":
        url_string = url_string + ip_add + "/lgr/predict"
    elif n == "3":
        url_string = url_string + ip_add + "/svm/predict"
    elif n == "4":
        url_string = url_string + ip_add + "/put_result"
    else:
        msg = "DataLoader Error getComponentURL: Unable to find the service component, no Service component found with code: " + n
        log.info(msg)
    return url_string

#################################################
class DataLoader:
    KEYSPACE=0
    cluster=0
    session=0
    rec_cnt=2000
    def __init__(self):
        self.KEYSPACE = "ccproj_db"
        self.cluster = Cluster(contact_points=['10.176.67.91'], load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'), port=9042, protocol_version=3)
        self.session = self.cluster.connect()
        self.session.set_keyspace(self.KEYSPACE)

    def validation(self, usr_name):
        result = 0
        qry = "SELECT COUNT(*) FROM " + usr_name + " ALLOW FILTERING;"
        try:
            stat = self.session.prepare(qry)
            x = self.session.execute(stat)
            for row in x:
                result = row.count
        except Exception as e:
            result = -1
            log.info("DataLoader validation: No Table found in Cassandra database.")
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
        except Exception as e:
            log.error(traceback.format_exc())
            status = -1
            self.session.shutdown()
        return status

    def employee_data_gen(self, usr_name):
        # define constants
        status = 0
        dept1 = ["1", "2", "3", "4", "5", "6"]
        gender1 = ["male", "female"]
        race1 = ["1", "2", "3", "4", "5", "6"]
        day1 = ["MON", "TUE", "WED", "THU", "FRI"]
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
        except Exception as e:
            log.error(traceback.format_exc())
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
        except Exception as e:
            log.error(traceback.format_exc())
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
                                            duration   TEXT,
                                            num_in_icu DECIMAL,
                                            amount DECIMAL,
                                            rate DECIMAL,
                                            total_items DECIMAL,
                                            value DECIMAL,
                                            dilution_value DECIMAL,
                                            abnormal_count DECIMAL,
                                            item_distinct_abnormal DECIMAL,
                                            checkin_datetime TEXT,
                                            day_of_week TEXT);"""
        try:
            cre_tbl = self.session.prepare(stat1)
            self.session.execute(cre_tbl)
        except Exception as e:
            log.error(traceback.format_exc())
            status = -1
            self.session.shutdown()
        return status

    # hospital Data generator
    def hospital_data_gen(self, usr_name):
        # define constants
        status = 0
        hospital_expire_flag1 = [1, 2]
        insurance1 = [1, 2, 3, 4, 5]
        day1 = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
        duration1 = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24"]

        checkin_time1 = ["00:00", "00:30", "01:00", "01:30", "02:00", "02:30", "03:00", "03:30", "04:00", "04:30", "05:00", "05:30", "06:00",
                         "06:30","07:00", "07:30", "08:00", "08:30", "09:00", "09:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30"]
        # Insert query format
        ins_stat = "INSERT INTO " + usr_name + """ (uu_id, hadm_id, hospital_expire_flag, insurance, duration, num_in_icu,
                                                               amount, rate, total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_datetime, day_of_week)
                                                               VALUES (now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        try:
            insert_q = self.session.prepare(ins_stat)
        #generate data
            for i in range(1000, self.rec_cnt + 1000):
                hadm_id = i
                hospital_expire_flag = random.choice(hospital_expire_flag1)
                insurance = random.choice(insurance1)
                duration = random.choice(duration1)
                num_in_icu = random.randint(1, 40)
                amount = random.uniform(-0.013434843, 1)
                rate = random.uniform(0, 1)
                total_items = random.uniform(0.093117409, 1)
                value = random.uniform(0.000110609, 1)
                dilution_value = random.uniform(0, 1)
                abnormal_count = random.uniform(0.001908852, 1)
                item_distinct_abnormal = random.uniform(0.059405941, 1)
                checkin_datetime = random.choice(checkin_time1)
                day_of_week = random.choice(day1)
                self.session.execute(insert_q,
                             [hadm_id, hospital_expire_flag, insurance, duration, num_in_icu, amount, rate,
                             total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_datetime, day_of_week])
        except Exception as e:
            log.error(traceback.format_exc())
            status = -1
            self.session.shutdown()
        return status

    def hospital_append_data(self, usr_name, data):
        # define constants
        status = 0
        # Insert query format
        ins_stat = "INSERT INTO " + usr_name + """ (uu_id, hadm_id, hospital_expire_flag, insurance, duration, num_in_icu,
                                                                       amount, rate, total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_datetime, day_of_week)
                                                                       VALUES (now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        try:
            insert_q = self.session.prepare(ins_stat)
            #generate data
            hadm_id = data["hadm_id"]
            hospital_expire_flag = data["hospital_expire_flag"]
            insurance = data["insurance"]
            duration = data["duration"]
            num_in_icu = data["num_in_icu"]
            amount = data["amount"]
            rate = data["rate"]
            total_items = data["total_items"]
            value = data["value"]
            dilution_value = data["dilution_value"]
            abnormal_count = data["abnormal_count"]
            item_distinct_abnormal = data["item_distinct_abnormal"]
            checkin_datetime = data["checkin_datetime"]
            day_of_week = data["day_of_week"]
            self.session.execute(insert_q,
                             [hadm_id, hospital_expire_flag, insurance, duration, num_in_icu, amount, rate,
                             total_items, value, dilution_value, abnormal_count, item_distinct_abnormal, checkin_datetime, day_of_week])
        except Exception as e:
            log.error(traceback.format_exc())
            status = -1
            self.session.shutdown()
        return status

    def __del__(self):
        self.session.shutdown()
        log.info("Object Destroyed")

@app.route("/dataloader", methods=['POST', 'GET'])
def dataloader():
    start = time.time()
    #get request parameters
    req = request.get_json()
    workflow = req["workflow"]
    comp_name = req["client_name"]
    order = list(req["workflow_specification"])
    ip_list = req["ips"]
    log.info("*********** DATALOADER API REQUEST PARAMETERS *************")
    log.info("workflow = " + workflow)
    log.info("comp_name = " + comp_name)
    log.info("workflow_specification = " + str(order))
    log.info("ips = " + str(ip_list))
    if len(order) == 1: order.append(["4"])
    res = 0
    status1 = status2 = -1
    #start workflow
    log.info("DataLoader - Starting workflow: " + workflow)
    db1 = DataLoader()
    #Check if company name is already exist if yes then get records count
    tbl_name = workflow + "_" + comp_name
    res = db1.validation(tbl_name)
    if res <= 0:
        res = db1.rec_cnt
        log.info("DataLoader - Creating a new table for the client: " + comp_name)
        #Workflow check - Hospital flow or Employee flow and switch to the corresponding block of code
        if (workflow.upper() == "EMPLOYEE"):
            status1 = db1.employee_create_schema(tbl_name)
            if status1 == 0:
                log.info("DataLoader - A new Table created successfully: " + db1.KEYSPACE + "." + tbl_name)
                log.info("DataLoader - Progressing initial data loading.......")
                status2 = db1.employee_data_gen(tbl_name)
                if status2 == 0:
                    log.info("DataLoader - Data Loaded Successfully. Total Number of records: " + str(res))
                else:
                    msg = {"DataLoader employee_data_gen ERROR": "Failed in Data generation, No records added."}
                    log.info(msg)
                    return jsonify(dataloader), 400
                # end of if status2 == 0:
            else:
                msg = {"DataLoader employee_create_schema ERROR": "Cassandra failed to create a new table."}
                log.info(msg)
                return jsonify(dataloader), 404
            # end of if status1 == 0:
        elif (workflow.upper() == "HOSPITAL"):
            status1 = db1.hospital_create_schema(tbl_name)
            if status1 == 0:
                log.info("DataLoader - A new Table created successfully: " + db1.KEYSPACE + "." + tbl_name)
                log.info("DataLoader - Progressing initial data loading.......")
                status2 = db1.hospital_data_gen(tbl_name)
                if status2 == 0:
                    log.info("DataLoader - Data Loaded Successfully. Total Number of records: " + str(res))
                else:
                    msg = {"DataLoader hospital_data_gen ERROR": "Failed in Data generation, No records added."}
                    log.info(msg)
                    return jsonify(dataloader), 400
                # end of if status2 == 0:
            else:
                msg = {"DataLoader hospital_create_schema ERROR": "Cassandra failed to create a new table."}
                log.info(msg)
                return jsonify(dataloader), 404
            # end of if status1 == 0:
        else:
            msg = {"DataLoader ERROR": "Wrong Workflow Name in Request - " + workflow}
            log.info(msg)
            return jsonify(msg), 404
        #end of if (workflow.upper() == "EMPLOYEE"):
    else:
        log.info("DataLoader - User/Company name already active: " + comp_name)
    # end of if res <= 0:

    db1.session.shutdown()

    #update hashmap with workflow
    updateHashMap(workflow, comp_name, order, ip_list)
    log.info("DataLoader - HashMap Update complete for the new Key: (" + workflow + ", " + comp_name + ")")
    if (workflow.upper() == "EMPLOYEE"):
        log.info(str(emp_dict[comp_name]))
    if (workflow.upper() == "HOSPITAL"):
        log.info(str(hosp_dict[comp_name]))

    end = time.time()
    t = end - start
    if (first_call_result["comp_name"] == comp_name) and (first_call_result["workflow"] == workflow):
        dataloader = first_call_result["dataloader"]
    else:
        dataloader = {"start_time": start, "end_time": end, "elapsed_time": t, "records_added": res}
    return jsonify(dataloader), 200

@app.route("/dataflow_append", methods=['POST', 'GET'])
def dataflow_append():
    start = time.time()
    # get request parameters
    req = request.get_json()
    workflow = req["workflow"]
    comp_name = req["client_name"]
    data = req["data"]
    log.info("*********** DATALOADER APPEND API REQUEST PARAMETERS *************")
    log.info("workflow = " + workflow)
    log.info("comp_name = " + comp_name)
    log.info("data = " + str(data))
    res = 0
    status = -1
    tbl_name = workflow + "_" + comp_name
    # start workflow
    log.info("DataLoader Append - Starting data addition process for: " + workflow)
    db1 = DataLoader()
    # Workflow check - Hospital flow or Employee flow and switch to the corresponding block of code
    if (workflow.upper() == "EMPLOYEE"):
        #get the workflow and list
        order = emp_dict[comp_name]["order"]
        ip_list = emp_dict[comp_name]["ips"]
        log.info("DataLoader Append - Appending new data to the table.......")
        status = db1.employee_append_data(tbl_name, data)
        if status == 0:
            res = db1.validation(tbl_name)
            log.info("DataLoader Append - New record added Successfully. Total Number of records in table : " + str(res))
        else:
            msg = {"DataLoader Append employee_append_data ERROR": "Failed in data addition, No new records added."}
            log.info(msg)
            return jsonify(msg), 420
        # end of if status == 0:
    elif (workflow.upper() == "HOSPITAL"):
        # get the workflow and list
        order = hosp_dict[comp_name]["order"]
        ip_list = hosp_dict[comp_name]["ips"]
        log.info("DataLoader Append - Appending new data to the table.......")
        status = db1.hospital_append_data(tbl_name, data)
        if status == 0:
            res = db1.validation(tbl_name)
            log.info("DataLoader Append - New record added Successfully. Total Number of records in table : " + str(res))
        else:
            msg = {"DataLoader Append hospital_append_data ERROR": "Failed in data addition, No new records added."}
            log.info(msg)
            return jsonify(msg), 420
        # end of if status == 0:
    else:
        msg = {"DataLoader Append ERROR": "Wrong Workflow Name in Request - " + workflow}
        log.info(msg)
        return jsonify(msg), 404
        # end of if (workflow.upper() == "EMPLOYEE"):
    db1.session.shutdown()
    end = time.time()
    t = end - start
    #Prepare forward request Json
    for_req = {}
    analytics = [{"start_time": start, "end_time": end, "elapsed_time": t, "record_count": res}]
    msg = {"start_time": start, "end_time": end, "elapsed_time": t, "record_count": res}
    for_req = req
    for_req["analytics"] = analytics
    log.info("DataLoader: Forwarding a request to the next component.......")
    log.info(for_req)
    # redirect
    log.info("DataLoader: Work flow next component list: " + str(order[1]))
    for i in order[1]:
        FWD_URL = getComponentURL(str(i), ip_list[str(i)])
        if FWD_URL != "http://":
                log.info("--------------------------------------------------------------")
                log.info("DataLoader: Calling API for the component: " + str(i))
                log.info(FWD_URL)
                fw_res = requests.post(FWD_URL, headers={'content-type': 'application/json'}, json=for_req, timeout=120)
                if fw_res.status_code != 200:
                    log.info(str(fw_res.json()))
                    log.info("DataLoader ERROR: Fail in API call, requested service is not available: \n" + FWD_URL)
                else:
                    log.info("DataLoader: API call is successful for component :" + str(i))
    return jsonify(msg), 200

def Dataloader_Launch(req):
    start = time.time()
    #get request parameters
    workflow = req["workflow"]
    comp_name = req["client_name"]
    log.info("*********** DATALOADER LAUNCH API REQUEST PARAMETERS *************")
    log.info("workflow = " + workflow)
    log.info("comp_name = " + comp_name)
    res = 0
    status1 = status2 = -1
    #start workflow
    log.info("DataLoader Launch - Starting workflow: " + workflow)
    db1 = DataLoader()
    #Check if company name is already exist if yes then get records count
    tbl_name = workflow + "_" + comp_name
    res = db1.validation(tbl_name)
    if res <= 0:
        res = db1.rec_cnt
        log.info("DataLoader Launch - Creating a new table for the client: " + comp_name)
        #Workflow check - Hospital flow or Employee flow and switch to the corresponding block of code
        if (workflow.upper() == "EMPLOYEE"):
            status1 = db1.employee_create_schema(tbl_name)
            if status1 == 0:
                log.info("DataLoader Launch - A new Table created successfully: " + db1.KEYSPACE + "." + tbl_name)
                log.info("DataLoader Launch - Progressing initial data loading.......")
                status2 = db1.employee_data_gen(tbl_name)
                if status2 == 0:
                    log.info("DataLoader Launch - Data Loaded Successfully. Total Number of records: " + str(res))
                else:
                    msg = {"status": 400, "dataloader": {"Dataloader_Launch ERROR": "employee_data_gen - Failed in Data generation, No records added."}}
                    log.info(msg)
                    return msg
                # end of if status2 == 0:
            else:
                msg = {"status": 404, "dataloader": {"Dataloader_Launch ERROR": "employee_create_schema - Cassandra failed to create a new table."}}
                log.info(msg)
                return msg
            # end of if status1 == 0:
        elif (workflow.upper() == "HOSPITAL"):
            status1 = db1.hospital_create_schema(tbl_name)
            if status1 == 0:
                log.info("DataLoader Launch - A new Table created successfully: " + db1.KEYSPACE + "." + tbl_name)
                log.info("DataLoader Launch - Progressing initial data loading.......")
                status2 = db1.hospital_data_gen(tbl_name)
                if status2 == 0:
                    log.info("DataLoader Launch - Data Loaded Successfully. Total Number of records: " + str(res))
                else:
                    msg = {"status": 400, "dataloader": {"Dataloader_Launch ERROR": "hospital_data_gen - Failed in Data generation, No records added."}}
                    log.info(msg)
                    return msg
                # end of if status2 == 0:
            else:
                msg = {"status": 404, "dataloader": {"Dataloader_Launch ERROR": "hospital_create_schema - Cassandra failed to create a new table."}}
                log.info(msg)
                return msg
            # end of if status1 == 0:
        else:
            msg = {"status": 404, "dataloader": {"Dataloader_Launch ERROR": "Wrong Workflow Name in Request - " + workflow}}
            log.info(msg)
            return msg
        #end of if (workflow.upper() == "EMPLOYEE"):
    else:
        log.info("DataLoader Launch - User/Company name already active: " + comp_name)
    # end of if res <= 0:

    db1.session.shutdown()
    end = time.time()
    t = end - start
    msg = {"status": 200, "workflow": workflow, "comp_name": comp_name, "dataloader": {"start_time": start, "end_time": end, "elapsed_time": t, "records_added": res}}
    return msg

if __name__ == "__main__":
    workflow = os.environ['workflow']
    client_name = os.environ['client_name']
    log.info("DataLoader: initializing the process.......")
    content = {"client_name": client_name, "workflow": workflow}
    with app.app_context():
        st = Dataloader_Launch(content)
        if st["status"] == 200:
            log.info("DataLoader: started successfully.")
            log.info(st)
            first_call_result = st
            app.run(debug=True, host="0.0.0.0", port=303)
        else:
            log.info("DataLoader ERROR: DataLoader Failed to start application")
            log.info(str(st))
            sys.exit(1)
            
