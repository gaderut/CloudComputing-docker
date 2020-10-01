import sys
import logging as log
from cassandra.cluster import Cluster

#Workflow name

workflow=sys.argv[1] 

if workflow.upper() not in [ "EMPLOYEE", "HOSPITAL" ]:
    print("Unknown workflow. Please check the workflow name and try again.")
    exit()

#Connection configuration

KEYSPACE = "ccproj_db"
cluster = Cluster(['10.176.67.91']) #Cluster(['0.0.0.0'], port=9042) #Cluster(['10.176.67.91'])
session = cluster.connect()

# csv file location
file_path= "./csv/"+workflow.lower()+"_data_file.csv"

log.info("setting DB keyspace . . .")
session.set_keyspace(KEYSPACE)

if (workflow.upper() == "EMPLOYEE"):
    print(workflow + " WORKFLOW DATA LOADING.......................................")
    insert_q=session.prepare("""INSERT INTO employee (UU_ID, EMP_ID, DEPT_TYPE, GENDER, RACE, DAY_OF_WEEK, CHECKIN_DATETIME, DURATION)
                            VALUES (now(), ?, ?, ?, ?, ?, ?, ?)""")

#load data
    with open(file_path, "r") as file_ptr:
        for i in file_ptr:
            columns=i.split(",")
            empid=columns[0]
            dept=columns[1]
            gender=columns[2]
            race=columns[3]
            day=columns[4]
            checkin=columns[5]
            duration=columns[6].replace("\n",'')

            session.execute(insert_q, [empid,dept,gender,race,day,checkin,duration])
         
else:
     print(workflow + " WORKFLOW DATA LOADING.......................................")
     insert_q=session.prepare("""INSERT INTO hospital (UU_ID, P_ID, GENDER, RACE, WARD, BED_NUM, ADM_DATETIME, DIS_DATETIME)
                            VALUES (now(), ?, ?, ?, ?, ?, ?, ?)""")
#load data
     with open(file_path, "r") as file_ptr:
           for i in file_ptr:
            columns=i.split(",")
            p_id=columns[0]
            gender=columns[1]
            race=columns[2]
            ward=columns[3]
            bed_no=columns[4]
            admit_date=columns[5]
            dis_date=columns[6].replace("\n",'')

            session.execute(insert_q, [p_id,gender,race,ward,bed_no,admit_date,dis_date])


print("--------------------- " + workflow + " WORKFLOW DATA LOADED SUCCESSFULLY.----------------------")
#closing the file
file_ptr.close()

#closing Cassandra connection
session.shutdown()
