from flask import Flask, request
import requests
import subprocess
import sys
import time
import json
from flask_cors import CORS
import logging
import threading

app = Flask(__name__)
CORS(app)
@app.route('/')
def index():
    return 'Server is alive!!'


# http://d925019d0b9d.ngrok.io/graph?g0.expr=(1%20-%20avg(irate(node_cpu_seconds_total%7Bmode%3D%22idle%22%2Cinstance%3D~%2210.*%22%7D%5B10m%5D))%20by%20(instance))%20*%20100&g0.tab=0&g0.stacked=0&g0.range_input=1h&g1.expr=100%20*%20(1%20-%20((avg_over_time(node_memory_MemFree_bytes%7Binstance%3D~%2210.*%22%7D%5B1m%5D)%20%2B%20avg_over_time(node_memory_Cached_bytes%7Binstance%3D~%2210.*%22%7D%5B1m%5D)%20%2B%20avg_over_time(node_memory_Buffers_bytes%7Binstance%3D~%2210.*%22%7D%5B1m%5D))%20%2F%20avg_over_time(node_memory_MemTotal_bytes%7Binstance%3D~%2210.*%22%7D%5B1m%5D)))&g1.tab=0&g1.stacked=0&g1.range_input=1h&g2.expr=sum(irate(node_network_transmit_bytes_total%7Binstance%3D~%2210.*%22%2C%20device!~%22lo%7Cbr.*%7Cveth.*%22%7D%5B1m%5D))%20by%20(instance)&g2.tab=0&g2.stacked=0&g2.range_input=1h&g3.expr=sum(irate(node_network_receive_bytes_total%7Binstance%3D~%2210.*%22%2C%20device!~%22lo%7Cbr.*%7Cveth.*%22%7D%5B1m%5D))%20by%20(instance)&g3.tab=0&g3.stacked=0&g3.range_input=1h
# CPU
# (1 - avg(irate(node_cpu_seconds_total{mode="idle",instance=~"10.*"}[10m])) by (instance)) * 100
# Memory
# 100 * (1 - ((avg_over_time(node_memory_MemFree_bytes{instance=~"10.*"}[1m]) + avg_over_time(node_memory_Cached_bytes{instance=~"10.*"}[1m]) + avg_over_time(node_memory_Buffers_bytes{instance=~"10.*"}[1m])) / avg_over_time(node_memory_MemTotal_bytes{instance=~"10.*"}[1m])))
# Network
# sum(irate(node_network_transmit_bytes_total{instance=~"10.*", device!~"lo|br.*|veth.*"}[1m])) by (instance)
# sum(irate(node_network_receive_bytes_total{instance=~"10.*", device!~"lo|br.*|veth.*"}[1m])) by (instance)

# sudo docker service ls --format {{.ID}}  | while read line ; do sudo docker service ps $line | sed '1 d'; done;
# sudo docker service ls --format {{.ID}}  | while read line ; do sudo docker service rm $line; done;
# command="docker service create --restart-condition=none -qd --name " + name + " busybox sleep 100"
#
# /opt/cassandra/bin/cqlsh
# SELECT * FROM system_schema.keyspaces;
# SELECT * FROM system_schema.tables WHERE keyspace_name = 'ccproj_db';
# DELETE FROM system_schema.tables WHERE keyspace_name = 'ccproj_db' AND table_name = 'client1';

# sudo docker service ls --format {{.ID}}  | while read line ; do sudo docker service ps $line -f desired-state=Running | sed '1 d';done;
# sudo docker node ps -f desired-state=running managernode
ipList = {'managernode': '10.176.67.91', 'workernode2':'10.176.67.93', 'workernode1':'10.176.67.92'}

workflowInstances = {
    'employee': dict(), 
    'hospital': dict(), 
    'ipsHashMap' : {'employee': dict(), 'hospital': dict()}
    }

noReuseWorkflowInstances = {'employee': dict(), 'hospital': dict()}

isDbAlreadyLaunched = False

def isDBRunning():
    global isDbAlreadyLaunched
    return isDbAlreadyLaunched

def setDBToRunning():
    global isDbAlreadyLaunched
    isDbAlreadyLaunched = True

currAnalystPorCount=-1
anaylstPortList = [12340,12341,12342,12343,12344,12345,12346,12347,12348,12349]
def getAnalystPort():
    global currAnalystPorCount
    currAnalystPorCount = currAnalystPorCount + 1
    return str(anaylstPortList[currAnalystPorCount])

glPort = 4000
def getPort():
    global glPort
    glPort = glPort + 1
    return str(glPort)

@app.route('/noreuse/request', methods=['GET', 'POST'])
def initWorkflow1():
    jsonContent = request.get_json()
    workflow = jsonContent["workflow"]
    client_name = jsonContent["client_name"]
    seq = jsonContent["workflow_specification"]
    
    if clientExists(workflow, client_name):
        logging.warning("Deployment request is already done for " + workflow + ":" + client_name)
        return "FAILED\n"
    
    if (workflow == "employee") | (workflow == "hospital"):
        noReuseWorkflowInstances[workflow][client_name] = dict()
        for i, li in enumerate(seq):
            for j, item in enumerate(li):
                if (launchContainer(item, workflow ,client_name, getPort(),"NOREUSE") == False):
                    killContainers(workflow ,client_name,seq, i, j,"REUSE")
                    noReuseWorkflowInstances[workflow].pop(client_name,None)
                    logging.warning(noReuseWorkflowInstances)
                    return "FAILED\n"
        launchAnalyst("4", workflow, client_name, getAnalystPort(),"NOREUSE")
        noReuseWorkflowInstances[workflow][client_name]["seq"] = seq
        makeAPIRequestToSendIPs(workflow,client_name, seq,"NOREUSE")
        logging.warning("Internal noreuse HashMap: " + json.dumps(noReuseWorkflowInstances))
        return noReuseWorkflowInstances[workflow][client_name]["4"]
    else:
        logging.warning("Invalid workflow specified")
        return "FAILED\n"

@app.route('/reuse/request', methods=['GET', 'POST'])
def initWorkflow2():
    jsonContent = request.get_json()
    workflow = jsonContent["workflow"]
    client_name = jsonContent["client_name"]
    seq = jsonContent["workflow_specification"]

    if clientExists(workflow, client_name):
        logging.warning("Deployment request is already done for " + workflow + ":" + client_name)
        return "FAILED\n"
    
    if (workflow == "employee") | (workflow == "hospital"):
        if len(workflowInstances['ipsHashMap'][workflow]) == 0:
            workflowInstances[workflow][client_name] = dict()
            for i, li in enumerate(seq):
                for j, item in enumerate(li):
                    if (launchContainer(item, workflow ,client_name, getPort(), "REUSE") == False):
                        killContainers(workflow ,client_name,seq, i, j,"REUSE")
                        workflowInstances[workflow].pop(client_name,None)
                        workflowInstances['ipsHashMap'][workflow] = dict()
                        logging.warning(workflowInstances)
                        return "FAILED\n"
            launchAnalyst("4", workflow, client_name, getAnalystPort(),"REUSE")
            workflowInstances[workflow][client_name]["seq"] = seq
            makeAPIRequestToSendIPs(workflow,client_name, seq,"REUSE")
        else: 
            workflowInstances[workflow][client_name] = dict()
            launchAnalyst("4", workflow, client_name, getAnalystPort(),"REUSE")
            workflowInstances[workflow][client_name]["seq"] = seq
            if makeAPIRequestToTrain(seq,workflow,client_name,"REUSE") == False:
                KillAnalyst(workflow, client_name,"REUSE")
                workflowInstances[workflow].pop(client_name,None)
                logging.warning(workflowInstances)
                return "FAILED\n"
        logging.warning("Internal reuse HashMap: " + json.dumps(workflowInstances))
        return workflowInstances[workflow][client_name]["4"]
    else:
        logging.warning("Invalid workflow specified")
        return "FAILED\n"

def clientExists(workflow, client_name):
    if client_name in workflowInstances[workflow]:
        return True
    if client_name in noReuseWorkflowInstances[workflow]:
        return True
    return False

def makeAPIRequestToSendIPs(request,wflowname,seq,deploymentType):
    pd = formData(request, wflowname,deploymentType)
    analyticsArr = []
    flat_list = [item for sublist in seq for item in sublist]
    for i,component in enumerate(flat_list):
        if component == "1":
            logging.warning("Making API request for sending IPs to Component DL Data: " + json.dumps(pd))
            resp = requests.post(url = "http://" + getIP(request,wflowname,"1",deploymentType) + "/dataloader", headers={'content-type': 'application/json'}, json = pd)
            if resp.status_code == 200:
                analyticsArr.append(resp.json())
            else:
                logging.warning("Error: API response code is NON-200 when sending IP's to dataloader")
        elif component == "2":
            logging.warning("Making API request for sending IPs to Component LR Data: " + json.dumps(pd))
            resp = requests.post(url = "http://" + getIP(request,wflowname,"2",deploymentType) + "/lgr/ipwfspec", headers={'content-type': 'application/json'}, json = pd)
            if resp.status_code == 200:
                analyticsArr.append(resp.json())
            else:
                logging.warning("Error: API response code is NON-200 when sending IP's to Logistic regression") 
        elif component == "3":
            logging.warning("Making API request for sending IPs to Component SVM Data: " + json.dumps(pd))
            resp = requests.post(url = "http://" + getIP(request,wflowname,"3",deploymentType) + "/svm/append-ip", headers={'content-type': 'application/json'}, json = pd)
            if resp.status_code == 200:
                analyticsArr.append(resp.json())
            else:
                logging.warning("Error: API response code is NON-200 when sending IP's to SVM")
    pd["analytics"] = analyticsArr
    pd["requestID"] = 1
    logging.warning("Making API request to send details to Analytics component with Data: " + json.dumps(pd))
    requests.post(url = "http://" + getIP(request,wflowname,"4",deploymentType) + "/put_result", headers={'content-type': 'application/json'}, json = pd)

def getIP(request,wflowname,item,deploymentType):
    if deploymentType == "REUSE":
        if item == "1":
            return workflowInstances['ipsHashMap'][request]["1"]
        elif item == "2":
            return workflowInstances['ipsHashMap'][request]["2"]
        elif item == "3":
            return workflowInstances['ipsHashMap'][request]["3"]
        elif item == "4":
            return workflowInstances[request][wflowname]["4"]
    else:
        if item == "1":
            return noReuseWorkflowInstances[request][wflowname]["1"]
        elif item == "2":
            return noReuseWorkflowInstances[request][wflowname]["2"]
        elif item == "3":
            return noReuseWorkflowInstances[request][wflowname]["3"]
        elif item == "4":
            return noReuseWorkflowInstances[request][wflowname]["4"]

def makeAPIRequestToTrain(seq,request,wflowname,deploymentType):
    pd = formData(request, wflowname,deploymentType)
    analyticsArr = []
    for i, li in enumerate(seq):
        for j, item in enumerate(li):
            if (item == "1"):
                logging.warning("Making API request for start training to Component DL Data: " + json.dumps(pd))
                resp = requests.post(url = "http://" + getIP(request,wflowname,"1",deploymentType) + "/dataloader", headers={'content-type': 'application/json'}, json = pd)
                # resp = requests.post(url = 'https://c0cbf714b395e1ce81b2f6c804ee2b35.m.pipedream.net', headers={'content-type': 'application/json'}, json = pd)
                if resp.status_code != 200:
                    logging.warning("Error: API response code is NON-200 when training request made to dataloader")
                    return False
                analyticsArr.append(resp.json())
            elif (item == "2"):
                logging.warning("Making API request for start training to Component LR Data: " + json.dumps(pd))
                resp = requests.post(url = "http://" + getIP(request,wflowname,"2",deploymentType) + "/lgr/train", headers={'content-type': 'application/json'}, json = pd)
                if resp.status_code != 200:
                    logging.warning("Error: API response code is NON-200 when training request made to logistic regression")
                    return False
                analyticsArr.append(resp.json())
            elif (item == "3"):
                logging.warning("Making API request for start training to Component SVM Data: " + json.dumps(pd))
                resp = requests.post(url = "http://" + getIP(request,wflowname,"3",deploymentType) + "/svm/train", headers={'content-type': 'application/json'}, json = pd)
                if resp.status_code != 200:
                    logging.warning("Error: API response code is NON-200 when training request made to SVM")
                    return False
                analyticsArr.append(resp.json())
    pd["analytics"] = analyticsArr
    pd["requestID"] = 1
    logging.warning("Making API request for start training to Analytics component with Data: " + json.dumps(pd))
    requests.post(url = "http://" + getIP(request,wflowname,"4",deploymentType) + "/put_result", headers={'content-type': 'application/json'}, json = pd)
    return True

def formData(request,wflowname,deploymentType):
    data = dict()
    data['client_name'] = wflowname
    data['workflow'] = request
    if deploymentType == "NOREUSE":
        data['workflow_specification'] = noReuseWorkflowInstances[request][wflowname]["seq"]
        data['ips'] = noReuseWorkflowInstances[request][wflowname]
    else: 
        data['workflow_specification'] = workflowInstances[request][wflowname]["seq"]
        data['ips'] = workflowInstances['ipsHashMap'][request]
        data['ips']['4'] = workflowInstances[request][wflowname]["4"]
    return data

def killContainers(request,wflowname,seq, k, l,deploymentType):
    for i in range(k+1):
        for j in range(len(seq[i])):
            if i == k & j == l:
                killContainer(request,wflowname,seq[i][j],deploymentType)
                break
            killContainer(request,wflowname,seq[i][j],deploymentType)

def killContainer(request,wflowname,item,deploymentType):
    if (item == "1"):
        return KillDBAndDL(request,wflowname,deploymentType)
    elif (item == "2"):
        return KillLR(request,wflowname,deploymentType)
    elif (item == "3"):
        return KillSVM(request,wflowname,deploymentType)
    elif (item == "4"):
        return KillAnalyst(request,wflowname,deploymentType)

def KillDBAndDL(request,wflowname,deploymentType):
    # TODO
    # subprocess.Popen("docker service rm db", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    name = getContainerName("dataloader",request,wflowname,deploymentType)
    subprocess.Popen("docker service rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def KillLR(request,wflowname,deploymentType):
    name = getContainerName("logisticregression",request,wflowname,deploymentType)
    subprocess.Popen("docker service rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def KillSVM(request,wflowname,deploymentType):
    name = getContainerName("svm",request,wflowname,deploymentType)
    subprocess.Popen("docker service rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def KillAnalyst(request,wflowname,deploymentType):
    name = getContainerName("analyst",request,wflowname,deploymentType)
    subprocess.Popen("docker stop "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.Popen("docker rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def launchContainer(item,request,wflowname, port, deploymentType):
    if (item == "1"):
        return launchDBAndDL(item,request,wflowname, port, deploymentType)
    elif (item == "2"):
        return launchLR(item,request,wflowname, port, deploymentType)
    elif (item == "3"):
        return launchSVM(item,request,wflowname, port, deploymentType)

def launchDBAndDL(item,request,wflowname, port, deploymentType):

    if isDBRunning() == False:
        command = "docker stack deploy -c cassandra-compose.yml cassandra"
        logging.warning("Initializing Cassandra database, waiting for 90sec..")
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()
        time.sleep(90)
        logging.warning("Cassandra database initialization is done.")
        setDBToRunning()
    else:
        logging.warning("Cassandra database initialization is already done.")
    
    name = getContainerName("dataloader",request,wflowname,deploymentType)
    command ="docker service create -p " + port + ":303" + " --restart-condition=none -qd --env workflow=" + request + " --env client_name=" + wflowname + " --name " + name + " ayutiwari/data_loader:2.0"
    logging.warning("Initializing data loader, waiting for 20sec..")
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        logging.warning("Error while running command: '{}'. Error: '{}'".format(command, error.decode()))
        logging.warning("Above command output: " + output.decode())
        return False
    time.sleep(20)
    if (checkStatus(item,request,wflowname,output.decode(), port, deploymentType) == False):
        logging.warning("Error while running command: '{}'. Error: '{}'".format(command, error.decode()))
        logging.warning("Above command output: " + output.decode())
        return False
    logging.warning("Data loader initialization is done.")
    return True

def launchLR(item,request,wflowname, port, deploymentType):
    name = getContainerName("logisticregression",request,wflowname,deploymentType)
    logging.warning("Initializing Logistic regression model, waiting for 20sec..")
    command ="docker service create -p " + port + ":50" + " --restart-condition=none -qd --env workflow=" + request + " --env client_name=" + wflowname + " --name " + name + " gaderut/cc4_lgr:1.0"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        logging.warning("Error while running command: '{}'. Error: '{}'".format(command, error))
        logging.warning("Above command output: " + output.decode())
        return False
    time.sleep(20)
    if (checkStatus(item,request,wflowname,output.decode(), port,deploymentType) == False):
        logging.warning("Error while running command: '{}'. Error: '{}'".format(command, error.decode()))
        logging.warning("Above command output: " + output.decode())
        return False
    logging.warning("Logistic regression model initialization is done.")
    return True

def launchSVM(item,request,wflowname, port, deploymentType):
    name = getContainerName("svm",request,wflowname,deploymentType)
    logging.warning("Initializing SVM model, waiting for 20sec..")
    command ="docker service create -p " + port + ":8765" + " --restart-condition=none -qd --env workflow=" + request + " --env client_name=" + wflowname + " --name " + name + " conradyen/svm-component"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        logging.warning("Error while running command: '{}'. Error: '{}'".format(command, error.decode()))
        logging.warning("Above command output: " + output.decode())
        return False
    time.sleep(20)
    if (checkStatus(item,request,wflowname,output.decode(), port,deploymentType) == False):
        logging.warning("Error while running command: '{}'. Error: '{}'".format(command, error.decode()))
        logging.warning("Above command output: " + output.decode())
        return False
    logging.warning("SVM model initialization is done.")
    return True

def launchAnalyst(item,request,wflowname, port, deploymentType):
    name = getContainerName("analyst",request,wflowname,deploymentType)
    logging.warning("Initializing Analyst, waiting for 20sec..")
    command = "docker run -d -p "+port+":12345" + " --name " + name + " --env WORKFLOW=" + request + " --env CLIENT_NAME=" + wflowname+ " conradyen/analytics"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        logging.warning("Error while running command: '{}'. Error: '{}'".format(command, error.decode()))
        logging.warning("Above command output: " + output.decode())
        return False
    time.sleep(20)
    if (containerExist(name) == False):
        return False
    if deploymentType == "NOREUSE":
        noReuseWorkflowInstances[request][wflowname][item] = "10.176.67.91:"+port
    else:
        workflowInstances[request][wflowname][item] = "10.176.67.91:"+port
    logging.warning("Analyst initialization is done.")
    return True

def getContainerName(suffix, request,wflowname,deploymentType):
    if suffix == "analyst":
        return request+"-"+wflowname+"-"+suffix

    if deploymentType == "REUSE":
        return request+"-"+suffix

    return request+"-"+wflowname+"-"+suffix

def checkStatus(item,request,wflowname,uid, port, deploymentType):
    command = 'sudo docker service ps --format {{.Node}},{{.DesiredState}} ' + uid
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        logging.warning("Error during checkStatus command: '{}', Error: '{}'".format(command, error.decode()))
        logging.warning("Above command output: " + output.decode())
        return False
    res = output.decode().strip().split(",")
    if (res[1].lower() == "running"):
        if deploymentType == "NOREUSE":
            noReuseWorkflowInstances[request][wflowname][item] = ipList[res[0]]+":"+port
        else:
            workflowInstances['ipsHashMap'][request][item] = ipList[res[0]]+":"+port
        return True
    logging.warning("Error component is not running: '{}'".format(output.decode()))
    return False

def containerExist(containerName):
    bashCommand = "docker ps --filter name={}| sed '1 d'".format(containerName)
    process = subprocess.Popen(bashCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        logging.warning("Error analyst did not launch for: " + containerName)
        logging.warning("Error: " + error.decode())
        logging.warning("Output: " + output.decode())
        return False
    if (len(output) > 0):
        return True
    logging.warning("Error analyst did not launch for: " + containerName)
    return False

@app.route('/predict', methods=['GET', 'POST'])
def predict():
    jsonContent = request.get_json()
    workflow = jsonContent["workflow"]
    client_name = jsonContent["client_name"]
    logging.warning("Making prediction request to Dataloader with data: " + json.dumps(jsonContent))
    
    url = ""
    if client_name in workflowInstances[workflow]:
        url = workflowInstances['ipsHashMap'][workflow]["1"]
    elif client_name in noReuseWorkflowInstances[workflow]:
        url = noReuseWorkflowInstances[workflow][client_name]["1"]
    else:
        logging.warning("Error: There is not Deployment found for " + workflow + ":" + client_name)
        return ""
    requests.post(url = "http://" + url + "/dataflow_append", headers={'content-type': 'application/json'}, json = jsonContent)
    return "Request forwarded to Data loader"

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s  -->  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    app.run(host="0.0.0.0", port=5000)