from flask import Flask, request
import requests
import subprocess
import sys
import time
import json

app = Flask(__name__)

@app.route('/')
def index():
    return 'Server is alive!!'

ipList = {'managernode': '10.176.67.91', 'workernode2':'10.176.67.93', 'workernode1':'10.176.67.92'}

workflowInstances = {'employee': dict(), 'hospital': dict()}
ipsHashMap = {'employee': dict(), 'hospital': dict()}

i=-1
anaylstPortList = [12340,12341,12342,12343,12344,12345]
def getAnalystPort():
    global i
    i = i + 1
    return str(anaylstPortList[i])

port = 4000
def getPort():
    global port
    port = port + 1
    return str(port)

@app.route('/request', methods=['GET', 'POST'])
def initWorkflow():
    jsonContent = request.get_json()
    workflow = jsonContent["workflow"]
    client_name = jsonContent["client_name"]
    seq = jsonContent["workflow_specification"]
    if (workflow == "employee") | (workflow == "hospital"):
        if len(ipsHashMap[workflow]) == 0:
            workflowInstances[workflow][client_name] = dict()
            for i, li in enumerate(seq):
                for j, item in enumerate(li):
                    if (launchContainer(item, workflow ,client_name, getPort()) == False):
                        killContainers(workflow ,client_name,seq, i, j)
                        ipsHashMap[workflow] = dict()
                        workflowInstances[workflow].pop(client_name,None)
                        print(workflowInstances)
                        print(ipsHashMap)
                        return "FAILED\n"
            launchAnalyst("4", workflow, client_name, getAnalystPort())
            workflowInstances[workflow][client_name]["seq"] = seq
            makeAPIRequestToSendIPs(workflow,client_name)
        else: 
            if (client_name not in workflowInstances[workflow]):
                workflowInstances[workflow][client_name] = dict()
                launchAnalyst("4", workflow, client_name, getAnalystPort())
                workflowInstances[workflow][client_name]["seq"] = seq
                if makeAPIRequestToTrain(seq,workflow,client_name) == False:
                    KillAnalyst(workflow, client_name)
                    workflowInstances[workflow].pop(client_name,None)
                    print(workflowInstances)
                    print(ipsHashMap)
                    return "FAILED\n"
            else: 
                print("Deployment request is already done")
        print(workflowInstances)
        print(ipsHashMap)
        return "SUCCESS\n"
    else:
        print("Invalid workflow specified")
        return "FAILED\n"

def makeAPIRequestToSendIPs(request,wflowname):
    pd = formData(request, wflowname)
    requests.post(url = "http://" + ipsHashMap[request]["1"] + "/dataloader", headers={'content-type': 'application/json'}, json = pd)
    requests.post(url = "http://" + ipsHashMap[request]["2"] + "/lgr/train", headers={'content-type': 'application/json'}, json = pd)
    requests.post(url = "http://" + ipsHashMap[request]["3"] + "/svm/append-ip", headers={'content-type': 'application/json'}, json = pd)
    # requests.post(url = 'https://a538501a65fe50e9e434be18eb3eaa6e.m.pipedream.net', headers={'content-type': 'application/json'}, json = pd)

# Amazon, Once all the container are launched, Manager SendIPs
# Google, Analyst, Trains and capture IPs

# Prediction

def makeAPIRequestToTrain(seq,request,wflowname):
    pd = formData(request, wflowname)
    analyticsArr = []
    # TODO change paths
    for i, li in enumerate(seq):
        for j, item in enumerate(li):
            if (item == "1"):
                resp = requests.post(url = "http://" + ipsHashMap[request]["1"] + "/dataloader", headers={'content-type': 'application/json'}, json = pd)
                # resp = requests.post(url = 'https://a538501a65fe50e9e434be18eb3eaa6e.m.pipedream.net', headers={'content-type': 'application/json'}, json = pd)
                if resp.status_code != 200:
                    return False
                analyticsArr.append(resp.json())
            elif (item == "2"):
                resp = requests.post(url = "http://" + ipsHashMap[request]["2"] + "/lgr/train", headers={'content-type': 'application/json'}, json = pd)
                # resp = requests.post(url = 'https://a538501a65fe50e9e434be18eb3eaa6e.m.pipedream.net', headers={'content-type': 'application/json'}, json = pd)
                if resp.status_code != 200:
                    return False
                analyticsArr.append(resp.json())
            elif (item == "3"):
                resp = requests.post(url = "http://" + ipsHashMap[request]["3"] + "/svm/train", headers={'content-type': 'application/json'}, json = pd)
                # resp = requests.post(url = 'https://a538501a65fe50e9e434be18eb3eaa6e.m.pipedream.net', headers={'content-type': 'application/json'}, json = pd)
                if resp.status_code != 200:
                    return False
                analyticsArr.append(resp.json())
    pd["analytics"] = analyticsArr
    requests.post(url = "http://" + workflowInstances[request][wflowname]["4"] + "/put_result", headers={'content-type': 'application/json'}, json = pd)
    # requests.post(url = 'https://a538501a65fe50e9e434be18eb3eaa6e.m.pipedream.net', headers={'content-type': 'application/json'}, json = pd)
    return True

def formData(request,wflowname):
    data = dict()
    data['client_name'] = wflowname
    data['workflow'] = request
    data['workflow_specification'] = workflowInstances[request][wflowname]["seq"]
    data['ips'] = ipsHashMap[request]
    data['ips']['4'] = workflowInstances[request][wflowname]["4"]
    return data

def killContainers(request,wflowname,seq, k, l):
    for i in range(k+1):
        for j in range(len(seq[i])):
            if i == k & j == l:
                killContainer(request,wflowname,seq[i][j])
                break
            killContainer(request,wflowname,seq[i][j])

def killContainer(request,wflowname,item):
    if (item == "1"):
        return KillDBAndDL(request,wflowname)
    elif (item == "2"):
        return KillLR(request,wflowname)
    elif (item == "3"):
        return KillSVM(request,wflowname)
    elif (item == "4"):
        return KillAnalyst(request,wflowname)

def KillDBAndDL(request,wflowname):
    # subprocess.Popen("docker service rm db", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    name = request+"-"+wflowname+"-dl"
    subprocess.Popen("docker service rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def KillLR(request,wflowname):
    name = request+"-"+wflowname+"-logisticregression"
    subprocess.Popen("docker service rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def KillSVM(request,wflowname):
    name = request+"-"+wflowname+"-svm"
    subprocess.Popen("docker service rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def KillAnalyst(request,wflowname):
    name = request+"-"+wflowname+"-ana"
    subprocess.Popen("docker service rm "+name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def launchContainer(item,request,wflowname, port):
    if (item == "1"):
        return launchDBAndDL(item,request,wflowname, port)
    elif (item == "2"):
        return launchLR(item,request,wflowname, port)
    elif (item == "3"):
        return launchSVM(item,request,wflowname, port)
    elif (item == "4"):
        return launchAnalyst(item,request,wflowname, port)

def launchDBAndDL(item,request,wflowname, port):
    command = "docker stack deploy -qd -c cassandra-compose.yml cassandra"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    # print("{}, {}".format(output, error))
    print("Initializing Cassandra database, waiting for 90sec..")
    # time.sleep(90)
    print("Cassandra database initialization is done.")
    
    name = request+"-"+wflowname+"-dl"
    command ="docker service create -p " + port + ":303" + " --restart-condition=none -qd --env workflow=" + request + " --env client_name=" + wflowname + " --name " + name + " ayutiwari/data_loader:2.0"
    # command="docker service create --restart-condition=none -qd --name " + name + " busybox sleep 100"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        print("Error while launching data loader. Error: {}".format(error))
        return False
    print("Initializing data loader, waiting for 15sec..")
    time.sleep(5)
    if (checkStatus(item,request,wflowname,output.decode(), port, False) == False):
        print("Error while launching data loader")
        return False
    print("Data loader initialization is done.")
    return True

def launchLR(item,request,wflowname, port):
    name = request+"-"+wflowname+"-logisticregression"
    command ="docker service create -p " + port + ":50" + " --restart-condition=none -qd --env workflow=" + request + " --env client_name=" + wflowname + " --name " + name + " gaderut/cc4_lgr:1.0"
    # command="docker service create --restart-condition=none -qd --name " + name + " busybox sleep 1"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        print("Error while running command {}. Error: {}".format(command, error))
        return False
    print("Initializing Logistic regression model, waiting for 15sec..")
    time.sleep(5)
    if (checkStatus(item,request,wflowname,output.decode(), port, False) == False):
        print("Error while launching Logistic regression model.")
        return False
    print("Logistic regression model initialization is done.")
    return True

def launchSVM(item,request,wflowname, port):
    name = request+"-"+wflowname+"-svm"
    command ="docker service create -p " + port + ":8765" + " --restart-condition=none -qd --env workflow=" + request + " --env client_name=" + wflowname + " --name " + name + " conradyen/svm-component"
    # command="docker service create --restart-condition=none -qd --name " + name + " busybox sleep 100"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        print("Error while running command {}. Error: {}".format(command, error))
        return False
    print("Initializing SVM model, waiting for 15sec..")
    time.sleep(5)
    if (checkStatus(item,request,wflowname,output.decode(), port, False) == False):
        print("Error while launching SVM model.")
        return False
    print("SVM model initialization is done.")
    return True

def launchAnalyst(item,request,wflowname, port):
    name = request+"-"+wflowname+"-ana"
    command = "docker run -d -p "+port+":12345 conradyen/analytics"
    # command="docker service create --restart-condition=none -qd --name " + name + " busybox sleep 100"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    # print("{}, {}".format(output, error))
    print("Initializing Analyst, waiting for 15sec..")
    time.sleep(5)
    workflowInstances[request][wflowname][item] = ":"+port
    print("Analyst initialization is done.")
    return True

def checkStatus(item,request,wflowname,uid, port, isAnalyst):
    command = 'sudo docker service ps --format {{.Node}},{{.DesiredState}} ' + uid
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if not ((error == None) | (error.decode().strip() == "")):
        print("Error during checkStatus: {}".format(error))
        return False
    res = output.decode().strip().split(",")
    if (res[1].lower() == "running"):
        if (isAnalyst == False):
            ipsHashMap[request][item] = ipList[res[0]]+":"+port
        else:
            workflowInstances[request][wflowname][item] = ipList[res[0]]+":"+port
        return True
    return False

def containerExist1(containerName):
    bashCommand = "docker ps --filter name={}| sed '1 d'".format(containerName)
    process = subprocess.Popen(bashCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if (error != None):
        print("Error while calling containerExist for {}. Error: {}".format(containerName, error))
        sys.exit()
    if (len(output) > 0):
        return True
    return False

def containerExist(containerName):
    bashCommand = "docker service ls --filter name={}| sed '1 d'".format(containerName)
    process = subprocess.Popen(bashCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if (error != None):
        print("Error while calling containerExist for {}. Error: {}".format(containerName, error))
        sys.exit()
    if (len(output) > 0):
        return True
    return False

@app.route('/predict', methods=['GET', 'POST'])
def predict():
    jsonContent = request.get_json()
    workflow = jsonContent["workflow"]
    client_name = jsonContent["client_name"]
    requests.post(url = "http://" + workflowInstances[workflow][client_name]["1"] + "/dataloader_append", headers={'content-type': 'application/json'}, json = jsonContent)
    return "Request forwarded to Data loader"

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)



# Amazon [1] [2] [3] Manager will launch the containers 'docker service  --env client_name=amazon workflow=employee'and then send ips(API1) to DL
# Google [3] [2] [1] Manger will hit API2(training)

# Prediction:

# [1] [2] [3]
# [1] [2,3]
# [1] [3] [2]

# DATA source -> Manager -> DL -> SVM -> LR -> Analytics -> Data source