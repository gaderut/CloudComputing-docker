from flask import Flask, request
import requests
import subprocess
import sys

app = Flask(__name__)

@app.route('/')
def index():
    return 'Server is alive!!'

import random
ipList = ['10.176.67.91', '10.176.67.93', '10.176.67.92']

@app.route('/employee', methods=['GET', 'POST'])
def init():
    content = request.json
    # if (containerExist("cassandra") == False):
    #     launchContainer("docker stack deploy -c cassandra-compose.yml cassandra") #DB
    #     launchContainer("docker run -v /home/generic/cassandra/data:/usr/src/app/csv -it --rm data_loader:latest python ./data_loader.py EMPLOYEE") #Dataloader
    # if (containerExist("logisticregression") == False):
    #     launchContainer("docker service create -p 50:50 --name logisticregression --replicas 3 gaderut/cc4_lgr:1.0") #ML1
    # if (containerExist("db") == False):
    #     launchContainer("ls") #ML2
    r1 = requests.post(url = "http://" + random.choice(ipList) + ":50/app/getPredictionLR", data = content)
    print(r1)
    # r2 = requests.post(url = "http://127.0.0.1:5000/test", data = content)
    return "Employee exit time: " + r1.text + ",  Number of employees: " + "1" + "\n"

@app.route('/test', methods=['GET', 'POST'])
def test():
    content = request.json
    print(content)
    return "1"

def launchContainer(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    output, error = process.communicate()
    if (error != None):
        print("Error while running command {}. Error: {}".format(command, error))
        sys.exit()
    print(output)

def containerExist(containerName):
    bashCommand = "docker service ls --filter name={}| sed '1 d'".format(containerName)
    process = subprocess.Popen(bashCommand, shell=True, stdout=subprocess.PIPE)
    output, error = process.communicate()
    if (error != None):
        print("Error while calling containerExist for {}. Error: {}".format(containerName, error))
        sys.exit()
    if (len(output) > 0):
        return True
    return False

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
