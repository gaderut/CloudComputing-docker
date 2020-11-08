from flask import Flask, request, jsonify, render_template
import pandas as pd
import sys
import json
import time
from io import BytesIO
import numpy as np
import requests
from svm import support_vector_machine
from DataPreprocessor import DataPreprocessor
from sklearn.model_selection import train_test_split
from requestHandler import *

app = Flask(__name__)
# CORS(app)
# {name : model}
model = {}
preprocessor = {}
requestHandler = requestHandler("2")


@app.route("/svm/append-ip", methods=['POST'])
def append_ip():
    jsondata = request.get_json()
    name = requestHandler.appendIP(jsondata)

    return name


@app.route("/svm/train", methods=['POST'])
def build_model():
    '''
    request body{
                "client_name": "amazon" ,
                "workflow": "employee",
                “workflow_specification”:[["1"],["2"],["3"]],
                "ips" : {“1” : "10.0.12.31:5000", 
                        “2” : "10.0.12.32:5001",
                        “3” : "10.0.12.31:5002",
                        "4":"10.0.12.33:5003"
                        }
                }
    '''
    jsondata = request.get_json()
    name, _, __ = requestHandler.parseReq(jsondata, "nfw")

    if name not in model.keys():
        _model = support_vector_machine()
        model[name] = _model

    if name not in preprocessor.keys():
        _preprocessor = DataPreprocessor()
        preprocessor[name] = _preprocessor

    dataX, datay = preprocessor[name].getData()
    trainX,  testX, trainy, testy = train_test_split(
        dataX, datay, test_size=0.2)

    model[name].train(trainX, trainy)

    score = model[name].score(testX, testy)

    return str(score)


def init_model(initReq):

    name, _, __ = requestHandler.parseReq(initReq, "fwf")

    if name not in model.keys():
        _model = support_vector_machine()
        model[name] = _model

    if name not in preprocessor.keys():
        _preprocessor = DataPreprocessor()
        preprocessor[name] = _preprocessor

    dataX, datay = preprocessor[name].getData()
    trainX,  testX, trainy, testy = train_test_split(
        dataX, datay, test_size=0.2)

    model[name].train(trainX, trainy)

    score = model[name].score(testX, testy)

    print("test score : " + str(score))


@app.route("/svm/predict", methods=['POST'])
def predict():
    '''
    request body{
                "client_name": "amazon",
                "workflow": "employee"
                “data” : {"emp_id":"1","dept_type":"1", 
                        "gender" : "male", "race": "2", 
                        "day_of_week":"MON", 
                        "checkin_datetime" : "8:00",
                        "time":"14:00"}
                "analytics": [{"start_time":123,"end_time":130, “pred”: 100}]
                }
    '''
    jsondata = request.get_json()
    name, data, analytics = requestHandler.parseReq(jsondata, "pred")
    tic = time.time()
    try:
        dayOfWeek = data['day_of_week']
        datatime = data['time']
    except KeyError:
        return "wrong data format"

    if name not in model.keys():
        return "model not exist"
    else:
        fitmodel = model[name]
        prepro = preprocessor[name]
        data = prepro.transform_test(dayOfWeek, datatime)
        print(data)
        pred = fitmodel.predict(data)

    tok = time.time()
    nextComponent = 'http://'+requestHandler.getNextAddress(name)
    analytics.append({"start_time": tic, "end_time": tok, "pred": pred})
    jsondata["analytics"] = analytics
    res = requests.post(nextComponent, json=jsondata)
    print('response :', res.text)

    # return 200 ok
    return jsonify(success=True)


if __name__ == '__main__':

    data = json.loads(sys.argv[1])

    print('*loading SVM model...')
    init_model(data)
    print('*starting flask app...')
    # host='0.0.0.0', port=8765
    app.run(debug=True, host="0.0.0.0", port=8765)
