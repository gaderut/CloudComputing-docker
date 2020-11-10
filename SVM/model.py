from flask import Flask, request, jsonify, render_template
import pandas as pd
import sys
import json
import time
from io import BytesIO
import numpy as np
import requests

from HospitalDataPreprocessor import HospitalDataPreprocessor
from svm import support_vector_machine
from DataPreprocessor import DataPreprocessor
from sklearn.model_selection import train_test_split
from requestHandler import *
import os
import sys

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
        name: model name,
        path: path to model
    }
    '''
    jsondata = request.get_json()

    name, _, __ = requestHandler.parseReq(jsondata, "nfw")
    # name, _, __ = requestHandler.parseReq(initReq, "fwf")
    # name = jsondata["workflow"]+"#"+jsondata["client_name"]
    client = jsondata["client_name"]
    if name not in model.keys():
        _model = support_vector_machine()
        model[name] = _model

    if name not in preprocessor.keys():
        _preprocessor = DataPreprocessor()
        preprocessor[name] = _preprocessor

    dataX, datay = DataPreprocessor().getData(client)
    trainX,  testX, trainy, testy = train_test_split(
        dataX, datay, test_size=0.2)

    model[name].train(trainX, trainy)

    score = model[name].score(testX, testy)

    # payload = {"test R square": str(score),
    #            "result": "success",
    #            }
    return str(score)


def init_model(workflow, client):
    initReq = workflow+"#"+client

    name, _, __ = requestHandler.parseReq(initReq, "fwf")

    if name not in model.keys():
        _model = support_vector_machine()
        model[name] = _model

    if name not in preprocessor.keys():
        _preprocessor = DataPreprocessor()
        preprocessor[name] = _preprocessor

    client = name.split("#")
    dataX, datay = preprocessor[name].getData(client[1])
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
    workflow = os.environ['workflow']
    table = ""
    if os.environ['client_name'] is not None:
        table = os.environ['client_name']
    else:
        print("Provide env variables")
        sys.exit(1)

    print('*loading SVM model...')
    # init_model(data)
    init_model(workflow, table)
    print('*starting flask app...')
    # host='0.0.0.0', port=8765
    app.run(debug=True, host="0.0.0.0", port=8765)
