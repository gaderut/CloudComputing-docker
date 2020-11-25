from flask import Flask, request, jsonify, render_template
import pandas as pd
import sys
import json
import time
from io import BytesIO
import numpy as np
import requests

# from HospitalDataPreprocessor import HospitalDataPreprocessor
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
requestHandler = requestHandler("3")
train_start_time = 0.0
train_end_time = 0.0


@app.route("/svm/append-ip", methods=['POST'])
def append_ip():
    global train_start_time
    global train_end_time
    global requestHandler
    jsondata = request.get_json()
    app.logger.info("json data : " + str(jsondata))
    name = requestHandler.appendIP(jsondata)
    app.logger.info("start time : " + str(train_start_time) +
                    " end time : " + str(train_end_time))
    res = {"start_time": train_start_time, "end_time": train_end_time}
    return jsonify(res)


@app.route("/svm/train", methods=['POST'])
def build_model():
    '''
    request body{
        name: model name,
        path: path to model
    }
    '''
    global model
    global preprocessor
    global requestHandler
    global train_start_time
    global train_end_time
    jsondata = request.get_json()
    app.logger.info("json data : " + str(jsondata))
    name, _, __ = requestHandler.parseReq(jsondata, "nwf")
    # name, _, __ = requestHandler.parseReq(initReq, "fwf")
    # name = jsondata["workflow"]+"#"+jsondata["client_name"]
    _client = jsondata["client_name"]
    _workflow = jsondata["workflow"]
    client = _workflow+"_"+_client
    if name not in model.keys():
        _model = support_vector_machine()
        model[name] = _model
        app.logger.info("name key error ")
    if name not in preprocessor.keys():
        _preprocessor = DataPreprocessor()
        preprocessor[name] = _preprocessor
        app.logger.info("name key error ")
    train_start_time = time.time()
    app.logger.info("model start training")
    dataX, datay = DataPreprocessor().getData(client)
    trainX,  testX, trainy, testy = train_test_split(
        dataX, datay, test_size=0.2)

    model[name].train(trainX, trainy)
    app.logger.info("training success")
    score = model[name].score(testX, testy)
    train_end_time = time.time()
    # payload = {"test R square": str(score),
    #            "result": "success",
    #            }
    res = {"start_time": train_start_time, "end_time": train_end_time}
    return jsonify(res)


def init_model(workflow, client):
    global train_start_time
    global train_end_time
    initReq = workflow+"#"+client

    name, _, __ = requestHandler.parseReq(initReq, "fwf")
    print("whrkflow name : "+name)
    if name not in model.keys():
        _model = support_vector_machine()
        model[name] = _model

    if name not in preprocessor.keys():
        _preprocessor = DataPreprocessor()
        preprocessor[name] = _preprocessor
    train_start_time = time.time()
    dataX, datay = preprocessor[name].getData(workflow+"_"+client)
    trainX,  testX, trainy, testy = train_test_split(
        dataX, datay, test_size=0.2)

    model[name].train(trainX, trainy)
    train_end_time = time.time()
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
    global model
    global preprocessor
    global requestHandler
    jsondata = request.get_json()
    app.logger.info("json data : " + str(jsondata))
    name, data, analytics = requestHandler.parseReq(jsondata, "pred")
    app.logger.info("name : " + str(name))
    tic = time.time()
    try:
        dayOfWeek = data['day_of_week']
        datatime = data['time']
    except:
        app.logger.info("wrong data format")
        return "wrong data format"

    if name not in model.keys():
        app.logger.info("model not exist")
        return "model not exist"
    else:
        fitmodel = model[name]
        prepro = preprocessor[name]
        data = prepro.transform_test(dayOfWeek, datatime)
        app.logger.info(data)
        pred = fitmodel.predict(data)

    tok = time.time()
    nextComponents = requestHandler.getNextAddress(name)
    app.logger.info("next component : " + str(nextComponents))
    app.logger.info("pred : " + str(pred))
    analytics.append({"start_time": tic, "end_time": tok, "pred": pred[0]})
    jsondata["analytics"] = analytics
    for addr in nextComponents:
        app.logger.info("sending request to : " + addr)

        res = requests.post("http://"+addr, json=jsondata)
        # app.logger.info('response :', res.text)
        # except:
        #     app.logger.info("error in sending request")
        #     return "error in sending request"

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
    # _client = jsondata["client_name"]
    # _workflow = jsondata["workflow"]
    # client = workflow+"_"+table
    print('*loading SVM model...')
    # init_model(data)
    init_model(workflow, table)
    print('*starting flask app...')
    # host='0.0.0.0', port=8765
    app.run(debug=True, host="0.0.0.0", port=8765)
