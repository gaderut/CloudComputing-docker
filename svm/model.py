from flask import Flask, request, jsonify, render_template
import pandas as pd
import json
from io import BytesIO
import numpy as np
import requests
from svm import support_vector_machine
from DataPreprocessor import DataPreprocessor
from sklearn.model_selection import train_test_split

app = Flask(__name__)
# CORS(app)
# {name : model}
model = {}
preprocessor = {}


@app.route("/svm/build_model", methods=['POST'])
def build_model():
    '''
    request body{
        name: model name,
        path: path to model
    }
    '''
    jsondata = request.get_json()
    name = jsondata["name"]
    if name not in model.keys():
        _model = support_vector_machine()
        model[name] = _model

    if name not in preprocessor.keys():
        _preprocessor = DataPreprocessor()
        preprocessor[name] = _preprocessor

    dataX, datay = DataPreprocessor().getData()
    trainX,  testX, trainy, testy = train_test_split(
        dataX, datay, test_size=0.2)

    model[name].train(trainX, trainy)

    score = model[name].score(testX, testy)

    # payload = {"test R square": str(score),
    #            "result": "success",
    #            }
    return str(score)


@app.route("/svm/predict", methods=['POST'])
def predict():
    '''
    request body{
        name: model name,
        "dayOfWeek":,
         "time"
    }
    '''
    jsondata = request.get_json()

    try:
        name = jsondata["name"]
        dayOfWeek = jsondata['dayOfWeek']
        time = jsondata['time']
    except KeyError:
        return "wrong data format"

    if name not in model.keys():
        return "model not exist"
    else:
        fitmodel = model[name]
        prepro = preprocessor[name]
        data = prepro.transform_test(dayOfWeek, time)
        print(data)
        pred = fitmodel.predict(data)

    return str(int(round(pred[0])))


if __name__ == '__main__':
    # print('*loading lenet model...')
    # build_model()
    print('*starting flask app...')
    # host='0.0.0.0', port=80
    app.run(debug=True, host='0.0.0.0', port=8080)
