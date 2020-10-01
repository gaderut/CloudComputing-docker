# from cassandra.cluster import Cluster
from timeParser import timeParser
import pandas as pd
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
import numpy as np
import random
# for testing
import csv
from sklearn.feature_extraction import DictVectorizer


class DataPreprocessor:
    timeChoice = ["8:00", "8:30", "9:00",
                  "9:30", "10:00", "10:30", "11:00",
                  "11:30", "12:00", "12:30", "13:00",
                  "13:30", "14:00", "14:30", "15:00",
                  "15:30", "16:00", "16:30",
                  "17:00", "17:30", "18:00",
                  "18:30", "19:00", "19:30", '20:00']
    timeencodeDict = {"8:00": 0, "8:30": 1, "9:00": 2,
                      "9:30": 3, "10:00": 4, "10:30": 5, "11:00": 6,
                      "11:30": 7, "12:00": 8, "12:30": 9, "13:00": 10,
                      "13:30": 11, "14:00": 12, "14:30": 13, "15:00": 14,
                      "15:30": 15, "16:00": 16, "16:30": 17,
                      "17:00": 18, "17:30": 19, "18:00": 20,
                      "18:30": 21, "19:00": 22, "19:30": 23, '20:00': 24}
    dayencodeDict = {
        'MON': 0, "TUE": 1, 'WED': 2, 'THU': 3, "FRI": 4
    }

    def __init__(self):
        self.le_dict = {}
        # self.cluster = Cluster(['0.0.0.0'], port=9042)
        # self.session = self.cluster.connect(
        #     'employee', wait_for_all_pools=True)
        # self.session.execute('USE employee')

    def getData(self):
        self._countByGroup()
        # dayOfWeek = []
        # time = []
        features = []
        count = []

        for r in self.group_count.keys():
            # print(r)
            sp = r.split('#')
            dayrow = [0]*5
            dayrow[self.dayencodeDict[sp[0]]] = 1
            # dayOfWeek += dayrow
            timeRow = [0]*25
            timeRow[self.timeencodeDict[sp[1]]] = 1
            # time += [sp[1]]
            features.append(dayrow+timeRow)
            count += [self.group_count[r]]

        train_data = np.array(features)
        target = np.array(count)
        return (train_data, target)

    def transform_test(self, dayOfWeek, time):
        # going random if not valid input
        try:
            dayidx = self.dayencodeDict[dayOfWeek]
        except KeyError:
            dayidx = random.randint(0, 4)

        try:
            timeidx = self.timeencodeDict[time]
        except KeyError:
            timeidx = random.randint(0, 24)

        dayrow = [0]*5
        dayrow[dayidx] = 1

        timeRow = [0]*25
        timeRow[timeidx] = 1

        test_data = dayrow+timeRow

        return np.array(test_data).reshape(1, -1)

    def _countByGroup(self):
        self.group_count = {}
        # rows = self.session.execute('SELECT * FROM employee')
        with open('employeedata.csv', newline='\n') as csvfile:
            rows = csv.reader(csvfile, delimiter=',', quotechar='|')
            next(rows)
            for row in rows:
                for t in self.timeChoice:
                    # key = row.DAY_OF_WEEK + ':' + t
                    key = row[4] + '#' + t
                    # outTime = timeParser(row.CHECKIN_DATETIME) + \
                    #     timeParser(row.DURATION)
                    outTime = timeParser(row[5]) + \
                        timeParser(row[6])
                    if outTime > timeParser(t):
                        if key not in self.group_count.keys():
                            self.group_count[key] = 1
                        else:
                            self.group_count[key] += 1

    def oneHotEncode2(self, df, le_dict={}):
        # don't work this way
        if not le_dict:
            columnsToEncode = list(df.select_dtypes(
                include=['category', 'object']))
            train = True
        else:
            columnsToEncode = le_dict.keys()
            train = False

        for feature in columnsToEncode:
            if train:
                le_dict[feature] = LabelEncoder()
            try:
                if train:
                    df[feature] = le_dict[feature].fit_transform(df[feature])
                else:
                    df[feature] = le_dict[feature].transform(df[feature])

                df = pd.concat([df,
                                pd.get_dummies(df[feature]).rename(columns=lambda x: feature + '_' + str(x))], axis=1)
                df = df.drop(feature, axis=1)
            except:
                print('Error encoding '+feature)
                #df[feature]  = df[feature].convert_objects(convert_numeric='force')
                df[feature] = df[feature].apply(pd.to_numeric, errors='coerce')
        return (df, le_dict)
