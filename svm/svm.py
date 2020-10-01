import os
from sklearn import svm
from sklearn.linear_model import SGDRegressor
from sklearn.externals import joblib


class support_vector_machine:
    _model = None

    def __init__(self):
        self._model = SGDRegressor()

    def train(self, data_x, data_y):
        self._model.fit(data_x, data_y)
        joblib.dump(self._model, 'svm_model.pickle')

    def predict(self, X):
        ret = self._model.predict(X)
        return ret

    def score(self, X, y):
        score = self._model.score(X, y)
        return score

    def load_model(self, path):
        path = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), path)
        print(path)
        self._model = joblib.load(path)
        return self._model

    def get_model(self):
        return self._model
