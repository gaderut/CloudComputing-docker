'''Project Members:
1) Brij Malhotra
2) Gokul Ravi Kumar
3) Rutuja Vijaykumar Gadekar
'''
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.plotting import scatter_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV , train_test_split , cross_val_score
from sklearn.feature_selection import RFE
from sklearn.model_selection import train_test_split
from sklearn.ensemble import AdaBoostClassifier
from collections import Counter
from sklearn.svm import SVC
from sklearn.utils import shuffle
from sklearn import tree
from sklearn import preprocessing
from sklearn.model_selection import validation_curve, StratifiedKFold, cross_val_predict
from sklearn.metrics import precision_recall_curve, precision_score, recall_score, confusion_matrix
from sklearn.metrics import roc_curve, auc,roc_auc_score,plot_roc_curve

#final_table_without_na.describe()

#Correlation Matrix plot using HeatMap

final_table_without_hadm = final_table_without_na.drop(columns = ['hadm_id'])
correlation_matrix = final_table_without_hadm.corr(method = 'pearson')
sns.heatmap(correlation_matrix, vmin = -1, vmax = 1, center = 0)
#plt.show()

#Pair Plot for the Features using SNS.pairplot()

sns.pairplot(final_table_without_hadm, hue = 'hospital_expire_flag')
#plt.show()

#Splitting the Train and Test Dataset

x = final_table_without_na.iloc[:,3:]
y = final_table_without_na.iloc[:,2]

x_train, x_test, y_train, y_test = train_test_split(x,y, test_size = 0.50,random_state = 42)

#Defining the models

lg_clf = LogisticRegression(class_weight='balanced', solver='liblinear', C= 0.1, max_iter=10000)
tree_clf = tree.DecisionTreeClassifier(max_depth=10, criterion= "entropy")
svm_rbf = SVC(kernel='rbf', C=10)
ada_classifier_log = AdaBoostClassifier(LogisticRegression(class_weight='balanced', solver='liblinear',penalty='l2', C= 2, max_iter=10000),n_estimators=10)
ada_classifier_tree = AdaBoostClassifier(tree.DecisionTreeClassifier(max_depth=4), n_estimators=8)

clfs = {"logistic_regression":lg_clf,
        "svm_rbf":svm_rbf,
        "decision_tree":tree_clf,
        "ada_boost_logistic": ada_classifier_log,
        "ada_boost_tree": ada_classifier_tree
        }


for name,clf in clfs.items():
  pred = clf.fit(x_train,y_train)
  y_pred = pred.predict(x_test)
  accuracy = accuracy_score(y_test, y_pred)
  precision = precision_score(y_test,y_pred)
  recall = recall_score(y_test,y_pred)
  roc_score = roc_auc_score(y_test, y_pred)
  print("Classifier Name: ", name, "Accuracy = ", accuracy, "Precision = ", precision, "Recall = ", recall)


#Logistic Regression and Logistic Regression with AdaBoost

#Train and Test accuracy for Logistic Regression with different C values
clf = clfs['logistic_regression']
train_score, test_score, aoc_score,train_scores_aoc, mean_cv = [], [],[],[],[]
r = np.arange(0.000001, 10, 0.01)
for C in r:
  clf.C = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  mean_cv.append(np.mean(predicted))
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_scores_aoc.append(roc_auc)
  train_score.append(model.score(x_train,y_train))
  test_score.append(model.score(x_test,y_test))
  aoc_score.append(roc_auc_score(y_test,y_pred))
plt.plot(r, train_score, label='train')
plt.plot(r, test_score, label='test')
plt.plot(r, aoc_score, label = 'AOC_Score')
plt.plot(r, train_scores_aoc, label = 'Train AOC Curve')
plt.plot(r, mean_cv, label = 'Mean Cross Validation Error')
plt.title("Train and Test Accuracy with changing C value in Logistic Regression")
plt.xlabel("C")
plt.ylabel("accuracy_score/ AUC Value")
plt.legend()

#Train and Test accuracy for AdaBoost Logistic Regression with different values for C
clf = clfs['ada_boost_logistic']
train_score, test_score, aoc_score, train_scores_aoc, mean_cv = [], [],[],[],[]
r = np.arange(0.001, 10, 0.1)
for C in r:
  clf.base_estimator.C = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  mean_cv.append(np.mean(predicted))
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_scores_aoc.append(roc_auc)
  train_score.append(model.score(x_train,y_train))
  test_score.append(model.score(x_test,y_test))
  aoc_score.append(roc_auc_score(y_test,y_pred))
plt.plot(r, train_score, label='train')
plt.plot(r, test_score, label='test')
plt.plot(r, aoc_score, label = 'AOC_Score')
plt.plot(r, train_scores_aoc, label = 'Train AOC Curve')
plt.plot(r, mean_cv, label = 'Mean Cross Validation Error')
plt.title("Train and Test Accuracy with changing C value in Logistic Regression with an AdaBoost")
plt.xlabel("C")
plt.ylabel("accuracy_score/ AUC Value")
plt.legend()

#Train and Test accuracy for AdaBoost with Logistic regression with different values for n_estimators
clf = clfs['ada_boost_logistic']
train_score, test_score,aoc_score, train_scores_aoc, mean_cv = [], [],[],[],[]
r = np.arange(1, 40, 2)
for C in r:
  clf.n_estimators = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  mean_cv.append(np.mean(predicted))
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_scores_aoc.append(roc_auc)
  train_score.append(model.score(x_train,y_train))
  test_score.append(model.score(x_test,y_test))
  aoc_score.append(roc_auc_score(y_test,y_pred))
plt.plot(r, train_score, label='train')
plt.plot(r, test_score, label='test')
plt.plot(r, aoc_score, label = 'AOC_Score')
plt.plot(r, train_scores_aoc, label = 'Train AOC Curve')
plt.plot(r, mean_cv, label = 'Mean Cross Validation Error')
plt.title("Train and Test Accuracy with changing n_estimators value in AdaBoost with Logistic Regression")
plt.xlabel("n_estimators")
plt.ylabel("accuracy_score/ AUC Value")
plt.legend()

#SVM with RBF Kernel

#Train and Test AUC-ROC for SVM RBF with different C values
clf = clfs['svm_rbf']
aoc_score, train_scores_aoc, = [], []
r = np.arange(0.001, 20, 0.1)
for C in r:
  clf.C = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_scores_aoc.append(roc_auc)
  aoc_score.append(roc_auc_score(y_test,y_pred))
plt.plot(r, aoc_score, label = 'AOC_Score')
plt.plot(r, train_scores_aoc, label = 'Train AOC Curve')
plt.title("Train and Test AUC-ROC with changing C value in SVM with RBF Kernel")
plt.xlabel("C")
plt.ylabel("accuracy_score/ AUC Value")
plt.legend()

#Train, Test and Mean CV accuracy for SVM RBF with different C values
clf = clfs['svm_rbf']
train_score, test_score, mean_cv = [], [], []
r = np.arange(0.01, 20, 0.1)
for C in r:
  clf.C = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  mean_cv.append(np.mean(predicted))
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_score.append(model.score(x_train,y_train))
  test_score.append(model.score(x_test,y_test))
plt.plot(r, train_score, label='train')
plt.plot(r, test_score, label='test')
plt.plot(r, mean_cv, label = 'Mean Cross Validation Error')
plt.title("Train,Test and mean CV Accuracy with changing C value in SVM with RBF Kernel")
plt.xlabel("C")
plt.ylabel("accuracy_score")
plt.legend()

#Decision Trees

#Different depth levels of Decision Tree
clf = clfs['decision_tree']
train_score, test_score, aoc_score, train_scores_aoc, mean_cv = [], [],[],[],[]
r = np.arange(1, 20, 1)
for C in r:
  clf.max_depth = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  mean_cv.append(np.mean(predicted))
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_scores_aoc.append(roc_auc)
  train_score.append(model.score(x_train,y_train))
  test_score.append(model.score(x_test,y_test))
  aoc_score.append(roc_auc_score(y_test,y_pred))
plt.plot(r, train_score, label='train')
plt.plot(r, test_score, label='test')
plt.plot(r, aoc_score, label = 'AOC_Score')
plt.plot(r, train_scores_aoc, label = 'Train AOC Curve')
plt.plot(r, mean_cv, label = 'Mean Cross Validation Error')
plt.title("Train and Test Accuracy with changing depth level of Decision Tree")
plt.xlabel("Depth Level")
plt.ylabel("accuracy_score/ AUC Value")
plt.legend()

#Train and Test accuracy for AdaBoost with Decision Tree with different values for decision tree depth
clf = clfs['ada_boost_tree']
train_score, test_score,aoc_score, train_scores_aoc,mean_cv = [], [],[],[],[]
r = np.arange(1, 10, 1)
for C in r:
  clf.base_estimator.max_depth = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  mean_cv.append(np.mean(predicted))
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_scores_aoc.append(roc_auc)
  train_score.append(model.score(x_train,y_train))
  test_score.append(model.score(x_test,y_test))
  aoc_score.append(roc_auc_score(y_test,y_pred))
plt.plot(r, train_score, label='train')
plt.plot(r, test_score, label='test')
plt.plot(r, aoc_score, label = 'AOC_Score')
plt.plot(r, train_scores_aoc, label = 'Train AOC Curve')
plt.plot(r, mean_cv, label = 'Mean Cross Validation Error')
plt.title("Train and Test Accuracy with changing Depth level of decision Tree with adaBoost")
plt.xlabel("Depth Level")
plt.ylabel("accuracy_score/ AUC Value")
plt.legend()

#Train and Test accuracy for AdaBoost with Decision Tree with different values for n_estimators
clf = clfs['ada_boost_tree']
train_score, test_score,aoc_score, train_scores_aoc, mean_cv = [], [],[],[],[]
r = np.arange(1, 100, 2)
for C in r:
  clf.n_estimators = C
  model = clf.fit(x_train,y_train)
  train_pred = clf.predict(x_train)
  y_pred = clf.predict(x_test)
  predicted = cross_val_score(clf,x_train, y_train, cv = 5)
  mean_cv.append(np.mean(predicted))
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_scores_aoc.append(roc_auc)
  train_score.append(model.score(x_train,y_train))
  test_score.append(model.score(x_test,y_test))
  aoc_score.append(roc_auc_score(y_test,y_pred))
plt.plot(r, train_score, label='train')
plt.plot(r, test_score, label='test')
plt.plot(r, aoc_score, label = 'AOC_Score')
plt.plot(r, train_scores_aoc, label = 'Train AOC Curve')
plt.plot(r, mean_cv, label = 'Mean Cross Validation Error')
plt.title("Train and Test Accuracy with changing n_estimators value in AdaBoost")
plt.xlabel("n_estimators")
plt.ylabel("accuracy_score/ AUC Value")
plt.legend()