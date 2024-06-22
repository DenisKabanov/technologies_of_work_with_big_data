import numpy as np
import sys
import os
import psutil
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession 
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.types import FloatType 
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler 
from pyspark.ml.classification import GBTClassifier 
# from sklearn.metrics import classification_report

OPTIMIZED = True if sys.argv[1] == "True" else False
time_start = time.time()

SparkContext.getOrCreate(SparkConf().setMaster('spark://spark-master:7077')).setLogLevel("INFO")
spark = SparkSession.builder.master("spark://spark-master:7077").appName("practice").getOrCreate() 

data = spark.read.format("csv").option("header", "true").option('inferSchema', 'true').load("hdfs://namenode:9000/data/data_simple.csv") 
if OPTIMIZED:
    data.cache()
    data = data.repartition(4)

data = data.withColumn("trans_date_trans_time", (unix_timestamp("trans_date_trans_time", format='dd/MM/yyyy HH:mm') / 86400).cast(FloatType())) 
data = data.withColumn("dob", (unix_timestamp("dob", format='dd/MM/yyyy') / 86400).cast(FloatType())) 
for col in ["merchant", "category", "gender", "job"]: 
    # indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index") 
    indexer = StringIndexer(inputCol=col, outputCol= col + "_index") 
    data = indexer.fit(data).transform(data) 
    data = data.drop(col) 
    # data = data.withColumnRenamed(f"{col}_index", col) 
    data = data.withColumnRenamed(col + "_index", col) 

one_hot_encoder = OneHotEncoder(inputCol='gender', outputCol='gender_one_hot') 
one_hot_encoder = one_hot_encoder.fit(data) 
data = one_hot_encoder.transform(data) 
data = data.drop('gender') 

numeric_cols = ["trans_date_trans_time", "cc_num", "amt", "lat", "long", "city_pop", "dob", "merch_lat", "merch_long", "merchant", "category", "job", "gender_one_hot"] 
assembler = VectorAssembler(inputCols=numeric_cols, outputCol='vectorized_data') 
data = assembler.transform(data) 

data_train, data_test = data.randomSplit([0.7, 0.3]) 
if OPTIMIZED:
    data_train.cache()
    data_train = data_train.repartition(4)
    data_test.cache()
    data_test = data_test.repartition(4)

model = GBTClassifier(featuresCol="vectorized_data", labelCol="is_fraud", maxBins=700) 
model = model.fit(data_train) 


pred_test = model.transform(data_test)

# pred_test = pred_test.toPandas()
# y_true = pred_test["is_fraud"].values
# y_pred = pred_test["prediction"].values
# labels = ["not fraud", "fraud"]
# print(classification_report(y_true, y_pred, zero_division=0, target_names=labels))

y_true = np.array(pred_test.select("is_fraud").collect()).reshape(-1)
y_pred = np.array(pred_test.select("prediction").collect()).reshape(-1)
N = float(y_true.shape[0])
TP = float(((y_pred == 1) & (y_true == 1)).sum())
TN = float(((y_pred == 0) & (y_true == 0)).sum())
FP = float(((y_pred == 1) & (y_true == 0)).sum())
FN = float(((y_pred == 0) & (y_true == 1)).sum())
accuracy = (y_true == y_pred).sum() / N 
precision = TP / (TP+FP)
recall = TP / (TP+FN)
f_score = 2*precision*recall / (precision+recall)
print("Accuracy: ", accuracy)
print("Precision: ", precision)
print("recall: ", recall)
print("F score: ", f_score)

time_res = time.time() - time_start
RAM_res = psutil.Process(os.getpid()).memory_info().rss / (float(1024)**2)

spark.stop()

with open('/log.txt', 'a') as f:
    f.write("Time: " + str(time_res) + " seconds, RAM: " + str(RAM_res) + " Mb.\n")
