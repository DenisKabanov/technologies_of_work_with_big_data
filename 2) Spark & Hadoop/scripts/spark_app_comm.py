# import numpy as np
import sys # для ключей запуска
import os # для получения PID процесса
import psutil # для получения данных об используемой оперативной памяти
import time # для отслеживания времени работы
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession # для создания Spark сессии
from pyspark.sql.functions import unix_timestamp, col # функции для работы с данными
from pyspark.sql.types import FloatType # типы переменных
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler # StringIndexer — конвертер категориальных переменных в числовые; OneHotEncoder — One-hot encoder признаков, VectorAssembler — для объединения нескольких числовых переменных в одну
from pyspark.ml.classification import GBTClassifier # модель
from sklearn.metrics import classification_report # для оценки качества предсказаний

OPTIMIZED = True if sys.argv[1] == "True" else False # флаг, оптимизированный ли запуск
time_start = time.time() # замеряем время начала выполнения приложения


# подключаемся к Spark сессии
SparkContext.getOrCreate(SparkConf().setMaster('spark://spark-master:7077')).setLogLevel("INFO") # оставляем логирование только информации
# spark = SparkSession.builder.master("local").appName("practice").getOrCreate() # создание Spark сессии (getOrCreate) с указанием spark-master (master: local — мастером является это же устройство), и названием (appName)
spark = SparkSession.builder.master("spark://spark-master:7077").appName("practice").getOrCreate() # создание Spark сессии (getOrCreate) с указанием spark-master (master: spark://spark-master:7077 — URL до мастера), и названием (appName)


# загружаем данные в Spark
data = spark.read.format("csv").option("header", "true").option('inferSchema', 'true').load("hdfs://namenode:9000/data/data_simple.csv") # read — считываем данные в формате format с указанием опций option по пути load
if OPTIMIZED: # если есть ключ оптимизации
    data.cache() # кэширование объекта в оперативную память
    data = data.repartition(5) # меняем число партиций файла

# обрабатываем данные в Spark
# Date ==> Float
data = data.withColumn("trans_date_trans_time", (unix_timestamp("trans_date_trans_time", format='dd/MM/yyyy HH:mm') / 86400).cast(FloatType())) # конвертируем столбец типа str сначала в date (unix_timestamp), а потом в float (cast(FloatType()))
data = data.withColumn("dob", (unix_timestamp("dob", format='dd/MM/yyyy') / 86400).cast(FloatType())) # конвертируем столбец типа str сначала в date (unix_timestamp), а потом в float (cast(FloatType()))

# Categoric ==> Int
for col in ["merchant", "category", "gender", "job"]: # идём по категориальным колонкам
    indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index") # создаём объект для трансформации категориальных признаков в int (inputCol — входная колонка, outputCol — выходная колонка)
    data = indexer.fit(data).transform(data) # обучаем (fit) на данных и трансформируем (transform) их
    data = data.drop(col) # удаляем предыдущую колонку
    data = data.withColumnRenamed(f"{col}_index", col) # переименовываем новую в старую

# Int ==> One-hot
one_hot_encoder = OneHotEncoder(inputCol='gender', outputCol='gender_one_hot') # объект для One-hot кодирования колонки
one_hot_encoder = one_hot_encoder.fit(data) # обучаем энкодер на данных
data = one_hot_encoder.transform(data) # трансформируем данные
data = data.drop('gender') # удаляем предыдущую колонку

# Комбинируем числовые колонки в один вектор
numeric_cols = ["trans_date_trans_time", "cc_num", "amt", "lat", "long", "city_pop", "dob", "merch_lat", "merch_long", "merchant", "category", "job", "gender_one_hot"] # список с числовыми колонками
assembler = VectorAssembler(inputCols=numeric_cols, outputCol='vectorized_data') # объект для комбинации данных из колонок inputCols в outputCol
data = assembler.transform(data) # комбинируем данные из колонок

# train-test split
data_train, data_test = data.randomSplit([0.7, 0.3]) # разбиваем данные на обучающую и тестовую выборки
if OPTIMIZED: # если есть ключ оптимизации
    data_train.cache() # кэширование объекта в оперативную память
    data_train = data_train.repartition(5) # меняем число партиций файла
    data_test.cache() # кэширование объекта в оперативную память
    data_test = data_test.repartition(5) # меняем число партиций файла


# Создание модели и её обучение
model = GBTClassifier(featuresCol="vectorized_data", labelCol="is_fraud", maxBins=700) # модель для классификации
model = model.fit(data_train) # обучаем модель на тренировочных данных

# Подсчёт метрик
pred_test = model.transform(data_test) # делаем предсказание на тестовых данных

pred_test = pred_test.toPandas() # конвертируем в pandas.DataFrame
y_true = pred_test["is_fraud"].values
y_pred = pred_test["prediction"].values
labels = ["not fraud", "fraud"]
print(classification_report(y_true, y_pred, zero_division=0, target_names=labels)) # выводим основные метрики, такие как precision, recall, f1-score, accuracy (zero_division=0 — деление на ноль заменять нулём)

# y_true = np.array(pred_test.select("is_fraud").collect()).reshape(-1) # берём элементы колонки и конвертируем их в np.array (reshape для 1-D array)
# y_pred = np.array(pred_test.select("prediction").collect()).reshape(-1) # берём элементы колонки и конвертируем их в np.array (reshape для 1-D array)
# N = float(y_true.shape[0])
# TP = float(((y_pred == 1) & (y_true == 1)).sum())
# TN = float(((y_pred == 0) & (y_true == 0)).sum())
# FP = float(((y_pred == 1) & (y_true == 0)).sum())
# FN = float(((y_pred == 0) & (y_true == 1)).sum())
# accuracy = (y_true == y_pred).sum() / N 
# precision = TP / (TP+FP)
# recall = TP / (TP+FN)
# f_score = 2*precision*recall / (precision+recall)
# print("Accuracy: " + str(accuracy))
# print("Precision: " + str(precision))
# print("recall: " + str(recall))
# print("F score: " + str(f_score))


time_res = time.time() - time_start # замеряем время, чито потребовалось на выполнение кода
RAM_res = psutil.Process(os.getpid()).memory_info().rss / (float(1024)**2) # запоминаем RAM, что потребовалась процессу на выполнение (1024**2 — в мегабайтах, так как изначально измеряется в байтах)

spark.stop() # отключение от Spark сессии

with open('/log.txt', 'a') as f: # открываем файл для дозаписи (a)
    f.write("Time: " + str(time_res) + " seconds, RAM: " + str(RAM_res) + " Mb.\n") # добавляем строку в файл
