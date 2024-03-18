from confluent_kafka import Consumer # Consumer в Kafka
import pandas as pd # для удобной работы с датасетом
import pickle # для сохранения и загрузки объектов
import json # для сохранения, загрузки и работы с JSON данными
import re # для регулярных выражений
from settings import * # импорт параметров

with open(f"{MODELS_DIR}LabelEncoders.pkl", 'rb') as f: # открытие файла для бинарного ('b') чтения ('r')
    encoders = pickle.load(f) # загружаем энкодеры

bootstrap_servers = 'localhost:9094' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions) 
topic = 'raw_data' # название топика, что отправляет Producer (на него будет подписан Consumer)
conf = {'bootstrap.servers': bootstrap_servers, 'group.id': 'data_processors'} # конфиг для Consumer (group.id — группа для консьюмеров)

# можно сделать несколько консьюмеров
consumer = Consumer(conf) # создаём объект Kafka — Consumer
consumer.subscribe([topic]) # подписываем его на topic (данные из этих топиков будет получать Consumer от брокеров(Producer-ов))


while True: # бесконечный цикл
    msg = consumer.poll(timeout=1000) # потребление одного сообщение (timeout — максимальное время ожидания сообщения в секундах, если сообщение не пришло по истечению таймера — вернёт None)

    if msg is not None: # если сообщение получено
        data = json.loads(msg.value().decode('utf-8')) # полученное сообщение конвертируем bp ОЫЩТ обратно в utf-8 кодировку
        data = pd.DataFrame(data) # конвертируем полученный словарь в DataFrame
        
        data["trans_date_trans_time"] = pd.to_datetime(data["trans_date_trans_time"], format="%d/%m/%Y %H:%M") # приводим колонку к типу времени
        data["trans_date_trans_time"] = pd.to_numeric(data["trans_date_trans_time"]) # конвертируем дату в число (потому что модели не умеют работать с типом date)

        data["dob"] = pd.to_datetime(data["dob"], format="%d/%m/%Y") # приводим колонку к типу времени
        data["dob"] = pd.to_numeric(data["dob"]) # конвертируем дату в число (потому что модели не умеют работать с типом date)

        data["merchant"] = data["merchant"].apply(lambda text: re.sub('fraud_', '', text)) # удаляем приписки в столбце merchant

        for column in ["merchant", "category", "gender", "job"]: # идём по столбцам, что нужно сконвертировать из строк в числа
            data[column] = encoders[column].transform(data[column]) # конвертируем строковые столбцы в числовые (с использованием предобученного LabelEncoder-а)
        
        print(data)