#===============================================================================
# файл для предобработки данных
#===============================================================================

from confluent_kafka import Producer, Consumer # Producer и Consumer в Kafka
import pandas as pd # для удобной работы с датасетом
import pickle # для сохранения и загрузки объектов
import json # для сохранения, загрузки и работы с JSON данными
import re # для регулярных выражений
from settings import * # импорт параметров

import warnings # для обработки предупреждений
warnings.simplefilter(action='ignore', category=FutureWarning) # игнорируем FutureWarning (от pandas за is_sparse is deprecated)


# настройки Kafka
bootstrap_server_consume = 'localhost:9094' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions) 
topic_consume = ['raw_data'] # названия топиков, что отправляют Producer-ы (на них будет подписан Consumer)
conf_consume = {'bootstrap.servers': bootstrap_server_consume, 'group.id': 'data_processors'} # конфиг для Consumer (group.id — группа для консьюмеров)
consumer = Consumer(conf_consume) # создаём объект Kafka — Consumer (можно сделать несколько консьюмеров)
consumer.subscribe(topic_consume) # подписываем его на topic_consume (данные из этих топиков будет получать Consumer от брокеров(Producer-ов))

bootstrap_server_produce = 'localhost:9097' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions)
topic_produce = 'processed_data' # название топика, что отправляет Producer
conf_produce = {'bootstrap.servers': bootstrap_server_produce} # конфиг для Producer
producer = Producer(conf_produce) # создаём объект Kafka — Producer


with open(f"{MODELS_DIR}LabelEncoders.pkl", 'rb') as f: # открытие файла для бинарного ('b') чтения ('r')
    encoders = pickle.load(f) # загружаем энкодеры

while True: # бесконечный цикл
    msg = consumer.poll(timeout=1000) # потребление одного сообщение (timeout — максимальное время ожидания сообщения в секундах, если сообщение не пришло по истечению таймера — вернёт None)

    if (msg is not None) and (msg.value() != b'Subscribed topic not available: raw_data: Broker: Unknown topic or partition'): # если сообщение получено (и нет ошибки отсутствующего топика у Consumer-а)
        data = json.loads(msg.value().decode('utf-8')) # полученное сообщение конвертируем bp ОЫЩТ обратно в utf-8 кодировку
        data = pd.DataFrame(data) # конвертируем полученный словарь в DataFrame
        
        data["trans_date_trans_time"] = pd.to_datetime(data["trans_date_trans_time"], format="%d/%m/%Y %H:%M") # приводим колонку к типу времени
        data["trans_date_trans_time"] = pd.to_numeric(data["trans_date_trans_time"]) # конвертируем дату в число (потому что модели не умеют работать с типом date)
        data["dob"] = pd.to_datetime(data["dob"], format="%d/%m/%Y") # приводим колонку к типу времени
        data["dob"] = pd.to_numeric(data["dob"]) # конвертируем дату в число (потому что модели не умеют работать с типом date)
        data["merchant"] = data["merchant"].apply(lambda text: re.sub('fraud_', '', text)) # удаляем приписки в столбце merchant

        for column in ["merchant", "category", "gender", "job"]: # идём по столбцам, что нужно сконвертировать из строк в числа
            data[column] = encoders[column].transform(data[column]) # конвертируем строковые столбцы в числовые (с использованием предобученного LabelEncoder-а)
        
        data_y = data["is_fraud"].to_numpy().tolist() # берём таргеты и конвертируем их сначала в np.array, а потом в list (так как в .produce требуются простые объекты и он не умеет работать с DataFrame и np.array))
        data_X = data.drop(columns=["is_fraud"]).to_numpy().tolist() # берём данные (без столбца таргета) и конвертируем их сначала в np.array, а потом в list (так как в .produce требуются простые объекты и он не умеет работать с DataFrame и np.array))
        data = {"X": data_X, "y_true": data_y} # собираем словарь из списков (list): data_X — фич, data_y — таргетов, что будем отправлять

        producer.produce(topic_produce, key='1', value=json.dumps(data)) # отправляем данные брокеру (topic_produce — в какие топики отправлять сообщение, key — Message key, value — сообщение (json.dumps возвращает строку в формате JSON))
        producer.flush() # ожидание получения брокером сообщения

        if VERBOSE: # если стоит флаг подробного вывода в консоль
            print(f"Producer из processing.py отправил данные в topic '{topic_produce}':\n {pd.DataFrame(data)}") # вывод сообщения об отправленных данных