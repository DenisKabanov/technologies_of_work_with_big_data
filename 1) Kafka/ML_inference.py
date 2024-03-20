#===============================================================================
# файл для ML 
#===============================================================================

from confluent_kafka import Producer, Consumer # Producer и Consumer в Kafka
import pandas as pd # для удобной работы с датасетом
import pickle # для сохранения и загрузки объектов
import json # для сохранения, загрузки и работы с JSON данными
from sklearn.metrics import f1_score # для оценки качества предсказаний
from settings import * # импорт параметров


# настройки Kafka
bootstrap_server_consume = 'localhost:9097' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions) 
topic_consume = ['processed_data'] # названия топиков, что отправляют Producer-ы (на них будет подписан Consumer)
conf_consume = {'bootstrap.servers': bootstrap_server_consume, 'group.id': 'ML_inference'} # конфиг для Consumer (group.id — группа для консьюмеров)
consumer = Consumer(conf_consume) # создаём объект Kafka — Consumer (можно сделать несколько консьюмеров)
consumer.subscribe(topic_consume) # подписываем его на topic_consume (данные из этих топиков будет получать Consumer от брокеров(Producer-ов))

bootstrap_server_produce = 'localhost:9094' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions)
topic_produce = 'ML_results' # название топика, что отправляет Producer
conf_produce = {'bootstrap.servers': bootstrap_server_produce} # конфиг для Producer
producer = Producer(conf_produce) # создаём объект Kafka — Producer


with open(f"{MODELS_DIR}KNN.pkl", 'rb') as f: # открытие файла для бинарного ('b') чтения ('r')
    model = pickle.load(f) # загружаем предобученную модель

while True: # бесконечный цикл
    msg = consumer.poll(timeout=1000) # потребление одного сообщение (timeout — максимальное время ожидания сообщения в секундах, если сообщение не пришло по истечению таймера — вернёт None)
    
    if (msg is not None) and (msg.value() != b'Subscribed topic not available: processed_data: Broker: Unknown topic or partition'): # если сообщение получено (и нет ошибки отсутствующего топика у Consumer-а)
        data = json.loads(msg.value().decode('utf-8')) # полученное сообщение конвертируем bp ОЫЩТ обратно в utf-8 кодировку

        y_pred = model.predict(data["X"]).tolist() # делаем предсказание (tolist — конвертируем его из np.array в list)        
        score = f1_score(data["y_true"], y_pred, average="weighted", zero_division=0) # считаем f1 score (average: "macro" — по всем класса, без учёта дисбаланса, "weighted": с учётом дисбаланса) (zero_division=0 — деление на ноль заменять нулём)
        
        results = {"y_true": data["y_true"], "y_pred": y_pred, "f1": score} # словарь, содержащий результаты для визуализации (настоящие таргеты, предсказанные, F1 score)
        
        producer.produce(topic_produce, key='1', value=json.dumps(results)) # отправляем данные брокеру (topic_produce — в какие топики отправлять сообщение, key — Message key, value — сообщение (json.dumps возвращает строку в формате JSON))
        producer.flush() # ожидание получения брокером сообщения

        print(f"Producer отправил данные в topic '{topic_produce}':\n {results}") # вывод сообщения об отправленных данных