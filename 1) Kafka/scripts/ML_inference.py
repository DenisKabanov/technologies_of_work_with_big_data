#===============================================================================
# файл для ML 
#===============================================================================

from sklearn.metrics import f1_score # для оценки качества предсказаний
import pickle # для сохранения и загрузки объектов
from utils import Producer_custom, Consumer_custom # кастомный Kafka Producer и Consumer
from settings import * # импорт параметров


# настройки Kafka
bootstrap_server_consume = 'localhost:9097' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions) 
topic_consume = ['processed_data'] # названия топиков, что отправляют Producer-ы (на них будет подписан Consumer)
conf_consume = {'bootstrap.servers': bootstrap_server_consume, 'group.id': 'ML_inference'} # конфиг для Consumer (group.id — группа для консьюмеров)
consumer = Consumer_custom(conf_consume) # создаём объект Kafka — Consumer (можно сделать несколько консьюмеров)
consumer.subscribe(topic_consume) # подписываем его на topic_consume (данные из этих топиков будет получать Consumer от брокеров(Producer-ов))

bootstrap_server_produce = 'localhost:9094' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions)
topic_produce = 'ML_results' # название топика, что отправляет Producer
conf_produce = {'bootstrap.servers': bootstrap_server_produce} # конфиг для Producer
producer = Producer_custom(conf_produce) # создаём объект Kafka — Producer


with open(f"{MODELS_DIR}KNN.pkl", 'rb') as f: # открытие файла для бинарного ('b') чтения ('r')
    model = pickle.load(f) # загружаем предобученную модель

while True: # бесконечный цикл
    data, _ = consumer.get_message(timeout=1000) # потребление одного сообщение (timeout — максимальное время ожидания сообщения в секундах, если сообщение не пришло по истечению таймера или оно об ошибке существования топика — вернёт None, None)
    if data is not None: # если нужное сообщение получено
        y_pred = model.predict(data["X"]).tolist() # делаем предсказание (tolist — конвертируем его из np.array в list)        
        score = f1_score(data["y_true"], y_pred, average="weighted", zero_division=0) # считаем f1 score (average: "macro" — по всем класса, без учёта дисбаланса, "weighted": с учётом дисбаланса) (zero_division=0 — деление на ноль заменять нулём)
        results = {"y_true": data["y_true"], "y_pred": y_pred, "f1": score} # словарь, содержащий результаты для визуализации (настоящие таргеты, предсказанные, F1 score)
        
        producer.send_message(topic_produce, key='1', value=results) # отправляем данные брокеру и ожидаем получения им сообщения (topic_produce — в какие топики отправлять сообщение, key — Message key, value — данные, что нужно отправить)

        if VERBOSE: # если стоит флаг подробного вывода в консоль
            print(f"Producer из ML_inference.py отправил данные в topic '{topic_produce}':\n {results}") # вывод сообщения об отправленных данных