#===============================================================================
# файл для генерации данных
#===============================================================================

from confluent_kafka import Producer # Producer в Kafka
import pandas as pd # для удобной работы с датасетом
import random # для генерации случайных чисел
import json # для сохранения, загрузки и работы с JSON данными
import time # для работы с временем
from settings import * # импорт параметров


# настройки Kafka
bootstrap_server_produce = 'localhost:9094' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions)
topic_produce = 'raw_data' # название топика, что отправляет Producer
conf_produce = {'bootstrap.servers': bootstrap_server_produce} # конфиг для Producer
producer_1 = Producer(conf_produce) # создаём объект Kafka — Producer
producer_2 = Producer(conf_produce) # создаём объект Kafka — Producer


dataset = pd.read_csv(f"{DATA_DIR}data_simple.csv",  sep=',', encoding='utf8', index_col=None) # считывание данных (колонки разделены с помощью sep, данные хранятся в кодировке encoding, индексы указаны в колонке index_col (None, если в данных её изначально нет))

while True: # бесконечный цикл
    data_1 = dataset.sample(frac=random.uniform(0.001, 0.01)).to_dict() # случайно выбираем frac пропорцию данных (random.uniform — равномерное распределение), что будут отправлены (to_dict — конвертируем в словарь, так как в .produce требуются простые объекты и он не умеет работать с DataFrame и np.array)
    data_2 = dataset.sample(frac=random.uniform(0.001, 0.01)).to_dict() # случайно выбираем frac пропорцию данных (random.uniform — равномерное распределение), что будут отправлены (to_dict — конвертируем в словарь, так как в .produce требуются простые объекты и он не умеет работать с DataFrame и np.array)

    producer_1.produce(topic_produce, key='1', value=json.dumps(data_1)) # отправляем данные брокеру (topic_produce — в какие топики отправлять сообщение, key — Message key, value — сообщение (json.dumps возвращает строку в формате JSON))
    producer_2.produce(topic_produce, key='1', value=json.dumps(data_2)) # отправляем данные брокеру (topic_produce — в какие топики отправлять сообщение, key — Message key, value — сообщение (json.dumps возвращает строку в формате JSON))

    producer_1.flush() # ожидание получения брокером сообщения
    producer_2.flush() # ожидание получения брокером сообщения

    print(f"Producer 1 отправил данные в topic '{topic_produce}':\n {pd.DataFrame(data_1)}") # вывод сообщения об отправленных данных
    print(f"Producer 2 отправил данные в topic '{topic_produce}':\n {pd.DataFrame(data_2)}") # вывод сообщения об отправленных данных
    time.sleep(30 + random.uniform(-5.0, 5.0)) # ждём случайные n секунд