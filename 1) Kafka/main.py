from confluent_kafka import Producer # Producer в Kafka
import json # для сохранения, загрузки и работы с JSON данными
import random # для генерации случайных чисел
import time # для работы с временем

bootstrap_servers = 'localhost:9095' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions), 
topic = 'stock_topic' # название топика, что отправляет Producer

conf = {'bootstrap.servers': bootstrap_servers} # конфиг для Producer
producer = Producer(conf) # создаём объект Kafka — Producer

def generate_stock_data(): # функция для генерации одного сэмпла данных
    return { # возвращает словарь 
        'name': 'Gazprom', # название
        'price': round(random.uniform(100, 200), 2), # случайное число — цена
        'timestamp': int(time.time()) # текущее время с конвертацией в int
    }

def produce_stoke_data(): # функция для бесконечной генерации данных
    while True: # бесконечный цикл
        stock_data = generate_stock_data() # генерируем один сэмпл
        producer.produce(topic, key='1', value=json.dumps(stock_data)) # отправляем данные брокеру (topic — в какие топики отправлять сообщение, key — Message key, value — сообщение (json.dumps возвращает строку в формате JSON))
        producer.flush() # для проверки получения брокером сообщения
        print(f'Produced: {stock_data}') # вывод сообщения об отправленных данных
        time.sleep(1) # ждём 1 секунду

if __name__ == "__main__": # main функция
    produce_stoke_data() # бесконечно генерируем данные