import streamlit as st # для Dashboard
import json # для сохранения, загрузки и работы с JSON данными
from confluent_kafka import Consumer # Consumer в Kafka

st.sidebar # для боковой панели (?)
st.set_page_config( # конфигурация для страницы Streamlit (Dashboard)
    page_title="Real-Time Data Dashboard", # название вкладки
    layout="wide", # настройка размещения контента ("wide" — по всему экрану)
)

if "price" not in st.session_state: # если "price" нет в сессии
    st.session_state["price"] = [] # создаём пустой список под "price"

bootstrap_servers = 'localhost:9095' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions), 
topic = 'stock_topic' # название топика, что отправляет Producer (на него ьудет подписан Consumer)

conf = {'bootstrap.servers': bootstrap_servers, 'group.id': 'my_consumers'} # конфиг для Consumer (group.id — группа для консьюмеров)

# можно сделать несколько консьюмеров
consumer = Consumer(conf) # создаём объект Kafka — Consumer
consumer.subscribe([topic]) # подписываем его на topic (данные из этих топиков будет получать Consumer от брокеров(Producer-ов))

st.title("Prices") # добавление заголовка на страницу

chart_holder = st.empty() # пустой placeholder (контейнер) для данных

while True: # бесконечный цикл
    msg = consumer.poll(timeout=1000) # потребление одного сообщение (timeout — максимальное время ожидания сообщения в секундах, если сообщение не пришло по истечению таймера — вернёт None)

    if msg is not None: # если сообщение получено
        stock_data = json.loads(msg.value().decode('utf-8')) # полученное сообщение конвертируем bp ОЫЩТ обратно в utf-8 кодировку
        st.session_state["price"].append(stock_data['price']) # добавляем данные в список ["price"] для Dashboard
    
    chart_holder.line_chart(st.session_state["price"]) # создаём линейный график на основе данных в списке session_state["price"]