#===============================================================================
# файл для визуализации данных
#===============================================================================

from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay # для оценки качества предсказаний
import matplotlib.pyplot as plt # для построения графиков
import streamlit as st # для Dashboard
import pandas as pd # для удобной работы с датасетом
from utils import Consumer_custom # кастомный Kafka Consumer
from settings import * # импорт параметров


# настройки Kafka
bootstrap_server_consume = 'localhost:9094' # url Kafka брокера, который будет получать метаданные о Kafka cluster для организации сообщения (метаданные же сами состоят из: topics, их partitions, leader brokers для partitions) 
topic_consume = ['raw_data', 'ML_results'] # названия топиков, что отправляют Producer-ы (на них будет подписан Consumer)
# topic_consume = ['raw_data'] # названия топиков, что отправляют Producer-ы (на них будет подписан Consumer)
conf_consume = {'bootstrap.servers': bootstrap_server_consume, 'group.id': 'data_visualizers'} # конфиг для Consumer (group.id — группа для консьюмеров)
consumer = Consumer_custom(conf_consume) # создаём объект Kafka — Consumer
consumer.subscribe(topic_consume) # подписываем его на topic_consume (данные из этих топиков будет получать Consumer от брокеров(Producer-ов))


# настройки Streamlit
st.set_page_config( # конфигурация для страницы Streamlit (Dashboard)
    page_title="Real-Time Data Dashboard", # название вкладки
    layout="wide", # настройка размещения контента ("wide" — по всему экрану)
)

# боковая понель
width = st.sidebar.slider(label="Categories plot width", min_value=1, max_value=15, value=6) # ползунок ширины графика (с значением по умолчанию — value)
height = st.sidebar.slider(label="Categories plot height", min_value=1, max_value=15, value=6) # ползунок высоты графика (с значением по умолчанию — value)
radius = st.sidebar.slider(label="Genders plot radius", min_value=1, max_value=15, value=6) # ползунок ширины графика (с значением по умолчанию — value)

# используемые данные в сессии
categories = ['misc_net', 'shopping_pos', 'entertainment', 'food_dining', 'travel', 'grocery_net', 'grocery_pos', 'shopping_net', 'home', 'kids_pets', 'health_fitness', 'misc_pos', 'gas_transport', 'personal_care'] # категории товаров (используется при визуализации)
for key in ["operations count", "categories", "genders", "F1 weighted score", "confuse"]:
    if key not in st.session_state.keys(): # если ключа нет в словаре сессии
        if key == "categories": # если рассматриваемый ключ имеет значение "categories"
            st.session_state[key] = pd.DataFrame(index=categories, columns=["old", "new"]) # создаём в словаре пустой DataFrame
        elif key == "confuse": # если рассматриваемый ключ имеет значение "confuse"
            st.session_state[key] = confusion_matrix([0,0,1,1], [0,1,1,0]) # создаём confusion_matrix с мусорными значениями
        else: # если рассматриваемый ключ имеет любое другое значение
            st.session_state[key] = [] # создаём пустой список под данные

# контейнеры для Streamlit страницы
container_operations = st.container(border=True) # placeholder (контейнер) для данных с фиксированной позицией (и рамкой border=True)
container_operations.title("Number of operations") # добавление заголовка к контейнеру
holder_operations = container_operations.empty() # пустой подконтейнер, что может вмещать только один объект (для самообновления графиков)

container_categories_and_genders = st.container(border=True) # placeholder (контейнер) для данных с фиксированной позицией (и рамкой border=True)
column_categories, column_genders = container_categories_and_genders.columns(spec=2) # делим контейнер на spec колонок (или в пропорции spec)
column_categories.title("Popular categories") # добавление заголовка к контейнеру
holder_categories = column_categories.empty() # пустой подконтейнер, что может вмещать только один объект (для самообновления графиков)
column_genders.title("Frequent gender") # добавление заголовка к контейнеру
holder_genders = column_genders.empty() # пустой подконтейнер, что может вмещать только один объект (для самообновления графиков)

container_score = st.container(border=True) # placeholder (контейнер) для данных с фиксированной позицией (и рамкой border=True)
container_score.title("Model F1 weighted score") # добавление заголовка к контейнеру
holder_score = container_score.empty() # пустой подконтейнер, что может вмещать только один объект (для самообновления графиков)

container_confus = st.container(border=True) # placeholder (контейнер) для данных с фиксированной позицией (и рамкой border=True)
container_confus.title("Confusion matrix") # добавление заголовка к контейнеру
column_confus_old, column_confus_new = container_confus.columns(spec=2) # делим контейнер на spec колонок (или в пропорции spec)
column_confus_old.title("Old") # добавление заголовка к контейнеру
holder_confus_old = column_confus_old.empty() # пустой подконтейнер, что может вмещать только один объект (для самообновления графиков)
column_confus_new.title("New") # добавление заголовка к контейнеру
holder_confus_new = column_confus_new.empty() # пустой подконтейнер, что может вмещать только один объект (для самообновления графиков)

while True: # бесконечный цикл
    data, data_topic = consumer.get_message(timeout=1000) # потребление одного сообщение (timeout — максимальное время ожидания сообщения в секундах, если сообщение не пришло по истечению таймера или оно об ошибке существования топика — вернёт None, None)

    if data is not None: # если нужное сообщение получено
        if VERBOSE: # если стоит флаг подробного вывода в консоль
            print(f"Got message from topic '{data_topic}'") # вывод сообщения о получении данных

        if data_topic == "raw_data": # если topic сообщения это "raw_data" (пришли необработанные данные)
            data = pd.DataFrame(data) # конвертируем полученный словарь в DataFrame

            st.session_state["operations count"].append(data.shape[0])

            st.session_state["categories"]["old"] = st.session_state["categories"]["new"]
            st.session_state["categories"]["new"] = data["category"].value_counts() # добавляем данные в список ["price"] для Dashboard

            st.session_state["genders"] = data["gender"].value_counts()

            # строим графики
            holder_operations.line_chart(st.session_state["operations count"]) # создаём линейный график на основе данных в словаре session_state

            fig_2 = st.session_state["categories"].reset_index(names="category").plot(x="category", y=["old", "new"], kind="bar").get_figure() # строим (plot) и получаем фигуру графика matplotlib.figure.Figure (get_figure)
            # reset_index меняет текущие индексы (в данном случае — типы категорий) на цифрцы от 0, при этом предыдущие превращая в колонку "category" (names="category")
            fig_2.set_size_inches(width, height) # изменяем размер графика (с помощью slider)
            fig_2.autofmt_xdate(rotation=45, ha='right') # поворот подписей на оси OX
            plt.ylim(0, 200) # лимит значений для оси OY
            holder_categories.pyplot(fig_2) # выводим Matplotlib фигуру в Streamlit
            plt.close() # отключает повторное отображение графика из-за наложения их в Matplotlib

            fig_3 = st.session_state["genders"].plot(kind="pie").get_figure() # строим (plot) и получаем фигуру графика matplotlib.figure.Figure (get_figure)
            fig_3.set_size_inches(radius, radius) # изменяем размер графика (с помощью slider)
            holder_genders.pyplot(fig_3) # выводим Matplotlib фигуру в Streamlit 
            plt.close() # отключает повторное отображение графика из-за наложения их в Matplotlib

        elif data_topic == "ML_results": # если topic сообщения это "ML_results" (пришли результаты от модели)
            st.session_state["F1 weighted score"].append(data["f1"]) # добавляем пришедшие данные о F1 score в сессию
            holder_score.line_chart(st.session_state["F1 weighted score"]) # создаём линейный график на основе данных в словаре session_state

            fig_5 = ConfusionMatrixDisplay(st.session_state["confuse"], display_labels=range(st.session_state["confuse"].shape[0])).plot().figure_ # берём фигуру (figure_) отображения (plot) confusion_matrix с подписями на осях (display_labels = range(st.session_state["confuse"].shape[0]) — подписи от 0 до размерности таблицы-1 )
            holder_confus_old.pyplot(fig_5) # выводим Matplotlib фигуру в Streamlit
            plt.close() # отключает повторное отображение графика из-за наложения их в Matplotlib

            st.session_state["confuse"] = confusion_matrix(data["y_true"], data["y_pred"]) # обновляем confusion_matrix на основе пришедших значений
            fig_6 = ConfusionMatrixDisplay(st.session_state["confuse"], display_labels=range(st.session_state["confuse"].shape[0])).plot().figure_ # берём фигуру (figure_) отображения (plot) confusion_matrix с подписями на осях (display_labels = range(st.session_state["confuse"].shape[0]) — подписи от 0 до размерности таблицы-1 )
            holder_confus_new.pyplot(fig_6) # выводим Matplotlib фигуру в Streamlit
            plt.close() # отключает повторное отображение графика из-за наложения их в Matplotlib