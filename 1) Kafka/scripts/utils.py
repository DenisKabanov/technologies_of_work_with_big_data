#===============================================================================
# файл, содержащий вспомогательные классы и функции
#===============================================================================

from confluent_kafka import Producer, Consumer # Producer и Consumer в Kafka
import json # для сохранения, загрузки и работы с JSON данными

class Producer_custom(Producer):
    '''
    Кастомный класс Producer.
    '''
    def send_message(self, topic, key, value):
        '''
        Метод для отправки сообщения.\n
        Parameters:
            * topic: топик, в который нужно отправить сообщение
            * key: Message key
            * value: содержимое сообщения, должно быть представлено простым (JSON serializable) типом (например — int, dict, ...)
        '''
        value = json.dumps(value) # конвертируем данные в JSON строку
        self.produce(topic, key=key, value=value) # отправляем данные брокеру (topic — в какие топики отправлять сообщение, key — Message key, value — сообщение в виде JSON строки)
        self.flush() # ожидание получения брокером сообщения


class Consumer_custom(Consumer):
    '''
    Кастомный класс Consumer.
    '''
    def get_message(self, timeout: int=1000) -> tuple:
        '''
        Метод для получения сообщения.\n
        Parameters:
            * timeout: максимальное время ожидания сообщения в секундах\n
        Returns:
            * tuple: пара (данные, топик), если пришло валидное сообщение, иначе — (None, None)
        '''
        msg = self.poll(timeout=timeout) # потребление одного сообщение (timeout — максимальное время ожидания сообщения в секундах, если сообщение не пришло по истечению таймера — вернёт None)

        if msg is not None: # если сообщение не пустое
            msg_data = msg.value().decode('utf-8') # декодируем данные из сообщения (приходят в виде байт-строки b"...") в строку
            if msg_data.find("Subscribed topic not available") == -1: # если это сообщение не об ошибке несуществующего топика
                return json.loads(msg_data), msg.topic() # возвращаем объект, сконвертированный из JSON строки обратно в исходный формат и topic сообщения
        
        return None, None # в проитивных случаях возвращаем None
 