#===============================================================================
# файл для быстрого запуска проекта
#===============================================================================

import subprocess # для запуска дополнительных процессов
from scripts.settings import * # импорт параметров

# запуск подпроцессов (run — с ожиданием завершения; Popen — без, то есть параллельное выполнение)
if DOWNLOAD_REQUIREMENTS: # если стоит флаг на установку модулей, используемых в проекте
    subprocess.run([PYTHON_PATH, "-m", "pip", "install", "-r", "requirements.txt"]) # установка необходимых пакетов с помощью модуля pip указанных в файле (-r) requirements.txt

brokers = subprocess.run(["docker-compose", "up", "-d"]) # запуск docker-compose (в нём указаны брокеры Kafka) в виде демона (-d)
if brokers.returncode != 0: # если код завершения процесса не равен 0 (выполнен не успешно)
    raise Exception("Не удалось запустить брокеров!") # выкидываем исключение


processes = {} # словарь под процессы
processes["1"] = subprocess.Popen([PYTHON_PATH, "./scripts/generation.py"]) # запуск скрипта, "генерирующего" данные
processes["2"] = subprocess.Popen([PYTHON_PATH, "./scripts/processing.py"]) # запуск скрипта, обрабатывающего данные
processes["3"] = subprocess.Popen([PYTHON_PATH, "./scripts/ML_inference.py"]) # запуск скрипта, анализирующего данные
processes["4"] = subprocess.Popen([PYTHON_PATH, "-m", "streamlit", "run", "./scripts/visualization.py"]) # запуск скрипта с визуализацией 

try: # блок для ожидания прерывания KeyboardInterrupt
    while True: # бесконечный цикл
        continue # переходим на следующую итерацию цикла
except KeyboardInterrupt: # ожидаем прекращения по комбинации клавиш Ctrl + C
    print("Interrupting by keyboard.")

    for p_id in processes.keys(): # идём по словарю созданных процессов (p_id != PID)
        processes[p_id].kill() # убиваем подпроцесс
        if processes[p_id].poll() is None: # проверяем, был ли процесс прекращён (poll возвращает код завершения процесса)
            print(f"Процесс {p_id} всё ещё выполняется, его PID: {processes[p_id].pid}!")
        else: # если код завершения есть
            print(f"Процесс {p_id} был завершён!")

    subprocess.run(["docker-compose", "down"]) # отключение docker-compose (брокеров)
