#!/bin/bash

apk add --update make automake gcc g++ # обновляем gcc (так как возникает ошибка при скачивании numpy)
apk add --update python-dev # обновляем python-dev (из-за ошибки SystemError: Cannot compile 'Python.h'. Perhaps you need to install python-dev|python-devel.)
apk add linux-headers # добавляем linux-headers для psutil
pip install numpy psutil # устанавливаем необходимые библиотеки
for ((i=1; i<=20; i++)) # делаем 20 запусков
do # начало цикла
    /spark/bin/spark-submit /spark_app.py $1 # запускаем Spark приложение с пришедшим параметром
done # завершение цикла
exit # выходим из вложенного терминала Spark