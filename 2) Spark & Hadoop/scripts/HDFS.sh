#!/bin/bash

hdfs dfs -mkdir /data/ # создаём папку в hdfs под названием data
hdfs dfs -D dfs.block.size=32M -put /data_simple.csv /data/ # отправляем данные с NameNode в hdfs (на DataNode)
hdfs dfsadmin -setSpaceQuota 5g / # выставление ограничение на используемую память 
exit # выходим из вложенного терминала Hadoop