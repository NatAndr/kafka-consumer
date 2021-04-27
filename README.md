# Домашнее задание

## Приложение для чтения данных из Kafka

**Цель:** 
В этом ДЗ студент научится использовать Apache Kafka - управлять топиками и читать/писать данные с помощью Scala.

Перед началом выполнения требуется развернуть Kafka и создать топик books с 3 партициями. Требуется написать приложение, которое будет выполнять следующее:

1) Вычитывать из CSV-файла, который можно скачать по ссылке - https://www.kaggle.com/sootersaalu/amazon-top-50-bestselling-books-2009-2019, данные, сериализовывать их в JSON, и записывать в топик books локльно развернутого сервиса Apache Kafka.
2) Вычитать из топика books данные и распечатать в stdout последние 5 записей (c максимальным значением offset) из каждой партиции. При чтении топика одновременно можно хранить в памяти только 15 записей.