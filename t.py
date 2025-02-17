import os
import csv
import datetime
from elasticsearch import Elasticsearch, helpers

# Настройки подключения к Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Путь к директории с CSV файлами
directory_path = 'D:/csvs'

# Индекс для записи в Elasticsearch
index_name = "twitch"

# Установленная дата, до которой файлы не обрабатываются
cutoff_date = datetime.datetime(2025, 2, 5, 18, 7, 27)

# Создание индекса (если его нет)
def create_index():
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "user_id": { "type": "keyword" },
                "user_login": { "type": "keyword" },
                "user_name": { "type": "keyword" },
                "game_id": { "type": "keyword" },
                "game_name": { "type": "keyword" },
                "viewers_count": { "type": "integer" },
                "language": { "type": "keyword" },
                "timestamp": { "type": "date" },
            }
        }
    }
    
    if not es.indices.exists(index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Индекс {index_name} создан.")

# Получаем дату из имени файла
def extract_timestamp_from_filename(filename):
    # Пример формата: '2025-February-5 18_28_27.csv'
    date_str = filename.split('.')[0]
    timestamp = datetime.datetime.strptime(date_str, "%Y-%B-%d %H_%M_%S")
    return timestamp

# Прочитать CSV файл и преобразовать данные в формат, подходящий для Elasticsearch
def process_csv_file(filepath, timestamp):
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        actions = []
        
        for row in reader:
            action = {
                "_index": index_name,
                "_op_type": "index",  # Можно использовать "index" или "create"
                "_source": {
                    "user_id": row["user_id"],
                    "user_login": row["user_login"],
                    "user_name": row["user_name"],
                    "game_id": row["game_id"],
                    "game_name": row["game_name"],
                    "viewers_count": int(row["viewers_count"]),
                    "language": row["language"],
                    "timestamp": (timestamp - datetime.timedelta(hours=3)).isoformat()
                }
            }
            print(timestamp)
            actions.append(action)
        
        return actions

# Обработать все файлы в директории
def process_files():
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            # Получаем путь к файлу
            filepath = os.path.join(directory_path, filename)
            
            # Извлекаем timestamp из имени файла
            timestamp = extract_timestamp_from_filename(filename)
            
            # Пропускаем файлы, дата которых меньше cutoff_date
            if timestamp < cutoff_date:
                #print(f"Файл {filename} пропущен, так как его дата до {cutoff_date}.")
                continue
            
            # Обрабатываем CSV файл
            actions = process_csv_file(filepath, timestamp)
            
            # Пишем данные в Elasticsearch
            if actions:
                helpers.bulk(es, actions)
                #print(f"Данные из файла {filename} успешно загружены в Elasticsearch.")

# Запуск обработки
process_files()
