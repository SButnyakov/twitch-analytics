from elasticsearch import Elasticsearch, helpers
import csv

# Конфигурация Elasticsearch
ES_HOST = "http://localhost:9200"  # Адрес Elasticsearch
NEW_INDEX = "twitch_v2"  # Новый индекс
CSV_FILES = ["twitch_data_1.csv", "twitch_data_2.csv", "twitch_data_3.csv", "twitch_data_4.csv", "twitch_data_5.csv", "twitch_data_6.csv", "twitch_data_7.csv"]  # Файл с данными
BATCH_SIZE = 10000  # Количество записей в одной пачке

# Маппинг индекса
MAPPING = {
    "settings": {
        "number_of_shards": 4,
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
            "timestamp": { "type": "date" }
        }
    }
}

def create_index(es):
    """Создаёт новый индекс в Elasticsearch с указанным маппингом."""
    if es.indices.exists(index=NEW_INDEX):
        print(f"Индекс {NEW_INDEX} уже существует, удаляем его...")
        es.indices.delete(index=NEW_INDEX)

    print(f"Создаём индекс {NEW_INDEX}...")
    es.indices.create(index=NEW_INDEX, body=MAPPING)
    print("Индекс успешно создан!")

def read_csv_in_chunks(file):
    """Читает CSV и возвращает данные порциями (по 500 записей)."""
    with open(file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        chunk = []
        for i, row in enumerate(reader):
            # Преобразуем timestamp в формат даты
            if "timestamp" in row:
                row["timestamp"] = row["timestamp"]  # Можно преобразовать в datetime, если нужно
            chunk.append(row)

            if len(chunk) >= BATCH_SIZE:
                yield chunk
                chunk = []

        # Отдаём оставшиеся строки, если они есть
        if chunk:
            yield chunk

def bulk_insert(es, data):
    """Загружает данные в Elasticsearch пакетами по BATCH_SIZE записей."""
    print(f"Начинаем загрузку данных в индекс {NEW_INDEX}...")

    actions = []
    for i, chunk in enumerate(data):
        for row in chunk:
            action = {
                "_index": NEW_INDEX,
                "_source": row
            }
            actions.append(action)

        # Загружаем пакетами
        helpers.bulk(es, actions)
        print(f"Загружено {len(actions)} записей... Пакет {i + 1}")
        actions = []

def main():
    """Основной процесс: создаём индекс, читаем CSV, загружаем данные."""
    es = Elasticsearch(ES_HOST, request_timeout=120)

    create_index(es)
    for i, file in enumerate(CSV_FILES):
        data = read_csv_in_chunks(file=file)
        bulk_insert(es, data)
        print(f"Импортирована {file}")

    print("Импорт завершён!")

if __name__ == "__main__":
    main()
