from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
import json

# Подключение к Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Настройки
INDEX_NAME = "twitch"
TIMESTAMP_FIELD = "timestamp"
TARGET_TIMESTAMP = "2025-02-07T15:07:00.000Z"  # Укажи нужный timestamp
TIME_SHIFT = timedelta(minutes=10)  # Смещение на 10 минут
BATCH_SIZE = 5000  # Количество документов за один запрос

# Функция для получения данных с пагинацией
def fetch_documents(index, timestamp):
    all_docs = []
    search_after = None
    while True:
        query = {
            "size": BATCH_SIZE,
            "query": {
                "range": {
                    "timestamp": {
                        "gt": "2025-02-06T15:07:00"
                    }
                }
            },
            "sort": [{"user_id": "asc"}]
        }
        if search_after:
            query["search_after"] = search_after

        response = es.search(index=index, body=query)
        hits = response["hits"]["hits"]

        if not hits:
            break

        all_docs.extend([hit["_source"] for hit in hits])
        search_after = hits[-1]["sort"]

    return all_docs

# Функция для создания новых записей с измененным timestamp
def copy_documents_with_new_timestamp(docs, time_shift):
    new_docs = []
    for doc in docs:
        new_doc = doc.copy()
        original_timestamp = datetime.strptime(new_doc[TIMESTAMP_FIELD], "%Y-%m-%dT%H:%M:%S")
        new_doc[TIMESTAMP_FIELD] = (original_timestamp + time_shift).strftime("%Y-%m-%dT%H:%M:%S")

        new_docs.append({"index": {"_index": INDEX_NAME}})
        new_docs.append(new_doc)
    
    return new_docs

# Основной процесс
documents = fetch_documents(INDEX_NAME, TARGET_TIMESTAMP)

if documents:
    new_documents = copy_documents_with_new_timestamp(documents, TIME_SHIFT)
    print(json.dumps(new_documents[:4], indent=2))
    es.bulk(body=new_documents)
    print(f"Добавлено {len(new_documents)//2} записей с timestamp {TARGET_TIMESTAMP} + 10 минут")
else:
    print("Нет данных для копирования")
