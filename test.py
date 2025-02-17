import random
import datetime
from collections import defaultdict, deque
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

# Конфиг
DAYS_BACK = 20  # На сколько дней назад генерировать данные
BATCH_SIZE = 5000  # Количество документов за один запрос
INDEX_FETCH = "twitch"
INDEX_PUT = "twitch"
WINDOW_SIZE = 5  # Размер окна для сглаживания

def fetch_all_terms_aggs(index, field, size=10000):
    aggs = []
    after_key = None
    while True:
        query = {
            "size": 0,
            "aggs": {
                "unique_terms": {
                    "composite": {
                        "sources": [{field: {"terms": {"field": field}}}],
                        "size": size,
                    }
                }
            }
        }
        if after_key:
            query["aggs"]["unique_terms"]["composite"]["after"] = after_key
        response = es.search(index=index, body=query)
        buckets = response["aggregations"]["unique_terms"]["buckets"]
        aggs.extend([bucket["key"] for bucket in buckets])
        after_key = response["aggregations"]["unique_terms"].get("after_key")
        if not after_key:
            break
    return aggs

user_ids = fetch_all_terms_aggs(INDEX_FETCH, "user_id", size=BATCH_SIZE)
timestamps = fetch_all_terms_aggs(INDEX_FETCH, "timestamp", size=BATCH_SIZE)
print(f"Найдено {len(timestamps)} уникальных timestamp.")

# 1️⃣ Собираем всех user_id
user_activity_map = defaultdict(list)

# 2️⃣ Генерируем карту активности (50% шанс стрима)
for user_id in user_ids:
    for _ in range(DAYS_BACK):
        user_activity_map[user_id["user_id"]].append(random.random() < 0.85)

print(f"✅ Сгенерирована карта активности для {len(user_ids)} пользователей.")


game_counts = defaultdict(int)

for i, tsmp in enumerate(timestamps):
    timestamp = tsmp["timestamp"]
    print(f"Обрабатываем timestamp {i}/{len(timestamps) - 1}")

    # 4.1 Загружаем все документы для этого timestamp (пагинация)
    all_docs = []
    search_after = None
    while True:
        query = {
            "size": BATCH_SIZE,
            "query": {
                "range": {
                    "timestamp": {
                        "gte": timestamp - 1,
                        "lte": timestamp + 1
                    }
                }
            },
            "sort": [{"user_id": "asc"}]
        }
        if search_after:
            query["search_after"] = search_after

        response = es.search(index=INDEX_FETCH, body=query)
        hits = response["hits"]["hits"]

        if not hits:
            break

        all_docs.extend([hit["_source"] for hit in hits])
        search_after = hits[-1]["sort"]

    print(f"Загружено {len(all_docs)} документов для игр.")

    # Собираем сколько раз каждая игра встречается
    for doc in all_docs:
        game_counts[doc["game_name"]] += 1

del game_counts[""]
# Теперь у нас есть словарь `game_counts`, который хранит количество встреч каждой игры
# 3.2 Вычисляем проценты для каждой игры
total_games = sum(game_counts.values())
game_percentages = {game: count / total_games for game, count in game_counts.items()}

user_docs = []
search_after = None
while True:
    query = {
        "size": BATCH_SIZE,
        "query": {
            "match_all": {}
        },
        "sort": [{"user_id": "asc"}]
    }

    if search_after:
        query["search_after"] = search_after

    response = es.search(index=INDEX_FETCH, body=query)
    hits = response["hits"]["hits"]

    if not hits:
        break

    user_docs.extend([hit["_source"] for hit in hits])
    search_after = hits[-1]["sort"]

user_viewers = defaultdict(list)
for doc in user_docs:
    user_viewers[doc["user_id"]].append(doc["viewers_count"])

user_stats = {
    user: {
        "avg": sum(viewers) // len(viewers),
        "min": min(viewers),
        "max": max(viewers)
    }
    for user, viewers in user_viewers.items()
}

# Очередь для хранения последних значений зрителей (скользящее окно)
viewers_queue = deque(maxlen=WINDOW_SIZE)

for i, tsmp in enumerate(timestamps):
    timestamp = tsmp["timestamp"]
    print(f"Обрабатываем timestamp {i}/{len(timestamps) - 1}")
    
    all_docs = []
    search_after = None
    while True:
        query = {
            "size": BATCH_SIZE,
            "query": {
                "range": {
                    "timestamp": {
                        "gte": timestamp - 1,
                        "lte": timestamp + 1
                    }
                }
            },
            "sort": [{"user_id": "asc"}]
        }
        if search_after:
            query["search_after"] = search_after

        response = es.search(index=INDEX_FETCH, body=query)
        hits = response["hits"]["hits"]

        if not hits:
            break

        all_docs.extend([hit["_source"] for hit in hits])
        search_after = hits[-1]["sort"]


    print(f"Загружено {len(all_docs)} документов.")

    
    # Добавляем текущее среднее в очередь
    # mean_viewers = sum(user_avg_viewers.values()) / max(len(user_avg_viewers), 1)
    # viewers_queue.append(mean_viewers)
    
    # Вычисляем сглаженное значение
    # smoothed_viewers = sum(viewers_queue) / len(viewers_queue)
    error_count = 0
    new_docs = []
    timestamp_dt = datetime.datetime.utcfromtimestamp(timestamp / 1000)
    for days_ago in range(1, DAYS_BACK + 1):
        new_timestamp = timestamp_dt - datetime.timedelta(days=days_ago)

        for doc in all_docs:
            user_id = doc["user_id"]
            # Проверяем, стримил ли этот user_id в этот день
            
            if not user_activity_map[user_id]:
                continue
            if not user_activity_map[user_id][days_ago - 1]:
                continue  # Пропускаем этот user_id 

            stats = user_stats.get(user_id, {"avg": doc["viewers_count"], "min": 10, "max": 5000})

            avg_viewers = stats["avg"]
            min_viewers = stats["min"]
            max_viewers = stats["max"]
    
            new_doc = doc.copy()
            new_doc["timestamp"] = new_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            rand = random.random()  # Генерируем случайное число от 0 до 1
            cumulative_prob = 0  # Накопительная вероятность (сумма вероятностей игр)
            
            for game, percentage in game_percentages.items():  
                cumulative_prob += percentage  # Увеличиваем накопительную вероятность
                if rand < cumulative_prob:  # Если случайное число меньше текущей накопленной вероятности
                    new_doc["game_name"] = game  # Выбираем игру
                    break  # Выходим из цикла, т.к. игра уже выбрана

            if new_doc["game_id"] == '' or not new_doc["game_id"]:
                new_doc["game_id"] = '1'

            weekday = new_timestamp.weekday()
            weekend_boost = random.uniform(1.1, 1.2) if weekday in [5, 6] else 1.0

            #noise = int(random.gauss(0, smoothed_viewers * 0.05))
            #new_doc["viewers_count"] = max(0, int(smoothed_viewers + noise))
            noise = int(random.gauss(0, avg_viewers * 0.05))
            new_viewers_count = max(0, int((avg_viewers + noise) * weekend_boost))
            new_viewers_count = min(max_viewers, max(min_viewers, new_viewers_count))
            new_doc["viewers_count"] = new_viewers_count            
                    
            new_docs.append({"index": {"_index": INDEX_PUT}})
            new_docs.append(new_doc)
    
        if new_docs:
            es.bulk(body=new_docs)
            print(f"{len(new_docs)//2} новых записей добавлено за {new_timestamp.strftime('%Y-%m-%d')}.")
            new_docs = []
