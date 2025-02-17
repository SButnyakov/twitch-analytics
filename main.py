import random
import datetime
from collections import defaultdict
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

# Конфиг
DAYS_BACK = 1  # На сколько дней назад генерировать данные
BATCH_SIZE = 5000  # Количество документов за один запрос
INDEX_FETCH = "twitch"
INDEX_PUT = "twitch_analytics"
FORBIDDEN_GAME = "Kingdom Come: Deliverance 2"
FORBIDDEN_GAME_CUTOFF = datetime.datetime(2024, 2, 4, 16, 0)  # 4 февраля 16:00 UTC

def fetch_all_terms_aggs(index, field, size=10000):
    aggs = []
    after_key = None

    while True:
        query = {
            "size": 0,
            "aggs": {
                "unique_terms": {
                    "composite": {
                        "sources": [
                            {field: {"terms": {"field": field}}}
                        ],
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

        # Если есть следующий "after" курсор, продолжаем пагинацию
        after_key = response["aggregations"]["unique_terms"].get("after_key")
        if not after_key:
            break

    return aggs


# MAIN
user_ids = fetch_all_terms_aggs(INDEX_FETCH, "user_id", size=BATCH_SIZE)

# 1️⃣ Собираем всех user_id
user_activity_map = defaultdict(list)

# 2️⃣ Генерируем карту активности (50% шанс стрима)
for user_id in user_ids:
    for _ in range(DAYS_BACK):
        user_activity_map[user_id["user_id"]].append(random.random() < 0.5)

print(f"✅ Сгенерирована карта активности для {len(user_ids)} пользователей.")

# 3️⃣ Собираем все уникальные timestamp за сутки
timestamps = fetch_all_terms_aggs(INDEX_FETCH, "timestamp", size=BATCH_SIZE)

print(f"Найдено {len(timestamps)} уникальных timestamp.")

import random
import datetime
from collections import defaultdict
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

# Конфиг
DAYS_BACK = 60  # На сколько дней назад генерировать данные
BATCH_SIZE = 5000  # Количество документов за один запрос
INDEX_FETCH = "twitch"
INDEX_PUT = "twitch_analytics"
FORBIDDEN_GAME = "Kingdom Come: Deliverance 2"
FORBIDDEN_GAME_CUTOFF = datetime.datetime(2024, 2, 4, 16, 0)  # 4 февраля 16:00 UTC

def fetch_all_terms_aggs(index, field, size=10000):
    aggs = []
    after_key = None

    while True:
        query = {
            "size": 0,
            "aggs": {
                "unique_terms": {
                    "composite": {
                        "sources": [
                            {field: {"terms": {"field": field}}}
                        ],
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

        # Если есть следующий "after" курсор, продолжаем пагинацию
        after_key = response["aggregations"]["unique_terms"].get("after_key")
        if not after_key:
            break

    return aggs


# MAIN
user_ids = fetch_all_terms_aggs(INDEX_FETCH, "user_id", size=BATCH_SIZE)

# 1️⃣ Собираем всех user_id
user_activity_map = defaultdict(list)

# 2️⃣ Генерируем карту активности (50% шанс стрима)
for user_id in user_ids:
    for _ in range(DAYS_BACK):
        user_activity_map[user_id["user_id"]].append(random.random() < 0.5)

print(f"✅ Сгенерирована карта активности для {len(user_ids)} пользователей.")

# 3️⃣ Собираем все уникальные timestamp за сутки
timestamps = fetch_all_terms_aggs(INDEX_FETCH, "timestamp", size=BATCH_SIZE)

print(f"Найдено {len(timestamps)} уникальных timestamp.")

# 3.1 Подсчитываем количество каждой игры по timestamp
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

    print(f"Загружено {len(all_docs)} документов.")

    # Собираем сколько раз каждая игра встречается
    for doc in all_docs:
        game_counts[doc["game_name"]] += 1

# Теперь у нас есть словарь `game_counts`, который хранит количество встреч каждой игры
# 3.2 Вычисляем проценты для каждой игры
total_games = sum(game_counts.values())
game_percentages = {game: count / total_games for game, count in game_counts.items()}

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

    print(f"Загружено {len(all_docs)} документов.")

    user_viewers = defaultdict(list)
    for doc in all_docs:
        user_viewers[doc["user_id"]].append(doc["viewers_count"])
    
    print(f"Пользователи сгруппированы")

    user_avg_viewers = {
        user: (sum(viewers) // len(viewers), min(viewers), max(viewers))
        for user, viewers in user_viewers.items()
    }

    # 4.3 Определяем топ-игры
    game_counts = defaultdict(int)
    for doc in all_docs:
        game_counts[doc["game_name"]] += 1

    popular_games = sorted(game_counts, key=game_counts.get, reverse=True)[:10]

    # print(f"Топ игры: {popular_games}")

    # 4.4 Генерируем данные за предыдущие дни
    new_docs = []
    timestamp_dt = datetime.datetime.utcfromtimestamp(timestamp/1000)
    
    for days_ago in range(1, DAYS_BACK + 1):
        new_timestamp = timestamp_dt - datetime.timedelta(days=days_ago)
        weekday = new_timestamp.weekday()  # 0 = Пн, 6 = Вс
    
        for doc in all_docs:
            user_id = doc["user_id"]

            # Проверяем, стримил ли этот user_id в этот день
            if not user_activity_map[user_id]:
                continue
            if not user_activity_map[user_id][days_ago - 1]:
                continue  # Пропускаем этот user_id

            new_doc = doc.copy()
            new_doc["timestamp"] = new_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            hour = new_timestamp.hour
            time_factor = 1.2 if hour in [18, 19, 20, 21, 22] else 0.8 if hour in [3, 4, 5, 6] else 1.0
            weekend_boost = random.uniform(1.1, 1.2) if weekday in [5, 6] else 1.0

            avg_viewers, min_viewers, max_viewers = user_avg_viewers.get(user_id, (doc["viewers_count"], 10, 5000))
            noise = int(random.gauss(0, avg_viewers * 0.05))
            new_viewers_count = max(0, int((avg_viewers + noise) * time_factor * weekend_boost))
            new_doc["viewers_count"] = min(max_viewers, max(min_viewers, new_viewers_count))

            # 90% — топ-игры, 10% — случайная игра
            if random.random() < 0.2:
                new_doc["game_name"] = random.choice(list(game_counts.keys()))
            elif "Just Chatting" in popular_games and random.random() < 0.4:
                new_doc["game_name"] = "Just Chatting"
            else:
                new_doc["game_name"] = random.choice(popular_games)

            if new_timestamp < FORBIDDEN_GAME_CUTOFF and new_game == FORBIDDEN_GAME:
                new_game = random.choice([g for g in popular_games if g != FORBIDDEN_GAME])
            
            new_docs.append({"index": {"_index": INDEX_PUT}})
            new_docs.append(new_doc)
    
        if new_docs:
            es.bulk(body=new_docs)
            print(f"  {len(new_docs)//2} новых записей добавлено за {new_timestamp.strftime('%Y-%m-%d')}.")

        new_docs = []  # Очищаем список перед следующим днём
