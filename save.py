from elasticsearch import Elasticsearch
import csv

# Конфигурация Elasticsearch
ES_HOST = "http://localhost:9200"  # Адрес Elasticsearch
INDEX = "twitch"  # Индекс, который выгружаем
SCROLL_TIME = "1m"  # Время жизни scroll-сессии
BATCH_SIZE = 10000  # Количество документов за одну итерацию
OUTPUT_CSV = "twitch_data.csv"  # Имя выходного файла

def fetch_data():
    """Функция выгружает данные из Elasticsearch с пагинацией и сохраняет в CSV"""

    # Подключение к Elasticsearch
    es = Elasticsearch(ES_HOST, request_timeout=60)

    # 1. Первый запрос для инициализации scroll
    response = es.search(
        index=INDEX,
        scroll=SCROLL_TIME,
        size=BATCH_SIZE,
        body={"query": {"match_all": {}}},
        _source=True
    )

    # 2. Получаем scroll_id и первую партию данных
    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]

    if not hits:
        print("Индекс пустой или данные недоступны.")
        return

    # 3. Определяем заголовки CSV по первым данным
    fieldnames = list(hits[0]["_source"].keys())

    i = 0
    counter = 1
    first = True
    while True:
        # 4. Открываем CSV файл и записываем заголовки
        with open(f"twitch_data_{counter}.csv", "w", newline="", encoding="utf-8") as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csv_writer.writeheader()

            if first == True:
                # 5. Записываем первую порцию данных
                for hit in hits:
                    csv_writer.writerow(hit["_source"])

                print(f"Записано {len(hits)} записей...")
                first = False
                i += 1

            # 6. Продолжаем итерации с scroll
            while len(hits) > 0:
                response = es.scroll(scroll_id=scroll_id, scroll=SCROLL_TIME)
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]

                for hit in hits:
                    csv_writer.writerow(hit["_source"])

                print(f"Записано ещё {len(hits)} записей...")
                i += 1
                if i >= 500:
                    i = 0
                    counter += 1
                    break
            
            if len(hits) == 0:
                break

    # 7. Удаляем scroll-сессию
    es.clear_scroll(scroll_id=scroll_id)
    print(f"Готово! Данные сохранены в {OUTPUT_CSV}")

if __name__ == "__main__":
    fetch_data()
