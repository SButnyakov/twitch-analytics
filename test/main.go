package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	broker = "localhost:9092"
	topic  = "twitch_data"
)

func main() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		log.Fatalf("Не удалось подключиться к Kafka: %v", err)
	}
	defer conn.Close()

	// Пинг для проверки соединения
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		log.Fatalf("Ошибка установки дедлайна: %v", err)
	}
	if _, err := conn.WriteMessages(); err != nil {
		log.Fatalf("Проверка соединения не удалась: %v", err)
	}

	fmt.Println("Успешно подключено к Kafka и соединение проверено.")
}
