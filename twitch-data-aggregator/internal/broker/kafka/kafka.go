package kafka

import (
	"context"
	"encoding/json"
	"log"
	"twitch-data-aggregator/config"

	"github.com/segmentio/kafka-go"
)

type StreamsJSONMessage struct {
	UserId      string `json:"user_id"`
	UserLogin   string `json:"user_login"`
	UserName    string `json:"user_name"`
	GameId      string `json:"game_id"`
	GameName    string `json:"game_name"`
	ViewerCount uint32 `json:"viewer_count"`
	Language    string `json:"language"`
}

type Message struct {
	Data       []StreamsJSONMessage `json:"data"`
	IsFinished bool                 `json:"is_finished"`
}

func ReadMessages(cfg *config.Config, messageChan chan Message) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.Kafka.Broker},
		GroupID:  cfg.Kafka.GroupID,
		Topic:    cfg.Kafka.Topic,
		MaxBytes: 20e6,
	})

	for {
		log.Println("waiting for message")
		msg, _ := r.ReadMessage(context.Background())

		var message Message
		err := json.Unmarshal(msg.Value, &message)
		if err != nil {
			log.Printf("failed to unmarshal message: %v\n", err)
			continue
		}

		messageChan <- message
	}
}
