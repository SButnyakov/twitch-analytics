package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"twitch-data-fetcher/config"
	"twitch-data-fetcher/internal/endpoints"

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

func Connect(cfg *config.Config) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", cfg.Kafka.Broker, cfg.Kafka.Topic, cfg.Kafka.Partition)
	return conn, err
}

func WriteStreamsMessage(cfg *config.Config, conn *kafka.Conn, streams map[string]endpoints.Stream) {
	streamsArr := make([]StreamsJSONMessage, 0, len(streams))
	for _, v := range streams {
		streamsArr = append(streamsArr, streamsToStreamJSONMessage(v))
	}

	messages := make([]kafka.Message, 0)
	for i := 0; i < len(streamsArr); i += 5000 {
		data := Message{}
		if i+5000 > len(streamsArr) {
			data.Data = streamsArr[i:]
			data.IsFinished = true
		} else {
			data.Data = streamsArr[i : i+5000]
			data.IsFinished = false
		}

		json_data, err := json.Marshal(data)
		if err != nil {
			log.Printf("failed to encode message: %v\n", err)
		}

		messages = append(messages, kafka.Message{Value: json_data})
	}

	messagesLength := 0
	for _, v := range messages {
		messagesLength += len(v.Value)
	}

	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	writtenBytes, err := conn.WriteMessages(messages...)
	if writtenBytes == messagesLength {
		log.Println("all bytes successfully written")
	} else {
		log.Printf("failed to write %d bytes\n", messagesLength-writtenBytes)
	}
	if err != nil {
		log.Printf("failed to write messages: %v\n", err)
		conn.Close()
		conn, err = Connect(cfg)
		if err != nil {
			log.Printf("failed to reconnect kafka: %v\n", err)
		} else {
			log.Printf("successfull kafka reconnect: %v\n", err)
			conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			writtenBytes, err = conn.WriteMessages(messages...)
			if writtenBytes == messagesLength {
				log.Println("all bytes successfully written")
			} else {
				log.Printf("failed to write %d bytes\n", messagesLength-writtenBytes)
			}
			if err != nil {
				log.Printf("failed to write messages: %v\n", err)
			}
		}
	}
}

func streamsToStreamJSONMessage(stream endpoints.Stream) StreamsJSONMessage {
	return StreamsJSONMessage{
		UserId:      stream.UserId,
		UserLogin:   stream.UserLogin,
		UserName:    stream.UserName,
		GameId:      stream.GameId,
		GameName:    stream.GameName,
		ViewerCount: stream.ViewerCount,
		Language:    stream.Language,
	}
}
