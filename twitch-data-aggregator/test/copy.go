package main

import (
	"log"
	"twitch-data-aggregator/config"
	"twitch-data-aggregator/internal/broker/kafka"
	"twitch-data-aggregator/internal/db/clickhouse"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		panic(err)
	}

	cfg := config.MustLoad()
	log.Println(cfg)

	conn, err := clickhouse.Connect(cfg)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	data, err := clickhouse.Select(conn)
	if err != nil {
		log.Fatal(err)
	}

	minData := data[0].Timestamp
	maxData := data[0].Timestamp

	for _, v := range data {
		if v.Timestamp.Before(minData) {
			minData = v.Timestamp
		} else if v.Timestamp.After(maxData) {
			maxData = v.Timestamp
		}
	}

	delta := maxData.Sub(minData)

	for i := 0; i < 4319; i++ {
		jsons := make([]kafka.StreamsJSONMessage, len(data))
		for _, v := range data {
			v.Timestamp = v.Timestamp.Add(delta)
			jsons = append(jsons, kafka.StreamsJSONMessage{
				UserId:      v.UserId,
				UserLogin:   v.UserLogin,
				UserName:    v.UserName,
				GameId:      v.GameId,
				GameName:    v.GameName,
				ViewerCount: v.ViewersCount,
				Language:    v.Language,
				Timestamp:   v.Timestamp,
			})
		}
		clickhouse.Insert(conn, jsons)
	}
}
