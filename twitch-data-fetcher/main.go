package main

import (
	"fmt"
	"log"
	"time"
	"twitch-data-fetcher/config"
	"twitch-data-fetcher/internal/broker/kafka"
	"twitch-data-fetcher/internal/endpoints"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	cfg := config.MustLoad()

	fmt.Println(cfg)

	conn, err := kafka.Connect(cfg)
	if err != nil {
		panic(err)
	}

	log.Printf("URLs: %v\n", cfg.URLs)

	for {
		go func() {
			token, err := endpoints.GetToken(cfg)
			if err != nil {
				panic(err)
			}
			fmt.Println(token)

			cfg.Auth.Token = token

			log.Printf("Access token: %s\n", cfg.Auth.Token.AccessToken)
			log.Printf("Expires in: %d\n", cfg.Auth.Token.ExpiresIn)

			start := time.Now()
			streams, err := endpoints.GetStreams(cfg)
			if err != nil {
				panic(err)
			}
			elapsed := time.Since(start)
			log.Printf("GetStreams length %d\n", len(streams))
			log.Printf("GetStreams time took %s\n", elapsed)

			log.Println("Sending messages...")

			kafka.WriteStreamsMessage(cfg, conn, streams)

			log.Println("Message sent")
		}()
		time.Sleep(time.Duration(cfg.FetchIntervalMins) * time.Minute)
	}
}

/*
	m := make(map[string]endpoints.Stream, 3)
	for i := 0; i < 3; i++ {
		m[string(i)] = endpoints.Stream{
			UserLogin:   fmt.Sprintf("userlogin%d", i),
			UserName:    fmt.Sprintf("username%d", i),
			GameId:      fmt.Sprintf("gameid%d", i),
			GameName:    fmt.Sprintf("gamename%d", i),
			ViewerCount: uint32(i * 100),
			Language:    "RU",
			Timestamp:   time.Now(),
		}
	}

	clickhouse.Insert(conn, m)
	clickhouse.Select(conn)
*/
