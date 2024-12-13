package main

import (
	"fmt"
	"time"
	"twitch-data-fetcher/config"
	"twitch-data-fetcher/endpoints"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	cfg := config.MustLoad()

	token, err := endpoints.GetToken(cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println(token)

	cfg.Auth.Token = token

	fmt.Printf("Access token: %s\n", cfg.Auth.Token.AccessToken)
	fmt.Printf("Expires in: %d\n", cfg.Auth.Token.ExpiresIn)

	fmt.Printf("URLs: %v\n", cfg.URLs)

	start := time.Now()
	streams, err := endpoints.GetStreams(cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println(streams)
	fmt.Println(len(streams))
	elapsed := time.Since(start)
	fmt.Printf("Time took %s\n", elapsed)

	m := make(map[string]int)
	for _, stream := range streams {
		m[stream.Language] = m[stream.Language] + 1
	}
	fmt.Println(m)
}
