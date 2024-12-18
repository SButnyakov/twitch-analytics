package main

import (
	"log"
	"twitch-data-api/config"
	"twitch-data-api/internal/db/redis"
	"twitch-data-api/internal/handlers"

	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	cfg := config.MustLoad()
	log.Println(cfg)

	client, err := redis.Connect(cfg)
	if err != nil {
		panic(err)
	}

	app := fiber.New()

	app.Get("/avgonline/games/:game", handlers.AvgGameViews(client))
	app.Get("/avgonline/streamers/:streamer", handlers.AvgStreamerViews(client))

	log.Fatal(app.Listen(cfg.HTTP.Address))
}
