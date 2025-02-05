package main

import (
	"log"
	"twitch-data-api/config"
	"twitch-data-api/internal/db/clickhouse"
	"twitch-data-api/internal/db/redis"
	"twitch-data-api/internal/handlers"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	cfg := config.MustLoad()
	log.Println(cfg)

	clients, err := redis.Connect(cfg)
	if err != nil {
		panic(err)
	}

	conn, err := clickhouse.Connect(cfg)
	if err != nil {
		panic(err)
	}

	app := fiber.New()

	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:3000"},
	}))

	app.Get("/avgonline/games/:game", handlers.AvgGameViews(clients[cfg.GamesAvgOnline]))
	app.Get("/avgonline/streamers/:streamer", handlers.AvgStreamerViews(clients[cfg.StreamersAvgOnline]))
	app.Get("/timepoints/games/:game", handlers.GameOnlineTimepoints(clients[cfg.GamesTimepoints]))
	app.Get("/timepoints/streamers/:streamer", handlers.StreamerOnlineTimepoints(clients[cfg.StreamersTimepoints]))
	app.Get("/stats/streamers/:streamer", handlers.StreamerStats(conn))
	app.Get("/search", handlers.Search(clients[cfg.Games], clients[cfg.Streamers]))

	log.Fatal(app.Listen(cfg.HTTP.Address))
}
