package handlers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
)

func StreamerOnlineTimepoints(client *redis.Client) fiber.Handler {
	return func(c fiber.Ctx) error {
		days, err := strconv.Atoi(c.Query("days"))
		if err != nil {
			log.Printf("failed to parse days parameter: %v\n", err)
			return fiber.ErrBadRequest
		}
		game := strings.ReplaceAll(c.Params("streamer"), "%20", " ")

		log.Printf("StreamerOnlineTimepoints %s %d\n", game, days)

		res := client.Get(context.Background(), fmt.Sprintf("streamers_online_timepoints:%d:%s", days, game))
		if res.Err() != nil {
			log.Printf("streamer not found: %v\n", err)
			return fiber.ErrNotFound
		}

		c.WriteString(res.Val())

		return nil
	}
}
