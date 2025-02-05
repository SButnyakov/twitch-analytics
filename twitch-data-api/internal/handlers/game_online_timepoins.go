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

func GameOnlineTimepoints(client *redis.Client) fiber.Handler {
	return func(c fiber.Ctx) error {
		days, err := strconv.Atoi(c.Query("days"))
		if err != nil {
			log.Printf("failed to parse days parameter: %v\n", err)
			return fiber.ErrBadRequest
		}
		game := strings.ReplaceAll(c.Params("game"), "%20", " ")

		log.Printf("GameOnlineTimepoints %s %d\n", game, days)

		res := client.Get(context.Background(), fmt.Sprintf("games_online_timepoints:%d:%s", days, game))
		if res.Err() != nil {
			log.Printf("game not found: %v\n", err)
			return fiber.ErrNotFound
		}

		log.Print(res.Val())

		c.WriteString(res.Val())

		return nil
	}
}
