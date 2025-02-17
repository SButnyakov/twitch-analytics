package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
)

type AvgGameViewsResponse struct {
	Views uint32 `json:"views"`
}

func AvgGameViews(client *redis.Client) fiber.Handler {
	return func(c fiber.Ctx) error {
		days, err := strconv.Atoi(c.Query("days"))
		if err != nil {
			log.Printf("failed to parse days parameter: %v\n", err)
			return fiber.ErrBadRequest
		}
		game := strings.ReplaceAll(c.Params("game"), "%20", " ")

		log.Printf("avgonline %s %d\n", game, days)

		res := client.Get(context.Background(), fmt.Sprintf("average_game_online:%d:%s", days, game))
		if res.Err() != nil {
			log.Printf("game not found: %v\n", err)
			return fiber.ErrNotFound
		}

		online, err := strconv.ParseUint(res.Val(), 10, 32)
		if err != nil {
			log.Printf("failed to parse game's online: %v\n", err)
			return fiber.ErrInternalServerError
		}

		response := AvgGameViewsResponse{
			Views: uint32(online),
		}

		data, err := json.Marshal(response)
		if err != nil {
			log.Printf("failed to marhal response (%v): %v", response, err)
		}

		c.WriteString(string(data))

		return nil
	}
}
