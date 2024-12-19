package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
)

type SearchGamesResponse struct {
	Data []string `json:"data"`
}

func SearchGames(client *redis.Client) fiber.Handler {
	return func(c fiber.Ctx) error {
		prefix := c.Query("startswith")
		if prefix == "" {
			log.Printf("empty \"startswith\" search parameter\n")
			return fiber.ErrBadRequest
		}
		log.Printf("\"startswith\"=%s", prefix)

		top, err := strconv.Atoi(c.Query("top", "5"))
		if err != nil {
			log.Printf("wrong \"top\" search parameter (%v)\n", top)
			return fiber.ErrBadRequest
		}
		log.Printf("\"top\"=%d", top)

		res, err := client.Keys(context.Background(), fmt.Sprintf("game:%s*", prefix)).Result()
		if err != nil {
			log.Printf("failed to search games: %v\n", err)
			return fiber.ErrInternalServerError
		}

		for i, v := range res {
			res[i] = v[5:]
		}

		sgr := SearchGamesResponse{}
		if len(res) > top {
			sgr.Data = res[:top]
		} else {
			sgr.Data = res
		}

		data, err := json.Marshal(sgr)
		if err != nil {
			log.Printf("failed to marshal games: %v\n", err)
			return fiber.ErrInternalServerError
		}

		c.WriteString(string(data))
		return nil
	}
}
