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

type SearchStreamersResponse struct {
	Data []string `json:"data"`
}

func SearchStreamers(client *redis.Client) fiber.Handler {
	return func(c fiber.Ctx) error {
		prefix := c.Query("startswith")
		if prefix == "" {
			log.Printf("empty \"startswith\" search parameter\n")
			return fiber.ErrBadRequest
		}

		top, err := strconv.Atoi(c.Query("top", "5"))
		if err != nil {
			log.Printf("wrong \"top\" search parameter (%v)\n", top)
			return fiber.ErrBadRequest
		}

		log.Printf("SearchStreamers startswith=%s top=%d", prefix, top)

		res, err := client.Keys(context.Background(), fmt.Sprintf("streamer:%s*", prefix)).Result()
		if err != nil {
			log.Printf("failed to search streamers: %v\n", err)
			return fiber.ErrInternalServerError
		}

		for i, v := range res {
			res[i] = v[9:]
		}

		sgr := SearchGamesResponse{}
		if len(res) > top {
			sgr.Data = res[:top]
		} else {
			sgr.Data = res
		}

		data, err := json.Marshal(sgr)
		if err != nil {
			log.Printf("failed to marshal streamers: %v\n", err)
			return fiber.ErrInternalServerError
		}

		log.Printf("SearchStreamers result: %s", string(data))

		c.WriteString(string(data))
		return nil
	}
}
