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
	Games     []string `json:"data"`
	Streamers []string `json:"streamers"`
}

func Search(client *redis.Client) fiber.Handler {
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

		log.Printf("SearchGames startswith=%s top=%d", prefix, top)

		games, err := client.Keys(context.Background(), fmt.Sprintf("game:%s*", prefix)).Result()
		if err != nil {
			log.Printf("failed to search games: %v\n", err)
			return fiber.ErrInternalServerError
		}

		for i, v := range games {
			games[i] = v[5:] // remove "game:" from key
		}

		streamers, err := client.Keys(context.Background(), fmt.Sprintf("streamer:%s*", prefix)).Result()
		if err != nil {
			log.Printf("failed to search games: %v\n", err)
			return fiber.ErrInternalServerError
		}

		for i, v := range streamers {
			streamers[i] = v[5:] // remove "streamer:" from key
		}

		sgr := SearchGamesResponse{}
		if len(games) > top {
			sgr.Games = games[:top]
		} else {
			sgr.Games = games
		}

		if len(streamers) > top {
			sgr.Streamers = streamers[:top]
		} else {
			sgr.Streamers = streamers
		}

		data, err := json.Marshal(sgr)
		if err != nil {
			log.Printf("failed to marshal search results: %v\n", err)
			return fiber.ErrInternalServerError
		}

		log.Printf("Search result: %s", string(data))

		c.WriteString(string(data))
		return nil
	}
}
