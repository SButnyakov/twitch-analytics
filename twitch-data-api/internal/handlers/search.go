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
	Games     []DataUnit `json:"games"`
	Streamers []DataUnit `json:"streamers"`
}

type DataUnit struct {
	Id   string `json:"id"`
	Name string `json:"name"`
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

		gamesKeys, err := client.Keys(context.Background(), fmt.Sprintf("game:%s*", prefix)).Result()
		if err != nil {
			log.Printf("failed to search games: %v\n", err)
			return fiber.ErrInternalServerError
		}

		gamesIds, err := client.MGet(context.Background(), gamesKeys...).Result()
		if err != nil {
			log.Printf("failed to search games ids: %v\n", err)
			return fiber.ErrInternalServerError
		}

		for i, v := range gamesKeys {
			gamesKeys[i] = v[5:] // remove "game:" from key
		}

		streamersKeys, err := client.Keys(context.Background(), fmt.Sprintf("streamer:%s*", prefix)).Result()
		if err != nil {
			log.Printf("failed to search games: %v\n", err)
			return fiber.ErrInternalServerError
		}

		streamersIds, err := client.MGet(context.Background(), streamersKeys...).Result()
		if err != nil {
			log.Printf("failed to search streamers ids: %v\n", err)
			return fiber.ErrInternalServerError
		}

		for i, v := range streamersKeys {
			streamersKeys[i] = v[5:] // remove "streamer:" from key
		}

		sgr := SearchGamesResponse{}
		for i := 0; i < top; i++ {
			if i == len(gamesKeys) {
				break
			}
			sgr.Games = append(sgr.Games, DataUnit{Id: gamesIds[i].(string), Name: gamesKeys[i]})
		}

		for i := 0; i < top; i++ {
			if i == len(streamersKeys) {
				break
			}
			sgr.Games = append(sgr.Streamers, DataUnit{Id: streamersIds[i].(string), Name: streamersKeys[i]})
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
