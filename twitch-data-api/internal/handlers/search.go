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

func Search(gClient *redis.Client, sClient *redis.Client) fiber.Handler {
	return func(c fiber.Ctx) error {
		q := c.Query("q")
		if q == "" {
			log.Printf("empty \"q\" search parameter\n")
			return fiber.ErrBadRequest
		}

		top, err := strconv.Atoi(c.Query("top", "5"))
		if err != nil {
			log.Printf("wrong \"top\" search parameter (%v)\n", top)
			return fiber.ErrBadRequest
		}

		log.Printf("SearchGames q=%s top=%d", q, top)

		gamesKeys := make([]string, 0, top)
		cursor := uint64(0)
		for {
			res, nextCursor, err := gClient.Scan(context.Background(), cursor, fmt.Sprintf("game:%s*", q), 10000).Result()
			if err != nil {
				log.Printf("failed to search games: %v\n", err)
				return fiber.ErrInternalServerError
			}
			gamesKeys = append(gamesKeys, res...)

			if len(gamesKeys) >= top {
				gamesKeys = gamesKeys[:top]
				break
			}

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}

		log.Println(gamesKeys)

		var gamesIds []interface{}
		if len(gamesKeys) > 0 {
			gamesIds, err = gClient.MGet(context.Background(), gamesKeys...).Result()
			if err != nil {
				log.Printf("failed to search games ids: %v\n", err)
				return fiber.ErrInternalServerError
			}
		}

		for i, v := range gamesKeys {
			gamesKeys[i] = v[5:] // remove "game:" from key
		}

		streamersKeys := make([]string, 0, top)
		cursor = uint64(0)
		for {
			res, nextCursor, err := sClient.Scan(context.Background(), cursor, fmt.Sprintf("streamer:%s*", q), 10000).Result()
			if err != nil {
				log.Printf("failed to search games: %v\n", err)
				return fiber.ErrInternalServerError
			}
			streamersKeys = append(streamersKeys, res...)

			if len(streamersKeys) >= top {
				streamersKeys = streamersKeys[:top]
				break
			}

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}

		var streamersIds []interface{}
		if len(streamersKeys) > 0 {
			streamersIds, err = sClient.MGet(context.Background(), streamersKeys...).Result()
			if err != nil {
				log.Printf("failed to search streamers ids: %v\n", err)
				return fiber.ErrInternalServerError
			}
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
			sgr.Streamers = append(sgr.Streamers, DataUnit{Id: streamersIds[i].(string), Name: streamersKeys[i]})
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
