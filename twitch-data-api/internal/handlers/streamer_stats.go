package handlers

import (
	"encoding/json"
	"log"
	"strconv"
	"twitch-data-api/internal/db/clickhouse"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v3"
)

type StreamerStatsResponse struct {
	Id           string  `json:"id"`
	Name         string  `json:"name"`
	AvgViewers   float64 `json:"avgonline"`
	GlobalRank   int     `json:"globalrank"`
	LanguageRank int     `json:"languagerank"`
	TopGameId    string  `json:"topgameid"`
}

func StreamerStats(conn driver.Conn) fiber.Handler {
	return func(c fiber.Ctx) error {
		id, err := strconv.Atoi(c.Params("streamer"))
		if err != nil {
			log.Printf("failed to parse streamer id: %v\n", err)
			return fiber.ErrBadRequest
		}

		log.Printf("streamer stats %d\n", id)

		stats, err := clickhouse.GetStreamerStats(conn, id)
		if err != nil {
			log.Printf("failed to get streamer stats: %v\n", err)
			return fiber.ErrInternalServerError
		}

		response := StreamerStatsResponse{
			Id:           stats.UserID,
			Name:         stats.Username,
			AvgViewers:   stats.AvgViewers,
			GlobalRank:   stats.GlobalRank,
			LanguageRank: stats.LanguageRank,
			TopGameId:    stats.TopGameID,
		}

		data, err := json.Marshal(response)
		if err != nil {
			log.Printf("failed to marshal response (%v): %v\n", response, err)
		}

		c.WriteString(string(data))

		return nil
	}
}
