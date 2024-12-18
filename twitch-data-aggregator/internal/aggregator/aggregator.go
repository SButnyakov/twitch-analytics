package aggregator

import (
	"context"
	"fmt"
	"log"
	"twitch-data-aggregator/config"
	"twitch-data-aggregator/internal/db/clickhouse"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
)

var (
	periods = []int{1, 7, 30}
)

func Aggregate(cfg *config.Config, conn driver.Conn, client *redis.Client) {
	for _, days := range periods {
		gamesAvgViewers, err := clickhouse.GetGameAvgViewerCountForNDays(conn, days)
		if err != nil {
			log.Printf("failed to get average games online for %d days: %v\n", days, err)
		}

		m := make(map[string]interface{}, len(gamesAvgViewers))
		for _, v := range gamesAvgViewers {
			m[fmt.Sprintf("average_game_online:%d:%s", days, v.Game)] = v.ViewersCount
		}

		if err := client.MSet(context.Background(), m).Err(); err != nil {
			log.Printf("failed to save average games online for %d days: %v\n", days, err)
		}

		log.Printf("average games online for %d days saved\n", days)
	}

	for _, days := range periods {
		streamersAvgViewers, err := clickhouse.GetStreamerAvgViewerCountForNDays(conn, days)
		if err != nil {
			log.Printf("failed to get average streamers online for %d days: %v\n", days, err)
		}

		m := make(map[string]interface{}, len(streamersAvgViewers))
		for _, v := range streamersAvgViewers {
			m[fmt.Sprintf("average_streamer_online:%d:%s", days, v.Game)] = v.ViewersCount
		}

		if err := client.MSet(context.Background(), m).Err(); err != nil {
			log.Printf("failed to save average streamers online for %d days: %v\n", days, err)
		}

		log.Printf("average streamers online for %d days saved\n", days)
	}
}
