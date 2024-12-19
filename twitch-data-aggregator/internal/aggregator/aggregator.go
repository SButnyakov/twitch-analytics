package aggregator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"twitch-data-aggregator/config"
	"twitch-data-aggregator/internal/db/clickhouse"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
)

var (
	periods = []int{1, 7, 30}
)

func Aggregate(cfg *config.Config, conn driver.Conn, client *redis.Client) {
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		games, err := clickhouse.GetAllGames(conn)
		if err != nil {
			log.Printf("failed to get all games: %v\n", err)
		} else {
			m := make(map[string]interface{}, len(games))
			for _, v := range games {
				m[fmt.Sprintf("game:%s", v)] = true
			}
			if err := client.MSet(context.Background(), m).Err(); err != nil {
				log.Printf("failed to save all games: %v\n", err)
			}
		}
		wg.Done()
	}()

	go func() {
		streamers, err := clickhouse.GetAllStreamers(conn)
		if err != nil {
			log.Printf("failed to get all streamers: %v\n", err)
		} else {
			m := make(map[string]interface{}, len(streamers))
			for _, v := range streamers {
				m[fmt.Sprintf("streamer:%s", v)] = true
			}
			if err := client.MSet(context.Background(), m).Err(); err != nil {
				log.Printf("failed to save all streamers: %v\n", err)
			}
		}
		wg.Done()
	}()

	go func() {
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
		wg.Done()
	}()

	go func() {
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
		wg.Done()
	}()

	wg.Wait()
}
