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

func Aggregate(cfg *config.Config, conn driver.Conn, clients []*redis.Client) {
	var wg sync.WaitGroup
	wg.Add(6)
	go func() {
		games, err := clickhouse.GetAllGames(conn)
		if err != nil {
			log.Printf("failed to get all games: %v\n", err)
		} else {
			m := make(map[string]interface{}, len(games))
			for _, v := range games {
				m[fmt.Sprintf("game:%s", v.Name)] = v.Id
			}
			if err := clients[cfg.Games].MSet(context.Background(), m).Err(); err != nil {
				log.Printf("failed to save all games: %v\n", err)
			}
			clear(m)
			for _, v := range games {
				m[fmt.Sprintf("game:%s", v.Id)] = v.Name
			}
			if err := clients[cfg.Games].MSet(context.Background(), m).Err(); err != nil {
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
				m[fmt.Sprintf("streamer:%s", v.Name)] = v.Id
			}
			if err := clients[cfg.Streamers].MSet(context.Background(), m).Err(); err != nil {
				log.Printf("failed to save all streamers: %v\n", err)
			}
			clear(m)
			for _, v := range streamers {
				m[fmt.Sprintf("streamer:%s", v.Id)] = v.Name
			}
			if err := clients[cfg.Streamers].MSet(context.Background(), m).Err(); err != nil {
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
				continue
			}

			m := make(map[string]interface{}, len(gamesAvgViewers))
			for _, v := range gamesAvgViewers {
				m[fmt.Sprintf("average_game_online:%d:%s", days, v.Id)] = v.ViewersCount
			}

			if err := clients[cfg.GamesAvgOnline].MSet(context.Background(), m).Err(); err != nil {
				log.Printf("failed to save average games online for %d days: %v\n", days, err)
				continue
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
				continue
			}

			m := make(map[string]interface{}, len(streamersAvgViewers))
			for _, v := range streamersAvgViewers {
				m[fmt.Sprintf("average_streamer_online:%d:%s", days, v.Id)] = v.ViewersCount
			}

			if err := clients[cfg.StreamersAvgOnline].MSet(context.Background(), m).Err(); err != nil {
				log.Printf("failed to save average streamers online for %d days: %v\n", days, err)
				continue
			}

			log.Printf("average streamers online for %d days saved\n", days)
		}
		wg.Done()
	}()

	go func() {
		for _, days := range periods {
			gamesTimepoints, err := clickhouse.GetGamesOnlineTimepointsForNDays(conn, days)
			if err != nil {
				log.Printf("failed to get games online timepoints for %d days: %v\n", days, err)
				continue
			}

			m := make(map[string]interface{}, len(gamesTimepoints))
			for k, v := range gamesTimepoints {
				m[fmt.Sprintf("games_online_timepoints:%d:%s", days, k)] = v
			}

			if err := clients[cfg.GamesTimepoints].MSet(context.Background(), m).Err(); err != nil {
				log.Printf("failed to save games online timepoints for %d days: %v\n", days, err)
				continue
			}

			log.Printf("games online timepoints for %d days saved\n", days)
		}
		wg.Done()
	}()

	go func() {
		for _, days := range periods {
			streamersTimepoints, err := clickhouse.GetStreamersOnlineTimepointsForNDays(conn, days)
			if err != nil {
				log.Printf("failed to get streamers online timepoints for %d days: %v\n", days, err)
				continue
			}

			m := make(map[string]interface{}, len(streamersTimepoints))
			for k, v := range streamersTimepoints {
				m[fmt.Sprintf("streamers_online_timepoints:%d:%s", days, k)] = v
				if len(m) == 100000 {
					if err := clients[cfg.StreamersTimepoints].MSet(context.Background(), m).Err(); err != nil {
						log.Printf("failed to save streamers online timepoints for %d days: %v\n", days, err)
						continue
					}
					clear(m)
				}
			}

			log.Printf("streamers online timepoints for %d days saved\n", days)
		}
		wg.Done()
	}()

	wg.Wait()
}
