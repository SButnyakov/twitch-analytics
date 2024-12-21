package clickhouse

import (
	"context"
	"log"
	"time"
	"twitch-data-aggregator/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Game struct {
	Id   string
	Name string
}

type GameAvgViewers struct {
	Id           string
	ViewersCount uint32
}

func GetAllGames(conn driver.Conn) ([]Game, error) {
	start := time.Now()
	query := "SELECT DISTINCT game_id, game_name FROM twitch"

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]Game, 0)

	for rows.Next() {
		var game Game

		if err := rows.Scan(&game.Id, &game.Name); err != nil {
			return nil, err
		}

		res = append(res, game)
	}
	log.Printf("GetAllGames took %s\n", time.Since(start))
	return res, nil
}

func GetGameAvgViewerCountForNDays(conn driver.Conn, days int) ([]GameAvgViewers, error) {
	start := time.Now()
	query := `
	SELECT 
		game_id, 
		AVG(viewers_count) AS avg_viewers_count
	FROM
		twitch
	WHERE
		timestamp >= toTimeZone(now(), 'Europe/London') - INTERVAL (?) DAY
	GROUP BY
		game_id;
	`

	rows, err := conn.Query(context.Background(), query, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]GameAvgViewers, 0)

	for rows.Next() {
		var game GameAvgViewers
		var avgViewers float64

		if err := rows.Scan(&game.Id, &avgViewers); err != nil {
			return nil, err
		}

		game.ViewersCount = uint32(avgViewers)

		res = append(res, game)
	}

	log.Printf("GetGameAvgViewerCountFor%dDays took %s\n", days, time.Since(start))

	return res, nil
}

func GetGamesOnlineTimepointsForNDays(conn driver.Conn, days int) (map[string]models.OnlineTimepoints, error) {
	start := time.Now()
	query := `
		SELECT 
			game_id,
			toStartOfMinute(timestamp) AS minute,
			SUM(viewers_count) AS total_viewers
		FROM 
			twitch
		WHERE 
			timestamp >= toTimeZone(now(), 'Europe/London') - INTERVAL (?) DAY
		GROUP BY 
			game_id, minute
		ORDER BY 
			game_id, minute;
	`

	rows, err := conn.Query(context.Background(), query, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[string]models.OnlineTimepoints, 0)

	for rows.Next() {
		var game_id string
		var online uint64
		var timestamp time.Time

		if err := rows.Scan(&game_id, &timestamp, &online); err != nil {
			return nil, err
		}

		v := res[game_id]
		vArr := append(v.Data, models.OnlineTimepoint{Online: online, Timestamp: timestamp})
		v.Data = vArr
		res[game_id] = v
	}

	log.Printf("GetGamesOnlineTimepointsFor%dDays took %s\n", days, time.Since(start))

	return res, nil
}
