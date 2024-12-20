package clickhouse

import (
	"context"
	"log"
	"time"
	"twitch-data-aggregator/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type GameAvgViewers struct {
	Game         string
	ViewersCount uint32
}

func GetAllGames(conn driver.Conn) ([]string, error) {
	start := time.Now()
	query := "SELECT DISTINCT game_name FROM twitch"

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]string, 0)

	for rows.Next() {
		var gamename string

		if err := rows.Scan(&gamename); err != nil {
			return nil, err
		}

		res = append(res, gamename)
	}
	log.Printf("GetAllGames took %s\n", time.Since(start))
	return res, nil
}

func GetGameAvgViewerCountForNDays(conn driver.Conn, days int) ([]GameAvgViewers, error) {
	start := time.Now()
	query := `
	SELECT 
		game_name, 
		AVG(viewers_count) AS avg_viewers_count
	FROM
		twitch
	WHERE
		timestamp >= toTimeZone(now(), 'Europe/London') - INTERVAL (?) DAY
	GROUP BY
		game_name;
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

		if err := rows.Scan(&game.Game, &avgViewers); err != nil {
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
			game_name,
			toStartOfMinute(timestamp) AS minute,
			SUM(viewers_count) AS total_viewers
		FROM 
			twitch
		WHERE 
			timestamp >= toTimeZone(now(), 'Europe/London') - INTERVAL (?) DAY
		GROUP BY 
			game_name, minute
		ORDER BY 
			game_name, minute;
	`

	rows, err := conn.Query(context.Background(), query, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[string]models.OnlineTimepoints, 0)

	for rows.Next() {
		var game string
		var online uint64
		var timestamp time.Time

		if err := rows.Scan(&game, &timestamp, &online); err != nil {
			return nil, err
		}

		v := res[game]
		vArr := append(v.Data, models.OnlineTimepoint{Online: online, Timestamp: timestamp})
		v.Data = vArr
		res[game] = v
	}

	log.Printf("GetGamesOnlineTimepointsFor%dDays took %s\n", days, time.Since(start))

	return res, nil
}
