package clickhouse

import (
	"context"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type StreamerAvgViewers struct {
	Name         string
	ViewersCount uint32
}

func GetAllStreamers(conn driver.Conn) ([]string, error) {
	start := time.Now()
	query := "SELECT DISTINCT username FROM twitch"

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
	log.Printf("GetAllStreamers took %s\n", time.Since(start))
	return res, nil
}

func GetStreamerAvgViewerCountForNDays(conn driver.Conn, days int) ([]GameAvgViewers, error) {
	start := time.Now()
	query := `
	SELECT 
		username, 
		AVG(viewers_count) AS avg_viewers_count
	FROM
		twitch
	WHERE
		timestamp >= toTimeZone(now(), 'Europe/London') - INTERVAL (?) DAY
	GROUP BY
		username;
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
