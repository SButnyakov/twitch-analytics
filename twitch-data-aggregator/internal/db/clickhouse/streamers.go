package clickhouse

import (
	"context"
	"log"
	"time"
	"twitch-data-aggregator/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Streamer struct {
	Id   string
	Name string
}

type StreamerAvgViewers struct {
	Id           string
	ViewersCount uint32
}

func GetAllStreamers(conn driver.Conn) ([]Streamer, error) {
	start := time.Now()
	query := "SELECT DISTINCT user_id, username FROM twitch"

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]Streamer, 0)

	for rows.Next() {
		var streamer Streamer

		if err := rows.Scan(&streamer.Id, &streamer.Name); err != nil {
			return nil, err
		}

		res = append(res, streamer)
	}
	log.Printf("GetAllStreamers took %s\n", time.Since(start))
	return res, nil
}

func GetStreamerAvgViewerCountForNDays(conn driver.Conn, days int) ([]StreamerAvgViewers, error) {
	start := time.Now()
	query := `
	SELECT 
		user_id, 
		AVG(viewers_count) AS avg_viewers_count
	FROM
		twitch
	WHERE
		timestamp >= toTimeZone(now(), 'Europe/London') - INTERVAL (?) DAY
	GROUP BY
		user_id;
	`

	rows, err := conn.Query(context.Background(), query, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]StreamerAvgViewers, 0)

	for rows.Next() {
		var streamer StreamerAvgViewers
		var avgViewers float64

		if err := rows.Scan(&streamer.Id, &avgViewers); err != nil {
			return nil, err
		}

		streamer.ViewersCount = uint32(avgViewers)

		res = append(res, streamer)
	}

	log.Printf("GetStreamerAvgViewerCountFor%dDays took %s\n", days, time.Since(start))

	return res, nil
}

func GetStreamersOnlineTimepointsForNDays(conn driver.Conn, days int) (map[string]models.OnlineTimepoints, error) {
	start := time.Now()
	query := `
		SELECT 
			user_id,
			toStartOfMinute(timestamp) AS minute,
			SUM(viewers_count) AS total_viewers
		FROM 
			twitch
		WHERE 
			timestamp >= toTimeZone(now(), 'Europe/London') - INTERVAL (?) DAY
		GROUP BY 
			user_id, minute
		ORDER BY 
			user_id, minute;
	`

	rows, err := conn.Query(context.Background(), query, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[string]models.OnlineTimepoints, 0)

	for rows.Next() {
		var streamer_id string
		var online uint64
		var timestamp time.Time

		if err := rows.Scan(&streamer_id, &timestamp, &online); err != nil {
			return nil, err
		}

		v := res[streamer_id]
		vArr := append(v.Data, models.OnlineTimepoint{Online: online, Timestamp: timestamp})
		v.Data = vArr
		res[streamer_id] = v
	}

	log.Printf("GetStreamersOnlineTimepointsFor%dDays took %s\n", days, time.Since(start))

	return res, nil
}
