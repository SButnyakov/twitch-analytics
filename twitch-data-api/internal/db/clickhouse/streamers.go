package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type StreamerStats struct {
	UserID       string
	Username     string
	AvgViewers   float64
	GlobalRank   int
	LanguageRank int
	TopGameID    string
}

func GetStreamerStats(conn driver.Conn, userID int) (*StreamerStats, error) {
	query := `
	WITH 
	    user_avg AS (
	        SELECT user_id, username, AVG(viewers_count) AS avg_viewers 
	        FROM twitch 
	        WHERE timestamp >= now() - INTERVAL 30 DAY AND user_id = ?
	        GROUP BY user_id, username
	    ),
	    ranked_users AS (
	        SELECT user_id, avg_viewers, RANK() OVER (ORDER BY avg_viewers DESC) AS global_rank 
	        FROM (SELECT user_id, AVG(viewers_count) AS avg_viewers FROM twitch WHERE timestamp >= now() - INTERVAL 30 DAY GROUP BY user_id)
	    ),
	    ranked_lang AS (
	        SELECT user_id, language, AVG(viewers_count) AS avg_viewers, 
	               RANK() OVER (PARTITION BY language ORDER BY AVG(viewers_count) DESC) AS language_rank 
	        FROM twitch 
	        WHERE timestamp >= now() - INTERVAL 30 DAY 
	        GROUP BY user_id, language
	    )
	SELECT u.user_id, u.username, u.avg_viewers, r.global_rank, rl.language_rank,
	       (
	            SELECT game_id 
	            FROM twitch 
	            WHERE user_id = u.user_id AND timestamp >= now() - INTERVAL 30 DAY 
	            GROUP BY game_id 
	            ORDER BY AVG(viewers_count) DESC 
	            LIMIT 1
	       ) AS top_game_id
	FROM user_avg u
	LEFT JOIN ranked_users r ON u.user_id = r.user_id
	LEFT JOIN ranked_lang rl ON u.user_id = rl.user_id;
	`

	row := conn.QueryRow(context.Background(), query, userID)

	var s StreamerStats
	if err := row.Scan(&s.UserID, &s.Username, &s.AvgViewers, &s.GlobalRank, &s.LanguageRank, &s.TopGameID); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return &s, nil
}
