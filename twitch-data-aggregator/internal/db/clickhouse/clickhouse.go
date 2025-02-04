package clickhouse

import (
	"context"
	"fmt"
	"log"
	"time"
	"twitch-data-aggregator/config"
	"twitch-data-aggregator/internal/broker/kafka"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type StreamData struct {
	Id           string
	UserId       string
	UserLogin    string
	UserName     string
	GameId       string
	GameName     string
	ViewersCount uint32
	Language     string
	Timestamp    time.Time
}

func Connect(cfg *config.Config) (driver.Conn, error) {
	connectOptions := clickhouse.Options{
		Addr: []string{cfg.Clickhouse.Address},
		Auth: clickhouse.Auth{
			Database: cfg.Clickhouse.Database,
			Username: cfg.Clickhouse.Username,
			Password: cfg.Clickhouse.Password,
		},
	}

	conn, err := clickhouse.Open(&connectOptions)
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к ClickHouse: %v", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}

	// SQL-запрос для создания таблицы, если она не существует
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS twitch (
		id UUID DEFAULT generateUUIDv4(),
		user_id String,
		userlogin String,
		username String,
		game_id String,
		game_name String,
		viewers_count UInt32,
		language String,
		timestamp DateTime('Europe/London')
	) ENGINE = MergeTree()
	ORDER BY (username, game_name, timestamp);
	`

	err = conn.Exec(context.Background(), createTableQuery)
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании таблицы: %v", err)
	}

	return conn, nil
}

func Insert(conn driver.Conn, streamsJSON []kafka.StreamsJSONMessage) error {
	query := "INSERT INTO twitch (user_id, userlogin, username, game_id, game_name, viewers_count, language, timestamp) VALUES "
	values := []interface{}{}

	streams := streamsJSONToStreamData(streamsJSON)

	for _, v := range streams {
		if v.ViewersCount < 10 {
			continue
		}
		query += "(?, ?, ?, ?, ?, ?, ?, ?),"
		values = append(values, v.UserId, v.UserLogin, v.UserName, v.GameId, v.GameName, v.ViewersCount, v.Language, v.Timestamp)
	}

	query = query[:len(query)-1]

	if err := conn.Exec(context.Background(), query, values...); err != nil {
		return err
	}

	log.Printf("inserted %d rows\n", len(values)/8)

	return nil
}

func Select(conn driver.Conn) error {
	query := "SELECT id, userlogin, username, game_id, game_name, viewers_count, language, timestamp FROM twitch"

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var stream StreamData

		if err := rows.Scan(&stream.Id, &stream.UserLogin, &stream.UserName, &stream.GameId, &stream.GameName, &stream.ViewersCount, &stream.Language, &stream.Timestamp); err != nil {
			return err
		}

		fmt.Printf("ID: %s, UserId: %s, Userlogin: %s, Username: %s, GameId: %s, GameName: %s, ViewersCount: %d, Language: %s, Timestamp: %s",
			stream.Id, stream.UserId, stream.UserLogin, stream.UserName, stream.GameId, stream.GameName, stream.ViewersCount, stream.Language, stream.Timestamp)
	}

	return nil
}

func Delete(conn driver.Conn) error {
	query := "TRUNCATE TABLE twitch"

	if err := conn.Exec(context.Background(), query); err != nil {
		return err
	}
	return nil
}

func streamsJSONToStreamData(streams []kafka.StreamsJSONMessage) []StreamData {
	res := make([]StreamData, 0, len(streams))
	for _, v := range streams {
		res = append(res, StreamData{
			UserId:       v.UserId,
			UserLogin:    v.UserLogin,
			UserName:     v.UserName,
			GameId:       v.GameId,
			GameName:     v.GameName,
			ViewersCount: v.ViewerCount,
			Language:     v.Language,
			Timestamp:    time.Now(),
		})
	}

	return res
}
