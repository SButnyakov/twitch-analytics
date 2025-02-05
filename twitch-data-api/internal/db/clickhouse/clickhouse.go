package clickhouse

import (
	"context"
	"fmt"
	"twitch-data-api/config"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

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

	return conn, nil
}
