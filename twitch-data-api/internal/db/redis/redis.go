package redis

import (
	"context"
	"twitch-data-api/config"

	"github.com/redis/go-redis/v9"
)

func Connect(cfg *config.Config) (*redis.Client, error) {
	r := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.Database,
	})

	if err := r.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return r, nil
}
