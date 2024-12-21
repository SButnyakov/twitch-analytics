package redis

import (
	"context"
	"twitch-data-aggregator/config"

	"github.com/redis/go-redis/v9"
)

func Connect(cfg *config.Config) ([]*redis.Client, error) {
	clients := make([]*redis.Client, 0)
	clients = append(clients, redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.Games,
	}))
	clients = append(clients, redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.Streamers,
	}))
	clients = append(clients, redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.GamesAvgOnline,
	}))
	clients = append(clients, redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.StreamersAvgOnline,
	}))
	clients = append(clients, redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.GamesTimepoints,
	}))
	clients = append(clients, redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.StreamersTimepoints,
	}))

	for _, client := range clients {
		if err := client.Ping(context.Background()).Err(); err != nil {
			return nil, err
		}
	}

	return clients, nil
}
