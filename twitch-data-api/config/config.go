package config

import (
	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	HTTP
	Redis
}

type HTTP struct {
	Address string `env:"HTTP_ADDR" env-default:"localhost:8000"`
}

type Redis struct {
	Address             string `env:"REDIS_ADDR" env-default:"localhost:6379"`
	Password            string `env:"REDIS_PASSWORD" env-default:""`
	Games               int    `env:"REDIS_GAMES" env-default:"0"`
	Streamers           int    `env:"REDIS_STREAMERS" env-default:"1"`
	GamesAvgOnline      int    `env:"REDIS_GAMES_AVG_ONLINE" env-default:"2"`
	StreamersAvgOnline  int    `env:"REDIS_STREAMERS_AVG_ONLINE" env-default:"3"`
	GamesTimepoints     int    `env:"REDIS_GAMES_TIMEPOINTS" env-default:"4"`
	StreamersTimepoints int    `env:"REDIS_STREAMERS_TIMEPOINTS" env-default:"5"`
}

func MustLoad() *Config {
	var cfg Config

	if err := cleanenv.ReadEnv(&cfg); err != nil {
		panic(err)
	}

	return &cfg
}
