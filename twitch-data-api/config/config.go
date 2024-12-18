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
	Address  string `env:"REDIS_ADDR" env-default:"localhost:6379"`
	Password string `env:"REDIS_PASSWORD" env-default:""`
	Database int    `env:"REDIS_DATABASE" env-default:"0"`
}

func MustLoad() *Config {
	var cfg Config

	if err := cleanenv.ReadEnv(&cfg); err != nil {
		panic(err)
	}

	return &cfg
}
