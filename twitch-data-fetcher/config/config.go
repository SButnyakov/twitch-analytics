package config

import (
	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Auth
	URLs
}

type Auth struct {
	ClientID     string `env:"CLIENT_ID" env-required:"true"`
	ClientSecret string `env:"CLIENT_SECRET" env-required:"true"`
	Token        *Token
}

type URLs struct {
	GetStreams string `yaml:"get_streams" env-default:"https://api.twitch.tv/helix/streams"`
	GetToken   string `yaml:"get_token" env-default:"https://id.twitch.tv/oauth2/token"`
}

type Token struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

func MustLoad() *Config {
	var cfg Config

	if err := cleanenv.ReadEnv(&cfg); err != nil {
		panic(err)
	}

	if err := cleanenv.ReadConfig("./config/urls.yaml", &cfg); err != nil {
		panic(err)
	}

	return &cfg
}
