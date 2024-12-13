package endpoints

import (
	"encoding/json"
	"fmt"
	"net/http"
	"twitch-data-fetcher/config"
)

func GetToken(cfg *config.Config) (*config.Token, error) {
	client := &http.Client{}

	url := fmt.Sprintf("%s?client_id=%s&client_secret=%s&grant_type=client_credentials",
		cfg.URLs.GetToken,
		cfg.Auth.ClientID,
		cfg.Auth.ClientSecret,
	)

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	var token config.Token

	if err = json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, err
	}

	return &token, nil
}
