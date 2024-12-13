package endpoints

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"twitch-data-fetcher/config"
)

type Stream struct {
	UserId      string
	UserLogin   string
	UserName    string
	GameId      string
	GameName    string
	Type        string
	Tags        []string
	ViewerCount int
	Language    string
}

type streamJSON struct {
	Id          string   `json:"id"`
	UserId      string   `json:"user_id"`
	UserLogin   string   `json:"user_login"`
	UserName    string   `json:"user_name"`
	GameId      string   `json:"game_id"`
	GameName    string   `json:"game_name"`
	Type        string   `json:"type"`
	Tags        []string `json:"tags"`
	ViewerCount int      `json:"viewer_count"`
	Language    string   `json:"language"`
}

type getStreamsResponse struct {
	Data       []streamJSON `json:"data"`
	Pagination pagination   `json:"pagination"`
}

var (
	languages = [][]string{
		{"en"},
		{"zh", "bg", "cs", "da", "nl", "fi", "fr", "ge", "el", "hu", "it", "ja", "ko", "no", "pl", "pt", "ro", "ru", "sk", "es", "sv", "th", "tr", "vi"},
	}
)

func GetStreams(cfg *config.Config) (map[string]Stream, error) {
	streamsChan := make(chan []streamJSON)

	for _, v := range languages {
		languageQuery := languageQuery(v)
		go func(cfg *config.Config, languageQuery string, streamsChan chan []streamJSON) {
			getStreams(cfg, languageQuery, streamsChan)
		}(cfg, languageQuery, streamsChan)
	}

	res := make(map[string]Stream, 0)

	for i := 0; i < len(languages); i++ {
		streams := <-streamsChan
		for _, stream := range streams {
			id, v := streamJSONtoStream(stream)
			res[id] = v
		}
	}

	return res, nil
}

func getStreams(cfg *config.Config, languageQuery string, streamsChan chan []streamJSON) {
	res := make([]streamJSON, 0)
	cursor := ""
	client := &http.Client{}
	for {
		gsr, err := getStreamsRequest(cfg, client, languageQuery, cursor)
		if err != nil {
			fmt.Printf("ERROR: %s", err.Error())
			streamsChan <- nil
			return
		}

		res = append(res, gsr.Data...)

		cursor = gsr.Pagination.Cursor
		if cursor == "" {
			break
		}
	}
	streamsChan <- res
}

func getStreamsRequest(cfg *config.Config, client *http.Client, languageQuery string, cursor string) (*getStreamsResponse, error) {
	url := cfg.URLs.GetStreams
	if cursor != "" {
		url += "?type=live&first=100&" + languageQuery + "&after=" + cursor
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Auth.Token.AccessToken)
	req.Header.Set("Client-Id", cfg.Auth.ClientID)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	var gsr getStreamsResponse

	if err = json.NewDecoder(resp.Body).Decode(&gsr); err != nil {
		return nil, err
	}

	return &gsr, nil
}

func languageQuery(languages []string) string {
	var sb strings.Builder
	for _, language := range languages {
		sb.WriteString("language=")
		sb.WriteString(language)
		sb.WriteString("&")
	}
	return sb.String()[:len(sb.String())-1]
}

func streamJSONtoStream(streamJSON streamJSON) (string, Stream) {
	return streamJSON.Id, Stream{
		UserId:      streamJSON.UserId,
		UserLogin:   streamJSON.UserLogin,
		UserName:    streamJSON.UserName,
		GameId:      streamJSON.GameId,
		GameName:    streamJSON.GameName,
		Type:        streamJSON.Type,
		Tags:        streamJSON.Tags,
		ViewerCount: streamJSON.ViewerCount,
		Language:    streamJSON.Language,
	}
}
