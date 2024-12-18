package lib

import (
	"encoding/csv"
	"os"
	"strconv"
	"twitch-data-aggregator/internal/broker/kafka"
)

func CreateCSVWriter(path string) (*csv.Writer, *os.File, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, nil, err
	}

	file, err := os.Create(wd + "\\csvs\\" + path)
	if err != nil {
		return nil, nil, err
	}

	writer := csv.NewWriter(file)
	headers := []string{"user_id", "user_login", "user_name", "game_id", "game_name", "viewers_count", "language"}
	writer.Write(headers)
	writer.Flush()

	return writer, file, nil
}

func WriteStreamsToCSV(writer *csv.Writer, streams []kafka.StreamsJSONMessage) {
	for _, v := range streams {
		writer.Write([]string{v.UserId, v.UserLogin, v.UserName, v.GameId, v.GameName, strconv.FormatUint(uint64(v.ViewerCount), 10), v.Language})
	}
	writer.Flush()
}
