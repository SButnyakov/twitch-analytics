package models

import (
	"encoding/json"
	"time"
)

type OnlineTimepoint struct {
	Online    uint64    `json:"online"`
	Timestamp time.Time `json:"timestamp"`
}

type OnlineTimepoints struct {
	Data []OnlineTimepoint `json:"data"`
}

func (o OnlineTimepoints) MarshalBinary() ([]byte, error) {
	return json.Marshal(o)
}
