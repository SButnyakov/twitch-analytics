package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"twitch-data-aggregator/config"
	"twitch-data-aggregator/internal/broker/kafka"

	"github.com/elastic/go-elasticsearch/v8"
)

const (
	batchSize = 5000
)

func Connect(cfg *config.Config) (*elasticsearch.Client, error) {
	elCfg := elasticsearch.Config{
		Addresses: []string{cfg.Elasticsearch.Address},
	}

	es, err := elasticsearch.NewClient(elCfg)
	if err != nil {
		return nil, err
	}

	res, err := es.Info()
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return es, nil
}

func Insert(es *elasticsearch.Client, msgs []kafka.StreamsJSONMessage) error {
	var bulkBuffer bytes.Buffer
	count := 0
	for _, v := range msgs {
		fmt.Println(v.Timestamp, v.StartedAt)
		meta := fmt.Sprintf("{\"index\":{\"_index\":\"twitch\"}}")
		jsonData, err := json.Marshal(v)
		if err != nil {
			log.Printf("failed to marshal (%v): %v\n", v, err)
			continue
		}
		bulkBuffer.WriteString(meta)
		bulkBuffer.WriteString("\n")
		bulkBuffer.Write(jsonData)
		bulkBuffer.WriteString("\n")
		count++

		if count >= batchSize {
			res, err := es.Bulk(bytes.NewReader(bulkBuffer.Bytes()), es.Bulk.WithContext(context.Background()))
			if err != nil {
				log.Printf("Ошибка при отправке в Elasticsearch: %s", err)
				continue
			}
			res.Body.Close()
			bulkBuffer.Reset()
			count = 0
		}
	}

	if count != 0 {
		res, err := es.Bulk(bytes.NewReader(bulkBuffer.Bytes()), es.Bulk.WithContext(context.Background()))
		if err != nil {
			log.Printf("Ошибка при отправке в Elasticsearch: %s", err)
		}
		res.Body.Close()
		bulkBuffer.Reset()
	}

	return nil
}
