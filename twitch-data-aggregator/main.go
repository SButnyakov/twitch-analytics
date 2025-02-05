package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"twitch-data-aggregator/config"
	"twitch-data-aggregator/internal/broker/kafka"
	"twitch-data-aggregator/internal/db/clickhouse"
	"twitch-data-aggregator/internal/db/elasticsearch"
	"twitch-data-aggregator/internal/lib"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	cfg := config.MustLoad()
	log.Println(cfg)

	conn, err := clickhouse.Connect(cfg)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	es, err := elasticsearch.Connect(cfg)
	if err != nil {
		panic(err)
	}

	/*
		clients, err := redis.Connect(cfg)
		if err != nil {
			panic(err)
		}


		start := time.Now()
		aggregator.Aggregate(cfg, conn, clients)
		log.Printf("aggregation took %s\n", time.Since(start))
	*/

	messageChan := make(chan kafka.Message)
	csvChan := make(chan kafka.Message)

	go kafka.ReadMessages(cfg, messageChan)

	go func(msgChan chan kafka.Message, writeChan chan kafka.Message) {
		for {
			msg := <-messageChan
			log.Println("got message")

			writeChan <- msg
			log.Println("resend message to csv writer")

			start := time.Now()
			err := elasticsearch.Insert(es, msg.Data)
			if err != nil {
				log.Printf("failed to inserd data: %v\n", err)
				continue
			}
			elapsed := time.Since(start)
			log.Printf("inserting took %s\n", elapsed)

			/*
				if msg.IsFinished {
					start = time.Now()
					aggregator.Aggregate(cfg, conn, clients)
					log.Printf("aggregation took %s\n", time.Since(start))
				}
			*/
		}
	}(messageChan, csvChan)

	go func(writeChan chan kafka.Message) {
		savePath := fmt.Sprintf("%s/%s.csv", cfg.CSVS.SavePath, lib.TimeNowToString())
		writer, file, err := lib.CreateCSVWriter(savePath)
		if err != nil {
			log.Printf("failed to create csv writer: %v", err)
		}
		log.Printf("SAVE_PATH: %s\n", savePath)

		for {
			msg := <-writeChan
			csvWriteStart := time.Now()
			lib.WriteStreamsToCSV(writer, msg.Data)

			if msg.IsFinished {
				file.Close()
				savePath = fmt.Sprintf("%s/%s.csv", cfg.CSVS.SavePath, lib.TimeNowToString())
				log.Printf("SAVE_PATH: %s\n", savePath)
				writer, file, err = lib.CreateCSVWriter(savePath)
				if err != nil {
					log.Printf("failed to create csv writer: %v", err)
				}
			}

			log.Printf("write csv took %s\n", time.Since(csvWriteStart))
		}
	}(csvChan)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interrupt

	log.Println("shutting down")
}
