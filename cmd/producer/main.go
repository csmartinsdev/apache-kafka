package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Hello Go!")
}

func newKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		panic(err)
	}

	return producer
}
