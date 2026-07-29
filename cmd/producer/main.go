package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "fullcycle-trilha-kafka"

	producer := newKafkaProducer()
	Publish("Hello, Kafka!", topic, producer, nil)
	producer.Flush(15 * 1000)

}

func newKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return producer
}
func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
		Key:            key,
	}

	err := producer.Produce(message, nil)
	if err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}

	return nil
}
