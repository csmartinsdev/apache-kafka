package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChannel := make(chan kafka.Event)
	topic := "FULLCYCLE-Trilha-Kafka"

	producer := newKafkaProducer()
	Publish("Hello, Kafka!", topic, producer, nil, deliveryChannel)

	event := <-deliveryChannel
	message := event.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		log.Printf("Failed to deliver message: %v\n", message.TopicPartition.Error)
		return
	}

	log.Printf("Topic:(%s |Partition:[%d] |Offset %v)\n",
		*message.TopicPartition.Topic, message.TopicPartition.Partition, message.TopicPartition.Offset)

	producer.Flush(1000)

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
func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
		Key:            key,
	}

	err := producer.Produce(message, deliveryChannel)
	if err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}

	return nil
}
