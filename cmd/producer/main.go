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
	Publish("BOL $ 30.0", topic, producer, nil, deliveryChannel)
	go DeliveryReportHandler(deliveryChannel) // go executar em uma thread separada, para não travar o fluxo do programa | Async
	producer.Flush(5 * 1000)                  // Flush: aguarda a entrega de todas as mensagens pendentes antes de encerrar o produtor
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

func DeliveryReportHandler(deliveryChannel chan kafka.Event) {
	for event := range deliveryChannel {
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message: %v\n", ev.TopicPartition.Error)
			}

			if ev.TopicPartition.Error == nil {
				log.Printf("Topic:(%s |Partition:[%d] |Offset %v)\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		}
	}
}
