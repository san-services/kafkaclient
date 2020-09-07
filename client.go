package kafkaclient

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// onMessage receives topic, message
type onMessage func(string, string)

// KafkaClient ...
type KafkaClient struct {
	Server            string
	GroupID           string
	OnMessageReceived onMessage
}

// New creates broker object
func New(server string, groupID string, onMessageReceived onMessage) *KafkaClient {
	return &KafkaClient{
		Server:            server,
		GroupID:           groupID,
		OnMessageReceived: onMessageReceived,
	}
}

// ProduceToTopic Sends a message to the specified topic
func (kc *KafkaClient) ProduceToTopic(ctx context.Context, topic string, message string) error {

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kc.Server},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	defer w.Close()

	err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(topic),
		Value: []byte(message),
	})

	if err != nil {
		return err
	}

	return nil
}

// ListenToTopic starts listening for the specified topic
func (kc *KafkaClient) ListenToTopic(ctx context.Context, topic string) {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kc.Server},
		GroupID: kc.GroupID,
		Topic:   topic,
		MaxWait: 1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
	})

	defer reader.Close()

	log.Printf("Listening to kafka topic: %s", topic)
	for {
		msg, err := reader.ReadMessage(ctx)
		if kc.OnMessageReceived != nil {
			kc.OnMessageReceived(msg.Topic, string(msg.Value))
		}

		if err != nil {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}
