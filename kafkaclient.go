package kafkaclient

import (
	"context"
)

type baseLibrary string

const (
	// BaseSarama can be used in kafkaclient.New to specify that
	// the underlying library used will be Shopify's sarama (https://github.com/Shopify/sarama/)
	BaseSarama baseLibrary = "SARAMA"

	// BaseKafkaGO can be used in kafkaclient.New to specify that
	// the underlying library used will be kafkago (https://github.com/segmentio/kafka-go)
	BaseKafkaGO baseLibrary = "KAFKAGO"
)

// KafkaClient is an interface describing the primary uses of this library
type KafkaClient interface {
	StartConsume() error
	CancelConsume() error
	ProduceMessage(ctx context.Context, topic string, key string, msg interface{}) error
}

// New constructs and returns a new KafkaClient implementation
func New(base baseLibrary, config Config) (KafkaClient, error) {
	switch base {
	case BaseSarama:
		return newSaramaClient(config)
	case BaseKafkaGO:
		return newKafkaGOClient(config)
	default:
		return nil, errInvalidBase(base)
	}
}
