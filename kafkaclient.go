package kafkaclient

import (
	"context"

	logger "github.com/san-services/apilogger"
)

type baseLibrary string

const (
	// BaseSarama can be used in kafkaclient.New to specify that
	// the underlying library used will be Shopify's sarama (https://github.com/Shopify/sarama/)
	BaseSarama baseLibrary = "SARAMAAAAAA"

	// BaseKafkaGO can be used in kafkaclient.New to specify that
	// the underlying library used will be kafkago (https://github.com/segmentio/kafka-go)
	BaseKafkaGO baseLibrary = "KAFKAGO"
)

// KafkaClient is an interface describing the primary uses of this library
type KafkaClient interface {
	// StartConsume starts the consumption of messages from the configured Kafka topics
	StartConsume() error
	handleProcessingFail() error
	// CancelConsume cancels the consumption of messages from configured topics
	CancelConsume() error
	// ProduceMessage adds messages to a specified topic
	ProduceMessage(ctx context.Context, topic string, key string, msg interface{}) error
}

// New constructs and returns a new KafkaClient implementation
func New(base baseLibrary, config Config) (c KafkaClient, e error) {
	lg := logger.New(nil, "")

	e = config.validate()
	if e != nil {
		lg.Error(logger.LogCatKafkaConfig, e)
		return
	}

	config.finalize()

	switch base {
	case BaseSarama:
		return newSaramaClient(config)
	case BaseKafkaGO:
		return newKafkaGOClient(config)
	default:
		return nil, errInvalidBase(base)
	}
}
