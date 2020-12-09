package kafkaclient

import (
	"context"
	"fmt"
)

var (
	errInvalidBase = func(base baseLibrary) error { return fmt.Errorf("cannot use unimplemented base library [%s]", base) }
)

type baseLibrary string

const (
	BaseSarama  baseLibrary = "SARAMA"
	BaseKafkaGO baseLibrary = "KAFKAGO"
)

type KafkaClient interface {
	StartConsume() error
	CancelConsume() error
	ProduceMessage(ctx context.Context, topic string, key string, msg interface{}) error
}

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
