package kafkaclient

import (
	"context"
)

type KafkaGOClient struct {
}

func newKafkaGOClient(conf Config) (*KafkaGOClient, error) {
	return &KafkaGOClient{}, nil
}

func (c *KafkaGOClient) StartConsume(ctx context.Context) (e error) {
	return
}

func (c *KafkaGOClient) CancelConsume() (e error) {
	return
}

func (c *KafkaGOClient) handleProcessingFail() (e error) {
	return
}

func (c *KafkaGOClient) ProduceMessage(ctx context.Context, topic string, key string, msg interface{}) (e error) {
	return
}
