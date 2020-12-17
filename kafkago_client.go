package kafkaclient

import (
	"context"
)

// KafkaGoClient implements the KafkaClient interface
type KafkaGoClient struct {
	consumer kafkagoConsumer
	producer kafkagoProducer
}

func newKafkaGOClient(conf Config) (c KafkaClient, e error) {
	// consumer := newKafkagoConsumer(
	// 	conf.ConsumerGroupID, conf.Brokers, conf.TopicNames(), conf.TopicMap())

	// return &KafkaGoClient{consumer: consumer}, nil
	return nil, errNotImpl
}

// StartConsume starts consuming configured kafka topic messages
func (c *KafkaGoClient) StartConsume(ctx context.Context) (e error) {
	// lg := logger.New(ctx, "")

	// e = c.consumer.startConsume(ctx)
	// if e != nil {
	// 	lg.Error(logger.LogCatUncategorized, e)
	// 	return
	// }

	return errNotImpl
}

// CancelConsume call the context's context.cancelFunc
// in order to stop the process of message consumption
func (c *KafkaGoClient) CancelConsume() (e error) {
	return errNotImpl
}

func (c *KafkaGoClient) handleProcessingFail() (e error) {
	return errNotImpl
}

// ProduceMessage creates/encodes a
// message and sends it to the specified topic
func (c *KafkaGoClient) ProduceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	return errNotImpl
}
