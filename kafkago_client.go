package kafkaclient

import (
	"context"

	logger "github.com/disturb16/apilogger"
)

// KafkaGoClient implements the KafkaClient interface
type KafkaGoClient struct {
	consumer kafkagoConsumer
	producer kafkagoProducer
}

func newKafkaGOClient(conf Config) (c KafkaClient, e error) {
	consumer := newKafkagoConsumer(
		conf.ConsumerGroupID, conf.Brokers,
		conf.TopicNames(), conf.TopicMap(), conf.TLS)

	return &KafkaGoClient{consumer: consumer}, nil
}

// StartConsume starts consuming configured kafka topic messages
func (c *KafkaGoClient) StartConsume(ctx context.Context) (e error) {
	lg := logger.New(ctx, "")

	e = c.consumer.initConsumerGroup(ctx)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	go c.consumer.startConsume(ctx)
	return
}

// CancelConsume call the context's context.cancelFunc
// in order to stop the process of message consumption
func (c *KafkaGoClient) CancelConsume() (e error) {
	lg := logger.New(context.Background(), "")

	e = c.consumer.group.Close()
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return
}

func (c *KafkaGoClient) handleProcessingFail() (e error) {
	ctx := context.Background()
	lg := logger.New(ctx, "")

	for {
		select {
		case fail := <-c.consumer.failMessages:
			retryMsg := NewRetryTopicMessage(
				fail.msg.Topic(), fail.msg.Partition(),
				fail.msg.Offset(), fail.msg.Value(), fail.e)

			e = c.producer.produceMessage(
				ctx, fail.retryTopic, fail.msg.Key(), retryMsg)

			if e != nil {
				lg.Error(logger.LogCatUncategorized, e)
			}
		}
	}
}

// ProduceMessage creates/encodes a
// message and sends it to the specified topic
func (c *KafkaGoClient) ProduceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(context.Background(), "")

	e = c.producer.produceMessage(ctx, topic, key, msg)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return
}
