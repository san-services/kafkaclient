package kafkaclient

import (
	"context"
	"crypto/tls"
	"time"

	logger "github.com/disturb16/apilogger"
)

// KafkaGoClient implements the KafkaClient interface
type KafkaGoClient struct {
	consumer *kafkagoConsumer
	producer *kafkagoProducer
}

func newKafkaGOClient(conf Config) (c KafkaClient, e error) {
	ctx := context.Background()
	lg := logger.New(ctx, "")

	consumer := getKafkaGoConsumer(ctx,
		conf.ConsumerGroupID, conf.Brokers,
		conf.ReadTopicNames(), conf.TopicMap(), conf.TLS)

	sr, e := newSchemaReg(conf.SchemaRegURL, conf.TLS, conf.TopicMap())
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	producer := newKafkaGoProducer(
		conf.ProducerType, conf.WriteTopicNames(),
		conf.TopicMap(), conf.Brokers, conf.TLS, sr)

	return &KafkaGoClient{
		consumer: consumer,
		producer: &producer}, nil
}

// StartConsume starts consuming configured kafka topic messages
func (c *KafkaGoClient) StartConsume(ctx context.Context) (e error) {
	go func() {
		select {
		case <-c.consumer.initialized:
			c.consumer.startConsume(ctx)
		}
	}()
	return
}

// CancelConsume calls the context's context.cancelFunc
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

// ProduceMessage creates/encodes a
// message and sends it to the specified topic
func (c *KafkaGoClient) ProduceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(context.Background(), "")

	if !c.producer.initialized {
		e = errProducerUninit
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	e = c.producer.produceMessage(ctx, topic, key, msg)
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

func getKafkaGoConsumer(ctx context.Context,
	groupID string, brokers []string, topicNames []string,
	topicMap map[string]TopicConfig, tls *tls.Config) *kafkagoConsumer {

	lg := logger.New(ctx, "")

	c, e := newKafkagoConsumer(groupID,
		brokers, topicNames, topicMap, tls)

	if e != nil {
		// retry init in background on fail
		go func(e error) {
			for e != nil {
				lg.Error(logger.LogCatUncategorized, e)
				time.Sleep(retryInitDelay)

				c, e = newKafkagoConsumer(groupID,
					brokers, topicNames, topicMap, tls)
			}
		}(e)
	}

	return &c
}
