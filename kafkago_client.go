package kafkaclient

import (
	"context"
	"crypto/tls"
	"time"

	logger "github.com/san-services/apilogger"
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
		conf.ReadTopicNames(), conf.TopicMap(),
		conf.TLS, conf.ProcDependencies)

	sr, e := newSchemaReg(conf.SchemaRegURL, conf.TLS, conf.TopicMap())
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
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
func (c *KafkaGoClient) StartConsume() (e error) {
	select {
	case <-c.consumer.initialized:
		go c.handleProcessingFail()
		go c.consumer.startConsume()
	}
	return
}

// CancelConsume calls the context's context.cancelFunc
// in order to stop the process of message consumption
func (c *KafkaGoClient) CancelConsume() (e error) {
	lg := logger.New(context.Background(), "")

	e = c.consumer.group.Close()
	if e != nil {
		lg.Error(logger.LogCatKafkaConsumerClose, e)
		return
	}

	return
}

// ProduceMessage creates/encodes a
// message and sends it to the specified topic
func (c *KafkaGoClient) ProduceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(nil, "")

	if !c.producer.initialized {
		e = errProducerUninit
		lg.Error(logger.LogCatKafkaProduce, e)
		return
	}

	e = c.producer.produceMessage(ctx, topic, key, msg)
	if e != nil {
		lg.Error(logger.LogCatKafkaProduce, e)
		return
	}

	return
}

func (c *KafkaGoClient) handleProcessingFail() (e error) {
	lg := logger.New(nil, "")

	for {
		select {
		case fail := <-c.consumer.failMessages:
			retryMsg := NewRetryTopicMessage(
				fail.msg.Topic(), fail.msg.Partition(),
				fail.msg.Offset(), fail.msg.Value(), fail.e)

			e = c.producer.produceMessage(nil,
				fail.retryTopic, fail.msg.Key(), retryMsg)

			if e != nil {
				lg.Error(logger.LogCatKafkaProduce, e)
			}
		}
	}
}

func getKafkaGoConsumer(ctx context.Context,
	groupID string, brokers []string, topicNames []string,
	topicMap map[string]TopicConfig, tls *tls.Config,
	pd ProcessorDependencies) *kafkagoConsumer {

	lg := logger.New(ctx, "")

	c, e := newKafkagoConsumer(groupID,
		brokers, topicNames, topicMap, pd, tls)

	if e != nil {
		// retry init in background on fail
		go func(e error) {
			for e != nil {
				lg.Error(logger.LogCatKafkaConsumerInit, e)
				time.Sleep(retryInitDelay)

				c, e = newKafkagoConsumer(groupID,
					brokers, topicNames, topicMap, pd, tls)
			}
		}(e)
	}

	return &c
}
