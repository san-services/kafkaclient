package kafkaclient

import (
	"context"
	"log"
	"os"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
)

const (
	logPrefSarama = "DEBUG [sarama] "
)

// SaramaClient implements the KafkaClient interface
type SaramaClient struct {
	consumer saramaConsumer
	producer saramaProducer
}

func newSaramaClient(conf Config) (c KafkaClient, e error) {
	ctx := context.Background()
	lg := logger.New(ctx, "")

	if conf.Debug {
		sarama.Logger = log.New(os.Stdout, logPrefSarama, log.LstdFlags)
	}

	sc, e := getSaramaConf(ctx, conf.KafkaVersion,
		conf.ConsumerGroupID, conf.ReadFromOldest, conf.TLS)

	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	consumer, e := newSaramaConsumer(sc, conf.ConsumerGroupID,
		conf.TopicMap(), conf.TopicNames(), conf.Brokers)

	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	sr, e := newSchemaReg(conf.SchemaRegURL, conf.TLS, conf.TopicMap())
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	producer, e := newSaramaProducer(ctx,
		conf.ProducerType, conf.Brokers, conf.TopicMap(), sc, sr)

	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return &SaramaClient{
		consumer: consumer,
		producer: producer,
	}, nil
}

// StartConsume starts consuming configured kafka topic messages
func (c *SaramaClient) StartConsume(ctx context.Context) (e error) {
	lg := logger.New(ctx, "")

	e = c.consumer.startConsume()
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
}

// CancelConsume call the context's context.cancelFunc
// in order to stop the process of message consumption
func (c *SaramaClient) CancelConsume() (e error) {
	c.consumer.cancel()
	return nil
}

func (c *SaramaClient) handleProcessingFail() (e error) {
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

// ProduceMessage creates/encodes a message and sends it to the specified topic
func (c *SaramaClient) ProduceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(ctx, "")

	e = c.producer.produceMessage(ctx, topic, key, msg)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
}
