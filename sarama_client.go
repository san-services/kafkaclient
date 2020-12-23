package kafkaclient

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
	"github.com/hashicorp/go-uuid"
)

const (
	logPrefSarama = "DEBUG [sarama] "
)

// SaramaClient implements the KafkaClient interface
type SaramaClient struct {
	consumer *saramaConsumer
	producer *saramaProducer
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

	consumer := getSaramaConsumer(ctx, sc, conf.ConsumerGroupID,
		conf.TopicMap(), conf.ReadTopicNames(), conf.Brokers)

	sr, e := newSchemaReg(conf.SchemaRegURL, conf.TLS, conf.TopicMap())
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	producer := getSaramaProducer(ctx,
		conf.ProducerType, conf.Brokers, conf.TopicMap(), sc, sr)

	return &SaramaClient{
		consumer: consumer,
		producer: producer,
	}, nil
}

// StartConsume starts consuming configured kafka topic messages
func (c *SaramaClient) StartConsume(ctx context.Context) (e error) {
	go func() {
		select {
		case <-c.consumer.initialized:
			c.consumer.startConsume(ctx)
		}
	}()
	return
}

// CancelConsume call the context's context.cancelFunc
// in order to stop the process of message consumption
func (c *SaramaClient) CancelConsume() (e error) {
	c.consumer.cancel()
	return nil
}

// ProduceMessage creates/encodes a message and sends it to the specified topic
func (c *SaramaClient) ProduceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(ctx, "")

	if !c.producer.initialized {
		e = errProducerUninit
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	e = c.producer.produceMessage(ctx, topic, key, msg)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
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

func getSaramaConf(ctx context.Context, kafkaVersion string,
	groupID string, fromOldest bool, tls *tls.Config) (c *sarama.Config, e error) {

	lg := logger.New(ctx, "")

	c = sarama.NewConfig()

	version, e := sarama.ParseKafkaVersion(kafkaVersion)
	if e != nil {
		e = errKafkaVersion(kafkaVersion)
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	id, e := uuid.GenerateUUID()
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	c.Version = version
	c.ClientID = groupID + "_" + string(id)
	c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	c.Consumer.Offsets.AutoCommit.Enable = false
	c.Consumer.Group.Session.Timeout = 15 * time.Second

	if fromOldest {
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if tls != nil {
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = tls
	}

	return
}

func getSaramaConsumer(ctx context.Context, sc *sarama.Config,
	groupID string, topicMap map[string]TopicConfig,
	topicNames []string, brokers []string) *saramaConsumer {

	lg := logger.New(ctx, "")

	c, e := newSaramaConsumer(ctx,
		sc, groupID, topicMap, topicNames, brokers)

	if e != nil {
		// retry init in background on fail
		go func(e error) {
			for e != nil {
				lg.Error(logger.LogCatUncategorized, e)
				time.Sleep(retryInitDelay)

				c, e = newSaramaConsumer(ctx,
					sc, groupID, topicMap, topicNames, brokers)
			}
		}(e)
	}

	return &c
}

func getSaramaProducer(ctx context.Context,
	prodType producerType, brokers []string, topicMap map[string]TopicConfig,
	sc *sarama.Config, sr schemaRegistry) *saramaProducer {

	lg := logger.New(ctx, "")

	p, e := newSaramaProducer(ctx,
		prodType, brokers, topicMap, sc, sr)

	if e != nil {
		// retry init in background on fail
		go func(e error) {
			for e != nil {
				lg.Error(logger.LogCatUncategorized, e)
				time.Sleep(retryInitDelay)

				p, e = newSaramaProducer(ctx,
					prodType, brokers, topicMap, sc, sr)
			}
		}(e)
	}

	return &p
}
