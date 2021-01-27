package kafkaclient

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-uuid"
	logger "github.com/san-services/apilogger"
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
	lg := logger.New(nil, "")

	if conf.Debug {
		sarama.Logger = log.New(os.Stdout, logPrefSarama, log.LstdFlags)
	}

	sc, e := getSaramaConf(conf.KafkaVersion,
		conf.ConsumerGroupID, conf.ReadFromOldest, conf.TLS)

	if e != nil {
		lg.Error(logger.LogCatKafkaConfig, e)
		return
	}

	consumer := getSaramaConsumer(
		sc, conf.ConsumerGroupID,
		conf.TopicMap(), conf.ReadTopicNames(),
		conf.Brokers, conf.ProcDependencies)

	sr, e := newSchemaReg(conf.SchemaRegURL, conf.TLS, conf.TopicMap())
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
		return
	}

	producer := getSaramaProducer(
		conf.ProducerType, conf.Brokers, conf.TopicMap(), sc, sr)

	return &SaramaClient{
		consumer: consumer,
		producer: producer,
	}, nil
}

// StartConsume starts consuming configured kafka topic messages
func (c *SaramaClient) StartConsume() (e error) {
	go func() {
		select {
		case <-c.consumer.initialized:
			go c.handleProcessingFail()
			go c.consumer.startConsume()
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
		lg.Error(logger.LogCatKafkaProduce, e)
		return
	}

	e = c.producer.produceMessage(ctx, topic, key, msg)
	if e != nil {
		lg.Error(logger.LogCatKafkaProduce, e)
	}

	return
}

func (c *SaramaClient) handleProcessingFail() (e error) {
	lg := logger.New(nil, "")

	for {
		select {
		case fail := <-c.consumer.failMessages:
			retryMsg := NewRetryTopicMessage(
				fail.msg.Topic(), fail.msg.Partition(),
				fail.msg.Offset(), fail.msg.Value(), fail.e)

			e = c.producer.produceMessage(
				nil, fail.retryTopic, fail.msg.Key(), retryMsg)

			if e != nil {
				lg.Error(logger.LogCatKafkaProduce, e)
			}
		}
	}
}

func getSaramaConf(kafkaVersion string, groupID string,
	fromOldest bool, tls *tls.Config) (c *sarama.Config, e error) {

	lg := logger.New(nil, "")
	c = sarama.NewConfig()

	version, e := sarama.ParseKafkaVersion(kafkaVersion)
	if e != nil {
		e = errKafkaVersion(kafkaVersion)
		lg.Error(logger.LogCatKafkaConfig, e)
		return
	}

	id, e := uuid.GenerateUUID()
	if e != nil {
		lg.Error(logger.LogCatKafkaConfig, e)
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

func getSaramaConsumer(sc *sarama.Config,
	groupID string, topicMap map[string]TopicConfig, topicNames []string,
	brokers []string, pd ProcessorDependencies) *saramaConsumer {

	lg := logger.New(nil, "")

	c, e := newSaramaConsumer(sc,
		groupID, topicMap, topicNames, brokers, pd)

	if e != nil {
		// retry init in background on fail
		go func(e error) {
			for e != nil {
				lg.Error(logger.LogCatKafkaConsumerInit, e)
				time.Sleep(retryInitDelay)

				c, e = newSaramaConsumer(sc,
					groupID, topicMap, topicNames, brokers, pd)
			}
		}(e)
	}

	return &c
}

func getSaramaProducer(prodType producerType,
	brokers []string, topicMap map[string]TopicConfig,
	sc *sarama.Config, sr schemaRegistry) *saramaProducer {

	lg := logger.New(nil, "")

	p, e := newSaramaProducer(
		prodType, brokers, topicMap, sc, sr)

	if e != nil {
		// retry init in background on fail
		go func(e error) {
			for e != nil {
				lg.Error(logger.LogCatKafkaProducerInit, e)
				time.Sleep(retryInitDelay)

				p, e = newSaramaProducer(
					prodType, brokers, topicMap, sc, sr)
			}
		}(e)
	}

	return &p
}
