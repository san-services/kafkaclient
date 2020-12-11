package kafkaclient

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
)

const (
	logPrefSarama = "DEBUG [sarama] "
)

// SaramaClient implements the KafkaClient interface
type SaramaClient struct {
	consumer    saramaConsumer
	producer    saramaProducer
	topics      map[string]TopicConfig
	avroCodec   EncoderDecoder
	jsonCodec   EncoderDecoder
	stringCodec EncoderDecoder
}

func newSaramaClient(conf Config) (*SaramaClient, error) {
	if conf.Debug {
		sarama.Logger = log.New(os.Stdout, logPrefSarama, log.LstdFlags)
	}

	return &SaramaClient{}, nil
}

// StartConsume starts consuming kafka topic messages
func (c *SaramaClient) StartConsume() (e error) {
	return c.consumer.startConsume()
}

// CancelConsume call the context's context.cancelFunc in order to stop the
// process of message consumption
func (c *SaramaClient) CancelConsume() (e error) {
	c.consumer.cancel()
	return nil
}

// ProduceMessage creates/encodes a message and sends it to the specified topic
func (c *SaramaClient) ProduceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(ctx, "")

	var saramaEncoder sarama.Encoder
	var codec EncoderDecoder

	topicConf := c.topics[topic]

	switch topicConf.MessageType {
	case MessageFormatAvro:
		codec = c.avroCodec
		break
	case MessageFormatJSON:
		codec = c.jsonCodec
		break
	case MessageFormatString:
		codec = c.stringCodec
		break
	default:
		e = errMessageFormat
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	switch msg.(type) {
	case string:
		saramaEncoder = sarama.StringEncoder(msg.(string))
		break
	case []byte:
		saramaEncoder = newSaramaByteEncoder(ctx, topic, msg.([]byte), codec)
		break
	case int32, int64, float32, float64:
		saramaEncoder = sarama.StringEncoder(fmt.Sprint(msg))
	default:
		if reflect.ValueOf(msg).Kind() == reflect.Struct {
			saramaEncoder, e = newSaramaStructEncoder(ctx, topic, msg, codec)
			if e != nil {
				lg.Error(logger.LogCatUncategorized, e)
				return
			}
			break
		}
		e = errMessageType
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	e = c.producer.produceMessage(ctx, topic, key, saramaEncoder)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
}
