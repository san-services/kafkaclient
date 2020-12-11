package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
)

const (
	logPrefSarama = "DEBUG [sarama] "
)

var (
	errInvalidProducer = errors.New("invalid producer type configured")
	errMessageFormat   = errors.New("unsupported topic message format")
	errMessageType     = errors.New("unsupported message type")
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

// must implement sarama.ConsumerGroupHandler
type saramaConsumer struct {
	groupID    string
	group      sarama.ConsumerGroup
	config     *sarama.Config
	topicConf  map[string]TopicConfig
	topicNames []string
	brokers    []string
	ready      chan bool
	cancel     context.CancelFunc
	ctx        context.Context
}

func (c *saramaConsumer) startConsume() (e error) {
	lg := logger.New(c.ctx, "")

	c.group, e = sarama.NewConsumerGroup(
		c.brokers, c.groupID, c.config)

	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop,
			// when a server-side rebalance happens, the consumer
			// session will need to be recreated to get the new claims
			if e := (c.group).Consume(c.ctx, c.topicNames, c); e != nil {
				lg.Fatal(logger.LogCatUncategorized, errConsumer(e))
			}

			// check if context was cancelled, signaling
			// that the consumer should stop
			if c.ctx.Err() != nil {
				return
			}

			c.ready = make(chan bool)
		}
	}()

	<-c.ready // await consumer set up
	lg.Info(logger.LogCatUncategorized, infoConsumerReady)

	select {
	case <-c.ctx.Done():
		lg.Info(logger.LogCatUncategorized,
			infoConsumerTerm("context cancelled"))
	}

	c.cancel()
	wg.Wait()

	if e := c.close(); e != nil {
		lg.Fatal(logger.LogCatUncategorized, errConsumerClose(e))
	}

	return
}

// ConsumeClaim read ConsumerGroupClaim's Messages() in a loop.
// Method in sarama.ConsumerGroupHandler interface.
//
// Handles consuming and processing or delegating prosessing of topic messages.
// This method is called within a goroutine:
// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
func (c *saramaConsumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) (e error) {

	lg := logger.New(c.ctx, "")

	for {
		select {
		case msg := <-claim.Messages():
			lg.Infof(logger.LogCatUncategorized,
				infoMsgClaimed(msg.Topic, msg.Partition, msg.Offset))

			conf := c.topicConf[msg.Topic]

			e = conf.MessageProcessor(session.Context(), newSaramaMessage(msg, conf.MessageEncoderDecoder))
			if e != nil {
				lg.Error(logger.LogCatUncategorized, e)
				continue
			}

			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new consumer session, before ConsumeClaim.
// Method in sarama.ConsumerGroupHandler interface.
func (c *saramaConsumer) Setup(sarama.ConsumerGroupSession) (e error) {
	// mark the consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
// Method in sarama.ConsumerGroupHandler interface.
func (c *saramaConsumer) Cleanup(sarama.ConsumerGroupSession) (e error) {
	return nil
}

// Close stops the ConsumerGroup and detaches any running sessions
func (c *saramaConsumer) close() (e error) {
	lg := logger.New(c.ctx, "")

	e = c.group.Close()
	if e != nil {
		lg.Error(logger.LogCatUncategorized, errConsumerClose(e))
	}

	return
}

type saramaProducer struct {
	producer       interface{}
	encoderDecoder EncoderDecoder
}

func (p *saramaProducer) produceMessage(
	ctx context.Context, topic string, key string, encoder sarama.Encoder) (e error) {

	lg := logger.New(ctx, "")

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: encoder}

	switch p.producer.(type) {
	case sarama.SyncProducer:
		_, _, e = (p.producer.(sarama.SyncProducer)).SendMessage(msg)
		if e != nil {
			lg.Error(logger.LogCatUncategorized, e)
		}
	case sarama.AsyncProducer:
		(p.producer.(sarama.AsyncProducer)).Input() <- msg
	default:
		return errInvalidProducer
	}

	return
}
