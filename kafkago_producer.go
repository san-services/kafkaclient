package kafkaclient

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	logger "github.com/san-services/apilogger"
	"github.com/segmentio/kafka-go"
)

type kafkagoProducer struct {
	writers     map[string]*kafka.Writer
	topicConf   map[string]TopicConfig
	initialized bool
}

func newKafkaGoProducer(prodType producerType,
	topicNames []string, topicConf map[string]TopicConfig, brokers []string,
	tls *tls.Config, schemaReg schemaRegistry) (p kafkagoProducer) {

	p.topicConf = topicConf

	transport := &kafka.Transport{
		TLS: tls,
		Dial: (&net.Dialer{
			Timeout:   3 * time.Second,
			DualStack: true,
		}).DialContext}

	for _, t := range topicNames {
		w := &kafka.Writer{
			Addr:      kafka.TCP(brokers[0]),
			Topic:     t,
			Balancer:  &kafka.RoundRobin{},
			Transport: transport}

		if prodType == ProducerTypeAsync {
			w.Async = true
			w.Completion = p.handleAsyncResponses
		}
		p.writers[t] = w
	}

	p.initialized = true
	return
}

func (p *kafkagoProducer) produceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(ctx, "")

	topicConf := p.topicConf[topic]
	msgbytes, e := topicConf.messageCodec.Encode(ctx, topic, msg)

	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	m := kafka.Message{
		Key:   []byte(key),
		Value: msgbytes}

	e = p.writers[topic].WriteMessages(ctx, m)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return errNotImpl
}

func (p *kafkagoProducer) handleAsyncResponses(messages []kafka.Message, e error) {
	lg := logger.New(context.Background(), "")

	for _, m := range messages {
		if e != nil {
			lg.Error(logger.LogCatUncategorized, errProduceFail(m.Topic), e)
		} else {
			lg.Infof(logger.LogCatUncategorized,
				infoEvent(infoProduceSuccess, m.Topic, int32(m.Partition), m.Offset))
		}
	}
}
