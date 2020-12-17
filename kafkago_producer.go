package kafkaclient

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	logger "github.com/disturb16/apilogger"
	"github.com/segmentio/kafka-go"
)

type kafkagoProducer struct {
	writers     map[string]*kafka.Writer
	topicConf   map[string]TopicConfig
	avroCodec   EncoderDecoder
	jsonCodec   EncoderDecoder
	stringCodec EncoderDecoder
}

func newKafkaGoProducer(topicNames []string,
	topicConf map[string]TopicConfig, brokers []string,
	tls *tls.Config, schemaReg schemaRegistry) (p kafkagoProducer) {

	p.topicConf = topicConf
	p.avroCodec = newAvroEncDec(schemaReg)
	p.jsonCodec = newJSONEncDec(schemaReg)
	p.stringCodec = newStringEncDec()

	transport := &kafka.Transport{
		TLS: tls,
		Dial: (&net.Dialer{
			Timeout:   3 * time.Second,
			DualStack: true,
		}).DialContext}

	for _, t := range topicNames {
		p.writers[t] = &kafka.Writer{
			Addr:      kafka.TCP(brokers[0]),
			Topic:     t,
			Balancer:  &kafka.RoundRobin{},
			Transport: transport}
	}

	return
}

func (p *kafkagoProducer) produceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	lg := logger.New(ctx, "")

	topicConf := p.topicConf[topic]
	var msgbytes []byte

	switch topicConf.MessageType {
	case MessageFormatAvro:
		msgbytes, e = p.avroCodec.Encode(ctx, topic, msg)
	case MessageFormatJSON:
		msgbytes, e = p.jsonCodec.Encode(ctx, topic, msg)
	case MessageFormatString:
		msgbytes, e = p.stringCodec.Encode(ctx, topic, msg)
	default:
	}

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
