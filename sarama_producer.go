package kafkaclient

import (
	"context"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
)

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
