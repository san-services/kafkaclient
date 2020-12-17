package kafkaclient

import (
	"context"
	"crypto/tls"

	"github.com/segmentio/kafka-go"
)

type kafkagoProducer struct {
	writers map[string]*kafka.Writer
}

func newKafkaGoProducer(topicNames []string,
	brokers []string, tls *tls.Config) (p kafkagoProducer) {

	// for _, t := range topicNames {
	// 	p.writers[t] = &kafka.Writer{
	// 		Addr:     kafka.TCP(brokers[0]),
	// 		Topic:    t,
	// 		Balancer: &kafka.RoundRobin{}}

	// }

	return kafkagoProducer{}
}

func (p *kafkagoProducer) produceMessage(
	ctx context.Context, topic string, key string, msg interface{}) (e error) {

	// lg := logger.New(ctx, "")

	// m := kafka.Message{
	// 	Key:   []byte("Key-A"),
	// 	Value: []byte("Hello World!")}

	// e = p.writers[topic].WriteMessages(ctx, m)
	// if e != nil {
	// 	lg.Error(logger.LogCatUncategorized, e)
	// 	return
	// }

	return errNotImpl
}
