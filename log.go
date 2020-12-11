package kafkaclient

import (
	"errors"
	"fmt"
)

var (
	// INFO
	infoEvent = func(event string, topic string, partition int32, offset int64) string {
		return fmt.Sprintf("%s: topic[%s], partition[%d], offset[%d]", event, topic, partition, offset)
	}
	infoMsgClaimed = func(topic string, partition int32, offset int64) string {
		return infoEvent("kafka message claimed", topic, partition, offset)
	}
	infoConsumerReady = "kafka consumer ready"
	infoConsumerTerm  = func(reason string) string { return fmt.Sprintf("terminating datasync consumer: %s", reason) }

	// ERROR
	errKafkaVersion  = func(v string) error { return fmt.Errorf("error parsing kafka version [%s]", v) }
	errConsumer      = func(e error) error { return fmt.Errorf("datasync consumer error - %s", e) }
	errConsumerClose = func(e error) error { return fmt.Errorf("error closing consumer - %s", e) }
	errNotImpl       = errors.New("functionality not yet implemented")
)
