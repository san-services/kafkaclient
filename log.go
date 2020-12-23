package kafkaclient

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	// INFO
	infoConsumerReady  = "kafka consumer ready"
	infoProduceSuccess = "kafka message added to topic"
	infoConsumerTerm   = func(reason string) string { return fmt.Sprintf("terminating kafka consumer: %s", reason) }
	infoEvent          = func(e string, topic string, partition int32, offset int64) string {
		return fmt.Sprintf("%s: topic[%s], partition[%d], offset[%d]", e, topic, partition, offset)
	}

	// ERROR
	// general
	errNotImpl = errors.New("functionality not yet implemented")

	// config
	errTopicConfMissing = errors.New("topic config missing")
	errKafkaVersion     = func(v string) error { return fmt.Errorf("error parsing kafka version [%s]", v) }
	errInvalidBase      = func(base baseLibrary) error { return fmt.Errorf("cannot use unimplemented base library [%s]", base) }

	// produce/consume
	errConsumer        = func(e error) error { return fmt.Errorf("kafka consumer error - %s", e) }
	errConsumerClose   = func(e error) error { return fmt.Errorf("error closing consumer - %s", e) }
	errConsumerUninit  = errors.New("kafka consumer not yet initialized, see logs")
	errInvalidProducer = errors.New("invalid producer type configured")
	errProduceFail     = func(topic string) error { return fmt.Errorf("failed to produce kafka message", topic) }
	errProducerUninit  = errors.New("kafka producer not initialized, see logs")
	errMessageFormat   = errors.New("unsupported topic message format configured")
	errMessageType     = errors.New("unsupported message data type")
	errCommit          = func(e error) error { return fmt.Errorf("error committing offset - %s", e.Error()) }
	errSetOffset       = func(e error) error { return fmt.Errorf("error setting reader offset - %s", e.Error()) }
	errMsgRead         = func(e error) error { return fmt.Errorf("error reading message - %s", e.Error()) }

	// encode/decode
	errMessageFmt          = errors.New("encode/decode - problem with message format")
	errStructRequired      = errors.New("input is not a struct")
	errPtrStructRequired   = errors.New("pointer to struct required")
	errFieldValNil         = errors.New("field value is nil")
	errUnmarshallFieldType = func(fName string, targetType reflect.Type, actualType reflect.Type) error {
		return fmt.Errorf("struct field [%s], currently type %s, should be type %s to match binary data",
			fName, targetType.String(), actualType.String())
	}
)
