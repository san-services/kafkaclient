package kafkaclient

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	// INFO
	infoConsumerReady = "kafka consumer ready"
	infoConsumerTerm  = func(reason string) string { return fmt.Sprintf("terminating kafka consumer: %s", reason) }

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
	errInvalidProducer = errors.New("invalid producer type configured")
	errMessageFormat   = errors.New("unsupported topic message format configured")
	errMessageType     = errors.New("unsupported message data type")

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
