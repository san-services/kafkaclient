package kafkaclient

import "context"

// EncoderDecoder interface
type EncoderDecoder interface {
	// Encode encodes native golang as binary.
	//
	// topic: name of topic the message will be sent to
	// native: the golang data structure to be encoded
	Encode(ctx context.Context, topic string, native interface{}) (b []byte, e error)
	// Decode decodes binary into native golang.
	//
	// topic: name of topic the message was received from
	// b: the binary to be decoded,
	// target: pointer to data structure the binary data will be decoded into
	Decode(ctx context.Context, topic string, b []byte, target interface{}) error

	// GetSchemaID returns the topic schema ID, if applicable
	GetSchemaID(ctx context.Context, topic string) (int, error)
}
