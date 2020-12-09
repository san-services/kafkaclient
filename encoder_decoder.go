package kafkaclient

import "context"

// EncoderDecoder is an interface implemented by schema codecs
type encoderDecoder interface {
	GetCodec(ctx context.Context, topic string) (codec, error)
}

// Codec is an interface implemented by kafka topic codecs
type codec interface {
	BinaryToNative(ctx context.Context, b []byte, ptr interface{}) error
	NativeToBinary(ctx context.Context, s interface{}) (b []byte, e error)
	GetSchemaID() int
}
