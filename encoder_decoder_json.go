package kafkaclient

import (
	"context"
)

type jsonEncoderDecoder struct {
	schemaReg schemaRegistry
}

// newJSONEncDec constructs and returns a new JSON message EncoderDecoder
func newJSONEncDec(s schemaRegistry) EncoderDecoder {
	return &avroEncoderDecoder{schemaReg: s}
}

func (ed jsonEncoderDecoder) Encode(
	ctx context.Context, topic string, s interface{}) (b []byte, e error) {

	return nil, errNotImpl
}

func (ed jsonEncoderDecoder) Decode(
	topic string, b []byte, target interface{}) (e error) {

	return errNotImpl
}

func (ed jsonEncoderDecoder) GetSchemaID(
	ctx context.Context, topic string) (id int, e error) {

	return
}
