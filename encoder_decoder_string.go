package kafkaclient

import "context"

type strEncoderDecoder struct {
	schemaReg schemaRegistry
}

// newJSONEncDec constructs and returns a new JSON message EncoderDecoder
func newStringEncDec(s schemaRegistry) EncoderDecoder {
	return &avroEncoderDecoder{schemaReg: s}
}

func (ed strEncoderDecoder) Encode(
	ctx context.Context, topic string, s interface{}) (b []byte, e error) {

	return nil, errNotImpl
}

func (ed strEncoderDecoder) Decode(ctx context.Context,
	topic string, b []byte, target interface{}) (e error) {

	return errNotImpl
}

func (ed strEncoderDecoder) GetSchemaID(
	ctx context.Context, topic string) (id int, e error) {

	return
}
