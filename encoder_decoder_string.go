package kafkaclient

import "context"

type strEncoderDecoder struct {
}

// newJSONEncDec constructs and returns a new JSON message EncoderDecoder
func newStringEncDec() EncoderDecoder {
	return &avroEncoderDecoder{}
}

func (ed strEncoderDecoder) Encode(
	ctx context.Context, topic string, s interface{}) (b []byte, e error) {

	return nil, errNotImpl
}

func (ed strEncoderDecoder) Decode(
	topic string, b []byte, target interface{}) (e error) {

	return errNotImpl
}

func (ed strEncoderDecoder) GetSchemaID(
	ctx context.Context, topic string) (id int, e error) {

	return
}
