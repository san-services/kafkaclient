package kafkaclient

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"

	logger "github.com/disturb16/apilogger"
)

var (
	errPtrRequired = errors.New("pointer to struct required")
)

// StructEncoder implements the sarama.Encoder interface
type structEncoder struct {
	topic          string
	encoderDecoder encoderDecoder
	msgBinary      []byte
	msgStruct      interface{}
	ctx            context.Context
}

// NewStructEncoder constructs and returns a new StructEncoder
func newStructEncoder(ctx context.Context, topic string,
	msgStruct interface{}, ed encoderDecoder) (s *structEncoder, e error) {

	lg := logger.New(ctx, "")

	if reflect.ValueOf(msgStruct).Kind() != reflect.Ptr {
		e = errPtrRequired
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return &structEncoder{
		topic:          topic,
		encoderDecoder: ed,
		msgStruct:      msgStruct,
		ctx:            ctx}, nil
}

// Encode transforms and encodes the struct data
func (s *structEncoder) Encode() (b []byte, e error) {
	lg := logger.New(s.ctx, "")

	codec, e := s.encoderDecoder.GetCodec(s.ctx, s.topic)

	b, e = codec.NativeToBinary(s.ctx, s.msgStruct)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	be := newByteEncoder(s.ctx, s.topic, b, s.encoderDecoder)
	s.msgBinary, e = be.Encode()
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
}

func (s *structEncoder) Length() int {
	return 5 + len(s.msgBinary)
}

// ByteEncoder implements the sarama.Encoder interface
type byteEncoder struct {
	schemaID  int
	msgBinary []byte
}

func newByteEncoder(ctx context.Context,
	topic string, msgBin []byte, ed encoderDecoder) (b *byteEncoder) {

	lg := logger.New(ctx, "")

	codec, e := ed.GetCodec(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return &byteEncoder{
		msgBinary: msgBin,
		schemaID:  codec.GetSchemaID()}
}

// Encode finalizes the binary avro message, by adding bytes for required meta data.
//
// Note: the Confluent schema registry has special requirements for Avro serialization -
// message content needs to be serialized along with the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
// Code ref: https://github.com/dangkaka/go-kafka-avro/blob/master/avroProducer.go
func (b *byteEncoder) Encode() ([]byte, error) {
	var finalMsg []byte

	// Confluent serialization format version number; currently always 0.
	finalMsg = append(finalMsg, byte(0))

	// 4-byte schema ID as returned by Schema Registry
	binarySchemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaID, uint32(b.schemaID))
	finalMsg = append(finalMsg, binarySchemaID...)

	// Avro serialized data in Avro's binary encoding
	finalMsg = append(finalMsg, b.msgBinary...)
	return finalMsg, nil
}

// Length of schemaId and Content.
func (b *byteEncoder) Length() int {
	return 5 + len(b.msgBinary)
}
