package kafkaclient

import (
	"context"
	"encoding/binary"
	"errors"

	logger "github.com/san-services/apilogger"
)

var (
	errPtrRequired = errors.New("pointer to struct required")
)

// saramaStructEncoder implements the sarama.Encoder interface
type saramaStructEncoder struct {
	topic          string
	encoderDecoder EncoderDecoder
	msgBinary      []byte
	msgStruct      interface{}
	ctx            context.Context
}

// newSaramaStructEncoder constructs and returns a new saramaStructEncoder
func newSaramaStructEncoder(ctx context.Context, topic string,
	msgStruct interface{}, ed EncoderDecoder) (s *saramaStructEncoder, e error) {

	return &saramaStructEncoder{
		topic:          topic,
		encoderDecoder: ed,
		msgStruct:      msgStruct,
		ctx:            ctx}, nil
}

// Encode transforms and encodes the struct data
func (se *saramaStructEncoder) Encode() (b []byte, e error) {
	lg := logger.New(se.ctx, "")

	initialBytes, e := se.encoderDecoder.Encode(se.ctx, se.topic, se.msgStruct)
	if e != nil {
		lg.Error(logger.LogCatKafkaEncode, e)
		return
	}

	be := newSaramaByteEncoder(se.ctx,
		se.topic, initialBytes, se.encoderDecoder)

	b, e = be.Encode()
	if e != nil {
		lg.Error(logger.LogCatKafkaEncode, e)
		return
	}

	se.msgBinary = b
	return
}

// Length of schemaID and message content
func (se *saramaStructEncoder) Length() int {
	return 5 + len(se.msgBinary)
}

// ByteEncoder implements the sarama.Encoder interface
type saramaByteEncoder struct {
	schemaID  int
	msgBinary []byte
}

func newSaramaByteEncoder(ctx context.Context,
	topic string, msgBin []byte, ed EncoderDecoder) (b *saramaByteEncoder) {

	lg := logger.New(ctx, "")

	schemaID, e := ed.GetSchemaID(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
		return
	}

	return &saramaByteEncoder{
		msgBinary: msgBin,
		schemaID:  schemaID}
}

// Encode finalizes the binary avro message, by adding bytes for required meta data.
//
// Note: the Confluent schema registry has special requirements for Avro serialization -
// message content needs to be serialized along with the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
// Code ref: https://github.com/dangkaka/go-kafka-avro/blob/master/avroProducer.go
func (b *saramaByteEncoder) Encode() ([]byte, error) {
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

// Length of schemaID and message content
func (b *saramaByteEncoder) Length() int {
	return 5 + len(b.msgBinary)
}
