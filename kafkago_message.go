package kafkaclient

import (
	"fmt"

	logger "github.com/san-services/apilogger"
	"github.com/segmentio/kafka-go"
)

// KafkaGoMessage holds kafka-go message contents
// as well as an EncoderDecoder used to unmarshall message data
type KafkaGoMessage struct {
	m              kafka.Message
	encoderDecoder EncoderDecoder
}

// newMessage constructs and returns a new Message struct
func newKafkaGoMessage(
	m kafka.Message,
	ed EncoderDecoder) (msg ConsumerMessage) {

	msg = KafkaGoMessage{m: m, encoderDecoder: ed}
	return
}

// Unmarshall unmarshalls the message contents into the provided struct
func (m KafkaGoMessage) Unmarshall(native interface{}) (e error) {
	lg := logger.New(nil, "")

	e = m.encoderDecoder.Decode(m.m.Topic, m.m.Value, native)
	if e != nil {
		lg.Error(logger.LogCatKafkaDecode, e)
	}

	return
}

// InfoEvent constructs and returns a loggable event relating to the message
func (m KafkaGoMessage) InfoEvent(event string) string {
	return fmt.Sprintf(
		"%s: topic[%s], partition[%d], offset[%d]",
		event, m.m.Topic, m.m.Partition, m.m.Offset)
}

// Topic returns the message topic
func (m KafkaGoMessage) Topic() string {
	return m.m.Topic
}

// Partition returns the message partition
func (m KafkaGoMessage) Partition() int32 {
	return int32(m.m.Partition)
}

// Offset returns the message offset
func (m KafkaGoMessage) Offset() int64 {
	return m.m.Offset
}

// Key returns the message key
func (m KafkaGoMessage) Key() string {
	return string(m.m.Key)
}

// Value returns the message byte value
func (m KafkaGoMessage) Value() []byte {
	return m.m.Value
}
