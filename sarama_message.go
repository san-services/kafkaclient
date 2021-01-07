package kafkaclient

import (
	"context"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
)

// SaramaMessage holds sarama message contents
// as well as an EncoderDecoder used to unmarshall message data
type SaramaMessage struct {
	m              *sarama.ConsumerMessage
	encoderDecoder EncoderDecoder
}

// newMessage constructs and returns a new Message struct
func newSaramaMessage(
	m *sarama.ConsumerMessage,
	ed EncoderDecoder) (msg ConsumerMessage) {

	msg = SaramaMessage{m: m, encoderDecoder: ed}
	return
}

// Unmarshall unmarshalls the message contents into the provided struct
func (m SaramaMessage) Unmarshall(ctx context.Context, native interface{}) (e error) {
	lg := logger.New(ctx, "")

	e = m.encoderDecoder.Decode(ctx, m.m.Topic, m.m.Value, native)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
}

// Topic returns the message topic
func (m SaramaMessage) Topic() string {
	return m.m.Topic
}

// Partition returns the message partition
func (m SaramaMessage) Partition() int32 {
	return m.m.Partition
}

// Offset returns the message offset
func (m SaramaMessage) Offset() int64 {
	return m.m.Offset
}

// Key returns the message key
func (m SaramaMessage) Key() string {
	return string(m.m.Key)
}

// Value returns the message byte value
func (m SaramaMessage) Value() []byte {
	return m.m.Value
}
