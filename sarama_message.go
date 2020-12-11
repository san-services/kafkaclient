package kafkaclient

import (
	"github.com/Shopify/sarama"
)

// // SaramaMessage
// type SaramaMessage struct {
// 	m              *sarama.ConsumerMessage
// 	encoderDecoder EncoderDecoder
//

// newMessage constructs and returns a new Message struct
func newSaramaMessage(
	m *sarama.ConsumerMessage,
	ed EncoderDecoder) (msg ConsumerMessage) {

	msg = ConsumerMessage{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		ed:        ed}

	return
}

// // Unmarshall unmarshalls the message contents into the provided struct
// func (m SaramaMessage) Unmarshall(ctx context.Context, native interface{}) (e error) {
// 	lg := logger.New(ctx, "")

// 	e = m.encoderDecoder.Decode(ctx, m.m.Topic, m.m.Value, native)
// 	if e != nil {
// 		lg.Error(logger.LogCatUncategorized, e)
// 	}

// 	return
// }

// // InfoEvent constructs and returns a loggable event relating to the message
// func (m SaramaMessage) InfoEvent(event string) string {
// 	return fmt.Sprintf(
// 		"%s: topic[%s], partition[%d], offset[%d]",
// 		event, m.m.Topic, m.m.Partition, m.m.Offset)
// }

// // Topic returns the message topic
// func (m SaramaMessage) Topic() string {
// 	return m.m.Topic
// }

// // Partition returns the message partition
// func (m SaramaMessage) Partition() int32 {
// 	return m.m.Partition
// }

// // Offset returns the message offset
// func (m SaramaMessage) Offset() int64 {
// 	return m.m.Offset
// }

// // Key returns the message key
// func (m SaramaMessage) Key() string {
// 	return string(m.m.Key)
// }

// // Value returns the message byte value
// func (m SaramaMessage) Value() []byte {
// 	return m.m.Value
// }
