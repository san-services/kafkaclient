package kafkaclient

import "context"

const (
	tagOrigTopic = "original_topic"
)

// ConsumerMessage is an interface implememented by kafka consumer message types
type ConsumerMessage interface {
	Unmarshall(ctx context.Context, native interface{}) (e error)
	Topic() string
	Key() string
	Offset() int64
	Partition() int32
	Value() []byte
}

type failedMessage struct {
	msg        ConsumerMessage
	retryTopic string
	e          error
}

func newFailedMessage(msg ConsumerMessage,
	retryTopic string, e error) failedMessage {

	return failedMessage{
		msg: msg, retryTopic: retryTopic, e: e}
}

// RetryTopicMessage is a native go representation of a message on a retry topic
type RetryTopicMessage struct {
	OriginalTopic     string `json:"original_topic" avro:"original_topic"`
	OriginalPartition int32  `json:"original_partition" avro:"original_partition"`
	OriginalOffset    int64  `json:"original_offset" avro:"original_offset"`
	OriginalMessage   []byte `json:"original_message" avro:"original_message"`
	Error             string `json:"error" avro:"error"`
}

// NewRetryTopicMessage constructs and returns a new RetryTopicMessage to be added to a retry topic
func NewRetryTopicMessage(
	origTopic string, origPart int32,
	origOffset int64, origMsg []byte, e error) RetryTopicMessage {

	return RetryTopicMessage{
		OriginalTopic:     origTopic,
		OriginalPartition: origPart,
		OriginalOffset:    origOffset,
		OriginalMessage:   origMsg,
		Error:             e.Error()}
}
