package kafkaclient

import (
	"context"
	"crypto/tls"
	"time"
)

type consumerType string
type messageType string
type producerType string

const (
	// ConsumerTypeSimple configures a simple consumer as opposed to a node in a consumer group
	ConsumerTypeSimple consumerType = "CONSUMER_SIMPLE"
	// ConsumerTypeGroup configures the consumer as part of a consumer group
	ConsumerTypeGroup consumerType = "CONSUMER_GROUP"

	// MessageTypeAvro specified that messages in a topic are stored in avro format
	MessageTypeAvro messageType = "MESSAGE_AVRO"
	// MessageTypeJSON specified that messages in a topic are stored in JSON format
	MessageTypeJSON messageType = "MESSAGE_JSON"
	// MessageTypeString specified that messages in a topic are stored in string format
	MessageTypeString messageType = "MESSAGE_STRING"

	// ProducerTypeAsync configures a producer with an asynchronous response mechanism
	ProducerTypeAsync producerType = "PRODUCER_ASYNC"
	// ProducerTypeSync configures a producer with synchronous feedback
	ProducerTypeSync producerType = "PRODUCER_SYNC"

	// config defaults
)

type Config struct {
	KafkaVersion    string
	Brokers         []string
	Topics          []TopicConfig
	SchemaRegURL    string
	ConsumerType    consumerType
	ConsumerGroupID string
	ProducerType    producerType
	ReadFromOldest  bool
	TLS             *tls.Config
	Debug           bool
}

// NewConfig constructs and returns a Config struct
func NewConfig(
	version string, brokers []string, topics []TopicConfig,
	schemaRegURL string, consType consumerType, groupID string, prodType producerType,
	readFromOldest bool, tls *tls.Config, debug bool) Config {

	return Config{
		KafkaVersion:    version,
		Brokers:         brokers,
		Topics:          topics,
		SchemaRegURL:    schemaRegURL,
		ConsumerType:    consType,
		ConsumerGroupID: groupID,
		ProducerType:    prodType,
		ReadFromOldest:  readFromOldest,
		TLS:             tls,
		Debug:           debug}
}

// TopicMap constructs and returns a map of topic
// configuration, using each topic name as the map key
func (c Config) TopicMap() (m map[string]TopicConfig) {
	m = make(map[string]TopicConfig)
	for i, t := range c.Topics {
		m[t.Name] = c.Topics[i]
	}
	return
}

// TopicNames constructs and returns a slice of all topic names
func (c Config) TopicNames() (n []string) {
	n = make([]string, len(c.Topics))
	for i := range c.Topics {
		n[i] = c.Topics[i].Name
	}
	return
}

// TopicConfig is a struct that holds data regarding an
// existing Kafka topic that can be consumed from or written to
type TopicConfig struct {
	Name                  string
	MessageEncoderDecoder encoderDecoder
	DelayProcessingMins   time.Duration
	// FailedProcessingTopic is the retry topic to which a message
	// should be handed off in the case of a failure to process the message
	FailedProcessingTopic string
	// Schema is an optional string representation of the topic schema
	Schema           string
	SchemaVersion    int
	MessageProcessor func(context.Context, ConsumerMessage) error
}

// NewTopicConfig constructs and returns a TopicConfig struct
func NewTopicConfig(
	name string, msgType messageType, delayMins time.Duration,
	failTopic string, schema string, schemaVersion int,
	processorFunc func(context.Context, ConsumerMessage) error) TopicConfig {

	switch msgType {
	case MessageTypeAvro:
	case MessageTypeJSON:
	case MessageTypeString:
	default:
	}

	return TopicConfig{
		Name: name,
		// MessageEncoderDecoder: msgType,
		DelayProcessingMins:   delayMins,
		FailedProcessingTopic: failTopic,
		Schema:                schema,
		SchemaVersion:         schemaVersion,
		MessageProcessor:      processorFunc}
}
