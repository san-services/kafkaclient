package kafkaclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	logger "github.com/disturb16/apilogger"
)

var (
	errNoEncoderDecoder = func(typ string) error {
		return fmt.Errorf("%s message encoder/decoder not yet implemented", typ)
	}
)

type consumerType string
type messageFormat string
type producerType string

const (
	// ConsumerTypeSimple configures a simple consumer as opposed to a node in a consumer group
	// ConsumerTypeSimple consumerType = "CONSUMER_SIMPLE" // not yet implemented

	// ConsumerTypeGroup configures the consumer as part of a consumer group
	ConsumerTypeGroup consumerType = "CONSUMER_GROUP"

	// MessageFormatAvro specifies that messages in a topic are stored in avro format
	MessageFormatAvro messageFormat = "MESSAGE_AVRO"
	// MessageFormatJSON specifies that messages in a topic are stored in JSON format
	MessageFormatJSON messageFormat = "MESSAGE_JSON"
	// MessageFormatString specifies that messages in a topic are stored in string format
	MessageFormatString messageFormat = "MESSAGE_STRING"

	// ProducerTypeAsync configures a producer with an asynchronous response mechanism
	ProducerTypeAsync producerType = "PRODUCER_ASYNC"
	// ProducerTypeSync configures a producer with synchronous feedback
	ProducerTypeSync producerType = "PRODUCER_SYNC"

	// config defaults
)

// Config holds specifics used to configure different
// part of the kafka client
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
	ctx context.Context, version string, brokers []string,
	topics []TopicConfig, schemaRegURL string, consType consumerType,
	groupID string, prodType producerType, readFromOldest bool,
	tls *tls.Config, debug bool) (c Config, e error) {

	lg := logger.New(ctx, "")

	c = Config{
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

	sr, e := newSchemaReg(schemaRegURL, tls, c.TopicMap())
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	for i, t := range topics {
		if t.MessageProcessor == nil {
			t.MessageProcessor = DefaultProcessor
		}

		switch t.MessageFormat {
		case MessageFormatAvro:
			topics[i].messageCodec = newAvroEncDec(sr)
		case MessageFormatJSON:
			e = errNoEncoderDecoder("JSON")
		case MessageFormatString:
			e = errNoEncoderDecoder("string")
		default:
		}
	}

	return
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

// ReadTopicNames constructs and returns a slice of all topic names
func (c Config) ReadTopicNames() (n []string) {
	n = make([]string, len(c.Topics))
	for i, t := range c.Topics {
		if t.DoRead {
			n[i] = c.Topics[i].Name
		}
	}
	return
}

// WriteTopicNames constructs and returns a slice of all topic names
func (c Config) WriteTopicNames() (n []string) {
	n = make([]string, len(c.Topics))
	for i, t := range c.Topics {
		if t.DoWrite {
			n[i] = c.Topics[i].Name
		}
	}
	return
}

// TopicConfig is a struct that holds data regarding an
// existing Kafka topic that can be consumed from or written to
type TopicConfig struct {
	Name                string
	MessageFormat       messageFormat
	messageCodec        EncoderDecoder
	DoRead              bool
	DoWrite             bool
	DelayProcessingMins time.Duration
	// FailedProcessingTopic is the retry topic to which a message
	// should be handed off in the case of a failure to process the message
	FailedProcessingTopic string
	// Schema is an optional string representation of the topic schema
	Schema           string
	SchemaVersion    int
	MessageProcessor func(context.Context, ConsumerMessage) error
}
