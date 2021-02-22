package kafkaclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/hashicorp/go-uuid"
	cache "github.com/patrickmn/go-cache"
	logger "github.com/san-services/apilogger"
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
	retryInitDelay = 10 * time.Second
)

// Config holds specifics used to configure different
// part of the kafka client
type Config struct {
	KafkaVersion     string
	Brokers          []string
	Topics           []TopicConfig
	SchemaRegURL     string
	ConsumerType     consumerType
	ConsumerGroupID  string
	ProcDependencies ProcessorDependencies // injectable dependencies for message processors
	ProducerType     producerType
	ReadFromOldest   bool
	TLS              *tls.Config
	Debug            bool
}

func (c *Config) validate() (e error) {
	lg := logger.New(nil, "")

	if c.KafkaVersion == "" {
		errConfigMissing("KafkaVersion")
		lg.Error(logger.LogCatKafkaConfig, e)
		return
	}
	if c.Brokers == nil || len(c.Brokers) == 0 {
		errConfigMissing("KafkaVersion")
		lg.Error(logger.LogCatKafkaConfig, e)
		return
	}
	if c.Topics == nil || len(c.Topics) == 0 {
		errConfigMissing("KafkaVersion")
		lg.Error(logger.LogCatKafkaConfig, e)
		return
	}

	return
}

func (c *Config) finalize() {
	lg := logger.New(nil, "")

	sr, e := newSchemaReg(c.SchemaRegURL, c.TLS, c.TopicMap())
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
		return
	}

	if c.ConsumerType == "" {
		c.ConsumerType = ConsumerTypeGroup
	}

	if c.ConsumerType == ConsumerTypeGroup && c.ConsumerGroupID == "" {
		id, e := uuid.GenerateUUID()
		if e != nil {
			lg.Error(logger.LogCatKafkaConfig, e)
			return
		}
		c.ConsumerGroupID = fmt.Sprintf("consumergroup_%s", id)
	}

	if c.ProducerType == "" {
		c.ProducerType = ProducerTypeAsync
	}

	cacheTime := time.Minute * 10
	purgeTime := time.Minute * 10
	cache := cache.New(cacheTime, purgeTime)

	for i, t := range c.Topics {
		if t.MessageProcessor == nil {
			t.MessageProcessor = DefaultProcessor
		}

		switch t.MessageFormat {
		case MessageFormatAvro:
			c.Topics[i].messageCodec = newAvroEncDec(sr, cache, cacheTime)
		case MessageFormatJSON:
			e = errNoEncoderDecoder("JSON")
		case MessageFormatString:
			e = errNoEncoderDecoder("string")
		default:
			e = errNoMessageFmt(t.Name)
		}

		if e != nil {
			lg.Error(logger.LogCatKafkaConfig, e)
			return
		}
	}
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
	for _, t := range c.Topics {
		if t.DoConsume {
			n = append(n, t.Name)
		}
	}
	return
}

// WriteTopicNames constructs and returns a slice of all topic names
func (c Config) WriteTopicNames() (n []string) {
	for _, t := range c.Topics {
		if t.DoProduce {
			n = append(n, t.Name)
		}
	}
	return
}

// TopicConfig is a struct that holds data regarding an
// existing Kafka topic that can be consumed from or written to
type TopicConfig struct {
	Name          string
	MessageFormat messageFormat
	messageCodec  EncoderDecoder
	// Set DoConsume to true if this topic should be consumed from
	DoConsume bool
	// Set SoProduce to true if you will need to produce messages to this topic
	DoProduce           bool
	DelayProcessingMins time.Duration
	// FailedProcessingTopic is the retry topic to which a message
	// should be handed off in the case of a failure to process the message
	FailedProcessingTopic string
	// Schema is an optional string representation of the topic schema
	Schema           string
	SchemaVersion    int
	MessageProcessor func(context.Context, ProcessorDependencies, ConsumerMessage) error
}
