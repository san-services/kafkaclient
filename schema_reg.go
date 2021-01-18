package kafkaclient

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	schemaregistry "github.com/landoop/schema-registry"
	logger "github.com/san-services/apilogger"
)

// schemaRegistry is an interface implemented by kafka schema registry clients
type schemaRegistry interface {
	GetSchemaByID(ctx context.Context, id int) (string, error)
	GetSchemaByTopic(ctx context.Context, topic string) (schema string, schemaID int, e error)
	RegisterSchema(ctx context.Context, topic string) (schemaID int, e error)
}

var (
	// usually, for each topic there are subjects $TOPIC-key (if there is a key schema)
	// and $TOPIC-value, each one with their own schema
	subject = func(topic string) string { return topic + "-value" }

	errNoSchema = errors.New("no schema file path configured")
)

// SchemaReg implements the kafka.SchemaRegistry interface
type schemaReg struct {
	client *schemaregistry.Client
	topics map[string]TopicConfig
}

// New contructs and returns a new SchemaReg struct
func newSchemaReg(
	url string, tlsConf *tls.Config,
	topicMap map[string]TopicConfig) (c schemaRegistry, e error) {

	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConf,
			TLSNextProto:    make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			Dial: func(network string, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, 10*time.Second)
			},
		},
	}

	client, e := schemaregistry.NewClient(url,
		schemaregistry.UsingClient(httpsClient))


	if e != nil {
		return nil, e
	}

	return &schemaReg{client: client, topics: topicMap}, nil
}

// GetSchemaByID retrieves a schema in string form identified by the given id
func (sr *schemaReg) GetSchemaByID(
	ctx context.Context, id int) (schema string, e error) {

	lg := logger.New(ctx, "")

	schema, e = sr.client.GetSchemaByID(id)
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
	}

	return
}

// GetSchemaByTopic looks for a schema and it's id for the given topic, if it
// does not find one, it registers the schema and returns the schema and id.
// In order to register an unregistered schema, a schema file for the topic should
// both exist and have its location configured in the project settings
func (sr *schemaReg) GetSchemaByTopic(
	ctx context.Context, topic string) (schema string, schemaID int, e error) {

	lg := logger.New(ctx, "")

	s, e := sr.client.GetLatestSchema(subject(topic))
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
		return
	}

	topicConf := sr.topics[topic]
	if s.Version < topicConf.SchemaVersion {
		schemaID, e = sr.RegisterSchema(ctx, topic)
		if e != nil {
			lg.Error(logger.LogCatKafkaSchemaReg, e)
			return
		}

		schema, e = sr.GetSchemaByID(ctx, schemaID)
		if e != nil {
			lg.Error(logger.LogCatKafkaSchemaReg, e)
		}
	}

	return s.Schema, s.ID, e
}

// RegisterSchema registers a schema for the given topic.
// In order for this operation to succeed, a schema file for the topic should
// both exist and have its location configured in the project settings
func (sr *schemaReg) RegisterSchema(
	ctx context.Context, topic string) (schemaID int, e error) {

	lg := logger.New(ctx, "")

	schemaID, e = sr.client.RegisterNewSchema(subject(topic), sr.topics[topic].Schema)
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
	}

	return
}
