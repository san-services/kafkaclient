package kafkaclient

import (
	"context"
	"testing"
)

var (
	testTopicName   = "test"
	testTopicSchema = `{
		"type": "record",
		"name": "testSchema",
		"fields": [
			{
				"name": "ID",
				"type": [
					"null",
					"long"
				],
				"default": null
			},
			{
				"name": "NAME",
				"type": [
					"null",
					"string"
				],
				"default": null
			}
		]
	}`
	retryTopicName   = "test_retry_1"
	retryTopicSchema = `{
		"type": "record",
		"name": "retrySchema",
		"fields": [
			{
				"name": "ERROR_MESSAGE",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "ORIGINAL_TOPIC",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "ORIGINAL_MESSAGE",
				"type": [
					"null",
					"bytes"
				],
				"default": null
			}
		]
	}`
)

type testTopicMessage struct {
	ID   int64  `avro:"ID"`
	Name string `avro:"NAME"`
}

type retryTopicMessage struct {
	ErrorMessage    string `avro:"ERROR_MESSAGE"`
	OriginalTopic   string `avro:"ORIGINAL_TOPIC"`
	OriginalMessage []byte `avro:"ORIGINAL_MESSAGE"`
}

func TestAvroDecode(t *testing.T) {
	ctx := context.Background()
	sr := mockSchemaReg{}
	ed := newAvroEncDec(sr)

	testMessage := testTopicMessage{ID: 1, Name: "TestMessage"}

	b, e := ed.Encode(ctx, testTopicName, testMessage)
	if e != nil {
		t.Error(e)
	}

	retryMessage := retryTopicMessage{
		ErrorMessage:    "test error",
		OriginalTopic:   testTopicName,
		OriginalMessage: b}

	rb, e := ed.Encode(ctx, retryTopicName, retryMessage)
	if e != nil {
		t.Error(e)
	}

	decodedRetryMessage := retryTopicMessage{}
	e = ed.Decode(ctx, retryTopicName, rb, &decodedRetryMessage)
	if e != nil {
		t.Error(e)
	}

	decodedOrigMessage := testTopicMessage{}
	e = ed.Decode(ctx, testTopicName, decodedRetryMessage.OriginalMessage, &decodedOrigMessage)
	if e != nil {
		t.Error(e)
	}
}

type mockSchemaReg struct {
}

func (s mockSchemaReg) GetSchemaByID(
	ctx context.Context, id int) (schemaID string, e error) {

	return
}

func (s mockSchemaReg) GetSchemaByTopic(
	ctx context.Context, topic string) (schema string, schemaID int, e error) {

	switch topic {
	case testTopicName:
		return testTopicSchema, 1, nil
	case retryTopicName:
		return retryTopicSchema, 2, nil
	default:
		return
	}
}

func (s mockSchemaReg) RegisterSchema(
	ctx context.Context, topic string) (schemaID int, e error) {

	return
}
