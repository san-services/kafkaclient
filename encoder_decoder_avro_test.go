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

func TestAvroDecode(t *testing.T) {
	ctx := context.Background()
	sr := mockSchemaReg{}
	ed := newAvroEncDec(sr)

	// decode initial topic message
	testMessage := testTopicMessage{ID: 1, Name: "TestMessage"}

	b, e := ed.Encode(ctx, testTopicName, testMessage)
	if e != nil {
		t.Error(e)
	}

	decodedTestMessage := testTopicMessage{}
	e = ed.Decode(ctx, testTopicName, b, &decodedTestMessage)
	if e != nil {
		t.Error(e)
	}

	// decode retry topic message,
	// decode inner (original) separately
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

	if decodedRetryMessage.ErrorMessage != retryMessage.ErrorMessage ||
		decodedRetryMessage.OriginalTopic != retryMessage.OriginalTopic ||
		decodedRetryMessage.OriginalMessage == nil {

		t.Error("decode failed")
	}

	decodedOrigMessage := testTopicMessage{}
	e = ed.Decode(ctx, testTopicName, decodedRetryMessage.OriginalMessage, &decodedOrigMessage)
	if e != nil {
		t.Error(e)
	}

	if decodedOrigMessage.ID != testMessage.ID ||
		decodedOrigMessage.Name != testMessage.Name {

		t.Error("decode failed")
	}

	// decode retry topic message,
	// decode inner (original) into target's inner struct (complex decode)
	nb, e := ed.Encode(ctx, retryTopicName, retryMessage)
	if e != nil {
		t.Error(e)
	}

	decodedComplex := retryTopicMsgComplex{}
	e = ed.Decode(ctx, retryTopicName, nb, &decodedComplex)
	if e != nil {
		t.Error(e)
	}
}

type testTopicMessage struct {
	ID   int64  `avro:"ID"`
	Name string `avro:"NAME"`
}

type retryTopicMessage struct {
	ErrorMessage    string `avro:"ERROR_MESSAGE"`
	OriginalTopic   string `avro:"ORIGINAL_TOPIC"`
	OriginalMessage []byte `avro:"ORIGINAL_MESSAGE"`
}

type retryTopicMsgComplex struct {
	ErrorMessage    string           `avro:"ERROR_MESSAGE"`
	OriginalTopic   string           `avro:"ORIGINAL_TOPIC"`
	OriginalMessage testTopicMessage `avro:"ORIGINAL_MESSAGE" topic:"test"`
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
