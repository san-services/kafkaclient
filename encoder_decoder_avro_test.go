package kafkaclient

import (
	"context"
	"testing"
)

func TestAvroDecode(t *testing.T) {
	ctx := context.Background()
	sr := mockSchemaReg{}
	ed := newAvroEncDec(sr, nil, 0)

	// decode initial topic message
	testMessage := testTopicMessage{ID: 1, Name: "TestMessage"}

	b, e := ed.Encode(ctx, testTopicName, testMessage)
	if e != nil {
		t.Error(e)
	}

	//Decode something..

	decodedTestMessage := testTopicMessage{}
	e = ed.Decode(testTopicName, b, &decodedTestMessage)
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
	e = ed.Decode(retryTopicName, rb, &decodedRetryMessage)
	if e != nil {
		t.Error(e)
	}

	if decodedRetryMessage.ErrorMessage != retryMessage.ErrorMessage ||
		decodedRetryMessage.OriginalTopic != retryMessage.OriginalTopic ||
		decodedRetryMessage.OriginalMessage == nil {

		t.Error("decode failed")
	}

	decodedOrigMessage := testTopicMessage{}
	e = ed.Decode(testTopicName, decodedRetryMessage.OriginalMessage, &decodedOrigMessage)
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
	e = ed.Decode(retryTopicName, nb, &decodedComplex)
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
