package kafkaclient

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestUnmarshall_avro(t *testing.T) {
	ctx := context.Background()

	sr := mockSchemaReg{}
	ed := newAvroEncDec(sr)

	k := []byte("testkey")
	v := testTopicMessage{ID: 1, Name: "unmarshall test"}

	vb, e := ed.Encode(ctx, testTopicName, v)
	if e != nil {
		t.Error(e)
	}

	m := sarama.ConsumerMessage{
		Timestamp: time.Now(),
		Topic:     testTopicName,
		Key:       k,
		Value:     vb,
		Partition: 0,
		Offset:    22}

	msg := newSaramaMessage(&m, ed)

	decoded := testTopicMessage{}
	e = msg.Unmarshall(ctx, &decoded)
	if e != nil {
		t.Error(e)
	}

	if decoded.ID != v.ID || decoded.Name != v.Name {
		t.Error("decode failed")
	}
}
