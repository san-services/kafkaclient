package kafkaclient

import "context"

// ConsumerMessage is an interface implememented by kafka consumer message types
type ConsumerMessage interface {
	Unmarshall(ctx context.Context, native interface{}) (e error)
	Topic() string
	Key() string
	Offset() int64
	Partition() int32
	Value() []byte
}
