# kafkaclient

An overly-opinionated library to simplify interactions with existing go/kafka libraries.

## Supported base libraries

- shopify/sarama (implemented)
- segmentio/kafka-go (planned)

## Use

# Setup

```
const (
	TestTopic       = "test_topic"
	TestTopicRetry1 = "test_topic_retry"
	TestTopicDLQ    = "test_topic_dlq"
)

func main() {
    ctx := context.Background()

    topics := []kafkaclient.TopicConfig{
        {
            Name:                  TestTopic,
            DoConsume:             true,
            MessageFormat:         kafkaclient.MessageFormatAvro,
            DelayProcessingMins:   0,
            FailedProcessingTopic: TestTopicDLQ,
            MessageProcessor:      processTestTopic,
        },
        {
            Name:                  TestTopicRetry1,
            DoConsume:             true,
            DoProduce:             true,
            MessageFormat:         kafkaclient.MessageFormatAvro,
            DelayProcessingMins:   15,
            FailedProcessingTopic: TestTopicDLQ,
            MessageProcessor:      processTestTopic,
        },
        {
            Name:                  TestTopicDLQ,
            DoProduce:             true,
            MessageFormat:         kafkaclient.MessageFormatAvro,
        },
    }

config, e := kafkaclient.NewConfig(ctx, 
    "2.5.0", 
    []string{"127.0.0.1"}, 
    topics, 
    "", 
    kafkaclient.ConsumerTypeGroup,
    "test_consumer", 
    kafkaclient.ProducerTypeSync, 
    true, 
    nil, 
    true)

    if e != nil {
        log.Println(e)
        return
    }

    kc, e := kafkaclient.New(BaseSarama, config)
    if e != nil {
        t.Error(e)
        return
    }

    kc.StartConsume(ctx)

    e := kc.kafkaClient.ProduceMessage(ctx, TestTopic, "message_key_238to2efgb", testTopicAvroMessage{ID: 1, Name: "foofoo"})
    if e != nil {
        log.Println(e)
    }
}

func processTestTopic(ctx context.Context, msg kafkaclient.ConsumerMessage) (e error) {
	return
}

type testTopicAvroMessage struct {
	ID    int64   `avro:"id"`
	Name  string  `avro:"name"`
}
```

## Notes

- topic consumption starts in its own goroutine, so calling StartConsume is non-blocking
- 