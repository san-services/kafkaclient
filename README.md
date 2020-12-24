# kafkaclient

An overly-opinionated library to simplify interactions with existing go/kafka libraries.

## Supported base libraries

- shopify/sarama 
- segmentio/kafka-go 

## Use

```go
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

    kc, e := kafkaclient.New(kafkaclient.BaseSarama, config)
    if e != nil {
        log.Println(e)
        return
    }

    kc.StartConsume(ctx)

    e := kc.kafkaClient.ProduceMessage(ctx, TestTopic, 
	    "message_key_238to2efgb", testTopicAvroMessage{ID: 1, Name: "foofoo"})

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
- kafkaclient will automatically add messages that have processing errors to a retry/dead-letter topic if one is configured for that topic, i.e TopicConfig.FailedProcessingTopic. If you do not want this to happen, simply avoid setting this attribute in the topic config
- if there are problems with intializing the client (e.g. not being able to reach kafka), kafkaclient will continue to retry the init at intervals in a separate goroutine, so that the parent application can continue executing, e.g. in order to start a web server