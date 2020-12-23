# kafkaclient

An overly-opinionated library to simplify interactions with existing go/kafka libraries.

## Supported base libraries

- shopify/sarama (implemented)
- segmentio/kafka-go (planned)

## Use

```
func main()
	ctx := context.Background()

    topics := []kafkaclient.TopicConfig{
		{
			Name:                  "test_topic",
			DoRead:                true,
			MessageFormat:         kafkaclient.MessageFormatAvro,
			DelayProcessingMins:   0,
			FailedProcessingTopic: "test_topic_dlq",
			MessageProcessor:      processTestTopic,
		},
		{
			Name:                  "test_topic_retry",
			DoRead:                true,
			DoWrite:			   true,
			MessageFormat:         kafkaclient.MessageFormatAvro,
			DelayProcessingMins:   15,
			FailedProcessingTopic: "test_topic_dlq",
			MessageProcessor:      processTestTopic,
		},
		{
			Name:                  "test_topic_dlq",
			DoWrite:			   true,
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
        kafkaclient.ProducerTypeAsync, 
        true, 
        nil, 
        true)

	if e != nil {
		t.Error(e)
	}

	kc, e := kafkaclient.New(BaseSarama, config)
	if e != nil {
		t.Error(e)
		return
	}

	kc.StartConsume(ctx)
}

func processTestTopic(ctx context.Context, msg kafkaclient.ConsumerMessage) (e error) {
	return
}
```