# kafkaclient

An overly-opinionated library to simplify interactions with existing go/kafka libraries.

## Supported base libraries

- shopify/sarama (implemented)
- segmentio/kafka-go (planned)

## Use

```
func main()
    topics := []kafkaclient.TopicConfig{
		kafkaclient.NewTopicConfig(
			"my_topic", 
            kafkaclient.MessageFormatAvro,
			5, 
            "my_topic_retry",
			"", 
            0, 
            processTestTopic)}

	config, e := kafkaclient.NewConfig(ctx, 
        "2.5.0", 
        []string{"127.0.0.1"}, 
        topics, 
        "", 
        kafkaclient.ConsumerTypeGroup,
		"test_consumer", 
        kafkaclient.ProducerTypeAsync, 
        true, 
        &tls.Config{}, 
        true)

	if e != nil {
		t.Error(e)
	}

	kc, e := kafkaclient.New(BaseSarama, config)
	if e != nil {
		t.Error(e)
		return
	}

	e = kc.StartConsume(ctx)
	if e != nil {
		t.Error(e)
	}
}

func processTestTopic(ctx context.Context, msg kafkaclient.ConsumerMessage) (e error) {
	return
}
```