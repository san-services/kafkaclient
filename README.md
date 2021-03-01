# kafkaclient

An overly-opinionated library to simplify interactions with existing go/kafka libraries.

## Supported base libraries

- shopify/sarama 
- segmentio/kafka-go 

## Basic Use

```
   go get github.com/san-services/kafkaclient/v2
```

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
            // Schema:                myCustomSchema,    <-- see Avro docs below on configuring custom schemas
            // SchemaVersion:         1                                                 
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

    // you can (optionally) configure a struct to inject important
    // data / data structures into your message processors, e.g. in 
    // order to access service-layer functionality
    pd := processorDependencies{
        service: mydomain.NewService()}

    tls := getTLSConfig()

    conf := kafkaclient.Config{
		KafkaVersion:     "2.5.0",
		Brokers:          []string{"192.15.16.1:9093", "192.15.16.2:9093"},
		Topics:           topics,
		SchemaRegURL:     "https://192.15.16.1:8181",
		ConsumerType:     kafkaclient.ConsumerTypeGroup,
		ConsumerGroupID:  "my-service",
		ProcDependencies: pd,
		ProducerType:     kafkaclient.ProducerTypeAsync,
		ReadFromOldest:   false,
		TLS:              tls,
		Debug:            true}

    kc, e := kafkaclient.New(kafkaclient.BaseSarama, config)
    if e != nil {
        log.Println(e)
        return
    }

    kc.StartConsume(ctx)

    e := kc.ProduceMessage(ctx, TestTopic, 
	    "message_key_238to2efgb", testTopicAvroMessage{ID: 1, Name: "foofoo"})

    if e != nil {
        log.Println(e)
    }
}

type processorDependencies struct {
    service mydomain.Service
}

func processTestTopic(ctx context.Context, 
    dependencies kafkaclient.ProcessorDependencies, 
    msg kafkaclient.ConsumerMessage) (e error) {

    d, ok := dependencies.(processorDependencies)
    if !ok {
        e = errors.New("kafka processor incorrectly configured")
        log.Println(e)
        return
    } 
    
    if d == nil {
    	e = errors.New("kafka processor dependencies should not be nil")
        log.Println(e)
        return
    }

    data := testTopicAvroMessage{}
    e = msg.Unmarshall(ctx, &data)
    if e != nil {
        log.Println(e)
        return
    }
    
    e = d.service.Save(ctx, data.ID, data.Name)
    if e != nil {
        log.Println(e)
    }

    return
}

type testTopicAvroMessage struct {
	ID    int64   `avro:"id"`
	Name  string  `avro:"name"`
}
```

### Notes

- topic consumption starts in its own goroutine, so calling StartConsume is non-blocking
- kafkaclient will automatically add messages that have processing errors to a retry/dead-letter topic if one is configured for that topic, i.e TopicConfig.FailedProcessingTopic. If you do not want this to happen, simply avoid setting this attribute in the topic config
- if there are problems with intializing the client (e.g. not being able to reach kafka), kafkaclient will continue to retry the init at intervals in a separate goroutine, so that the parent application can continue executing, e.g. in order to start a web server

## Avro

Often topic messages will be stored in avro format. kafkaclient uses linkedin/go-avro to help with encoding and decoding them using their avro schemas. Schemas are fetched from, or can be added to, a confluent schema registry. 

kafkaclient augments the capabilities of the go-avro library, by enabling the conversion of binary avro messages to golang structs and vice versa. 
For this to work, struct fields need to contain tags referencing schema record field names. See below:

Avro schema:
```json
{
    "type": "record",
    "name": "TestTopicRecord",
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
        },  
    ]
}
```
Matching struct:
```go
type Thing struct {
    ID    int64  `avro:"ID"`
    Name  string `avro:"NAME"`
}
```

### Nested (complex) messages

For encoding/decoding messages including nested messages (as might be the case with retry topics, which could include the error message as well as the original message binary for which processing has failed), an additional tag is necessary for finding the schema needed to decode/encode the nested message. See below:

Avro schema:
```json
{
    "type": "record",
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
            "name": "ORIGINAL_MESSAGE",
            "type": [
                "null",
                "bytes"
            ],
            "default": null
        },
    ]
}
```
Matching struct:
```go
type ThingRetry struct {
    ErrorMessage     string `avro:"ERROR_MESSAGE"`
    OriginalMessage  Thing  `avro:"ORIGINAL_MESSAGE" topic:"thing_retry_1"`
}
```	
	 
## Producing a new avro message with a custom schema

Scenario: you've just created a brand new test topic and would like to send an avro message to it without having to configure the schema on the schema registry itself.

1. Create a new avro schema to represent your message and store it somewhere in your codebase - you will need to be able to retrieve it in string form, e.g.:

```go
const (
    MyCustomSchema = `
    {
		"type": "record",
		"name": "MyCustomSchema",
		"fields": [
			{
				"name": "greeting",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "name",
				"type": [
					"null",
					"string"
				],
				"default": null
			}
		]
	}`
)
```

2. Make sure you have a go struct representing your custom schema - this will help you build a message to send, e.g.:

```go
type testMessage struct {
    Greeting string `avro:"greeting"`
    Name     string `avro:"name"`
}
```

3. Specify the schema and the schema version in your TopicConfig. The version will be 1 if you have never used or changed this schema for this topic before. Every time you want to change the schema after it has initally been used, you must increment the version number so that the newer version will be saved on the schema registry, e.g.:

```go
testTopicName := "my-test-topic"

topics := []kafkaclient.TopicConfig{
        {
            Name:                  testTopicName,
            DoProduce:             true,
            MessageFormat:         kafkaclient.MessageFormatAvro,
            DelayProcessingMins:   0,
            Schema:                MyCustomSchema,
            SchemaVersion:         1
            MessageProcessor:      processTestTopic,
        },
        ...
```

4. Produce a message to this topic, e.g.:

```go
    kc, e := kafkaclient.New(kafkaclient.BaseSarama, config)
    if e != nil {
        log.Println(e)
        return
    }

    e := kc.ProduceMessage(ctx, testTopicName, 
	    "test-message-1", testMessage{Greeting: "hello", Name: "world"})

    if e != nil {
        log.Println(e)
    }
```

Notes:
- you can then obviously consume from this topic using the same golang struct to unmarshall the message into (make sure `DoConsume` is set to `true` in the TopcConfig entry)