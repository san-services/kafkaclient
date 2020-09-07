# kafkaclient
Golang kafka package to simplify te consumer and producer usage

## Install
`go get github.com/san-services/kafkaclient`

## Consumer
```go
func onMessageReceived(topic string, message string){
  // do stuff with message
}


// listen to test topic
client := kafkaclient.New("localhost:9092", "group1", onMessageReceived)
go client.ListenToTopic(context.Background(), "test")
```

## Producer

```go
client := kafkaclient.New("localhost:9092", "producerGroup", nil)

// produce message to test topic
err := client.ProduceToTopic("test", "this is a test message")

if err != nil {
  // handle error...
}
```
