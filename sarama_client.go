package kafkaclient

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
)

const (
	logPrefSarama = "DEBUG [sarama] "
)

type SaramaClient struct {
	config     *sarama.Config
	topicConf  map[string]TopicConfig
	topicNames []string
	brokers    []string
	groupID    string
	group      sarama.ConsumerGroup
	ready      chan bool
	cancel     context.CancelFunc
	ctx        context.Context
}

func newSaramaClient(conf Config) (*SaramaClient, error) {
	if conf.Debug {
		sarama.Logger = log.New(os.Stdout, logPrefSarama, log.LstdFlags)
	}

	return &SaramaClient{}, nil
}

// StartConsume starts consuming kafka topic messages
func (c *SaramaClient) StartConsume() (e error) {
	lg := logger.New(c.ctx, "")

	c.group, e = sarama.NewConsumerGroup(
		c.brokers, c.groupID, c.config)

	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop,
			// when a server-side rebalance happens, the consumer
			// session will need to be recreated to get the new claims
			if e := (c.group).Consume(c.ctx, c.topicNames, c); e != nil {
				lg.Fatal(logger.LogCatUncategorized, errConsumer(e))
			}

			// check if context was cancelled, signaling
			// that the consumer should stop
			if c.ctx.Err() != nil {
				return
			}

			c.ready = make(chan bool)
		}
	}()

	<-c.ready // await consumer set up
	lg.Info(logger.LogCatUncategorized, infoConsumerReady)

	select {
	case <-c.ctx.Done():
		lg.Info(logger.LogCatUncategorized,
			infoConsumerTerm("context cancelled"))
	}

	c.CancelConsume()
	wg.Wait()

	if e := c.Close(); e != nil {
		lg.Fatal(logger.LogCatUncategorized, errConsumerClose(e))
	}

	return
}

// CancelConsume call the context's context.cancelFunc in order to stop the
// process of message consumption
func (c *SaramaClient) CancelConsume() (e error) {
	c.cancel()
	return nil
}

// ConsumeClaim read ConsumerGroupClaim's Messages() in a loop.
// It handles consuming and processing or delegating prosessing of topic messages.
// This method is called within a goroutine:
// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
func (c *SaramaClient) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) (e error) {

	lg := logger.New(c.ctx, "")

	for {
		select {
		case msg := <-claim.Messages():
			lg.Infof(logger.LogCatUncategorized,
				infoMsgClaimed(msg.Topic, msg.Partition, msg.Offset))

			conf := c.topicConf[msg.Topic]

			e = conf.MessageProcessor(session.Context(), newSaramaMessage(msg, conf.MessageEncoderDecoder))
			if e != nil {
				lg.Error(logger.LogCatUncategorized, e)
				continue
			}

			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new consumer session, before ConsumeClaim
func (c *SaramaClient) Setup(sarama.ConsumerGroupSession) (e error) {
	// mark the consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *SaramaClient) Cleanup(sarama.ConsumerGroupSession) (e error) {
	return nil
}

// Close stops the ConsumerGroup and detaches any running sessions
func (c *SaramaClient) Close() (e error) {
	lg := logger.New(c.ctx, "")

	e = c.group.Close()
	if e != nil {
		lg.Error(logger.LogCatUncategorized, errConsumerClose(e))
	}

	return
}

// ProduceMessage creates/encodes a message and sends it to the specified topic
func (c *SaramaClient) ProduceMessage(ctx context.Context, topic string, key string, msg interface{}) (e error) {
	return
}
