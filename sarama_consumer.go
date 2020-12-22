package kafkaclient

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	logger "github.com/disturb16/apilogger"
)

// must implement sarama.ConsumerGroupHandler
type saramaConsumer struct {
	groupID      string
	group        sarama.ConsumerGroup
	config       *sarama.Config
	topicConf    map[string]TopicConfig
	topicNames   []string
	brokers      []string
	ready        chan bool
	failMessages chan failedMessage
	cancel       context.CancelFunc
	ctx          context.Context
}

func newSaramaConsumer(saramaConf *sarama.Config,
	groupID string, topicConf map[string]TopicConfig, topicNames []string,
	brokers []string) (c saramaConsumer, e error) {

	consumerCtx, cancel := context.WithCancel(context.Background())

	return saramaConsumer{
		groupID:    groupID,
		config:     saramaConf,
		topicConf:  topicConf,
		topicNames: topicNames,
		brokers:    brokers,
		cancel:     cancel,
		ctx:        consumerCtx}, nil
}

func (c *saramaConsumer) initConsumerGroup() (e error) {
	lg := logger.New(c.ctx, "")

	c.group, e = sarama.NewConsumerGroup(
		c.brokers, c.groupID, c.config)

	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
}

func (c *saramaConsumer) startConsume() (e error) {
	lg := logger.New(c.ctx, "")

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

	c.cancel()
	wg.Wait()

	if e := c.close(); e != nil {
		lg.Fatal(logger.LogCatUncategorized, errConsumerClose(e))
	}

	return
}

// ConsumeClaim read ConsumerGroupClaim's Messages() in a loop.
// Method in sarama.ConsumerGroupHandler interface.
//
// Handles consuming and processing or delegating prosessing of topic messages.
// This method is called within a goroutine:
// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
func (c *saramaConsumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) (e error) {

	lg := logger.New(c.ctx, "")

	for {
		select {
		case msg := <-claim.Messages():
			conf := c.topicConf[msg.Topic]

			if conf.Name == "" || conf.MessageEncoderDecoder == nil {
				e = errTopicConfMissing
				lg.Error(logger.LogCatUncategorized, e)
				continue
			}

			m := newSaramaMessage(msg, conf.MessageEncoderDecoder)
			lg.Infof(logger.LogCatUncategorized,
				infoEvent("message claimed", msg.Topic, msg.Partition, msg.Offset))

			e = conf.MessageProcessor(session.Context(), m)
			if e != nil {
				lg.Error(logger.LogCatUncategorized, e)

				if conf.FailedProcessingTopic != "" {
					c.failMessages <- newFailedMessage(m, conf.FailedProcessingTopic, e)
				}
				continue
			}

			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new consumer session, before ConsumeClaim.
// Method in sarama.ConsumerGroupHandler interface.
func (c *saramaConsumer) Setup(sarama.ConsumerGroupSession) (e error) {
	// mark the consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
// Method in sarama.ConsumerGroupHandler interface.
func (c *saramaConsumer) Cleanup(sarama.ConsumerGroupSession) (e error) {
	return nil
}

// Close stops the ConsumerGroup and detaches any running sessions
func (c *saramaConsumer) close() (e error) {
	lg := logger.New(c.ctx, "")

	e = c.group.Close()
	if e != nil {
		lg.Error(logger.LogCatUncategorized, errConsumerClose(e))
	}

	return
}
