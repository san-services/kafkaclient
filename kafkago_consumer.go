package kafkaclient

import (
	"context"

	logger "github.com/disturb16/apilogger"
	"github.com/segmentio/kafka-go"
)

type kafkagoConsumer struct {
	gen          *kafka.Generation
	groupID      string
	brokers      []string
	topicNames   []string
	topicConfig  map[string]TopicConfig
	failMessages chan failedMessage
}

func newKafkagoConsumer(groupID string, brokers []string,
	topicNames []string, topicConf map[string]TopicConfig) kafkagoConsumer {

	return kafkagoConsumer{
		groupID:     groupID,
		brokers:     brokers,
		topicNames:  topicNames,
		topicConfig: topicConf}
}

func (c *kafkagoConsumer) startConsume(ctx context.Context) (e error) {
	lg := logger.New(ctx, "")

	group, e := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:          c.groupID,
		Brokers:     c.brokers,
		Topics:      c.topicNames,
		StartOffset: kafka.LastOffset})

	for {
		// get the next (latest) generation of a consumer group - every time
		// a member (service instance in our case) enters or exits the group,
		// a new generation occurs, which results in a new *kafka.Generation instance
		c.gen, e = group.Next(context.TODO())
		if e != nil {
			lg.Error(logger.LogCatUncategorized, e)
			break
		}

		for _, t := range c.topicNames {
			go c.consumeTopic(ctx, t)
		}
	}

	return
}

func (c *kafkagoConsumer) consumeTopic(ctx context.Context, topic string) {
	lg := logger.New(ctx, "")

	conf := c.topicConfig[topic]

	// get this consumer's partition assignments by topic
	assignments := c.gen.Assignments[topic]

	// loop through each partition assigned, find the offset, and
	// start the work or processing that should be done (gen.start func param)
	// for each message
	for _, assignment := range assignments {
		partition, offset := assignment.ID, assignment.Offset

		c.gen.Start(func(ctx context.Context) {
			// create reader for this topic/partition.
			reader := c.reader(topic, partition)
			defer reader.Close()

			// tell the reader where to read from -
			// the last committed offset for this partition
			reader.SetOffset(offset)

			// read messages
			for {
				msg, e := reader.ReadMessage(ctx)
				if e != nil {
					lg.Error(logger.LogCatUncategorized, errMsgRead(e))
					return
				}
				offset = msg.Offset

				m := newKafkaGoMessage(msg, conf.MessageEncoderDecoder)
				e = conf.MessageProcessor(ctx, m)

				if e != nil {
					// Failed message processing
					if conf.FailedProcessingTopic != "" {
						c.failMessages <- newFailedMessage(m, conf.FailedProcessingTopic, e)
					}

					e = reader.SetOffset(offset)
					if e != nil {
						lg.Error(logger.LogCatUncategorized, errSetOffset(e))
					}

					c.commitOffset(ctx, topic, partition, offset-1)
				}

				// Successful message processing
				c.commitOffset(ctx, topic, partition, offset)
			}
		})
	}
}

func (c *kafkagoConsumer) commitOffset(
	ctx context.Context, topic string, partition int, offset int64) {

	lg := logger.New(ctx, "")

	e := c.gen.CommitOffsets(
		map[string]map[int]int64{topic: {partition: offset}})

	if e != nil {
		lg.Error(logger.LogCatUncategorized, errCommit(e))

		// retry
		e := c.gen.CommitOffsets(
			map[string]map[int]int64{topic: {partition: offset}})

		if e != nil {
			lg.Error(logger.LogCatUncategorized, errCommit(e))
		}
	}
}

func (c *kafkagoConsumer) reader(topic string, partition int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   c.brokers,
		Topic:     topic,
		Partition: partition})
}
