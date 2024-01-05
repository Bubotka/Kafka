package kafkamq

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gitlab.com/ptflp/gopubsub/queue"
	"log"
)

type MQ struct {
	Producer  *kafka.Producer
	Consumer  *kafka.Consumer
	topicsOut map[string]chan queue.Message
	topics    []string
}

type PartitionOffset struct {
	Partition int32
	Offset    kafka.Offset
	Topic     *string
}

func NewKafkaMQ(broker, groupID string) (queue.MessageQueuer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		return nil, err
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    broker,
		"auto.offset.reset":    "earliest",
		"group.id":             groupID,
		"max.poll.interval.ms": 12000,
		"session.timeout.ms":   12000,
	})
	if err != nil {
		return nil, err
	}

	topicsOut := make(map[string]chan queue.Message)

	go func() {
		for {
			var msg *kafka.Message
			msg, err = c.ReadMessage(-1)
			if err != nil {
				for _, ch := range topicsOut {
					ch <- queue.Message{Err: err}
				}
				log.Println(err)
				continue
			}
			topicsOut[*msg.TopicPartition.Topic] <- queue.Message{
				Topic: *msg.TopicPartition.Topic,
				Data:  msg.Value,
				// В Kafka offset может быть использован вместо DeliveryTag в RabbitMQ
				Identity: msg.TopicPartition,
			}
		}
	}()

	return &MQ{
		Producer:  p,
		Consumer:  c,
		topicsOut: topicsOut,
	}, nil
}

func (k *MQ) Publish(topic string, message []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}
	return k.Producer.Produce(msg, nil)
}

func (k *MQ) Subscribe(topic string) (<-chan queue.Message, error) {
	k.topics = append(k.topics, topic)
	err := k.Consumer.SubscribeTopics(k.topics, nil)
	if err != nil {
		return nil, err
	}
	out := make(chan queue.Message)
	k.topicsOut[topic] = out

	return out, nil
}

func (k *MQ) Ack(msg *queue.Message) error {
	var err error
	if v, ok := msg.Identity.(kafka.TopicPartition); ok {
		_, err = k.Consumer.CommitOffsets([]kafka.TopicPartition{v})
		return err
	}

	return fmt.Errorf("invalid identity type %T", msg.Identity)
}

func (k *MQ) Close() error {
	k.Producer.Close()
	return k.Consumer.Close()
}
