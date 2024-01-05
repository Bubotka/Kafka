package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"gitlab.com/ptflp/gopubsub/queue"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewRabbitMQ(conn *amqp.Connection) (queue.MessageQueuer, error) {
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &RabbitMQ{conn: conn, channel: ch}, nil
}

func (r *RabbitMQ) Publish(topic string, message []byte) error {
	err := r.channel.Publish(
		topic, // exchange
		"",    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
	return err
}

func (r *RabbitMQ) Subscribe(topic string) (<-chan queue.Message, error) {
	q, err := r.channel.QueueDeclare(
		topic, // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	if err = r.channel.QueueBind(
		q.Name, // queue name
		"",     // routing key (если вы хотите использовать имя очереди как ключ маршрутизации, замените это на q.Name)
		topic,  // exchange
		false,
		nil,
	); err != nil {
		return nil, err
	}

	msgs, err := r.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack изменено на false
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	out := make(chan queue.Message)

	go func() {
		for d := range msgs {
			out <- queue.Message{
				Topic:    topic,
				Data:     d.Body,
				Err:      nil,
				Identity: d.DeliveryTag,
			}
		}
		close(out)
	}()

	return out, nil
}

func (r *RabbitMQ) Ack(msg *queue.Message) error {
	if v, ok := msg.Identity.(uint64); ok {
		return r.channel.Ack(v, false)
	}

	return fmt.Errorf("invalid identity type: %T", msg.Identity)
}

func (r *RabbitMQ) Close() error {
	if err := r.channel.Close(); err != nil {
		return err
	}
	return r.conn.Close()
}

func CreateExchange(conn *amqp.Connection, exchangeName, exchangeType string) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	return channel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type, например "direct", "fanout", "topic", "headers"
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
}
