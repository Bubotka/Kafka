package queue

type Message struct {
	Data     []byte
	Err      error
	Identity any // Тег доставки для RabbitMQ, смещение для Kafka
	Topic    string
}

type MessageQueuer interface {
	Publish(topic string, message []byte) error
	Subscribe(topic string) (<-chan Message, error)
	Ack(msg *Message) error
	Close() error
}
