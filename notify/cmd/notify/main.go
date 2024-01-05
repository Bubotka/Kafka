package main

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"gitlab.com/ptflp/gopubsub/kafkamq"
	"gitlab.com/ptflp/gopubsub/queue"
	"gitlab.com/ptflp/gopubsub/rabbitmq"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	fmt.Println(os.Getenv("BROKER"))
	broker, err := GetBroker()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Подключились к брокеру")

	messages, err := broker.Subscribe("limit")
	for msg := range messages {
		if string(msg.Data) == "" {
			continue
		}
		sendSMS("Усё ты попал, кончились попытки")
		sendEmail(string(msg.Data))
	}
}

func GetBroker() (queue.MessageQueuer, error) {
	var queuer queue.MessageQueuer
	var err error
	if os.Getenv("BROKER") == "rabbit" {
		conn, _ := ConnectAmqpWithRetry("amqp://guest:guest@rabbitmq:5672/")
		rabbitmq.CreateExchange(conn, "limit", "topic")
		queuer, err = rabbitmq.NewRabbitMQ(conn)
	} else {
		queuer, err = ConnectKafkaWithRetry("kafka:9093")
	}
	return queuer, err
}

func sendSMS(message string) {
	fmt.Println("Отправляем смс пользовтелю")
	response, err := http.Post("http://proxy:8080/api/sms/send", "application/json", bytes.NewBuffer([]byte(message)))
	fmt.Println("Смс отправили")
	if err != nil {
		fmt.Println(err)
	}

	data, _ := ioutil.ReadAll(response.Body)

	fmt.Println("Содержимое сообщения", string(data)+"\n")
}

func sendEmail(email string) {
	fmt.Println("Отправляем письмо пользовтелю")
	response, err := http.Post("http://proxy:8080/api/email/send", "application/json", bytes.NewBuffer([]byte(email+" превысил лимит запросов")))
	fmt.Println("Письмо отправили")
	if err != nil {
		fmt.Println(err)
	}

	data, _ := ioutil.ReadAll(response.Body)

	fmt.Println("Содержимое письма", string(data)+"\n")
}

func ConnectAmqpWithRetry(address string) (*amqp.Connection, error) {
	time.Sleep(15 * time.Second)
	for i := 0; i < 3; i++ {
		conn, err := amqp.Dial(address)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}
		return conn, nil
	}
	return nil, fmt.Errorf("не удалось подключиться к rabbit")
}

func ConnectKafkaWithRetry(address string) (queue.MessageQueuer, error) {
	time.Sleep(15 * time.Second)
	for i := 0; i < 3; i++ {
		queuer, err := kafkamq.NewKafkaMQ(address, "1")
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}
		return queuer, nil
	}
	return nil, fmt.Errorf("не удалось подключиться к kafka")
}
