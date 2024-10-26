package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

type RabbitClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	url     string
}

// newConnection establishes a new connection and channel with RabbitMQ
func newConnection(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close() // Ensure the connection is closed if channel creation fails
		return nil, nil, fmt.Errorf("failed to open channel: %v", err)
	}

	return conn, channel, nil
}

// NewRabbitClient creates a new RabbitMQ client with the given connection URL
func NewRabbitClient(connectionURL string) (*RabbitClient, error) {
	conn, channel, err := newConnection(connectionURL)
	if err != nil {
		return nil, err
	}

	return &RabbitClient{
		conn:    conn,
		channel: channel,
		url:     connectionURL,
	}, nil
}

// ConsumeMessages consumes messages from a specified exchange using a custom queue name and routing key
func (client *RabbitClient) ConsumeMessages(exchange, routingKey, queueName string) (<-chan amqp.Delivery, error) {
	err := client.channel.ExchangeDeclare(
		exchange, "direct", true, true, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	queue, err := client.channel.QueueDeclare(
		queueName, true, true, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	err = client.channel.QueueBind(queue.Name, routingKey, exchange, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue: %v", err)
	}

	// consumindo a mensagem
	msgs, err := client.channel.Consume(queue.Name, "goapp", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to consume messages: %v", err)
	}

	return msgs, nil
}

func (client *RabbitClient) PublishMessage(exchange, routingKey, queueName string, message [] byte) error{
	err := client.channel.ExchangeDeclare(
		exchange, "direct", true, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %v", err)
	}

	queue, err := client.channel.QueueDeclare(
		queueName, true, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}

	err = client.channel.QueueBind(queue.Name, routingKey, exchange, false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}

	err = client.channel.Publish(
		exchange, routingKey, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body: message,
		},
	)
	if err != nil{
		return fmt.Errorf("failed to publish messages: %v", err)
	}
	return nil
}

func (client *RabbitClient) Close() {
	client.channel.Close()
	client.conn.Close()
}
