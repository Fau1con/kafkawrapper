package kafkawrapper

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Cons interface {
	GetMessages(ctx context.Context) (kafka.Message, error)
}

type Prod interface {
	SendMessage(ctx context.Context, topic string, message []byte) error
}
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic string) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: "my-group",
		Topic:   topic,
	})
	return &Consumer{reader}, nil
}
func (c *Consumer) GetMessages(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		log.Printf("cant reading message from Kafka - %v\n", err)
		return kafka.Message{}, err
	}
	log.Printf("received message from Kafka: %s\n", string(msg.Value))
	return msg, nil
}

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) (*Producer, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	return &Producer{writer}, nil
}

func (p *Producer) SendMessage(ctx context.Context, topic string, message []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Value: message,
	}
	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("failed to write message in Kafka: %v\n", err)
		return err
	}
	return nil
}
