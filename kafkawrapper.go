package kafkawrapper

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kgo.Writer
	log    *slog.Logger
}
type Consumer struct {
	reader *kgo.Reader
	log    *slog.Logger
}

func NewProducer(brokers []string, topic string, log *slog.Logger) *Producer {
	return &Producer{
		writer: &kgo.Writer{
			Addr:         kgo.TCP(brokers...),
			Topic:        topic,
			RequiresAsks: kgo.RequireOne,
			Balancer:     &kgo.LeastBytes{},
			BatckTime:    10 * time.Millisecond,
		},
		log: log,
	}
}
func NewConsumer(brokers []string, group, topic string, log *slog.Logger) *Consumer {
	return &Consumer{
		reader: kgo.NewReader(kgo.ReaderConfig{
			Brokers:        brokers,
			GroupID:        group,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
		log: log,
	}
}
func (p *Producer) Close() error {
	return p.writer.Close()
}
func (c *Consumer) Close() error {
	return c.reader.Close()
}
func (p *Producer) Send(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessage(ctx, kgo.Message{Key: key, Value: value})
}
func (c *Consumer) Fetch(ctx context.Context) (kgo.Message, error) {
	return c.reader.FetchMessage(ctx)
}
func (c *Consumer) Commit(ctx context.Context, message kgo.Message) error {
	return c.reader.CommitMessage(ctx, message)
}

type CommandMessage struct {
	RequestID string            `json:request_id"`
	Path      string            `json:"path"`
	Query     map[string]string `json:"query"`
	Payload   json.RawMessage   `json:"payload"`
}
type ResponseMessage struct {
	RequestID string          `json:"request_id"`
	Status    int             `json:"status"`
	Data      json.RawMessage `json:"data"`
	Error     string          `json:"error"`
}
