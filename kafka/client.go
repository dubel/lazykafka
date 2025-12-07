package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

type Client struct {
	saramaClient sarama.Client
	Brokers      []string
}

func NewClient(brokers []string) (*Client, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0 // Assume a relatively recent version
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Net.DialTimeout = 5 * time.Second

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Client{
		saramaClient: client,
		Brokers:      brokers,
	}, nil
}

func (c *Client) Close() error {
	if c.saramaClient != nil {
		return c.saramaClient.Close()
	}
	return nil
}

func (c *Client) ListTopics() ([]string, error) {
	return c.saramaClient.Topics()
}

func (c *Client) GetPartitions(topic string) ([]int32, error) {
	return c.saramaClient.Partitions(topic)
}
