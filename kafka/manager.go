package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

type Manager struct {
	Client   *Client
	Producer sarama.SyncProducer
}

func NewManager(brokers []string) (*Manager, error) {
	client, err := NewClient(brokers)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client.saramaClient)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &Manager{
		Client:   client,
		Producer: producer,
	}, nil
}

func (m *Manager) Close() {
	if m.Producer != nil {
		m.Producer.Close()
	}
	if m.Client != nil {
		m.Client.Close()
	}
}

func (m *Manager) ProduceMessage(topic, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}
	_, _, err := m.Producer.SendMessage(msg)
	return err
}

func (m *Manager) ConsumeTopic(topic string, fromBeginning bool, messages chan<- string, errors chan<- error, stop <-chan struct{}) {
	consumer, err := sarama.NewConsumerFromClient(m.Client.saramaClient)
	if err != nil {
		errors <- err
		return
	}
	defer consumer.Close()

	partitions, err := m.Client.GetPartitions(topic)
	if err != nil {
		errors <- err
		return
	}

	// For simplicity, we'll just consume from all partitions
	// In a real app, we might want to handle this more gracefully
	for _, partition := range partitions {
		offset := sarama.OffsetNewest
		if fromBeginning {
			offset = sarama.OffsetOldest
		}

		pc, err := consumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			errors <- fmt.Errorf("failed to start consumer for partition %d: %w", partition, err)
			continue
		}

		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					messages <- fmt.Sprintf("[%d] %s", msg.Partition, string(msg.Value))
				case err := <-pc.Errors():
					errors <- err
				case <-stop:
					return
				}
			}
		}(pc)
	}

	<-stop
}
