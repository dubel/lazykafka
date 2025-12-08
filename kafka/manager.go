package kafka

import (
	"fmt"
	"log"
	"time"

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

func (m *Manager) getAdminConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 5 * time.Second
	return config
}

func (m *Manager) CreateTopic(topic string) error {
	log.Printf("Manager.CreateTopic: Starting creation of topic '%s'", topic)

	// Use a separate admin client to avoid closing the shared client
	log.Printf("Manager.CreateTopic: Creating new ClusterAdmin connection to brokers: %v", m.Client.Brokers)
	admin, err := sarama.NewClusterAdmin(m.Client.Brokers, m.getAdminConfig())
	if err != nil {
		log.Printf("Manager.CreateTopic: ERROR - Failed to create ClusterAdmin: %v", err)
		return err
	}
	defer func() {
		log.Println("Manager.CreateTopic: Closing ClusterAdmin")
		if err := admin.Close(); err != nil {
			log.Printf("Manager.CreateTopic: Warning - Failed to close ClusterAdmin: %v", err)
		}
	}()

	log.Printf("Manager.CreateTopic: Sending CreateTopic request for '%s'", topic)
	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)

	if err != nil {
		log.Printf("Manager.CreateTopic: ERROR - Failed to create topic: %v", err)
		return err
	}

	log.Printf("Manager.CreateTopic: Successfully created topic '%s'", topic)
	return nil
}

func (m *Manager) DeleteTopic(topic string) error {
	log.Printf("Manager.DeleteTopic: Starting deletion of topic '%s'", topic)

	// Use a separate admin client to avoid closing the shared client
	log.Printf("Manager.DeleteTopic: Creating new ClusterAdmin connection to brokers: %v", m.Client.Brokers)
	admin, err := sarama.NewClusterAdmin(m.Client.Brokers, m.getAdminConfig())
	if err != nil {
		log.Printf("Manager.DeleteTopic: ERROR - Failed to create ClusterAdmin: %v", err)
		return err
	}
	defer func() {
		log.Println("Manager.DeleteTopic: Closing ClusterAdmin")
		if err := admin.Close(); err != nil {
			log.Printf("Manager.DeleteTopic: Warning - Failed to close ClusterAdmin: %v", err)
		}
	}()

	log.Printf("Manager.DeleteTopic: Sending DeleteTopic request for '%s'", topic)
	err = admin.DeleteTopic(topic)

	if err != nil {
		log.Printf("Manager.DeleteTopic: ERROR - Failed to delete topic: %v", err)
		return err
	}

	log.Printf("Manager.DeleteTopic: Successfully deleted topic '%s'", topic)
	return nil
}
