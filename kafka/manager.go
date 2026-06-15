package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type Manager struct {
	Client   *Client
	Producer sarama.SyncProducer
}

// BrokerInfo holds per-broker metadata for the cluster state view.
type BrokerInfo struct {
	ID           int32
	Address      string
	IsController bool
	LeaderCount  int
	ReplicaCount int
}

// ConsumerGroupInfo holds summary info for one consumer group.
type ConsumerGroupInfo struct {
	Name      string
	State     string
	Members   int
	TotalLag  int64
	TopicLags map[string]int64
}

// TopicOffsetInfo holds per-topic offset summary.
type TopicOffsetInfo struct {
	Name           string
	Partitions     int
	TotalMessages  int64 // sum of (newest - oldest) across partitions
	NewestOffset   int64 // max newest offset across partitions
	OldestOffset   int64 // min oldest offset across partitions
}

// ClusterState is the full snapshot returned to the UI.
type ClusterState struct {
	Brokers               []BrokerInfo
	ControllerID          int32
	TotalTopics           int
	TotalPartitions       int
	UnderReplicatedCount  int
	OfflinePartitionCount int
	ConsumerGroups        []ConsumerGroupInfo
	TopicOffsets          []TopicOffsetInfo
	ClusterConfig         []ConfigEntry // key broker configs from the controller
}

// ConfigEntry holds a single broker/cluster configuration value.
type ConfigEntry struct {
	Key       string
	Value     string
	IsDefault bool
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

// ConsumeTopic consumes messages from all partitions of a topic.
// It uses context for cancellation and WaitGroup for clean goroutine shutdown.
// The function blocks until the context is cancelled and all partition consumers have stopped.
func (m *Manager) ConsumeTopic(ctx context.Context, topic string, fromBeginning bool, messages chan<- string, errors chan<- error) {
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

	var wg sync.WaitGroup

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

		wg.Add(1)
		go func(pc sarama.PartitionConsumer, partitionID int32) {
			defer wg.Done()
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					if msg != nil {
						messages <- fmt.Sprintf("[P%d] %s", msg.Partition, string(msg.Value))
					}
				case err := <-pc.Errors():
					if err != nil {
						errors <- err
					}
				case <-ctx.Done():
					log.Printf("ConsumeTopic: Partition %d consumer stopping (context cancelled)", partitionID)
					return
				}
			}
		}(pc, partition)
	}

	// Wait for context cancellation
	<-ctx.Done()
	// Wait for all partition consumers to finish cleanly
	wg.Wait()
	log.Printf("ConsumeTopic: All partition consumers stopped for topic %s", topic)
}

// GetClusterState returns a read-only snapshot of cluster health and consumer group lag.
func (m *Manager) GetClusterState() (*ClusterState, error) {
	log.Println("Manager.GetClusterState: fetching cluster metadata")

	admin, err := sarama.NewClusterAdmin(m.Client.Brokers, m.getAdminConfig())
	if err != nil {
		return nil, fmt.Errorf("cluster admin: %w", err)
	}
	defer admin.Close()

	// --- Broker metadata ---
	brokers := m.Client.BrokersList()
	controller, _ := m.Client.Controller()
	controllerID := int32(-1)
	if controller != nil {
		controllerID = controller.ID()
	}

	brokerMap := make(map[int32]*BrokerInfo, len(brokers))
	for _, b := range brokers {
		brokerMap[b.ID()] = &BrokerInfo{
			ID:           b.ID(),
			Address:      b.Addr(),
			IsController: b.ID() == controllerID,
		}
	}

	// --- Topic/partition metadata ---
	topics, err := m.Client.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	state := &ClusterState{
		ControllerID: controllerID,
		TotalTopics:  len(topics),
	}

	for _, topic := range topics {
		partitions, err := m.Client.GetPartitions(topic)
		if err != nil {
			continue
		}
		state.TotalPartitions += len(partitions)

		ti := TopicOffsetInfo{
			Name:       topic,
			Partitions: len(partitions),
		}
		firstPartition := true

		for _, p := range partitions {
			leader, err := m.Client.Leader(topic, p)
			replicas, _ := m.Client.Replicas(topic, p)
			isr, _ := m.Client.InSyncReplicas(topic, p)

			if err == nil && leader != nil {
				if bi, ok := brokerMap[leader.ID()]; ok {
					bi.LeaderCount++
				}
			} else {
				state.OfflinePartitionCount++
			}

			for _, r := range replicas {
				if bi, ok := brokerMap[r]; ok {
					bi.ReplicaCount++
				}
			}

			if len(isr) < len(replicas) {
				state.UnderReplicatedCount++
			}

			// Collect offset info for topic offsets section
			newest, errN := m.Client.GetOffset(topic, p, sarama.OffsetNewest)
			oldest, errO := m.Client.GetOffset(topic, p, sarama.OffsetOldest)
			if errN == nil && errO == nil {
				ti.TotalMessages += newest - oldest
				if newest > ti.NewestOffset {
					ti.NewestOffset = newest
				}
				if firstPartition || oldest < ti.OldestOffset {
					ti.OldestOffset = oldest
					firstPartition = false
				}
			}
		}

		state.TopicOffsets = append(state.TopicOffsets, ti)
	}

	for _, bi := range brokerMap {
		state.Brokers = append(state.Brokers, *bi)
	}

	// --- Controller / cluster config ---
	if controllerID >= 0 {
		resource := sarama.ConfigResource{
			Type: sarama.BrokerResource,
			Name: fmt.Sprintf("%d", controllerID),
		}
		// Key configs that are operationally useful
		interestingKeys := map[string]bool{
			"log.retention.hours":          true,
			"log.retention.ms":             true,
			"log.retention.bytes":          true,
			"message.max.bytes":            true,
			"replica.fetch.max.bytes":      true,
			"num.partitions":               true,
			"default.replication.factor":   true,
			"auto.create.topics.enable":    true,
			"min.insync.replicas":          true,
			"log.segment.bytes":            true,
			"log.cleanup.policy":           true,
			"compression.type":             true,
			"unclean.leader.election.enable": true,
			"max.message.bytes":            true,
			"offsets.retention.minutes":    true,
		}
		configs, err := admin.DescribeConfig(resource)
		if err != nil {
			log.Printf("GetClusterState: warning - could not describe controller config: %v", err)
		} else {
			for _, entry := range configs {
				if interestingKeys[entry.Name] {
					state.ClusterConfig = append(state.ClusterConfig, ConfigEntry{
						Key:       entry.Name,
						Value:     entry.Value,
						IsDefault: entry.Default,
					})
				}
			}
		}
	}

	// --- Consumer groups ---
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Printf("GetClusterState: warning - could not list consumer groups: %v", err)
		return state, nil
	}

	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}

	groupDescs, err := admin.DescribeConsumerGroups(groupNames)
	if err != nil {
		log.Printf("GetClusterState: warning - could not describe groups: %v", err)
		return state, nil
	}

	for _, gd := range groupDescs {
		gi := ConsumerGroupInfo{
			Name:      gd.GroupId,
			State:     gd.State,
			Members:   len(gd.Members),
			TopicLags: make(map[string]int64),
		}

		offsets, err := admin.ListConsumerGroupOffsets(gd.GroupId, nil)
		if err != nil {
			state.ConsumerGroups = append(state.ConsumerGroups, gi)
			continue
		}

		for topic, partitionOffsets := range offsets.Blocks {
			for partition, block := range partitionOffsets {
				if block.Offset < 0 {
					continue
				}
				newest, err := m.Client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					continue
				}
				lag := newest - block.Offset
				if lag < 0 {
					lag = 0
				}
				gi.TopicLags[topic] += lag
				gi.TotalLag += lag
			}
		}

		state.ConsumerGroups = append(state.ConsumerGroups, gi)
	}

	log.Printf("Manager.GetClusterState: done – %d brokers, %d topics, %d groups",
		len(state.Brokers), state.TotalTopics, len(state.ConsumerGroups))
	return state, nil
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
