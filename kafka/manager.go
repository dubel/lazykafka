package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

// ErrManagerClosed is returned by operations started after Close.
var ErrManagerClosed = errors.New("kafka connection closed")

// Manager owns one broker connection. It is safe for concurrent use and may be
// closed while operations are in flight: Close returns immediately, new
// operations fail with ErrManagerClosed, and the underlying clients are closed
// once the in-flight operations have finished. Callers therefore never need to
// hold a lock across network calls to keep the manager alive.
type Manager struct {
	Client   *Client
	Producer sarama.SyncProducer

	mu       sync.Mutex
	closed   bool
	inflight sync.WaitGroup
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

// acquire registers an in-flight operation. It returns false once the manager
// is closed; on true the caller must call release when done.
func (m *Manager) acquire() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.inflight.Add(1)
	return true
}

func (m *Manager) release() {
	m.inflight.Done()
}

// Close marks the manager closed and returns without waiting. Clients are
// closed in the background after in-flight operations finish (they may run
// until their network timeout when the broker is unreachable).
func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.mu.Unlock()

	go func() {
		m.inflight.Wait()
		if m.Producer != nil {
			m.Producer.Close()
		}
		if m.Client != nil {
			m.Client.Close()
		}
	}()
}

// ListTopics returns topic names, refreshing cluster metadata first when refresh is set.
// A failed refresh is logged and the cached metadata is used.
func (m *Manager) ListTopics(refresh bool) ([]string, error) {
	if !m.acquire() {
		return nil, ErrManagerClosed
	}
	defer m.release()
	if refresh {
		if err := m.Client.RefreshMetadata(); err != nil {
			log.Printf("Manager.ListTopics: warning - failed to refresh metadata: %v", err)
		}
	}
	return m.Client.ListTopics()
}

// CheckConnection verifies the broker is still reachable.
func (m *Manager) CheckConnection() error {
	if !m.acquire() {
		return ErrManagerClosed
	}
	defer m.release()
	return m.Client.CheckConnection()
}

func (m *Manager) ProduceMessage(topic, value string) error {
	if !m.acquire() {
		return ErrManagerClosed
	}
	defer m.release()
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
	// Sends must never block past cancellation: the reader stops reading once
	// ctx is done, and a blocked send would leak this goroutine and the consumer.
	sendErr := func(err error) bool {
		select {
		case errors <- err:
			return true
		case <-ctx.Done():
			return false
		}
	}

	// The consumer gets its own client (and broker connections). Fetch requests
	// long-poll on the broker, and Kafka answers requests on one connection in
	// order, so sharing the main client would stall metadata, offset and
	// health-check requests behind every fetch.
	// Only the partition lookup uses the shared client; the consumer below has
	// its own, so a long-running consumption does not delay Close.
	if !m.acquire() {
		sendErr(ErrManagerClosed)
		return
	}
	partitions, err := m.Client.GetPartitions(topic)
	m.release()
	if err != nil {
		sendErr(err)
		return
	}

	consumer, err := sarama.NewConsumer(m.Client.Brokers, newSaramaConfig(m.Client.Brokers))
	if err != nil {
		sendErr(err)
		return
	}
	defer consumer.Close()

	var wg sync.WaitGroup

	for _, partition := range partitions {
		offset := sarama.OffsetNewest
		if fromBeginning {
			offset = sarama.OffsetOldest
		}

		pc, err := consumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			sendErr(fmt.Errorf("failed to start consumer for partition %d: %w", partition, err))
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
						select {
						case messages <- fmt.Sprintf("[P%d] %s", msg.Partition, string(msg.Value)):
						case <-ctx.Done():
						}
					}
				case err := <-pc.Errors():
					if err != nil {
						sendErr(err)
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
	if !m.acquire() {
		return nil, ErrManagerClosed
	}
	defer m.release()

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

	// Partition layout comes from the client's cached metadata (no network).
	topicPartitions := make(map[string][]int32, len(topics))
	byLeader := make(map[int32][]topicPartition)
	leaders := make(map[int32]*sarama.Broker)
	for _, t := range topics {
		partitions, err := m.Client.GetPartitions(t)
		if err != nil {
			continue
		}
		topicPartitions[t] = partitions
		state.TotalPartitions += len(partitions)

		for _, p := range partitions {
			leader, err := m.Client.Leader(t, p)
			replicas, _ := m.Client.Replicas(t, p)
			isr, _ := m.Client.InSyncReplicas(t, p)

			if err == nil && leader != nil {
				if bi, ok := brokerMap[leader.ID()]; ok {
					bi.LeaderCount++
				}
				leaders[leader.ID()] = leader
				byLeader[leader.ID()] = append(byLeader[leader.ID()], topicPartition{t, p})
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
		}
	}

	// Offsets: one batched ListOffsets request per leader and direction instead
	// of two round-trips per partition.
	newestOffsets := make(map[topicPartition]int64)
	oldestOffsets := make(map[topicPartition]int64)
	var wg sync.WaitGroup
	var offsetsMu sync.Mutex
	for id, parts := range byLeader {
		wg.Add(1)
		go func(b *sarama.Broker, parts []topicPartition) {
			defer wg.Done()
			newest := fetchOffsets(b, parts, sarama.OffsetNewest)
			oldest := fetchOffsets(b, parts, sarama.OffsetOldest)
			offsetsMu.Lock()
			for tp, o := range newest {
				newestOffsets[tp] = o
			}
			for tp, o := range oldest {
				oldestOffsets[tp] = o
			}
			offsetsMu.Unlock()
		}(leaders[id], parts)
	}
	wg.Wait()

	for _, t := range topics {
		partitions, ok := topicPartitions[t]
		if !ok {
			continue
		}
		ti := TopicOffsetInfo{
			Name:       t,
			Partitions: len(partitions),
		}
		firstPartition := true
		for _, p := range partitions {
			newest, okN := newestOffsets[topicPartition{t, p}]
			oldest, okO := oldestOffsets[topicPartition{t, p}]
			if !okN || !okO {
				continue
			}
			ti.TotalMessages += newest - oldest
			if newest > ti.NewestOffset {
				ti.NewestOffset = newest
			}
			if firstPartition || oldest < ti.OldestOffset {
				ti.OldestOffset = oldest
				firstPartition = false
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

	var cgWg sync.WaitGroup
	var cgMu sync.Mutex

	for _, gd := range groupDescs {
		cgWg.Add(1)
		go func(gDesc *sarama.GroupDescription) {
			defer cgWg.Done()
			gi := ConsumerGroupInfo{
				Name:      gDesc.GroupId,
				State:     gDesc.State,
				Members:   len(gDesc.Members),
				TopicLags: make(map[string]int64),
			}

			offsets, err := admin.ListConsumerGroupOffsets(gDesc.GroupId, nil)
			if err != nil {
				cgMu.Lock()
				state.ConsumerGroups = append(state.ConsumerGroups, gi)
				cgMu.Unlock()
				return
			}

			for topic, partitionOffsets := range offsets.Blocks {
				for partition, block := range partitionOffsets {
					if block.Offset < 0 {
						continue
					}

					newest, ok := newestOffsets[topicPartition{topic, partition}]
					if !ok {
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

			cgMu.Lock()
			state.ConsumerGroups = append(state.ConsumerGroups, gi)
			cgMu.Unlock()
		}(gd)
	}

	cgWg.Wait()

	log.Printf("Manager.GetClusterState: done – %d brokers, %d topics, %d groups",
		len(state.Brokers), state.TotalTopics, len(state.ConsumerGroups))
	return state, nil
}

func (m *Manager) getAdminConfig() *sarama.Config {
	return newSaramaConfig(m.Client.Brokers)
}

type topicPartition struct {
	topic     string
	partition int32
}

// fetchOffsets asks one leader for the newest or oldest offset of all given
// partitions in a single ListOffsets request. Partitions with errors are omitted.
func fetchOffsets(b *sarama.Broker, parts []topicPartition, at int64) map[topicPartition]int64 {
	req := &sarama.OffsetRequest{Version: 1}
	for _, tp := range parts {
		req.AddBlock(tp.topic, tp.partition, at, 1)
	}
	out := make(map[topicPartition]int64, len(parts))
	resp, err := b.GetAvailableOffsets(req)
	if err != nil {
		log.Printf("GetClusterState: warning - list offsets on broker %d failed: %v", b.ID(), err)
		return out
	}
	for _, tp := range parts {
		block := resp.GetBlock(tp.topic, tp.partition)
		if block == nil || block.Err != sarama.ErrNoError {
			continue
		}
		out[tp] = block.Offset
	}
	return out
}

func (m *Manager) CreateTopic(topic string) error {
	if !m.acquire() {
		return ErrManagerClosed
	}
	defer m.release()

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
	if !m.acquire() {
		return ErrManagerClosed
	}
	defer m.release()

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

func (m *Manager) DeleteConsumerGroup(group string) error {
	if !m.acquire() {
		return ErrManagerClosed
	}
	defer m.release()

	log.Printf("Manager.DeleteConsumerGroup: Starting deletion of group '%s'", group)

	// Use a separate admin client to avoid closing the shared client
	log.Printf("Manager.DeleteConsumerGroup: Creating new ClusterAdmin connection to brokers: %v", m.Client.Brokers)
	admin, err := sarama.NewClusterAdmin(m.Client.Brokers, m.getAdminConfig())
	if err != nil {
		log.Printf("Manager.DeleteConsumerGroup: ERROR - Failed to create ClusterAdmin: %v", err)
		return err
	}
	defer func() {
		log.Println("Manager.DeleteConsumerGroup: Closing ClusterAdmin")
		if err := admin.Close(); err != nil {
			log.Printf("Manager.DeleteConsumerGroup: Warning - Failed to close ClusterAdmin: %v", err)
		}
	}()

	log.Printf("Manager.DeleteConsumerGroup: Sending DeleteConsumerGroup request for '%s'", group)
	err = admin.DeleteConsumerGroup(group)

	if err != nil {
		log.Printf("Manager.DeleteConsumerGroup: ERROR - Failed to delete consumer group: %v", err)
		return err
	}

	log.Printf("Manager.DeleteConsumerGroup: Successfully deleted consumer group '%s'", group)
	return nil
}
