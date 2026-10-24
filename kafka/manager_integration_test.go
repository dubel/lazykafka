//go:build integration
// +build integration

package kafka_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dubel/lazykafka/kafka"
)

// These tests require Docker Kafka to be running:
//   docker compose up -d
//
// Run with:
//   go test -tags=integration -v ./kafka/...

const testBroker = "localhost:19092"

func skipIfNoKafka(t *testing.T) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = testBroker
	}

	// Quick check if Kafka is reachable
	client, err := kafka.NewClient([]string{broker})
	if err != nil {
		t.Skipf("Skipping integration test: Kafka not available at %s: %v", broker, err)
	}
	client.Close()
}

func getTestBroker() string {
	if broker := os.Getenv("KAFKA_BROKER"); broker != "" {
		return broker
	}
	return testBroker
}

func TestManager_ProduceAndConsume(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	topic := "test-integration-" + time.Now().Format("20060102150405")
	testMessage := "Hello from integration test!"

	// Produce a message
	err = manager.ProduceMessage(topic, testMessage)
	if err != nil {
		t.Fatalf("Failed to produce message: %v", err)
	}
	t.Logf("Produced message to topic %s", topic)

	// Consume the message
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messages := make(chan string, 10)
	errors := make(chan error, 10)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.ConsumeTopic(ctx, topic, true, messages, errors)
	}()

	// Wait for message or timeout
	select {
	case msg := <-messages:
		t.Logf("Received message: %s", msg)
		if msg == "" {
			t.Error("Received empty message")
		}
		// Message format is now "[P<partition>] <value>"
		// Just verify we got something
	case err := <-errors:
		t.Fatalf("Consumer error: %v", err)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for message")
	}

	cancel()
	wg.Wait()
}

func TestManager_ConsumeMultiplePartitions(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Use the pre-created test-partitions topic (has 3 partitions)
	topic := "test-partitions"

	// Produce messages with different keys to hit different partitions
	for i := 0; i < 5; i++ {
		err = manager.ProduceMessage(topic, "test message "+time.Now().Format("15:04:05.000"))
		if err != nil {
			t.Fatalf("Failed to produce message: %v", err)
		}
	}
	t.Log("Produced 5 messages to test-partitions topic")

	// Consume and verify we get messages from multiple partitions
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messages := make(chan string, 20)
	errors := make(chan error, 10)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.ConsumeTopic(ctx, topic, true, messages, errors)
	}()

	partitionsSeen := make(map[string]bool)
	messageCount := 0
	timeout := time.After(5 * time.Second)

collect:
	for messageCount < 5 {
		select {
		case msg := <-messages:
			messageCount++
			// Extract partition from "[P0] message" format
			if len(msg) > 3 && msg[0] == '[' && msg[1] == 'P' {
				partition := string(msg[2])
				partitionsSeen[partition] = true
				t.Logf("Message %d from partition %s: %s", messageCount, partition, msg)
			}
		case err := <-errors:
			t.Logf("Consumer error (may be ok): %v", err)
		case <-timeout:
			break collect
		}
	}

	cancel()
	wg.Wait()

	t.Logf("Saw messages from %d partitions: %v", len(partitionsSeen), partitionsSeen)
	if messageCount == 0 {
		t.Error("Did not receive any messages")
	}
}

func TestManager_GracefulShutdown(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	topic := "test-partitions"

	ctx, cancel := context.WithCancel(context.Background())
	messages := make(chan string, 10)
	errors := make(chan error, 10)

	// Track when consumption actually stops
	done := make(chan struct{})
	go func() {
		manager.ConsumeTopic(ctx, topic, false, messages, errors)
		close(done)
	}()

	// Give it time to start consuming
	time.Sleep(500 * time.Millisecond)

	// Cancel and measure shutdown time
	start := time.Now()
	cancel()

	select {
	case <-done:
		elapsed := time.Since(start)
		t.Logf("Graceful shutdown completed in %v", elapsed)
		if elapsed > 2*time.Second {
			t.Errorf("Shutdown took too long: %v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown timed out - goroutines may be leaked!")
	}
}

func TestManager_CreateAndDeleteTopic(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	topic := "test-create-delete-" + time.Now().Format("20060102150405")

	// Create topic
	err = manager.CreateTopic(topic)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("Created topic: %s", topic)

	// Verify topic exists - need to refresh metadata first as client caches it
	time.Sleep(1 * time.Second) // Give Kafka time to propagate
	if err := manager.Client.RefreshMetadata(); err != nil {
		t.Logf("Warning: Failed to refresh metadata: %v", err)
	}
	topics, err := manager.Client.ListTopics()
	if err != nil {
		t.Fatalf("Failed to get topics: %v", err)
	}

	found := false
	for _, tp := range topics {
		if tp == topic {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Created topic %s not found in topic list", topic)
	}

	// Delete topic
	err = manager.DeleteTopic(topic)
	if err != nil {
		t.Fatalf("Failed to delete topic: %v", err)
	}
	t.Logf("Deleted topic: %s", topic)
}

// TestManager_GetClusterState_Consistency calls GetClusterState multiple times
// and verifies that the concurrent goroutine-based aggregation produces
// deterministic, consistent results (same broker/topic/partition counts).
func TestManager_GetClusterState_Consistency(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	const iterations = 3
	type snapshot struct {
		brokers    int
		topics     int
		partitions int
		offsets    int
	}

	var snapshots [iterations]snapshot
	for i := 0; i < iterations; i++ {
		state, err := manager.GetClusterState()
		if err != nil {
			t.Fatalf("GetClusterState call %d failed: %v", i, err)
		}
		snapshots[i] = snapshot{
			brokers:    len(state.Brokers),
			topics:     state.TotalTopics,
			partitions: state.TotalPartitions,
			offsets:    len(state.TopicOffsets),
		}
	}

	// All calls should agree on every counter.
	for i := 1; i < iterations; i++ {
		if snapshots[i] != snapshots[0] {
			t.Errorf("inconsistency between call 0 and call %d:\n  call 0: %+v\n  call %d: %+v",
				i, snapshots[0], i, snapshots[i])
		}
	}

	// TopicOffsets slice length must equal TotalTopics (one entry per topic).
	if snapshots[0].offsets != snapshots[0].topics {
		t.Errorf("TopicOffsets count (%d) != TotalTopics (%d) — concurrent aggregation may be losing entries",
			snapshots[0].offsets, snapshots[0].topics)
	}

	t.Logf("GetClusterState consistent across %d calls: %d brokers, %d topics, %d partitions",
		iterations, snapshots[0].brokers, snapshots[0].topics, snapshots[0].partitions)
}

// TestManager_GetClusterState_Performance verifies that GetClusterState
// completes within a reasonable time bound.  A regression from concurrent
// to sequential execution would cause this to exceed the deadline on any
// cluster with more than a handful of topics.
func TestManager_GetClusterState_Performance(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	start := time.Now()
	state, err := manager.GetClusterState()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("GetClusterState failed: %v", err)
	}

	t.Logf("GetClusterState completed in %v (%d topics, %d partitions, %d consumer groups)",
		elapsed, state.TotalTopics, state.TotalPartitions, len(state.ConsumerGroups))

	// 5 seconds is generous; on a local single-broker cluster this should
	// finish in well under 1 second.  The bound exists to catch regressions
	// back to sequential behaviour (which on Alpine caused 30 s+ delays).
	const deadline = 5 * time.Second
	if elapsed > deadline {
		t.Errorf("GetClusterState took %v, expected < %v (possible sequential regression)", elapsed, deadline)
	}
}

// TestManager_GetClusterState_WhileConsuming guards against head-of-line
// blocking: offset lookups must not queue behind the consumer's long-polling
// fetch requests (this used to take ~20 s on a 28-topic demo cluster).
func TestManager_GetClusterState_WhileConsuming(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	topic := "test-cluster-state-while-consuming"
	_ = manager.CreateTopic(topic)
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messages := make(chan string, 10)
	errors := make(chan error, 10)
	go manager.ConsumeTopic(ctx, topic, false, messages, errors)
	go func() {
		for {
			select {
			case <-messages:
			case <-errors: //nolint:errcheck // only draining the channel
			case <-ctx.Done():
				return
			}
		}
	}()
	time.Sleep(2 * time.Second) // let partition consumers start fetching

	start := time.Now()
	if _, err := manager.GetClusterState(); err != nil {
		t.Fatalf("GetClusterState failed: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("GetClusterState with active consumer: %v", elapsed)

	const deadline = 3 * time.Second
	if elapsed > deadline {
		t.Errorf("GetClusterState took %v with an active consumer, expected < %v", elapsed, deadline)
	}
}

// TestManager_ConsumeTopic_StopsWhenReaderGone verifies that ConsumeTopic
// returns after cancellation even when nobody reads the channels anymore
// (previously partition goroutines blocked forever on the send).
func TestManager_ConsumeTopic_StopsWhenReaderGone(t *testing.T) {
	skipIfNoKafka(t)

	manager, err := kafka.NewManager([]string{getTestBroker()})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	topic := "test-consume-reader-gone"
	_ = manager.CreateTopic(topic)
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if err := manager.ProduceMessage(topic, "msg"); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	messages := make(chan string) // unbuffered and never read
	errors := make(chan error)
	done := make(chan struct{})
	go func() {
		manager.ConsumeTopic(ctx, topic, true, messages, errors)
		close(done)
	}()

	time.Sleep(2 * time.Second) // partition consumers are now blocked on send
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeTopic did not return after cancellation with no reader")
	}
}
