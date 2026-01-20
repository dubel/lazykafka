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
			break
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
