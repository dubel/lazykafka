package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// closeRecorder is a SyncProducer whose Close signals the test; other methods
// are never called (the embedded nil interface would panic if they were).
type closeRecorder struct {
	sarama.SyncProducer
	closed chan struct{}
}

func (c *closeRecorder) Close() error {
	close(c.closed)
	return nil
}

func TestManager_CloseDoesNotWaitForInflightOperations(t *testing.T) {
	prod := &closeRecorder{closed: make(chan struct{})}
	m := &Manager{Producer: prod}

	if !m.acquire() { // an operation stuck on a hung broker
		t.Fatal("acquire on open manager failed")
	}

	done := make(chan struct{})
	go func() {
		m.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked on an in-flight operation")
	}

	select {
	case <-prod.closed:
		t.Fatal("producer closed while an operation was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	m.release()
	select {
	case <-prod.closed:
	case <-time.After(time.Second):
		t.Fatal("producer not closed after the in-flight operation finished")
	}

	m.Close() // idempotent: must not panic or close twice
}

func TestManager_OperationsAfterCloseFail(t *testing.T) {
	m := &Manager{}
	m.Close()

	if err := m.ProduceMessage("t", "v"); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("ProduceMessage: got %v, want ErrManagerClosed", err)
	}
	if _, err := m.ListTopics(true); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("ListTopics: got %v, want ErrManagerClosed", err)
	}
	if err := m.CheckConnection(); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("CheckConnection: got %v, want ErrManagerClosed", err)
	}
	if _, err := m.GetClusterState(); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("GetClusterState: got %v, want ErrManagerClosed", err)
	}
	if err := m.CreateTopic("t"); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("CreateTopic: got %v, want ErrManagerClosed", err)
	}

	errs := make(chan error, 1)
	m.ConsumeTopic(context.Background(), "t", false, make(chan string), errs)
	if err := <-errs; !errors.Is(err, ErrManagerClosed) {
		t.Errorf("ConsumeTopic: got %v, want ErrManagerClosed", err)
	}
}
