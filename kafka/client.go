package kafka

import (
	"net"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// bootstrapFallbackDialer implements proxy.Dialer (golang.org/x/net/proxy).
// When Sarama tries to connect to a broker address that was advertised in
// metadata (e.g. "kafka-0:9092") but cannot be resolved by DNS, the dialer
// transparently substitutes the host portion of the original bootstrap address
// so that the same reachable IP is used instead.
type bootstrapFallbackDialer struct {
	bootstrapHost string        // host part of the address the user actually typed
	timeout       time.Duration
	cacheMutex    sync.RWMutex
	lookupCache   map[string]bool
	lookupFunc    func(string) ([]string, error) // injectable for testing; defaults to net.LookupHost
}

// newBootstrapFallbackDialer resolves the host part of bootstrapAddr once so
// that we always have a concrete IP to fall back to.
func newBootstrapFallbackDialer(bootstrapAddr string, timeout time.Duration) *bootstrapFallbackDialer {
	host, _, err := net.SplitHostPort(bootstrapAddr)
	if err != nil {
		// bootstrapAddr has no port (unusual); use it verbatim
		host = bootstrapAddr
	}
	// If the user supplied a hostname rather than an IP, resolve it now so the
	// fallback is always an IP address and never causes a second DNS failure.
	if net.ParseIP(host) == nil {
		if addrs, err := net.LookupHost(host); err == nil && len(addrs) > 0 {
			host = addrs[0]
		}
	}
	return &bootstrapFallbackDialer{
		bootstrapHost: host,
		timeout:       timeout,
		lookupCache:   make(map[string]bool),
		lookupFunc:    net.LookupHost,
	}
}

// Dial satisfies the proxy.Dialer interface used by sarama.Config.Net.Proxy.Dialer.
func (d *bootstrapFallbackDialer) Dial(network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// Unexpected format; attempt a direct dial and let it fail naturally.
		return net.DialTimeout(network, addr, d.timeout)
	}

	// If the broker hostname is unresolvable, redirect to the bootstrap IP.
	// We still keep the original port so that multi-broker clusters where
	// each broker uses the same IP but different ports work correctly.
	if net.ParseIP(host) == nil {
		d.cacheMutex.RLock()
		resolvable, cached := d.lookupCache[host]
		d.cacheMutex.RUnlock()

		if !cached {
			_, lookupErr := d.lookupFunc(host)
			resolvable = (lookupErr == nil)
			d.cacheMutex.Lock()
			d.lookupCache[host] = resolvable
			d.cacheMutex.Unlock()
		}

		if !resolvable {
			addr = net.JoinHostPort(d.bootstrapHost, port)
		}
	}

	return net.DialTimeout(network, addr, d.timeout)
}

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
	// Keep read/write timeouts short so health-checks detect a dead broker quickly
	// rather than hanging for sarama's 30-second default.
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	// Enable the fallback dialer so that broker addresses advertised in metadata
	// (e.g. "kafka-0" in a Kubernetes StatefulSet) are transparently rewritten to
	// the bootstrap IP when they cannot be resolved by DNS.  This is a no-op in
	// environments where all broker hostnames are resolvable.
	if len(brokers) > 0 {
		config.Net.Proxy.Enable = true
		config.Net.Proxy.Dialer = newBootstrapFallbackDialer(brokers[0], config.Net.DialTimeout)
	}

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

func (c *Client) RefreshMetadata() error {
	return c.saramaClient.RefreshMetadata()
}

// CheckConnection verifies the broker is still reachable by refreshing metadata.
// Returns nil when healthy, a non-nil error when the broker cannot be reached.
func (c *Client) CheckConnection() error {
	return c.saramaClient.RefreshMetadata()
}

func (c *Client) Leader(topic string, partition int32) (*sarama.Broker, error) {
	return c.saramaClient.Leader(topic, partition)
}

func (c *Client) Replicas(topic string, partition int32) ([]int32, error) {
	return c.saramaClient.Replicas(topic, partition)
}

func (c *Client) InSyncReplicas(topic string, partition int32) ([]int32, error) {
	return c.saramaClient.InSyncReplicas(topic, partition)
}

func (c *Client) GetOffset(topic string, partition int32, time int64) (int64, error) {
	return c.saramaClient.GetOffset(topic, partition, time)
}

func (c *Client) BrokersList() []*sarama.Broker {
	return c.saramaClient.Brokers()
}

func (c *Client) Controller() (*sarama.Broker, error) {
	return c.saramaClient.Controller()
}
