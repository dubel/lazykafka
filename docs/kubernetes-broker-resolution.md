# Kubernetes Broker Resolution — The Advertised Listeners Problem

## The Problem

When running Kafka inside Kubernetes (typically as a StatefulSet) and connecting
to it from another pod, you may hit a frustrating pattern:

| Operation       | Result |
|-----------------|--------|
| Connect via IP  | ✅ Works |
| List topics     | ✅ Works |
| Consume / Produce | ❌ `"cannot reach kafka-0 broker"` |

### Why connecting and listing topics works but consuming fails

Kafka's protocol has a metadata layer.  When a client connects to a bootstrap
broker (say `10.12.0.78:9092`), that broker responds with cluster metadata
including the **advertised listener** for every broker.  In a typical
Kubernetes StatefulSet the advertised hostname is the pod's own name:

```
broker.id=0  →  advertised.listeners=PLAINTEXT://kafka-0:9092
```

For **listing topics** or **describing the cluster**, Sarama (the Go Kafka
library used by lazykafka) reuses the *existing* TCP connection to the
bootstrap address — so the advertised hostname is never dialled and everything
works fine.

For **consuming** (or producing to a specific partition), Sarama must open a
**new connection** to the *leader* of each partition.  It looks up the leader
in the metadata and dials the advertised address directly:

```
dial tcp: lookup kafka-0: no such host
```

If `kafka-0` is not resolvable from the consumer pod's DNS (common when the
consumer lives in a different namespace or cluster, or when you used a raw IP
to connect), the connection fails.

### Visual overview

```
Consumer Pod                          Kafka StatefulSet
─────────────                         ──────────────────
 lazykafka
   │
   ├─ bootstrap connect ──────────►  10.12.0.78:9092   (works — direct IP)
   │   └─ metadata response:
   │       broker-0 = kafka-0:9092
   │
   ├─ list topics ─────────────────►  reuses connection (works)
   │
   └─ consume(topic, partition 0)
       └─ leader = broker-0
           └─ dial kafka-0:9092 ──►  DNS FAIL ✘
                                      kafka-0 not resolvable
```

## The Solution — `bootstrapFallbackDialer`

Rather than requiring users to reconfigure Kafka's `advertised.listeners`
(which they often cannot control), lazykafka transparently works around the
problem at the client level.

### How it works

Sarama exposes `Config.Net.Proxy.Dialer` — a `proxy.Dialer` interface
(`golang.org/x/net/proxy`) through which **every** broker connection is
routed.  lazykafka plugs in a custom `bootstrapFallbackDialer`:

1. **At construction time** (`newBootstrapFallbackDialer`), the dialer
   extracts the host portion of the bootstrap address the user typed
   (e.g. `10.12.0.78` from `10.12.0.78:9092`).  If the user provided a
   hostname rather than an IP, it is resolved to an IP immediately so the
   fallback never causes a second DNS failure.

2. **At dial time** (`Dial`), for each outgoing connection Sarama requests:
   - If the target host is already an IP address → **dial directly** (zero
     overhead).
   - If the target host is a hostname → **try DNS first**.
   - Only if DNS lookup fails → **substitute the bootstrap IP**, keeping the
     **original port** intact.

### Why the port matters

In a multi-broker cluster each broker may listen on a different port
(`:9092`, `:9093`, …) while sharing the same IP or load-balancer.  By
replacing *only* the host and preserving the port, the fallback routes each
broker connection correctly:

```
kafka-0:9092  →  DNS fails  →  10.12.0.78:9092
kafka-1:9093  →  DNS fails  →  10.12.0.78:9093
```

### When the dialer is a no-op

In any environment where all broker hostnames are resolvable (local
development, cloud managed Kafka, properly configured Kubernetes DNS), the DNS
lookup succeeds and the dialer passes the address through unchanged.  There is
**no performance penalty** in the happy path.

### Sequence with the fix

```
Consumer Pod                          Kafka StatefulSet
─────────────                         ──────────────────
 lazykafka
   │
   ├─ bootstrap connect ──────────►  10.12.0.78:9092   ✔
   │   └─ metadata response:
   │       broker-0 = kafka-0:9092
   │
   ├─ list topics ─────────────────►  reuses connection ✔
   │
   └─ consume(topic, partition 0)
       └─ leader = broker-0
           └─ dial kafka-0:9092
               └─ DNS fails
               └─ fallback ────────►  10.12.0.78:9092   ✔
```

## Implementation Details

The implementation lives in [`kafka/client.go`](../kafka/client.go).

### Key types and functions

| Symbol | Purpose |
|--------|---------|
| `bootstrapFallbackDialer` (struct) | Stores the resolved bootstrap host and timeout |
| `newBootstrapFallbackDialer(bootstrapAddr, timeout)` | Constructor — extracts and resolves the bootstrap host |
| `(*bootstrapFallbackDialer).Dial(network, addr)` | `proxy.Dialer` implementation — DNS-try-then-fallback logic |

### Integration point

In `NewClient`, the dialer is wired into Sarama's config:

```go
if len(brokers) > 0 {
    config.Net.Proxy.Enable = true
    config.Net.Proxy.Dialer = newBootstrapFallbackDialer(
        brokers[0], config.Net.DialTimeout,
    )
}
```

The first bootstrap broker address is used as the fallback target.  This is
the address the user explicitly provided, so it is known to be reachable.

## Summary

| Aspect | Detail |
|--------|--------|
| **Root cause** | Kafka advertises pod hostnames (`kafka-0`) that are unresolvable outside the StatefulSet's headless service |
| **Symptom** | Connect + list topics works; consume/produce fails with "cannot reach broker" |
| **Fix** | Custom `proxy.Dialer` that falls back to the bootstrap IP when DNS fails |
| **Scope of change** | `kafka/client.go` only — no Kafka server config changes needed |
| **Side effects** | None — the dialer is a no-op when DNS resolution succeeds |
