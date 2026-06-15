# Consumer Behavior in lazykafka

## Overview

lazykafka is a terminal-based Kafka inspection and debugging tool.  Its
built-in consumer is intentionally designed to be **zero-footprint** — it reads
messages without leaving any trace on the Kafka cluster.

## Two ways to consume from Kafka

The Kafka protocol offers two consumption models.  Understanding the
difference is key to knowing why lazykafka behaves the way it does.

### 1. Consumer Group mode (`ConsumerGroup` API)

This is the model most production applications use:

- The client sends a **JoinGroup** request to Kafka's group coordinator.
- The coordinator assigns partitions across all members of the group.
- Consumed offsets are **committed** back to Kafka (topic
  `__consumer_offsets`).
- The group is visible via `ListConsumerGroups` / `DescribeConsumerGroups`
  admin APIs.

```
┌─────────────┐     JoinGroup      ┌──────────────────┐
│  Consumer A  │ ─────────────────► │                  │
│  group=my-app│ ◄───── Assign ──── │  Group           │
└─────────────┘    P0, P1          │  Coordinator     │
                                    │                  │
┌─────────────┐     JoinGroup      │  (Kafka broker)  │
│  Consumer B  │ ─────────────────► │                  │
│  group=my-app│ ◄───── Assign ──── │                  │
└─────────────┘    P2, P3          └──────────────────┘
```

### 2. Partition-assign mode (`ConsumePartition` API)

The client directly opens a fetch session on specific partitions.  No group
coordinator is involved:

- No `JoinGroup` / `SyncGroup` requests are sent.
- No offsets are committed.
- No group metadata is stored on the broker.
- The consumer is **invisible** to admin APIs.

```
┌─────────────┐    Fetch(P0)       ┌──────────────────┐
│  lazykafka   │ ─────────────────► │  Partition leader │
│  (no group)  │ ◄──── Messages ─── │  (Kafka broker)  │
│              │    Fetch(P1)       │                  │
│              │ ─────────────────► │                  │
│              │ ◄──── Messages ─── │                  │
└─────────────┘                    └──────────────────┘
```

## What lazykafka uses and why

lazykafka uses **partition-assign mode**.  The implementation lives in
[`kafka/manager.go → ConsumeTopic`](../kafka/manager.go):

```go
// For each partition of the selected topic:
pc, err := consumer.ConsumePartition(topic, partition, offset)
```

This is a deliberate design choice for a debugging/inspection tool:

| Concern | Why partition-assign is better here |
|---------|-------------------------------------|
| **No side effects** | Does not create consumer group metadata that would pollute the cluster state you are trying to inspect. |
| **No rebalancing** | Opening multiple lazykafka instances against the same topic won't trigger partition reassignment in any existing consumer group. |
| **No offset commits** | Does not interfere with the committed offsets of production consumers. |
| **Simplicity** | No group coordinator round-trips — just open a fetch session and start reading. |
| **Full partition coverage** | Always reads from *all* partitions, regardless of how many lazykafka instances are running. Two instances will each see all messages independently. |

### Offset handling

When you select a topic in lazykafka, the consume panel offers two starting
points:

| Mode | Sarama constant | Behavior |
|------|-----------------|----------|
| Latest (default) | `sarama.OffsetNewest` | Only new messages produced *after* consumption starts |
| From beginning | `sarama.OffsetOldest` | All messages still retained on the broker |

Since no offsets are committed, closing and reopening lazykafka always starts
fresh from the chosen position.

### Lifecycle

1. User selects a topic in the topics panel.
2. Any previously running consumer is cancelled via `context.WithCancel`.
3. A new `ConsumeTopic` goroutine is launched for each partition.
4. Messages flow into the consume panel until the user selects a different
   topic or disconnects.

```
  Select topic "orders"
        │
        ▼
  Cancel previous consumer (if any)
        │
        ▼
  ctx, cancel = context.WithCancel(...)
        │
        ▼
  ┌─────────────────────────────────────┐
  │  for each partition:                │
  │    ConsumePartition(topic, p, off)  │
  │    └─ goroutine: read messages      │
  │       until ctx.Done()              │
  └─────────────────────────────────────┘
        │
        ▼
  Messages displayed in consume panel
```

## "No consumer groups found" — what it means

The cluster state view in lazykafka queries `ListConsumerGroups` via the Kafka
admin API.  Since lazykafka's own consumer never joins a group, it will not
appear in that list.

The message displayed:

> No consumer groups found (built-in consumer uses partition mode, not group
> mode)

…is informational — it tells you that the absence of groups is expected if
lazykafka is the only client connected to the cluster.

## How to see consumer groups for testing

To populate the Consumer Groups table in the cluster state view, you need an
external consumer that uses group mode.

### Using `kafka-console-consumer` (bundled with Kafka)

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 \
  --topic my-topic \
  --group test-group
```

### Using Docker (with the project's docker-compose Kafka)

```bash
docker exec -it kafka-0 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --group test-group
```

### Using kcat (formerly kafkacat)

```bash
kcat -b localhost:19092 -G test-group my-topic
```

### What you'll see in lazykafka

Once a grouped consumer has connected (or has previously committed offsets),
the cluster state view will display:

| Group Name | State  | Members | Total Lag | Top Topic by Lag |
|------------|--------|---------|-----------|------------------|
| test-group | Stable | 1       | 42        | my-topic (42)    |

Groups are sorted by total lag (highest first), and the state is
color-coded: green for `Stable`, yellow for `PreparingRebalance`, red for
`Dead` or `Empty`.

## Summary

| Aspect | lazykafka behavior |
|--------|-------------------|
| **Consumer API** | `ConsumePartition` (partition-assign) |
| **Group ID** | None — no group is created |
| **Offset commits** | None — offsets are not persisted |
| **Cluster footprint** | Zero — invisible to admin APIs |
| **Multiple instances** | Each instance independently reads all partitions |
| **Rebalancing** | Not applicable — no group coordinator involved |
