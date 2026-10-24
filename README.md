# LazyKafka

A terminal-based Kafka client for developers who prefer the command line.

Customer-facing docs, demo GIFs and the promo video live in the public
[`dubel/lazykafka`](https://github.com/dubel/lazykafka) repo (`lazykafka-dist`).

## Features

- **Broker Management** - Add, edit, remove, and connect to multiple Kafka brokers (persisted in `~/.lazykafka`)
- **Topic Operations** - List, filter (`/`), create, and delete topics (with confirmation); auto-refresh every 5 minutes
- **Message Consumption** - Live tail or replay from the beginning, across all partitions, without joining a consumer group
- **Message Production** - Type a message inline or publish a file via the built-in file picker
- **Cluster State** - Brokers, partition health, topic offsets, consumer groups with lag (removable), key broker config
- **Clipboard** - Copy the last consumed message (wl-copy, xsel, xclip, pbcopy, or OSC 52 over SSH)
- **Log Viewer** - Built-in application log viewer (`F5`)

## Requirements

- Kafka broker(s) running and accessible (PLAINTEXT listeners; TLS/SASL is not supported yet)
- Terminal with 80x24 minimum size

## Installation

### From Release

Download the latest binary from the [releases page](https://github.com/dubel/lazykafka/releases).

### From Source

```bash
make build
```

### Quick Start

1. Start Kafka (optional - if you need a local broker for testing):
   ```bash
   make kafka-up        # empty broker on localhost:19092
   make demo-up         # or: broker pre-loaded with demo topics, groups and a live producer
   ```

2. Run the application:
   ```bash
   ./lazykafka
   ```

3. Add a broker:
   - Press `a` in the Brokers panel
   - Enter broker address (e.g., `localhost:19092`)
   - Optionally add an alias

4. Connect to broker:
   - Press `Enter` on the broker to connect (focus moves to Topics)

5. Create a topic:
   - In the Topics panel (`F2`) press `a`, enter the topic name and confirm
   - New topics are created with 1 partition and replication factor 1

6. Consume messages:
   - Press `Enter` on a topic: consumption starts and the topic becomes the produce target
   - Press `t` in the Consume panel to toggle between new messages only and from beginning

7. Produce a message:
   - Navigate to Produce panel (`F4`)
   - Type your message and press `Alt+Enter` or `Ctrl+S`, or press `Ctrl+P` to publish a file

## Keyboard Shortcuts

### Global

| Key | Action |
|-----|--------|
| `Tab` | Cycle through panels |
| `F1` / `F2` / `F3` / `F4` | Focus Brokers / Topics / Consume / Produce |
| `F5` | Toggle application log viewer |
| `F6` | Show splash screen |
| `Esc` | Close search / clear filter; otherwise exit application |

### Brokers panel

| Key | Action |
|-----|--------|
| `Enter` | Connect to broker |
| `a` | Add broker |
| `e` | Edit broker |
| `r` / `Delete` | Remove broker from the list |
| `d` | Open Cluster State |

### Topics panel

| Key | Action |
|-----|--------|
| `Enter` | Start consuming and set as produce target |
| `/` | Filter topics (`Enter` applies, `Esc` clears) |
| `a` | Create topic |
| `r` | Delete topic (asks for confirmation) |
| `f` | Refresh topic list |

### Consume panel

| Key | Action |
|-----|--------|
| `t` | Toggle from beginning / new messages only |
| `c` | Copy last consumed message to clipboard |

The view keeps the last 2,000 messages.

### Produce panel

| Key | Action |
|-----|--------|
| `Alt+Enter` / `Ctrl+S` | Send message |
| `Ctrl+P` | Publish a file (file picker: `Enter`/`Space` open, `Backspace` parent dir, `Esc` cancel) |

### Cluster State (`d`)

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Cycle pages: topic offsets, consumer groups, config |
| `↑` / `↓` | Scroll |
| `r` | Refresh; on the consumer groups page: remove selected group (asks for confirmation) |
| `f` | Refresh |
| `Esc` | Close (also cancels loading) |

## Configuration

Files are stored in `~/.lazykafka/`:

- `brokers.json` - Saved broker connections
- `settings.json` - Application settings (splash on startup, last file picker directory)
- `lazykafka-DD-MM-YYYY.log` - Today's application log; logs from previous days are moved to `old/`

Config files are written atomically (temp file + rename) with mode `0600`.
If a config file cannot be parsed at startup, it is moved to `<name>.corrupt-<timestamp>`
and the app starts with defaults, so the original content is never overwritten.

Logs record topic names and message sizes, never message contents.

## Connection behaviour

- A health check refreshes metadata every 5 seconds; the connection is reported as lost after
  3 consecutive failures (about 15 seconds).
- Advertised broker hostnames that cannot be resolved (e.g. `kafka-0` from a Kubernetes StatefulSet)
  are transparently redirected to the bootstrap host. See `docs/kubernetes-broker-resolution.md`.

## Development

### Run Tests

```bash
make test              # Unit tests only
make test-integration  # Tests requiring Kafka (starts Docker Kafka)
make test-all          # All tests
```

Integration tests are tag-gated (`//go:build integration`) and use `localhost:19092`
unless `KAFKA_BROKER` is set. Run them with `-race` when touching consume or connection code.

### Run with Local Kafka

```bash
make kafka-up      # Start Kafka in Docker
make kafka-down    # Stop Kafka
make demo-up       # Demo Kafka with seed data (for screenshots / recordings)
make demo-down     # Stop demo Kafka
```

### Release notes

Release notes for each version live in `docs/release-notes/<version>.md`.

## License

MIT
