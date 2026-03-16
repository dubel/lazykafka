# LazyKafka

A terminal-based Kafka client for developers who prefer the command line.

## Features

- **Broker Management** - Add, edit, remove, and connect to multiple Kafka brokers
- **Topic Operations** - List, create, and delete topics with auto-refresh
- **Message Production** - Send messages to any topic
- **Message Consumption** - Real-time message streaming with partition awareness
- **Log Viewer** - Built-in application log viewer

## Requirements

- Kafka broker(s) running and accessible
- Terminal with 80x24 minimum size

## Installation

### From Release

Download the latest binary from the [releases page](https://github.com/dubel/lazykafka/releases).

### From Source

```bash
go build -o lazykafka .
```

### Quick Start

1. Start Kafka (optional - if you need a local broker for testing):
   ```bash
   docker compose up -d
   ```

2. Run the application:
   ```bash
   ./lazykafka
   ```

3. Add a broker:
   - Press `a` in the Brokers panel
   - Enter broker address (e.g., `localhost:9092` or `localhost:19092`)
   - Optionally add an alias

4. Connect to broker:
   - Press `Enter` on the broker to connect

5. Create a topic:
   - Navigate to Topics panel (`F2` or `Tab`)
   - Press `a` to add a new topic
   - Enter topic name and confirm

6. Produce a message:
   - Navigate to Produce panel (`F4` or `Tab`)
   - Type your message
   - Press `Alt+Enter` or `Ctrl+S` to send

7. Consume messages:
   - Navigate to Topics panel (`F2`)
   - Press `Enter` on a topic to start consuming
   - Switch to Consume panel (`F3`) to view messages
   - Press `t` to toggle between new messages only and from beginning

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Tab` | Cycle through panels |
| `F1` | Focus Brokers panel |
| `F2` | Focus Topics panel |
| `F3` | Focus Consume panel |
| `F4` | Focus Produce panel |
| `F5` | View application logs |
| `F6` | Show splash screen |
| `Esc` | Exit application |
| `a` | Add (in Brokers/Topics panel) |
| `e` | Edit broker (in Brokers panel) |
| `r` | Remove (in Brokers panel) |
| `f` | Refresh topics |
| `t` | Toggle consume from beginning / new messages |
| `Alt+Enter` | Send message (in Produce panel) |
| `Ctrl+S` | Send message (in Produce panel) |

## Configuration

Configuration files are stored in `~/.lazykafka/`:

- `brokers.json` - Saved broker connections
- `settings.json` - Application settings
- `lazykafka.log` - Application logs

## Development

### Run Tests

```bash
make test           # Unit tests only
make test-integration  # Tests requiring Kafka
make test-all       # All tests
```

### Run with Local Kafka

```bash
make kafka-up      # Start Kafka in Docker
make kafka-down    # Stop Kafka
```

## License

MIT
