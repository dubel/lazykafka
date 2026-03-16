# AGENTS.md

## Scope
- This repo is a terminal Kafka client (`tview` + `tcell`) with a thin Kafka layer (`sarama`).
- Entry point is `main.go`; runtime state and panel wiring live in `ui/app.go`.

## Architecture You Need First
- UI is split into panels under `ui/`: brokers, topics, consume, produce, splash.
- `App` (`ui/app.go`) owns shared mutable state: `KafkaManager`, `ManagerMutex`, modal mode (`InModal`), config/settings.
- Kafka boundary is `kafka/manager.go` + `kafka/client.go`; UI should call manager methods, not `sarama` directly.
- Persistence boundary is `config/config.go` and writes to `~/.lazykafka/{brokers.json,settings.json,lazykafka.log}`.

## Critical Concurrency / UI Rules
- Guard all access to `App.KafkaManager` with `ManagerMutex` (`RLock` for read operations, `Lock` for replacement/close).
- Only mutate UI from `QueueUpdateDraw(...)` when called from goroutines (examples: `ui/topics.go`, `ui/consume.go`, `ui/brokers.go`).
- Topic consumption is cancel-driven: `ConsumePanel.cancelFunc` + `context.WithCancel` (`ui/consume.go`); always stop prior consumer before starting a new one.
- `ConnectToBroker` resets consume/produce state before replacing manager (`ui/app.go`); preserve that order when editing connection flow.

## Dev Workflows (Source of Truth: Makefile)
- Build binary:
```bash
make build
```
- Unit tests:
```bash
make test
```
- Integration tests (starts Docker Kafka automatically):
```bash
make test-integration
```
- Full suite:
```bash
make test-all
```
- Local Kafka lifecycle:
```bash
make kafka-up
make kafka-down
```

## Integration/Test Details
- Integration tests are tag-gated with `//go:build integration` (`kafka/manager_integration_test.go`, `ui/tui_simulation_test.go`).
- Default integration broker is `localhost:19092`; override with `KAFKA_BROKER` env var.
- Docker Kafka uses KRaft mode and exposes external listener on `localhost:19092` (`docker-compose.yml`).

## Project-Specific Patterns to Follow
- Keep panel footers/titles as status surfaces (temporary messages via timers in `ui/topics.go` and `ui/produce.go`).
- Prefer async Kafka ops from UI handlers (`go ...`) and then queue UI updates; avoid blocking tview input handlers.
- Topic list behavior: refresh metadata before listing, sort topics, and preserve selection marker (`ui/topics.go:LoadTopics`).
- Produce/consume coupling: selecting a topic starts consume and sets produce target (`ui/topics.go:SetSelectedFunc`).

## Dependencies and Boundaries
- Core libs: `github.com/rivo/tview`, `github.com/gdamore/tcell/v2`, `github.com/IBM/sarama` (`go.mod`).
- Do not add direct Kafka or terminal logic to `main.go`; keep orchestration in `ui/` and transport in `kafka/`.
