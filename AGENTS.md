# AGENTS.md

## Scope
- This repo is a terminal Kafka client (`tview` + `tcell`) with a thin Kafka layer (`sarama`).
- Entry point is `main.go`; runtime state and panel wiring live in `ui/app.go`.

## Architecture You Need First
- UI is split into panels under `ui/`: brokers, topics, consume, produce, splash.
- `App` (`ui/app.go`) owns shared mutable state: the current Kafka connection (`App.Manager()`), modal mode (`InModal`), config/settings.
- Kafka boundary is `kafka/manager.go` + `kafka/client.go`; UI should call manager methods, not `sarama` directly.
- Persistence boundary is `config/config.go` and writes to `~/.lazykafka/{brokers.json,settings.json,lazykafka.log}`.

## Critical Concurrency / UI Rules
- Get the connection with `App.Manager()` (lock-free `atomic.Pointer`; nil when disconnected). Snapshot it once per operation and use that pointer; never hold a lock across a network call.
- Replace or drop the connection only via `manager.Swap` / `CompareAndSwap` in `ui/app.go`, then `Close()` the old one. `kafka.Manager.Close()` returns immediately and frees clients after in-flight calls finish; later calls return `kafka.ErrManagerClosed`.
- Every `kafka.Manager` method that touches the shared client must `acquire()`/`release()`.
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

## Repo, CI and Release Workflow
- Public repo `dubel/lazykafka` (formerly the private `lazykafka-src`; old `lazykafka-dist` is archived). Roadmap and open work live in GitHub issues, not in docs.
- `main` is protected by a ruleset: no direct pushes, changes go through a PR with green `lint`, `test` and `integration` checks (`.github/workflows/ci.yml`), squash merge only. The repo admin may merge their own PR without approval: `gh pr merge <n> --squash --admin --delete-branch`.
- Lint: `golangci-lint run ./...` with `.golangci.yml` (includes gofmt and integration-tagged files). CI also requires `go mod tidy -diff` to be clean.
- Release: bump `current-version` in a PR, merge, then `make publish` from `main` (tags and pushes `vX.Y.Z`). `.github/workflows/release.yml` runs GoReleaser, which publishes the GitHub Release in this repo, deb/rpm/archlinux packages, the Homebrew formula in `dubel/homebrew-tap` (needs the `GH_PAT_FOR_ACTIONS` secret) and the multi-arch image `ghcr.io/dubel/lazykafka` (`dockers_v2`).
- `Dockerfile` is release-only: it copies the binary prebuilt by GoReleaser (`$TARGETPLATFORM/lazykafka`) and runs as non-root user `lazykafka`. It cannot be built with a plain `docker build .`; test it with `goreleaser release --snapshot --clean`.
- `--version` prints the version plus commit and build date injected via ldflags (`main.version`, `main.commit`, `main.date`); local builds fall back to the embedded `current-version`.
- Release notes are written by hand in `docs/release-notes/<version>.md`; they are not yet wired into GitHub releases (see issues).

## Integration/Test Details
- Integration tests are tag-gated with `//go:build integration` (`kafka/manager_integration_test.go`, `ui/tui_simulation_test.go`); `make test-integration` runs all packages.
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
