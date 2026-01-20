.PHONY: build run test test-integration kafka-up kafka-down clean

# Build the application
build:
	go build -o lazykafka .

# Run the application
run:
	go run .

# Run unit tests only
test:
	go test -v ./...

# Run integration tests (requires Docker Kafka)
test-integration: kafka-up
	@echo "Waiting for Kafka to be ready..."
	@sleep 5
	go test -tags=integration -v ./kafka/...

# Start Kafka in Docker
kafka-up:
	docker compose up -d
	@echo "Kafka starting on localhost:19092"
	@echo "Waiting for health check..."
	@timeout 60 bash -c 'until docker compose ps | grep -q "healthy"; do sleep 2; done' || (echo "Kafka failed to start" && exit 1)
	@echo "Kafka is ready!"

# Stop Kafka
kafka-down:
	docker compose down

# Clean build artifacts
clean:
	rm -f lazykafka
	rm -f lazykafka.log

# Full test suite
test-all: test test-integration
