.PHONY: build run test test-integration kafka-up kafka-down demo-up demo-down clean publish

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

# Start demo Kafka pre-loaded with sample topics & messages (for screenshots / recordings)
demo-up:
	docker compose -f demo/docker-compose.demo.yml up -d kafka-demo
	@echo "Demo Kafka starting on localhost:19092"
	@echo "Waiting for health check..."
	@timeout 60 bash -c 'until docker compose -f demo/docker-compose.demo.yml ps | grep -q "healthy"; do sleep 2; done' || (echo "Kafka failed to start" && exit 1)
	@echo "Kafka is ready — seeding demo data..."
	docker compose -f demo/docker-compose.demo.yml run --rm kafka-demo-seed
	@echo ""
	@echo "Demo environment is ready! Connect lazykafka to localhost:19092"

# Stop demo Kafka and remove volumes
demo-down:
	docker compose -f demo/docker-compose.demo.yml down -v

# Clean build artifacts
clean:
	rm -f lazykafka
	rm -f lazykafka.log

# Full test suite
test-all: test test-integration

# Tag and push the current version to trigger the release pipeline
publish:
	@VERSION=$$(cat current-version | tr -d '[:space:]'); \
	echo "Publishing version: $$VERSION"; \
	git tag $$VERSION && git push origin $$VERSION

