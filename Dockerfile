# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# Disabling CGO ensures we get a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o lazykafka .

# Final stage
FROM alpine:latest

# Add ca-certificates in case Kafka connects over TLS/SSL
RUN apk --no-cache add ca-certificates

# Connect the image to the GitHub repository so it shows up in the right sidebar
LABEL org.opencontainers.image.source="https://github.com/dubel/lazykafka"

# Copy the binary from builder
COPY --from=builder /app/lazykafka /usr/local/bin/lazykafka

# Command to run the executable
ENTRYPOINT ["lazykafka"]
