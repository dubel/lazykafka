# Release image, built by GoReleaser (dockers_v2) from the prebuilt binary.
# GoReleaser places each platform's binary under $TARGETPLATFORM/ in the build context.
FROM alpine:latest

# Add ca-certificates in case Kafka connects over TLS/SSL
RUN apk --no-cache add ca-certificates

# Connect the image to the GitHub repository so it shows up in the right sidebar
LABEL org.opencontainers.image.source="https://github.com/dubel/lazykafka"

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/lazykafka /usr/local/bin/lazykafka

# Command to run the executable
ENTRYPOINT ["lazykafka"]
