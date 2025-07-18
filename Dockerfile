# Example Dockerfile for a Go application (this is just a placeholder)
FROM scratch
COPY go-batcher /
ENTRYPOINT ["/go-batcher"]
