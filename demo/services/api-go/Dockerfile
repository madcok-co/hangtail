FROM golang:1.26-bookworm AS builder
WORKDIR /app
COPY . .
# go mod tidy generates go.sum if absent, then builds.
RUN go mod tidy && CGO_ENABLED=0 go build -o /api-go .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /api-go /usr/local/bin/api-go
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api-go"]
