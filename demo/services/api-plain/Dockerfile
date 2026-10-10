FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o api-plain .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /app/api-plain /usr/local/bin/
CMD ["api-plain"]
