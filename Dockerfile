# Stage 1: Build the Go binary
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY third_party/ ./third_party/
COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /bin/messenger-agent ./cmd/server

# Stage 2: Minimal production runtime image
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -g '' appuser

WORKDIR /app
COPY --from=builder /bin/messenger-agent /app/messenger-agent
COPY migrations/ /app/migrations/

USER appuser
EXPOSE 8080

ENTRYPOINT ["/app/messenger-agent"]
