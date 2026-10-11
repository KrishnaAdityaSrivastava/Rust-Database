FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /gateway .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /gateway /usr/local/bin/gateway

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/gateway"]