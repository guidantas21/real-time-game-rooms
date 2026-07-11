# --- Etapa de build ---
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o room-service main.go

# --- Etapa final (imagem enxuta) ---
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/room-service .

EXPOSE 8081
ENTRYPOINT ["./room-service"]
