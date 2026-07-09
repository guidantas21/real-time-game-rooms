// Gateway WebSocket do POC 1 — Real-Time.
//
// Este binário concentra, para fins do POC, os contêineres do C4 Nível 2:
//   - WebSocket Gateway: mantém as conexões persistentes com os jogadores.
//   - Rate Limiter: aplica limites por jogador usando contadores TTL no Redis (ADR/4.3).
//   - Broadcast: repassa aos clientes da sala os eventos publicados pelo Room Service (4.1).
//
// O Serviço de Sala roda em processo separado (room-service) e troca eventos
// via Redis Pub/Sub (Event Bus).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	rateLimitMax    = 20              // mensagens por jogador
	rateLimitWindow = 1 * time.Second // janela de tempo
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Client representa uma conexão WebSocket ativa de um jogador em uma sala.
type Client struct {
	conn *websocket.Conn
	room string
}

// Hub mantém, em memória, os clientes conectados agrupados por sala,
// permitindo o disparo sincronizado do desafio (broadcast) para todos
// os jogadores de uma mesma sala.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]map[*Client]bool
}

func newHub() *Hub {
	return &Hub{clients: make(map[string]map[*Client]bool)}
}

func (h *Hub) join(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[c.room] == nil {
		h.clients[c.room] = make(map[*Client]bool)
	}
	h.clients[c.room][c] = true
}

func (h *Hub) leave(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients[c.room], c)
}

func (h *Hub) broadcast(room string, msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients[room] {
		_ = c.conn.WriteMessage(websocket.TextMessage, msg)
	}
}

var (
	rdb *redis.Client
	hub = newHub()
	ctx = context.Background()
)

// rateLimited implementa o padrão Rate Limiting/Throttling (item 4.3 do documento),
// usando contadores com TTL no Redis, por jogador.
func rateLimited(playerID string) bool {
	key := "ratelimit:" + playerID
	count, err := rdb.Incr(ctx, key).Result()
	if err != nil {
		log.Println("erro no rate limiter (redis):", err)
		return false
	}
	if count == 1 {
		rdb.Expire(ctx, key, rateLimitWindow)
	}
	return count > rateLimitMax
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	room := r.URL.Query().Get("room")
	playerID := r.URL.Query().Get("player")
	if room == "" || playerID == "" {
		http.Error(w, "parâmetros 'room' e 'player' são obrigatórios", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("erro no upgrade da conexão:", err)
		return
	}
	defer conn.Close()

	client := &Client{conn: conn, room: room}
	hub.join(client)
	defer hub.leave(client)

	// Avisa o Room Service que o jogador entrou na sala.
	publishJoin(room, playerID)

	log.Printf("jogador %s conectado na sala %s", playerID, room)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("jogador %s desconectado da sala %s: %v", playerID, room, err)
			return
		}

		if rateLimited(playerID) {
			_ = conn.WriteJSON(map[string]string{"error": "rate_limit_exceeded"})
			continue
		}

		// Encaminha a mensagem para o Serviço de Sala via Event Bus (Redis Pub/Sub).
		if err := rdb.Publish(ctx, "room:"+room+":in", msg).Err(); err != nil {
			log.Println("erro ao publicar no event bus:", err)
		}
	}
}

func publishJoin(room, playerID string) {
	payload := []byte(`{"type":"join","player":"` + playerID + `"}`)
	if err := rdb.Publish(ctx, "room:"+room+":in", payload).Err(); err != nil {
		log.Println("erro ao publicar join:", err)
	}
}

// subscribeBroadcast escuta os eventos publicados pelo Room Service (ex.: disparo
// sincronizado do desafio) e repassa para todos os clientes conectados na sala
// correspondente — implementação do contêiner "Broadcast".
func subscribeBroadcast() {
	pubsub := rdb.PSubscribe(ctx, "room:*:out")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for msg := range ch {
		room := extractRoom(msg.Channel, ":out")
		hub.broadcast(room, []byte(msg.Payload))
	}
}

func extractRoom(channel, suffix string) string {
	room := channel[len("room:"):]
	return room[:len(room)-len(suffix)]
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})

	go subscribeBroadcast()

	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.Handle("/metrics", promhttp.Handler())

	log.Println("WebSocket Gateway ouvindo na porta 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
