// Serviço de Sala do POC 1 — Real-Time.
//
// Responsável pelo ciclo de vida das salas, estado dos jogadores e regras de
// sincronização (item 3.3), consumindo eventos do Gateway via Redis Pub/Sub
// (Event Bus) e persistindo snapshots em Redis para suportar reconexão (ADR-002).
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

// RoomState é o snapshot da sala persistido no Redis (Armazenamento de Estado),
// usado para reconexão sem perda total de progresso (ADR-002).
type RoomState struct {
	RoomID      string    `json:"room_id"`
	Players     []string  `json:"players"`
	ChallengeAt time.Time `json:"challenge_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

const snapshotTTL = 10 * time.Minute

func saveSnapshot(state RoomState) error {
	state.UpdatedAt = time.Now()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, "room:state:"+state.RoomID, data, snapshotTTL).Err()
}

func loadSnapshot(roomID string) (*RoomState, error) {
	data, err := rdb.Get(ctx, "room:state:"+roomID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state RoomState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// handleIncoming processa as mensagens publicadas pelo Gateway (canal room:<id>:in)
// e dispara o desafio sincronizado, publicando no canal room:<id>:out (Event Bus).
func handleIncoming(roomID string, payload []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(payload, &msg); err != nil {
		log.Println("mensagem inválida recebida:", err)
		return
	}

	state, err := loadSnapshot(roomID)
	if err != nil {
		log.Println("erro ao carregar snapshot:", err)
		return
	}
	if state == nil {
		state = &RoomState{RoomID: roomID}
	}

	switch msg["type"] {
	case "join":
		if player, ok := msg["player"].(string); ok {
			state.Players = append(state.Players, player)
		}
	case "start_challenge":
		// Agenda o cronômetro e o broadcast sincronizado do desafio (item 1.1/4.1).
		state.ChallengeAt = time.Now().Add(3 * time.Second)
		go scheduleBroadcast(roomID, state.ChallengeAt)
	}

	if err := saveSnapshot(*state); err != nil {
		log.Println("erro ao salvar snapshot:", err)
	}
}

func scheduleBroadcast(roomID string, at time.Time) {
	time.Sleep(time.Until(at))

	event := map[string]interface{}{
		"type":    "challenge_start",
		"room_id": roomID,
		"at":      at,
	}
	data, err := json.Marshal(event)
	if err != nil {
		log.Println("erro ao serializar evento:", err)
		return
	}
	if err := rdb.Publish(ctx, "room:"+roomID+":out", data).Err(); err != nil {
		log.Println("erro ao publicar evento de desafio:", err)
	}
}

func subscribeIncoming() {
	pubsub := rdb.PSubscribe(ctx, "room:*:in")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for msg := range ch {
		roomID := extractRoom(msg.Channel, ":in")
		handleIncoming(roomID, []byte(msg.Payload))
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

	go subscribeIncoming()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.Handle("/metrics", promhttp.Handler())

	log.Println("Serviço de Sala ouvindo na porta 8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
