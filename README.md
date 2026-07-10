# POC 1 — Real-Time Multiplayer

POC de um sistema de salas multiplayer com **desafio sincronizado** entre
jogadores, usando WebSocket para conexões persistentes e Redis Pub/Sub como
Event Bus entre os serviços.

Mapeamento para o Diagrama C4 Nível 2:

| Container C4 | Serviço no compose |
|---|---|
| Load Balancer | `nginx` |
| WebSocket Gateway + Rate Limiter + Broadcast | `gateway` |
| Serviço de Sala + Event Bus consumer | `room-service` |
| Event Bus (Pub/Sub) + Armazenamento de Estado | `redis` |
| Observabilidade | `otel-collector`, `prometheus`, `grafana` (perfil opcional) |
| Ferramenta de Carga | `k6` (perfil opcional) |

---

## Arquitetura

```
Jogador
  │  ws://.../ws?room=X&player=Y
  ▼
┌────────┐   ip_hash    ┌─────────┐  Redis Pub/Sub   ┌──────────────┐
│  nginx │─────────────▶│ gateway │◀────────────────▶│ room-service │
└────────┘              └─────────┘   room:*:in/out   └──────────────┘
                              │                                │
                              └───────────► redis ◄────────────┘
                                    (Pub/Sub + snapshots de estado)
```

**Fluxo de uma partida:**

1. Jogador conecta em `ws://nginx/ws?room=<room>&player=<player>`.
2. O `gateway` registra a conexão no `Hub` (em memória, agrupado por sala) e
   publica um evento `join` no canal `room:<room>:in`.
3. O `room-service`, inscrito em `room:*:in`, consome o evento, atualiza o
   snapshot da sala e salva no Redis (TTL de 10 min).
4. Quando um jogador envia `start_challenge`, o `room-service` agenda um
   disparo sincronizado 3s no futuro e, ao chegar a hora, publica o evento
   `challenge_start` no canal `room:<room>:out`.
5. O `gateway`, inscrito em `room:*:out`, faz o broadcast do evento para
   todos os clientes WebSocket conectados naquela sala.

---

## Estrutura do repositório

```
poc-realtime/
├── docker-compose.yml
├── gateway/
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go
├── room-service/
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go
├── nginx/
│   └── nginx.conf
├── observability/
│   ├── otel-collector-config.yaml
│   └── prometheus.yml
└── loadtest/
    └── load-test.js
```

---

## O que tem em cada arquivo

### `gateway/main.go` — WebSocket Gateway

Concentra três responsabilidades do C4 (WebSocket Gateway, Rate Limiter e
Broadcast) em um único binário Go:

- **`Hub`**: estrutura em memória (`map[room]map[*Client]bool`) protegida por
  `sync.RWMutex`, que mantém quem está conectado em cada sala.
- **`wsHandler`** (`/ws`): faz o upgrade da conexão HTTP para WebSocket,
  registra o cliente no `Hub`, publica o evento de `join` e entra em loop
  lendo mensagens do jogador.
- **`rateLimited`**: usa `INCR` + `EXPIRE` no Redis para limitar cada
  jogador a **20 mensagens por segundo** (`rateLimitMax` / `rateLimitWindow`).
  Mensagens acima do limite recebem `{"error": "rate_limit_exceeded"}`.
- **`subscribeBroadcast`**: assina `room:*:out` via `PSubscribe` e repassa
  cada evento recebido para todos os clientes da sala correspondente.
- Endpoints auxiliares: `/healthz` (liveness) e `/metrics` (Prometheus, via
  `promhttp.Handler()`).

### `room-service/main.go` — Serviço de Sala

- **`RoomState`**: struct com `RoomID`, `Players`, `ChallengeAt` e
  `UpdatedAt`, serializada em JSON e persistida no Redis como
  `room:state:<id>` com TTL de 10 minutos (suporta reconexão sem perda
  total do progresso — ADR-002).
- **`handleIncoming`**: consome mensagens de `room:*:in`. Trata dois tipos
  de evento:
  - `join`: adiciona o jogador à lista `Players` do snapshot.
  - `start_challenge`: agenda `scheduleBroadcast` para 3 segundos no futuro.
- **`scheduleBroadcast`**: dorme até o horário agendado e publica o evento
  `challenge_start` em `room:<id>:out`, que o gateway vai repassar aos
  clientes.
- Também expõe `/healthz` e `/metrics`.

### `nginx/nginx.conf` — Load Balancer

- `upstream gateway_backend` com **`ip_hash`**, usado como aproximação de
  sticky session por jogador.
  > ⚠️ **Limitação conhecida**: `ip_hash` garante afinidade por IP, não por
  > *sala*. Uma solução completa de afinidade por sala exigiria roteamento
  > pela query string `room` (ex.: hashing consistente ou script Lua no
  > NGINX) — fora do escopo mínimo deste POC.
- Roteia `/ws` (com headers de upgrade para WebSocket e timeouts de 1h),
  `/healthz` e `/metrics` para o `gateway_backend`.

### `docker-compose.yml`

Orquestra todos os serviços em uma rede bridge (`poc-net`):

| Serviço | Porta exposta | Observação |
|---|---|---|
| `nginx` | `80` | entrada pública |
| `redis` | `6379` | sem persistência (`--appendonly no`) |
| `gateway` | `8080` (interno) | pode escalar horizontalmente |
| `room-service` | `8081` (interno) | |
| `otel-collector` | — | perfil `observability` |
| `prometheus` | `9090` | perfil `observability` |
| `grafana` | `3000` | perfil `observability` |
| `k6` | — | perfil `loadtest` |

### `observability/`

- **`otel-collector-config.yaml`**: recebe métricas via OTLP (gRPC/HTTP) e
  exporta no formato Prometheus na porta `8889`. Preparado para uma futura
  evolução em que os serviços Go passem a usar o SDK OpenTelemetry.
- **`prometheus.yml`**: hoje faz *scrape* direto de `gateway:8080/metrics`
  e `room-service:8081/metrics` (auto-instrumentação via
  `prometheus/client_golang`), a cada 5s.

### `loadtest/load-test.js`

Script k6 que simula carga progressiva de jogadores conectando via
WebSocket:

- Rampa de VUs: `0 → 100 → 500 → 1.000` em ~90s, mantém 1.000 por 1 min,
  depois cai a 0.
- Cada VU conecta em uma sala aleatória (`room-0` a `room-49`), envia
  `start_challenge` e valida que recebeu alguma resposta.

---

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose v2 (`docker compose`, não `docker-compose`)
- Não é necessário instalar Go localmente — o build acontece dentro dos containers.

---

## Como executar

### 1. Subir o núcleo do sistema

```bash
docker compose up --build
```

Isso sobe `nginx`, `gateway`, `room-service` e `redis`. A aplicação fica
disponível em:

- WebSocket: `ws://localhost/ws?room=<room>&player=<player>`
- Healthcheck: `http://localhost/healthz`
- Métricas: `http://localhost/metrics`

Para testar manualmente uma conexão WebSocket, use uma ferramenta como
[websocat](https://github.com/vi/websocat) ou a extensão de WebSocket do seu
cliente HTTP favorito:

```bash
websocat "ws://localhost/ws?room=room-1&player=player-1"
```

### 2. Subir com observabilidade (Prometheus + Grafana + OTel Collector)

```bash
docker compose --profile observability up -d
```

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (login padrão `admin` / `admin`)

> Grafana sobe sem datasource ou dashboard pré-configurado — é preciso
> adicionar o Prometheus (`http://prometheus:9090`) manualmente como fonte
> de dados na primeira vez.

### 3. Rodar o teste de carga (k6)

Com o núcleo já em execução (`docker compose up`), em outro terminal:

```bash
docker compose --profile loadtest run k6
```

O k6 vai simular até 1.000 jogadores simultâneos conectando via
`ws://nginx/ws`, seguindo a rampa definida em `loadtest/load-test.js`.

### 4. Simular múltiplos nós do Gateway

Para testar o comportamento do nginx com `ip_hash` sob múltiplas
instâncias do gateway:

```bash
docker compose up --build --scale gateway=3
```

### Parar e limpar

```bash
docker compose down          # remove containers e rede
docker compose down -v       # idem + remove volumes (se houver)
```

---

## Limitações conhecidas / débitos técnicos

- **Sticky session aproximada**: `ip_hash` no nginx não garante afinidade
  real por sala, apenas por IP do jogador.
- **Redis sem persistência**: `--save ""` e `--appendonly no` — adequado
  apenas ao escopo do POC; dados de sala não sobrevivem a um restart do
  Redis.
- **Rate limit fixo**: 20 mensagens/segundo por jogador, hardcoded em
  `gateway/main.go` (`rateLimitMax`, `rateLimitWindow`), sem configuração
  externa via variável de ambiente.
- **Grafana sem provisioning**: datasource e dashboards precisam ser
  configurados manualmente.
- **Sem `go.sum`**: os `go.mod` fixam apenas as versões diretas; rode
  `go mod tidy` dentro de cada serviço se precisar de builds
  100% reprodutíveis.

---

## Versões principais

| Item | Versão |
|---|---|
| Go | 1.22 |
| nginx | 1.27-alpine |
| redis | 7-alpine |
| gorilla/websocket | v1.5.3 |
| redis/go-redis/v9 | v9.5.1 |
| prometheus/client_golang | v1.19.1 |
| opentelemetry-collector-contrib | 0.104.0 |
| prometheus (servidor) | v2.53.0 |
| grafana | 11.1.0 |
| k6 | 0.52.0 |