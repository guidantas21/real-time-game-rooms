# Real-Time Salas/Rodadas + Sincronia de Evento

Validar a capacidade de manter salas de jogo com comunicação bidirecional em tempo real via WebSocket, com latência inferior a 200ms (p95) e escala de até 1.000 jogadores simultâneos.

## Escopo

- Motor de sala dedicado com WebSocket (ex.: Socket.IO, uWebSockets, Ably)
- Medição de latência end-to-end com percentis (p50, p95, p99)
- Simulação de carga progressiva: 100 → 500 → 1.000 jogadores
- Broadcast sincronizado: cronômetro regressivo + disparo simultâneo do desafio
- Medição de fan-out, reconexão e sincronização de estado

## Padrões Recomendados

Load Balancing, Stateless/Stateful, Event Driven Architecture (EDA), Streaming vs Messaging, Bulkhead/Isolation Pattern, Rate Limit/Throttling

## Decisões-Chave

- Tecnologia de WebSocket
- Arquitetura de sala
- Limites de capacidade por nó
