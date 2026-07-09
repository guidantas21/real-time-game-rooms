import { check } from 'k6';
import ws from 'k6/ws';

// Simula a carga progressiva descrita no documento (100 -> 500 -> 1.000 jogadores).
export const options = {
  scenarios: {
    progressive_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 100 },
        { duration: '30s', target: 500 },
        { duration: '30s', target: 1000 },
        { duration: '1m', target: 1000 },
        { duration: '30s', target: 0 },
      ],
    },
  },
};

export default function () {
  const room = `room-${Math.floor(Math.random() * 50)}`;
  const player = `player-${__VU}-${__ITER}`;
  const url = `ws://nginx/ws?room=${room}&player=${player}`;

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', () => {
      socket.send(JSON.stringify({ type: 'start_challenge' }));
    });

    socket.on('message', (data) => {
      check(data, { 'recebeu evento': (d) => d.length > 0 });
    });

    socket.setTimeout(() => socket.close(), 5000);
  });

  check(res, { 'conexão estabelecida': (r) => r && r.status === 101 });
}
