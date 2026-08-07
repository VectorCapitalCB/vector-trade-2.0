/**
 * E2E del vector-chat-service. Sin dependencias: usa el WebSocket nativo de Node 22+.
 *
 *   node src/test/e2e/chat-e2e.mjs [ws://localhost:8097/ws/]
 *
 * Levanta dos clientes, ejercita todas las acciones del endpoint y valida el contrato
 * de payloads que consume ChatController del blotter. Usa usernames con sufijo aleatorio
 * para no ensuciar las conversaciones reales.
 */

const URL = process.argv[2] || 'ws://localhost:8097/ws/';
const SUFFIX = Math.random().toString(36).slice(2, 8);
const A = `e2e_alice_${SUFFIX}`;
const B = `e2e_bob_${SUFFIX}`;

let passed = 0;
const failures = [];

function check(name, ok, detail = '') {
  if (ok) {
    passed++;
    console.log(`  PASS  ${name}`);
  } else {
    failures.push(`${name}${detail ? ' — ' + detail : ''}`);
    console.log(`  FAIL  ${name}${detail ? ' — ' + detail : ''}`);
  }
}

function connect(label) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(URL);
    const inbox = [];
    const waiters = [];
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      const i = waiters.findIndex((w) => w.match(msg));
      if (i >= 0) waiters.splice(i, 1)[0].resolve(msg);
      else inbox.push(msg);
    };
    ws.onerror = () => reject(new Error(`${label}: error de websocket`));
    ws.onopen = () =>
      resolve({
        label,
        send: (obj) => ws.send(JSON.stringify(obj)),
        close: () => ws.close(),
        drain: () => inbox.splice(0, inbox.length),
        // Espera un mensaje que cumpla `match`, mirando primero lo ya recibido.
        expect(match, timeoutMs = 8000) {
          const i = inbox.findIndex(match);
          if (i >= 0) return Promise.resolve(inbox.splice(i, 1)[0]);
          return new Promise((res, rej) => {
            const waiter = { match, resolve: res };
            waiters.push(waiter);
            setTimeout(() => {
              const j = waiters.indexOf(waiter);
              if (j >= 0) {
                waiters.splice(j, 1);
                rej(new Error(`${label}: timeout esperando mensaje`));
              }
            }, timeoutMs);
          });
        },
      });
  });
}

const byType = (t) => (m) => m.type === t;
const isAscending = (arr) => arr.every((v, i) => i === 0 || arr[i - 1] <= v);

(async () => {
  console.log(`E2E contra ${URL}  (usuarios ${A} / ${B})\n`);
  const t0 = Date.now();
  const alice = await connect('alice');
  const bob = await connect('bob');

  console.log('registro');
  alice.send({ action: 'chat_register', username: A });
  bob.send({ action: 'chat_register', username: B });
  const regA = await alice.expect(byType('chat_registered'));
  await bob.expect(byType('chat_registered'));
  check('chat_register devuelve chat_registered con el username', regA.username === A);

  console.log('envio y fan-out');
  alice.send({ action: 'chat_send', fromUsername: A, toUsername: B, message: 'hola 1' });
  const rxB = await bob.expect(byType('chat_message'));
  const rxA = await alice.expect(byType('chat_message'));
  check('el destinatario recibe el mensaje', rxB.message === 'hola 1' && rxB.fromUsername === A);
  check('el emisor recibe su propio eco', rxA.message === 'hola 1');
  check(
    'payload trae claves cortas y largas (contrato del blotter)',
    rxB.from === A && rxB.to === B && rxB.msg === 'hola 1' && rxB.message === 'hola 1',
    JSON.stringify(rxB)
  );
  check('payload trae id, conversationId y timestamp', !!rxB.id && !!rxB.conversationId && typeof rxB.timestamp === 'number');

  console.log('mensajes identicos consecutivos');
  alice.send({ action: 'chat_send', fromUsername: A, toUsername: B, message: 'repetido' });
  const dup1 = await bob.expect((m) => m.type === 'chat_message' && m.message === 'repetido');
  alice.send({ action: 'chat_send', fromUsername: A, toUsername: B, message: 'repetido' });
  const dup2 = await bob.expect((m) => m.type === 'chat_message' && m.message === 'repetido' && m.id !== dup1.id);
  check('dos mensajes de igual texto llegan como dos eventos distintos', dup1.id !== dup2.id);

  console.log('orden bajo rafaga');
  alice.drain();
  bob.drain();
  const BURST = 50;
  for (let i = 0; i < BURST; i++) {
    alice.send({ action: 'chat_send', fromUsername: A, toUsername: B, message: `burst-${String(i).padStart(3, '0')}` });
  }
  await bob.expect((m) => m.type === 'chat_message' && m.message === `burst-${String(BURST - 1).padStart(3, '0')}`, 20000);

  console.log('historial');
  const tHist = Date.now();
  alice.send({ action: 'chat_history', username: A, withUsername: B, limit: 500 });
  const hist = await alice.expect(byType('chat_history'));
  const histMs = Date.now() - tHist;
  const burstLines = hist.messages.filter((m) => m.message.startsWith('burst-')).map((m) => m.message);
  check('historial devuelve los mensajes de la rafaga', burstLines.length === BURST, `${burstLines.length}/${BURST}`);
  check('historial respeta el orden de envio', burstLines.join(',') === burstLines.slice().sort().join(','));
  check('historial viene en timestamp ascendente', isAscending(hist.messages.map((m) => m.timestamp)));
  check('historial trae los campos largos', hist.messages.every((m) => m.fromUsername && m.toUsername && m.conversationId));

  console.log('conversaciones y usuarios');
  alice.send({ action: 'chat_conversations', username: A, limit: 100 });
  const convs = await alice.expect(byType('chat_conversations'));
  const withBob = convs.conversations.find((c) => c.withUsername === B);
  check('chat_conversations lista al peer con su ultimo mensaje', !!withBob && !!withBob.lastMessage);

  alice.send({ action: 'chat_users', limit: 300 });
  const usersMsg = await alice.expect(byType('chat_users'));
  check('chat_users incluye a ambos participantes', usersMsg.users.includes(A) && usersMsg.users.includes(B));

  console.log('snapshot');
  const tSnap = Date.now();
  alice.send({ type: 'snapshot_request', user: A });
  const snap = await alice.expect(byType('snapshot'), 20000);
  const snapMs = Date.now() - tSnap;
  check('snapshot responde al alias snapshot_request', snap.user === A);
  check('snapshot no se incluye a si mismo en users', !snap.users.includes(A));
  check('snapshot viene en timestamp ascendente', isAscending(snap.messages.map((m) => m.timestamp)));
  check(
    'snapshot contiene la conversacion completa',
    snap.messages.filter((m) => m.msg.startsWith('burst-')).length === BURST
  );
  check('snapshot usa claves cortas (las que parsea el blotter)', snap.messages.every((m) => m.from && m.to && m.msg));

  console.log('errores');
  alice.send({ action: 'chat_send', fromUsername: A, toUsername: B, message: '' });
  const err1 = await alice.expect(byType('error'));
  check('chat_send sin mensaje devuelve error', /requiere/.test(err1.message));
  alice.send({ action: 'no_existe' });
  const err2 = await alice.expect(byType('error'));
  check('accion desconocida devuelve error', /no soportada/.test(err2.message));
  alice.send({ action: 'chat_history', username: A });
  const err3 = await alice.expect(byType('error'));
  check('chat_history sin withUsername devuelve error', /requiere/.test(err3.message));

  console.log('reconexion');
  bob.close();
  const bob2 = await connect('bob2');
  bob2.send({ type: 'snapshot_request', user: B });
  const snapB = await bob2.expect(byType('snapshot'), 20000);
  check('tras reconectar el snapshot trae el historial', snapB.messages.length >= BURST);
  alice.send({ action: 'chat_send', fromUsername: A, toUsername: B, message: 'post-reconexion' });
  const rxB2 = await bob2.expect((m) => m.type === 'chat_message' && m.message === 'post-reconexion');
  check('la sesion reconectada recibe mensajes nuevos', !!rxB2);

  alice.close();
  bob2.close();

  console.log(`\nhistorial ${histMs}ms | snapshot ${snapMs}ms | total ${Date.now() - t0}ms`);
  console.log(`\n${passed} OK, ${failures.length} fallidos`);
  if (failures.length) {
    failures.forEach((f) => console.log(`  - ${f}`));
    process.exit(1);
  }
  process.exit(0);
})().catch((e) => {
  console.error('\nE2E abortado:', e.message);
  process.exit(1);
});
