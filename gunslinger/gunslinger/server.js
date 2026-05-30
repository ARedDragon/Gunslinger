const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
const PUBLIC_DIR = path.join(__dirname, 'public');

const MAX_BULLETS = 4;
const ROUNDS_TO_WIN = 3;
const TOTAL_ROUNDS = 5;
const VALID_MOVES = ['load', 'shoot', 'dodge', 'aim', 'fanfire'];

const waitingQueue = [];
const activeRooms = new Set();
const allSockets = new Set();
const profiles = new Map();
const seenMatches = new Set();

function sanitizeName(name) {
  return String(name || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[^a-zA-Z0-9 _-]/g, '')
    .slice(0, 20);
}

function expectedScore(rating, oppRating) {
  return 1 / (1 + Math.pow(10, (oppRating - rating) / 400));
}

function getProfile(name) {
  const safe = sanitizeName(name);
  if (!safe) return null;
  if (!profiles.has(safe)) {
    profiles.set(safe, { name: safe, rating: 1000, wins: 0, games: 0, updatedAt: Date.now() });
  }
  return profiles.get(safe);
}

function sendWs(ws, msg) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function broadcastOnlineCount() {
  const online = allSockets.size;
  const payload = JSON.stringify({ type: 'online_count', online });
  for (const ws of allSockets) {
    if (ws.readyState === ws.OPEN) {
      ws.send(payload);
    }
  }
}

function notifyQueueStatus() {
  for (const entry of waitingQueue) {
    sendWs(entry.ws, {
      type: 'queue_wait',
      queueSize: waitingQueue.length,
      online: allSockets.size,
    });
  }
}

function removeFromQueue(entry) {
  const idx = waitingQueue.indexOf(entry);
  if (idx >= 0) {
    waitingQueue.splice(idx, 1);
    entry.inQueue = false;
    notifyQueueStatus();
  }
}

function enqueue(entry) {
  if (entry.inQueue || entry.room) return;
  entry.inQueue = true;
  entry.joinedAt = Date.now();
  waitingQueue.push(entry);
  sendWs(entry.ws, {
    type: 'queue_wait',
    queueSize: waitingQueue.length,
    online: allSockets.size,
  });
  notifyQueueStatus();
}

function newGame() {
  return {
    round: 1,
    host: { bullets: 1, score: 0, move: null },
    guest: { bullets: 1, score: 0, move: null },
    duel: null,
  };
}

function publicGame(g) {
  return {
    round: g.round,
    host: { bullets: g.host.bullets, score: g.host.score, moved: !!g.host.move },
    guest: { bullets: g.guest.bullets, score: g.guest.score, moved: !!g.guest.move },
  };
}

function applyAmmo(player, move) {
  if (move === 'load') player.bullets = Math.min(MAX_BULLETS, player.bullets + 1);
  if (move === 'shoot') player.bullets = Math.max(0, player.bullets - 1);
  if (move === 'fanfire') player.bullets = Math.max(0, player.bullets - 2);
}

function winningMove(a, b) {
  if (a === b) return null;

  // Rebalance: dodge is no longer a broad counter, and fanfire/aim have distinct value.
  const beats = {
    shoot: ['load', 'aim'],
    dodge: ['shoot'],
    load: [],
    aim: ['dodge', ],
    fanfire: ['dodge', 'aim', 'load'],
  };

  if (beats[a]?.includes(b)) return a;
  if (beats[b]?.includes(a)) return b;
  return null;
}

function isShotMove(move) {
  return move === 'shoot' || move === 'fanfire';
}

class MatchRoom {
  constructor(id) {
    this.id = id;
    this.sessions = [];
    this.game = null;
  }

  addPlayer(entry, role) {
    entry.role = role;
    entry.room = this;
    entry.inQueue = false;
    this.sessions.push(entry);
  }

  start() {
    this.game = newGame();

    for (const s of this.sessions) {
      sendWs(s.ws, {
        type: 'start',
        role: s.role,
        matchId: this.id,
        game: publicGame(this.game),
      });
    }

    this.broadcastNames();
  }

  broadcast(msg) {
    const data = JSON.stringify(msg);
    for (const s of this.sessions) {
      if (s.ws.readyState === s.ws.OPEN) {
        s.ws.send(data);
      }
    }
  }

  broadcastNames() {
    const host = this.sessions.find((s) => s.role === 'host');
    const guest = this.sessions.find((s) => s.role === 'guest');

    this.broadcast({
      type: 'player_names',
      host: host?.name || 'Host',
      guest: guest?.name || 'Guest',
    });
  }

  handleMessage(entry, msg) {
    if (msg.type === 'intro') {
      const next = sanitizeName(msg.name);
      if (next) {
        entry.name = next;
      }
      this.broadcastNames();
      return;
    }

    if (msg.type === 'duel_click') {
      this.handleDuelClick(entry, msg);
      return;
    }

    if (!this.game || msg.type !== 'move') return;

    const move = msg.move;
    const player = this.game[entry.role];

    if (!VALID_MOVES.includes(move)) return;
    if (player.move) return;
    if (move === 'shoot' && player.bullets < 1) return;
    if (move === 'fanfire' && player.bullets < 2) return;
    if (move === 'load' && player.bullets >= MAX_BULLETS) return;

    player.move = move;
    sendWs(entry.ws, { type: 'move_ack', move });

    if (this.game.host.move && this.game.guest.move) {
      this.resolveRound();
    }
  }

  resolveRound() {
    const g = this.game;
    const hMove = g.host.move;
    const gMove = g.guest.move;

    applyAmmo(g.host, hMove);
    applyAmmo(g.guest, gMove);

    if (hMove === 'shoot' && gMove === 'shoot') {
      this.startShootout();
      return;
    }

    const winner = winningMove(hMove, gMove);
    let hostPoint = 0;
    let guestPoint = 0;

    if (hMove !== gMove) {
      if (winner === hMove && isShotMove(hMove)) hostPoint = 1;
      else if (winner === gMove && isShotMove(gMove)) guestPoint = 1;
    }

    this.finishRound(hostPoint, guestPoint, {
      hostMove: hMove,
      guestMove: gMove,
      hostPoint,
      guestPoint,
    });
  }

  startShootout() {
    const roundId = `${this.game.round}-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
    const greenDelay = 1200 + Math.floor(Math.random() * 1800);
    const greenAt = Date.now() + greenDelay;

    this.game.duel = {
      roundId,
      greenAt,
      hostClick: null,
      guestClick: null,
    };

    this.broadcast({ type: 'shootout_start', roundId, greenAt });
  }

  handleDuelClick(entry, msg) {
    const duel = this.game?.duel;
    if (!duel || msg.roundId !== duel.roundId) return;

    const reactionMs = Number.isFinite(Number(msg.reactionMs)) ? Number(msg.reactionMs) : 9999;
    const click = {
      falseStart: !!msg.falseStart,
      reactionMs: Math.max(0, Math.min(9999, reactionMs)),
      at: Date.now(),
    };

    if (entry.role === 'host' && !duel.hostClick) duel.hostClick = click;
    if (entry.role === 'guest' && !duel.guestClick) duel.guestClick = click;

    if (duel.hostClick && duel.guestClick) {
      this.resolveShootout();
    }
  }

  resolveShootout() {
    const duel = this.game.duel;
    const host = duel.hostClick;
    const guest = duel.guestClick;
    let hostPoint = 0;
    let guestPoint = 0;

    if (host.falseStart && !guest.falseStart) guestPoint = 1;
    else if (guest.falseStart && !host.falseStart) hostPoint = 1;
    else if (!host.falseStart && !guest.falseStart) {
      if (host.reactionMs < guest.reactionMs) hostPoint = 1;
      else if (guest.reactionMs < host.reactionMs) guestPoint = 1;
    }

    this.game.duel = null;

    this.finishRound(hostPoint, guestPoint, {
      hostMove: 'shoot',
      guestMove: 'shoot',
      hostPoint,
      guestPoint,
      shootout: {
        hostFalseStart: host.falseStart,
        guestFalseStart: guest.falseStart,
        hostReactionMs: host.reactionMs,
        guestReactionMs: guest.reactionMs,
      },
    });
  }

  finishRound(hostPoint, guestPoint, round) {
    const g = this.game;
    g.host.score += hostPoint;
    g.guest.score += guestPoint;
    g.host.move = null;
    g.guest.move = null;
    g.round += 1;

    const over = g.host.score >= ROUNDS_TO_WIN || g.guest.score >= ROUNDS_TO_WIN || g.round > TOTAL_ROUNDS;

    this.broadcast({
      type: over ? 'game_over' : 'round_result',
      round,
      game: publicGame(g),
    });
  }

  removePlayer(entry) {
    this.sessions = this.sessions.filter((s) => s !== entry);
    entry.room = null;

    this.broadcast({ type: 'opponent_left' });

    if (this.sessions.length === 0) {
      activeRooms.delete(this);
    }
  }
}

function tryMatchmake() {
  while (waitingQueue.length >= 2) {
    waitingQueue.sort((a, b) => a.joinedAt - b.joinedAt);
    const first = waitingQueue.shift();

    if (!first || first.ws.readyState !== first.ws.OPEN || first.room) {
      continue;
    }

    let bestIdx = -1;
    let bestScore = Number.POSITIVE_INFINITY;
    const now = Date.now();

    for (let i = 0; i < waitingQueue.length; i++) {
      const candidate = waitingQueue[i];
      if (!candidate || candidate.ws.readyState !== candidate.ws.OPEN || candidate.room) {
        continue;
      }

      const ratingDiff = Math.abs(first.rating - candidate.rating);
      const waitedMs = now - candidate.joinedAt;
      const score = ratingDiff - waitedMs / 2000;

      if (score < bestScore) {
        bestScore = score;
        bestIdx = i;
      }
    }

    if (bestIdx < 0) {
      first.inQueue = true;
      waitingQueue.unshift(first);
      break;
    }

    const second = waitingQueue.splice(bestIdx, 1)[0];

    if (!second || second.ws.readyState !== second.ws.OPEN || second.room) {
      first.inQueue = true;
      waitingQueue.unshift(first);
      continue;
    }

    first.inQueue = false;
    second.inQueue = false;

    const room = new MatchRoom(`match-${Date.now()}-${Math.floor(Math.random() * 100000)}`);
    room.addPlayer(first, 'host');
    room.addPlayer(second, 'guest');
    activeRooms.add(room);

    sendWs(first.ws, { type: 'queue_matched' });
    sendWs(second.ws, { type: 'queue_matched' });
    room.start();
  }

  notifyQueueStatus();
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 1_000_000) {
        req.destroy();
        reject(new Error('Body too large'));
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, payload) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'GET' && url.pathname === '/api/online') {
    return sendJson(res, 200, {
      online: allSockets.size,
      queue: waitingQueue.length,
      activeMatches: activeRooms.size,
    });
  }

  if (req.method === 'GET' && url.pathname === '/api/profile') {
    const profile = getProfile(url.searchParams.get('name') || '');
    if (!profile) return sendJson(res, 400, { error: 'Invalid name' });
    return sendJson(res, 200, { profile });
  }

  if (req.method === 'GET' && url.pathname === '/api/leaderboard') {
    const limitRaw = Number(url.searchParams.get('limit') || 20);
    const limit = Math.max(1, Math.min(50, Number.isFinite(limitRaw) ? Math.floor(limitRaw) : 20));
    const leaderboard = Array.from(profiles.values())
      .sort((a, b) => (b.rating - a.rating) || (b.wins - a.wins) || a.name.localeCompare(b.name))
      .slice(0, limit);
    return sendJson(res, 200, { leaderboard });
  }

  if (req.method === 'POST' && url.pathname === '/api/match') {
    try {
      const body = await readJson(req);
      const mode = body.mode === 'online' ? 'online' : 'ai';
      const matchId = `${mode}:${String(body.matchId || '').slice(0, 80)}`;
      if (!body.matchId) return sendJson(res, 400, { error: 'Missing matchId' });
      if (seenMatches.has(matchId)) return sendJson(res, 200, { deduped: true });

      const player = getProfile(body.player || '');
      if (!player) return sendJson(res, 400, { error: 'Invalid player' });

      if (mode === 'ai') {
        const outcome = body.outcome;
        if (!['win', 'loss', 'draw'].includes(outcome)) {
          return sendJson(res, 400, { error: 'Invalid outcome' });
        }

        const aiRating = 1000;
        const score = outcome === 'win' ? 1 : outcome === 'loss' ? 0 : 0.5;
        const k = 16;
        const expected = expectedScore(player.rating, aiRating);

        player.rating = Math.max(100, Math.round(player.rating + k * (score - expected)));
        player.games += 1;
        if (outcome === 'win') player.wins += 1;
        player.updatedAt = Date.now();

        seenMatches.add(matchId);
        return sendJson(res, 200, { mode, profiles: [player] });
      }

      const opponent = getProfile(body.opponent || '');
      if (!opponent) return sendJson(res, 400, { error: 'Invalid opponent' });
      if (opponent.name === player.name) return sendJson(res, 400, { error: 'Player and opponent must differ' });

      const myScore = Number(body.myScore);
      const oppScore = Number(body.oppScore);
      if (!Number.isFinite(myScore) || !Number.isFinite(oppScore)) {
        return sendJson(res, 400, { error: 'Scores are required' });
      }

      let scoreA = 0.5;
      let scoreB = 0.5;
      if (myScore > oppScore) {
        scoreA = 1;
        scoreB = 0;
      } else if (myScore < oppScore) {
        scoreA = 0;
        scoreB = 1;
      }

      const k = 24;
      const expectedA = expectedScore(player.rating, opponent.rating);
      const expectedB = expectedScore(opponent.rating, player.rating);

      player.rating = Math.max(100, Math.round(player.rating + k * (scoreA - expectedA)));
      opponent.rating = Math.max(100, Math.round(opponent.rating + k * (scoreB - expectedB)));

      player.games += 1;
      opponent.games += 1;
      if (scoreA === 1) player.wins += 1;
      if (scoreB === 1) opponent.wins += 1;

      player.updatedAt = Date.now();
      opponent.updatedAt = Date.now();
      seenMatches.add(matchId);

      return sendJson(res, 200, { mode, profiles: [player, opponent] });
    } catch (_) {
      return sendJson(res, 400, { error: 'Bad JSON' });
    }
  }

  let filePath = url.pathname;
  if (filePath === '/' || filePath === '') filePath = '/index.html';
  const abs = path.join(PUBLIC_DIR, path.normalize(filePath).replace(/^([.][.][/\\])+/, ''));
  if (!abs.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end('Forbidden');
  }

  fs.readFile(abs, (err, data) => {
    if (err) {
      res.writeHead(404);
      return res.end('Not found');
    }

    const ext = path.extname(abs).toLowerCase();
    const typeMap = {
      '.html': 'text/html; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.js': 'application/javascript; charset=utf-8',
      '.json': 'application/json; charset=utf-8',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg',
      '.svg': 'image/svg+xml',
      '.ico': 'image/x-icon',
      '.webp': 'image/webp',
    };

    res.writeHead(200, { 'content-type': typeMap[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  if (url.pathname !== '/ws/queue') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, (ws) => {
    const entry = {
      ws,
      name: 'Outlaw',
      rating: 1000,
      joinedAt: Date.now(),
      inQueue: false,
      role: null,
      room: null,
    };

    allSockets.add(ws);
    sendWs(ws, { type: 'online_count', online: allSockets.size });
    broadcastOnlineCount();

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(String(raw));
      } catch (_) {
        return;
      }

      if (msg.type === 'queue_cancel') {
        removeFromQueue(entry);
        return;
      }

      if (msg.type === 'intro') {
        const safe = sanitizeName(msg.name);
        if (safe) {
          entry.name = safe;
          const profile = getProfile(safe);
          if (profile) {
            entry.rating = profile.rating;
          }
        }

        if (entry.room) {
          entry.room.handleMessage(entry, msg);
          return;
        }

        enqueue(entry);
        tryMatchmake();
        return;
      }

      if (entry.room) {
        entry.room.handleMessage(entry, msg);
      }
    });

    ws.on('close', () => {
      removeFromQueue(entry);
      if (entry.room) {
        entry.room.removePlayer(entry);
      }
      allSockets.delete(ws);
      broadcastOnlineCount();
      notifyQueueStatus();
    });
  });
});

server.listen(PORT, () => {
  console.log(`Frontier Legends server running on http://localhost:${PORT}`);
});
