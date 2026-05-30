// src/room.js
// Durable Object: one instance per room code, holds 2 WebSocket connections

const MAX_BULLETS = 4;
const ROUNDS_TO_WIN = 3;
const TOTAL_ROUNDS = 5;
const VALID_MOVES = ['load', 'shoot', 'dodge', 'aim', 'fanfire'];

export class Room {
  constructor(state, env) {
    this.state = state;
    this.sessions = []; // [{ws, role}]  role: 'host' | 'guest'
    this.game = null;
  }

  async fetch(request) {
    // Must be a WebSocket upgrade
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    // Assign role
    const role = this.sessions.length === 0 ? 'host' : 'guest';
    if (this.sessions.length >= 2) {
      server.close(4000, 'Room full');
      return new Response(null, { status: 101, webSocket: client });
    }

    const session = { ws: server, role, name: role === 'host' ? 'Host' : 'Guest' };
    this.sessions.push(session);

    // Init game on second player joining
    if (this.sessions.length === 2) {
      this.game = newGame();
      this.broadcastNames();
      this.broadcast({ type: 'start', game: publicGame(this.game) });
    } else {
      server.send(JSON.stringify({ type: 'waiting', role }));
    }

    server.addEventListener('message', evt => {
      try {
        const msg = JSON.parse(evt.data);
        this.handleMessage(session, msg);
      } catch (e) {
        server.send(JSON.stringify({ type: 'error', message: 'Bad message' }));
      }
    });

    server.addEventListener('close', () => {
      this.sessions = this.sessions.filter(s => s !== session);
      this.broadcast({ type: 'opponent_left' });
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  handleMessage(session, msg) {
    if (msg.type === 'intro') {
      session.name = cleanName(msg.name) || session.name;
      this.broadcastNames();
      return;
    }

    if (msg.type === 'duel_click') {
      this.handleDuelClick(session, msg);
      return;
    }

    if (!this.game) return;
    if (msg.type !== 'move') return;

    const move = msg.move;
    const role = session.role;

    // Validate move
    if (!VALID_MOVES.includes(move)) return;
    if (this.game[role].move) return; // already moved this round

    const playerState = this.game[role];
    if (move === 'shoot' && playerState.bullets < 1) return; // no ammo
    if (move === 'fanfire' && playerState.bullets < 2) return; // needs 2 ammo
    if (move === 'load' && playerState.bullets >= MAX_BULLETS) return;

    // Store move
    this.game[role].move = move;

    // Ack to mover
    session.ws.send(JSON.stringify({ type: 'move_ack', move }));

    // Both moved? Resolve round
    if (this.game.host.move && this.game.guest.move) {
      this.resolveRound();
    }
  }

  resolveRound() {
    const g = this.game;
    const hMove = g.host.move;
    const gMove = g.guest.move;

    // Apply ammo
    applyAmmo(g.host, hMove);
    applyAmmo(g.guest, gMove);

    // Special case: if both shoot, run a red/green reaction duel.
    if (hMove === 'shoot' && gMove === 'shoot') {
      this.startShootout();
      return;
    }

    // Determine winner. A point is only awarded for a landed shot.
    const winner = winningMove(hMove, gMove);
    let hostPoint = 0, guestPoint = 0;
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
    const g = this.game;
    const roundId = `${g.round}-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
    const greenDelayMs = 1200 + Math.floor(Math.random() * 1800);
    const greenAt = Date.now() + greenDelayMs;

    g.duel = {
      roundId,
      greenAt,
      hostClick: null,
      guestClick: null,
    };

    this.broadcast({
      type: 'shootout_start',
      roundId,
      greenAt,
    });
  }

  handleDuelClick(session, msg) {
    if (!this.game?.duel) return;

    const duel = this.game.duel;
    if (msg.roundId !== duel.roundId) return;

    const reactionMs = Number.isFinite(Number(msg.reactionMs)) ? Number(msg.reactionMs) : 9999;
    const click = {
      falseStart: !!msg.falseStart,
      reactionMs: Math.max(0, Math.min(9999, reactionMs)),
      at: Date.now(),
    };

    if (session.role === 'host' && !duel.hostClick) duel.hostClick = click;
    if (session.role === 'guest' && !duel.guestClick) duel.guestClick = click;

    if (duel.hostClick && duel.guestClick) {
      this.resolveShootout();
    }
  }

  resolveShootout() {
    const g = this.game;
    const duel = g.duel;
    if (!duel) return;

    const host = duel.hostClick;
    const guest = duel.guestClick;
    let hostPoint = 0;
    let guestPoint = 0;

    if (host.falseStart && !guest.falseStart) {
      guestPoint = 1;
    } else if (guest.falseStart && !host.falseStart) {
      hostPoint = 1;
    } else if (!host.falseStart && !guest.falseStart) {
      if (host.reactionMs < guest.reactionMs) hostPoint = 1;
      else if (guest.reactionMs < host.reactionMs) guestPoint = 1;
    }

    g.duel = null;

    this.finishRound(hostPoint, guestPoint, {
      hostMove: 'shoot',
      guestMove: 'shoot',
      hostPoint,
      guestPoint,
      shootout: {
        hostFalseStart: !!host.falseStart,
        guestFalseStart: !!guest.falseStart,
        hostReactionMs: host.reactionMs,
        guestReactionMs: guest.reactionMs,
      },
    });
  }

  finishRound(hostPoint, guestPoint, roundResult) {
    const g = this.game;

    g.host.score += hostPoint;
    g.guest.score += guestPoint;

    // Clear moves for next round
    g.host.move = null;
    g.guest.move = null;
    g.round++;

    // Check game over
    const over = g.host.score >= ROUNDS_TO_WIN || g.guest.score >= ROUNDS_TO_WIN || g.round > TOTAL_ROUNDS;

    const payload = {
      type: over ? 'game_over' : 'round_result',
      round: roundResult,
      game: publicGame(g),
    };

    this.broadcast(payload);
  }

  broadcast(msg) {
    const data = JSON.stringify(msg);
    for (const s of this.sessions) {
      try { s.ws.send(data); } catch (_) {}
    }
  }

  broadcastNames() {
    const host = this.sessions.find(s => s.role === 'host');
    const guest = this.sessions.find(s => s.role === 'guest');
    this.broadcast({
      type: 'player_names',
      host: host?.name || 'Host',
      guest: guest?.name || 'Guest',
    });
  }
}

// ── Helpers ──────────────────────────────────────────────────

function newGame() {
  return {
    round: 1,
    host: { bullets: 1, score: 0, move: null },
    guest: { bullets: 1, score: 0, move: null },
  };
}

function publicGame(g) {
  // Expose everything — ammo is public info in Gunslinger
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

function isShotMove(move) {
  return move === 'shoot' || move === 'fanfire';
}

function cleanName(name) {
  return String(name || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[^a-zA-Z0-9 _-]/g, '')
    .slice(0, 20);
}

function winningMove(a, b) {
  if (a === b) return null;

  const beats = {
    shoot: ['load', 'aim'],
    dodge: ['shoot', 'fanfire'],
    load: ['dodge'],
    aim: ['dodge', 'load'],
    fanfire: ['shoot', 'load', 'aim'],
  };

  if (beats[a]?.includes(b)) return a;
  if (beats[b]?.includes(a)) return b;
  return null;
}
