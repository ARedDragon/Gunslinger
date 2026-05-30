const MAX_BULLETS = 4;
const ROUNDS_TO_WIN = 3;
const TOTAL_ROUNDS = 7;
const ROUND_INACTIVITY_MS = 10_000;
const VALID_MOVES = ['load', 'shoot', 'dodge', 'aim', 'fanfire'];
const DEFAULT_RATING = 1000;
const FLOOR_RATING = 100;
const MAX_CHAT_MESSAGES = 60;
const NAME_CHANGE_COOLDOWN_MS = 60 * 60 * 1000;
const ADMIN_HANDLES = new Set(['jaydenlian']);

export class Matchmaker {
  constructor(state) {
    this.state = state;
    this.clients = new Map(); // id -> {ws,name,handle,rating,inQueue,joinedAt,roomId,role}
    this.queue = []; // client ids
    this.rooms = new Map(); // roomId -> room
    this.profilesCache = null;
    this.globalChat = [];
    this.initPromise = this.init();
  }

  async init() {
    this.globalChat = (await this.state.storage.get('globalChat')) || [];
  }

  async fetch(request) {
    await this.initPromise;

    const url = new URL(request.url);
    const isWebSocket = request.headers.get('Upgrade') === 'websocket';

    if (url.pathname === '/ws/queue' && isWebSocket) {
      return this.handleWebSocket(request);
    }

    if (url.pathname === '/api/online' && request.method === 'GET') {
      return Response.json({
        online: this.clients.size,
        queue: this.queue.length,
        activeMatches: this.rooms.size,
      });
    }

    if (url.pathname === '/api/profile' && request.method === 'GET') {
      const handle = sanitizeHandle(url.searchParams.get('handle') || '');
      const name = sanitizeName(url.searchParams.get('name') || '');
      if (!handle) return Response.json({ error: 'Handle is required' }, { status: 400 });
      const profile = await this.getOrCreateProfile({
        handle,
        name: name || 'Outlaw',
      });

      const updated = await this.applyNameChange(profile, name);
      if (updated.changed) {
        await this.saveProfile(profile);
      }

      return Response.json({ profile });
    }

    if (url.pathname === '/api/leaderboard' && request.method === 'GET') {
      const limit = clamp(parseInt(url.searchParams.get('limit') || '20', 10), 1, 50);
      const profiles = await this.getProfilesMap();
      const leaderboard = Array.from(profiles.values())
        .sort((a, b) => (b.rating - a.rating) || (b.wins - a.wins) || a.name.localeCompare(b.name))
        .slice(0, limit);
      return Response.json({ leaderboard });
    }

    if (url.pathname === '/api/match' && request.method === 'POST') {
      const body = await request.json().catch(() => null);
      if (!body) return Response.json({ error: 'Bad JSON' }, { status: 400 });
      const result = await this.recordMatch(body);
      if (!result.ok) return Response.json({ error: result.error }, { status: 400 });
      return Response.json(result.payload);
    }

    return new Response('Not found', { status: 404 });
  }

  handleWebSocket(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const id = crypto.randomUUID();
    this.clients.set(id, {
      ws: server,
      name: 'Outlaw',
      handle: '',
      rating: DEFAULT_RATING,
      inQueue: false,
      joinedAt: Date.now(),
      roomId: null,
      role: null,
    });

    this.sendTo(id, { type: 'online_count', online: this.clients.size });
    this.sendTo(id, { type: 'global_chat_history', items: this.globalChat });
    this.broadcastOnlineCount();

    server.addEventListener('message', (evt) => {
      this.handleSocketMessage(id, evt.data).catch(() => {});
    });

    server.addEventListener('close', () => {
      this.handleDisconnect(id);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  async handleSocketMessage(id, raw) {
    const client = this.clients.get(id);
    if (!client) return;

    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === 'hello' || msg.type === 'intro') {
      const safe = sanitizeName(msg.name || '');
      const safeHandle = sanitizeHandle(msg.handle || client.handle || safe);
      if (!safeHandle) {
        this.sendTo(id, { type: 'identity_required', message: 'Handle is required.' });
        return;
      }
      client.handle = safeHandle;

      if (await this.isBannedHandle(client.handle)) {
        this.sendTo(id, { type: 'banned', message: 'This handle is banned from the server.' });
        try {
          client.ws.close(4003, 'Banned');
        } catch {}
        return;
      }

      const profile = await this.getOrCreateProfile({
        handle: client.handle,
        name: safe || client.name || 'Outlaw',
      });
      const updated = await this.applyNameChange(profile, safe);
      if (updated.changed) {
        await this.saveProfile(profile);
      }

      client.name = profile.name;
      client.handle = profile.handle;
      client.rating = profile.rating;

      this.sendTo(id, { type: 'global_chat_history', items: this.globalChat });
      this.sendTo(id, { type: 'online_count', online: this.clients.size });
      this.sendTo(id, {
        type: 'identity',
        name: client.name,
        handle: client.handle,
        coins: Number(profile.coins) || 0,
        isAdmin: ADMIN_HANDLES.has(client.handle),
        nameLockedUntil: profile.nameLockedUntil || 0,
      });
      if (msg.type === 'intro' && !client.roomId) {
        this.enqueueClient(id);
        this.tryMatchmake();
      }
      return;
    }

    if (msg.type === 'queue_join') {
      if (!client.roomId) {
        this.enqueueClient(id);
        this.tryMatchmake();
      }
      return;
    }

    if (msg.type === 'queue_cancel') {
      this.removeFromQueue(id);
      return;
    }

    if (msg.type === 'leave_match') {
      this.leaveRoom(id);
      return;
    }

    if (msg.type === 'global_chat_send') {
      const text = sanitizeChat(msg.text);
      if (!text) return;
      const item = { from: client.name, text, ts: Date.now() };
      this.globalChat.push(item);
      if (this.globalChat.length > MAX_CHAT_MESSAGES) this.globalChat.shift();
      await this.state.storage.put('globalChat', this.globalChat);
      this.broadcastAll({ type: 'global_chat', item });
      return;
    }

    if (msg.type === 'report_user') {
      await this.handleReport(client, msg);
      return;
    }

    if (msg.type === 'admin_ban' || msg.type === 'admin_give_coins' || msg.type === 'admin_announce') {
      if (!ADMIN_HANDLES.has(client.handle)) {
        this.sendTo(id, { type: 'admin_result', ok: false, message: 'Not authorized.' });
        return;
      }

      if (msg.type === 'admin_ban') {
        await this.handleAdminBan(client, msg);
      } else if (msg.type === 'admin_give_coins') {
        await this.handleAdminGiveCoins(client, msg);
      } else {
        this.handleAdminAnnounce(client, msg);
      }
      return;
    }

    if (msg.type === 'admin_get_reports') {
      if (!ADMIN_HANDLES.has(client.handle)) {
        this.sendTo(id, { type: 'admin_result', ok: false, message: 'Not authorized.' });
        return;
      }
      await this.sendAdminSnapshot(id);
      return;
    }

    if (!client.roomId) return;

    if (msg.type === 'move') {
      this.handleMove(id, msg.move);
      return;
    }

    if (msg.type === 'duel_click') {
      this.handleDuelClick(id, msg);
      return;
    }

    if (msg.type === 'game_chat_send') {
      const text = sanitizeChat(msg.text);
      if (!text) return;
      this.broadcastRoom(client.roomId, {
        type: 'game_chat',
        item: { from: client.name, text, ts: Date.now() },
      });
    }
  }

  handleDisconnect(id) {
    this.removeFromQueue(id);
    this.leaveRoom(id);
    this.clients.delete(id);
    this.broadcastOnlineCount();
    this.notifyQueueStatus();
    this.pushAdminPlayerSnapshots();
  }

  broadcastAll(msg) {
    const data = JSON.stringify(msg);
    for (const entry of this.clients.values()) {
      try {
        entry.ws.send(data);
      } catch {}
    }
  }

  broadcastOnlineCount() {
    this.broadcastAll({ type: 'online_count', online: this.clients.size });
  }

  async sendAdminSnapshot(id) {
    this.sendTo(id, {
      type: 'admin_online_players',
      players: this.getOnlinePlayersSnapshot(),
    });
    this.sendTo(id, {
      type: 'admin_reports',
      items: await this.getReports(60),
    });
  }

  pushAdminPlayerSnapshots() {
    const players = this.getOnlinePlayersSnapshot();
    for (const [id, client] of this.clients.entries()) {
      if (!ADMIN_HANDLES.has(client.handle)) continue;
      this.sendTo(id, { type: 'admin_online_players', players });
    }
  }

  getOnlinePlayersSnapshot() {
    const out = [];
    for (const client of this.clients.values()) {
      if (!client.handle) continue;
      out.push({
        handle: client.handle,
        name: client.name || 'Outlaw',
        rating: client.rating || DEFAULT_RATING,
        inMatch: !!client.roomId,
      });
    }
    out.sort((a, b) => a.handle.localeCompare(b.handle));
    return out;
  }

  async getReports(limit) {
    const rows = [];
    const list = await this.state.storage.list({ prefix: 'report:', reverse: true, limit: clamp(limit, 1, 200) });
    for (const [, value] of list) {
      rows.push(value);
    }
    return rows;
  }

  async isBannedHandle(handle) {
    if (!handle) return false;
    const data = await this.state.storage.get(`ban:${handle}`);
    return !!data;
  }

  findClientByHandle(handle) {
    for (const [id, client] of this.clients.entries()) {
      if (client.handle === handle) return { id, client };
    }
    return null;
  }

  async handleReport(reporter, msg) {
    const targetHandle = sanitizeHandle(msg.targetHandle || '');
    const targetName = sanitizeName(msg.targetName || '');
    const reason = String(msg.reason || '').trim().slice(0, 300);
    if (!targetHandle || !reason) {
      this.sendToClient(reporter, { type: 'report_received', ok: false, message: 'Invalid report.' });
      return;
    }
    if (targetHandle === reporter.handle) {
      this.sendToClient(reporter, { type: 'report_received', ok: false, message: 'Cannot report yourself.' });
      return;
    }

    const key = `report:${Date.now()}:${Math.floor(Math.random() * 100000)}`;
    await this.state.storage.put(key, {
      reporterHandle: reporter.handle,
      reporterName: reporter.name,
      targetHandle,
      targetName,
      reason,
      context: String(msg.context || '').slice(0, 80),
      createdAt: Date.now(),
    });

    this.sendToClient(reporter, { type: 'report_received', ok: true, message: 'Report sent to admins.' });
    const reports = await this.getReports(60);
    for (const [id, client] of this.clients.entries()) {
      if (!ADMIN_HANDLES.has(client.handle)) continue;
      this.sendTo(id, { type: 'admin_reports', items: reports });
    }
  }

  async handleAdminBan(admin, msg) {
    const targetHandle = sanitizeHandle(msg.targetHandle || '');
    const reason = String(msg.reason || '').trim().slice(0, 200);
    if (!targetHandle) {
      this.sendToClient(admin, { type: 'admin_result', ok: false, message: 'Target handle required.' });
      return;
    }
    if (ADMIN_HANDLES.has(targetHandle)) {
      this.sendToClient(admin, { type: 'admin_result', ok: false, message: 'Cannot ban admin handle.' });
      return;
    }

    await this.state.storage.put(`ban:${targetHandle}`, {
      by: admin.handle,
      reason,
      at: Date.now(),
    });

    const found = this.findClientByHandle(targetHandle);
    if (found) {
      this.sendTo(found.id, { type: 'banned', message: reason || 'You were banned by an admin.' });
      try {
        found.client.ws.close(4003, 'Banned');
      } catch {}
    }

    this.pushAdminPlayerSnapshots();

    this.sendToClient(admin, { type: 'admin_result', ok: true, message: `Banned @${targetHandle}.` });
  }

  async handleAdminGiveCoins(admin, msg) {
    const targetHandle = sanitizeHandle(msg.targetHandle || '');
    const delta = Math.trunc(Number(msg.amount));
    if (!targetHandle || !Number.isFinite(delta) || delta === 0) {
      this.sendToClient(admin, { type: 'admin_result', ok: false, message: 'Valid target handle and non-zero amount required.' });
      return;
    }

    const profile = await this.getOrCreateProfile({
      handle: targetHandle,
      name: targetHandle,
    });
    profile.coins = Math.max(0, (Number(profile.coins) || 0) + delta);
    await this.saveProfile(profile);

    const found = this.findClientByHandle(targetHandle);
    if (found) {
      this.sendTo(found.id, { type: 'coins_update', coins: profile.coins });
      this.sendTo(found.id, { type: 'server_message', message: `Admin adjusted your coins by ${delta}.` });
    }

    this.sendToClient(admin, {
      type: 'admin_result',
      ok: true,
      message: `Updated @${targetHandle} coins by ${delta}. New balance: ${profile.coins}.`,
    });
  }

  handleAdminAnnounce(admin, msg) {
    const text = String(msg.text || '').trim().slice(0, 240);
    if (!text) {
      this.sendToClient(admin, { type: 'admin_result', ok: false, message: 'Message cannot be empty.' });
      return;
    }

    this.broadcastAll({
      type: 'server_message',
      message: text,
      from: `@${admin.handle}`,
      ts: Date.now(),
    });
    this.sendToClient(admin, { type: 'admin_result', ok: true, message: 'Announcement sent.' });
  }

  sendToClient(client, msg) {
    if (!client?.ws) return;
    try {
      client.ws.send(JSON.stringify(msg));
    } catch {}
  }

  sendTo(id, msg) {
    const entry = this.clients.get(id);
    if (!entry) return;
    try {
      entry.ws.send(JSON.stringify(msg));
    } catch {}
  }

  enqueueClient(id) {
    const entry = this.clients.get(id);
    if (!entry || entry.inQueue || entry.roomId) return;
    entry.inQueue = true;
    entry.joinedAt = Date.now();
    this.queue.push(id);
    this.notifyQueueStatus();
  }

  removeFromQueue(id) {
    const entry = this.clients.get(id);
    if (entry) entry.inQueue = false;
    const idx = this.queue.indexOf(id);
    if (idx >= 0) {
      this.queue.splice(idx, 1);
      this.notifyQueueStatus();
    }
  }

  notifyQueueStatus() {
    for (const id of this.queue) {
      this.sendTo(id, {
        type: 'queue_wait',
        queueSize: this.queue.length,
        online: this.clients.size,
      });
    }
  }

  tryMatchmake() {
    while (this.queue.length >= 2) {
      this.queue.sort((a, b) => {
        const aEntry = this.clients.get(a);
        const bEntry = this.clients.get(b);
        return (aEntry?.joinedAt || 0) - (bEntry?.joinedAt || 0);
      });

      const firstId = this.queue.shift();
      const first = this.clients.get(firstId);
      if (!first || first.roomId) continue;

      let bestIdx = -1;
      let bestScore = Number.POSITIVE_INFINITY;
      const now = Date.now();

      for (let i = 0; i < this.queue.length; i++) {
        const candidate = this.clients.get(this.queue[i]);
        if (!candidate || candidate.roomId) continue;

        const ratingDiff = Math.abs(first.rating - candidate.rating);
        const waitedMs = now - candidate.joinedAt;
        const score = ratingDiff - waitedMs / 2000;

        if (score < bestScore) {
          bestScore = score;
          bestIdx = i;
        }
      }

      if (bestIdx < 0) {
        this.queue.unshift(firstId);
        break;
      }

      const secondId = this.queue.splice(bestIdx, 1)[0];
      const second = this.clients.get(secondId);
      if (!second || second.roomId) {
        this.queue.unshift(firstId);
        continue;
      }

      first.inQueue = false;
      second.inQueue = false;

      const roomId = `match-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
      const room = {
        id: roomId,
        hostId: firstId,
        guestId: secondId,
        game: newGame(),
        turnTimer: null,
      };

      first.roomId = roomId;
      second.roomId = roomId;
      first.role = 'host';
      second.role = 'guest';
      this.rooms.set(roomId, room);

      this.sendTo(firstId, { type: 'queue_matched' });
      this.sendTo(secondId, { type: 'queue_matched' });

      this.sendTo(firstId, { type: 'start', role: 'host', matchId: roomId, game: publicGame(room.game) });
      this.sendTo(secondId, { type: 'start', role: 'guest', matchId: roomId, game: publicGame(room.game) });

      this.broadcastRoom(roomId, {
        type: 'player_names',
        host: first.name || 'Host',
        guest: second.name || 'Guest',
        hostHandle: first.handle || '',
        guestHandle: second.handle || '',
      });

      this.armTurnTimer(room);
    }

    this.notifyQueueStatus();
  }

  broadcastRoom(roomId, msg) {
    const room = this.rooms.get(roomId);
    if (!room) return;
    this.sendTo(room.hostId, msg);
    this.sendTo(room.guestId, msg);
  }

  getRoomAndRole(id) {
    const entry = this.clients.get(id);
    if (!entry || !entry.roomId) return { room: null, role: null };
    return { room: this.rooms.get(entry.roomId) || null, role: entry.role };
  }

  leaveRoom(id) {
    const entry = this.clients.get(id);
    if (!entry || !entry.roomId) return;

    const room = this.rooms.get(entry.roomId);
    const roomId = entry.roomId;
    entry.roomId = null;
    entry.role = null;

    if (!room) return;
    this.clearTurnTimer(room);

    const opponentId = room.hostId === id ? room.guestId : room.hostId;
    const opponent = this.clients.get(opponentId);
    if (opponent) {
      opponent.roomId = null;
      opponent.role = null;
      this.sendTo(opponentId, { type: 'opponent_left' });
    }

    this.rooms.delete(roomId);
  }

  handleMove(id, move) {
    const { room, role } = this.getRoomAndRole(id);
    if (!room || !role) return;

    const game = room.game;
    const player = game[role];

    if (!VALID_MOVES.includes(move)) return;
    if (player.move) return;
    if (move === 'shoot' && player.bullets < 1) return;
    if (move === 'fanfire' && player.bullets < 2) return;
    if (move === 'load' && player.bullets >= MAX_BULLETS) return;

    player.move = move;
    this.sendTo(id, { type: 'move_ack', move });

    if (game.host.move && game.guest.move) {
      this.clearTurnTimer(room);
      this.resolveRound(room);
    }
  }

  resolveRound(room) {
    const game = room.game;
    const hMove = game.host.move;
    const gMove = game.guest.move;

    applyAmmo(game.host, hMove);
    applyAmmo(game.guest, gMove);

    if (hMove === 'shoot' && gMove === 'shoot') {
      this.startShootout(room);
      return;
    }

    const winner = winningMove(hMove, gMove);
    let hostPoint = 0;
    let guestPoint = 0;

    if (hMove !== gMove) {
      if (winner === hMove && isShotMove(hMove)) hostPoint = 1;
      else if (winner === gMove && isShotMove(gMove)) guestPoint = 1;
    }

    this.finishRound(room, hostPoint, guestPoint, {
      hostMove: hMove,
      guestMove: gMove,
      hostPoint,
      guestPoint,
    });
  }

  startShootout(room) {
    const roundId = `${room.game.round}-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
    const greenAt = Date.now() + 1200 + Math.floor(Math.random() * 1800);

    room.game.duel = {
      roundId,
      greenAt,
      hostClick: null,
      guestClick: null,
    };

    this.armTurnTimer(room);
    this.broadcastRoom(room.id, { type: 'shootout_start', roundId, greenAt });
  }

  handleDuelClick(id, msg) {
    const { room, role } = this.getRoomAndRole(id);
    if (!room || !role || !room.game.duel) return;

    const duel = room.game.duel;
    if (msg.roundId !== duel.roundId) return;

    const reactionMs = Number.isFinite(Number(msg.reactionMs)) ? Number(msg.reactionMs) : 9999;
    const click = {
      falseStart: !!msg.falseStart,
      reactionMs: Math.max(0, Math.min(9999, reactionMs)),
      at: Date.now(),
    };

    if (role === 'host' && !duel.hostClick) duel.hostClick = click;
    if (role === 'guest' && !duel.guestClick) duel.guestClick = click;

    if (duel.hostClick && duel.guestClick) {
      this.clearTurnTimer(room);
      this.resolveShootout(room);
    }
  }

  resolveShootout(room) {
    const duel = room.game.duel;
    if (!duel) return;

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

    room.game.duel = null;

    this.finishRound(room, hostPoint, guestPoint, {
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

  finishRound(room, hostPoint, guestPoint, round) {
    const game = room.game;
    game.host.score += hostPoint;
    game.guest.score += guestPoint;
    game.host.move = null;
    game.guest.move = null;
    game.round += 1;

    const over = game.host.score >= ROUNDS_TO_WIN || game.guest.score >= ROUNDS_TO_WIN || game.round > TOTAL_ROUNDS;

    this.broadcastRoom(room.id, {
      type: over ? 'game_over' : 'round_result',
      round,
      game: publicGame(game),
    });

    if (over) {
      this.clearTurnTimer(room);
    } else {
      this.armTurnTimer(room);
    }
  }

  armTurnTimer(room) {
    this.clearTurnTimer(room);
    room.turnTimer = setTimeout(() => {
      this.handleRoundInactivity(room.id).catch(() => {});
    }, ROUND_INACTIVITY_MS);
  }

  clearTurnTimer(room) {
    if (!room?.turnTimer) return;
    clearTimeout(room.turnTimer);
    room.turnTimer = null;
  }

  async handleRoundInactivity(roomId) {
    const room = this.rooms.get(roomId);
    if (!room) return;

    const game = room.game;
    const hostInactive = game.duel ? !game.duel.hostClick : !game.host.move;
    const guestInactive = game.duel ? !game.duel.guestClick : !game.guest.move;
    if (!hostInactive && !guestInactive) {
      this.armTurnTimer(room);
      return;
    }

    this.clearTurnTimer(room);

    const round = {
      hostMove: game.host.move || 'dodge',
      guestMove: game.guest.move || 'dodge',
      hostPoint: 0,
      guestPoint: 0,
      inactive: hostInactive && guestInactive ? 'both' : hostInactive ? 'host' : 'guest',
    };

    if (hostInactive && !guestInactive) {
      game.guest.score = ROUNDS_TO_WIN;
      round.guestPoint = 1;
    } else if (guestInactive && !hostInactive) {
      game.host.score = ROUNDS_TO_WIN;
      round.hostPoint = 1;
    }

    this.broadcastRoom(room.id, {
      type: 'game_over',
      round,
      game: publicGame(game),
    });

    const host = this.clients.get(room.hostId);
    const guest = this.clients.get(room.guestId);

    await this.recordMatch({
      matchId: room.id,
      mode: 'online',
      player: host?.name || 'Host',
      playerHandle: host?.handle || sanitizeHandle(host?.name || 'host'),
      opponent: guest?.name || 'Guest',
      opponentHandle: guest?.handle || sanitizeHandle(guest?.name || 'guest'),
      hostHandle: host?.handle || sanitizeHandle(host?.name || 'host'),
      guestHandle: guest?.handle || sanitizeHandle(guest?.name || 'guest'),
      myScore: game.host.score,
      oppScore: game.guest.score,
      inactive: round.inactive,
    });

    if (host) {
      host.roomId = null;
      host.role = null;
    }
    if (guest) {
      guest.roomId = null;
      guest.role = null;
    }

    this.rooms.delete(room.id);
  }

  async getProfilesMap() {
    if (this.profilesCache) return this.profilesCache;
    const map = new Map();
    const list = await this.state.storage.list({ prefix: 'profile:' });
    for (const [, value] of list) {
      if (!value) continue;
      const handle = sanitizeHandle(value.handle || value.name || '');
      if (!handle) continue;
      if (!value.name) value.name = handle;
      value.handle = handle;
      map.set(handle, value);
    }
    this.profilesCache = map;
    return map;
  }

  async getOrCreateProfile(identity) {
    const desiredHandle = sanitizeHandle(identity?.handle || identity?.name || '');
    const desiredName = sanitizeName(identity?.name || '') || 'Outlaw';
    if (!desiredHandle) return null;

    const profiles = await this.getProfilesMap();
    let profile = profiles.get(desiredHandle);
    if (!profile) {
      profile = {
        handle: desiredHandle,
        name: desiredName,
        rating: DEFAULT_RATING,
        wins: 0,
        games: 0,
        coins: 0,
        nameChangedAt: Date.now(),
        nameLockedUntil: 0,
        updatedAt: Date.now(),
      };
      profiles.set(desiredHandle, profile);
      await this.state.storage.put(`profile:${desiredHandle}`, profile);
    }

    return profile;
  }

  async applyNameChange(profile, requestedName) {
    const safe = sanitizeName(requestedName || '');
    if (!safe || safe === profile.name) {
      return { changed: false };
    }

    const now = Date.now();
    const nextAllowed = (profile.nameChangedAt || 0) + NAME_CHANGE_COOLDOWN_MS;
    if (profile.name && now < nextAllowed) {
      profile.nameLockedUntil = nextAllowed;
      return { changed: false };
    }

    profile.name = safe;
    profile.nameChangedAt = now;
    profile.nameLockedUntil = 0;
    return { changed: true };
  }

  async saveProfile(profile) {
    if (!Number.isFinite(Number(profile.coins))) profile.coins = 0;
    profile.updatedAt = Date.now();
    const profiles = await this.getProfilesMap();
    profiles.set(profile.handle, profile);
    await this.state.storage.put(`profile:${profile.handle}`, profile);
  }

  async recordMatch(data) {
    const mode = data.mode === 'online' ? 'online' : 'ai';
    const matchId = `${mode}:${String(data.matchId || '').slice(0, 80)}`;

    if (!data.matchId) return { ok: false, error: 'Missing matchId' };
    const exists = await this.state.storage.get(`match:${matchId}`);
    if (exists) return { ok: true, payload: { deduped: true } };

    const player = await this.getOrCreateProfile({
      handle: data.playerHandle || data.player || '',
      name: data.player || '',
    });
    if (!player) return { ok: false, error: 'Invalid player' };

    if (mode === 'ai') {
      const outcome = normalizeOutcome(data.outcome);
      if (!outcome) return { ok: false, error: 'Invalid outcome' };

      const aiRating = 1000;
      const score = outcome === 'win' ? 1 : outcome === 'loss' ? 0 : 0.5;
      const k = 16;
      const expected = expectedScore(player.rating, aiRating);

      player.rating = Math.max(FLOOR_RATING, Math.round(player.rating + k * (score - expected)));
      player.games += 1;
      if (outcome === 'win') player.wins += 1;

      await this.saveProfile(player);
      await this.state.storage.put(`match:${matchId}`, Date.now());

      return { ok: true, payload: { mode, profiles: [player] } };
    }

    const opponent = await this.getOrCreateProfile({
      handle: data.opponentHandle || data.opponent || '',
      name: data.opponent || '',
    });
    if (!opponent) return { ok: false, error: 'Invalid opponent' };
    if (opponent.handle === player.handle) return { ok: false, error: 'Player and opponent must differ' };

    const myScore = Number(data.myScore);
    const oppScore = Number(data.oppScore);
    if (!Number.isFinite(myScore) || !Number.isFinite(oppScore)) {
      return { ok: false, error: 'Scores are required' };
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
    const oldPlayerRating = player.rating;
    const oldOpponentRating = opponent.rating;
    const expectedA = expectedScore(player.rating, opponent.rating);
    const expectedB = expectedScore(opponent.rating, player.rating);

    player.rating = Math.max(FLOOR_RATING, Math.round(player.rating + k * (scoreA - expectedA)));
    opponent.rating = Math.max(FLOOR_RATING, Math.round(opponent.rating + k * (scoreB - expectedB)));

    if (data.inactive === 'host' || data.inactive === 'guest') {
      const hostHandle = sanitizeHandle(data.hostHandle || '');
      const guestHandle = sanitizeHandle(data.guestHandle || '');
      const inactiveHandle = data.inactive === 'host' ? hostHandle : guestHandle;
      if (inactiveHandle) {
        if (player.handle === inactiveHandle) {
          const normalLoss = Math.max(0, oldPlayerRating - player.rating);
          player.rating = Math.max(FLOOR_RATING, player.rating - normalLoss);
        } else if (opponent.handle === inactiveHandle) {
          const normalLoss = Math.max(0, oldOpponentRating - opponent.rating);
          opponent.rating = Math.max(FLOOR_RATING, opponent.rating - normalLoss);
        }
      }
    }

    player.games += 1;
    opponent.games += 1;
    if (scoreA === 1) player.wins += 1;
    if (scoreB === 1) opponent.wins += 1;

    await this.saveProfile(player);
    await this.saveProfile(opponent);
    await this.state.storage.put(`match:${matchId}`, Date.now());

    return { ok: true, payload: { mode, profiles: [player, opponent] } };
  }
}

function sanitizeName(name) {
  return String(name || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[^a-zA-Z0-9 _-]/g, '')
    .slice(0, 20);
}

function sanitizeHandle(handle) {
  return String(handle || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '')
    .slice(0, 12);
}

function sanitizeChat(text) {
  return String(text || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, 220);
}

function expectedScore(rating, oppRating) {
  return 1 / (1 + Math.pow(10, (oppRating - rating) / 400));
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, value));
}

function normalizeOutcome(outcome) {
  if (outcome === 'win' || outcome === 'loss' || outcome === 'draw') return outcome;
  return null;
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

function isShotMove(move) {
  return move === 'shoot' || move === 'fanfire';
}

function winningMove(a, b) {
  if (a === b) return null;
  const beats = {
    shoot: ['load', 'aim'],
    dodge: ['shoot'],
    load: ['dodge'],
    aim: ['dodge', 'load'],
    fanfire: ['dodge', 'aim', 'load'],
  };

  if (beats[a]?.includes(b)) return a;
  if (beats[b]?.includes(a)) return b;
  return null;
}
