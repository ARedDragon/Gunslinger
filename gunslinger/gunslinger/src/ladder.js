const DEFAULT_RATING = 1000;
const FLOOR_RATING = 100;

export class Ladder {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === '/api/leaderboard' && request.method === 'GET') {
      const limit = clamp(parseInt(url.searchParams.get('limit') || '20', 10), 1, 50);
      const leaderboard = await this.getLeaderboard(limit);
      return Response.json({ leaderboard });
    }

    if (url.pathname === '/api/profile' && request.method === 'GET') {
      const name = sanitizeName(url.searchParams.get('name') || '');
      if (!name) return Response.json({ error: 'Invalid name' }, { status: 400 });
      const profile = await this.getProfile(name);
      return Response.json({ profile });
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

  async recordMatch(data) {
    const mode = data.mode === 'online' ? 'online' : 'ai';
    const matchId = `${mode}:${String(data.matchId || '').slice(0, 80)}`;

    if (!data.matchId) return { ok: false, error: 'Missing matchId' };
    if (await this.state.storage.get(`match:${matchId}`)) {
      return { ok: true, payload: { deduped: true } };
    }

    const player = sanitizeName(data.player || '');
    if (!player) return { ok: false, error: 'Invalid player name' };

    if (mode === 'ai') {
      const outcome = normalizeOutcome(data.outcome);
      if (!outcome) return { ok: false, error: 'Invalid outcome' };
      const updated = await this.updateSingleVsAI(player, outcome);
      await this.state.storage.put(`match:${matchId}`, Date.now());
      return { ok: true, payload: { mode, profiles: [updated] } };
    }

    const opponent = sanitizeName(data.opponent || '');
    if (!opponent) return { ok: false, error: 'Invalid opponent name' };
    if (opponent === player) return { ok: false, error: 'Player and opponent must differ' };

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

    const profileA = await this.getProfile(player);
    const profileB = await this.getProfile(opponent);

    const k = 24;
    const expectedA = expectedScore(profileA.rating, profileB.rating);
    const expectedB = expectedScore(profileB.rating, profileA.rating);

    profileA.rating = Math.max(FLOOR_RATING, Math.round(profileA.rating + k * (scoreA - expectedA)));
    profileB.rating = Math.max(FLOOR_RATING, Math.round(profileB.rating + k * (scoreB - expectedB)));

    profileA.games += 1;
    profileB.games += 1;
    if (scoreA === 1) profileA.wins += 1;
    if (scoreB === 1) profileB.wins += 1;

    await this.saveProfile(profileA);
    await this.saveProfile(profileB);
    await this.state.storage.put(`match:${matchId}`, Date.now());

    return { ok: true, payload: { mode, profiles: [profileA, profileB] } };
  }

  async updateSingleVsAI(name, outcome) {
    const profile = await this.getProfile(name);
    const aiRating = 1000;
    const k = 16;

    const score = outcome === 'win' ? 1 : outcome === 'loss' ? 0 : 0.5;
    const expected = expectedScore(profile.rating, aiRating);

    profile.rating = Math.max(FLOOR_RATING, Math.round(profile.rating + k * (score - expected)));
    profile.games += 1;
    if (outcome === 'win') profile.wins += 1;

    await this.saveProfile(profile);
    return profile;
  }

  async getProfile(name) {
    const safe = sanitizeName(name);
    const key = profileKey(safe);
    const existing = await this.state.storage.get(key);
    if (existing) return existing;

    return {
      name: safe,
      rating: DEFAULT_RATING,
      wins: 0,
      games: 0,
      updatedAt: Date.now(),
    };
  }

  async saveProfile(profile) {
    profile.updatedAt = Date.now();
    await this.state.storage.put(profileKey(profile.name), profile);
  }

  async getLeaderboard(limit) {
    const list = [];
    const iter = this.state.storage.list({ prefix: 'profile:' });
    for await (const [, value] of iter) {
      list.push(value);
    }

    list.sort((a, b) => {
      if (b.rating !== a.rating) return b.rating - a.rating;
      if (b.wins !== a.wins) return b.wins - a.wins;
      return a.name.localeCompare(b.name);
    });

    return list.slice(0, limit);
  }
}

function expectedScore(rating, opponentRating) {
  return 1 / (1 + Math.pow(10, (opponentRating - rating) / 400));
}

function profileKey(name) {
  return `profile:${name}`;
}

function sanitizeName(name) {
  return String(name || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[^a-zA-Z0-9 _-]/g, '')
    .slice(0, 20);
}

function normalizeOutcome(outcome) {
  if (outcome === 'win' || outcome === 'loss' || outcome === 'draw') return outcome;
  return null;
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, value));
}
