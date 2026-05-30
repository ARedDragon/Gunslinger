// src/worker.js
// Cloudflare Worker + Durable Objects for real-time Gunslinger multiplayer

export { Room } from './room.js';
export { Ladder } from './ladder.js';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // WebSocket upgrade for game rooms
    if (url.pathname.startsWith('/ws/')) {
      const code = url.pathname.slice(4).toUpperCase();
      if (!code || code.length !== 4) {
        return new Response('Invalid room code', { status: 400 });
      }
      const id = env.ROOMS.idFromName(code);
      const room = env.ROOMS.get(id);
      return room.fetch(request);
    }

    // Leaderboard + ratings API (single global DO)
    if (url.pathname.startsWith('/api/leaderboard') || url.pathname.startsWith('/api/profile') || url.pathname === '/api/match') {
      const id = env.LADDER.idFromName('global-ladder');
      const ladder = env.LADDER.get(id);
      return ladder.fetch(request);
    }

    // REST: create a room code
    if (url.pathname === '/api/create' && request.method === 'POST') {
      const code = genCode();
      return Response.json({ code });
    }

    // Serve static files (the HTML frontend)
    return env.ASSETS.fetch(request);
  }
};

function genCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({ length: 4 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}
