# Gunslinger: Frontier Legends

A real-time 1v1 multiplayer duel game built on **Node.js + WebSockets**.

## Project structure

```
gunslinger/
├── public/
│   └── index.html      ← Frontend (served as static asset)
├── server.js           ← Node server (HTTP APIs + WebSockets + static files)
├── src/
│   ├── worker.js       ← Legacy Cloudflare worker entry (unused)
│   └── room.js         ← Legacy durable object code (unused)
└── package.json
```

## Prerequisites

- [Node.js](https://nodejs.org/) 18+
- Node.js 18+

---

## Quick start in VS Code

### 1. Install dependencies

Open a terminal in VS Code (`Ctrl+`` ` or `Terminal → New Terminal`):

```bash
npm install
```

### 2. Run locally

```bash
npm run dev
```

Node starts a local server at **http://localhost:8787**.  
Open two browser tabs to test multiplayer locally.

---

## Deploy

Deploy this as a standard Node web app on platforms that support `npm install` + `npm start`.

Set `PORT` if your platform requires a specific listener port.

---

## How multiplayer works

1. Click **Ranked Queue** to join matchmaking (no room codes)
2. Server pairs players by closest rating, with wider matching over queue wait time
3. Matched players connect to an isolated in-memory duel room and play in real time

You can also see current connected players via `/api/online`.

---

## Game rules

| Move    | Beats                | Cost |
|---------|----------------------|------|
| Load    | Dodge                | free (+1 bullet) |
| Shoot   | Load, Aim            | 1 bullet |
| Dodge   | Shoot, Fanfire       | free |
| Aim     | Dodge, Load          | free |
| Fanfire | Shoot, Load, Aim     | 2 bullets |

- Max 4 bullets per player
- Both players always see each other's ammo count
- Best of 5 rounds (first to 3 wins)
- Can't Shoot with 0 bullets; can't Fanfire with fewer than 2 bullets; can't Load at max ammo

## Progression systems

- Leaderboard: global leaderboard from server memory (`/api/leaderboard`)
- Rating system: Elo-style rating updates after every match (`/api/match`, `/api/profile`)
- Shop: local coin economy with unlockable themes and cosmetics in the main menu

## Backend notes

- `server.js` now provides all APIs and WebSocket room logic used by the frontend.
- Legacy Cloudflare files in `src/` are left in place for reference and can be removed.

---

## Note on persistence

Leaderboard/rating data is in memory in this Node version and resets when the server restarts.
