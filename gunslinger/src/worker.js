export { Matchmaker } from './matchmaker.js';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/ws/queue' || url.pathname.startsWith('/api/')) {
      const id = env.MATCHMAKER.idFromName('global-matchmaker');
      const stub = env.MATCHMAKER.get(id);
      return stub.fetch(request);
    }

    return env.ASSETS.fetch(request);
  },
};
