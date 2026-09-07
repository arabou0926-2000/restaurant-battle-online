import { DurableObject } from "cloudflare:workers";

const MAX_PEERS = 20;
const MAX_MESSAGE = 200_000;
const ROOM_RE = /^[A-Za-z0-9_-]{1,40}$/;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      return new Response("Restaurant Battle signaling server OK", {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    // Public room directory
    if (url.pathname === "/api/rooms" && request.method === "GET") {
      const id = env.DIRECTORY.idFromName("global");
      return env.DIRECTORY.get(id).fetch(new Request("https://directory/rooms"));
    }

    const m = url.pathname.match(/^\/room\/([A-Za-z0-9_-]{1,40})$/);
    if (m) {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("WebSocket Upgrade required", { status: 426 });
      }
      const roomId = m[1];
      const id = env.ROOMS.idFromName(roomId);
      return env.ROOMS.get(id).fetch(request);
    }

    // Serve the actual game from /public instead of returning Hello World.
    return env.ASSETS.fetch(request);
  },
};

export class RestaurantBattleRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket Upgrade required", { status: 426 });
    }

    const sockets = this.ctx.getWebSockets();
    if (sockets.length >= MAX_PEERS) {
      return new Response("Room is full (max 20 players)", { status: 409 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    this.roomId = new URL(request.url).pathname.split("/")[2] || "";
    server.serializeAttachment({ id: crypto.randomUUID(), peerId: null, name: "", team: "A" });

    return new Response(null, { status: 101, webSocket: client });
  }

  updateDirectory() {
    if (!this.env?.DIRECTORY || !this.roomId) return;
    const sockets = this.ctx.getWebSockets();
    const players = sockets.length;
    const id = this.env.DIRECTORY.idFromName("global");
    this.env.DIRECTORY.get(id).fetch(new Request("https://directory/update", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        id: this.roomId,
        name: this.roomId,
        players,
        maxPlayers: MAX_PEERS,
        status: "waiting",
      }),
    })).catch(() => {});
  }

  broadcast(obj, except) {
    const raw = JSON.stringify(obj);
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) continue;
      try { ws.send(raw); } catch (_) {}
    }
  }

  safeMessage(message) {
    if (typeof message !== "string" || message.length > MAX_MESSAGE) return null;
    try { return JSON.parse(message); } catch (_) { return null; }
  }

  webSocketMessage(ws, message) {
    const m = this.safeMessage(message);
    if (!m || typeof m.type !== "string" || m.type.length > 32) return;
    const state = ws.deserializeAttachment() || {};

    if (m.type === "hello") {
      if (typeof m.peerId !== "string" || !/^[\w-]{8,100}$/.test(m.peerId)) return;
      const peers = this.ctx.getWebSockets()
        .map(s => s.deserializeAttachment())
        .filter(x => x && x.peerId && x.peerId !== m.peerId)
        .map(x => ({ id: x.peerId, name: x.name || "Player", team: x.team || "A" }));

      ws.serializeAttachment({
        id: state.id || crypto.randomUUID(),
        peerId: m.peerId,
        name: typeof m.name === "string" ? m.name.slice(0, 30) : "Player",
        team: m.team === "B" ? "B" : "A",
      });

      this.updateDirectory();

      try {
        ws.send(JSON.stringify({ type: "peer_list", peers }));
      } catch (_) {}
      this.broadcast({
        type: "peer_joined",
        peer: { id: m.peerId, name: typeof m.name === "string" ? m.name.slice(0, 30) : "Player", team: "A" },
      }, ws);
      return;
    }

    if (!state.peerId) return;

    if (m.type === "signal") {
      if (typeof m.to !== "string" || typeof m.from !== "string" || m.from !== state.peerId) return;
      const target = this.ctx.getWebSockets().find(s => s.deserializeAttachment()?.peerId === m.to);
      if (!target) return;
      if (!m.data || typeof m.data !== "object" || typeof m.data.kind !== "string") return;
      const payload = { type: "signal", from: state.peerId, to: m.to, data: m.data };
      try { target.send(JSON.stringify(payload)); } catch (_) {}
      return;
    }

    // Cloudflare is deliberately NOT a gameplay transport here.
    // Cursor/recipe/asset payloads stay on WebRTC DataChannels.
    if (m.type === "ping") {
      try { ws.send(JSON.stringify({ type: "pong", t: Date.now() })); } catch (_) {}
    }
  }

  webSocketClose(ws) {
    const state = ws.deserializeAttachment() || {};
    if (state.peerId) this.broadcast({ type: "peer_left", peerId: state.peerId }, ws);
    this.updateDirectory();
  }

  webSocketError(ws) {
    const state = ws.deserializeAttachment() || {};
    if (state.peerId) this.broadcast({ type: "peer_left", peerId: state.peerId }, ws);
    this.updateDirectory();
  }
}

export class RoomDirectory extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/rooms") {
      const rooms = await this.ctx.storage.list({ prefix: "room:" });
      const now = Date.now();
      const out = [];
      for (const [key, value] of rooms) {
        if (!value || now - (value.updatedAt || 0) > 30000) {
          await this.ctx.storage.delete(key);
          continue;
        }
        out.push(value);
      }
      out.sort((a, b) => (b.players || 0) - (a.players || 0));
      return Response.json({ rooms: out });
    }

    if (request.method === "POST" && url.pathname === "/update") {
      let data;
      try { data = await request.json(); } catch (_) {
        return new Response("Bad JSON", { status: 400 });
      }
      if (!data?.id || !ROOM_RE.test(String(data.id))) {
        return new Response("Bad room id", { status: 400 });
      }
      const players = Math.max(0, Math.min(MAX_PEERS, Number(data.players) || 0));
      const key = `room:${data.id}`;
      if (players === 0) {
        await this.ctx.storage.delete(key);
      } else {
        await this.ctx.storage.put(key, {
          id: String(data.id),
          name: String(data.name || data.id).slice(0, 40),
          players,
          maxPlayers: MAX_PEERS,
          status: data.status === "playing" ? "playing" : "waiting",
          updatedAt: Date.now(),
        });
      }
      return Response.json({ ok: true });
    }

    return new Response("Not found", { status: 404 });
  }
}
