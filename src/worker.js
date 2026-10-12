// Los Purebreds chat relay.
//
// One Durable Object per room keeps every browser connected over WebSocket.
// The first browser to connect is the "host" (the page's own host logic keeps
// running in that browser: passwords, bans, DM routing...). Everyone else talks
// to the host through this relay, so nobody needs to reach the host's network.
// If the host leaves, everyone else is dropped; the page auto-reconnects and the
// first one back becomes the new host.
//
// Frames (plain text, no JSON parsing here so big images stay cheap):
//   server -> client : "c{json}" control   | "d<payload>" data from the host
//   server -> host   : "c{json}" control   | "o<cid>" client joined | "x<cid>" client left
//                      "d<cid>\n<payload>" data from that client
//   client -> server : "<payload>"
//   host   -> server : "d<cid>\n<payload>" to one client | "k<cid>" disconnect that client
import { DurableObject } from "cloudflare:workers";

const MAX_FRAME = 1_000_000;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected a WebSocket", { status: 426 });
      }
      const room = (url.searchParams.get("room") || "main").replace(/[^\w-]/g, "").slice(0, 64) || "main";
      return env.ROOM.get(env.ROOM.idFromName(room)).fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // keep-alive pings are answered without waking the object
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  fetch() {
    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);
    const cid = crypto.randomUUID().replace(/-/g, "").slice(0, 10);
    const host = this.hostSocket(server);
    if (host) {
      server.serializeAttachment({ cid, host: false });
      server.send("c" + JSON.stringify({ k: "hello", cid, host: false }));
      host.send("o" + cid);
    } else {
      server.serializeAttachment({ cid, host: true });
      server.send("c" + JSON.stringify({ k: "hello", cid, host: true }));
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  hostSocket(except) {
    return this.ctx.getWebSockets().find((s) => s !== except && s.deserializeAttachment()?.host);
  }

  webSocketMessage(ws, message) {
    if (typeof message !== "string" || message.length > MAX_FRAME) return;
    const me = ws.deserializeAttachment();
    if (!me) return;
    if (!me.host) {
      const host = this.hostSocket(ws);
      if (host) host.send("d" + me.cid + "\n" + message);
      return;
    }
    const tag = message[0];
    if (tag === "d") {
      const nl = message.indexOf("\n");
      if (nl < 0) return;
      const target = this.byCid(message.slice(1, nl));
      if (target) target.send("d" + message.slice(nl + 1));
    } else if (tag === "k") {
      const target = this.byCid(message.slice(1));
      if (target) {
        const t = target.deserializeAttachment();
        // mark it handled so the close event that follows doesn't report it a second time
        target.serializeAttachment({ ...t, gone: true });
        try { target.close(4001, "removed"); } catch {}
        ws.send("x" + t.cid);
      }
    }
  }

  byCid(cid) {
    return this.ctx.getWebSockets().find((s) => s.deserializeAttachment()?.cid === cid);
  }

  webSocketClose(ws) { this.gone(ws); }
  webSocketError(ws) { this.gone(ws); }

  gone(ws) {
    const me = ws.deserializeAttachment();
    if (!me || me.gone) return;
    ws.serializeAttachment({ ...me, gone: true });
    if (me.host) {
      // host left: drop everyone so they reconnect and the first one back becomes host
      for (const s of this.ctx.getWebSockets()) {
        if (s !== ws) { try { s.close(1012, "host left"); } catch {} }
      }
    } else {
      const host = this.hostSocket(ws);
      if (host) { try { host.send("x" + me.cid); } catch {} }
    }
    try { ws.close(1000, "bye"); } catch {}
  }
}
