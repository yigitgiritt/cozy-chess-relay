// Cozy Chess — oda kodlu aracı sunucu (WebSocket).
// Oyunu bilmez: iki oyuncuyu bir oda kodunda buluşturur ve mesajlarını birbirine taşır.
// Hamlelerin kurallara uygunluğunu oyunun kendisi kontrol eder.
//
// Çalıştırma:  npm install  &&  npm start      (PORT ortam değişkeni, varsayılan 8080)
//
// İstemci -> sunucu (JSON):
//   {t:"create", cfg:{...}}            oda kur (cfg: renk, süre, varyant — karşı tarafa iletilir)
//   {t:"join", code:"K7RM2"}            odaya katıl
//   {t:"rejoin", code, token}           bağlantı koptuktan sonra aynı yere geri dön
//   {t:"relay", d:{...}}                rakibe ilet
//   {t:"leave"}                         odadan çık
//   {t:"quick", cfg:{...}}              hızlı eşleşme: rastgele bir rakip bekle (cfg: süre/varyant isteği)
//   {t:"quick_cancel"}                  beklemeyi bırak
//   {t:"ping"}
// Sunucu -> istemci:
//   {t:"created", code, token}  {t:"joined", code, token, cfg}  {t:"peer_joined"}
//   {t:"waiting", n}  {t:"matched", code, token, role, cfg}   (hızlı eşleşme)
//   {t:"peer_left"}  {t:"peer_back"}  {t:"rejoined", role}  {t:"msg", d}  {t:"error", reason}  {t:"pong"}

const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 8080;
const ALPHABET = "ABCDEFGHJKLMNPRSTUVYZ23456789"; // karışmasın diye O/0, I/1 yok
const ROOM_TTL_MS = 30 * 60 * 1000; // iki oyuncu da yoksa oda 30 dk sonra silinir
const MAX_MSG = 8 * 1024;
const MAX_ROOMS = 5000;

const rooms = new Map(); // code -> {cfg, host:{ws,token}, guest:{ws,token}, empty_since}
let queue = []; // hızlı eşleşme bekleyenler: [{ws, cfg, since}]

function unqueue(ws) {
  queue = queue.filter((q) => q.ws !== ws);
}

// Hızlı eşleşme: kuyrukta başka biri varsa ikisi için oda kurulur. Süre ve tür, önce bekleyenin
// isteğine göre olur; renkler rastgele dağıtılır.
function quickMatch(ws, cfg) {
  unqueue(ws);
  detach(ws, true);
  queue = queue.filter((q) => q.ws.readyState === 1);
  const mate = queue.shift();
  if (!mate) {
    queue.push({ ws, cfg: cfg || {}, since: Date.now() });
    return send(ws, { t: "waiting", n: queue.length });
  }
  const code = newCode();
  if (!code || rooms.size >= MAX_ROOMS) {
    queue.unshift(mate);
    return send(ws, { t: "error", reason: "full" });
  }
  const pref = mate.cfg || {};
  const roomCfg = {
    host_color: crypto.randomInt(2) === 0 ? 1 : -1,
    minutes: Number(pref.minutes) || 0,
    variant: String(pref.variant || ""),
    seed: crypto.randomInt(1000000),
    quick: true,
  };
  const ht = crypto.randomBytes(12).toString("hex");
  const gt = crypto.randomBytes(12).toString("hex");
  rooms.set(code, { cfg: roomCfg, host: { ws: mate.ws, token: ht }, guest: { ws, token: gt }, empty_since: 0 });
  mate.ws.code = code;
  mate.ws.role = "host";
  ws.code = code;
  ws.role = "guest";
  send(mate.ws, { t: "matched", code, token: ht, role: "host", cfg: roomCfg });
  send(ws, { t: "matched", code, token: gt, role: "guest", cfg: roomCfg });
}

function newCode() {
  for (let tries = 0; tries < 50; tries++) {
    let c = "";
    for (let i = 0; i < 5; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function other(room, role) {
  return role === "host" ? room.guest : room.host;
}

function detach(ws, notify) {
  const room = rooms.get(ws.code);
  if (!room) return;
  const seat = room[ws.role];
  if (seat && seat.ws === ws) {
    seat.ws = null;
    const o = other(room, ws.role);
    if (notify && o && o.ws) send(o.ws, { t: "peer_left" });
    if (!room.host.ws && !(room.guest && room.guest.ws)) room.empty_since = Date.now();
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Cozy Chess relay calisiyor. Odalar: " + rooms.size + " · Bekleyen: " + queue.length + "\n");
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });

wss.on("connection", (ws) => {
  ws.code = null;
  ws.role = null;
  ws.alive = true;
  ws.on("pong", () => (ws.alive = true));

  ws.on("message", (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch {
      return send(ws, { t: "error", reason: "bad_json" });
    }
    ws.alive = true;
    switch (m.t) {
      case "ping":
        return send(ws, { t: "pong" });
      case "create": {
        if (rooms.size >= MAX_ROOMS) return send(ws, { t: "error", reason: "full" });
        detach(ws, true);
        const code = newCode();
        if (!code) return send(ws, { t: "error", reason: "full" });
        const token = crypto.randomBytes(12).toString("hex");
        rooms.set(code, { cfg: m.cfg || {}, host: { ws, token }, guest: null, empty_since: 0 });
        ws.code = code;
        ws.role = "host";
        return send(ws, { t: "created", code, token });
      }
      case "join": {
        const code = String(m.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
        const room = rooms.get(code);
        if (!room) return send(ws, { t: "error", reason: "no_room" });
        if (room.guest) return send(ws, { t: "error", reason: "room_full" });
        detach(ws, true);
        const token = crypto.randomBytes(12).toString("hex");
        room.guest = { ws, token };
        ws.code = code;
        ws.role = "guest";
        send(ws, { t: "joined", code, token, cfg: room.cfg });
        return send(room.host.ws, { t: "peer_joined" });
      }
      case "rejoin": {
        const room = rooms.get(String(m.code || ""));
        if (!room) return send(ws, { t: "error", reason: "no_room" });
        for (const role of ["host", "guest"]) {
          const seat = room[role];
          if (seat && seat.token === m.token) {
            if (seat.ws && seat.ws !== ws) seat.ws.terminate();
            seat.ws = ws;
            ws.code = m.code;
            ws.role = role;
            room.empty_since = 0;
            send(ws, { t: "rejoined", role, peer: !!(other(room, role) && other(room, role).ws) });
            const o = other(room, role);
            if (o && o.ws) send(o.ws, { t: "peer_back" });
            return;
          }
        }
        return send(ws, { t: "error", reason: "bad_token" });
      }
      case "relay": {
        const room = rooms.get(ws.code);
        if (!room) return send(ws, { t: "error", reason: "no_room" });
        const o = other(room, ws.role);
        if (o && o.ws) send(o.ws, { t: "msg", d: m.d });
        return;
      }
      case "quick":
        return quickMatch(ws, m.cfg);
      case "quick_cancel":
        unqueue(ws);
        return;
      case "leave": {
        unqueue(ws);
        const room = rooms.get(ws.code);
        if (room) {
          const o = other(room, ws.role);
          if (o && o.ws) send(o.ws, { t: "peer_left", final: true });
          rooms.delete(ws.code);
        }
        ws.code = null;
        ws.role = null;
        return;
      }
    }
  });

  ws.on("close", () => {
    unqueue(ws);
    detach(ws, true);
  });
  ws.on("error", () => {});
});

// Ölü bağlantıları ve boş odaları temizle
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) {
      ws.terminate();
      continue;
    }
    ws.alive = false;
    ws.ping();
  }
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (room.empty_since && now - room.empty_since > ROOM_TTL_MS) rooms.delete(code);
  }
}, 30000);

server.listen(PORT, () => console.log("Cozy Chess relay: port " + PORT));
