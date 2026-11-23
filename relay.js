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
const MAX_MSG = 16 * 1024; // ortak kampta sesli konuşma paketleri için biraz geniş
const MAX_ROOMS = 5000;

const rooms = new Map(); // code -> {cfg, host:{ws,token}, guest:{ws,token}, empty_since}

// ---------------------------------------------------------------- yazışmalı (yavaş) oyunlar
// Hamleni yap, oyunu kapat; rakip ne zaman açarsa kendi hamlesini yapar. Oyunlar kalıcı depoda durur
// (Upstash Redis, REST). Depo ayarlı değilse bellekte tutulur ve "persistent:false" bildirilir: oyun bu
// durumda özelliği göstermez (ücretsiz sunucu uyuyunca bellekteki oyunlar silinir).
//   {t:"corr_hello"} -> {t:"corr_info", persistent}
//   {t:"corr_new", pid, name, variant, color}  {t:"corr_join", pid, name, code}  -> {t:"corr_game", game}
//   {t:"corr_list", pid} -> {t:"corr_list", games}
//   {t:"corr_move", pid, gid, ply, move:[from,to,promo]}  {t:"corr_end", pid, gid, result} -> {t:"corr_game", game}
//   {t:"corr_drop", pid, gid}  (biten oyunu listeden kaldır) -> {t:"corr_list", games}
// pid: oyuncunun kendi cihazında üretilen gizli kimlik; başka oyunculara asla gönderilmez.
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || "").replace(/\/$/, "");
const UP_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || "";
const PERSISTENT = !!(UP_URL && UP_TOKEN);
const CORR_TTL = 60 * 60 * 24 * 45; // 45 gün hiç hamle yapılmayan oyun silinir
const CORR_MAX_GAMES = 30; // bir oyuncunun aynı anda en çok bu kadar yavaş oyunu
const mem = new Map();

async function kvGet(k) {
  if (!PERSISTENT) return mem.has(k) ? mem.get(k) : null;
  const r = await fetch(UP_URL + "/get/" + encodeURIComponent(k), { headers: { Authorization: "Bearer " + UP_TOKEN } });
  const j = await r.json();
  return j.result == null ? null : JSON.parse(j.result);
}

async function kvSet(k, v) {
  if (!PERSISTENT) return void mem.set(k, v);
  await fetch(UP_URL, {
    method: "POST",
    headers: { Authorization: "Bearer " + UP_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(["SET", k, JSON.stringify(v), "EX", String(CORR_TTL)]),
  });
}

const validPid = (p) => typeof p === "string" && /^[a-f0-9]{16,64}$/.test(p);
const cleanName = (n) => String(n || "").replace(/[\u0000-\u001f]/g, "").slice(0, 20);

function pubGame(g, pid) {
  const color = g.white && g.white.pid === pid ? 1 : g.black && g.black.pid === pid ? -1 : 0;
  return {
    gid: g.gid, code: g.code, variant: g.variant, seed: g.seed, color,
    white: g.white ? g.white.name : "", black: g.black ? g.black.name : "",
    moves: g.moves, over: g.over || "", updated: g.updated,
  };
}

async function myGames(pid) {
  const ids = (await kvGet("p:" + pid)) || [];
  const out = [];
  const keep = [];
  for (const gid of ids) {
    const g = await kvGet("g:" + gid);
    if (!g) continue;
    keep.push(gid);
    out.push(pubGame(g, pid));
  }
  if (keep.length !== ids.length) await kvSet("p:" + pid, keep);
  return out;
}

async function addToList(pid, gid) {
  const ids = (await kvGet("p:" + pid)) || [];
  if (!ids.includes(gid)) ids.push(gid);
  await kvSet("p:" + pid, ids.slice(-CORR_MAX_GAMES));
}

async function newCorrCode() {
  for (let tries = 0; tries < 30; tries++) {
    let c = "";
    for (let i = 0; i < 6; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!(await kvGet("c:" + c))) return c;
  }
  return null;
}

async function handleCorr(ws, m) {
  if (m.t === "corr_hello") return send(ws, { t: "corr_info", persistent: PERSISTENT });
  const pid = m.pid;
  if (!validPid(pid)) return send(ws, { t: "error", reason: "bad_pid" });
  switch (m.t) {
    case "corr_new": {
      const mine = (await kvGet("p:" + pid)) || [];
      if (mine.length >= CORR_MAX_GAMES) return send(ws, { t: "error", reason: "too_many" });
      const code = await newCorrCode();
      if (!code) return send(ws, { t: "error", reason: "full" });
      const gid = crypto.randomBytes(8).toString("hex");
      let color = Number(m.color);
      if (color !== 1 && color !== -1) color = crypto.randomInt(2) === 0 ? 1 : -1;
      const seat = { pid, name: cleanName(m.name) };
      const g = {
        gid, code, variant: String(m.variant || "").slice(0, 12), seed: crypto.randomInt(1000000),
        white: color === 1 ? seat : null, black: color === -1 ? seat : null,
        moves: [], over: "", created: Date.now(), updated: Date.now(),
      };
      await kvSet("g:" + gid, g);
      await kvSet("c:" + code, gid);
      await addToList(pid, gid);
      return send(ws, { t: "corr_game", game: pubGame(g, pid) });
    }
    case "corr_join": {
      const code = String(m.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
      const gid = await kvGet("c:" + code);
      const g = gid ? await kvGet("g:" + gid) : null;
      if (!g) return send(ws, { t: "error", reason: "no_game" });
      const mineSeat = (g.white && g.white.pid === pid) || (g.black && g.black.pid === pid);
      if (!mineSeat) {
        if (g.white && g.black) return send(ws, { t: "error", reason: "room_full" });
        const seat = { pid, name: cleanName(m.name) };
        if (!g.white) g.white = seat;
        else g.black = seat;
        g.updated = Date.now();
        await kvSet("g:" + gid, g);
        await addToList(pid, gid);
      }
      return send(ws, { t: "corr_game", game: pubGame(g, pid) });
    }
    case "corr_list":
      return send(ws, { t: "corr_list", games: await myGames(pid) });
    case "corr_drop": {
      const ids = ((await kvGet("p:" + pid)) || []).filter((x) => x !== String(m.gid || ""));
      await kvSet("p:" + pid, ids);
      return send(ws, { t: "corr_list", games: await myGames(pid) });
    }
    case "corr_move":
    case "corr_end": {
      const g = await kvGet("g:" + String(m.gid || ""));
      if (!g) return send(ws, { t: "error", reason: "no_game" });
      const color = g.white && g.white.pid === pid ? 1 : g.black && g.black.pid === pid ? -1 : 0;
      if (!color) return send(ws, { t: "error", reason: "not_player" });
      if (m.t === "corr_end") {
        if (!g.over) {
          g.over = String(m.result || "bitti").slice(0, 60);
          g.updated = Date.now();
          await kvSet("g:" + g.gid, g);
        }
        return send(ws, { t: "corr_game", game: pubGame(g, pid) });
      }
      const turn = g.moves.length % 2 === 0 ? 1 : -1;
      const mv = Array.isArray(m.move) ? m.move.map((x) => Number(x) | 0) : [];
      const okMove = mv.length === 3 && mv[0] >= 0 && mv[0] < 64 && mv[1] >= 0 && mv[1] < 64;
      if (g.over || turn !== color || Number(m.ply) !== g.moves.length || !okMove || !(g.white && g.black)) {
        return send(ws, { t: "corr_game", game: pubGame(g, pid), stale: true });
      }
      g.moves.push(mv);
      g.updated = Date.now();
      await kvSet("g:" + g.gid, g);
      return send(ws, { t: "corr_game", game: pubGame(g, pid) });
    }
  }
}
// ---------------------------------------------------------------- ortak kamp (en çok 4 kişi)
// Kampı açan oyuncu (koltuk 0) ev sahibidir; kamp onun kaydından kurulur. Sunucu yalnızca koltukları tutar ve
// mesajları taşır. Oda kodu 6 harftir (iki kişilik oyun odaları 5 harf), menüdeki aynı kutuya yazılabilir.
//   {t:"camp_create", info}               -> {t:"camp_created", code, token, seat:0}
//   {t:"camp_join", code, info, cid}       -> {t:"camp_joined", code, token, seat, peers:[{seat, info, on}]}
//   {t:"camp_rejoin", code, token}         -> {t:"camp_rejoined", seat, peers}
//   {t:"camp_send", d, to?}                -> diğerlerine (ya da yalnız "to" koltuğuna) {t:"camp_msg", from, d}
//   {t:"camp_info", info}                  -> herkese {t:"camp_peer", seat, info, on:true}
//   {t:"camp_kick", seat}                  (yalnız ev sahibi) -> atılana {t:"camp_kicked"}, diğerlerine {t:"camp_left", seat, kicked:true}
//   {t:"camp_leave"}                       misafir: {t:"camp_left", seat}; ev sahibi: herkese {t:"camp_closed"}
//   {t:"camp_pub", pub|null}               (yalnız ev sahibi) Yardım Panosu'na as / indir
//   {t:"camp_cfg", lim, open}             (yalnız ev sahibi) oyuncu sınırı (2-4) ve Açık Kamplar listesinde görünme
//                                          (open: {nick, camp, lvl} | null)
//   {t:"camp_list"}                        -> {t:"camp_list", rooms:[{code, pub, open, n, lim}]} (odada olmak gerekmez)
// Bağlantısı kopan koltuk 90 sn tutulur ({t:"camp_peer", seat, on:false}); dönmezse boşalır. Ev sahibi dönmezse
// kamp kapanır. Atılan oyuncu (cid) o kampa 2 dakika giremez ({t:"error", reason:"banned", wait:sn}).
const CAMP_SEATS = 4;
const CAMP_BAN_MS = Number(process.env.CAMP_BAN_MS) || 2 * 60 * 1000; // (testler kısaltabilir)
const CAMP_GRACE_MS = 90 * 1000;
const CAMP_RATE = 80; // bir bağlantının saniyede en çok mesajı (konum + ses + sohbet)
const camps = new Map(); // code -> {seats:[{ws, token, info, cid, gone_at}|null], banned:Set, empty_since}

function newCampCode() {
  for (let tries = 0; tries < 50; tries++) {
    let c = "";
    for (let i = 0; i < 6; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!camps.has(c) && !rooms.has(c)) return c;
  }
  return null;
}

const cleanInfo = (i) => {
  // oyuncunun kampta görünen bilgisi: takma ad, kedinin adı, süsleri, seviyesi (küçük tutulur)
  try {
    const s = JSON.stringify(i || {});
    return s.length <= 1200 ? JSON.parse(s) : {};
  } catch {
    return {};
  }
};

function campPeers(room) {
  const out = [];
  room.seats.forEach((s, i) => {
    if (s) out.push({ seat: i, info: s.info, on: !!s.ws });
  });
  return out;
}

function campBroadcast(room, obj, except) {
  for (const s of room.seats) if (s && s.ws && s.ws !== except) send(s.ws, obj);
}

function campDetach(ws, why) {
  const room = camps.get(ws.camp);
  if (!room) {
    ws.camp = null;
    return;
  }
  const seat = ws.seat;
  const s = room.seats[seat];
  if (s && s.ws === ws) {
    if (why === "leave") {
      if (seat === 0) {
        campBroadcast(room, { t: "camp_closed" }, ws);
        camps.delete(ws.camp);
      } else {
        room.seats[seat] = null;
        campBroadcast(room, { t: "camp_left", seat });
      }
    } else {
      s.ws = null;
      s.gone_at = Date.now();
      campBroadcast(room, { t: "camp_peer", seat, info: s.info, on: false });
    }
  }
  ws.camp = null;
  ws.seat = -1;
}

function campRate(ws) {
  const now = Date.now();
  if (now - (ws.rate_at || 0) > 1000) {
    ws.rate_at = now;
    ws.rate_n = 0;
  }
  ws.rate_n = (ws.rate_n || 0) + 1;
  return ws.rate_n <= CAMP_RATE;
}

function handleCamp(ws, m) {
  switch (m.t) {
    case "camp_create": {
      if (camps.size >= MAX_ROOMS) return send(ws, { t: "error", reason: "full" });
      campDetach(ws, "leave");
      detach(ws, true);
      const code = newCampCode();
      if (!code) return send(ws, { t: "error", reason: "full" });
      const token = crypto.randomBytes(12).toString("hex");
      const seats = new Array(CAMP_SEATS).fill(null);
      seats[0] = { ws, token, info: cleanInfo(m.info), cid: String(m.cid || ""), gone_at: 0 };
      camps.set(code, { seats, banned: new Map(), lim: CAMP_SEATS, open: null, empty_since: 0 });
      ws.camp = code;
      ws.seat = 0;
      return send(ws, { t: "camp_created", code, token, seat: 0 });
    }
    case "camp_join": {
      const code = String(m.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
      const room = camps.get(code);
      if (!room || !room.seats[0]) return send(ws, { t: "error", reason: "no_room" });
      const cid = String(m.cid || "");
      if (cid && room.banned.has(cid)) {
        const left = room.banned.get(cid) - Date.now();
        if (left > 0) return send(ws, { t: "error", reason: "banned", wait: Math.ceil(left / 1000) });
        room.banned.delete(cid);
      }
      const free = room.seats.findIndex((s, i) => i > 0 && !s);
      const taken = room.seats.filter((s) => s).length;
      if (free < 0 || taken >= room.lim) return send(ws, { t: "error", reason: "room_full", lim: room.lim });
      campDetach(ws, "leave");
      detach(ws, true);
      const token = crypto.randomBytes(12).toString("hex");
      room.seats[free] = { ws, token, info: cleanInfo(m.info), cid, gone_at: 0 };
      ws.camp = code;
      ws.seat = free;
      send(ws, { t: "camp_joined", code, token, seat: free, peers: campPeers(room) });
      return campBroadcast(room, { t: "camp_peer", seat: free, info: room.seats[free].info, on: true, fresh: true }, ws);
    }
    case "camp_list": {
      // Yardım Panosu (Kamp Yolu'nda yardım isteyenler) + Açık Kamplar (herkese açık kamplar); dolu olanlar yok, en yeni 40
      const out = [];
      for (const [code, room] of camps) {
        if ((!room.pub && !room.open) || !room.seats[0] || !room.seats[0].ws) continue;
        const n = room.seats.filter((s) => s).length;
        if (n >= room.lim) continue;
        out.push({ code, pub: room.pub || null, open: room.open || null, n, lim: room.lim,
          at: Math.max(room.pub ? room.pub_at || 0 : 0, room.open ? room.open_at || 0 : 0) });
      }
      out.sort((a, b) => b.at - a.at);
      return send(ws, { t: "camp_list", rooms: out.slice(0, 40).map(({ at, ...r }) => r) });
    }
    case "camp_rejoin": {
      const room = camps.get(String(m.code || ""));
      if (!room) return send(ws, { t: "error", reason: "no_room" });
      const seat = room.seats.findIndex((s) => s && s.token === m.token);
      if (seat < 0) return send(ws, { t: "error", reason: "bad_token" });
      const s = room.seats[seat];
      if (s.ws && s.ws !== ws) s.ws.terminate();
      s.ws = ws;
      s.gone_at = 0;
      ws.camp = String(m.code);
      ws.seat = seat;
      send(ws, { t: "camp_rejoined", seat, peers: campPeers(room) });
      return campBroadcast(room, { t: "camp_peer", seat, info: s.info, on: true, back: true }, ws);
    }
  }
  // aşağıdakiler bir kampta oturmayı gerektirir
  const room = camps.get(ws.camp);
  if (!room) return send(ws, { t: "error", reason: "no_room" });
  switch (m.t) {
    case "camp_send": {
      if (!campRate(ws)) return;
      const out = { t: "camp_msg", from: ws.seat, d: m.d };
      if (Number.isInteger(m.to)) {
        const s = room.seats[m.to];
        if (s && s.ws && m.to !== ws.seat) send(s.ws, out);
        return;
      }
      return campBroadcast(room, out, ws);
    }
    case "camp_info": {
      const s = room.seats[ws.seat];
      if (!s) return;
      s.info = cleanInfo(m.info);
      return campBroadcast(room, { t: "camp_peer", seat: ws.seat, info: s.info, on: true }, ws);
    }
    case "camp_pub": {
      // ev sahibi kampını Yardım Panosu'na asar (pub: {nick, level, char, mode}) ya da indirir (pub: null)
      if (ws.seat !== 0) return;
      room.pub = m.pub ? cleanInfo(m.pub) : null;
      room.pub_at = Date.now();
      return;
    }
    case "camp_cfg": {
      // ev sahibi: oyuncu sınırı (2-4; içerideki kimse atılmaz, yalnız yeni giriş kapanır) ve Açık Kamplar
      if (ws.seat !== 0) return;
      const lim = Number(m.lim);
      if (Number.isInteger(lim) && lim >= 2 && lim <= CAMP_SEATS) room.lim = lim;
      const was = !!room.open;
      room.open = m.open ? cleanInfo(m.open) : null;
      if (room.open && !was) room.open_at = Date.now();
      return;
    }
    case "camp_kick": {
      if (ws.seat !== 0) return;
      const k = Number(m.seat);
      const s = room.seats[k];
      if (!(k > 0) || !s) return;
      if (s.cid) room.banned.set(s.cid, Date.now() + CAMP_BAN_MS);
      room.seats[k] = null;
      if (s.ws) {
        send(s.ws, { t: "camp_kicked" });
        s.ws.camp = null;
        s.ws.seat = -1;
      }
      return campBroadcast(room, { t: "camp_left", seat: k, kicked: true });
    }
    case "camp_leave":
      return campDetach(ws, "leave");
  }
}

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
  res.end("Cozy Chess relay calisiyor. Odalar: " + rooms.size + " · Kamplar: " + camps.size + " · Bekleyen: " + queue.length +
    " · Yavas oyun deposu: " + (PERSISTENT ? "bagli" : "yok") + "\n");
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
    if (typeof m.t === "string" && m.t.startsWith("camp_")) return handleCamp(ws, m);
    if (typeof m.t === "string" && m.t.startsWith("corr_")) {
      handleCorr(ws, m).catch((e) => {
        console.error("corr", e);
        send(ws, { t: "error", reason: "store" });
      });
      return;
    }
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
    campDetach(ws, "lost");
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
  campSweep(now);
}, 30000);

// Kopup geri dönmeyen kamp oyuncularını çıkar; ev sahibi dönmediyse kampı kapat.
function campSweep(now) {
  for (const [code, room] of camps) {
    room.seats.forEach((s, i) => {
      if (!camps.has(code) || !s || s.ws || !s.gone_at || now - s.gone_at < CAMP_GRACE_MS) return;
      if (i === 0) {
        campBroadcast(room, { t: "camp_closed" });
        for (const o of room.seats) if (o && o.ws) o.ws.camp = null;
        camps.delete(code);
      } else {
        room.seats[i] = null;
        campBroadcast(room, { t: "camp_left", seat: i });
      }
    });
  }
}
setInterval(() => campSweep(Date.now()), 10000);

server.listen(PORT, () => console.log("Cozy Chess relay: port " + PORT));
