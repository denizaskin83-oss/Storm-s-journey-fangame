// Hazine Haritası – sunucu (Node.js, dış bağımlılık yok)
// Hesaplar, çevrimiçi listesi, davet, eşleşme ve oyuncu durumu aktarımı.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const GAME_FILE = path.join(__dirname, 'index.html');
const OWNER_ENV = process.env.OWNER_ID || '';   // Render'da kendi ID'ni buraya yaz

let db = { users: {}, order: [] };             // users[id] = {id,name,salt,hash,created}
try { db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) {}
function saveDb() { try { fs.writeFileSync(DATA_FILE, JSON.stringify(db)); } catch (e) {} }

const sessions = new Map();                    // token -> id
const presence = new Map();                    // id -> {lastSeen, matchId}
const streams = new Map();                     // id -> res (SSE)
const invites = new Map();                     // invId -> {id,from,to}
const matches = new Map();                     // matchId -> {id, players:[a,b], states:{}}

const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function newId() {
  let id; do { id = 'HT-'; for (let i = 0; i < 6; i++) id += ALPHA[crypto.randomInt(ALPHA.length)]; } while (db.users[id]);
  return id;
}
const PW_ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function genPassword() {
  let p = ''; for (let i = 0; i < 8; i++) { p += PW_ALPHA[crypto.randomInt(PW_ALPHA.length)]; if (i === 3) p += '-'; }
  return p;
}
function findByName(name) {
  const n = name.trim().toLowerCase();
  return Object.values(db.users).find((u) => u.name.toLowerCase() === n) || null;
}
function hashPw(pw, salt) { return crypto.scryptSync(pw, salt, 32).toString('hex'); }
function ownerId() { return OWNER_ENV || db.order[0] || null; }
function publicUser(id) { const u = db.users[id]; return u ? { id, name: u.name, owner: id === ownerId() } : null; }

function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve) => {
    let b = ''; req.on('data', (c) => { b += c; if (b.length > 1e6) req.destroy(); });
    req.on('end', () => { try { resolve(b ? JSON.parse(b) : {}); } catch (e) { resolve({}); } });
  });
}
function authId(req, url) {
  const h = req.headers.authorization || '';
  const tok = h.startsWith('Bearer ') ? h.slice(7) : (url.searchParams.get('token') || '');
  return sessions.get(tok) || null;
}
function push(id, event, data) {
  const res = streams.get(id);
  if (res) res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}
function online() {
  const now = Date.now(), out = [];
  for (const [id, p] of presence) if (now - p.lastSeen < 20000 && db.users[id]) out.push({ id, name: db.users[id].name, busy: !!p.matchId });
  return out;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') return send(res, 204, {});

  // game page
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return fs.createReadStream(GAME_FILE).pipe(res);
  }

  // accounts
  if (req.method === 'POST' && url.pathname === '/api/register') {
    const b = await readBody(req);
    const name = String(b.name || '').trim().slice(0, 16);
    if (name.length < 2) return send(res, 400, { error: 'İsim en az 2 karakter olmalı' });
    if (findByName(name)) return send(res, 400, { error: 'Bu isim alınmış, başka bir isim dene' });
    const id = newId(), salt = crypto.randomBytes(16).toString('hex'), pw = genPassword();
    db.users[id] = { id, name, salt, hash: hashPw(pw, salt), created: Date.now() };
    db.order.push(id); saveDb();
    const token = crypto.randomBytes(24).toString('hex'); sessions.set(token, id);
    return send(res, 200, { token, user: publicUser(id), password: pw });
  }
  if (req.method === 'POST' && url.pathname === '/api/login') {
    const b = await readBody(req);
    const u = findByName(String(b.name || ''));
    if (!u || hashPw(String(b.password || '').trim().toUpperCase(), u.salt) !== u.hash) return send(res, 401, { error: 'İsim veya şifre yanlış' });
    const token = crypto.randomBytes(24).toString('hex'); sessions.set(token, u.id);
    return send(res, 200, { token, user: publicUser(u.id) });
  }

  // everything below needs a session
  const me = authId(req, url);
  if (!me && url.pathname.startsWith('/api/') && url.pathname !== '/api/events') return send(res, 401, { error: 'Giriş gerekli' });

  if (req.method === 'GET' && url.pathname === '/api/me') return send(res, 200, { user: publicUser(me) });

  if (req.method === 'POST' && url.pathname === '/api/heartbeat') {
    const p = presence.get(me) || { lastSeen: 0, matchId: null };
    p.lastSeen = Date.now(); presence.set(me, p);
    return send(res, 200, { ok: true });
  }
  if (req.method === 'GET' && url.pathname === '/api/online') {
    return send(res, 200, { users: online().filter((u) => u.id !== me) });
  }

  // live events (server -> browser)
  if (req.method === 'GET' && url.pathname === '/api/events') {
    const id = authId(req, url);
    if (!id) return send(res, 401, { error: 'Giriş gerekli' });
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'Access-Control-Allow-Origin': '*' });
    res.write('retry: 2000\n\n');
    streams.set(id, res);
    const ka = setInterval(() => res.write(': ping\n\n'), 15000);
    req.on('close', () => { clearInterval(ka); if (streams.get(id) === res) streams.delete(id); });
    return;
  }

  // invites
  if (req.method === 'POST' && url.pathname === '/api/invite') {
    const b = await readBody(req);
    const to = String(b.to || '');
    if (!db.users[to] || to === me) return send(res, 400, { error: 'Oyuncu bulunamadı' });
    const p = presence.get(to);
    if (!p || Date.now() - p.lastSeen > 20000) return send(res, 400, { error: 'Oyuncu çevrimiçi değil' });
    if (p.matchId) return send(res, 400, { error: 'Oyuncu başka bir maçta' });
    const invId = crypto.randomBytes(6).toString('hex');
    invites.set(invId, { id: invId, from: me, to });
    push(to, 'invite', { id: invId, from: publicUser(me) });
    return send(res, 200, { id: invId });
  }
  if (req.method === 'POST' && url.pathname === '/api/invite/respond') {
    const b = await readBody(req);
    const inv = invites.get(b.id);
    if (!inv || inv.to !== me) return send(res, 400, { error: 'Davet bulunamadı' });
    invites.delete(b.id);
    if (!b.accept) { push(inv.from, 'declined', { by: publicUser(me) }); return send(res, 200, { ok: true }); }
    if (presence.get(inv.from)?.matchId || presence.get(me)?.matchId) return send(res, 400, { error: 'Maç kurulamadı' });
    const matchId = crypto.randomBytes(6).toString('hex');
    const m = { id: matchId, players: [inv.from, me], states: {} };
    matches.set(matchId, m);
    for (const pid of m.players) { const p = presence.get(pid) || { lastSeen: Date.now() }; p.matchId = matchId; presence.set(pid, p); }
    for (const pid of m.players) {
      const partner = m.players.find((x) => x !== pid);
      push(pid, 'match', { matchId, partner: publicUser(partner) });
    }
    return send(res, 200, { matchId });
  }

  // match: state relay
  if (req.method === 'POST' && url.pathname === '/api/match/state') {
    const b = await readBody(req);
    const m = matches.get(b.matchId);
    if (!m || !m.players.includes(me)) return send(res, 400, { error: 'Maç yok' });
    m.states[me] = { x: +b.x || 0, y: +b.y || 0, face: b.face > 0 ? 1 : -1, mv: !!b.mv, ph: +b.ph || 0, on: !!b.on, hp: +b.hp || 0, t: Date.now() };
    const partner = m.players.find((x) => x !== me);
    push(partner, 'partner', m.states[me]);
    return send(res, 200, { ok: true });
  }
  if (req.method === 'POST' && url.pathname === '/api/match/leave') {
    const b = await readBody(req);
    const m = matches.get(b.matchId);
    if (m && m.players.includes(me)) {
      for (const pid of m.players) { const p = presence.get(pid); if (p) p.matchId = null; if (pid !== me) push(pid, 'matchEnd', { by: publicUser(me) }); }
      matches.delete(m.id);
    }
    return send(res, 200, { ok: true });
  }

  send(res, 404, { error: 'Bulunamadı' });
});

server.listen(PORT, () => console.log('Hazine Haritası sunucusu: ' + PORT));
