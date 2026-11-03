import 'dotenv/config';
import http from 'http';
import crypto from 'crypto';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const LOG_PASSWORD = process.env.LOG_PASSWORD || 'changeme';
const MAX_LOG_LINES = 2000;

// ═══════════════════════════════════════════════
//  LOG BUFFER
// ═══════════════════════════════════════════════
const logLines = [];

function pushLog(line) {
  const ts = new Date().toISOString();
  logLines.push({ ts, text: line });
  if (logLines.length > MAX_LOG_LINES) logLines.shift();
}

// ═══════════════════════════════════════════════
//  SESSION TOKENS (in-memory, no cookies lib needed)
// ═══════════════════════════════════════════════
const sessions = new Map(); // token -> expiry timestamp
const SESSION_TTL = 24 * 60 * 60 * 1000; // 24h

function createSession() {
  const token = crypto.randomBytes(32).toString('hex');
  sessions.set(token, Date.now() + SESSION_TTL);
  return token;
}

function isValidSession(token) {
  if (!token || !sessions.has(token)) return false;
  if (Date.now() > sessions.get(token)) {
    sessions.delete(token);
    return false;
  }
  return true;
}

function getTokenFromReq(req) {
  const cookies = (req.headers.cookie || '').split(';');
  for (const c of cookies) {
    const [k, v] = c.trim().split('=');
    if (k === 'sxtoken') return v;
  }
  return null;
}

// ═══════════════════════════════════════════════
//  START BOT PROCESS
// ═══════════════════════════════════════════════
function startBot() {
  pushLog('--- Starting auto-bet daemon ---');
  const bot = spawn('node', [join(__dirname, 'auto-bet.mjs')], {
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  bot.stdout.on('data', (data) => {
    const lines = data.toString().split('\n').filter(l => l.trim());
    for (const l of lines) {
      pushLog(l);
      process.stdout.write(l + '\n');
    }
  });

  bot.stderr.on('data', (data) => {
    const lines = data.toString().split('\n').filter(l => l.trim());
    for (const l of lines) {
      pushLog('[ERR] ' + l);
      process.stderr.write(l + '\n');
    }
  });

  bot.on('exit', (code) => {
    pushLog(`--- Bot exited (code ${code}), restarting in 30s ---`);
    setTimeout(() => startBot(), 30000);
  });
}

// ═══════════════════════════════════════════════
//  HTML PAGES
// ═══════════════════════════════════════════════
function loginPage(error = '') {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SX Bot - Login</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:#0a0a0f; color:#e0e0e0; font-family:'Segoe UI',sans-serif;
         display:flex; align-items:center; justify-content:center; min-height:100vh; }
  .card { background:#12121a; border:1px solid #1e1e2e; border-radius:16px;
          padding:40px; width:360px; box-shadow:0 8px 32px rgba(0,0,0,.6); }
  h1 { font-size:20px; text-align:center; margin-bottom:8px; color:#a78bfa; }
  .sub { text-align:center; color:#666; font-size:13px; margin-bottom:28px; }
  label { display:block; font-size:13px; color:#888; margin-bottom:6px; }
  input { width:100%; padding:12px 14px; border:1px solid #2a2a3a; border-radius:10px;
          background:#0d0d14; color:#fff; font-size:15px; outline:none; }
  input:focus { border-color:#a78bfa; }
  button { width:100%; padding:12px; margin-top:18px; border:none; border-radius:10px;
           background:linear-gradient(135deg,#7c3aed,#a78bfa); color:#fff; font-size:15px;
           font-weight:600; cursor:pointer; transition:opacity .2s; }
  button:hover { opacity:.85; }
  .err { color:#f87171; text-align:center; font-size:13px; margin-top:12px; }
</style>
</head>
<body>
<div class="card">
  <h1>🔒 SX Bet Bot</h1>
  <p class="sub">Enter password to view logs</p>
  <form method="POST" action="/login">
    <label>Password</label>
    <input type="password" name="password" autofocus autocomplete="off">
    <button type="submit">Unlock</button>
  </form>
  ${error ? `<p class="err">${error}</p>` : ''}
</div>
</body>
</html>`;
}

function logsPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SX Bot - Live Logs</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:#0a0a0f; color:#c8c8d0; font-family:'JetBrains Mono','Fira Code','Consolas',monospace;
         font-size:13px; }
  .topbar { position:fixed; top:0; left:0; right:0; z-index:10;
            background:rgba(10,10,15,.92); backdrop-filter:blur(12px);
            border-bottom:1px solid #1e1e2e; padding:12px 20px;
            display:flex; align-items:center; justify-content:space-between; }
  .topbar h1 { font-size:16px; color:#a78bfa; font-weight:600; }
  .topbar .info { display:flex; gap:16px; align-items:center; }
  .badge { padding:4px 10px; border-radius:20px; font-size:11px; font-weight:600; }
  .badge.live { background:#065f4620; color:#34d399; border:1px solid #34d39940; }
  .badge.count { background:#7c3aed20; color:#a78bfa; bder:1px solid #a78bfa40; }
  .btn-logout { padding:6px 14px; border-radius:8px; border:1px solid #333;
                background:transparent; color:#888; font-size:12px; cursor:pointer; }
  .btn-logout:hover { border-color:#f87171; color:#f87171; }
  #logs { padding:60px 20px 20px; white-space:pre-wrap; word-break:break-all; line-height:1.7; }
  .log-line { padding:2px 0; border-bottom:1px solid #ffffff06; }
  .log-ts { color:#555; margin-right:8px; }
  .log-text { color:#d1d5db; }
  .log-line:has(.log-text[data-err]) .log-text { color:#f87171; }
  .auto-scroll { position:fixed; bottom:20px; right:20px; padding:8px 16px;
                 border-radius:10px; background:#7c3aed; color:#fff; border:none;
                 font-size:12px; cursor:pointer; display:none; z-index:10; }
</style>
</head>
<body>
<div class="topbar">
  <h1>⚡ SX Bot Logs</h1>
  <div class="info">
    <span class="badge live">● LIVE</span>
    <span class="badge count" id="countBadge">0 lines</span>
    <button class="btn-logout" onclick="location.href='/logout'">Logout</button>
  </div>
</div>
<div id="logs"></div>
<button class="auto-scroll" id="scrollBtn" onclick="scrollToBottom()">↓ Scroll to bottom</button>
<script>
  const logsDiv = document.getElementById('logs');
  const countBadge = document.getElementById('countBadge');
  const scrollBtn = document.getElementById('scrollBtn');
  let lastIdx = 0;
  let autoScroll = true;

  window.addEventListener('scroll', () => {
    const atBottom = (window.innerHeight + window.scrollY) >= document.body.scrollHeight - 50;
    autoScroll = atBottom;
    scrollBtn.style.display = atBottom ? 'none' : 'block';
  });

  function scrollToBottom() {
    window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
    autoScroll = true;
    scrollBtn.style.display = 'none';
  }

  function esc(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  async function poll() {
    try {
      const res = await fetch('/api/logs?from=' + lastIdx);
      if (res.status === 401) { location.href = '/'; return; }
      const data = await res.json();
      if (data.lines && data.lines.length) {
        let html = '';
        for (const l of data.lines) {
          const errAttr = l.text.startsWith('[ERR]') ? ' data-err' : '';
          html += '<div class="log-line"><span class="log-ts">' + esc(l.ts.substring(11,19))
                + '</span><span class="log-text"' + errAttr + '>' + esc(l.text) + '</span></div>';
        }
        logsDiv.insertAdjacentHTML('beforeend', html);
        lastIdx = data.nextIdx;
        countBadge.textContent = lastIdx + ' lines';
        if (autoScroll) scrollToBottom();
      }
    } catch {}
    setTimeout(poll, 2000);
  }
  poll();
</script>
</body>
</html>`;
}

// ═══════════════════════════════════════════════
//  HTTP SERVER
// ═══════════════════════════════════════════════
function parseBody(req) {
  return new Promise((resolve) => {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      const params = {};
      for (const pair of body.split('&')) {
        const [k, v] = pair.split('=').map(decodeURIComponent);
        if (k) params[k] = v || '';
      }
      resolve(params);
    });
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname;

  // --- POST /login ---
  if (path === '/login' && req.method === 'POST') {
    const body = await parseBody(req);
    if (body.password === LOG_PASSWORD) {
      const token = createSession();
      res.writeHead(302, {
        'Set-Cookie': `sxtoken=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400`,
        'Location': '/logs',
      });
      res.end();
    } else {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(loginPage('Wrong password'));
    }
    return;
  }

  // --- GET /logout ---
  if (path === '/logout') {
    const token = getTokenFromReq(req);
    if (token) sessions.delete(token);
    res.writeHead(302, {
      'Set-Cookie': 'sxtoken=; Path=/; Max-Age=0',
      'Location': '/',
    });
    res.end();
    return;
  }

  // --- GET / (login page) ---
  if (path === '/' || path === '/login') {
    const token = getTokenFromReq(req);
    if (isValidSession(token)) {
      res.writeHead(302, { 'Location': '/logs' });
      res.end();
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(loginPage());
    return;
  }

  // --- Everything below requires auth ---
  const token = getTokenFromReq(req);
  if (!isValidSession(token)) {
    if (path.startsWith('/api/')) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end('{"error":"unauthorized"}');
    } else {
      res.writeHead(302, { 'Location': '/' });
      res.end();
    }
    return;
  }

  // --- GET /logs ---
  if (path === '/logs') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(logsPage());
    return;
  }

  // --- GET /api/logs?from=N ---
  if (path === '/api/logs') {
    const from = parseInt(url.searchParams.get('from') || '0');
    const lines = logLines.slice(from);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ lines, nextIdx: logLines.length }));
    return;
  }

  // --- 404 ---
  res.writeHead(404);
  res.end('not found');
});

// ═══════════════════════════════════════════════
//  START
// ═══════════════════════════════════════════════
server.listen(PORT, () => {
  console.log(`[server] Listening on port ${PORT}`);
  console.log(`[server] Log viewer at http://localhost:${PORT}`);
  startBot();
});
