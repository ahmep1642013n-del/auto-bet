#!/usr/bin/env node
/**
 * ============================================================
 *  SX Bet Auto-Betting Script (Daemon Mode)
 * ============================================================
 *  Runs continuously, checking Firebase every 10 minutes.
 *  Only processes NEW predictions not yet bet on.
 *
 *  Confidence-based bet sizing:
 *    75-79% -> 10% of balance
 *    80-84% -> 15% of balance
 *    >=85%  -> 25% of balance
 *
 *  Safety:
 *    - Max 5 bets per cycle
 *    - Max 60% of starting balance per day
 *    - Min reserve of 50 USDC always kept
 *    - Bet log file prevents duplicate bets across restarts
 *
 *  Usage:
 *    node auto-bet.mjs                 <- run daemon (real bets)
 *    node auto-bet.mjs --dry-run       <- run daemon (no real bets)
 *    node auto-bet.mjs --once          <- run once and exit
 *    node auto-bet.mjs --once --dry-run
 * ============================================================
 */

import 'dotenv/config';
import dns from 'dns';
dns.setServers(['8.8.8.8', '1.1.1.1']);
import { initializeApp, cert } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';
import { MongoClient } from 'mongodb';
import { Wallet, hexlify, randomBytes } from 'ethers';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import nodeFetch from 'node-fetch';
import { HttpsProxyAgent } from 'https-proxy-agent';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════
//  CONFIGURATION
// ═══════════════════════════════════════════════
const DRY_RUN = process.argv.includes('--dry-run');
const RUN_ONCE = process.argv.includes('--once');
const CHECK_INTERVAL_MS = 10 * 60 * 1000; // 10 minutes
const MAX_BETS_PER_CYCLE = 5;
const SX_API_BASE = 'https://api.sx.bet';
function cleanEnv(val) {
  if (!val) return '';
  let s = String(val).trim();
  if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
    s = s.slice(1, -1);
  }
  return s.trim();
}
const SX_API_KEY = cleanEnv(process.env.sx_bet_API || process.env.SX_BET_API);
const SX_PRIVATE_KEY = cleanEnv(process.env.PRIVATE_KEY);
const FETCH_TIMEOUT_MS = 15_000;
const MAX_SEARCH_RETRIES = 6; // Max times to retry "match not found" before giving up (6 × 10min = 1 hour)
const BET_LOG_FILE = join(__dirname, 'bet-history.json');

// ═══════════════════════════════════════════════
//  PROXY ROTATION FOR ORDER EXECUTION (Webshare)
// ═══════════════════════════════════════════════
const DEFAULT_PROXIES = [
  'http://bivwqmnj:6pssg3eaa1rx@64.137.10.153:5803', // Germany (Frankfurt)
  'http://bivwqmnj:6pssg3eaa1rx@45.38.107.97:6014',  // United Kingdom (London)
  'http://bivwqmnj:6pssg3eaa1rx@64.137.96.74:6641',  // Spain (Madrid)
  'http://bivwqmnj:6pssg3eaa1rx@84.247.60.125:6095', // Poland (Warsaw)
];

function getProxies() {
  const custom = cleanEnv(process.env.PROXY_URL || process.env.WEBSHARE_PROXY);
  if (custom) return [custom];
  const file = join(__dirname, 'Webshare 10 proxies.txt');
  if (existsSync(file)) {
    try {
      const lines = readFileSync(file, 'utf-8').split('\n').map(l => l.trim()).filter(Boolean);
      const parsed = lines.map(line => {
        const parts = line.split(':');
        if (parts.length === 4) {
          const [ip, port, user, pass] = parts;
          return `http://${user}:${pass}@${ip}:${port}`;
        }
        return line.startsWith('http') ? line : `http://${line}`;
      });
      // Prioritize allowed European jurisdictions (Germany, UK, Spain, Poland)
      const euroOrder = ['64.137.10.153', '45.38.107.97', '64.137.96.74', '84.247.60.125'];
      parsed.sort((a, b) => {
        const aIndex = euroOrder.findIndex(ip => a.includes(ip));
        const bIndex = euroOrder.findIndex(ip => b.includes(ip));
        if (aIndex !== -1 && bIndex !== -1) return aIndex - bIndex;
        if (aIndex !== -1) return -1;
        if (bIndex !== -1) return 1;
        return 0;
      });
      if (parsed.length) return parsed;
    } catch {}
  }
  return DEFAULT_PROXIES;
}

// ═══════════════════════════════════════════════
//  LOGGER
// ═══════════════════════════════════════════════
function log(emoji, ...args) {
  const time = new Date().toLocaleTimeString('en-US', { hour12: false });
  console.log(`[${time}] ${emoji}`, ...args);
}

// ═══════════════════════════════════════════════
//  MONGODB (State & History Persistence)
// ═══════════════════════════════════════════════
let mongoCol = null;

async function initMongo() {
  if (mongoCol) return mongoCol;
  const uri = cleanEnv(process.env.MONGODB_URI);
  if (!uri) throw new Error('Missing MONGODB_URI in .env');
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('water_football');
  mongoCol = db.collection('bot_state');

  // Migrate bet-history.json if Mongo collection is empty and local file exists
  try {
    const existing = await mongoCol.findOne({ _id: 'state' });
    if (!existing && existsSync(BET_LOG_FILE)) {
      const data = JSON.parse(readFileSync(BET_LOG_FILE, 'utf-8'));
      await mongoCol.updateOne(
        { _id: 'state' },
        {
          $set: {
            bets: data.bets || [],
            dailySpend: data.dailySpend || {},
            searchRetries: data.searchRetries || {},
            lastCleanup: data.lastCleanup || '',
            migratedAt: new Date()
          }
        },
        { upsert: true }
      );
      log('📦', 'Migrated bet-history.json to MongoDB successfully!');
    }
  } catch (e) {
    log('⚠️', `Migration check: ${e.message}`);
  }
  return mongoCol;
}

async function loadBetHistory(col) {
  try {
    const doc = await col.findOne({ _id: 'state' });
    if (doc) {
      return {
        bets: Array.isArray(doc.bets) ? doc.bets : [],
        events: Array.isArray(doc.events) ? doc.events : [],
        dailySpend: doc.dailySpend || {},
        searchRetries: doc.searchRetries || {},
        lastCleanup: doc.lastCleanup || ''
      };
    }
  } catch (e) {
    log('⚠️', `Failed to load history from Mongo: ${e.message}`);
  }
  return { bets: [], events: [], dailySpend: {}, lastCleanup: '', searchRetries: {} };
}

async function saveBetHistory(col, history) {
  try {
    await col.updateOne(
      { _id: 'state' },
      {
        $set: {
          bets: history.bets,
          events: history.events || [],
          dailySpend: history.dailySpend,
          searchRetries: history.searchRetries,
          lastCleanup: history.lastCleanup,
          updatedAt: new Date()
        }
      },
      { upsert: true }
    );
  } catch (e) {
    log('❌', `Failed to save history to Mongo: ${e.message}`);
  }
}

function getBetKey(date, matchIndex, prediction) {
  return `${date}|${matchIndex}|${prediction}`;
}

function wasBetPlaced(history, key) {
  return history.bets.includes(key);
}

async function recordBet(col, history, key, amount, eventId = null) {
  if (key && !history.bets.includes(key)) {
    history.bets.push(key);
  }
  if (!history.events) history.events = [];
  if (eventId && !history.events.includes(eventId)) {
    history.events.push(eventId);
    log('📌', `Recorded Event ID ${eventId} to prevent duplicate bets`);
  }
  const today = new Date().toISOString().split('T')[0];
  history.dailySpend[today] = (history.dailySpend[today] || 0) + amount;
  // Cleanup old entries (keep last 7 days)
  if (history.lastCleanup !== today) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 7);
    const cutoffStr = cutoff.toISOString().split('T')[0];
    history.bets = history.bets.filter(b => b >= cutoffStr);
    for (const d of Object.keys(history.dailySpend)) {
      if (d < cutoffStr) delete history.dailySpend[d];
    }
    // Limit event IDs history length
    if (history.events.length > 200) {
      history.events = history.events.slice(-100);
    }
    history.lastCleanup = today;
  }
  await saveBetHistory(col, history);
}

function getDailySpend(history) {
  const today = new Date().toISOString().split('T')[0];
  return history.dailySpend[today] || 0;
}

// ═══════════════════════════════════════════════
//  FIREBASE
// ═══════════════════════════════════════════════
let firebaseInitialized = false;

function initFirebase() {
  if (firebaseInitialized) return getFirestore();
  let raw = cleanEnv(process.env.FIREBASE_SERVICE_ACCOUNT);
  const sa = JSON.parse(raw);
  sa.private_key = sa.private_key.replace(/\\n/g, '\n');
  initializeApp({ credential: cert(sa) });
  firebaseInitialized = true;
  return getFirestore();
}

async function fetchTodayPredictions(db) {
  const today = new Date().toISOString().split('T')[0];
  log('📅', `Checking predictions for: ${today}`);

  const snap = await db.collection('ai_best_predictions')
    .where('apiDate', '==', today)
    .get();

  if (snap.empty) {
    log('💤', 'No predictions for today');
    return { today, preds: [] };
  }

  const preds = [];
  snap.forEach(doc => {
    const d = doc.data();
    if (Array.isArray(d.allAnalyses)) {
      for (const a of d.allAnalyses) preds.push(a);
    }
  });

  // Deduplicate: if the same match appears multiple times (from multiple prediction runs),
  // keep only the version with the highest confidence
  const deduped = new Map();
  for (const p of preds) {
    const lines = p.match.split('\n');
    const matchKey = lines[0].trim(); // "Team1 vs Team2"
    const conf = extractConfidence(p.analysis);
    const existing = deduped.get(matchKey);
    if (!existing || conf > existing.conf) {
      deduped.set(matchKey, { pred: p, conf });
    }
  }
  const uniquePreds = [...deduped.values()].map(v => v.pred);

  log('📊', `Found ${preds.length} total predictions (${uniquePreds.length} unique matches)`);
  return { today, preds: uniquePreds };
}

// ═══════════════════════════════════════════════
//  PREDICTION PARSING
// ═══════════════════════════════════════════════
function extractConfidence(analysis) {
  const matches = [...analysis.matchAll(/\[(\d+)%\]/g)];
  if (!matches.length) return 0;
  return parseInt(matches[matches.length - 1][1]);
}

function parsePrediction(pred) {
  const lines = pred.match.split('\n');
  const [team1, team2] = lines[0].split(' vs ').map(s => s.trim());
  const league = (lines.find(l => l.startsWith('League:')) || '').replace('League:', '').trim();
  const predLine = (lines.find(l => l.startsWith('Prediction:')) || '').replace('Prediction:', '').trim();
  const parts = predLine.split('|').map(s => s.trim());
  const prediction = parts[0] || '';
  const odds = parseFloat((parts[1] || '').replace('Odds:', '').trim()) || 0;
  const confidence = extractConfidence(pred.analysis);

  return { matchIndex: pred.matchIndex, team1, team2, league, prediction, odds, confidence };
}

function classifyPrediction(prediction) {
  const p = prediction.trim();

  // "Telekom -4.5 Wins" or "Barcelona +2.5 Win" → Asian Handicap
  const handicapRe = /^(.+?)\s+([+-]\d+\.?\d*)\s+Wins?$/i;
  const hm = p.match(handicapRe);
  if (hm) return { type: 'handicap', team: hm[1].trim(), line: parseFloat(hm[2]) };

  // "Ferencvaros Wins Or Draw" or "Bodo Glimt Win or Draw" -> double chance (1X)
  const dcRe = /^(.+?)\s+(?:Wins?\s+Or\s+Draw|Draw\s+Or\s+Wins?)$/i;
  const dc = p.match(dcRe);
  if (dc) return { type: 'double_chance', team: dc[1].trim() };

  // "Barcelona Wins" or "Barcelona Win" -> moneyline
  const mlRe = /^(.+?)\s+Wins?$/i;
  const ml = p.match(mlRe);
  if (ml) return { type: 'moneyline', team: ml[1].trim() };

  // "Over 2.5 Goals" or "Under 2.5 Goals" → match total
  const totalRe = /^(Over|Under) (\d+\.?\d*)\s*Goals?$/i;
  const tm = p.match(totalRe);
  if (tm) return { type: 'over_under', line: parseFloat(tm[2]), side: tm[1].toLowerCase() };

  // Both Teams To Score: Yes vs No
  if (/(?:both\s*teams?\s*(?:to\s*)?score|btts)/i.test(p)) {
    if (/\bno\b/i.test(p)) {
      return { type: 'btts_no' };
    }
    if (/\byes\b/i.test(p)) {
      return { type: 'btts_yes' };
    }
    return { type: 'unknown', raw: p };
  }

  // "Leverkusen Over 1.5 Goals" or "Leverkusen Under 1.5 Goals" → team-specific
  const teamOURe = /^(.+?)\s+(Over|Under)\s+(\d+\.?\d*)\s*Goals?$/i;
  const to = p.match(teamOURe);
  if (to) return { type: 'team_over', team: to[1].trim(), side: to[2].toLowerCase(), line: parseFloat(to[3]) };

  return { type: 'unknown', raw: p };
}

// ═══════════════════════════════════════════════
//  SX BET API HELPERS (with timeout + retry)
// ═══════════════════════════════════════════════
async function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    return res;
  } finally {
    clearTimeout(timer);
  }
}

async function sxGet(path, params = {}, auth = false, retries = 1) {
  const url = new URL(path, SX_API_BASE);
  for (const [k, v] of Object.entries(params)) {
    if (v != null) url.searchParams.set(k, String(v));
  }
  const headers = {};
  if (auth) headers['x-sx-api-key'] = SX_API_KEY;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetchWithTimeout(url, { headers });
      if (!res.ok) throw new Error(`SX GET ${path} -> ${res.status}: ${await res.text()}`);
      return res.json();
    } catch (e) {
      if (attempt < retries) {
        log('🔄', `Retry ${attempt + 1}/${retries} for ${path}: ${e.message}`);
        await sleep(2000);
      } else {
        throw e;
      }
    }
  }
}

async function sxPost(path, body) {
  const url = new URL(path, SX_API_BASE).toString();
  const headers = { 'Content-Type': 'application/json', 'x-sx-api-key': SX_API_KEY };
  const proxies = getProxies();

  // Route POST orders through European residential/datacenter proxy to avoid 403 Forbidden
  for (let i = 0; i < proxies.length; i++) {
    const proxyUrl = proxies[i];
    const proxyHost = proxyUrl.split('@')[1] || proxyUrl;
    try {
      log('🌐', `Routing POST ${path} through proxy #${i + 1} (${proxyHost})...`);
      const agent = new HttpsProxyAgent(proxyUrl);
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
      const res = await nodeFetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
        agent,
        signal: controller.signal,
      }).finally(() => clearTimeout(timer));

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(`SX POST ${path} -> ${res.status}: ${errText}`);
      }
      return await res.json();
    } catch (err) {
      log('⚠️', `Proxy #${i + 1} attempt error: ${err.message}`);
      if (i < proxies.length - 1) {
        log('🔄', `Failover to next proxy...`);
        await sleep(1000);
      }
    }
  }

  // Last-resort fallback: attempt direct connection
  log('⚠️', `All proxies exhausted, trying direct connection as fallback...`);
  const res = await fetchWithTimeout(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`SX POST ${path} -> ${res.status}: ${await res.text()}`);
  return res.json();
}

// ═══════════════════════════════════════════════
//  SX BET: BALANCE
// ═══════════════════════════════════════════════
async function getBalance() {
  const r = await sxGet('/user/balance-v3', {}, true);
  const balances = r.data?.balances || [];
  if (!balances.length) return { available: 0n, escrowed: 0n };
  let available = 0n;
  let escrowed = 0n;
  for (const b of balances) {
    available += BigInt(b.availableAmount || '0');
    escrowed += BigInt(b.escrowedAmount || '0');
  }
  return { available, escrowed };
}

// ═══════════════════════════════════════════════
//  SX BET: SEARCH FIXTURE
// ═══════════════════════════════════════════════
function normalise(s) { return s.toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim(); }

const COMMON_PREFIXES = new Set(['fc', 'sk', 'ac', 'as', 'cf', 'sc', 'cd', 'afk', 'fk', 'bk', 'de', 'la', 'el', 'al', 'cp']);

function teamsMatch(sxTeam, dbTeam) {
  const a = normalise(sxTeam);
  const b = normalise(dbTeam);
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;

  // Split into significant words (excluding short prefixes like SK, FC, AC)
  const wordsA = a.split(' ').filter(w => w.length >= 3 && !COMMON_PREFIXES.has(w));
  const wordsB = b.split(' ').filter(w => w.length >= 3 && !COMMON_PREFIXES.has(w));

  for (const wa of wordsA) {
    for (const wb of wordsB) {
      if (wa === wb) return true;
      if (wa.length >= 4 && wb.length >= 4 && (wa.includes(wb) || wb.includes(wa))) return true;
    }
  }

  return false;
}

async function searchFixture(team1, team2) {
  const terms = new Set();
  if (team1.length >= 3) terms.add(team1);
  if (team2.length >= 3) terms.add(team2);
  const w1 = team1.split(' ')[0]; if (w1.length >= 3) terms.add(w1);
  const w2 = team2.split(' ')[0]; if (w2.length >= 3) terms.add(w2);
  const c1 = team1.replace(/[^a-zA-Z0-9 ]/g, '').trim();
  const c2 = team2.replace(/[^a-zA-Z0-9 ]/g, '').trim();
  if (c1.length >= 3) terms.add(c1);
  if (c2.length >= 3) terms.add(c2);

  for (const q of terms) {
    try {
      log('🔍', `Searching SX Bet for: "${q}"`);
      const r = await sxGet('/search', { query: q });
      if (r.status !== 'success' || !r.data?.length) continue;

      for (const fix of r.data) {
        const m1 = teamsMatch(fix.teamOneName, team1) && teamsMatch(fix.teamTwoName, team2);
        const m2 = teamsMatch(fix.teamOneName, team2) && teamsMatch(fix.teamTwoName, team1);
        if (m1 || m2) {
          log('✅', `Match found: ${fix.teamOneName} vs ${fix.teamTwoName} (${fix.eventId})`);
          return fix;
        }
      }
    } catch (e) {
      log('⚠️', `Search error "${q}": ${e.message}`);
    }
  }
  return null;
}

// ═══════════════════════════════════════════════
//  SX BET: FIND MARKET
// ═══════════════════════════════════════════════
const MARKET_TYPES = {
  MONEYLINE_NO_DRAW: 52,
  MONEYLINE_OT: 226,
  OVER_UNDER: 2,
  OVER_UNDER_OT: 28,
  BTTS: 17,
  ASIAN_HANDICAP: 3,
  ONE_X_TWO_1: 1,              // 1X2: Will team 1 win?
  ONE_X_TWO_X: 15,             // 1X2: Will it be a draw?
  ONE_X_TWO_2: 14,             // 1X2: Will team 2 win?
};

async function findMarket(eventId, classification, availableTypes) {
  let targetTypes = [];

  switch (classification.type) {
    case 'moneyline':
      // For straight wins, prioritize 1X2 markets (Type 1 and 14).
      // Fallback to Moneyline No Draw (52) for 2-way sports without draws (e.g. Tennis, Basketball).
      targetTypes = [MARKET_TYPES.ONE_X_TWO_1, MARKET_TYPES.ONE_X_TWO_2, MARKET_TYPES.MONEYLINE_NO_DRAW, MARKET_TYPES.MONEYLINE_OT];
      break;
    case 'over_under':
    case 'team_over':
      targetTypes = [MARKET_TYPES.OVER_UNDER, MARKET_TYPES.OVER_UNDER_OT];
      break;
    case 'btts':
    case 'btts_yes':
      targetTypes = [MARKET_TYPES.BTTS];
      break;
    case 'double_chance':
      // SX Bet puts all 1X2 outcomes under type 1 (Team1/NotTeam1, Tie/NotTie, Team2/NotTeam2)
      targetTypes = [MARKET_TYPES.ONE_X_TWO_1, MARKET_TYPES.ONE_X_TWO_2];
      break;
    case 'handicap':
      targetTypes = [MARKET_TYPES.ASIAN_HANDICAP];
      break;
    default:
      return null;
  }

  // If fixture doesn't report available types, try all target types anyway
  const valid = availableTypes.length > 0
    ? targetTypes.filter(t => availableTypes.includes(t))
    : targetTypes;

  if (!valid.length) {
    log('⚠️', `Market type unavailable. Needed: [${targetTypes}], Available: [${availableTypes}]`);
    return null;
  }

  for (const mtype of valid) {
    try {
      // For over/under, handicap, and 1X2 (types 1, 14) we need ALL lines (not just main), so skip onlyMainLine
      const needAllLines = classification.type === 'over_under' || classification.type === 'team_over' || classification.type === 'handicap' || mtype === MARKET_TYPES.ONE_X_TWO_1 || mtype === MARKET_TYPES.ONE_X_TWO_2;
      const params = { eventId, type: mtype };
      if (!needAllLines) params.onlyMainLine = true;
      const r = await sxGet('/markets/active', params);
      const markets = r.data?.markets || [];
      if (!markets.length) continue;

      if (classification.type === 'over_under') {
        // Match total over/under — STRICT exact line only (NO risky fallbacks)
        const exact = markets.find(m => m.line === classification.line);
        if (exact) {
          log('✅', `Market: ${exact.outcomeOneName} / ${exact.outcomeTwoName} (line ${exact.line})`);
          return exact;
        }
        log('⚠️', `Exact line ${classification.line} not available on SX Bet — skipping to prevent high-risk lines`);
      } else if (classification.type === 'team_over') {
        // Team-specific over (e.g. "Leverkusen Over 1.5 Goals")
        // Search for exact line in match total (line MUST stay the same)
        const exact = markets.find(m => m.line === classification.line);
        if (exact) {
          log('✅', `Match total Over/Under found with same line: ${exact.outcomeOneName} / ${exact.outcomeTwoName} (line ${exact.line})`);
          return exact;
        }
        // Line not available at all — skip, don't change the line
        log('⚠️', `Line ${classification.line} not available on match total — skipping`);
      } else if (classification.type === 'handicap') {
        // Asian handicap — find the exact spread line (e.g. -4.5)
        const absLine = Math.abs(classification.line);
        const exact = markets.find(m => m.line === absLine);
        if (exact) {
          log('✅', `Handicap market found: ${exact.outcomeOneName} / ${exact.outcomeTwoName} (line ${exact.line})`);
          return exact;
        }
        log('⚠️', `Handicap line ${classification.line} not available`);
      } else if (classification.type === 'double_chance') {
        // Find the market where the OPPONENT's name appears
        // e.g. "Torreense / Not Torreense" -> bet "Not Torreense"
        const opp = classification.opponent;
        for (const m of markets) {
          const o1 = normalise(m.outcomeOneName);
          const o2 = normalise(m.outcomeTwoName);
          if (teamsMatch(m.outcomeOneName, opp) || teamsMatch(m.outcomeTwoName.replace(/^Not /i, ''), opp)) {
            log('✅', `Opponent market found: ${m.outcomeOneName} / ${m.outcomeTwoName}`);
            return m;
          }
        }
        log('⚠️', `Could not find opponent "${opp}" in 1X2 markets`);
      } else if (classification.type === 'moneyline') {
        // Straight Win (1X2): find the market where outcomeOne is this team (e.g. "Real Madrid / Not Real Madrid")
        const team = classification.team;
        for (const m of markets) {
          if (teamsMatch(m.outcomeOneName, team)) {
            log('✅', `1X2 Straight Win market found: ${m.outcomeOneName} / ${m.outcomeTwoName}`);
            return m;
          }
        }
        // Fallback for 2-way sports (e.g. "Team1 / Team2" in tennis/basketball)
        for (const m of markets) {
          if (teamsMatch(m.outcomeOneName, team) || teamsMatch(m.outcomeTwoName, team)) {
            log('✅', `Market found: ${m.outcomeOneName} / ${m.outcomeTwoName}`);
            return m;
          }
        }
        log('⚠️', `Could not find market for "${team}" in type ${mtype}`);
      } else {
        // BTTS → return first
        log('✅', `Market: ${markets[0].outcomeOneName} / ${markets[0].outcomeTwoName}`);
        return markets[0];
      }
    } catch (e) {
      log('⚠️', `Error fetching markets (type ${mtype}): ${e.message}`);
    }
  }
  return null;
}

// ═══════════════════════════════════════════════
//  BET DIRECTION
// ═══════════════════════════════════════════════
function determineBetDirection(market, classification) {
  switch (classification.type) {
    case 'moneyline': {
      const team = classification.team;
      if (teamsMatch(market.outcomeOneName, team)) {
        return { outcomeOne: true, label: market.outcomeOneName };
      }
      if (teamsMatch(market.outcomeTwoName, team)) {
        return { outcomeOne: false, label: market.outcomeTwoName };
      }
      log('⚠️', `Cannot determine direction: "${team}" != "${market.outcomeOneName}" / "${market.outcomeTwoName}"`);
      return null;
    }

    case 'double_chance': {
      // We found the opponent's market -> bet on "Not [opponent]"
      const o1 = market.outcomeOneName.toLowerCase();
      const o2 = market.outcomeTwoName.toLowerCase();
      // The "Not X" outcome is what we want
      if (o1.startsWith('not ')) {
        return { outcomeOne: true, label: market.outcomeOneName + ' (Win/Draw)' };
      }
      if (o2.startsWith('not ')) {
        return { outcomeOne: false, label: market.outcomeTwoName + ' (Win/Draw)' };
      }
      log('⚠️', `Cannot find "Not" outcome in: "${market.outcomeOneName}" / "${market.outcomeTwoName}"`);
      return null;
    }

    case 'over_under':
    case 'team_over': {
      const wantOver = classification.side === 'over';
      const o1 = market.outcomeOneName.toLowerCase();
      const o1IsOver = o1.includes('over');
      if (wantOver) {
        return o1IsOver
          ? { outcomeOne: true, label: market.outcomeOneName }
          : { outcomeOne: false, label: market.outcomeTwoName };
      } else {
        // want Under
        return o1IsOver
          ? { outcomeOne: false, label: market.outcomeTwoName }
          : { outcomeOne: true, label: market.outcomeOneName };
      }
    }

    case 'btts':
    case 'btts_yes': {
      const o1 = market.outcomeOneName.toLowerCase();
      if (o1.includes('yes') || !o1.includes('no')) {
        return { outcomeOne: true, label: market.outcomeOneName + ' (BTTS Yes)' };
      }
      return { outcomeOne: false, label: market.outcomeTwoName + ' (BTTS Yes)' };
    }

    case 'handicap': {
      // Same as moneyline direction — bet on the team the prediction says wins
      const hTeam = classification.team;
      if (teamsMatch(market.outcomeOneName, hTeam)) {
        return { outcomeOne: true, label: `${market.outcomeOneName} (${classification.line > 0 ? '+' : ''}${classification.line})` };
      }
      if (teamsMatch(market.outcomeTwoName, hTeam)) {
        return { outcomeOne: false, label: `${market.outcomeTwoName} (${classification.line > 0 ? '+' : ''}${classification.line})` };
      }
      log('⚠️', `Cannot determine handicap direction: "${hTeam}" != "${market.outcomeOneName}" / "${market.outcomeTwoName}"`);
      return null;
    }

    default:
      return null;
  }
}

// ═══════════════════════════════════════════════
//  BET SIZE CALCULATOR
// ═══════════════════════════════════════════════
function calcBetSize(balance, confidence) {
  let pct;
  if (confidence >= 85) pct = 25n;
  else if (confidence >= 80) pct = 15n;
  else if (confidence >= 75) pct = 10n;
  else return 0n;
  return balance * pct / 100n;
}

// ═══════════════════════════════════════════════
//  TRAILING ODDS TRACKER (In-Memory Peak Following)
// ═══════════════════════════════════════════════
// State is kept in-memory so each 6-hour runner session starts clean
// without persisting unplaced/tracking bets to MongoDB.
const oddsTracker = new Map(); // eventId -> { peakOdds, lastOdds, checksCount, firstSeenAt }
const KICKOFF_URGENT_MINUTES = 30; // Place bet immediately if match starts in <= 30 mins
const DROP_TRIGGER_DIFF = 0.01;    // Minimum drop from peak (0.01) to trigger execution
const betFailures = new Map();     // betKey -> failure count

function getMinutesToKickoff(fixture) {
  if (!fixture?.gameTime) return 999;
  const kickoffMs = new Date(fixture.gameTime).getTime();
  if (isNaN(kickoffMs)) return 999;
  return (kickoffMs - Date.now()) / (60 * 1000);
}

async function getOrderbookTop(market, isOutcomeOne) {
  try {
    const { data: book } = await sxGet('/orderbook-v3/snapshot/market', {
      marketHash: market.marketHash,
      showTakerPerspective: true,
    }, true);

    const levels = isOutcomeOne ? book?.outcomeOne : book?.outcomeTwo;
    if (!levels?.length) return null;

    const best = levels[0];
    const percentageOdds = best.percentageOdds;
    const prob = Number(BigInt(percentageOdds)) / 1e18;
    const decimalOdds = prob > 0 ? Number((100 / prob).toFixed(2)) : 0;
    const availableSize = BigInt(best.size);

    return {
      best,
      percentageOdds,
      prob,
      decimalOdds,
      availableSize,
    };
  } catch (e) {
    log('⚠️', `Orderbook snapshot error: ${e.message}`);
    return null;
  }
}

// ═══════════════════════════════════════════════
//  PLACE BET
// ═══════════════════════════════════════════════
async function placeBet(wallet, market, isOutcomeOne, betSize, orderbookTop = null) {
  // 1) Metadata
  const { data: meta } = await sxGet('/metadata/obv3');

  // 2) Orderbook top (use cached or fetch fresh)
  const top = orderbookTop || await getOrderbookTop(market, isOutcomeOne);
  if (!top) throw new Error('No liquidity available');

  const { percentageOdds, availableSize, prob, decimalOdds } = top;
  const actualBet = betSize < availableSize ? betSize : availableSize;

  if (actualBet < 1_000_000n) throw new Error(`Bet size too small: ${Number(actualBet) / 1e6} USDC`);

  log('📈', `Executing order: ${prob.toFixed(2)}% (Odds: ${decimalOdds}) | Liquidity: ${Number(availableSize) / 1e6} USDC | Bet: ${Number(actualBet) / 1e6} USDC`);

  if (DRY_RUN) {
    log('🧪', '<-- DRY RUN -- bet NOT placed');
    return { dryRun: true, bet: Number(actualBet) / 1e6, odds: decimalOdds };
  }

  // 3) Build order
  const order = {
    marketHash: market.marketHash,
    baseToken: meta.activeAsset.baseToken,
    totalBetSize: actualBet.toString(),
    percentageOdds,
    salt: hexlify(randomBytes(32)),
    expiry: Math.floor(Date.now() / 1000) + 3600,
    maker: wallet.address,
    isMakerBettingOutcomeOne: isOutcomeOne,
  };

  // 4) EIP-712 sign
  const domain = meta.domain;
  const types = {
    Order: [
      { name: 'marketHash', type: 'bytes32' },
      { name: 'baseToken', type: 'address' },
      { name: 'totalBetSize', type: 'uint256' },
      { name: 'percentageOdds', type: 'uint256' },
      { name: 'salt', type: 'uint256' },
      { name: 'expiry', type: 'uint256' },
      { name: 'maker', type: 'address' },
      { name: 'isMakerBettingOutcomeOne', type: 'bool' },
    ],
  };

  const orderSignature = await wallet.signTypedData(domain, types, order);

  // 5) Submit
  const result = await sxPost('/orders-v3', {
    orders: [{ ...order, timeInForce: 'FOK', orderSignature }],
  });

  return result;
}

// ═══════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ═══════════════════════════════════════════════
//  SINGLE CYCLE
// ═══════════════════════════════════════════════
async function runCycle(db, wallet, col) {
  const history = await loadBetHistory(col);
  const { today, preds } = await fetchTodayPredictions(db);
  if (!preds.length) return;

  // Filter out already-bet predictions
  const newPreds = preds.filter(raw => {
    const p = parsePrediction(raw);
    const key = getBetKey(today, p.matchIndex, p.prediction);
    return !wasBetPlaced(history, key);
  });

  if (!newPreds.length) {
    log('💤', 'No new predictions to process');
    return;
  }

  log('🆕', `${newPreds.length} new predictions (${preds.length - newPreds.length} already processed)`);

  // Check balance
  const { available: startBalance } = await getBalance();
  log('💰', `Available balance: ${Number(startBalance) / 1e6} USDC`);



  let betsPlaced = 0;
  const results = [];

  for (const raw of newPreds) {
    // Safety: max bets per cycle
    if (betsPlaced >= MAX_BETS_PER_CYCLE) {
      log('🛑', `Max bets per cycle reached (${MAX_BETS_PER_CYCLE}) -- stopping`);
      break;
    }

    console.log('\n' + '-'.repeat(55));
    const p = parsePrediction(raw);
    const betKey = getBetKey(today, p.matchIndex, p.prediction);

    log('⚽', `#${p.matchIndex}  ${p.team1}  vs  ${p.team2}`);
    log('📋', `Prediction: ${p.prediction}`);
    log('📊', `Confidence: ${p.confidence}% | League: ${p.league}`);

    // -- Check threshold --
    if (p.confidence < 75) {
      log('⏭️', `Skip -- confidence ${p.confidence}% below 75%`);
      await recordBet(col, history, betKey, 0); // mark as seen
      results.push({ ...p, status: 'SKIP', reason: `Confidence ${p.confidence}% < 75%` });
      continue;
    }

    // -- Classify --
    const cls = classifyPrediction(p.prediction);
    if (cls.type === 'btts_no') {
      log('⏭️', `Skip -- "Both Teams To Score No" is not supported on SX Bet`);
      await recordBet(col, history, betKey, 0);
      results.push({ ...p, status: 'SKIP', reason: 'BTTS No not supported on SX Bet' });
      continue;
    }
    if (cls.type === 'unknown') {
      log('⚠️', `Cannot classify prediction: "${p.prediction}"`);
      await recordBet(col, history, betKey, 0);
      results.push({ ...p, status: 'SKIP', reason: 'Unknown prediction type' });
      continue;
    }
    log('🎯', `Type: ${cls.type}`, cls.team ? `| Team: ${cls.team}` : '', cls.line != null ? `| Line: ${cls.line}` : '', cls.side ? `| Side: ${cls.side}` : '');

    // For double_chance, figure out the opponent
    if (cls.type === 'double_chance') {
      if (teamsMatch(cls.team, p.team1)) {
        cls.opponent = p.team2;
      } else if (teamsMatch(cls.team, p.team2)) {
        cls.opponent = p.team1;
      } else {
        cls.opponent = p.team2; // fallback
      }
      log('🎯', `Double chance: ${cls.team} wins or draw -> bet against ${cls.opponent}`);
    }

    // -- Search fixture (with retry limit) --
    const fixture = await searchFixture(p.team1, p.team2);
    if (!fixture) {
      const retryKey = `${today}|${p.matchIndex}|search`;
      history.searchRetries[retryKey] = (history.searchRetries[retryKey] || 0) + 1;
      const retryCount = history.searchRetries[retryKey];

      if (retryCount >= MAX_SEARCH_RETRIES) {
        log('🛑', `Match not found after ${retryCount} attempts — giving up permanently`);
        await recordBet(col, history, betKey, 0); // Mark as processed so we never retry
      } else {
        log('❌', `Match not found on SX Bet (attempt ${retryCount}/${MAX_SEARCH_RETRIES})`);
      }
      await saveBetHistory(col, history);
      results.push({ ...p, status: 'SKIP', reason: `Match not found (${retryCount}/${MAX_SEARCH_RETRIES})` });
      continue;
    }

    // 🛡️ SX Bet Event ID Deduplication: If this exact event was already bet on, NEVER bet again!
    if (history.events && history.events.includes(fixture.eventId)) {
      log('🛑', `Already placed a bet on this match on SX Bet (Event ID: ${fixture.eventId}) -- skipping duplicate!`);
      results.push({ ...p, status: 'SKIP', reason: `Event ${fixture.eventId} already bet` });
      continue;
    }

    // -- Find market --
    let market = await findMarket(fixture.eventId, cls, fixture.type || []);

    // BTTS fallback: if BTTS Yes market (type 17) is unavailable, convert to Over 1.5 ONLY for "Both Team Score Yes"
    // Logic: Both teams scoring = at least 2 goals = Over 1.5 (same idea, lower risk)
    if (!market && (cls.type === 'btts' || cls.type === 'btts_yes')) {
      log('🔄', 'BTTS Yes market unavailable — converting to Over 1.5 Goals (same logic, lower risk)');
      cls.type = 'over_under';
      cls.line = 1.5;
      cls.side = 'over';
      market = await findMarket(fixture.eventId, cls, fixture.type || []);
    }

    if (!market) {
      log('❌', 'Market type not available for this match');
      results.push({ ...p, status: 'SKIP', reason: 'Market unavailable' });
      continue;
    }

    // -- Bet direction --
    const dir = determineBetDirection(market, cls);
    if (!dir) {
      log('❌', 'Cannot determine bet direction');
      await recordBet(col, history, betKey, 0);
      results.push({ ...p, status: 'SKIP', reason: 'Direction unknown' });
      continue;
    }
    log('🎲', `Betting on: ${dir.label}`);

    // -- Check kickoff time --
    const minsToKickoff = getMinutesToKickoff(fixture);
    if (minsToKickoff < -15) {
      log('⏰', 'Match kicked off >15m ago -- skipping');
      await recordBet(col, history, betKey, 0);
      results.push({ ...p, status: 'SKIP', reason: 'Match already kicked off' });
      continue;
    }

    // -- Fetch current orderbook price & liquidity --
    const top = await getOrderbookTop(market, dir.outcomeOne);
    if (!top) {
      log('⏳', 'No liquidity on SX Bet orderbook yet -- waiting for next cycle');
      results.push({ ...p, status: 'WAIT', reason: 'No liquidity yet' });
      continue; // Intentionally NO recordBet, keeps MongoDB clean for next check
    }

    // -- Trailing Odds Peak-Tracking Logic --
    const curOdds = top.decimalOdds;
    let shouldBetNow = false;
    let triggerReason = '';

    if (minsToKickoff <= KICKOFF_URGENT_MINUTES && minsToKickoff >= -15) {
      shouldBetNow = true;
      triggerReason = `Kickoff in ${Math.round(minsToKickoff)}m (<= ${KICKOFF_URGENT_MINUTES}m urgency cutoff)`;
    } else {
      const tracker = oddsTracker.get(fixture.eventId);
      if (!tracker) {
        // First time seeing this fixture: start tracking and wait for price movement!
        oddsTracker.set(fixture.eventId, {
          peakOdds: curOdds,
          lastOdds: curOdds,
          checksCount: 1,
          firstSeenAt: Date.now(),
        });
        log('👀', `Started tracking #${p.matchIndex} (${p.team1} vs ${p.team2})`);
        log('📊', `Current Odds: ${curOdds} | Kickoff in: ${Math.round(minsToKickoff)}m -- Waiting for next cycle to check for increase`);
        results.push({ ...p, status: 'WAIT', reason: `Tracking started @ ${curOdds}` });
        continue; // Intentionally NO recordBet in DB, leaving state completely clean!
      }

      tracker.checksCount++;
      const prevPeak = tracker.peakOdds;
      const prevLast = tracker.lastOdds;

      if (curOdds > prevPeak) {
        // Odds increased! Update peak and hold bet to catch higher odds
        tracker.peakOdds = curOdds;
        tracker.lastOdds = curOdds;
        log('📈', `Odds INCREASED: ${prevPeak} -> ${curOdds} (New Peak! 🔥)`);
        log('⏳', `Holding bet to catch higher odds... (${Math.round(minsToKickoff)}m to kickoff, check #${tracker.checksCount})`);
        results.push({ ...p, status: 'WAIT', reason: `Odds rising: ${prevPeak} -> ${curOdds}` });
        continue; // Intentionally NO recordBet in DB!
      } else if (curOdds >= prevPeak - (DROP_TRIGGER_DIFF - 0.001)) {
        // Odds steady (within 0.01 noise margin)
        tracker.lastOdds = curOdds;
        log('⏳', `Odds steady at ${curOdds} (Peak: ${prevPeak}) -- waiting for movement (${Math.round(minsToKickoff)}m to kickoff, check #${tracker.checksCount})`);
        results.push({ ...p, status: 'WAIT', reason: `Odds steady @ ${curOdds}` });
        continue; // Intentionally NO recordBet in DB!
      } else {
        // Odds dropped from peak by at least 0.01! The peak has crested -> lock in high odds!
        shouldBetNow = true;
        triggerReason = `Odds dropped from peak ${prevPeak} to ${curOdds} (-${(prevPeak - curOdds).toFixed(2)}) -- locking in high odds!`;
      }
    }

    // -- Re-read balance & check safety limits --
    const { available: curBal } = await getBalance();
    const bettableBalance = curBal;
    let betSize = calcBetSize(bettableBalance, p.confidence);

    // Minimum bet = 1 USDC
    const MIN_BET = 1_000_000n;
    if (betSize < MIN_BET && bettableBalance >= MIN_BET) {
      betSize = MIN_BET;
    } else if (bettableBalance < MIN_BET) {
      log('⚠️', `Balance too low: ${Number(bettableBalance) / 1e6} USDC`);
      results.push({ ...p, status: 'SKIP', reason: 'Balance too low' });
      continue;
    }

    const pctLabel = p.confidence >= 85 ? '25%' : p.confidence >= 80 ? '15%' : '10%';
    log('💵', `Bet size: ${Number(betSize) / 1e6} USDC (${pctLabel} of bettable balance)`);
    log('🎯', `TRIGGER: ${triggerReason}`);

    // -- Place bet --
    try {
      const res = await placeBet(wallet, market, dir.outcomeOne, betSize, top);
      log('✅', 'Bet placed successfully!');
      if (!DRY_RUN) log('📝', JSON.stringify(res, null, 2));
      await recordBet(col, history, betKey, Number(betSize) / 1e6, fixture.eventId);
      oddsTracker.delete(fixture.eventId); // Done tracking
      betsPlaced++;
      results.push({ ...p, status: 'OK', result: res });
    } catch (err) {
      log('❌', `Bet failed: ${err.message}`);
      const failCount = (betFailures.get(betKey) || 0) + 1;
      betFailures.set(betKey, failCount);
      if (failCount >= 3) {
        log('🛑', 'Bet failed 3 times -- recording as finished to prevent endless retries');
        await recordBet(col, history, betKey, 0);
      }
      results.push({ ...p, status: 'FAIL', reason: err.message });
    }

    // Small delay between bets
    await sleep(1000);
  }

  // -- Summary --
  if (results.length) {
    console.log('\n' + '='.repeat(60));
    log('📊', '== Cycle Results ==');
    for (const r of results) {
      const icon = r.status === 'OK' ? '✅' : r.status === 'FAIL' ? '❌' : r.status === 'WAIT' ? '⏳' : '⏭️';
      log(icon, `#${r.matchIndex}  ${r.team1} vs ${r.team2}  ->  ${r.status}${r.reason ? '  (' + r.reason + ')' : ''}`);
    }
    log('💰', `Bets placed this cycle: ${betsPlaced}`);
    console.log('='.repeat(60));
  }
}

// ═══════════════════════════════════════════════
//  MAIN (Daemon Loop)
// ═══════════════════════════════════════════════
async function main() {
  console.log('\n' + '='.repeat(60));
  log('🚀', 'SX Bet Auto-Betting Script');
  if (DRY_RUN) log('🧪', '--- DRY RUN MODE ---');
  if (RUN_ONCE) log('1️⃣', '--- SINGLE RUN MODE ---');
  else log('🔁', `--- DAEMON MODE (check every ${CHECK_INTERVAL_MS / 60000} min) ---`);
  console.log('='.repeat(60) + '\n');

  // Validate env
  if (!SX_API_KEY) throw new Error('Missing sx_bet_API in .env');
  if (!SX_PRIVATE_KEY) throw new Error('Missing PRIVATE_KEY in .env');
  if (!process.env.FIREBASE_SERVICE_ACCOUNT) throw new Error('Missing FIREBASE_SERVICE_ACCOUNT in .env');
  if (!process.env.MONGODB_URI) throw new Error('Missing MONGODB_URI in .env');

  // Init
  const db = initFirebase();
  const mongoCol = await initMongo();
  log('🍃', 'MongoDB connected successfully for bet history storage');
  const wallet = new Wallet(SX_PRIVATE_KEY);
  log('👤', `Wallet: ${wallet.address}`);
  log('🛡️', `Safety: max ${MAX_BETS_PER_CYCLE} bets/cycle`);

  if (RUN_ONCE) {
    await runCycle(db, wallet, mongoCol);
    log('🏁', 'Done!');
    process.exit(0);
  }

  // Daemon loop
  let cycleNum = 0;
  while (true) {
    cycleNum++;
    console.log('\n' + '#'.repeat(60));
    log('🔄', `=== Cycle #${cycleNum} ===`);
    console.log('#'.repeat(60));

    try {
      await runCycle(db, wallet, mongoCol);
    } catch (err) {
      log('❌', `Cycle error: ${err.message}`);
      log('📋', err.stack);
    }

    log('⏰', `Next check in ${CHECK_INTERVAL_MS / 60000} minutes...`);
    await sleep(CHECK_INTERVAL_MS);
  }
}

main().catch(err => {
  console.error('\n>> FATAL ERROR:', err);
  process.exit(1);
});
