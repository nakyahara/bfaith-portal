#!/usr/bin/env node
/**
 * notify-yahoo-token-expiry.js — Yahoo refresh token の期限が近づいたら GChat で再認可を促す (daily-sync の 1 ステップ)
 *
 * 既定で **残り 5 日**から毎日 1 通。残り 2 日以下・失効・プロキシ到達不可・期限不明は 🔴。
 * 文面には「在庫検索ボットに『yahoo再認可』と送る」手順と認可 URL を入れる (#958 の GChat 再認可と接続)。
 *
 * 実行: node apps/warehouse/notify-yahoo-token-expiry.js [--force] [--dry-run]
 *   --force   … 1 日 1 通の抑止を無視して送る (テスト用)
 *   --dry-run … 送らずに文面を表示
 * env: DATA_DIR (必須) / YAHOO_PROXY_URL / YAHOO_PROXY_SECRET / GCHAT_WEBHOOK / YAHOO_TOKEN_WARN_DAYS (既定 5)
 * exit code: 0=正常 (通知なし・通知済み含む) / 1=通知が必要なのに送れなかった / 2=env エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { buildTokenExpiryNotice, jstDay, DEFAULT_WARN_DAYS } from './yahoo-token-expiry-lib.js';

const args = process.argv.slice(2);
const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const isDryRun = args.includes('--dry-run');
const isForce = args.includes('--force');
if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const proxyUrl = (process.env.YAHOO_PROXY_URL || '').trim().replace(/\/$/, '');
const proxySecret = (process.env.YAHOO_PROXY_SECRET || '').trim();
const webhook = (process.env.GCHAT_WEBHOOK || '').trim();
const warnDays = Number.parseInt(process.env.YAHOO_TOKEN_WARN_DAYS || '', 10) || DEFAULT_WARN_DAYS;
const nowIso = new Date().toISOString();

const db = new Database(dbPath);
db.pragma('busy_timeout = 5000');
db.exec(`CREATE TABLE IF NOT EXISTS yahoo_token_notify_state (
  kind         TEXT PRIMARY KEY,
  last_sent_day TEXT NOT NULL,
  last_expiry   TEXT,
  sent_count    INTEGER NOT NULL DEFAULT 0,
  updated_at    TEXT NOT NULL
)`);

async function getHealth() {
  if (!proxyUrl || !proxySecret) return { health: null, healthError: 'YAHOO_PROXY_URL / YAHOO_PROXY_SECRET 未設定' };
  try {
    const res = await fetch(`${proxyUrl}/yahoo/health`, { headers: { 'X-Proxy-Secret': proxySecret }, signal: AbortSignal.timeout(15000) });
    if (!res.ok) return { health: null, healthError: `health HTTP ${res.status}` };
    return { health: await res.json(), healthError: null };
  } catch (e) {
    return { health: null, healthError: e?.name === 'TimeoutError' ? 'timeout' : 'network' };
  }
}

async function getAuthUrl() {
  if (!proxyUrl || !proxySecret) return null;
  try {
    const res = await fetch(`${proxyUrl}/yahoo/auth-url`, { headers: { 'X-Proxy-Secret': proxySecret }, signal: AbortSignal.timeout(10000) });
    if (!res.ok) return null;
    return (await res.json())?.url || null;
  } catch { return null; }
}

let exitCode = 0;
try {
  const { health, healthError } = await getHealth();
  const authUrl = healthError ? null : await getAuthUrl();
  const notice = buildTokenExpiryNotice({ health, healthError, authUrl, nowIso, warnDays });

  if (!notice) {
    const days = health?.refreshTokenExpiresAt ? jstDay(health.refreshTokenExpiresAt) : '不明';
    console.log(`[yahoo-token] 通知不要 (期限 ${days}、しきい値 ${warnDays}日)`);
  } else {
    const today = jstDay(nowIso);
    const prev = db.prepare(`SELECT last_sent_day FROM yahoo_token_notify_state WHERE kind = ?`).get(notice.kind);
    const already = prev?.last_sent_day === today;
    console.log(`[yahoo-token] ${notice.kind} level=${notice.level} daysLeft=${notice.daysLeft ?? '-'}${already ? ' (本日送信済み)' : ''}`);
    if (isDryRun) {
      console.log('--- dry-run ---\n' + notice.text);
    } else if (already && !isForce) {
      console.log('[yahoo-token] 本日は送信済みのためスキップ (--force で強制送信)');
    } else if (!webhook) {
      console.error('[yahoo-token] ⚠ GCHAT_WEBHOOK 未設定のため通知できない');
      exitCode = 1;
    } else {
      try {
        const res = await fetch(webhook, {
          method: 'POST', headers: { 'Content-Type': 'application/json; charset=UTF-8' },
          body: JSON.stringify({ text: notice.text }), signal: AbortSignal.timeout(15000),
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        db.prepare(`INSERT INTO yahoo_token_notify_state (kind, last_sent_day, last_expiry, sent_count, updated_at)
                    VALUES (@kind, @day, @expiry, 1, @now)
                    ON CONFLICT(kind) DO UPDATE SET last_sent_day = @day, last_expiry = @expiry, sent_count = sent_count + 1, updated_at = @now`)
          .run({ kind: notice.kind, day: today, expiry: health?.refreshTokenExpiresAt || null, now: nowIso });
        console.log(`[yahoo-token] 📣 GChat 通知 (${res.status})`);
      } catch (e) {
        console.error(`[yahoo-token] ⚠ GChat 通知失敗: ${e.message}`);
        exitCode = 1;
      }
    }
  }
} finally {
  db.close();
}
// process.exit() は使わない (通知 fetch 直後の exit は Windows node で libuv abort を踏む)
process.exitCode = exitCode;
