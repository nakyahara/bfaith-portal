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
 * exit code: 0=正常 (通知失敗も 0。daily-sync 全体を落とさないため。失敗はログと翌日の再送で拾う) / 2=env エラー
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
// 0 (期限当日だけ通知) も有効値として扱う。不正値・範囲外は既定に戻す (Codex R1 Low)
const warnDaysRaw = Number.parseInt(process.env.YAHOO_TOKEN_WARN_DAYS ?? '', 10);
const warnDays = Number.isFinite(warnDaysRaw) && warnDaysRaw >= 0 && warnDaysRaw <= 28 ? warnDaysRaw : DEFAULT_WARN_DAYS;
if (process.env.YAHOO_TOKEN_WARN_DAYS && warnDays !== warnDaysRaw) {
  console.warn(`[yahoo-token] ⚠ YAHOO_TOKEN_WARN_DAYS='${process.env.YAHOO_TOKEN_WARN_DAYS}' は不正 (0〜28)。既定 ${DEFAULT_WARN_DAYS} 日を使う`);
}
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

/**
 * 「その kind をその JST 日に送る権利」を原子的に取る (Codex R1 High: SELECT→送信→UPDATE だと
 * daily-sync の retry・手動実行の同時起動で二重送信になる)。取れたら true。
 * 送信に失敗したら releaseClaim() で当日枠を戻し、次の retry が送れるようにする。
 */
function claimTodaySlot(kind, today, expiry) {
  const tx = db.transaction(() => {
    const prev = db.prepare(`SELECT last_sent_day FROM yahoo_token_notify_state WHERE kind = ?`).get(kind);
    if (prev?.last_sent_day === today) return false;
    db.prepare(`INSERT INTO yahoo_token_notify_state (kind, last_sent_day, last_expiry, sent_count, updated_at)
                VALUES (@kind, @day, @expiry, 1, @now)
                ON CONFLICT(kind) DO UPDATE SET last_sent_day = @day, last_expiry = @expiry, sent_count = sent_count + 1, updated_at = @now`)
      .run({ kind, day: today, expiry: expiry || null, now: nowIso });
    return true;
  });
  return tx.immediate();
}
function releaseTodaySlot(kind, prevDay) {
  try {
    db.prepare(`UPDATE yahoo_token_notify_state SET last_sent_day = ?, sent_count = MAX(sent_count - 1, 0), updated_at = ? WHERE kind = ?`)
      .run(prevDay || '', nowIso, kind);
  } catch { /* 戻せなくても翌日には送れる */ }
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
    const prevDay = db.prepare(`SELECT last_sent_day FROM yahoo_token_notify_state WHERE kind = ?`).get(notice.kind)?.last_sent_day || null;
    console.log(`[yahoo-token] ${notice.kind} level=${notice.level} daysLeft=${notice.daysLeft ?? '-'}${prevDay === today ? ' (本日送信済み)' : ''}`);
    if (isDryRun) {
      console.log('--- dry-run ---\n' + notice.text);
    } else if (!webhook) {
      console.error('[yahoo-token] ⚠ GCHAT_WEBHOOK 未設定のため通知できない (設定を確認してください)');
    } else if (isForce) {
      await send(notice, () => {});
    } else if (!claimTodaySlot(notice.kind, today, health?.refreshTokenExpiresAt)) {
      console.log('[yahoo-token] 本日は送信済みのためスキップ (--force で強制送信)');
    } else {
      // 枠を取ってから送る。失敗したら枠を戻して次の retry / 翌日に再送させる
      await send(notice, () => releaseTodaySlot(notice.kind, prevDay));
    }
  }
} finally {
  db.close();
}

async function send(notice, onFail) {
  try {
    const res = await fetch(webhook, {
      method: 'POST', headers: { 'Content-Type': 'application/json; charset=UTF-8' },
      body: JSON.stringify({ text: notice.text }), signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    console.log(`[yahoo-token] 📣 GChat 通知 (${res.status})`);
  } catch (e) {
    // 通知失敗で daily-sync 全体を失敗にしない (Codex R1 High)。枠を戻して retry / 翌朝に再送
    console.error(`[yahoo-token] ⚠ GChat 通知失敗 (次回再送): ${e.message}`);
    onFail();
  }
}
// process.exit() は使わない (通知 fetch 直後の exit は Windows node で libuv abort を踏む)
process.exitCode = exitCode;
