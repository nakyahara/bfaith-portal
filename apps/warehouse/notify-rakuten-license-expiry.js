#!/usr/bin/env node
/**
 * notify-rakuten-license-expiry.js — 楽天RMS ライセンスキーの期限が近づいたら GChat で更新を促す (daily-sync の 1 ステップ)
 *
 * 既定で **残り 14 日**から毎日 1 通。残り 3 日以下・失効・期限取得不能は 🔴。
 * 期限は RMS のライセンス管理API から取る (2026-08-30 実測):
 *   GET /es/1.0/license-management/license-key/expiry-date?licenseKey=<key> → { "expiryDate": "2026-11-28T23:59:59" }
 *
 * 実行: node apps/warehouse/notify-rakuten-license-expiry.js [--force] [--dry-run]
 *   --force   … 1 日 1 通の抑止を無視して送る (テスト用)
 *   --dry-run … 送らずに文面を表示 (状態も書き換えない)
 * env: DATA_DIR (必須) / RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY / GCHAT_WEBHOOK
 *      / RAKUTEN_LICENSE_WARN_DAYS (既定 14)
 * exit code: 0=正常 (通知失敗も 0。daily-sync 全体を落とさないため) / 2=env エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { rakutenRequest } from './rakuten-client.js';
import { buildLicenseExpiryNotice, jstDay, DEFAULT_WARN_DAYS } from './rakuten-license-expiry-lib.js';

const args = process.argv.slice(2);
const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const isDryRun = args.includes('--dry-run');
const isForce = args.includes('--force');
if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exitCode = 2; }
const dbPath = path.join(DATA_DIR || '.', 'warehouse.db');
if (process.exitCode !== 2 && !fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exitCode = 2;
}

const webhook = (process.env.GCHAT_WEBHOOK || '').trim();
// 0 (期限当日だけ通知) も有効値。不正値・範囲外は既定に戻す
const warnDaysRaw = Number.parseInt(process.env.RAKUTEN_LICENSE_WARN_DAYS ?? '', 10);
const warnDays = Number.isFinite(warnDaysRaw) && warnDaysRaw >= 0 && warnDaysRaw <= 90 ? warnDaysRaw : DEFAULT_WARN_DAYS;
if (process.env.RAKUTEN_LICENSE_WARN_DAYS && warnDays !== warnDaysRaw) {
  console.warn(`[rakuten-license] ⚠ RAKUTEN_LICENSE_WARN_DAYS='${process.env.RAKUTEN_LICENSE_WARN_DAYS}' は不正 (0〜90)。既定 ${DEFAULT_WARN_DAYS} 日を使う`);
}
const nowIso = new Date().toISOString();

/** ライセンス期限を取る。キーはURLに乗るのでログには一切出さない */
async function fetchExpiry() {
  const key = (process.env.RAKUTEN_LICENSE_KEY || '').trim();
  if (!key || !(process.env.RAKUTEN_SERVICE_SECRET || '').trim()) {
    return { expiryDate: null, authFailed: false, fetchError: 'RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY 未設定' };
  }
  try {
    const r = await rakutenRequest({
      path: `/es/1.0/license-management/license-key/expiry-date?licenseKey=${encodeURIComponent(key)}`,
      timeoutMs: 30000,
    });
    // 401 = キー自体が失効 (GA0001 Un-Authorised)。403 も権限喪失として同じ扱い
    if (r.status === 401 || r.status === 403) return { expiryDate: null, authFailed: true, fetchError: null };
    if (r.status !== 200) return { expiryDate: null, authFailed: false, fetchError: `期限API HTTP ${r.status}` };
    const v = r.data?.expiryDate ?? null;
    if (!v) return { expiryDate: null, authFailed: false, fetchError: '期限APIのレスポンスに expiryDate がありません' };
    return { expiryDate: v, authFailed: false, fetchError: null };
  } catch (e) {
    const timedOut = e?.name === 'TimeoutError' || e?.name === 'AbortError';
    return { expiryDate: null, authFailed: false, fetchError: timedOut ? '期限API タイムアウト' : `期限API 接続失敗: ${String(e?.message || e).slice(0, 80)}` };
  }
}

async function send(notice, onFail) {
  try {
    const res = await fetch(webhook, {
      method: 'POST', headers: { 'Content-Type': 'application/json; charset=UTF-8' },
      body: JSON.stringify({ text: notice.text }), signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    console.log(`[rakuten-license] 📣 GChat 通知 (${res.status})`);
  } catch (e) {
    // 通知失敗で daily-sync 全体を失敗にしない。枠を戻して retry / 翌朝に再送
    console.error(`[rakuten-license] ⚠ GChat 通知失敗 (次回再送): ${e.message}`);
    onFail();
  }
}

if (process.exitCode !== 2) {
  const db = new Database(dbPath);
  db.pragma('busy_timeout = 5000');
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_license_notify_state (
    kind          TEXT PRIMARY KEY,
    last_sent_day TEXT NOT NULL,
    last_expiry   TEXT,
    sent_count    INTEGER NOT NULL DEFAULT 0,
    updated_at    TEXT NOT NULL
  )`);

  /**
   * 「その kind をその JST 日に送る権利」を原子的に取る (SELECT→送信→UPDATE だと
   * daily-sync の retry・手動実行の同時起動で二重送信になる)。取れたら true。
   */
  const claimTodaySlot = (kind, today, expiry) => db.transaction(() => {
    const prev = db.prepare('SELECT last_sent_day FROM rakuten_license_notify_state WHERE kind = ?').get(kind);
    if (prev?.last_sent_day === today) return false;
    db.prepare(`INSERT INTO rakuten_license_notify_state (kind, last_sent_day, last_expiry, sent_count, updated_at)
                VALUES (@kind, @day, @expiry, 1, @now)
                ON CONFLICT(kind) DO UPDATE SET last_sent_day = @day, last_expiry = @expiry, sent_count = sent_count + 1, updated_at = @now`)
      .run({ kind, day: today, expiry: expiry || null, now: nowIso });
    return true;
  }).immediate();

  const releaseTodaySlot = (kind, prevDay) => {
    try {
      db.prepare('UPDATE rakuten_license_notify_state SET last_sent_day = ?, sent_count = MAX(sent_count - 1, 0), updated_at = ? WHERE kind = ?')
        .run(prevDay || '', nowIso, kind);
    } catch { /* 戻せなくても翌日には送れる */ }
  };

  try {
    const { expiryDate, authFailed, fetchError } = await fetchExpiry();
    const notice = buildLicenseExpiryNotice({ expiryDate, authFailed, fetchError, nowIso, warnDays });

    if (!notice) {
      console.log(`[rakuten-license] 通知不要 (期限 ${jstDay(expiryDate) || expiryDate || '不明'}、しきい値 ${warnDays}日)`);
    } else {
      const today = jstDay(nowIso);
      const prevDay = db.prepare('SELECT last_sent_day FROM rakuten_license_notify_state WHERE kind = ?').get(notice.kind)?.last_sent_day || null;
      console.log(`[rakuten-license] ${notice.kind} level=${notice.level} daysLeft=${notice.daysLeft ?? '-'} expiry=${notice.expiryDay ?? '-'}${prevDay === today ? ' (本日送信済み)' : ''}`);
      if (isDryRun) {
        console.log('--- dry-run ---\n' + notice.text);
      } else if (!webhook) {
        console.error('[rakuten-license] ⚠ GCHAT_WEBHOOK 未設定のため通知できない (設定を確認してください)');
      } else if (isForce) {
        await send(notice, () => {});
      } else if (!claimTodaySlot(notice.kind, today, expiryDate)) {
        console.log('[rakuten-license] 本日は送信済みのためスキップ (--force で強制送信)');
      } else {
        await send(notice, () => releaseTodaySlot(notice.kind, prevDay));
      }
    }
  } finally {
    db.close();
  }
}
// process.exit() は使わない (通知 fetch 直後の exit は Windows node で libuv abort を踏む)
