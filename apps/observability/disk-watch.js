/**
 * disk-watch.js — DATA_DIR (Render Persistent Disk) の使用率定期観測
 *
 * 目的: 2026-07-12 障害の再発防止。1GB の Persistent Disk が満杯になり
 * SQLite が "database or disk is full" で書き込み失敗 → packing-dispatch 取込エラー・
 * sessions.db 書込不可で「ログインできない」という遠い症状として発覚した。
 * mirror は分析データを毎日蓄積する設計なので満杯は「防ぐ」ものではなく、
 * 溢れる前に GChat で気づいて拡張するための監視。
 *
 * 15分毎 (初回は起動直後) に:
 *   - DATA_DIR が乗るファイルシステムの使用率を stdout に 1 行ログ
 *   - warn (既定80%) / crit (既定90%) 到達時に GChat へ 1 度だけ通知。
 *     送信失敗時は通知済みにせず次回チェックで再送。
 *     (閾値-3pt を下回るまで再通知しない = 境界での flapping 防止、
 *      ディスク拡張で使用率が下がれば自動で再武装)
 *
 * metrics.js と同じ GCHAT_WEBHOOK を使う。未設定なら stdout ログのみ。
 */
import fs from 'fs';
import os from 'os';
import { bootStart, bootEnd, getBootId } from './boot-log.js';

// env は不正値 (非数値・範囲外・warn>=crit) なら安全なデフォルトに戻す
function intEnv(name, def, min, max) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const v = Number(raw);
  if (!Number.isInteger(v) || v < min || v > max) {
    console.warn(`[DiskWatch] ${name}=${raw} は不正値 → デフォルト ${def} を使用`);
    return def;
  }
  return v;
}
const CHECK_INTERVAL_MS = intEnv('DISK_WATCH_INTERVAL_MS', 15 * 60 * 1000, 60 * 1000, 24 * 60 * 60 * 1000);
let WARN_PCT = intEnv('DISK_USAGE_WARN_PCT', 80, 1, 100);
let CRIT_PCT = intEnv('DISK_USAGE_CRIT_PCT', 90, 1, 100);
if (WARN_PCT >= CRIT_PCT) {
  console.warn(`[DiskWatch] warn(${WARN_PCT}) >= crit(${CRIT_PCT}) は不正 → デフォルト 80/90 を使用`);
  WARN_PCT = 80;
  CRIT_PCT = 90;
}
const REARM_MARGIN_PCT = 3; // 通知済みレベルの閾値からこれだけ下がったら再武装
const NOTIFY_TIMEOUT_MS = 10 * 1000;
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

let notifiedLevel = 0; // 0=ok, 1=warn, 2=crit
let checkInProgress = false;
let started = false;

/** @returns {Promise<boolean>} 送信できたか (webhook 未設定は「送る先がない」ので true 扱い) */
async function notify(text) {
  if (!GCHAT_WEBHOOK) return true;
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(NOTIFY_TIMEOUT_MS),
    });
    if (!res.ok) {
      console.error(`[DiskWatch] 通知エラー: HTTP ${res.status}`);
      return false;
    }
    return true;
  } catch (e) {
    console.error('[DiskWatch] 通知エラー:', e.message);
    return false;
  }
}

function hostLabel() {
  if (process.env.RENDER) return 'Render';
  return os.hostname() || 'unknown';
}

/** df の Use% と同じ定義で使用率を計算 (used / (used + avail)) */
export function readDiskUsage(dir) {
  const s = fs.statfsSync(dir);
  const availB = s.bavail * s.bsize; // 一般ユーザーが使える残量 (df の Avail)
  const usedB = (s.blocks - s.bfree) * s.bsize;
  const usedPct = usedB + availB > 0 ? (usedB / (usedB + availB)) * 100 : 0;
  return { availB, usedB, usedPct }; // usedPct は未丸め (閾値判定用)、表示側で丸める
}

function fmtGB(bytes) {
  return (bytes / 1e9).toFixed(2) + 'GB';
}

function levelOf(pct) {
  if (pct >= CRIT_PCT) return 2;
  if (pct >= WARN_PCT) return 1;
  return 0;
}

async function check(dataDir) {
  if (checkInProgress) return; // 通知の応答待ち中に次周期が重ならないように
  checkInProgress = true;
  try {
    let u;
    try {
      u = readDiskUsage(dataDir);
    } catch (e) {
      console.warn('[DiskWatch] 使用率取得失敗:', e.message);
      return;
    }
    const level = levelOf(u.usedPct);
    const pctDisp = Math.round(u.usedPct);
    const line = `[DiskWatch] boot=${getBootId()} host=${hostLabel()} dir=${dataDir} used=${fmtGB(u.usedB)}/${fmtGB(u.usedB + u.availB)} (${pctDisp}%) avail=${fmtGB(u.availB)}`;
    if (level === 2) console.error(line + ' 🔴 crit');
    else if (level === 1) console.warn(line + ' 🟡 warn');
    else console.log(line);

    if (level > notifiedLevel) {
      const mark = level === 2 ? '🔴' : '🟡';
      const th = level === 2 ? CRIT_PCT : WARN_PCT;
      const sent = await notify([
        `${mark} ディスク使用率 ${pctDisp}% (閾値${th}%超え) — ${hostLabel()} ${dataDir}`,
        `使用 ${fmtGB(u.usedB)} / 全体 ${fmtGB(u.usedB + u.availB)} (残り ${fmtGB(u.availB)})`,
        `満杯になると "database or disk is full" で取込・ログインが止まります。`,
        `対処: Render Dashboard → bfaith-portal → Disks でサイズ拡張`,
      ].join('\n'));
      if (sent) notifiedLevel = level; // 送信失敗時は据え置き → 次回チェックで再送
    } else if (level < notifiedLevel) {
      // 拡張・削除で使用率が下がった場合のみ再武装 (境界の上下では通知を繰り返さない)
      const rearmBelow = (notifiedLevel === 2 ? CRIT_PCT : WARN_PCT) - REARM_MARGIN_PCT;
      if (u.usedPct < rearmBelow) notifiedLevel = level;
    }
  } finally {
    checkInProgress = false;
  }
}

export function startDiskWatch(dataDir) {
  if (started) {
    console.warn('[DiskWatch] 既に起動済み → 二重起動をスキップ');
    return;
  }
  started = true;
  bootStart('disk-watch', 'usage-observer');
  try {
    fs.statfsSync(dataDir); // 対応環境か起動時に確認 (Node 18.15+)
  } catch (e) {
    console.warn('[DiskWatch] 起動スキップ (statfs不可):', e.message);
    bootEnd('disk-watch', 'usage-observer', 'skipped');
    return;
  }
  check(dataDir); // 初回は即時 (起動ログで現在値が分かる)
  const timer = setInterval(() => check(dataDir), CHECK_INTERVAL_MS);
  timer.unref();
  console.log(`[DiskWatch] 観測開始 (${Math.round(CHECK_INTERVAL_MS / 60000)}分毎、warn≥${WARN_PCT}% crit≥${CRIT_PCT}%) dir=${dataDir}`);
  bootEnd('disk-watch', 'usage-observer', `interval=${CHECK_INTERVAL_MS}ms`);
}
