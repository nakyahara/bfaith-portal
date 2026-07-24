/**
 * notify-crash.js — daily-sync のプロセス abort を即時 GChat 通知する (構造監査 H-3)
 *
 * daily-sync.bat が node daily-sync.js の exit code ≠ 0 の時に呼ぶ:
 *   node apps/warehouse/notify-crash.js %RC%
 *
 * 判定は lock ファイルの有無:
 *   - lock あり = daily-sync が通知に到達せず死んだ (libuv assertion 等の abort) → 🚨 通知して lock 削除
 *   - lock なし = DQ-fail exit 1 や main().catch 等、既に通知済みの失敗経路 → 何もしない (二重通知防止)
 */
import 'dotenv/config';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');
const LOCK_FILE = path.join(PROJECT_DIR, 'data', 'daily-sync.lock.json');
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

function isAliveNodeProcess(pid) {
  try { process.kill(pid, 0); } catch { return false; }
  try {
    const out = execFileSync('tasklist', ['/FI', `PID eq ${pid}`, '/FO', 'CSV', '/NH'], { encoding: 'utf-8', timeout: 10000 });
    return /^"node(\.exe)?"/i.test(out.trim());
  } catch {
    return true;  // 照合不能時は保守的に「生存扱い」= 誤通知より誤スキップ側に倒す (翌朝検知が拾う)
  }
}

// daily-sync.js の claimAndDeleteLock と同プロトコル (rename 原子取得→検証→削除 or 返却)
function claimAndDeleteLock(matchFn) {
  const claim = `${LOCK_FILE}.claim-${process.pid}-nc`;
  try { fs.renameSync(LOCK_FILE, claim); } catch { return 'gone'; }
  let raw = null;
  try { raw = fs.readFileSync(claim, 'utf-8'); } catch { /* 読めない claim は下で返却/破棄へ */ }
  let mine = false;
  try { mine = raw !== null && matchFn(raw); } catch { mine = false; }
  if (mine) {
    try { fs.unlinkSync(claim); } catch { /* 残置しても実害なし */ }
    return 'deleted';
  }
  try { fs.renameSync(claim, LOCK_FILE); return 'restored'; }
  catch {
    try { fs.unlinkSync(claim); } catch { /* 残置しても実害なし */ }
    return 'conflict';
  }
}

async function main() {
  const exitCode = process.argv[2] || '?';

  if (!fs.existsSync(LOCK_FILE)) {
    console.log('[notify-crash] lock なし = 通知済みの失敗経路。何もしない');
    return;
  }

  let lockInfo = '(lock 解析不能)';
  let observedRaw = null;
  try {
    observedRaw = fs.readFileSync(LOCK_FILE, 'utf-8');
    const lock = JSON.parse(observedRaw);
    lockInfo = `started_at=${lock.started_at}, pid=${lock.pid}`;
    // lock の pid が「生きている node プロセス」= 「多重起動中止で exit 1」パターン。
    // 先行 run は生きているので通知も lock 削除もしない (消すと先行 run のクラッシュ検知が壊れる)。
    // 単なる pid 生存ではなくプロセス名まで照合し、pid 再利用でクラッシュ通知を握り潰さない
    // (daily-sync.js の isAliveNodeProcess と同ロジック)
    if (lock.pid && isAliveNodeProcess(lock.pid)) {
      console.log(`[notify-crash] 先行 run が実行中 (pid=${lock.pid})。多重起動中止パターンのため何もしない`);
      return;
    }
  } catch { /* 破損 lock でも通知はする */ }

  const msg = `🚨 *Warehouse日次同期 プロセス異常終了* (exit=${exitCode})\n` +
    `daily-sync が通知に到達せずクラッシュ (${lockInfo})。` +
    `当該 run のジョブは途中までしか実行されていない。ログ確認を: logs\\daily-sync-*.log`;

  // lock 削除 = 「通知が実際に届いた」時のみ (Codex High #2)。
  // 未設定・fetch 例外・非 2xx では lock を残して exit 1 → 翌朝の起動時検知がフォールバックで拾う
  if (!GCHAT_WEBHOOK) {
    console.error('[notify-crash] GCHAT_WEBHOOK 未設定。通知不能 (lock 残置=翌朝検知に委譲):', msg);
    process.exitCode = 1;
    return;
  }
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: msg }),
    });
    if (!res.ok) {
      console.error(`[notify-crash] GChat 通知失敗: HTTP ${res.status} (lock 残置=翌朝検知に委譲)`);
      process.exitCode = 1;
      return;
    }
    console.log(`[notify-crash] GChat 通知送信: HTTP ${res.status}`);
  } catch (e) {
    console.error('[notify-crash] GChat 送信失敗 (lock 残置=翌朝検知に委譲):', e.message);
    process.exitCode = 1;
    return;
  }

  // 通知が届いたので lock を掃除 (翌朝の起動時検知と二重にならないように)。
  // read→compare→unlink は比較後の差し替え TOCTOU があるため、daily-sync.js と同じ
  // 「rename で claim → 中身検証 → 自分の読んだものだけ削除、他人のなら返却」プロトコルで消す (Codex R4)
  const result = claimAndDeleteLock(raw => observedRaw !== null && raw === observedRaw);
  if (result !== 'deleted') console.log(`[notify-crash] lock 削除スキップ (${result}) — 新 run の可能性、削除は翌朝検知に委譲`);
}

main().catch(e => {
  console.error('[notify-crash] エラー:', e.message);
  process.exit(1);
});
