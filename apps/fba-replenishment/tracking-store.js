/**
 * 追跡番号を「いつ・どの納品の・どの箱に・どの番号で」投入したかの自前記録。
 *
 * ⭐なぜAPIでなく自前で持つのか (2026-08-07 実測):
 *   Seller Central 画面で追跡番号を入れると status は即 SHIPPED になるのに、
 *   `GET /shipments/{id}` の trackingDetails は **35分後もまだ null** だった。
 *   反映は分単位ではない。つまり「APIが null だから未登録」と判断すると
 *   同じ納品へ二重投入する。冪等判定は必ずこの記録を第一とし、
 *   APIの trackingDetails は事後確認にのみ使う。
 *
 * 形式は追記専用の JSONL。sql.js のように毎回ファイル全体を書き戻さないので、
 * ポータル本体が同時に動いていても既存の記録を巻き込んで壊さない。
 * 1日あたり数十行なので線形探索で足りる。
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const STORE_FILE = path.join(DATA_DIR, 'fba-tracking-log.jsonl');

export function storePath() {
  return STORE_FILE;
}

function ensureDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

/** 全件読む。壊れた行は捨てずに記録して読み飛ばす (追記専用なので途中破損は最終行に出やすい)。 */
export function readAll() {
  if (!fs.existsSync(STORE_FILE)) return { entries: [], brokenLines: [] };
  const lines = fs.readFileSync(STORE_FILE, 'utf-8').split(/\r?\n/).filter((l) => l.trim() !== '');
  const entries = [];
  const brokenLines = [];
  lines.forEach((l, i) => {
    let e = null;
    try { e = JSON.parse(l); } catch { brokenLines.push(i + 1); return; }
    // JSONとして読めても、必要な欄が欠けていれば「読めない行」と同じ扱いにする。
    // 途中書きで {} だけが残る等を通すと、pending を見失って再送してしまう
    if (!e || typeof e !== 'object' || !e.shipmentConfirmationId || !e.result) {
      brokenLines.push(i + 1);
      return;
    }
    entries.push(e);
  });
  return { entries, brokenLines };
}

/**
 * この納品はもう投入済みか。
 * @param {string} shipmentConfirmationId 納品番号 (FBA15GG2MM9B)
 * @returns {object|null} 投入済みなら最後の成功記録
 */
export function findSuccess(shipmentConfirmationId) {
  const { entries } = readAll();
  for (let i = entries.length - 1; i >= 0; i--) {
    const e = entries[i];
    if (e.shipmentConfirmationId === shipmentConfirmationId && e.result === 'success') return e;
  }
  return null;
}

/**
 * 同じ内容を投入しようとしていないか (再実行時に完全同値なら送らない)。
 * items の並びまで含めて比較する。
 */
export function isSameAsRecorded(shipmentConfirmationId, items) {
  const rec = findSuccess(shipmentConfirmationId);
  if (!rec) return false;
  const a = (rec.items ?? []).map((x) => `${x.boxId}=${x.trackingId}`).join(',');
  const b = (items ?? []).map((x) => `${x.boxId}=${x.trackingId}`).join(',');
  return a === b && a !== '';
}

/**
 * 1件追記する。
 * @param {object} entry {runId, shipmentConfirmationId, shipmentId, inboundPlanId, fcCode,
 *                        matchedBy, items:[{boxId,trackingId,送り状番号}], result:'success'|'failed'|'skipped',
 *                        operationId?, error?, sourceFile?, sourceHash?}
 */
export function append(entry) {
  ensureDir();
  const line = JSON.stringify({ at: new Date().toISOString(), ...entry });
  fs.appendFileSync(STORE_FILE, line + '\n', 'utf-8');
  return line;
}

/**
 * 納品ごとの**最後の**記録を返す (成否を問わない)。
 * ⭐`pending` が最後に残っている = 「PUTは送ったが結果を書けずに落ちた」状態。
 *   APIの trackingDetails は数時間反映されないので、自動で送り直すと二重投入になる。
 *   人がSeller Central画面で確認するまで触らない。
 */
export function findLatest(shipmentConfirmationId) {
  const { entries } = readAll();
  for (let i = entries.length - 1; i >= 0; i--) {
    if (entries[i].shipmentConfirmationId === shipmentConfirmationId) return entries[i];
  }
  return null;
}

const LOCK_FILE = () => path.join(DATA_DIR, 'fba-tracking.lock');
const LOCK_STALE_MS = 30 * 60 * 1000; // これを超えて残っていたら落ちた実行の置き土産とみなす

function readLock() {
  try {
    const raw = fs.readFileSync(LOCK_FILE(), 'utf-8');
    if (!raw.trim()) return null; // 作成直後で中身がまだ書かれていない
    return JSON.parse(raw);
  } catch { return null; }
}

/**
 * 多重起動を止める。書き込みAPIを2本同時に走らせると、記録を確認してからPUTするまでの
 * 隙間で同じ納品へ二重投入しうる (手動実行と定期実行が重なる等)。
 *
 * ⭐**排他的作成 (wx) を使う。** 「存在を見てから書く」だと、2プロセスが同時に
 *   「無い」を見て両方ロックを取れてしまう。
 * ⭐**古いロックの回収は rename でやる。** 「読んでから unlink」だと、その隙に別プロセスが
 *   張り直した新しいロックを消してしまう。rename は原子的なので、
 *   **回収に成功した1プロセスだけ**が先に進める (2026-08-09 Codex 3巡目の指摘)。
 * @returns {{ok: boolean, reason?: string}}
 */
export function acquireLock(runId) {
  ensureDir();
  const body = () => JSON.stringify({ runId, at: new Date().toISOString(), pid: process.pid });
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const fd = fs.openSync(LOCK_FILE(), 'wx'); // 既にあれば EEXIST = 原子的
      try { fs.writeSync(fd, body()); } finally { fs.closeSync(fd); }
      return { ok: true };
    } catch (e) {
      if (e?.code !== 'EEXIST') return { ok: false, reason: `ロックを作成できません: ${e?.message ?? e}` };
      const info = readLock();
      let age = 0;
      try { age = Date.now() - (Date.parse(info?.at ?? '') || fs.statSync(LOCK_FILE()).mtimeMs); } catch { continue; }
      if (age < LOCK_STALE_MS) {
        return { ok: false, reason: `別の実行が動いています (runId=${info?.runId ?? '不明'} / ${Math.round(age / 1000)}秒前に開始)` };
      }
      // 落ちた実行の置き土産とみなして回収する。rename に成功した者だけが消せる
      const tomb = `${LOCK_FILE()}.stale.${runId}.${attempt}`;
      try {
        fs.renameSync(LOCK_FILE(), tomb);
        fs.unlinkSync(tomb);
      } catch { /* 他が先に回収した。次の試行でやり直す */ }
    }
  }
  return { ok: false, reason: 'ロックを取得できませんでした (競合)' };
}

/**
 * 実行が長引いても stale と誤判定されないよう、ロックの時刻を更新する。
 * これをしないと、納品が増えて30分を超えた実行が「落ちた」とみなされ、
 * 別プロセスに追い越される。
 */
export function touchLock(runId) {
  const info = readLock();
  if (!info || info.runId !== runId) return false; // 自分のロックでなければ触らない
  try {
    fs.writeFileSync(LOCK_FILE(), JSON.stringify({ ...info, at: new Date().toISOString() }), 'utf-8');
    return true;
  } catch { return false; }
}

/**
 * ⭐**自分が取ったロックのときだけ消す。**
 * 無条件に消すと、stale回収でロックを引き継いだ別プロセスのロックまで壊す。
 * 中身が読めないとき (作成途中の可能性) も触らない — 放置しても stale で回収される。
 */
export function releaseLock(runId) {
  const info = readLock();
  if (!runId) return;                       // 誰のものか分からないなら触らない
  if (!info || info.runId !== runId) return; // 他人のロック / 読めない = 触らない
  try { fs.unlinkSync(LOCK_FILE()); } catch { /* 既に無ければよい */ }
}

/**
 * このCSVファイル(内容ハッシュ)を既に処理したか。
 * 固定ファイル名運用で「同じファイルが置きっぱなし」のときに二度処理しないための保険。
 * 出荷日チェックと二重の防御にする (日付が同じでも中身が増えている場合は別ハッシュになる)。
 */
export function findBySourceHash(sourceHash) {
  if (!sourceHash) return null;
  const { entries } = readAll();
  return entries.find((e) => e.sourceHash === sourceHash && e.result === 'success') ?? null;
}
