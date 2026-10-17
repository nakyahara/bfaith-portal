/**
 * bc-import-runner.js — ポータルの「ロジザードに取り込む」ボタンの回を miniPC で動かす (2026-10-17 中原さん「ボタン押してその都度動かすものもほしい」)
 *
 * miniPC の定時 [Logizard-BcImport] (1 分おき・run-bc-import-poll.bat) が 1 回ずつ呼ぶ。1 回 = 次のとおり (動いていなければすぐ終わる):
 *   0. 前の回の終わりの知らせが送れていなければ、先に送る (logs\bc-import-pending\)。送れない = 新しい回は取らない
 *   1. ポータルに取りに来る (POST /apps/lz-bc-import/api/claim・Bearer LZ_BC_RUNNER_TOKEN)。
 *      この PC がほかのロジザードの作業中 (セッションの鍵 logizard-session.lock / logizard-barcode.lock がある)・夜 (JST 23:50〜01:30)・
 *      ほかの定時の前後 (毎時 :50〜:02 = 在庫の毎時の取込・08:26〜08:34 = 値札の CSV)・ロジザードの ID が無い = 「忙しい」と言うだけ (取らない。待ちは 15 分まで残る)
 *   2. 渡されたファイルを確かめる: sha256 が帳面と同じ・Shift-JIS で厳密に読める・見出しがロジザード登録の CSV (8 列) と同じ・行数が帳面と同じ。
 *      この PC で同じ中身を前に取り込んだ (logs\barcode_imported.json の import1) = 「もう一度」でなければ押さない
 *   3. ブラウザでロジザードを開いていない・② の保存先がある → セッションの鍵 (共通 = 毎晩の取込・在庫・値札と同じ) → ログイン (共通アカウント)
 *   4. ① 取込 [PM07/FM07_01] 商品マスタ / 新商品バーコード登録 (barcode-steps.js = auto-barcode.js と同じ手順・同じ守り)。
 *      **実行ボタンを押す直前にポータルへ「押す」**を知らせる。断られた・届かない = 押さない
 *   5. ① の結果 (総件数・エラー件数) を知らせる → (ボタンで選んだとき) ② 書き出し [PM08/FM08_01] → バーコードマスタ.csv (検証 NG = 前のまま)
 *   6. 終わりの知らせ (ログつき)。届かない = logs\bc-import-pending\ に残して次の回に送る
 * 分け方 (ポータルの帳面の状態):
 *   「押す」の前に止まった = not_executed (取り込んでいない)・① の結果がエラー / 件数が合わない = import_error・押した後に分からない = unknown (人が履歴で確かめる)
 * 途中で process が終わった (共通部品の process.exit など) = 終わりの知らせを pending に書いて終わる (押したかどうかもそのまま)。
 *
 * 使い方: node bc-import-runner.js          (定時から。何も無ければすぐ終わる)
 * env (このフォルダの .env): LOGIZARD_USER_ID / LOGIZARD_PASSWORD (共通アカウント)・LZ_BC_RUNNER_TOKEN・LZ_BC_PORTAL_URL (任意)・
 *   LOGIZARD_BC_OUT (② の保存先・既定 G:\共有ドライブ\入荷バーコード発行\バーコードマスタ.csv)・LOGIZARD_BC_EXPORT_DAYS・LOGIZARD_BC_MIN_ROWS・LOGIZARD_HEADLESS
 * 終了コード: 0 = 何も無い / 回を終えた (結果はポータルに)・1 = ポータルに届かない・設定の誤り
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { spawnSync } from 'child_process';
import { fileURLToPath, pathToFileURL } from 'url';
import { checkImportCsvBytes, LZ_BC_HEADER } from './barcode-steps.js';

export const VERSION = 'bc-runner-v1';
const DIR = path.dirname(fileURLToPath(import.meta.url));
const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');
const LOCK_STALE_MS = 30 * 60 * 1000;

/** JST 23:50〜01:30 (ポータルの「渡さない」と同じ・auto-barcode.js の夜の止め 00:00〜01:30 より前から) */
export function nightBusy(now = Date.now()) {
  const d = new Date(now + 9 * 3600e3);
  const m = d.getUTCHours() * 60 + d.getUTCMinutes();
  return m >= 23 * 60 + 50 || m < 90;
}

/**
 * ほかの定時の前後は取らない (JST): 毎時 :50〜:02 (在庫の毎時の取込 auto-zaiko.js・1 回 = 長くて 5 分ほどなので 00 分にかからないように)・08:26〜08:34 (値札の CSV 08:30)。
 * どちらも同じセッションの鍵を使い、鍵があると待たずに終わる = こちらが鍵を持っているとその回が抜ける (入荷受付の bat は 10 分待つので避けない)
 */
export function quietBusy(now = Date.now()) {
  const d = new Date(now + 9 * 3600e3);
  const h = d.getUTCHours(), m = d.getUTCMinutes();
  if (m >= 50 || m <= 2) return 'ほかの定時の前後 (毎時 00 分の在庫の取込)';
  if (h === 8 && m >= 26 && m <= 34) return 'ほかの定時の前後 (08:30 の値札の CSV)';
  return null;
}

const pidAlive = (pid) => {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; }
};
/** ロジザードのセッションの鍵 (共通・専用) が使われているか。古くて持ち主のいない鍵は acquireLock が片付ける = 使われていない扱い */
export function sessionLockBusy(logDir, now = Date.now()) {
  for (const name of ['logizard-session.lock', 'logizard-barcode.lock']) {
    const p = path.join(logDir, name);
    let st;
    try { st = fs.statSync(p); } catch { continue; }
    let j = null;
    try { j = JSON.parse(fs.readFileSync(p, 'utf8')); } catch { /* 壊れた鍵 */ }
    const alive = j && pidAlive(j.pid);
    if (alive || now - st.mtimeMs <= LOCK_STALE_MS) return `ほかのロジザードの作業中 (${name}${j && j.pid ? `・PID ${j.pid}` : ''})`;
  }
  return null;
}

/**
 * 渡されたファイルを確かめる (押す前)。だめ = 例外 (code = file_check)
 * @returns {{ dataRows: number }}
 */
export function verifyJobFile(buf, job, claimSha) {
  const bad = (m) => { const e = new Error(`ファイルの確かめ: ${m}`); e.code = 'file_check'; return e; };
  const h = sha256(buf);
  if (h !== job.file_sha256 || (claimSha && claimSha !== h)) throw bad(`sha256 が帳面と違う (${h.slice(0, 12)}… / 帳面 ${String(job.file_sha256).slice(0, 12)}…)`);
  let c;
  try { c = checkImportCsvBytes(buf, '取込CSV'); } catch (e) { throw bad(e.message); }
  const header = c.rows[0].map((x) => String(x).trim());
  if (header.join(',') !== LZ_BC_HEADER.join(',')) throw bad(`見出しがロジザード登録の CSV と違う (${header.join(',').slice(0, 120)})`);
  if (c.dataRows !== job.rows) throw bad(`行数が帳面と違う (${c.dataRows} 行 / 帳面 ${job.rows} 行)`);
  const badRow = c.rows.findIndex((r, i) => i > 0 && r.length !== LZ_BC_HEADER.length);
  if (badRow > 0) throw bad(`${badRow + 1} 行目の列の数が違う`);
  return { dataRows: c.dataRows };
}

/** 終わりの知らせを送る (届かない = pending に残す)。送れた / 送り直さなくてよい (形の断り) = true */
async function sendFinal(client, pendingDir, jobId, body0, { tries = 3, sleep = (ms) => new Promise((r) => setTimeout(r, ms)), log = console.log } = {}) {
  let body = body0;
  let shrunk = false;
  for (let i = 1; i <= tries; i++) {
    try {
      const r = await client.final(jobId, body);
      log(`📨 終わりを知らせた (#${jobId} → ${r.state}${r.already ? '・もう書いてあった' : ''})`);
      return true;
    } catch (e) {
      // 本文が大きすぎる = ログを短くして送り直す (日本語のログは文字の数より byte が多い。Codex R1 Medium)
      if (e && e.status === 413 && !shrunk && body.log) { shrunk = true; body = { ...body, log: tailBytes(body.log, 2000) }; log(`⚠ 終わりの知らせが大きすぎる (#${jobId}) → ログを短くして送り直す`); i--; continue; }
      // 鍵が違う・その回が無い = 送り直しても同じ (残さない)
      if (e && (e.status === 403 || e.status === 404 || e.status === 400)) { log(`⚠ 終わりの知らせを断られた (#${jobId}・${e.code}: ${e.message})`); return true; }
      log(`⚠ 終わりの知らせが届かない (#${jobId}・${i}/${tries}・${e && e.message})`);
      if (i < tries) await sleep(i * 5000);
    }
  }
  writePending(pendingDir, jobId, body);
  log(`📝 終わりの知らせを ${pendingDir} に残した (次の回に送る)`);
  return false;
}
function writePending(pendingDir, jobId, body) {
  fs.mkdirSync(pendingDir, { recursive: true });
  const f = path.join(pendingDir, `job-${jobId}.json`);
  const tmp = `${f}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ job_id: jobId, body }), 'utf8');
  fs.renameSync(tmp, f);
}
/** 残っている終わりの知らせを送る。送れないものが残る = false */
async function flushPending(client, pendingDir, log) {
  let files = [];
  try { files = fs.readdirSync(pendingDir).filter((f) => /^job-\d+\.json$/.test(f)); } catch { return true; }
  for (const f of files) {
    const p = path.join(pendingDir, f);
    let rec;
    try { rec = JSON.parse(fs.readFileSync(p, 'utf8')); } catch { log(`⚠ 読めない pending (${f}) は脇へ`); fs.renameSync(p, `${p}.broken`); continue; }
    const ok = await sendFinal(client, pendingDir, rec.job_id, rec.body, { tries: 1, log });
    if (!ok) return false;
    try { fs.unlinkSync(p); } catch { /* */ }
  }
  return true;
}

/** console の出力を写しておく (ポータルに送るログ) */
function captureConsole() {
  const lines = [];
  const orig = { log: console.log, error: console.error, warn: console.warn };
  const tee = (k) => (...a) => { lines.push(a.map((x) => (typeof x === 'string' ? x : x instanceof Error ? x.message : JSON.stringify(x))).join(' ')); orig[k](...a); };
  console.log = tee('log'); console.error = tee('error'); console.warn = tee('warn');
  return { text: () => tailBytes(lines.join('\n'), LOG_BYTES), restore: () => Object.assign(console, orig) };
}
/** ポータルに送るログの上限 (UTF-8 の byte・機械の口の本文の上限 256KB より十分小さく) */
export const LOG_BYTES = 24 * 1024;
/** 後ろの max byte を残す (文字の途中で切らない) */
export function tailBytes(s, max) {
  const b = Buffer.from(String(s || ''), 'utf8');
  if (b.length <= max) return String(s || '');
  return '…(前は省いた)\n' + b.subarray(b.length - max).toString('utf8').replace(/^\uFFFD+/, '');
}

/**
 * 1 回分。ロジザードの画面の操作は session (試験で差し替える)。
 * @param {object} p
 * @param {{ claim, executing, reportImport, final }} p.client  ポータルの機械の口 (bc-import-client.js)
 * @param {string} p.logDir   このフォルダの logs (鍵・pending・取込済みの記録)
 * @param {string} p.workDir  ファイルを置く所 (このフォルダの work\bc-import)
 * @param {() => string | null} p.localBusy  取らない理由 (null = 取ってよい)
 * @param {(job) => string | null} [p.afterClaimCheck]  取った後・押す前の確かめ (ブラウザでロジザードが開いている・② の保存先が無い など)。理由 = 押さない
 * @param {(o: { csvPath, withExport, beforeExecute, onImported, pressGuard }) => Promise<{ export: object | null }>} p.session  ロジザード (① ②)
 * @param {{ read: () => object, record: (info) => void }} p.imported  この PC の取込済みの記録 (logs\barcode_imported.json の import1)
 */
export async function runOnce({ client, logDir, workDir, localBusy, afterClaimCheck = () => null, session, imported, runnerId = 'minipc', now = () => Date.now(), log = console.log, sleep, onExitHook = (f) => process.once('exit', f), offExitHook = (f) => process.removeListener('exit', f) }) {
  const pendingDir = path.join(logDir, 'bc-import-pending');
  if (!(await flushPending(client, pendingDir, log))) return { state: 'pending_unsent' };
  const busy = localBusy();
  let r;
  try { r = await client.claim({ runner_id: runnerId, version: VERSION, busy }); } catch (e) {
    log(`❌ ポータルに届かない・断られた (${e && e.code}: ${e && e.message})`);
    return { state: 'portal_error', error: e };
  }
  if (!r.job) return { state: 'idle', why: r.why || null, busy };
  const job = r.job;
  const token = r.claim_token;
  const body = { claim_token: token, executed: false, import_result: null, export_result: null, reason: null, detail: null, log: '' };
  let finalSent = false;
  // 「押す」の後に押してよい間 (ポータルの press_window_ms・この PC の時計で数える)。過ぎたら実行ボタン・始める確認の OK を押さない (Codex R1 High)
  let pressUntil = null;
  const pressGuard = (where) => {
    if (pressUntil == null) return 30000;
    const left = pressUntil - now();
    if (left <= 0) { const e = new Error(`${where}: 「押す」から押してよい間を過ぎた (押さない)`); e.pressStop = true; throw e; }
    return left;
  };
  const cap = captureConsole();
  // 途中で process が終わった (共通部品の process.exit など) = 終わりの知らせを残す (押したかどうかもそのまま・次の回が送る)
  const onExit = () => {
    if (finalSent) return;
    try {
      writePending(pendingDir, job.job_id, { ...body, reason: body.reason || 'process_exit', detail: body.detail || 'miniPC の処理が途中で終わった (process.exit)', log: cap.text() });
    } catch { /* 残せない = ポータルの期限で分かる */ }
  };
  onExitHook(onExit);
  try {
    console.log(`===== ロジザードに取り込む #${job.job_id} (CSV #${job.export_id}・${job.rows} 行${job.with_export ? '・② も' : ''}${job.rerun ? '・もう一度' : ''}) =====`);
    const buf = Buffer.from(String(r.file_b64 || ''), 'base64');
    verifyJobFile(buf, job, job.file_sha256);
    console.log(`✔ ファイルの確かめ: sha256 ${job.file_sha256.slice(0, 12)}…・${job.rows} 行・見出し 8 列`);
    const prev = (imported.read() || {}).import1;
    if (prev && prev.sha256 === job.file_sha256 && !job.rerun) {
      const e = new Error(`この PC で同じ中身を前に取り込んだ (${prev.importedAt || '日時不明'})。もう一度なら画面で理由を書いて押す`); e.code = 'local_already_imported'; throw e;
    }
    const why = afterClaimCheck(job);
    if (why) { const e = new Error(why); e.code = 'precheck'; throw e; }
    const dir = path.join(workDir, `job-${job.job_id}`);
    fs.mkdirSync(dir, { recursive: true });
    const csvPath = path.join(dir, String(job.file_name || 'logizard_bc_upload.csv').replace(/[^\w.\-]/g, '_'));
    fs.writeFileSync(csvPath, buf, { flag: 'wx' });
    if (sha256(fs.readFileSync(csvPath)) !== job.file_sha256) { const e = new Error('置いたファイルの sha256 が違う'); e.code = 'file_check'; throw e; }
    const out = await session({
      csvPath, withExport: !!job.with_export, pressGuard,
      beforeExecute: async () => {
        const t0 = now();
        const x = await client.executing(job.job_id, token);   // 断られた・届かない = 例外 = 押さない
        pressUntil = t0 + Math.max(10e3, Math.min(Number(x && x.press_window_ms) || 90e3, 120e3)) - 5e3;   // 送る前の時刻から数える (応答が遅れた分も使ったことにする)
        body.executed = true;
        console.log('📨 ポータルに「押す」を知らせた → 実行ボタンを押します');
      },
      onImported: async (imp) => {
        pressUntil = null;   // ① の押すのは終わり (② は夜の止めだけで押す)
        body.import_result = { status: 'ok', summary: imp.summary || null, counts: imp.counts || null };
        try { imported.record({ sha256: job.file_sha256, path: csvPath, importedAt: new Date(now() + 9 * 3600e3).toISOString().replace('T', ' ').slice(0, 16) + ' (JST)', via: 'portal', job_id: job.job_id }); } catch (e) { console.log(`⚠ 取込済みの記録を残せない (${e.message})`); }
        let portal = null;
        try { portal = await client.reportImport(job.job_id, { claim_token: token, import_result: body.import_result, log: cap.text() }); } catch (e) { console.log(`⚠ ① の結果の知らせが届かない (終わりの知らせで送る): ${e && e.message}`); }
        // 件数が合わない (エラー件数 0・総件数 = 行数 でない) = ② はしない (ポータルの決まりと同じ・ポータルが ② に進めと言わないときも。Codex R1 Medium)
        const c = imp.counts || {};
        if (!(c.errors === 0 && c.total === job.rows)) { const e = new Error(`① の結果の件数が合わない (総件数 ${c.total ?? '読めない'}・エラー件数 ${c.errors ?? '読めない'}・CSV ${job.rows} 行)。② はしない`); e.code = 'count_mismatch'; throw e; }
        if (job.with_export && portal && portal.state !== 'exporting') { const e = new Error(`ポータルが ② に進めと言わない (状態 ${portal.state})。② はしない`); e.code = 'portal_stop'; throw e; }
      },
    });
    if (!body.import_result) throw new Error('① の結果が無いまま終わった');
    body.export_result = job.with_export
      ? (out && out.export && out.export.status === 'SUCCESS' ? { status: 'ok', rows: out.export.rows ?? null, detail: null } : { status: 'failed', rows: null, detail: '② の結果が無い' })
      : { status: 'skipped', rows: null, detail: null };
    console.log(`🎉 終わり: ① ${body.import_result.summary || ''}${job.with_export ? ` / ② ${body.export_result.status}` : ''}`);
  } catch (e) {
    const msg = String((e && e.message) || e).slice(0, 280);
    console.error(`❌ ${msg}`);
    if (!body.executed) {
      body.reason = e && e.nightBlock ? 'night' : (e && typeof e.code === 'string' && /^[a-z_]{2,40}$/.test(e.code) ? e.code : 'failed_before_execute');
      body.detail = `押す前に止めた (取り込んでいない): ${msg}`;
    } else if (body.import_result && e && (e.code === 'count_mismatch' || e.code === 'portal_stop')) {
      body.export_result = { status: 'skipped', rows: null, detail: msg };
      body.detail = msg;
    } else if (body.import_result) {
      body.export_result = { status: 'failed', rows: null, detail: msg };
      body.detail = `② で止まった: ${msg}`;
    } else if (e && e.importResult) {
      body.import_result = { status: 'error', summary: e.importResult.summary || null, counts: e.importResult.counts || null };
      body.detail = msg;
    } else {
      body.import_result = { status: 'unknown', summary: msg, counts: null };
      body.detail = `押した後に止まった (取り込めたか分からない): ${msg}`;
    }
  } finally {
    cap.restore();
  }
  body.log = cap.text();
  await sendFinal(client, pendingDir, job.job_id, body, { log, ...(sleep ? { sleep } : {}) });
  finalSent = true;
  offExitHook(onExit);
  return { state: 'ran', job_id: job.job_id, executed: body.executed, import_result: body.import_result, export_result: body.export_result, reason: body.reason };
}

// ── ここから下は miniPC で本当に動かすときだけ (試験は runOnce に偽物を渡す) ──

/** 自分の二重起動を止める (定時が重なった) */
function holdRunnerLock(logDir) {
  fs.mkdirSync(logDir, { recursive: true });
  const p = path.join(logDir, 'bc-import-runner.lock');
  for (let i = 0; i < 2; i++) {
    try {
      fs.writeFileSync(p, JSON.stringify({ pid: process.pid, at: new Date().toISOString() }), { flag: 'wx' });
      const release = () => { try { const j = JSON.parse(fs.readFileSync(p, 'utf8')); if (j.pid === process.pid) fs.unlinkSync(p); } catch { /* */ } };
      process.once('exit', release);
      return release;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      let j = null;
      try { j = JSON.parse(fs.readFileSync(p, 'utf8')); } catch { /* */ }
      if (j && pidAlive(j.pid)) return null;
      try { fs.unlinkSync(p); } catch { return null; }
    }
  }
  return null;
}

/** 1 時間に 1 回だけ jobs-monitor に生きている印 (台帳 lz-bc-import-runner・heartbeat)。ping.ps1 は失敗しても 0 で終わる */
function maybePing(logDir, now = Date.now()) {
  const mark = path.join(logDir, 'bc-import-runner-ping.json');
  let last = 0;
  try { last = JSON.parse(fs.readFileSync(mark, 'utf8')).at || 0; } catch { /* */ }
  if (now - last < 55 * 60e3) return;
  const ps1 = process.env.LZ_BC_PING_PS1 || 'C:\\Users\\bfaith\\bfaith-portal\\scripts\\jobs-monitor\\ping.ps1';
  if (!fs.existsSync(ps1)) return;
  spawnSync('powershell', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', ps1, '-Id', 'lz-bc-import-runner', '-Status', 'ok'], { timeout: 30000, windowsHide: true });
  try { fs.writeFileSync(mark, JSON.stringify({ at: now })); } catch { /* */ }
}

async function main() {
  const common = await import('./logizard-common.js');
  const { createBarcodeSteps, makeWithRelogin, IMPORT1_PATTERN_DEFAULT } = await import('./barcode-steps.js');
  const mode = await import('./barcode-mode.js');
  const { createBcImportClient } = await import('./bc-import-client.js');
  common.loadEnv();
  common.assertLocalWriteDirs();
  const logDir = common.LOG_DIR;
  const release = holdRunnerLock(logDir);
  if (!release) { console.log('ℹ 前の回がまだ動いている (何もしない)'); return 0; }
  const cfg = {
    importFiletypeLabel: process.env.LOGIZARD_BC_FILETYPE || '商品マスタ',
    exportTypeLabel: process.env.LOGIZARD_BC_EXPORT_TYPE || 'SKU',
    exportPattern: process.env.LOGIZARD_BC_EXPORT_PATTERN || 'バーコード情報',
    exportDays: common.requireIntEnv('LOGIZARD_BC_EXPORT_DAYS', 30, { min: 1, max: 365 }),
    minRows: common.requireIntEnv('LOGIZARD_BC_MIN_ROWS', 1, { min: 0, max: 10000000 }),
    exportOut: (process.env.LOGIZARD_BC_OUT || 'G:\\共有ドライブ\\入荷バーコード発行\\バーコードマスタ.csv').trim(),
  };
  const pattern = process.env.LOGIZARD_BC_IMPORT1_PATTERN || IMPORT1_PATTERN_DEFAULT;
  const headless = (process.env.LOGIZARD_HEADLESS || '0') === '1';
  let client;
  try { client = createBcImportClient(); } catch (e) { console.error(`❌ ${e.message}`); return 1; }
  const importedFile = path.join(logDir, 'barcode_imported.json');
  const imported = {
    read: () => { try { return JSON.parse(fs.readFileSync(importedFile, 'utf8')); } catch { return {}; } },
    record: (info) => { let s = {}; try { s = JSON.parse(fs.readFileSync(importedFile, 'utf8')); } catch { /* */ } s.import1 = info; fs.writeFileSync(importedFile, JSON.stringify(s, null, 2), 'utf8'); },
  };
  const localBusy = () => {
    if (nightBusy()) return 'night';
    const quiet = quietBusy();
    if (quiet) return quiet;
    if (!process.env.LOGIZARD_USER_ID || !process.env.LOGIZARD_PASSWORD) return 'no_credentials (.env の LOGIZARD_USER_ID / PASSWORD)';
    return sessionLockBusy(logDir);
  };
  const afterClaimCheck = (job) => {
    if (job.with_export && !fs.existsSync(path.dirname(cfg.exportOut))) return `② の保存先フォルダが無い (${path.dirname(cfg.exportOut)})。② なしで押すか、共有ドライブを確かめる`;
    const lockWhy = sessionLockBusy(logDir);
    if (lockWhy) return lockWhy;
    return null;
  };
  const session = async ({ csvPath, withExport, beforeExecute, onImported, pressGuard }) => {
    common.assertNoLogizardBrowserOpen();   // 開いている = その場で終わる (押していない・終わりの知らせは pending に残る)
    common.acquireLock();   // 共通の鍵 (毎晩の取込・在庫・値札と同じ)。取れない = その場で終わる (押していない)
    const releaseOnExit = () => { try { common.releaseLock(); } catch { /* */ } };
    process.once('exit', releaseOnExit);
    try {
      const { browser, page } = await common.launchBrowser({ headless });
      try {
        const steps = createBarcodeSteps({ page, dry: false, cfg, beforeExecute, pressGuard });
        const loginOpts = () => ({ label: 'ボタンの取込 (共通アカウント)', beforeSubmit: () => mode.clickBudgetMs('ログインのボタンの前') });
        const withRelogin = makeWithRelogin({ page, loginOpts });
        mode.assertOutsideNightBlock('ログインの前');
        await common.login(page, loginOpts()).catch((err) => { throw mode.asNightError(err, 'ログインのボタンの前'); });
        mode.assertOutsideNightBlock('①の前');
        const imp = await withRelogin('①', () => steps.runImport(csvPath, pattern, '①新商品バーコード登録'));
        await onImported(imp);
        let exp = null;
        if (withExport) {
          mode.assertOutsideNightBlock('②の前');
          exp = await withRelogin('②', () => steps.runExport());
        }
        steps.assertNoUnexpectedDialog();
        return { export: exp };
      } finally {
        await browser.close().catch(() => {});
      }
    } finally {
      common.releaseLock();
      process.removeListener('exit', releaseOnExit);
    }
  };
  const r = await runOnce({ client, logDir, workDir: path.join(DIR, 'work', 'bc-import'), localBusy, afterClaimCheck, session, imported, runnerId: process.env.COMPUTERNAME || 'minipc' });
  if (r.state === 'portal_error' || r.state === 'pending_unsent') return 1;
  maybePing(logDir);
  if (r.state === 'idle' && r.busy) console.log(`ℹ 忙しい (${r.busy})`);
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().then((code) => process.exit(code), (e) => { console.error(`❌ ${e && e.stack || e}`); process.exit(1); });
}
