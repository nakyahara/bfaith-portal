/**
 * lz-bc-import/drive-export.js — ② バーコードマスタ.csv を共有ドライブ (Google Drive の API) に書く (Render のポータルが書く)
 *
 * なぜ (2026-10-21 中原さん「WEB 上の G ドライブのファイルを上げたほうがいいんじゃない？」):
 *   miniPC には共有ドライブ (G:) が無い = miniPC が G: に書く道では ② ができず、ボタンの回が「押す前に止めた」で終わっていた。
 *   → miniPC は ② の書き出しをこの PC の作業用フォルダに置き、機械の口 (runner-router.js の /jobs/:id/export-file) でポータルに送る。
 *     ポータルが中身を確かめて (Shift_JIS のまま・見出し・行数の下限・大きさ)、Drive の **同じファイル ID** を上書きする (リンクを変えない・新しいファイルを作らない)。
 * 書く先: 共有ドライブ「入荷バーコード発行」(ドライブ ID 0AN1RrVoXZhRqUk9PVA) の「バーコードマスタ.csv」(ファイル ID 1nFY-9njrrzRgHNZcFUFJX3QNxGhrGD_0)。
 *   env LZ_BC_DRIVE_FILE_ID で差し替えられる (ドライブ ID・ファイル名は変えない = 違うドライブ・違う名前のファイルには書かない)。
 * 🚨 書くのは env LZ_BC_DRIVE_WRITE=1 のときだけ (既定 = 書かない = ② は押せない形)。出した後に中原さんが決めて Render の env で入れる
 *   (コードを出しただけでは共有ドライブのファイルを変えない)。
 * 書き手: サービスアカウント GOOGLE_SERVICE_ACCOUNT_KEY (fba-replenishment/drive-upload.js と同じ鍵・scope drive)。共有ドライブ = supportsAllDrives。
 *   共有ドライブで「コンテンツ管理者」以上が要る (閲覧者 = capabilities.canEdit が false = 書けない = ② は「書けなかった (権限)」・① は済ませる)。
 */
import crypto from 'node:crypto';
import { Readable } from 'node:stream';

export const DRIVE_FILE_ID_DEFAULT = '1nFY-9njrrzRgHNZcFUFJX3QNxGhrGD_0';
export const DRIVE_ID = '0AN1RrVoXZhRqUk9PVA';
export const DRIVE_NAME = '入荷バーコード発行';
export const DRIVE_FILE_NAME = 'バーコードマスタ.csv';
/** 行数の下限 (9/29 の全件 = 5,188 行・tools/logizard-automation/barcode-export.js の BARCODE_MIN_ROWS と同じ) */
export const EXPORT_MIN_ROWS = 4000;
export const EXPORT_MAX_BYTES = 3 * 1024 * 1024;
const TIMEOUT = 20000;
const OK_TTL_MS = 10 * 60e3;     // 書ける と分かったら 10 分は確かめ直さない
const NG_TTL_MS = 2 * 60e3;      // 書けない・確かめられない = 2 分で確かめ直す (権限を上げてもらった後にすぐ戻る)

/** 書けない理由の一言 (画面に出す・中原さん向けに短く) */
export const DRIVE_WORDS = Object.freeze({
  off: 'ポータルから共有ドライブに書く設定 (LZ_BC_DRIVE_WRITE) がまだ入っていない',
  no_key: 'ポータルに Google Drive の鍵 (GOOGLE_SERVICE_ACCOUNT_KEY) が無い',
  no_permission: `ポータルが共有ドライブ「${DRIVE_NAME}」に書けない (閲覧者のまま。コンテンツ管理者に上げてもらう)`,
  not_found: `共有ドライブ「${DRIVE_NAME}」の「${DRIVE_FILE_NAME}」が見えない`,
  wrong_file: `書く先のファイルが「${DRIVE_NAME}」の「${DRIVE_FILE_NAME}」でない`,
  unreachable: 'Google Drive に届かない',
  checking: '書く先 (Google Drive) を確かめています',
});

/** Drive の RFC3339 → 「YYYY-MM-DD HH:mm」(JST) */
export function jstOf(iso) {
  const t = Date.parse(iso || '');
  if (Number.isNaN(t)) return null;
  return new Date(t + 9 * 3600e3).toISOString().slice(0, 16).replace('T', ' ');
}

/** 引用符・"" ・引用符の中の改行に対応した素朴な CSV の読み (tools/logizard-automation/csv-util.js と同じ形)。閉じていない引用符 = 例外 */
function parseCsv(text) {
  const rows = [];
  let row = [], field = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; } else field += c; continue; }
    if (c === '"') inQ = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\r') { /* CRLF */ }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else field += c;
  }
  if (inQ) throw new Error('引用符が閉じていない (途中で切れた疑い)');
  if (!(field === '' && row.length === 0)) { row.push(field); rows.push(row); }
  return rows;
}

/**
 * 送られてきた ② の中身を確かめる (Shift_JIS の byte 列のまま。変えない)。だめ = { ok: false, reason }
 * @returns {{ ok: true, rows: number } | { ok: false, reason: string }}
 */
export function checkBarcodeMasterBytes(buf, { minRows = EXPORT_MIN_ROWS, maxBytes = EXPORT_MAX_BYTES } = {}) {
  if (!Buffer.isBuffer(buf) || !buf.length) return { ok: false, reason: '中身が空' };
  if (buf.length > maxBytes) return { ok: false, reason: `大きすぎる (${Math.round(buf.length / 1024)} KB)` };
  const head = buf.subarray(0, 2000).toString('latin1');
  if (/<html|<!DOCTYPE|user_id|SUSPENDED/i.test(head)) return { ok: false, reason: 'CSV でなく HTML (ログインの画面など)' };
  if (buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf) return { ok: false, reason: 'UTF-8 (BOM つき) になっている (Shift_JIS のまま送る)' };
  let text;
  try { text = new TextDecoder('shift_jis', { fatal: true }).decode(buf); } catch { return { ok: false, reason: 'Shift_JIS として読めない' }; }
  let rows;
  try { rows = parseCsv(text); } catch (e) { return { ok: false, reason: `CSV として読めない (${e.message})` }; }
  if (!rows.length) return { ok: false, reason: '行が無い' };
  const header = rows[0].map((h) => String(h).trim());
  for (const c of ['商品ID', 'バーコード']) {
    if (header.indexOf(c) < 0) return { ok: false, reason: `見出しに「${c}」が無い (${header.slice(0, 8).join(' / ')})` };
    if (header.indexOf(c) !== header.lastIndexOf(c)) return { ok: false, reason: `見出しの「${c}」が 2 つある` };
  }
  const blank = (r) => r.length === 1 && String(r[0] || '').trim() === '';
  const iId = header.indexOf('商品ID'), iBc = header.indexOf('バーコード');
  let n = 0, noBc = 0;
  for (let i = 1; i < rows.length; i++) {
    if (blank(rows[i])) continue;
    if (rows[i].length !== header.length) return { ok: false, reason: `${i + 1} 行目の列の数が見出しと違う` };
    // 商品ID が空の行 = 壊れた書き出し (入荷の照合に使えない。Codex R1 Medium)
    if (!String(rows[i][iId] || '').trim()) return { ok: false, reason: `${i + 1} 行目の商品ID が空` };
    if (!String(rows[i][iBc] || '').trim()) noBc++;
    n++;
  }
  if (n < minRows) return { ok: false, reason: `行が少なすぎる (${n} 行 < 下限 ${minRows} 行)` };
  if (noBc > n * 0.1) return { ok: false, reason: `バーコードが空の行が多すぎる (${noBc} / ${n} 行)` };
  return { ok: true, rows: n };
}

export class DriveWriteError extends Error {
  constructor(code, message) { super(message); this.code = code; }
}

/** 本物の Drive (書く scope)。鍵が無い = DriveWriteError no_key */
async function realDrive() {
  const keyB64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyB64) throw new DriveWriteError('no_key', DRIVE_WORDS.no_key);
  let key;
  try { key = JSON.parse(Buffer.from(keyB64, 'base64').toString('utf8')); } catch { throw new DriveWriteError('no_key', 'GOOGLE_SERVICE_ACCOUNT_KEY が読めない'); }
  const { google } = await import('googleapis');
  const auth = new google.auth.GoogleAuth({ credentials: key, scopes: ['https://www.googleapis.com/auth/drive'] });
  return google.drive({ version: 'v3', auth });
}

const httpStatus = (e) => Number((e && (e.code || e.status || (e.response && e.response.status))) || 0);

/**
 * 書く先 (1 つのファイル)。status = 書けるか (覚えておく)・peek = 覚えている答え (無い・古い = 裏で確かめ直す)・upload = 上書き
 * @param {object} [o]
 * @param {() => Promise<object>} [o.getDrive]  googleapis の drive (試験で差し替える)
 * @param {() => boolean} [o.enabled]  書いてよいか (既定 = env LZ_BC_DRIVE_WRITE=1)
 * @param {() => string} [o.fileId]
 * @param {() => number} [o.now]
 */
export function createDriveTarget({ getDrive = realDrive, enabled = () => process.env.LZ_BC_DRIVE_WRITE === '1', fileId = () => (process.env.LZ_BC_DRIVE_FILE_ID || DRIVE_FILE_ID_DEFAULT).trim(), now = () => Date.now() } = {}) {
  let cache = null;      // { ok, code, message, name, modified_time, size, checked_at }
  let inflight = null;
  async function check() {
    const at = now();
    const ng = (code, extra = '') => ({ ok: false, code, message: DRIVE_WORDS[code] + extra, checked_at: at });
    if (!enabled()) return ng('off');
    let drive;
    try { drive = await getDrive(); } catch (e) { return ng(e && e.code === 'no_key' ? 'no_key' : 'unreachable', e && e.code === 'no_key' ? '' : ` (${String(e && e.message).slice(0, 120)})`); }
    let m;
    try {
      m = (await drive.files.get({ fileId: fileId(), fields: 'id, name, driveId, trashed, mimeType, modifiedTime, size, capabilities(canEdit)', supportsAllDrives: true }, { timeout: TIMEOUT })).data;
    } catch (e) {
      const s = httpStatus(e);
      if (s === 404) return ng('not_found');
      if (s === 403 || s === 401) return ng('no_permission');
      return ng('unreachable', ` (${String(e && e.message).slice(0, 120)})`);
    }
    if (!m || m.name !== DRIVE_FILE_NAME || m.driveId !== DRIVE_ID || m.trashed || m.mimeType === 'application/vnd.google-apps.folder') return ng('wrong_file', ` (${m && m.name})`);
    if (!(m.capabilities && m.capabilities.canEdit)) return ng('no_permission');
    return { ok: true, code: null, message: null, name: m.name, modified_time: m.modifiedTime || null, size: m.size == null ? null : Number(m.size), checked_at: at };
  }
  const fresh = (c) => c && now() - c.checked_at < (c.ok ? OK_TTL_MS : NG_TTL_MS);
  async function status({ force = false } = {}) {
    if (!enabled()) return { ok: false, code: 'off', message: DRIVE_WORDS.off, checked_at: now() };
    if (!force && fresh(cache)) return cache;
    if (!inflight) inflight = check().then((c) => { cache = c; return c; }).finally(() => { inflight = null; });
    return inflight;
  }
  function peek() {
    if (!enabled()) return { ok: false, code: 'off', message: DRIVE_WORDS.off, checked_at: now() };
    if (!fresh(cache)) status().catch(() => {});
    return cache || { ok: false, code: 'checking', message: DRIVE_WORDS.checking, checked_at: null };
  }
  /**
   * 上書き (同じファイル ID・中身だけ)。書く前に確かめ直す (書いてよい設定・権限・同じファイル・今の大きさの半分より小さくない)。
   * @returns {Promise<{ modified_time: string | null }>}  だめ = DriveWriteError (code = off / no_key / no_permission / not_found / wrong_file / unreachable / shrunk / mismatch)
   */
  async function upload(buf) {
    if (!enabled()) throw new DriveWriteError('off', DRIVE_WORDS.off);
    const st = await status({ force: true });
    if (!st.ok) throw new DriveWriteError(st.code, st.message);
    if (st.size && buf.length < st.size / 2) throw new DriveWriteError('shrunk', `今の Drive のファイル (${Math.round(st.size / 1024)} KB) の半分より小さい (${Math.round(buf.length / 1024)} KB)。書かない`);
    const drive = await getDrive();
    let d;
    try {
      d = (await drive.files.update({ fileId: fileId(), media: { mimeType: 'text/csv', body: Readable.from(buf) }, fields: 'id, modifiedTime, md5Checksum, size', supportsAllDrives: true }, { timeout: 60000 })).data;
    } catch (e) {
      const s = httpStatus(e);
      cache = null;
      if (s === 403 || s === 401) throw new DriveWriteError('no_permission', DRIVE_WORDS.no_permission);
      if (s === 404) throw new DriveWriteError('not_found', DRIVE_WORDS.not_found);
      if (s >= 400 && s < 500) throw new DriveWriteError('rejected', `Google Drive が断った (HTTP ${s}: ${String(e && e.message).slice(0, 120)})`);
      // 届いたか分からない (時間切れ・通信が切れた・5xx) = 書けたかもしれない = 「前のまま」と言わない (Codex R1 Medium)
      throw new DriveWriteError('uncertain', `Google Drive の答えが無い (${String(e && e.message).slice(0, 120)})`);
    }
    const md5 = crypto.createHash('md5').update(buf).digest('hex');
    if (!d || !d.md5Checksum || d.md5Checksum !== md5) { cache = null; throw new DriveWriteError('uncertain', 'Drive に書いた後の中身の印 (md5) を確かめられない・送ったものと違う'); }
    cache = { ...st, modified_time: (d && d.modifiedTime) || null, size: buf.length, checked_at: now() };
    return { modified_time: (d && d.modifiedTime) || null };
  }
  return { status, peek, upload };
}

let shared = null;
export function getDriveTarget() { return (shared ||= createDriveTarget()); }
export function __setDriveTarget(t) { shared = t; }

/**
 * ② (バーコードマスタの書き出し) ができるか (画面の印・積むときの決まり)。
 *   miniPC が「② のファイルをポータルに送れる」と言っている (export_upload = true・新しい runner) **かつ** ポータルが Drive のファイルに書ける
 * @param {{ export_upload?: boolean | null }} runner  runnerView の答え
 * @param {{ ok, code, message }} drive  status / peek の答え
 * @returns {{ ok: boolean, message: string | null, code: string | null }}
 */
export function exportAvailability(runner, drive) {
  const tail = 'ので、しません。今までどおり Stream Deck で書き出してください';
  if (!runner || runner.export_upload !== true) return { ok: false, code: 'runner_old', message: `② (バーコードマスタの書き出し) は、miniPC の更新がまだな${tail}` };
  if (!drive || !drive.ok) return { ok: false, code: (drive && drive.code) || 'checking', message: `② (バーコードマスタの書き出し) は、${(drive && drive.message) || DRIVE_WORDS.checking}${tail}` };
  return { ok: true, code: null, message: null };
}
