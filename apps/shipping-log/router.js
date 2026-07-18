/**
 * 出荷実績ログ (shipping-log) — GAS 取込 API
 *
 * 出荷_no 掃除 GAS 専用の Bearer 認証エンドポイント。session 認証の外側に mount する
 * (server.js で requireAppAccess を通さない。packing-dispatch の ne-sync-worker と同じ構成)。
 *
 * 認証:
 *  - POST /ingest : SHIPPING_LOG_INGEST_TOKEN (env)。未設定なら 503 で fail-closed。
 *  - GET /recent  : MIRROR_READ_TOKEN (既存の read-only token、x-read-token ヘッダ)。
 *    取込用 token が漏れても参照はできない (Codex R1 low #9: 書込/読取の権限分離)。
 *
 * GAS 側の契約: POST /ingest が 200 かつ body.ok===true かつ inserted+ignored===total の
 * 場合のみ、該当フォルダのファイルを削除してよい。conflict 検出時は 409。
 */
import express from 'express';
import crypto from 'node:crypto';
import { ingestFolderSlips, recentSlips, getSchemaError } from './db.js';

const router = express.Router();

const MAX_ROWS = 3000;        // 1フォルダの伝票数上限 (通常は数十件)
const MAX_FIELD_LEN = 200;
const SLIP_NO_RE = /^SP\d{8,14}$/; // ロジザード出荷伝票NO (現行 SP+11桁、余裕を持たせる)

function timingSafeEq(got, expected) {
  const a = Buffer.from(got, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function requireIngestKey(req, res, next) {
  const expected = process.env.SHIPPING_LOG_INGEST_TOKEN || '';
  if (!expected) {
    // env 未設定 → fail-closed (絶対 fail-open しない)
    return res.status(503).json({ ok: false, error: 'not_configured',
      message: 'SHIPPING_LOG_INGEST_TOKEN が未設定です。サーバ管理者に連絡してください。' });
  }
  const m = /^Bearer\s+(\S+)$/.exec(String(req.headers.authorization || ''));
  if (!m) return res.status(401).json({ ok: false, error: 'unauthorized', message: 'Bearer token がありません' });
  if (!timingSafeEq(m[1], expected)) {
    console.warn('[shipping-log] ingest auth failed', { ip: req.ip, ua: req.headers['user-agent'] });
    return res.status(403).json({ ok: false, error: 'forbidden', message: 'Bearer token が一致しません' });
  }
  next();
}

// 読取は既存の mirror read-only token 系に合わせる (header のみ、query fallback なし、fail-closed)
function requireReadToken(req, res, next) {
  const expected = process.env.MIRROR_READ_TOKEN || '';
  if (!expected) return res.status(503).json({ ok: false, error: 'mirror_read_token_unset' });
  const got = String(req.headers['x-read-token'] || '');
  if (!got || !timingSafeEq(got, expected)) {
    return res.status(401).json({ ok: false, error: 'invalid_read_token' });
  }
  next();
}

function vErr(msg) { const e = new Error(msg); e.code = 'VALIDATION'; return e; }

function fieldStr(v, name, { required = false } = {}) {
  if (v == null || v === '') {
    if (required) throw vErr(`${name} は必須です`);
    return null;
  }
  if (typeof v !== 'string') throw vErr(`${name} は文字列で指定してください`);
  const s = v.trim();
  if (s.length > MAX_FIELD_LEN) throw vErr(`${name} が長すぎます (${MAX_FIELD_LEN}文字まで)`);
  return s || null;
}

// YYYY-MM-DD 形式 + 実在日 (Codex R1 medium #7: 2026-02-31 等を拒否。PK でなくなったが恒久保存のため)
function validateShipDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) throw vErr('ship_date は YYYY-MM-DD 形式で指定してください');
  const [y, m, d] = s.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== m - 1 || dt.getUTCDate() !== d) {
    throw vErr(`ship_date が実在しない日付です: ${s}`);
  }
  return s;
}

function validateBody(body) {
  if (!body || typeof body !== 'object') throw vErr('JSON body が必要です');
  const runId = fieldStr(body.run_id, 'run_id', { required: true });
  const folderName = fieldStr(body.folder, 'folder', { required: true });
  const shipDate = validateShipDate(fieldStr(body.ship_date, 'ship_date', { required: true }));
  const extractedAt = fieldStr(body.extracted_at, 'extracted_at');
  if (!Array.isArray(body.rows) || body.rows.length === 0) throw vErr('rows は1件以上の配列が必要です');
  if (body.rows.length > MAX_ROWS) throw vErr(`rows が多すぎます (${MAX_ROWS}件まで)`);
  const rows = body.rows.map((r, i) => {
    if (!r || typeof r !== 'object') throw vErr(`rows[${i}] が不正です`);
    const slipNo = fieldStr(r.slip_no, `rows[${i}].slip_no`, { required: true });
    // 形式検証 (Codex R1 medium #5: OCR誤認・不正データの恒久保存を入口で拒否)
    if (!SLIP_NO_RE.test(slipNo)) throw vErr(`rows[${i}].slip_no が出荷伝票NO形式 (SP+数字) ではありません: ${slipNo}`);
    return {
      slip_no: slipNo,
      mgmt_no: fieldStr(r.mgmt_no, `rows[${i}].mgmt_no`),
      mall_order_no: fieldStr(r.mall_order_no, `rows[${i}].mall_order_no`),
      source_file: fieldStr(r.source_file, `rows[${i}].source_file`),
    };
  });
  return { runId, folderName, shipDate, extractedAt, rows };
}

function handle(res, fn) {
  try { fn(); }
  catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message });
    if (e.code === 'SCHEMA_UNAVAILABLE') {
      return res.status(503).json({ ok: false, error: 'schema_unavailable', init_error: getSchemaError() });
    }
    // mirror 本体の boot リトライ中 (router.js の init IIFE 完了前) は一時的エラー扱い
    if (/初期化されていません/.test(String(e.message))) {
      return res.status(503).json({ ok: false, error: 'mirror_not_ready' });
    }
    console.error('[shipping-log]', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
}

// GAS 掃除ジョブ: 1フォルダ分の伝票を取込。
// 200 + ok:true + inserted+ignored===total のときのみ GAS はファイル削除に進む。
// 既存行と内容が食い違う伝票があれば 409 (削除させず人間の調査に回す)。
router.post('/ingest', requireIngestKey, (req, res) => handle(res, () => {
  const p = validateBody(req.body);
  const { inserted, ignored, conflicts } = ingestFolderSlips(p);
  if (conflicts.length > 0) {
    console.warn('[shipping-log] ingest conflicts', { folder: p.folderName, conflicts });
    return res.status(409).json({ ok: false, error: 'conflict', inserted, ignored, conflicts, total: p.rows.length });
  }
  res.json({ ok: true, inserted, ignored, total: p.rows.length });
}));

// 動作確認・突合ジョブ用: 直近の取込行 (read-only token)
router.get('/recent', requireReadToken, (req, res) => handle(res, () => {
  res.json({ ok: true, rows: recentSlips(req.query.limit) });
}));

export default router;
