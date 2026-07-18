/**
 * 出荷実績ログ (shipping-log) — GAS 取込 API
 *
 * 出荷_no 掃除 GAS 専用の Bearer 認証エンドポイント。session 認証の外側に mount する
 * (server.js で requireAppAccess を通さない。packing-dispatch の ne-sync-worker と同じ構成)。
 *
 * 認証: SHIPPING_LOG_INGEST_TOKEN (env)。未設定なら全 endpoint 503 で fail-closed。
 * GAS 側の契約: POST /ingest が 200 を返した場合のみ、該当フォルダのファイルを削除してよい。
 */
import express from 'express';
import crypto from 'node:crypto';
import { ingestFolderSlips, recentSlips, getSchemaError } from './db.js';

const router = express.Router();

const MAX_ROWS = 3000;        // 1フォルダの伝票数上限 (通常は数十件)
const MAX_FIELD_LEN = 200;

function requireIngestKey(req, res, next) {
  const expected = process.env.SHIPPING_LOG_INGEST_TOKEN || '';
  if (!expected) {
    // env 未設定 → fail-closed (絶対 fail-open しない)
    return res.status(503).json({ ok: false, error: 'not_configured',
      message: 'SHIPPING_LOG_INGEST_TOKEN が未設定です。サーバ管理者に連絡してください。' });
  }
  const m = /^Bearer\s+(\S+)$/.exec(String(req.headers.authorization || ''));
  if (!m) return res.status(401).json({ ok: false, error: 'unauthorized', message: 'Bearer token がありません' });
  const a = Buffer.from(m[1], 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    console.warn('[shipping-log] ingest auth failed', { ip: req.ip, ua: req.headers['user-agent'] });
    return res.status(403).json({ ok: false, error: 'forbidden', message: 'Bearer token が一致しません' });
  }
  next();
}
router.use(requireIngestKey);

function fieldStr(v, name, { required = false } = {}) {
  if (v == null || v === '') {
    if (required) { const e = new Error(`${name} は必須です`); e.code = 'VALIDATION'; throw e; }
    return null;
  }
  if (typeof v !== 'string') { const e = new Error(`${name} は文字列で指定してください`); e.code = 'VALIDATION'; throw e; }
  const s = v.trim();
  if (s.length > MAX_FIELD_LEN) { const e = new Error(`${name} が長すぎます (${MAX_FIELD_LEN}文字まで)`); e.code = 'VALIDATION'; throw e; }
  return s || null;
}

function validateBody(body) {
  if (!body || typeof body !== 'object') { const e = new Error('JSON body が必要です'); e.code = 'VALIDATION'; throw e; }
  const runId = fieldStr(body.run_id, 'run_id', { required: true });
  const folderName = fieldStr(body.folder, 'folder', { required: true });
  const shipDate = fieldStr(body.ship_date, 'ship_date', { required: true });
  if (!/^\d{4}-\d{2}-\d{2}$/.test(shipDate)) { const e = new Error('ship_date は YYYY-MM-DD 形式で指定してください'); e.code = 'VALIDATION'; throw e; }
  const extractedAt = fieldStr(body.extracted_at, 'extracted_at');
  if (!Array.isArray(body.rows) || body.rows.length === 0) { const e = new Error('rows は1件以上の配列が必要です'); e.code = 'VALIDATION'; throw e; }
  if (body.rows.length > MAX_ROWS) { const e = new Error(`rows が多すぎます (${MAX_ROWS}件まで)`); e.code = 'VALIDATION'; throw e; }
  const rows = body.rows.map((r, i) => {
    if (!r || typeof r !== 'object') { const e = new Error(`rows[${i}] が不正です`); e.code = 'VALIDATION'; throw e; }
    return {
      slip_no: fieldStr(r.slip_no, `rows[${i}].slip_no`, { required: true }),
      mgmt_no: fieldStr(r.mgmt_no, `rows[${i}].mgmt_no`),
      mall_order_no: fieldStr(r.mall_order_no, `rows[${i}].mall_order_no`),
      source_file: fieldStr(r.source_file, `rows[${i}].source_file`),
    };
  });
  return { runId, folderName, shipDate, extractedAt, rows };
}

function handle(res, fn) {
  try { res.json({ ok: true, ...fn() }); }
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

// GAS 掃除ジョブ: 1フォルダ分の伝票を取込 (200 が返った場合のみ GAS はファイル削除に進む)
router.post('/ingest', (req, res) => handle(res, () => {
  const p = validateBody(req.body);
  const { inserted, ignored } = ingestFolderSlips(p);
  return { inserted, ignored, total: p.rows.length };
}));

// 動作確認・突合ジョブ用: 直近の取込行
router.get('/recent', (req, res) => handle(res, () => {
  return { rows: recentSlips(req.query.limit) };
}));

export default router;
