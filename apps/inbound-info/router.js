/**
 * 入庫情報管理 — router (Render 完結、ミニPC 不使用)
 *
 * URL: /apps/inbound-info/        … UI (入数マスタ一覧 + 原産国 + Excel取込)
 *      /apps/inbound-info/api/*   … REST API
 *
 * 認証: server.js で requireAppAccess('inbound-info') を mount 時に適用。
 * データ: warehouse-mirror.db の f_inbound_info / f_inbound_origin (db.js)。
 * 旧スプレッドシート「入庫情報管理表.xlsx」の移行先。初回移行も本画面の Excel 取込
 * (プレビュー → 本取込 の二段階) で行う。
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import ExcelJS from 'exceljs';
import {
  stats, listInbound, updateInbound, addManual, deleteInbound, syncNewProducts,
  importWorkbook, listOrigin, upsertOrigin, deleteOrigin,
} from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

function currentUser(req) {
  return req.session?.email || req.session?.displayName || null;
}

// ─── UI (giftset-assembly と同じ末尾スラッシュ正規化) ───
router.get('/', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  const query = qIdx === -1 ? '' : req.originalUrl.slice(qIdx);
  if (!pathname.endsWith('/')) {
    return res.redirect(308, pathname + '/' + query);
  }
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ─── 統計 ───
router.get('/api/stats', (req, res) => {
  try {
    res.json({ ok: true, result: stats() });
  } catch (e) {
    console.error('[inbound-info] stats', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 一覧 ───
router.get('/api/list', (req, res) => {
  try {
    const { q, filter, offset, limit } = req.query;
    res.json({ ok: true, result: listInbound({ q, filter, offset, limit }) });
  } catch (e) {
    console.error('[inbound-info] list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 1行更新 ───
router.post('/api/update', (req, res) => {
  try {
    const { code_key, fields, expected_updated_at } = req.body || {};
    if (!code_key || typeof fields !== 'object' || fields == null) {
      return res.status(400).json({ ok: false, error: 'bad_request' });
    }
    const r = updateInbound(code_key, fields, currentUser(req), expected_updated_at);
    if (!r.ok) {
      const status = r.error === 'not_found' ? 404 : r.error === 'conflict' ? 409 : 400;
      return res.status(status).json(r);
    }
    res.json(r);
  } catch (e) {
    console.error('[inbound-info] update', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 手動追加 ───
router.post('/api/add', (req, res) => {
  try {
    const r = addManual(req.body?.code, currentUser(req));
    if (!r.ok) return res.status(400).json(r);
    res.json(r);
  } catch (e) {
    console.error('[inbound-info] add', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 1行削除 ───
router.post('/api/delete', (req, res) => {
  try {
    const r = deleteInbound(req.body?.code_key);
    if (!r.ok) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json(r);
  } catch (e) {
    console.error('[inbound-info] delete', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 新商品を今すぐ取込 (cron と同じ処理) ───
router.post('/api/sync-now', (req, res) => {
  try {
    const r = syncNewProducts();
    if (!r.ok) return res.status(409).json(r);
    res.json(r);
  } catch (e) {
    console.error('[inbound-info] sync-now', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── Excel パース ───
// exceljs の cell.value は string / number / boolean / Date / {richText} / {text,hyperlink} /
// {formula,result} を取り得るため、素の値へ潰すヘルパーを通す。
function plainValue(v) {
  if (v == null) return null;
  if (typeof v === 'object') {
    if (Array.isArray(v.richText)) return v.richText.map((t) => t.text).join('');
    if ('result' in v) return plainValue(v.result);
    if ('text' in v) return v.text;
    if (v instanceof Date) return v.toISOString();
    if ('error' in v) return null;
    return String(v);
  }
  return v;
}
// 商品コード等の表示: 数値セル (例 884389) を '884389.0' にしない
function asText(v) {
  const p = plainValue(v);
  if (p == null) return null;
  if (typeof p === 'number') return Number.isInteger(p) ? String(p) : String(p);
  return String(p);
}

// ヘッダー行 (1行目) から「ヘッダー名 → 列番号」を作る
function headerMap(ws) {
  const map = {};
  ws.getRow(1).eachCell((cell, colNumber) => {
    const t = asText(cell.value);
    if (t) map[String(t).trim()] = colNumber;
  });
  return map;
}

const IRISU_HEADERS = ['商品コード', '商品名', '入数', '入庫時BCシール貼りフラグ', '直接ピックロケ保管', 'BF保管荷姿', 'いろは在庫化作業有無'];

// export はテスト (scripts/test-inbound-info.mjs) 用
export function parseIrisuSheet(ws) {
  const h = headerMap(ws);
  if (!h['商品コード'] || !h['商品名']) return { error: 'header_mismatch', headers: Object.keys(h) };
  const rows = [];
  ws.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    const rec = { rowNumber };
    for (const name of IRISU_HEADERS) {
      if (!h[name]) continue;
      const v = plainValue(row.getCell(h[name]).value);
      rec[name] = name === '入数' ? v : asText(v);
    }
    if (rec.商品コード != null && String(rec.商品コード).trim() !== '') rows.push(rec);
  });
  return { rows };
}

export function parseOriginSheet(ws) {
  const h = headerMap(ws);
  if (!h['商品コード']) return { error: 'header_mismatch', headers: Object.keys(h) };
  // 旧シートのヘッダーは「産地・数値等」。将来「産地」へ改名されても拾えるよう両対応。
  const sanchiCol = h['産地・数値等'] || h['産地'];
  const rows = [];
  ws.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    const rec = {
      rowNumber,
      管理コード: h['管理コード'] ? asText(row.getCell(h['管理コード']).value) : null,
      識別番号: h['識別番号'] ? asText(row.getCell(h['識別番号']).value) : null,
      商品コード: asText(row.getCell(h['商品コード']).value),
      商品名: h['商品名'] ? asText(row.getCell(h['商品名']).value) : null,
      産地: sanchiCol ? asText(row.getCell(sanchiCol).value) : null,
      画像有無: h['画像'] ? plainValue(row.getCell(h['画像']).value) === true : false,
    };
    if (rec.商品コード != null && String(rec.商品コード).trim() !== '') rows.push(rec);
  });
  return { rows };
}

// ─── Excel 取込 (multipart)。dry_run=1 でプレビューのみ ───
router.post('/api/import', upload.single('file'), async (req, res) => {
  try {
    if (!req.file?.buffer) return res.status(400).json({ ok: false, error: 'no_file' });
    const dryRun = req.body?.dry_run === '1';
    const wb = new ExcelJS.Workbook();
    try {
      await wb.xlsx.load(req.file.buffer);
    } catch (e) {
      console.error('[inbound-info] xlsx parse', e.message);
      return res.status(400).json({ ok: false, error: 'invalid_xlsx' });
    }
    const irisuWs = wb.getWorksheet('入数マスタ') || wb.worksheets[0];
    if (!irisuWs) return res.status(400).json({ ok: false, error: 'no_sheet' });
    // 先に全シートをパース・検証してから、書込みは importWorkbook が単一トランザクションで実行
    // (Codex R1 Medium: シート別トランザクションだと途中失敗で半端な状態が残る)
    const parsed = parseIrisuSheet(irisuWs);
    if (parsed.error) {
      return res.status(400).json({ ok: false, error: parsed.error, headers: parsed.headers });
    }
    let originRows = null;
    let originHeaderError = null;
    const originWs = wb.getWorksheet('原産国');
    if (originWs) {
      const po = parseOriginSheet(originWs);
      if (po.error) originHeaderError = { error: po.error };
      else originRows = po.rows;
    }
    const user = currentUser(req);
    const reports = importWorkbook(parsed.rows, originRows, { dryRun, user });
    res.json({ ok: true, dry_run: dryRun, irisu: reports.irisu, origin: reports.origin ?? originHeaderError });
  } catch (e) {
    console.error('[inbound-info] import', e.message);
    res.status(500).json({ ok: false, error: 'import_failed' });
  }
});

// ─── 原産国 CRUD ───
router.get('/api/origin/list', (req, res) => {
  try {
    res.json({ ok: true, result: listOrigin() });
  } catch (e) {
    console.error('[inbound-info] origin list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.post('/api/origin/upsert', (req, res) => {
  try {
    const r = upsertOrigin(req.body || {}, currentUser(req));
    if (!r.ok) {
      const status = r.error === 'not_found' ? 404 : r.error === 'conflict' ? 409 : 400;
      return res.status(status).json(r);
    }
    res.json(r);
  } catch (e) {
    console.error('[inbound-info] origin upsert', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.post('/api/origin/delete', (req, res) => {
  try {
    const r = deleteOrigin(req.body?.id);
    if (!r.ok) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json(r);
  } catch (e) {
    console.error('[inbound-info] origin delete', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

export default router;
