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
import {
  stats, listInbound, getInbound, updateInbound, addManual, deleteInbound, syncNewProducts,
  listOrigin, upsertOrigin, deleteOrigin, scheduleState, pdfState, getJobSettings, saveJobSettings,
} from './db.js';
import { getNefudaInfo, refreshNefudaSchedule } from './nefuda-fetch.js';
import { exportSchedulePdfToDrive, buildSchedulePdf, PDF_FILENAME, PDF_FOLDER_ID } from './schedule-pdf.js';
import { applyInboundInfoSchedule, effectiveSchedule } from './sync-job.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

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
    const { q, filter, offset } = req.query;
    // limit=all は印刷用 (該当全件を1クエリで返す)。それ以外は db.js 側で 1〜500 に丸める
    const limit = req.query.limit === 'all' ? 'all' : req.query.limit;
    const result = listInbound({ q, filter, offset, limit });
    // limit=all で上限超過 (現実的には起こらないが API 契約として上限を持つ)
    if (result.error) return res.status(413).json({ ok: false, error: result.error, total: result.total, max_rows: result.max_rows });
    res.json({ ok: true, result });
  } catch (e) {
    console.error('[inbound-info] list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 1行取得 (保存時の競合復旧。存在しない場合も 200 + result:null を返す) ───
router.get('/api/row', (req, res) => {
  try {
    const key = req.query?.code_key;
    if (!key) return res.status(400).json({ ok: false, error: 'bad_request' });
    res.json({ ok: true, result: getInbound(String(key)) });
  } catch (e) {
    console.error('[inbound-info] row', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 1行更新 ───
router.post('/api/update', (req, res) => {
  try {
    const { code_key, fields, expected_version } = req.body || {};
    if (!code_key || typeof fields !== 'object' || fields == null) {
      return res.status(400).json({ ok: false, error: 'bad_request' });
    }
    const r = updateInbound(code_key, fields, currentUser(req), expected_version);
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

// ─── 入荷予定 (nefuda.csv) ───
// 状態表示: DB の最終取得状態 + Drive 上の現在の更新日時 (60秒キャッシュ)。
// Drive 側の取得失敗 (未共有・ネットワーク等) は状態表示を巻き込まず drive_error として返す。
router.get('/api/schedule/info', async (req, res) => {
  try {
    const state = scheduleState();
    let drive = null;
    let driveError = null;
    try {
      drive = await getNefudaInfo();
    } catch (e) {
      driveError = e.message;
    }
    res.json({ ok: true, result: { state, drive, drive_error: driveError } });
  } catch (e) {
    console.error('[inbound-info] schedule info', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// 最新の nefuda.csv を取得して入荷予定を置換 (UI ボタン。cron と同一実体)
router.post('/api/schedule/refresh', async (req, res) => {
  try {
    const r = await refreshNefudaSchedule(currentUser(req));
    if (!r.ok) {
      // stale_file: 反映済みの方が新しい (並行実行の後追い)。データは最新のまま
      return res.status(409).json(r);
    }
    res.json(r);
  } catch (e) {
    console.error('[inbound-info] schedule refresh', e.message);
    // lib/drive-csv.js の VALIDATION (ファイル無し・サイズ超過等) は利用者に読める文で返す
    const status = e.code === 'VALIDATION' ? 400 : 502;
    res.status(status).json({ ok: false, error: 'drive_error', message: e.message });
  }
});

// ─── 自動実行の設定 (時刻 / PDF出力の有無) ───
router.get('/api/settings', (req, res) => {
  try {
    const s = getJobSettings();
    const eff = effectiveSchedule();
    res.json({ ok: true, result: {
      ...s,
      // env で cron 式が直接指定されている場合は画面の時刻設定より優先される (その旨を画面に出す)
      env_override: eff.source === 'env' ? eff.expr : null,
      pdf: { filename: PDF_FILENAME(), folder_id: PDF_FOLDER_ID() },
      state: pdfState(),
    } });
  } catch (e) {
    console.error('[inbound-info] settings get', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

router.post('/api/settings', (req, res) => {
  try {
    const r = saveJobSettings(req.body || {}, currentUser(req));
    if (!r.ok) return res.status(400).json(r);
    // 保存した時刻で cron を張り替える (再デプロイ不要で反映)
    const applied = applyInboundInfoSchedule();
    res.json({ ok: true, ...r, cron: applied });
  } catch (e) {
    console.error('[inbound-info] settings post', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 入荷予定リストPDF ───
// Drive 上書き保存 (cron と同一実体)。UI の「今すぐPDFを作成してDriveに保存」ボタン用。
router.post('/api/pdf/export', async (req, res) => {
  try {
    res.json({ ok: true, ...(await exportSchedulePdfToDrive(currentUser(req))) });
  } catch (e) {
    console.error('[inbound-info] pdf export', e.message);
    res.status(502).json({ ok: false, error: 'pdf_export_failed', message: e.message });
  }
});

// 保存せずブラウザで中身を確認する用 (Drive に触らない)
router.get('/api/pdf/preview', async (req, res) => {
  try {
    const { buffer } = await buildSchedulePdf();
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="inbound-schedule.pdf"`);
    res.send(buffer);
  } catch (e) {
    console.error('[inbound-info] pdf preview', e.message);
    res.status(502).json({ ok: false, error: 'pdf_build_failed', message: e.message });
  }
});

// ─── Excel パース ───
// 【2026-07-25】初回移行が完了したため「Excel取込」タブと POST /api/import は廃止した
// (画面編集を Excel で一括上書きできる経路を残さない)。以下のパーサと db.js の
// importWorkbook / importInboundRows / importOriginRows は、再移行が必要になった時に
// scripts から呼べるよう残置 (scripts/test-inbound-info.mjs が動作を担保している)。
//
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
