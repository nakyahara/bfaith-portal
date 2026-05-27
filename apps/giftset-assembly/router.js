/**
 * ギフトセット組み依頼 — router (Render 完結、ミニPC 不使用)
 *
 * URL: /apps/giftset-assembly/        … UI (登録 + 依頼)
 *      /apps/giftset-assembly/api/*   … REST API
 *
 * 認証: server.js で requireAppAccess('giftset-assembly') を mount 時に適用。
 * データ: warehouse-mirror.db の f_giftset / f_giftset_components (db.js)。
 * Notion: notion.js (環境変数 NOTION_TOKEN / GIFTSET_NOTION_DB_ID)。
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  suggestProducts, listGiftsets, getGiftset,
  upsertGiftset, deactivateGiftset, buildPickingRows, getJstDateString,
} from './db.js';
import { createWorkCard, isNotionConfigured } from './notion.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

function currentUser(req) {
  return req.session?.email || req.session?.displayName || null;
}

// CSV セル: 数式インジェクション無害化 + RFC4180 引用
function csvCell(v) {
  let s = String(v == null ? '' : v);
  if (/^\s*[=+\-@]/.test(s)) s = "'" + s; // Excel で式評価され得る先頭文字を無害化(先頭空白経由も含む)
  return '"' + s.replace(/"/g, '""') + '"';
}

// 実在日チェック ('YYYY-MM-DD' 形式 + 例: 2026-02-31 を弾く)
function isRealDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// ─── UI ───
router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ─── 商品候補 (登録画面のオートコンプリート) ───
router.get('/api/suggest', (req, res) => {
  try {
    res.json({ ok: true, result: suggestProducts(req.query.q || '') });
  } catch (e) {
    console.error('[giftset] suggest', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── ギフトセット一覧 ───
router.get('/api/list', (req, res) => {
  try {
    res.json({ ok: true, result: listGiftsets() });
  } catch (e) {
    console.error('[giftset] list', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── ギフトセット1件 ───
router.get('/api/get', (req, res) => {
  try {
    const set = getGiftset(req.query.code || '');
    if (!set) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, result: set });
  } catch (e) {
    console.error('[giftset] get', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── 登録/更新 ───
router.post('/api/upsert', (req, res) => {
  try {
    const result = upsertGiftset(req.body || {}, currentUser(req));
    res.json({ ok: true, result });
  } catch (e) {
    if (e.code === 'VALIDATION') {
      return res.status(400).json({ ok: false, error: 'validation', message: e.message, detail: e.detail });
    }
    console.error('[giftset] upsert', e.message);
    res.status(500).json({ ok: false, error: 'db_error', message: e.message });
  }
});

// ─── 論理削除 ───
router.post('/api/delete', (req, res) => {
  try {
    const r = deactivateGiftset((req.body || {}).giftset_code || '', currentUser(req));
    if (!r.ok) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true });
  } catch (e) {
    console.error('[giftset] delete', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── ピッキングデータ (JSON、コピー用プレビュー) ───
router.get('/api/picking', (req, res) => {
  try {
    res.json({ ok: true, result: buildPickingRows(req.query.code || '', req.query.qty) });
  } catch (e) {
    if (e.code === 'NOT_FOUND') return res.status(404).json({ ok: false, error: 'not_found' });
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message });
    console.error('[giftset] picking', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── ピッキングデータ CSV (Shift-JIS、Excel 確認用バックアップ) ───
router.get('/api/picking.csv', async (req, res) => {
  try {
    const { rows } = buildPickingRows(req.query.code || '', req.query.qty);
    const header = ['商品ID', '品質区分', '出荷予定数', '単価'];
    const lines = [header.map(csvCell).join(',')];
    for (const r of rows) {
      lines.push([r.商品ID, r.品質区分, r.出荷予定数, r.単価].map(csvCell).join(','));
    }
    const csv = lines.join('\r\n');
    const iconv = (await import('iconv-lite')).default;
    const encoded = iconv.encode(csv, 'Shift_JIS');
    const fname = `picking_${(req.query.code || 'set')}_${rows.length}rows.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
    res.send(encoded);
  } catch (e) {
    if (e.code === 'NOT_FOUND') return res.status(404).json({ ok: false, error: 'not_found' });
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message });
    console.error('[giftset] picking.csv', e.message);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// ─── Notion 連携の設定状況 (UI でボタン活性制御) ───
router.get('/api/notion-status', (req, res) => {
  res.json({ ok: true, configured: isNotionConfigured() });
});

// ─── 依頼を発行: Notion 作業カードを作成 ───
router.post('/api/issue', async (req, res) => {
  try {
    const body = req.body || {};
    const set = getGiftset(body.code || '');
    if (!set) return res.status(404).json({ ok: false, error: 'not_found' });

    const qty = Number(body.qty);
    if (!Number.isInteger(qty) || qty < 1 || qty > 1000000) {
      return res.status(400).json({ ok: false, error: 'validation', message: '個数が不正です' });
    }
    // production_lot_size の倍数チェック (依頼時にもサーバ側で強制、UI 改竄や直接 API 叩き対策)
    const lotSize = Number.isInteger(set.production_lot_size) && set.production_lot_size >= 1
      ? set.production_lot_size : 1;
    if (qty % lotSize !== 0) {
      return res.status(400).json({
        ok: false, error: 'validation',
        message: `個数は ${lotSize} の倍数で入力してください (1ロット = ${lotSize}個)`,
      });
    }
    let people = null;
    if (body.people !== undefined && body.people !== null && body.people !== '') {
      const p = Number(body.people);
      if (!Number.isInteger(p) || p < 1 || p > 1000) {
        return res.status(400).json({ ok: false, error: 'validation', message: '人数が不正です' });
      }
      people = p;
    }
    let dueDate = null;
    if (body.due) {
      if (!isRealDate(body.due)) {
        return res.status(400).json({ ok: false, error: 'validation', message: '納期の形式が不正です' });
      }
      dueDate = body.due;
    }

    const card = await createWorkCard({
      set, qty, people, dueDate,
      requestDate: getJstDateString(),
      note: body.note ? String(body.note) : null,
    });
    res.json({ ok: true, result: card });
  } catch (e) {
    if (e.code === 'NOTION_NOT_CONFIGURED') {
      return res.status(503).json({ ok: false, error: 'notion_not_configured', message: e.message });
    }
    if (e.code === 'NOTION_API_ERROR') {
      return res.status(502).json({ ok: false, error: 'notion_api_error', message: e.message });
    }
    console.error('[giftset] issue', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
});

export default router;
