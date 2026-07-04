/**
 * product-hub (商品登録一元化) router。
 * 要件定義: AI_reference『システム設計/商品登録一元化_要件定義_20260703.md』P1 スコープ。
 *   - ドラフト一覧/詳細編集 (サムネ付き)
 *   - 参考URL必須ゲート (公式URL等が揃うまで「生成待ち」に進めない)
 *   - Notion カード自動作成 + 未作成バナー + リトライ
 * mount: server.js で /apps/product-hub (requireAppAccess 配下)
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

import {
  getDB, logEvent, gateReasons,
  DRAFT_STATUSES, STATUS_LABELS, AI_OUTPUT_KINDS,
} from './db.js';
import { parseDriveLink, thumbnailUrl, fileViewUrl } from './lib/drive-link.js';
import { attemptCardCreation, retryPendingCards, pendingCardCount } from './services/notion-card.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();
router.use(express.json({ limit: '512kb' }));

const view = (name) => path.join(__dirname, 'views', name);
const actorOf = (req) => req.session?.email || req.session?.displayName || null;

// ステータス遷移の許可表 (§3)。on_hold/excluded へは全ステータスから退避可
const TRANSITIONS = {
  draft: ['ready_for_ai'],
  ready_for_ai: ['draft', 'review'],
  review: ['approved', 'draft'],
  approved: ['listed', 'review'],
  listed: ['expanded'],
  expanded: [],
  on_hold: ['draft'],
  excluded: ['draft'],
};
const ESCAPE_STATUSES = ['on_hold', 'excluded'];

function canTransition(from, to) {
  if (!DRAFT_STATUSES.includes(to)) return false;
  if (ESCAPE_STATUSES.includes(to)) return from !== to;
  return (TRANSITIONS[from] || []).includes(to);
}

// 金額 sanitize (inventory-monthly の sanitizeMoney と同方針: 非有限→null、±1兆 clamp、整数化)
function sanitizeMoney(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  const clamped = Math.max(-1e12, Math.min(1e12, n));
  return Math.round(clamped);
}

function isHttpUrl(s) {
  if (typeof s !== 'string') return false;
  const t = s.trim();
  return /^https?:\/\/\S+$/i.test(t);
}

function cleanText(v, maxLen = 2000) {
  if (v == null) return null;
  const s = String(v).trim();
  if (s === '') return null;
  return s.length > maxLen ? s.slice(0, maxLen) : s;
}

function loadDraftOr404(req, res) {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ ok: false, error: 'invalid id' });
    return null;
  }
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  if (!draft) {
    res.status(404).json({ ok: false, error: 'draft not found' });
    return null;
  }
  return draft;
}

// ─── 画面 ───────────────────────────────────────────────

router.get('/', (req, res) => {
  const db = getDB();
  const statusFilter = DRAFT_STATUSES.includes(req.query.status) ? req.query.status : null;

  const counts = {};
  for (const s of DRAFT_STATUSES) counts[s] = 0;
  for (const row of db.prepare('SELECT status, COUNT(*) AS c FROM product_drafts GROUP BY status').all()) {
    if (counts[row.status] != null) counts[row.status] = row.c;
  }

  const where = statusFilter ? 'WHERE d.status = ?' : '';
  const params = statusFilter ? [statusFilter] : [];
  const drafts = db.prepare(`
    SELECT d.*,
      (SELECT drive_file_id FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_id
    FROM product_drafts d ${where}
    ORDER BY d.updated_at DESC
    LIMIT 500
  `).all(...params);
  for (const d of drafts) {
    d.thumb = d.first_image_id ? thumbnailUrl(d.first_image_id, 160) : null;
  }

  res.render(view('index.ejs'), {
    title: '商品登録ハブ',
    displayName: req.session?.displayName || req.session?.email || '',
    drafts, counts, statusFilter,
    statuses: DRAFT_STATUSES, statusLabels: STATUS_LABELS,
    notionPending: pendingCardCount(),
  });
});

router.get('/new', (req, res) => {
  res.render(view('new.ejs'), {
    title: '新規商品ドラフト',
    displayName: req.session?.displayName || req.session?.email || '',
  });
});

router.get('/detail/:id', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  const refs = db.prepare('SELECT * FROM draft_reference_urls WHERE draft_id = ? ORDER BY sort, id').all(draft.id);
  const images = db.prepare('SELECT * FROM draft_images WHERE draft_id = ? ORDER BY sort, id').all(draft.id);
  for (const img of images) {
    img.thumb = thumbnailUrl(img.drive_file_id, 320);
    img.view_url = img.drive_url || fileViewUrl(img.drive_file_id);
  }
  const specs = db.prepare('SELECT * FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draft.id);
  const aiRows = db.prepare('SELECT * FROM draft_ai_outputs WHERE draft_id = ?').all(draft.id);
  const aiOutputs = {};
  for (const k of AI_OUTPUT_KINDS) aiOutputs[k] = aiRows.find((r) => r.kind === k) || null;
  const events = db.prepare('SELECT * FROM draft_events WHERE draft_id = ? ORDER BY id DESC LIMIT 30').all(draft.id);

  const nextStatuses = (TRANSITIONS[draft.status] || [])
    .concat(ESCAPE_STATUSES.filter((s) => s !== draft.status));

  res.render(view('detail.ejs'), {
    title: `商品ドラフト #${draft.id}`,
    displayName: req.session?.displayName || req.session?.email || '',
    draft, refs, images, specs, aiOutputs, events,
    gate: gateReasons(db, draft),
    nextStatuses,
    statusLabels: STATUS_LABELS,
    aiKinds: AI_OUTPUT_KINDS,
  });
});

// ─── API: ドラフト作成/更新 ───────────────────────────────

router.post('/api/drafts', async (req, res) => {
  const name = cleanText(req.body?.name, 300);
  const neCode = cleanText(req.body?.ne_code, 100);
  const officialUrl = cleanText(req.body?.official_url, 1000);
  if (!name) return res.status(400).json({ ok: false, error: '商品名は必須です' });
  if (!neCode) return res.status(400).json({ ok: false, error: 'NE商品コードは必須です' });
  if (officialUrl && !isHttpUrl(officialUrl)) {
    return res.status(400).json({ ok: false, error: '公式ページURLの形式が不正です (http/https)' });
  }

  const db = getDB();
  let draftId;
  try {
    const info = db.prepare(`
      INSERT INTO product_drafts (ne_code, name, official_url, price, jan_code, created_by)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(neCode, name, officialUrl, sanitizeMoney(req.body?.price), cleanText(req.body?.jan_code, 20), actorOf(req));
    draftId = info.lastInsertRowid;
  } catch (e) {
    if (String(e.message).includes('UNIQUE')) {
      return res.status(409).json({ ok: false, error: `NE商品コード ${neCode} のドラフトは既に存在します` });
    }
    throw e;
  }
  logEvent(db, draftId, 'created', neCode, actorOf(req));

  // §5: 登録と同時に Notion カード作成 (失敗しても登録は成功。バナー+リトライで回収)
  const notion = await attemptCardCreation(draftId, { actor: actorOf(req) });
  res.json({ ok: true, id: draftId, notion });
});

router.post('/api/drafts/:id', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();

  const name = cleanText(req.body?.name, 300) || draft.name;
  const officialUrl = req.body?.official_url !== undefined ? cleanText(req.body.official_url, 1000) : draft.official_url;
  if (officialUrl && !isHttpUrl(officialUrl)) {
    return res.status(400).json({ ok: false, error: '公式ページURLの形式が不正です (http/https)' });
  }
  const driveFolderUrl = req.body?.drive_folder_url !== undefined ? cleanText(req.body.drive_folder_url, 1000) : draft.drive_folder_url;
  if (driveFolderUrl && !isHttpUrl(driveFolderUrl)) {
    return res.status(400).json({ ok: false, error: '画像フォルダURLの形式が不正です (http/https)' });
  }

  db.prepare(`
    UPDATE product_drafts SET
      name = ?, official_url = ?, price = ?, jan_code = ?, has_variation = ?,
      drive_folder_url = ?, memo = ?,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ?
  `).run(
    name,
    officialUrl,
    req.body?.price !== undefined ? sanitizeMoney(req.body.price) : draft.price,
    req.body?.jan_code !== undefined ? cleanText(req.body.jan_code, 20) : draft.jan_code,
    req.body?.has_variation !== undefined ? (req.body.has_variation ? 1 : 0) : draft.has_variation,
    driveFolderUrl,
    req.body?.memo !== undefined ? cleanText(req.body.memo, 4000) : draft.memo,
    draft.id,
  );
  logEvent(db, draft.id, 'updated', null, actorOf(req));
  res.json({ ok: true });
});

// ─── API: 参考URL / 画像 / 仕様表 / AI出力 ─────────────────

router.post('/api/drafts/:id/refs', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const url = cleanText(req.body?.url, 1000);
  if (!url || !isHttpUrl(url)) return res.status(400).json({ ok: false, error: 'URLの形式が不正です (http/https)' });
  const db = getDB();
  const info = db.prepare('INSERT INTO draft_reference_urls (draft_id, url) VALUES (?, ?)').run(draft.id, url);
  res.json({ ok: true, id: info.lastInsertRowid });
});

router.post('/api/drafts/:id/refs/:refId/delete', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  db.prepare('DELETE FROM draft_reference_urls WHERE id = ? AND draft_id = ?')
    .run(Number.parseInt(req.params.refId, 10) || 0, draft.id);
  res.json({ ok: true });
});

router.post('/api/drafts/:id/images', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const raw = cleanText(req.body?.url, 1000);
  if (!raw) return res.status(400).json({ ok: false, error: 'DriveリンクまたはファイルIDを入力してください' });
  const parsed = parseDriveLink(raw);
  if (!parsed) return res.status(400).json({ ok: false, error: 'Google Driveのリンクとして解釈できませんでした' });
  if (parsed.type === 'folder') {
    return res.status(400).json({
      ok: false,
      error: 'フォルダリンクです。フォルダ一括取り込みは今後対応予定なので、画像ファイル個別のリンクを貼ってください (フォルダURLは基本情報の「画像フォルダ」欄へ)',
    });
  }
  const db = getDB();
  const maxSort = db.prepare('SELECT COALESCE(MAX(sort), -1) AS m FROM draft_images WHERE draft_id = ?').get(draft.id).m;
  try {
    const info = db.prepare(`
      INSERT INTO draft_images (draft_id, drive_file_id, drive_url, sort) VALUES (?, ?, ?, ?)
    `).run(draft.id, parsed.id, isHttpUrl(raw) ? raw : null, maxSort + 1);
    res.json({ ok: true, id: info.lastInsertRowid, thumb: thumbnailUrl(parsed.id, 320) });
  } catch (e) {
    if (String(e.message).includes('UNIQUE')) {
      return res.status(409).json({ ok: false, error: 'この画像は既に追加されています' });
    }
    throw e;
  }
});

router.post('/api/drafts/:id/images/:imageId/delete', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  db.prepare('DELETE FROM draft_images WHERE id = ? AND draft_id = ?')
    .run(Number.parseInt(req.params.imageId, 10) || 0, draft.id);
  res.json({ ok: true });
});

router.post('/api/drafts/:id/specs', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const key = cleanText(req.body?.spec_key, 100);
  const value = cleanText(req.body?.spec_value, 500);
  if (!key) return res.status(400).json({ ok: false, error: '項目名を入力してください' });
  const db = getDB();
  const maxSort = db.prepare('SELECT COALESCE(MAX(sort), -1) AS m FROM draft_specs WHERE draft_id = ?').get(draft.id).m;
  const info = db.prepare('INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, ?, ?, ?)')
    .run(draft.id, key, value, maxSort + 1);
  res.json({ ok: true, id: info.lastInsertRowid });
});

router.post('/api/drafts/:id/specs/:specId/delete', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  db.prepare('DELETE FROM draft_specs WHERE id = ? AND draft_id = ?')
    .run(Number.parseInt(req.params.specId, 10) || 0, draft.id);
  res.json({ ok: true });
});

router.post('/api/drafts/:id/ai-outputs', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const kind = String(req.body?.kind || '');
  if (!AI_OUTPUT_KINDS.includes(kind)) return res.status(400).json({ ok: false, error: 'invalid kind' });
  const content = cleanText(req.body?.content, 50000);
  const db = getDB();
  db.prepare(`
    INSERT INTO draft_ai_outputs (draft_id, kind, content, edited_by_human)
    VALUES (?, ?, ?, 1)
    ON CONFLICT(draft_id, kind) DO UPDATE SET content = excluded.content, edited_by_human = 1
  `).run(draft.id, kind, content);
  logEvent(db, draft.id, 'ai_output_edited', kind, actorOf(req));
  res.json({ ok: true });
});

// ─── API: ステータス遷移 (§4 ゲート) ───────────────────────

router.post('/api/drafts/:id/status', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const to = String(req.body?.to || '');
  if (!canTransition(draft.status, to)) {
    return res.status(400).json({ ok: false, error: `${STATUS_LABELS[draft.status] || draft.status} から ${STATUS_LABELS[to] || to} へは遷移できません` });
  }
  const db = getDB();
  if (to === 'ready_for_ai') {
    const reasons = gateReasons(db, draft);
    if (reasons.length > 0) {
      return res.status(400).json({ ok: false, error: '必須項目が未入力です', reasons });
    }
  }
  db.prepare(`
    UPDATE product_drafts SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?
  `).run(to, draft.id);
  logEvent(db, draft.id, 'status_changed', `${draft.status} -> ${to}`, actorOf(req));
  res.json({ ok: true, status: to });
});

// ─── API: Notion カードリトライ ───────────────────────────

router.post('/api/drafts/:id/notion-retry', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const result = await attemptCardCreation(draft.id, { actor: actorOf(req) });
  res.json({ ok: result.outcome === 'created' || result.outcome === 'adopted_existing' || result.outcome === 'already_created', result });
});

router.post('/api/notion-retry-all', async (req, res) => {
  const results = await retryPendingCards({ actor: actorOf(req) });
  const failed = results.filter((r) => r.outcome === 'failed');
  res.json({ ok: failed.length === 0, tried: results.length, failed: failed.length, results });
});

export default router;
