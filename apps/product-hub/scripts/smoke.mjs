/**
 * product-hub P1 smoke テスト (DB分離・Notion API 非接続)。
 * 実行: DATA_DIR に一時ディレクトリを指定して node apps/product-hub/scripts/smoke.mjs
 *   例 (bash): DATA_DIR=/tmp/ph-smoke node apps/product-hub/scripts/smoke.mjs
 * 検証: DB冪等init / CRUD / ゲート / append-onlyトリガー / drive-link / EJS実render / Notion fail-closed
 */
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import ejs from 'ejs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR を指定してください (本番DBを触らないため必須)');
  process.exit(1);
}
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });

let failed = 0;
function check(name, cond, detail = '') {
  if (cond) console.log(`  OK  ${name}`);
  else { failed++; console.error(`  NG  ${name} ${detail}`); }
}

// ─── drive-link ───
const { parseDriveLink, thumbnailUrl } = await import('../lib/drive-link.js');
check('parse file/d/', parseDriveLink('https://drive.google.com/file/d/1AbC_dEf-123456789012345/view?usp=sharing')?.id === '1AbC_dEf-123456789012345');
check('parse open?id=', parseDriveLink('https://drive.google.com/open?id=1AbC_dEf-123456789012345')?.type === 'file');
check('parse folders/', parseDriveLink('https://drive.google.com/drive/folders/0AMb_oOR8-Ss1Uk9PVA')?.type === 'folder');
check('parse u/0/folders', parseDriveLink('https://drive.google.com/drive/u/0/folders/0AMb_oOR8-Ss1Uk9PVA')?.type === 'folder');
check('parse raw id', parseDriveLink('1AbC_dEf-1234567890123456789')?.type === 'unknown');
check('parse garbage', parseDriveLink('https://example.com/x') === null);
check('thumbnail url', thumbnailUrl('abc', 160).includes('sz=w160'));

// ─── DB init (2回呼んで冪等) ───
const { initMirrorDB } = await import('../../warehouse-mirror/db.js');
initMirrorDB(); // 本番では server.js が起動時に実行する (smoke では明示)
const dbmod = await import('../db.js');
const db = dbmod.initProductHubDB();
dbmod.initProductHubDB();
check('tables created', db.prepare(`SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name IN
  ('product_drafts','draft_reference_urls','draft_images','draft_specs','draft_ai_outputs','draft_events')`).get().c === 6);

// ─── CRUD + gate ───
db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('SMOKE-1', 'スモーク商品', 'smoke')`).run();
const draft = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'SMOKE-1'`).get();
check('draft inserted', draft && draft.status === 'draft' && draft.notion_card_status === 'pending');

let reasons = dbmod.gateReasons(db, draft);
check('gate blocks (no url/image)', reasons.length === 2, JSON.stringify(reasons));

db.prepare(`UPDATE product_drafts SET official_url = 'https://example.com/item' WHERE id = ?`).run(draft.id);
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'fileid_1234567890')`).run(draft.id);
reasons = dbmod.gateReasons(db, db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draft.id));
check('gate passes after url+image', reasons.length === 0, JSON.stringify(reasons));

// UNIQUE ne_code
let uniqueErr = null;
try { db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('SMOKE-1', '重複')`).run(); } catch (e) { uniqueErr = e; }
check('ne_code UNIQUE enforced', uniqueErr && String(uniqueErr.message).includes('UNIQUE'));

// append-only trigger
dbmod.logEvent(db, draft.id, 'smoke_event', 'detail', 'smoke');
let trigErr = null;
try { db.prepare('UPDATE draft_events SET detail = ? WHERE draft_id = ?').run('tamper', draft.id); } catch (e) { trigErr = e; }
check('draft_events append-only', trigErr && String(trigErr.message).includes('append-only'));

// ─── notion-card fail-closed (env 未設定 → failed で残り、登録は無事) ───
delete process.env.RYS_NOTION_TOKEN;
const notionCard = await import('../services/notion-card.js');
const attempt = await notionCard.attemptCardCreation(draft.id, { actor: 'smoke' });
check('notion attempt fails safely', attempt.outcome === 'failed', JSON.stringify(attempt));
const after = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draft.id);
check('notion status = failed + error saved', after.notion_card_status === 'failed' && !!after.notion_card_error);
check('pendingCardCount counts it', notionCard.pendingCardCount() >= 1);

// ─── EJS 実 render (RYS教訓: 全分岐を実データで) ───
const views = path.join(__dirname, '..', 'views');
const statuses = dbmod.DRAFT_STATUSES;
const statusLabels = dbmod.STATUS_LABELS;
const counts = Object.fromEntries(statuses.map((s) => [s, 1]));
const draftRow = { ...after, thumb: 'https://drive.google.com/thumbnail?id=x&sz=w160', first_image_id: 'x' };

const renders = [
  ['index.ejs (banner+rows)', 'index.ejs', {
    title: 't', displayName: 'smoke', drafts: [draftRow], counts, statusFilter: null,
    statuses, statusLabels, notionPending: 1,
  }],
  ['index.ejs (empty)', 'index.ejs', {
    title: 't', displayName: 'smoke', drafts: [], counts, statusFilter: 'draft',
    statuses, statusLabels, notionPending: 0,
  }],
  ['new.ejs', 'new.ejs', { title: 't', displayName: 'smoke' }],
  ['detail.ejs (full)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: after,
    refs: [{ id: 1, url: 'https://example.com/ref' }],
    images: [{ id: 1, drive_file_id: 'x', thumb: 'https://x', view_url: 'https://x' }],
    specs: [{ id: 1, spec_key: 'サイズ', spec_value: 'W10' }],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, k === 'desc_catch' ? { content: 'AI文', generated_at: '2026-07-04', edited_by_human: 0 } : null])),
    events: [{ created_at: '2026-07-04T00:00:00Z', event: 'created', detail: 'x', actor: 'smoke' }],
    gate: ['公式ページURLが未入力です'],
    nextStatuses: ['ready_for_ai', 'on_hold', 'excluded'],
    statusLabels,
    aiKinds: dbmod.AI_OUTPUT_KINDS,
  }],
  ['detail.ejs (created notion)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, status: 'review', notion_card_status: 'created', notion_page_id: 'abcd-ef', has_variation: 1, memo: 'm', price: 1980, jan_code: '49', drive_folder_url: 'https://drive.google.com/drive/folders/x', official_url: 'https://x' },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['approved', 'draft', 'on_hold', 'excluded'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
  }],
];
for (const [name, file, data] of renders) {
  try {
    const html = await ejs.renderFile(path.join(views, file), data);
    check(`render ${name}`, html.length > 500);
  } catch (e) {
    check(`render ${name}`, false, e.message);
  }
}

console.log(failed === 0 ? '\nSMOKE: ALL PASS' : `\nSMOKE: ${failed} FAILED`);
process.exit(failed === 0 ? 0 : 1);
