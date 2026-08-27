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
const { parseDriveLink, thumbnailUrl, fileViewUrl } = await import('../lib/drive-link.js');
check('parse file/d/', parseDriveLink('https://drive.google.com/file/d/1AbC_dEf-123456789012345/view?usp=sharing')?.id === '1AbC_dEf-123456789012345');
check('parse open?id=', parseDriveLink('https://drive.google.com/open?id=1AbC_dEf-123456789012345')?.type === 'file');
check('parse folders/', parseDriveLink('https://drive.google.com/drive/folders/0AMb_oOR8-Ss1Uk9PVA')?.type === 'folder');
check('parse u/0/folders', parseDriveLink('https://drive.google.com/drive/u/0/folders/0AMb_oOR8-Ss1Uk9PVA')?.type === 'folder');
check('parse raw id', parseDriveLink('1AbC_dEf-1234567890123456789')?.type === 'unknown');
check('parse garbage', parseDriveLink('https://example.com/x') === null);
check('thumbnail url = アプリ内プロキシ', thumbnailUrl('abc-123', 160) === '/apps/product-hub/api/thumb/abc-123?w=160');
check('thumbnail url allowlist外の幅→320', thumbnailUrl('abc-123', 999).endsWith('?w=320'));
check('thumbnail url 幅未指定→320', thumbnailUrl('abc-123').endsWith('?w=320'));

// ─── folder-import (画像フォルダ→スロット割当、2026-08-01) ───
const { assignImageSlots, parseImageFileName, slotOfParsedName, MAX_IMAGE_SLOTS, MAX_NUMBERED_IMAGE } =
  await import('../lib/folder-import.js');
check('MAX_IMAGE_SLOTS = 楽天の登録口20 / ファイル番号は _19 まで (_top が枠1)',
  MAX_IMAGE_SLOTS === 20 && MAX_NUMBERED_IMAGE === 19);
check('parseImageFileName 基本形', JSON.stringify(parseImageFileName('abc_01.jpg')) === '{"base":"abc","kind":"num","num":1,"label":"_01"}');
check('parseImageFileName 番号なし', parseImageFileName('abc.jpg') === null);
check('parseImageFileName 拡張子なし', parseImageFileName('abc_01') === null);
check('parseImageFileName ゼロ埋めなし _5', parseImageFileName('abc_5.png')?.num === 5);
check('parseImageFileName _top (大小無視)',
  parseImageFileName('abc_top.jpg')?.kind === 'top' && parseImageFileName('abc_TOP.PNG')?.kind === 'top');
check('parseImageFileName _00 は白抜き', parseImageFileName('abc_00.jpg')?.kind === 'white');
// 2026-08-08 スタッフ指摘: TOP画像 = 商品画像1、_01 は商品画像2 へずれる
check('slotOfParsedName: _top→1 / _01→2 / _19→20 / _00→null',
  slotOfParsedName(parseImageFileName('a_top.jpg')) === 1
  && slotOfParsedName(parseImageFileName('a_01.jpg')) === 2
  && slotOfParsedName(parseImageFileName('a_19.jpg')) === 20
  && slotOfParsedName(parseImageFileName('a_00.jpg')) === null);

const fim = (name, mimeType = 'image/jpeg', modifiedTime = null) => ({ id: 'id-' + name, name, mimeType, modifiedTime });
let asn = assignImageSlots([fim('code_02.png'), fim('code_00.jpg'), fim('code_top.jpg'), fim('code_01.jpg')], 'code');
check('_00→白抜き / _top→枠1 / _01,_02→枠2,3 (枠順)',
  asn.whiteBg?.id === 'id-code_00.jpg' && asn.slots.length === 3
  && asn.slots[0].slot === 1 && asn.slots[0].id === 'id-code_top.jpg'
  && asn.slots[1].slot === 2 && asn.slots[1].id === 'id-code_01.jpg'
  && asn.slots[2].slot === 3 && asn.slots[2].id === 'id-code_02.png',
  JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('code_19.jpg'), fim('code_20.jpg')], 'code');
check('_19 は枠20 (最終) / _20 は上限超えで skipped',
  asn.slots.some((x) => x.slot === 20 && x.id === 'id-code_19.jpg')
  && asn.skipped.some((x) => x.name === 'code_20.jpg'), JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('メモ.jpg'), fim('code_20.jpg'), fim('code_02.txt', 'text/plain')], 'code');
check('番号なし/上限超え/非画像 → skipped',
  asn.slots.length === 1 && asn.skipped.length === 3
  && asn.skipped.some((s) => s.name === 'code_20.jpg'), JSON.stringify(asn.skipped));

asn = assignImageSlots([fim('code_01.jpg'), fim('other_01.jpg'), fim('other_02.jpg')], 'code');
check('商品コード一致だけを採用 (他コードは除外・conflictにしない)',
  asn.slots.length === 1 && asn.slots[0].id === 'id-code_01.jpg' && asn.conflicts.length === 0
  && asn.skipped.length === 2, JSON.stringify(asn));

asn = assignImageSlots([fim('xxx_01.jpg'), fim('xxx_03.jpg')], 'code');
check('コード一致ゼロは fail-closed (codeMatched=false・何も採用しない)',
  asn.codeMatched === false && asn.slots.length === 0 && asn.whiteBg === null
  && asn.skipped.length === 2, JSON.stringify(asn));

asn = assignImageSlots([fim('code_top.jpg'), fim('code_01.jpg'), fim('code_04.jpg')], 'code');
check('欠番はファイル名ラベルで報告 (_02,_03)',
  JSON.stringify(asn.missingLabels) === '["_02","_03"]' && asn.slots.length === 3, JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('code_02.jpg')], 'code');
check('_top が無ければ missingLabels に _top (TOP画像未設定が見える)',
  asn.missingLabels[0] === '_top', JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('code_01.png')], 'code');
check('同一番号の重複 → conflicts (セットしない)',
  asn.slots.length === 0 && asn.conflicts.length === 1 && asn.conflicts[0].label === '_01', JSON.stringify(asn));

asn = assignImageSlots([fim('code_top.jpg'), fim('code_top.png')], 'code');
check('_top の重複も conflicts', asn.conflicts.length === 1 && asn.conflicts[0].label === '_top', JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('code_21.jpg'), fim('code_21.png')], 'code');
check('上限超え番号は重複でも conflicts にしない (skippedで_01は取り込める)',
  asn.slots.length === 1 && asn.conflicts.length === 0 && asn.skipped.length === 2, JSON.stringify(asn));

asn = assignImageSlots([fim('CODE_01.JPG')], 'code');
check('大文字小文字は同一視', asn.slots.length === 1, JSON.stringify(asn));

asn = assignImageSlots([fim('code_0.jpg'), fim('code_19.jpg')], 'code');
check('_0 は白抜き / _19 は最終スロット20', asn.whiteBg?.id === 'id-code_0.jpg' && asn.slots[0]?.slot === 20, JSON.stringify(asn));

// サムネの版数 (2026-08-08): Drive の更新日時を拾って URL に載せる
asn = assignImageSlots([fim('code_top.jpg', 'image/jpeg', '2026-08-08T01:02:03.000Z')], 'code');
check('modifiedTime を slots に保持', asn.slots[0]?.modifiedTime === '2026-08-08T01:02:03.000Z', JSON.stringify(asn));

const { thumbnailUrl: thumbUrlFn } = await import('../lib/drive-link.js');
check('thumbnailUrl: 版数ありは ?v= 付き / 無効値は付けない',
  thumbUrlFn('abc', 160, '2026-08-08T01:02:03.000Z') === '/apps/product-hub/api/thumb/abc?w=160&v=' + Date.parse('2026-08-08T01:02:03.000Z')
  && thumbUrlFn('abc', 160) === '/apps/product-hub/api/thumb/abc?w=160'
  && thumbUrlFn('abc', 160, 'not-a-date') === '/apps/product-hub/api/thumb/abc?w=160');

// ─── shop-categories: AI 初期候補の採点 (2026-08-02) ───
const { suggestShopCategories, SHOP_CATEGORY_AUTO_APPLY_MIN_SCORE, canAutoApplyShopCategory } =
  await import('../lib/shop-categories.js');
const CATS = [
  { id: 1, path: '生活雑貨・日用品 > 洗濯用品', is_active: 1 },
  { id: 2, path: '生活雑貨・日用品 > キッチン用品', is_active: 1 },
  { id: 3, path: 'コスメ・美容 > アロマオイル', is_active: 1 },
  { id: 4, path: 'ペット用品', is_active: 1 },
  { id: 5, path: '旧カテゴリ > 洗濯用品', is_active: 0 },
];
let sug = suggestShopCategories(CATS, { name: '洗濯ネット 3枚セット', genrePath: '日用品雑貨・文房具・手芸 > 洗濯用品 > 洗濯ネット' });
check('店舗内カテゴリ提案: 末端一致が1位 + 自動適用しきい値以上',
  sug[0]?.id === 1 && sug[0].score >= SHOP_CATEGORY_AUTO_APPLY_MIN_SCORE, JSON.stringify(sug));
check('店舗内カテゴリ提案: is_active=0 は候補に出ない', !sug.some((s) => s.id === 5), JSON.stringify(sug));
check('店舗内カテゴリ提案: スコア0 (無関係) は返さない', !sug.some((s) => s.id === 4), JSON.stringify(sug));
sug = suggestShopCategories(CATS, { name: 'アロマオイル ラベンダー 10ml', genrePath: '' });
check('店舗内カテゴリ提案: 商品名だけでも末端一致', sug[0]?.id === 3, JSON.stringify(sug));
check('店舗内カテゴリ提案: 材料が空なら空配列', suggestShopCategories(CATS, { name: '', genrePath: '' }).length === 0);
sug = suggestShopCategories(CATS, { name: '洗濯ネット' }, { max: 1 });
check('店舗内カテゴリ提案: max 件数制限', sug.length === 1, JSON.stringify(sug));
check('店舗内カテゴリ提案: 外れたカテゴリでも選択中なら採点対象',
  suggestShopCategories([{ id: 5, path: '旧カテゴリ > 洗濯用品', is_active: 0, selected: 1 }], { name: '洗濯ネット' }).length === 1);

// 自動適用の可否 (Codex R1 high: スコア合計だけだと「親だけ一致」で無関係な末端が自動保存される)
const SIBLINGS = [
  { id: 11, path: '生活雑貨・日用品 > 洗濯用品 > 物干し', is_active: 1 },
  { id: 12, path: '生活雑貨・日用品 > 洗濯用品 > アイロン', is_active: 1 },
];
const parentOnly = suggestShopCategories(SIBLINGS,
  { name: 'なにか', genrePath: '日用品雑貨・文房具・手芸 > 生活雑貨・日用品 > 洗濯用品' });
check('自動適用しない: 親セグメントだけ一致 (スコア4に達しても末端が言い当てられていない)',
  parentOnly[0].score >= SHOP_CATEGORY_AUTO_APPLY_MIN_SCORE && canAutoApplyShopCategory(parentOnly) === false,
  JSON.stringify(parentOnly));
check('自動適用しない: 同点1位が複数 (どの棚か決められない)',
  canAutoApplyShopCategory([
    { id: 1, path: 'A > 洗濯用品', score: 6, leafExact: true },
    { id: 2, path: 'B > 洗濯用品', score: 6, leafExact: true },
  ]) === false);
check('自動適用する: 末端完全一致で単独1位',
  canAutoApplyShopCategory([
    { id: 1, path: '生活雑貨・日用品 > 洗濯用品', score: 6, leafExact: true },
    { id: 2, path: '生活雑貨・日用品 > キッチン用品', score: 2, leafExact: false },
  ]) === true);
check('自動適用しない: 候補ゼロ', canAutoApplyShopCategory([]) === false && canAutoApplyShopCategory(null) === false);

// 保存直前の再検証 (Codex R3 high: GET後にジャンルが変わって候補が入れ替わったケース)
const { isAutoApplyRequestValid } = await import('../lib/shop-categories.js');
const FRESH = [
  { id: 7, path: '生活雑貨・日用品 > 洗濯用品', score: 6, leafExact: true },
  { id: 8, path: 'コスメ・美容 > アロマオイル', score: 2, leafExact: false },
];
check('自動適用の再検証: 現在の1位と一致すれば通す', isAutoApplyRequestValid(FRESH, [7]) === true);
check('自動適用の再検証: 別カテゴリ (古い候補) は弾く', isAutoApplyRequestValid(FRESH, [8]) === false);
check('自動適用の再検証: 複数IDや空は弾く',
  isAutoApplyRequestValid(FRESH, [7, 8]) === false && isAutoApplyRequestValid(FRESH, []) === false);
check('自動適用の再検証: 現在は自動適用不可なら弾く',
  isAutoApplyRequestValid([{ id: 7, path: 'x', score: 6, leafExact: false }], [7]) === false);
check('leafExact フラグが立つのは末端完全一致のときだけ',
  suggestShopCategories(CATS, { name: 'アロマオイル ラベンダー 10ml' })[0].leafExact === true
  && parentOnly.every((c) => c.leafExact === false), JSON.stringify(parentOnly));
// 語頭の補助点だけで拾った候補 (「洗濯ネット」→「洗濯用品」) は表示のみで自動適用しない
const headOnly = suggestShopCategories(CATS, { name: '洗濯ネット 3枚セット' });
check('自動適用しない: 語頭一致どまり (棚名そのものは言い当てていない)',
  headOnly[0].id === 1 && headOnly[0].leafExact === false && canAutoApplyShopCategory(headOnly) === false,
  JSON.stringify(headOnly));

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

// 画像の有無はゲートで見ない (2026-08-24 中原さん: 白抜きだけで登録を進める運用がある)
let reasons = dbmod.gateReasons(db, draft);
check('gate blocks (no url)', reasons.length === 1, JSON.stringify(reasons));

// AI の参照元は「公式URL / 参考URL / Amazon URL のどれか1つ」でよい (2026-08-02 中原さん)
db.prepare(`UPDATE product_drafts SET amazon_url = 'https://www.amazon.co.jp/dp/B0TEST' WHERE id = ?`).run(draft.id);
reasons = dbmod.gateReasons(db, db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draft.id));
check('gate: 公式URLが無くても Amazon URL があれば参照元は満たす',
  !reasons.some((r) => r.includes('URL')), JSON.stringify(reasons));
db.prepare(`UPDATE product_drafts SET amazon_url = NULL WHERE id = ?`).run(draft.id);
db.prepare(`INSERT INTO draft_reference_urls (draft_id, url) VALUES (?, 'https://example.com/ref')`).run(draft.id);
reasons = dbmod.gateReasons(db, db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draft.id));
check('gate: 参考URLだけでも参照元は満たす', !reasons.some((r) => r.includes('URL')), JSON.stringify(reasons));
db.prepare(`DELETE FROM draft_reference_urls WHERE draft_id = ?`).run(draft.id);

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

// ─── CHECK 制約 (Codex R1 low: 同居DBの不正状態防止) ───
let checkErr = null;
try { db.prepare(`INSERT INTO product_drafts (ne_code, name, status) VALUES ('SMOKE-BAD', 'x', 'bogus')`).run(); } catch (e) { checkErr = e; }
check('status CHECK enforced', checkErr && String(checkErr.message).includes('CHECK'));

// ─── 自動差し戻し (Codex R1 high: ゲートすり抜け防止。PR4 で workflow-progress へ移設) ───
const wfpEarly = await import('../lib/workflow-progress.js');
db.prepare(`UPDATE product_drafts SET status = 'ready_for_ai' WHERE id = ?`).run(draft.id);
check('demote no-op while gate ok', wfpEarly.demoteIfGateBroken(db, draft.id, 'smoke') === null);
db.prepare(`UPDATE product_drafts SET official_url = NULL WHERE id = ?`).run(draft.id);
const demoteReasons = wfpEarly.demoteIfGateBroken(db, draft.id, 'smoke');
check('demote fires when url cleared', Array.isArray(demoteReasons) && demoteReasons.length === 1);
check('demoted back to draft', db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(draft.id).status === 'draft');
check('demote resets basic_info step', (db.prepare(
  `SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'basic_info'`
).get(draft.id)?.state ?? 'todo') !== 'done');
db.prepare(`UPDATE product_drafts SET official_url = 'https://example.com/item' WHERE id = ?`).run(draft.id);

// ─── P1.5: draft_yahoo / draft_image_production / 新列 / 生成キュー ───
const cols = new Set(db.prepare('PRAGMA table_info(product_drafts)').all().map((c) => c.name));
check('new columns exist', ['asin', 'amazon_url', 'own_brand'].every((c) => cols.has(c)));

dbmod.upsertDraftYahoo(db, draft.id, { yahoo_price: 1980, delivery_label: 'ネコポス' });
dbmod.upsertDraftYahoo(db, draft.id, { tax_rate: '10%' }); // 部分更新: 既存値が残ること
const y = db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(draft.id);
check('draft_yahoo partial upsert', y.yahoo_price === 1980 && y.delivery_label === 'ネコポス' && y.tax_rate === '10%');

let yahooCheckErr = null;
try { dbmod.upsertDraftYahoo(db, draft.id, { yahoo_price: -5 }); } catch (e) { yahooCheckErr = e; }
check('draft_yahoo price CHECK', yahooCheckErr && String(yahooCheckErr.message).includes('CHECK'));

dbmod.upsertImageProduction(db, draft.id, { status: '参考画像収集', designer: '外注_大川さん' });
dbmod.upsertImageProduction(db, draft.id, { request_text: '画像作成お願いします' });
const ip = db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(draft.id);
check('image_production partial upsert', ip.status === '参考画像収集' && ip.designer === '外注_大川さん' && ip.request_text === '画像作成お願いします');

db.prepare(`UPDATE product_drafts SET status = 'ready_for_ai' WHERE id = ?`).run(draft.id);
db.prepare(`INSERT INTO draft_reference_urls (draft_id, url) VALUES (?, 'https://example.com/ref1')`).run(draft.id);
db.prepare(`INSERT INTO draft_specs (draft_id, spec_key, spec_value) VALUES (?, 'サイズ', 'W10cm')`).run(draft.id);
const queue = dbmod.listGenerationQueue(db);
const qd = queue.find((q) => q.id === draft.id);
check('generation queue shape', qd && qd.ne_code === 'SMOKE-1' && qd.reference_urls.length === 1
  && qd.specs[0].key === 'サイズ' && qd.yahoo?.yahoo_price === 1980 && qd.image_count === 1, JSON.stringify(qd));
db.prepare(`UPDATE product_drafts SET status = 'draft' WHERE id = ?`).run(draft.id);

// ─── notion-card fail-closed (env 未設定 → failed で残り、登録は無事) ───
delete process.env.RYS_NOTION_TOKEN;
const notionCard = await import('../services/notion-card.js');
// 既定 OFF (2026-07-25) の確認と、以降のテストのための ON 切替
check('notion連携は既定OFF', notionCard.isNotionCardEnabled() === false);
check('OFF中は disabled を返す', (await notionCard.attemptCardCreation(draft.id, {})).outcome === 'disabled');
process.env.PH_NOTION_CARD_ENABLED = '1'; // ここから下は「ONのときの内部動作」の検証

// buildProperties: 公式URL/Amazon URL が url 型プロパティで含まれる (2026-07-05 修正の回帰チェック)
const props = notionCard.buildProperties({
  name: 'テスト', ne_code: 'X-1', price: 1980, jan_code: '4901234567890',
  official_url: 'https://example.com/official', amazon_url: 'https://www.amazon.co.jp/dp/B0TEST',
});
check('buildProperties includes メーカーページURL', props['メーカーページURL']?.url === 'https://example.com/official');
check('buildProperties includes amazon販売ページ', props['amazon販売ページ']?.url === 'https://www.amazon.co.jp/dp/B0TEST');
const propsNoUrl = notionCard.buildProperties({ name: 'テスト', ne_code: 'X-2' });
check('buildProperties omits URL props when empty', !('メーカーページURL' in propsNoUrl) && !('amazon販売ページ' in propsNoUrl));

// syncCardLinks: カード未作成なら no_card / カードありで API 失敗なら fail-soft
check('syncCardLinks no_card', (await notionCard.syncCardLinks(draft.id, { actor: 'smoke' })).outcome === 'no_card');
db.prepare(`UPDATE product_drafts SET notion_page_id = 'fake-page-id' WHERE id = ?`).run(draft.id);
const syncFail = await notionCard.syncCardLinks(draft.id, { actor: 'smoke' });
check('syncCardLinks fail-soft (env欠落)', syncFail.outcome === 'failed' && !!syncFail.error, JSON.stringify(syncFail));
check('syncCardLinks failure logged', db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'notion_card_sync_failed'`).get(draft.id).c === 1);
db.prepare(`UPDATE product_drafts SET notion_page_id = NULL WHERE id = ?`).run(draft.id);

const attempt = await notionCard.attemptCardCreation(draft.id, { actor: 'smoke' });
check('notion attempt fails safely', attempt.outcome === 'failed', JSON.stringify(attempt));
const after = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draft.id);
check('notion status = failed + error saved', after.notion_card_status === 'failed' && !!after.notion_card_error);
check('pendingCardCount counts it', notionCard.pendingCardCount() >= 1);

// ─── Notion 取り込み (検証用の選択インポート。finder を注入して API 非接続) ───
const imp = await import('../services/notion-import.js');

check('parseNeCodes splits/dedups/trims',
  JSON.stringify(imp.parseNeCodes(' A-1 \nB-2,A-1\n\n , C-3\t')) === JSON.stringify(['A-1', 'B-2', 'C-3']),
  JSON.stringify(imp.parseNeCodes(' A-1 \nB-2,A-1\n\n , C-3\t')));

const rt = (s) => ({ type: 'rich_text', rich_text: [{ plain_text: s }] });
const fakePage = (neCode) => ({
  id: `page-${neCode}`,
  properties: {
    'Name': { type: 'title', title: [{ plain_text: `取込商品 ${neCode}` }] },
    '商品コード': rt(neCode),
    '売価': { type: 'number', number: 1980 },
    'Yahoo価格（佐川の場合）': { type: 'number', number: 2200 },
    '配送方法': { type: 'select', select: { name: 'ネコポス' } },
    '税率': { type: 'select', select: { name: '10%' } },
    'バリエーション有無': { type: 'select', select: { name: 'あり' } },
    'Yahoo!タイトル': rt('Yahooタイトル本文'),
    'キャッチコピー': rt('キャッチ本文'),
    '商品説明文': rt('説明本文'),
    'HTML_商品説明文': rt('<p>html</p>'),
    'JANコード': { type: 'number', number: 4901234567890 },
    'Status': { type: 'select', select: { name: '⓪新規商品_高島' } },
    'Yahoo!カテゴリID': { type: 'number', number: 43494 },
    'Yahoo!path': rt('おもちゃ > 知育'),
    'メーカーページURL': { type: 'url', url: 'https://example.com/official' },
    'amazon販売ページ': { type: 'url', url: 'https://www.amazon.co.jp/dp/B0IMP' },
  },
});

const rec = imp.buildImportRecord(fakePage('IMP-1'));
check('buildImportRecord maps core fields',
  rec.ne_code === 'IMP-1' && rec.name === '取込商品 IMP-1' && rec.price === 1980
  && rec.jan_code === '4901234567890' && rec.has_variation === 1
  && rec.official_url === 'https://example.com/official'
  && rec.amazon_url === 'https://www.amazon.co.jp/dp/B0IMP'
  && rec.notion_status === '⓪新規商品_高島',
  JSON.stringify(rec));
check('buildImportRecord maps yahoo fields',
  rec.yahoo.yahoo_price_sagawa === 2200 && rec.yahoo.delivery_label === 'ネコポス'
  && rec.yahoo.tax_rate === '10%' && rec.yahoo.yahoo_category_id === 43494,
  JSON.stringify(rec.yahoo));
check('buildImportRecord reports HTML_商品説明文 as skipped',
  rec.skipped_fields.length === 1 && rec.skipped_fields[0] === 'HTML_商品説明文');
check('buildImportRecord null on missing 商品コード',
  imp.buildImportRecord({ id: 'p', properties: { 'Name': { type: 'title', title: [{ plain_text: 'x' }] } } }) === null);
// 判別できない「バリエーション有無」は単品(0)に倒す
check('has_variation unknown → 0', imp.buildImportRecord({
  id: 'p2', properties: { '商品コード': rt('V-0'), 'バリエーション有無': { type: 'select', select: { name: '未確認' } } },
}).has_variation === 0);

const fakeFinder = async (code) => (code === 'MISSING' ? null : fakePage(code));

const r1 = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: fakeFinder });
check('import → imported', r1.summary.imported === 1 && r1.results[0].outcome === 'imported', JSON.stringify(r1));
const imported = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'IMP-1'`).get();
check('imported row marked source=notion_import',
  imported.source === 'notion_import' && imported.status === 'draft'
  && imported.notion_page_id === 'page-IMP-1' && imported.notion_card_status === 'created'
  && imported.source_notion_status === '⓪新規商品_高島' && !!imported.imported_at,
  JSON.stringify(imported));
check('imported yahoo row written',
  db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(imported.id)?.delivery_label === 'ネコポス');
const aiRows = db.prepare('SELECT kind, content FROM draft_ai_outputs WHERE draft_id = ? ORDER BY kind').all(imported.id);
check('imported ai outputs (yahoo_title/desc_catch/desc_features)',
  aiRows.length === 3 && aiRows.map((r) => r.kind).join(',') === 'desc_catch,desc_features,yahoo_title',
  JSON.stringify(aiRows));
check('import logged', db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'notion_imported'`).get(imported.id).c === 1);

const r2 = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: fakeFinder });
check('re-import → updated (冪等・二重行を作らない)',
  r2.summary.updated === 1
  && db.prepare(`SELECT COUNT(*) c FROM product_drafts WHERE ne_code = 'IMP-1'`).get().c === 1,
  JSON.stringify(r2));

const r3 = await imp.importFromNotion(['MISSING'], { actor: 'smoke', finder: fakeFinder });
check('import not_found', r3.summary.not_found === 1);

// Notion 側で空にした項目は再取り込みで消える (スキップして古い値を残さない)
const blankPage = fakePage('IMP-1');
blankPage.properties['キャッチコピー'] = { type: 'rich_text', rich_text: [] };
const r2b = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: async () => blankPage });
check('re-import clears emptied Notion value',
  r2b.summary.updated === 1
  && db.prepare(`SELECT content FROM draft_ai_outputs WHERE draft_id = ? AND kind = 'desc_catch'`).get(imported.id).content === null,
  JSON.stringify(r2b));

// 検索コードと返却カードのコードが不一致なら書かない
const r5 = await imp.importFromNotion(['WANT-A'], { actor: 'smoke', finder: async () => fakePage('OTHER-B') });
check('import rejects code mismatch',
  r5.summary.failed === 1
  && db.prepare(`SELECT COUNT(*) c FROM product_drafts WHERE ne_code IN ('WANT-A','OTHER-B')`).get().c === 0,
  JSON.stringify(r5));

// 同じ商品コードに別 Notion ページが来たら黙って付け替えない
const r6 = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: async () => ({ ...fakePage('IMP-1'), id: 'page-DIFFERENT' }) });
check('import conflict_page_id (別カードへ黙って付け替えない)',
  r6.summary.conflict_page_id === 1
  && db.prepare(`SELECT notion_page_id FROM product_drafts WHERE ne_code = 'IMP-1'`).get().notion_page_id === 'page-IMP-1',
  JSON.stringify(r6));

// ポータル起点の商品は取り込みで塗り潰さない
const r4 = await imp.importFromNotion(['SMOKE-1'], { actor: 'smoke', finder: fakeFinder });
check('import conflict_portal (ポータル起点を上書きしない)',
  r4.summary.conflict_portal === 1
  && db.prepare(`SELECT name FROM product_drafts WHERE ne_code = 'SMOKE-1'`).get().name === 'スモーク商品',
  JSON.stringify(r4));
check('default source = portal', db.prepare(`SELECT source FROM product_drafts WHERE ne_code = 'SMOKE-1'`).get().source === 'portal');

let overErr = null;
try {
  await imp.importFromNotion(Array.from({ length: imp.MAX_IMPORT_CODES + 1 }, (_, i) => `OVER-${i}`), { finder: fakeFinder });
} catch (e) { overErr = e; }
check('import rejects over MAX_IMPORT_CODES', !!overErr);

// ─── ステータス①〜⑥の一括移植 (2026-08-25 中原さん指示) ───
{
  const schema = {
    properties: {
      Status: {
        type: 'select',
        select: {
          options: [
            { name: '⓪新規商品_高島' }, { name: '①ページ作成中' }, { name: '③画像待ち' },
            { name: '⑥完了' }, { name: 'アーカイブ' },
          ],
        },
      },
    },
  };
  let capturedFilter = null;
  const mkPage = (code, status) => {
    const p = fakePage(code);
    p.id = `page-mig-${code}-${status}`;
    p.properties.Status.select.name = status;
    return p;
  };
  const migPages = [
    mkPage('MIG-NEW-1', '①ページ作成中'),
    mkPage('IMP-1', '③画像待ち'),   // 既にアプリに居る (上の取り込みテストで作成済み)
    mkPage('MIG-NEW-1', '⑥完了'),    // Notion 側の重複カード
    { id: 'page-mig-nocode', properties: { Name: { type: 'title', title: [{ plain_text: 'コード無し' }] } } },
  ];
  let capturedOpts = null;
  const di = {
    config: () => ({ databaseId: 'db-test' }),
    request: async () => schema,
    query: async (opts) => { capturedFilter = opts.filter; capturedOpts = opts; return { pages: migPages }; },
  };
  const prev = await imp.importByNotionStatus({ actor: 'smoke', ...di });   // dryRun 既定
  check('一括移植: フィルタは ①〜⑥ で始まる選択肢だけ (⓪・その他を含まない) + ページ上限を拡張',
    capturedFilter.or.length === 3 && capturedFilter.or.every((f) => /^[①③⑥]/.test(f.select.equals))
    && capturedOpts.maxPages === 200,
    JSON.stringify(capturedFilter));
  check('一括移植: dryRun 既定では書き込まず対象とスナップショットを返す',
    prev.summary.would_import === 1 && prev.summary.already_exists === 1
    && prev.summary.duplicate === 1 && prev.summary.failed === 1 && prev.total === 4
    && typeof prev.snapshot === 'string' && prev.snapshot.length > 0
    && !db.prepare(`SELECT 1 FROM product_drafts WHERE ne_code = 'MIG-NEW-1'`).get(),
    JSON.stringify(prev.summary));
  // 実行はプレビューのスナップショット必須 (無し・不一致は書き込まず止める — Codex R1 high)
  let misErr = null;
  try { await imp.importByNotionStatus({ actor: 'smoke', dryRun: false, ...di }); } catch (e) { misErr = e; }
  check('一括移植: スナップショット無しの実行は拒否 (プレビュー必須)',
    misErr?.code === 'snapshot_mismatch'
    && !db.prepare(`SELECT 1 FROM product_drafts WHERE ne_code = 'MIG-NEW-1'`).get());
  const migRun = await imp.importByNotionStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: prev.snapshot, ...di });
  const migRow = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'MIG-NEW-1'`).get();
  check('一括移植: 実行でカードの無い商品だけ入る (source=notion_import・status=draft・原文ステータス保持)',
    migRun.summary.imported === 1 && migRow && migRow.source === 'notion_import'
    && migRow.status === 'draft' && migRow.source_notion_status === '①ページ作成中',
    JSON.stringify(migRun.summary));
  // 対象が変わった後の古いスナップショットは 409 相当で止まる
  let misErr2 = null;
  try { await imp.importByNotionStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: prev.snapshot, ...di }); } catch (e) { misErr2 = e; }
  check('一括移植: プレビュー後に対象が変わったら実行を止める', misErr2?.code === 'snapshot_mismatch');
  const prev2 = await imp.importByNotionStatus({ actor: 'smoke', ...di });
  check('一括移植: 再プレビューは already_exists になり二重取り込みしない',
    prev2.summary.would_import === 0 && prev2.summary.already_exists === 2, JSON.stringify(prev2.summary));
  // R2対応: 対象の中身 (値) が変わっても古いスナップショットは止まる (コード+IDだけの照合では素通りする)
  const valPages = [mkPage('MIG-VAL-1', '①ページ作成中')];
  const diVal = { ...di, query: async () => ({ pages: valPages }) };
  const valPrev = await imp.importByNotionStatus({ actor: 'smoke', ...diVal });
  valPages[0].properties['売価'].number = 2980;   // プレビュー後に価格が変わった
  let valErr = null;
  try { await imp.importByNotionStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: valPrev.snapshot, ...diVal }); } catch (e) { valErr = e; }
  check('一括移植: プレビュー後に値が変わった商品も実行を止める (保存内容全体のハッシュ照合)',
    valErr?.code === 'snapshot_mismatch'
    && !db.prepare(`SELECT 1 FROM product_drafts WHERE ne_code = 'MIG-VAL-1'`).get());
  // 1回の実行上限: 超えた分は deferred で報告し、再プレビュー→実行で続きから入る
  const capPages = [mkPage('MIG-CAP-1', '①ページ作成中'), mkPage('MIG-CAP-2', '③画像待ち')];
  const diCap = { ...di, query: async () => ({ pages: capPages }) };
  const capPrev = await imp.importByNotionStatus({ actor: 'smoke', ...diCap });
  const capRun = await imp.importByNotionStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: capPrev.snapshot, maxPerRun: 1, ...diCap });
  check('一括移植: 1回の上限を超えた分は deferred (再実行で続きから)',
    capRun.summary.imported === 1 && capRun.summary.deferred === 1
    && db.prepare(`SELECT COUNT(*) c FROM product_drafts WHERE ne_code LIKE 'MIG-CAP-%'`).get().c === 1,
    JSON.stringify(capRun.summary));
  db.prepare(`DELETE FROM product_drafts WHERE ne_code IN ('MIG-NEW-1', 'MIG-CAP-1', 'MIG-CAP-2')`).run();
}

// 取り込み由来は Notion へ書き戻さない (これが無いと既存カードの URL が消える)
check('syncCardLinks skips imported',
  (await notionCard.syncCardLinks(imported.id, { actor: 'smoke' })).outcome === 'skipped_not_portal');
check('attemptCardCreation skips imported',
  (await notionCard.attemptCardCreation(imported.id, { actor: 'smoke' })).outcome === 'skipped_not_portal');
// fail-closed: source が未知の値でも書き戻さない (deny-list だとここが通ってしまう)
db.prepare(`UPDATE product_drafts SET source = 'notion-import' WHERE id = ?`).run(imported.id);
check('syncCardLinks fail-closed on unknown source',
  (await notionCard.syncCardLinks(imported.id, { actor: 'smoke' })).outcome === 'skipped_not_portal');
check('attemptCardCreation fail-closed on unknown source',
  (await notionCard.attemptCardCreation(imported.id, { actor: 'smoke' })).outcome === 'skipped_not_portal');
db.prepare(`UPDATE product_drafts SET source = 'notion_import' WHERE id = ?`).run(imported.id);
check('canWriteToNotion allow-list',
  dbmod.canWriteToNotion({ source: 'portal' }) === true
  && dbmod.canWriteToNotion({ source: 'notion_import' }) === false
  && dbmod.canWriteToNotion({ source: 'unknown' }) === false
  && dbmod.canWriteToNotion(null) === false);
// 空URLは送らない = Notion 側の既存値を消さない (#423 の { url: null } を撤回)
db.prepare(`UPDATE product_drafts SET official_url = NULL, amazon_url = NULL WHERE id = ?`).run(draft.id);
db.prepare(`UPDATE product_drafts SET notion_page_id = 'fake-page-id' WHERE id = ?`).run(draft.id);
check('syncCardLinks does not clear (nothing_to_sync)',
  (await notionCard.syncCardLinks(draft.id, { actor: 'smoke' })).outcome === 'nothing_to_sync');
db.prepare(`UPDATE product_drafts SET official_url = 'https://example.com/item', notion_page_id = NULL WHERE id = ?`).run(draft.id);
check('no sync/create event written for imported',
  db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ?
    AND event IN ('notion_card_links_synced','notion_card_sync_failed','notion_card_failed')`).get(imported.id).c === 0);
check('retryPendingCards excludes imported',
  (await notionCard.retryPendingCards({ actor: 'smoke' })).every((r) => r.draftId !== imported.id));

// 削除は取り込み由来だけ (children は CASCADE、draft_events は append-only なので孤児で残る)
const impId = imported.id;
db.prepare('DELETE FROM product_drafts WHERE id = ?').run(impId);
check('delete cascades children',
  db.prepare('SELECT COUNT(*) c FROM draft_yahoo WHERE draft_id = ?').get(impId).c === 0
  && db.prepare('SELECT COUNT(*) c FROM draft_ai_outputs WHERE draft_id = ?').get(impId).c === 0);

// ─── バリエーション判定 (NE 代表商品コード) ───
const vari = await import('../lib/variation.js');
// mirror_products に実データ相当を入れる (rooms = 代表コードだが商品としては実在しない = 本番の93%型)
const insProd = db.prepare(`INSERT OR REPLACE INTO mirror_products
  (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, ?, '1', '取扱中', 'ok', ?, '2026-07-25T00:00:00Z')`);
insProd.run(9001, 'rooms-l-bk', 'ルームズ L 黒', 'rooms');
insProd.run(9002, 'rooms-l-wh', 'ルームズ L 白', 'rooms');
insProd.run(9003, 'rooms-m-bk', 'ルームズ M 黒', 'rooms');
insProd.run(9004, 'SOLO-1', '単品商品', null);          // 代表なし = 単品
insProd.run(9005, 'SELF-REP', '自分が代表', 'SELF-REP'); // 代表 = 自分自身 (本番27件型)

const vChild = vari.resolveVariationGroup(db, 'rooms-l-bk');
check('variation: 子SKU → 代表コードでまとめ判定',
  vChild.kind === 'variation' && vChild.groupKey === 'rooms' && vChild.isChild === true
  && vChild.memberCount === 3 && vChild.members.length === 3,
  JSON.stringify({ ...vChild, members: vChild.members.length }));
const vRep = vari.resolveVariationGroup(db, 'rooms');
check('variation: 代表コード自体 (商品として実在しなくても解決)',
  vRep.kind === 'variation' && vRep.groupKey === 'rooms' && vRep.isChild === false
  && vRep.memberCount === 3 && vRep.found === false,
  JSON.stringify({ ...vRep, members: vRep.members.length }));
check('variation: 大文字/前後空白ゆらぎを吸収',
  vari.resolveVariationGroup(db, '  ROOMS-L-BK ').groupKey === 'rooms');
check('variation: 単品', vari.resolveVariationGroup(db, 'SOLO-1').kind === 'single');
check('variation: 代表=自分自身は子SKU扱いにしない',
  (() => { const v = vari.resolveVariationGroup(db, 'SELF-REP'); return v.kind === 'variation' && v.isChild === false; })());
check('variation: NEに無いコードは unknown',
  vari.resolveVariationGroup(db, 'NOT-IN-NE-XYZ').kind === 'unknown');

const batch = vari.resolveVariationGroupsBatch(db, ['rooms-l-bk', 'SOLO-1', 'NOT-IN-NE-XYZ', 'rooms']);
check('variation batch: 一括判定が単発と一致',
  batch.get('rooms-l-bk').kind === 'variation' && batch.get('rooms-l-bk').isChild === true
  && batch.get('rooms-l-bk').memberCount === 3
  && batch.get('solo-1').kind === 'single'
  && batch.get('not-in-ne-xyz').kind === 'unknown'
  && batch.get('rooms').isChild === false,
  JSON.stringify([...batch]));
check('variation batch: 空入力で落ちない', vari.resolveVariationGroupsBatch(db, []).size === 0);

check('effectiveHasVariation: NE優先 (手入力を上書き)',
  vari.effectiveHasVariation(vChild, { has_variation: 0 }).value === true
  && vari.effectiveHasVariation(vChild, { has_variation: 0 }).source === 'ne'
  && vari.effectiveHasVariation({ kind: 'single' }, { has_variation: 1 }).value === false);
check('effectiveHasVariation: NEに無ければ手入力へフォールバック',
  vari.effectiveHasVariation({ kind: 'unknown' }, { has_variation: 1 }).value === true
  && vari.effectiveHasVariation({ kind: 'unknown' }, { has_variation: 1 }).source === 'manual');
check('effectiveHasVariation: conflict も手入力へフォールバック',
  vari.effectiveHasVariation({ kind: 'conflict' }, { has_variation: 1 }).source === 'manual');

// 正規化後に重複する商品コードは判定不能 (conflict) にする — Codex R1 low-8
insProd.run(9006, 'DUP-1', '重複その1', null);
insProd.run(9007, ' dup-1 ', '重複その2', 'rooms');
check('variation: 正規化後の重複は conflict',
  vari.resolveVariationGroup(db, 'DUP-1').kind === 'conflict'
  && vari.resolveVariationGroupsBatch(db, ['DUP-1']).get('dup-1').kind === 'conflict');
db.prepare(`DELETE FROM mirror_products WHERE product_id IN (9006, 9007)`).run();

// IN 句の分割 (SQLite バインド上限を超える入力でも落ちない) — Codex R1 low-7
const many = Array.from({ length: 1200 }, (_, i) => `BULK-${i}`);
check('variation batch: 1200件でも too many SQL variables にならない',
  vari.resolveVariationGroupsBatch(db, many).size === 1200);

// 正規化 UNIQUE index: 'ABC' と ' abc ' を別ドラフトにできない — Codex R1 medium-4
db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('CASE-A', 'ケース検証')`).run();
let caseErr = null;
try { db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES (' case-a ', '大文字ゆらぎ')`).run(); } catch (e) { caseErr = e; }
check('ne_code 正規化 UNIQUE (大小/空白ゆらぎを別行にしない)', caseErr && /UNIQUE/i.test(String(caseErr.message)), String(caseErr && caseErr.message));

// ─── regroup API (子SKU → 代表コード)。競合条件まで検証 (Codex R2 補足) ───
const rg = await import('../services/regroup.js');
const mkChild = (extra = {}) => {
  db.prepare(`DELETE FROM product_drafts WHERE ne_code = ?`).run(`rooms-l-bk`);
  db.prepare(`DELETE FROM product_drafts WHERE ne_code = 'rooms'`).run();
  const info = db.prepare(`INSERT INTO product_drafts (ne_code, name, source) VALUES ('rooms-l-bk', '子SKU', 'portal')`).run();
  const did = Number(info.lastInsertRowid);
  for (const [k, v] of Object.entries(extra)) {
    db.prepare(`UPDATE product_drafts SET ${k} = ? WHERE id = ?`).run(v, did);
  }
  return did;
};

let did = mkChild();
const okRes = rg.regroupToRepCode(db, did, { expectedFrom: 'rooms-l-bk', expectedTo: 'rooms', actor: 'smoke' });
check('regroup: 成功して ne_code が代表コードになる',
  okRes.code === 200 && db.prepare('SELECT ne_code FROM product_drafts WHERE id = ?').get(did).ne_code === 'rooms',
  JSON.stringify(okRes));
check('regroup: 監査ログが残る',
  db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'regrouped_to_rep_code'`).get(did).c === 1);

did = mkChild();
check('regroup: expected_to 不一致は409 (画面が古い)',
  rg.regroupToRepCode(db, did, { expectedFrom: 'rooms-l-bk', expectedTo: 'OTHER' }).code === 409);
check('regroup: expected_from 不一致は409',
  rg.regroupToRepCode(db, did, { expectedFrom: 'WRONG', expectedTo: 'rooms' }).code === 409);
check('regroup: expected 欠落は400',
  rg.regroupToRepCode(db, did, {}).code === 400);
check('regroup: 弾かれた後も ne_code は変わっていない',
  db.prepare('SELECT ne_code FROM product_drafts WHERE id = ?').get(did).ne_code === 'rooms-l-bk');

// Notion カード作成を一度でも試みた行は付け替えない (旧コードのカードが新コードに紐づく事故を防ぐ)
did = mkChild({ notion_card_attempts: 1 });
check('regroup: attempts>0 は拒否',
  rg.regroupToRepCode(db, did, { expectedFrom: 'rooms-l-bk', expectedTo: 'rooms' }).code === 400);
did = mkChild({ notion_card_status: 'creating' });
check('regroup: creating 中は拒否',
  rg.regroupToRepCode(db, did, { expectedFrom: 'rooms-l-bk', expectedTo: 'rooms' }).code === 400);
did = mkChild({ notion_page_id: 'page-x', notion_card_status: 'created' });
check('regroup: カード作成済みは拒否',
  rg.regroupToRepCode(db, did, { expectedFrom: 'rooms-l-bk', expectedTo: 'rooms' }).code === 400);
did = mkChild({ source: 'notion_import' });
check('regroup: 取り込み由来は拒否',
  rg.regroupToRepCode(db, did, { expectedFrom: 'rooms-l-bk', expectedTo: 'rooms' }).code === 400);

// 代表コードのドラフトが既にある場合
did = mkChild();
db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('rooms', '先客')`).run();
check('regroup: 代表コードのドラフトが既にあれば拒否',
  rg.regroupToRepCode(db, did, { expectedFrom: 'rooms-l-bk', expectedTo: 'rooms' }).code === 400);
db.prepare(`DELETE FROM product_drafts WHERE ne_code = 'rooms'`).run();

// 単品/対象外
db.prepare(`DELETE FROM product_drafts WHERE ne_code = 'rooms-l-bk'`).run();
const soloId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('SOLO-1', '単品')`).run().lastInsertRowid);
check('regroup: 単品は対象外 (400)',
  rg.regroupToRepCode(db, soloId, { expectedFrom: 'SOLO-1', expectedTo: 'SOLO-1' }).code === 400);
check('regroup: 存在しないIDは404',
  rg.regroupToRepCode(db, 999999, { expectedFrom: 'a', expectedTo: 'b' }).code === 404);

// ─── バリエーション除外 (既定でまとめ、例外だけ外す) ───
db.prepare(`DELETE FROM product_drafts WHERE ne_code IN ('rooms-l-bk','rooms')`).run();
const grpId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, source) VALUES ('rooms', 'ルームズ', 'portal')`).run().lastInsertRowid);

let gv = vari.resolveVariationGroup(db, 'rooms', { draftId: grpId });
check('除外なし: 全SKUが載る', gv.members.length === 3 && gv.excludedMembers.length === 0 && gv.memberCount === 3);

db.prepare(`INSERT INTO draft_variation_exclusions (draft_id, ne_code, actor) VALUES (?, 'rooms-m-bk', 'smoke')`).run(grpId);
gv = vari.resolveVariationGroup(db, 'rooms', { draftId: grpId });
check('外したSKUは一覧から消え、除外欄に出る',
  gv.members.length === 2 && gv.excludedMembers.length === 1
  && gv.excludedMembers[0].商品コード === 'rooms-m-bk' && gv.memberCount === 2,
  JSON.stringify({ m: gv.members.map((x) => x.商品コード), e: gv.excludedMembers.map((x) => x.商品コード) }));
// 除外は SKU 単位でグローバル一意 (draft A で外して B から戻せない不整合を防ぐ)
let exclDupErr = null;
try {
  db.prepare(`INSERT INTO draft_variation_exclusions (draft_id, ne_code) VALUES (?, ' ROOMS-M-BK ')`).run(grpId);
} catch (e) { exclDupErr = e; }
check('除外は正規化してSKU単位でグローバル一意', exclDupErr && /UNIQUE/i.test(String(exclDupErr.message)));
check('除外の一意制約が式インデックスで張られている',
  /LOWER\s*\(\s*TRIM\s*\(\s*ne_code/i.test(
    db.prepare(`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_draft_vari_excl_code'`).get()?.sql || ''
  ));

// 旧形 (draft単位 UNIQUE) の DB からの移行 — CREATE IF NOT EXISTS では張り替わらない (Codex R2 high)
{
  const keep = db.prepare('SELECT id, draft_id, ne_code FROM draft_variation_exclusions').all();
  db.exec('DROP TABLE draft_variation_exclusions');
  db.exec(`CREATE TABLE draft_variation_exclusions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
    ne_code TEXT NOT NULL, actor TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(draft_id, ne_code))`);
  const dA = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('MIG-A', 'a')`).run().lastInsertRowid);
  const dB = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('MIG-B', 'b')`).run().lastInsertRowid);
  const ins = db.prepare('INSERT INTO draft_variation_exclusions (draft_id, ne_code) VALUES (?, ?)');
  ins.run(dA, 'MIG-SKU');
  ins.run(dB, ' mig-sku ');   // 旧形では同一SKUを2ドラフトから除外できてしまう
  ins.run(dA, 'MIG-OTHER');
  check('旧形DB: 移行前は同一SKUが複数draftに入る',
    db.prepare('SELECT COUNT(*) c FROM draft_variation_exclusions').get().c === 3);

  dbmod._resetInitForTest();
  dbmod.initProductHubDB();

  const idxSql = db.prepare(`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_draft_vari_excl_code'`).get()?.sql || '';
  const tblSql = db.prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name='draft_variation_exclusions'`).get().sql;
  check('旧形DB: 重複が集約される (3→2)',
    db.prepare('SELECT COUNT(*) c FROM draft_variation_exclusions').get().c === 2);
  check('旧形DB: 旧UNIQUE(draft_id, ne_code) が除去される', !/UNIQUE\s*\(\s*draft_id/i.test(tblSql));
  check('旧形DB: 式UNIQUEに張り替わる', /LOWER\s*\(\s*TRIM\s*\(\s*ne_code/i.test(idxSql) && /UNIQUE/i.test(idxSql));
  check('旧形DB: 移行後も foreign_keys=ON に戻る', db.pragma('foreign_keys', { simple: true }) === 1);
  let migDup = null;
  try { ins.run(dB, 'Mig-Sku'); } catch (e) { migDup = e; }
  check('旧形DB: 移行後は重複INSERTがブロックされる', migDup && /UNIQUE/i.test(String(migDup.message)));
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(dA);
  check('旧形DB: 移行後も CASCADE が効く',
    db.prepare('SELECT COUNT(*) c FROM draft_variation_exclusions WHERE draft_id = ?').get(dA).c === 0);

  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(dB);

  // 「最古が孤児・後発が有効」の組み合わせ: 有効な除外を消さずに残すこと (Codex R3 high)
  db.exec('DELETE FROM draft_variation_exclusions');
  db.exec('DROP INDEX IF EXISTS idx_draft_vari_excl_code');
  const dC = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('MIG-C', 'c')`).run().lastInsertRowid);
  const orphanDraftId = 987654; // 実在しない親 = 孤児行
  // 孤児は FK ON では作れないので、旧環境の状態を再現するため一時的に外す
  db.pragma('foreign_keys = OFF');
  db.prepare('INSERT INTO draft_variation_exclusions (id, draft_id, ne_code) VALUES (1, ?, ?)').run(orphanDraftId, 'ORPH-SKU');
  db.prepare('INSERT INTO draft_variation_exclusions (id, draft_id, ne_code) VALUES (2, ?, ?)').run(dC, ' orph-sku ');
  db.pragma('foreign_keys = ON');
  dbmod._resetInitForTest();
  dbmod.initProductHubDB();
  const survived = db.prepare('SELECT draft_id, ne_code FROM draft_variation_exclusions').all();
  check('旧形DB: 最古が孤児でも有効な除外が生き残る',
    survived.length === 1 && survived[0].draft_id === dC,
    JSON.stringify(survived));
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(dC);

  // 後続テストのために元の除外行を復元
  db.exec('DELETE FROM draft_variation_exclusions');
  for (const r of keep) ins.run(r.draft_id, r.ne_code);
}

// 外したSKUは detached: 単独ページ扱いになり、自動まとめの対象外
check('外したSKUは detached', vari.resolveVariationGroup(db, 'rooms-m-bk').kind === 'detached');
check('detached はバッチでも detached',
  vari.resolveVariationGroupsBatch(db, ['rooms-m-bk']).get('rooms-m-bk').kind === 'detached');
check('detached の has_variation は なし(NE確定)',
  vari.effectiveHasVariation(vari.resolveVariationGroup(db, 'rooms-m-bk'), { has_variation: 1 }).value === false);
check('isDetached', vari.isDetached(db, 'ROOMS-M-BK') === true && vari.isDetached(db, 'rooms-l-bk') === false);

// 実効SKUが1件になったら「バリエーションあり」ではなくなる
db.prepare(`INSERT INTO draft_variation_exclusions (draft_id, ne_code, actor) VALUES (?, 'rooms-l-wh', 'smoke')`).run(grpId);
check('外して実効1SKUになったら has_variation=false',
  vari.effectiveHasVariation(vari.resolveVariationGroup(db, 'rooms', { draftId: grpId }), {}).value === false);
db.prepare(`DELETE FROM draft_variation_exclusions WHERE draft_id = ? AND ne_code = 'rooms-l-wh'`).run(grpId);

// CASCADE: ドラフトを消したら除外も消える
db.prepare(`DELETE FROM product_drafts WHERE id = ?`).run(grpId);
check('除外は draft 削除で CASCADE',
  db.prepare('SELECT COUNT(*) c FROM draft_variation_exclusions WHERE draft_id = ?').get(grpId).c === 0);
check('CASCADE 後は detached でなくなる', vari.resolveVariationGroup(db, 'rooms-m-bk').kind === 'variation');

// Notion worker が「読んでから claim するまで」に ne_code が変わったら claim を成立させない
// (旧コードのカードが新コードのドラフトに紐づく事故 — Codex R3 high)
db.prepare(`DELETE FROM product_drafts WHERE ne_code IN ('rooms-l-bk','rooms')`).run();
const raceId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, source) VALUES ('rooms-l-bk', 'レース検証', 'portal')`).run().lastInsertRowid);
db.prepare(`UPDATE product_drafts SET ne_code = 'rooms' WHERE id = ?`).run(raceId); // worker が読んだ後に付け替わった状況
const raceClaim = db.prepare(`
  UPDATE product_drafts SET notion_card_status = 'creating', notion_card_claim = 'tok'
  WHERE id = ? AND source = 'portal' AND notion_page_id IS NULL AND ne_code = ?
    AND notion_card_status IN ('pending','failed')
`).run(raceId, 'rooms-l-bk'); // 古い ne_code で claim を試みる
check('notion claim: ne_code が変わっていたら claim できない', raceClaim.changes === 0);
db.prepare(`DELETE FROM product_drafts WHERE id = ?`).run(raceId);
db.prepare(`DELETE FROM product_drafts WHERE ne_code = 'SOLO-1'`).run();

// ─── NE 初期値 (商品名・税率のみ。配送方法/在庫/JANは対象外) ───
check('taxToPercent: 小数(0.1)→10', vari.taxToPercent(0.1) === 10);
check('taxToPercent: 百分率(10)→10', vari.taxToPercent(10) === 10);
check('taxToPercent: 8%系も両形式', vari.taxToPercent(0.08) === 8 && vari.taxToPercent(8) === 8);
check('taxToPercent: 0/null は未設定扱い (誤った0%をYahooへ流さない)',
  vari.taxToPercent(0) === null && vari.taxToPercent(null) === null);

db.prepare(`UPDATE mirror_products SET 商品名 = ?, 消費税率 = ? WHERE 商品コード = 'SOLO-1'`).run('NE側の商品名', 0.1);
const nd = vari.getNeDefaults(db, 'SOLO-1');
check('getNeDefaults: 商品名と税率を返す', nd && nd.name === 'NE側の商品名' && nd.taxPercent === 10, JSON.stringify(nd));
check('getNeDefaults: 大小/空白ゆらぎ吸収', vari.getNeDefaults(db, ' solo-1 ')?.taxPercent === 10);
db.prepare(`UPDATE mirror_products SET 消費税率 = 0 WHERE 商品コード = 'SOLO-1'`).run();
check('getNeDefaults: NE税率0%は初期値にしない (空欄で人に設定させる)',
  vari.getNeDefaults(db, 'SOLO-1').taxPercent === null);
db.prepare(`UPDATE mirror_products SET 消費税率 = 0.1 WHERE 商品コード = 'SOLO-1'`).run();
check('getNeDefaults: NEに無ければ null', vari.getNeDefaults(db, 'NOT-IN-NE-XYZ') === null);
check('taxToPercent: 想定外の値は推測せず null (1 / 0.8 / 1000)',
  vari.taxToPercent(1) === null && vari.taxToPercent(0.8) === null && vari.taxToPercent(1000) === null);
// 丸めてから照合すると 9.6→10% のように誤税率が通る (Codex R2 high)
check('taxToPercent: 近い値でも丸めて通さない (9.6 / 7.6 / 0.096)',
  vari.taxToPercent(9.6) === null && vari.taxToPercent(7.6) === null && vari.taxToPercent(0.096) === null,
  JSON.stringify([vari.taxToPercent(9.6), vari.taxToPercent(7.6), vari.taxToPercent(0.096)]));
check('taxToPercent: 浮動小数の誤差は許容 (0.1→10 / 0.08→8)',
  vari.taxToPercent(0.1) === 10 && vari.taxToPercent(0.08) === 8);
check('taxToPercent: 許可は8%/10%のみ', JSON.stringify(vari.ALLOWED_TAX_PERCENTS) === '[8,10]');

// 画面と登録で同じ規則にする: 代表行が無く子SKU行だけでも初期値が取れる (Codex medium-4)
db.prepare(`UPDATE mirror_products SET 商品名 = ?, 消費税率 = ? WHERE 商品コード = 'rooms-l-bk'`).run('ルームズ子SKU', 0.08);
const rd = vari.resolveNeDefaults(db, 'rooms-l-bk');
check('resolveNeDefaults: 代表コード行が無くても子SKUで補える',
  rd && rd.name === 'ルームズ子SKU' && rd.taxPercent === 8, JSON.stringify(rd));
// 代表コード自体は NE に商品として存在しない (365種中338種) ので初期値は取れない。
// どの子SKUの名前を採るかは決められないため、推測せず null にする。
// 画面(GET)も登録(POST)も **入力されたコード** で引くので結果は必ず一致する
check('resolveNeDefaults: 実在しない代表コード指定は null (推測しない)',
  vari.resolveNeDefaults(db, 'rooms') === null);
check('resolveNeDefaults: NEに無ければ null', vari.resolveNeDefaults(db, 'NOT-IN-NE-XYZ') === null);

// 正規化後に重複する行があるとき、値が割れていたら採用しない (Codex R3 high)
insProd.run(9101, 'DUPTAX', '重複その1', null);
db.prepare(`UPDATE mirror_products SET 商品名 = ?, 消費税率 = ? WHERE product_id = 9101`).run('名前A', 0.1);
insProd.run(9102, ' duptax ', '重複その2', null);
db.prepare(`UPDATE mirror_products SET 商品名 = ?, 消費税率 = ? WHERE product_id = 9102`).run('名前B', 0.08);
const dt = vari.getNeDefaults(db, 'DUPTAX');
check('getNeDefaults: 重複行で値が割れたら採用しない (不定な税率をYahooへ流さない)',
  dt && dt.name === null && dt.taxPercent === null, JSON.stringify(dt));
// 全行が一致していれば採用してよい
db.prepare(`UPDATE mirror_products SET 商品名 = '名前A', 消費税率 = 0.1 WHERE product_id = 9102`).run();
const dt2 = vari.getNeDefaults(db, 'DUPTAX');
check('getNeDefaults: 重複でも全行一致なら採用', dt2.name === '名前A' && dt2.taxPercent === 10, JSON.stringify(dt2));
// 片方だけ値がある (10% / 未設定、名前あり / 空欄) も「一致」とみなさない (Codex R4 high)
db.prepare(`UPDATE mirror_products SET 商品名 = NULL, 消費税率 = 0 WHERE product_id = 9102`).run();
const dt3 = vari.getNeDefaults(db, 'DUPTAX');
check('getNeDefaults: 片方が空欄/未設定でも一致扱いにしない',
  dt3 && dt3.name === null && dt3.taxPercent === null, JSON.stringify(dt3));
db.prepare(`DELETE FROM mirror_products WHERE product_id IN (9101, 9102)`).run();

// ─── 新商品の自動取込 / 商品コードからの一括登録 ───
process.env.PH_NOTION_CARD_ENABLED = ''; // 本番と同じ既定OFFで検証
const intake = await import('../services/new-product-intake.js');

// mirror に「新商品」を用意 (sgs 型のバリエーション + 単品)
const insP = db.prepare(`INSERT OR REPLACE INTO mirror_products
  (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, 消費税率, updated_at)
  VALUES (?, ?, ?, '単品', '取扱中', 'ok', ?, ?, '2026-07-25T00:00:00Z')`);
insP.run(9201, 'sgs-or', 'メガネストラップ オレンジ', 'sgs', 0.1);
insP.run(9202, 'sgs-bk', 'メガネストラップ ブラック', 'sgs', 0.1);
insP.run(9203, 'flaxseed', 'アマニシード 100g', null, 0.1);
insP.run(9204, 'notaxprod', '税率未設定商品', null, 0);

// mirror が痩せている (同期途中の疑い) 間はシードもintakeも拒否 (Codex critical-2)
check('intake: 件数が下限未満ならシードしない',
  intake.syncNewProducts({}).error === 'mirror_too_small');
// 一括登録で seen に書かれても「シード済み」と誤認しない (Codex critical-1)
// (この時点で ph_ne_seen_codes は空でないかもしれないが、状態キーが無い限り seed モードのまま)
db.prepare(`INSERT OR IGNORE INTO ph_ne_seen_codes (code_key, ne_code) VALUES ('manual-1', 'MANUAL-1')`).run();
check('intake: seen に行があってもシード未了なら intake モードに入らない',
  intake.syncNewProducts({}).error === 'mirror_too_small' && intake.intakeStatus().initialized === false);
db.prepare(`DELETE FROM ph_ne_seen_codes WHERE code_key = 'manual-1'`).run();

// 下限を満たすダミー行を入れてからシードする
const fillStmt = db.prepare(`INSERT OR REPLACE INTO mirror_products
  (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at)
  VALUES (?, ?, ?, '単品', '取扱中', 'ok', '2026-07-25T00:00:00Z')`);
for (let i = 0; i < intake.MIN_SEED_SINGLES; i++) fillStmt.run(50000 + i, `FILL-${i}`, `既存商品${i}`);

// 初回は「シードだけ」= 既存商品が大量にドラフト化されない (カットオフ)
const seedDry = intake.syncNewProducts({ dryRun: true });
check('intake: 初回は seed モード', seedDry.mode === 'seed' && seedDry.created === 0, JSON.stringify(seedDry));
const draftsBefore = db.prepare('SELECT COUNT(*) c FROM product_drafts').get().c;
const seeded = intake.syncNewProducts({});
check('intake: 初回シードでドラフトを作らない',
  seeded.mode === 'seed' && seeded.created === 0
  && db.prepare('SELECT COUNT(*) c FROM product_drafts').get().c === draftsBefore,
  JSON.stringify(seeded));
check('intake: シード後は初期化済み (状態キーで判定)',
  intake.intakeStatus().initialized === true
  && !!db.prepare(`SELECT value FROM ph_intake_state WHERE key='seed_completed_at'`).get()?.value);

// 2回目以降: 新しく現れたコードだけが対象
const again = intake.syncNewProducts({});
check('intake: 変化が無ければ何も作らない', again.mode === 'intake' && again.created === 0, JSON.stringify(again));

insP.run(9205, 'newitem1', '新商品その1', null, 0.1);
insP.run(9206, 'sgs-gr', 'メガネストラップ グリーン', 'sgs', 0.1); // 既存グループの追加色
const run2 = intake.syncNewProducts({});
// newitem1 は単品新商品 → 1件。sgs-gr は既存グループの追加色だが、グループのドラフトが
// まだ無い (シードはドラフトを作らない) ので sgs のドラフトが1件できる = 人に見える。計2件
check('intake: 新商品ぶんだけドラフトを作る (単品1 + 新色でグループ1)',
  run2.created === 2
  && run2.drafts.some((d) => d.ne_code === 'newitem1')
  && run2.drafts.some((d) => d.ne_code === 'sgs' && d.from === 'sgs-gr'),
  JSON.stringify(run2));
check('intake: グループのドラフトは代表コード1件だけ',
  db.prepare(`SELECT COUNT(*) c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'sgs'`).get().c === 1);
// 一括登録テストは「グループのドラフトが無い」状態から始めたいので一旦消す
db.prepare(`DELETE FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'sgs'`).run();
check('intake: 作ったドラフトに created_by が付く',
  db.prepare(`SELECT created_by FROM product_drafts WHERE ne_code = 'newitem1'`).get().created_by === 'auto:ne-intake');
check('intake: NE税率が初期値で入る',
  db.prepare(`SELECT tax_rate FROM draft_yahoo WHERE draft_id = (SELECT id FROM product_drafts WHERE ne_code='newitem1')`).get()?.tax_rate === '10%');

// ── 一括登録 (人が明示実行。カットオフと無関係) ──
const dry = intake.registerByCodes(['sgs-or', 'sgs-bk', 'flaxseed', 'ZZZ-NOT-IN-NE'], { dryRun: true });
check('一括登録 dry-run: 7色は代表コードで1件にまとまる',
  dry.summary.created === 2 && dry.summary.merged === 1 && dry.summary.not_in_ne === 1,
  JSON.stringify(dry.summary));
check('一括登録 dry-run: 書き込まない',
  db.prepare(`SELECT COUNT(*) c FROM product_drafts WHERE LOWER(TRIM(ne_code)) IN ('sgs','flaxseed')`).get().c === 0);

const reg = intake.registerByCodes(['sgs-or', 'sgs-bk', 'flaxseed', 'notaxprod', 'ZZZ-NOT-IN-NE'], { actor: 'smoke' });
check('一括登録: 代表コード単位でドラフトが作られる',
  db.prepare(`SELECT COUNT(*) c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'sgs'`).get().c === 1,
  JSON.stringify(reg.summary));
check('一括登録: 2件目の同グループは merged',
  reg.summary.created === 3 && reg.summary.merged === 1 && reg.summary.not_in_ne === 1,
  JSON.stringify(reg.summary));
check('一括登録: NE税率0%の商品は税率を入れない (空欄で人が設定)',
  !db.prepare(`SELECT tax_rate FROM draft_yahoo WHERE draft_id = (SELECT id FROM product_drafts WHERE ne_code='notaxprod')`).get()?.tax_rate);
check('一括登録: 再実行しても増えない (冪等)',
  intake.registerByCodes(['sgs-or', 'flaxseed'], { actor: 'smoke' }).summary.created === 0);
check('一括登録: NotionカードはOFFなので作られない',
  db.prepare(`SELECT notion_card_status FROM product_drafts WHERE LOWER(TRIM(ne_code))='sgs'`).get().notion_card_status === 'pending'
  && notionCard.isNotionCardEnabled() === false);
check('一括登録: 上限を超えたら弾く',
  intake.registerByCodes(Array.from({ length: intake.MAX_REGISTER_CODES + 1 }, (_, i) => `X-${i}`), {}).error === 'too_many_codes');
check('一括登録: 空入力は弾く', intake.registerByCodes([], {}).error === 'no_codes');

// 後片付け (後続の render fixture に影響させない)
db.prepare(`DELETE FROM product_drafts WHERE LOWER(TRIM(ne_code)) IN ('sgs','flaxseed','notaxprod','newitem1')`).run();
db.prepare(`DELETE FROM mirror_products WHERE product_id BETWEEN 9201 AND 9206`).run();
db.prepare(`DELETE FROM mirror_products WHERE product_id BETWEEN 50000 AND ${50000 + intake.MIN_SEED_SINGLES}`).run();

// ─── 楽天出品 (P3): payload builder / 属性パース (RMS 非接続) ───
const listing = await import('../services/rakuten-listing.js');

// ─── サムネイルプロキシの pure ヘルパー (Codex R1: SSRF/リンク差し替え) ───
check('sizedThumbnailLink: =s220 → =w320', listing.sizedThumbnailLink('https://lh3.googleusercontent.com/abc=s220', 320) === 'https://lh3.googleusercontent.com/abc=w320');
check('sizedThumbnailLink: サフィックス無しはそのまま', listing.sizedThumbnailLink('https://lh3.googleusercontent.com/abc', 320) === 'https://lh3.googleusercontent.com/abc');
check('isAllowedThumbnailHost: lh3.googleusercontent.com OK', listing.isAllowedThumbnailHost('https://lh3.googleusercontent.com/abc=s220'));
check('isAllowedThumbnailHost: http は拒否', !listing.isAllowedThumbnailHost('http://lh3.googleusercontent.com/abc'));
check('isAllowedThumbnailHost: 他ホストは拒否', !listing.isAllowedThumbnailHost('https://evil.example.com/abc'));
check('isAllowedThumbnailHost: サフィックス偽装は拒否', !listing.isAllowedThumbnailHost('https://evilgoogleusercontent.com/abc'));
check('isAllowedThumbnailHost: 不正URLは拒否', !listing.isAllowedThumbnailHost('not a url'));

check('parseAttributes: [{name,values}] を受ける',
  JSON.stringify(listing.parseAttributes('[{"name":"ブランド名","values":["ノーブランド"]}]'))
  === '[{"name":"ブランド名","values":["ノーブランド"]}]');
check('parseAttributes: value 単数形も受けて values に正規化',
  listing.parseAttributes('[{"name":"原産国／製造国","value":"日本"}]')[0].values[0] === '日本');
check('parseAttributes: 空JSON → []', JSON.stringify(listing.parseAttributes('')) === '[]');
check('parseAttributes: 壊れたJSON → null (エラー扱い)', listing.parseAttributes('{oops') === null);
check('parseAttributes: name欠落 → null', listing.parseAttributes('[{"values":["x"]}]') === null);

// payload builder — 単品ドラフトで組み立て
const rkId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, price) VALUES ('rk-smoke-1', '出品スモーク商品', 1980)`).run().lastInsertRowid);
let built = listing.buildItemPayload(db, rkId);
check('payload: 不足理由を列挙 (ジャンル/画像)',
  built.ok === false && built.reasons.some((r) => r.includes('ジャンル')) && built.reasons.some((r) => r.includes('画像')),
  JSON.stringify(built.reasons));

db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, article_number)
  VALUES (?, '205761', '[{"name":"ブランド名","values":["ノーブランド品"]}]', NULL)`).run(rkId);
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'gfile1')`).run(rkId);
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gfile1', '/app-newitems/rk-smoke-1-1.jpg')`).run(rkId);
db.prepare(`INSERT INTO draft_ai_outputs (draft_id, kind, content) VALUES (?, 'rakuten_title', '楽天用タイトル'), (?, 'desc_catch', 'キャッチ'), (?, 'desc_features', '特徴文'), (?, 'desc_notes', 'AI注意書き文')`).run(rkId, rkId, rkId, rkId);
db.prepare(`INSERT INTO draft_specs (draft_id, spec_key, spec_value) VALUES (?, 'サイズ', 'W10cm')`).run(rkId);
// 画像トラック (依頼→制作→登録→承認) は楽天出品のゲート (2026-08-23 配線)。
// 上の 1 回目の buildItemPayload で工程行が「画像なし」の状態で作られているので、ここで済ませておく
// (本番でも、詳細画面を開いてから画像を足した商品は人が画像トラックを進めるまで出せない = 意図どおり)
db.prepare(`UPDATE draft_step_progress SET state = 'done', done_by = 'smoke'
            WHERE draft_id = ? AND step_code IN (SELECT code FROM ph_steps WHERE track = 'image')`).run(rkId);

built = listing.buildItemPayload(db, rkId);
check('payload: 組み立て成功', built.ok === true, JSON.stringify(built.reasons || null));
const pl = built.payload;
check('payload: hideItem=false (公開で登録 — 2026-08-05 中原さん指示)', pl.hideItem === false);
check('payload: 商品番号 = 商品コード', pl.itemNumber === 'rk-smoke-1');
check('payload: システム連携用SKU番号 = 商品コード',
  pl.variants['rk-smoke-1'].merchantDefinedSkuId === 'rk-smoke-1');
check('payload: タイトルはAI楽天タイトル優先', pl.title === '楽天用タイトル');
check('payload: tagline=キャッチ', pl.tagline === 'キャッチ');
// 2026-08-01 店舗フォーマット: PC商品説明文 = 表1枚 (説明/注意事項/仕様表/…)
check('payload: PC説明文は表形式 — 説明行に商品名+特徴',
  pl.productDescription.pc.startsWith('<table')
  && pl.productDescription.pc.includes('<b>説明</b>')
  && pl.productDescription.pc.includes('楽天用タイトル')
  && pl.productDescription.pc.includes('特徴文'),
  pl.productDescription.pc);
check('payload: 仕様表は1項目1行',
  pl.productDescription.pc.includes('<b>サイズ</b>') && pl.productDescription.pc.includes('<td>W10cm</td>'));
check('payload: 注意事項行 = AI注意書き + 固定文 (説明の直後)',
  pl.productDescription.pc.includes('AI注意書き文<br>※モニター画面の状況')
  && pl.productDescription.pc.indexOf('<b>注意事項</b>') < pl.productDescription.pc.indexOf('<b>サイズ</b>'));
check('payload: 販売説明文 = 商品画像の画像HTML',
  pl.salesDescription === '<img src="https://image.rakuten.co.jp/b-faith/cabinet/app-newitems/rk-smoke-1-1.jpg" width="100%" border="0"><br><br><br>',
  pl.salesDescription);
check('payload: スマホ用説明文 = 販売説明文 + PC説明文',
  pl.productDescription.sp === pl.salesDescription + '\n' + pl.productDescription.pc);

// 10240字ガード (Codex R1 Low): PC説明文の超過は理由で止める
db.prepare(`UPDATE draft_ai_outputs SET content = ? WHERE draft_id = ? AND kind = 'desc_features'`).run('あ'.repeat(11000), rkId);
let bLen = listing.buildItemPayload(db, rkId);
check('payload: PC説明文10240字超は理由で止める',
  bLen.ok === false && bLen.reasons.some((r) => r.includes('PC用商品説明文が長すぎます')), JSON.stringify(bLen.reasons));
// スマホ用だけが連結で超過するケース (PC・販売は上限内)
db.prepare(`UPDATE draft_ai_outputs SET content = ? WHERE draft_id = ? AND kind = 'desc_features'`).run('あ'.repeat(6000), rkId);
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'glong')`).run(rkId);
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'glong', ?)`).run(rkId, '/app-newitems/' + 'x'.repeat(4000) + '.jpg');
bLen = listing.buildItemPayload(db, rkId);
check('payload: スマホ用 (販売+PC連結) だけの超過も止める',
  bLen.ok === false
  && bLen.reasons.some((r) => r.includes('スマホ用商品説明文'))
  && !bLen.reasons.some((r) => r.includes('PC用商品説明文'))
  && !bLen.reasons.some((r) => r.includes('画像HTML')),
  JSON.stringify(bLen.reasons));
db.prepare(`DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id = 'glong'`).run(rkId);
db.prepare(`DELETE FROM draft_cabinet_images WHERE draft_id = ? AND drive_file_id = 'glong'`).run(rkId);

// ページ表記の自動保存リビジョン列 (#691 冪等ALTER)
const piCols = new Set(db.prepare('PRAGMA table_info(draft_page_info)').all().map((c) => c.name));
check('draft_page_info に save_token/save_seq (冪等ALTER)', piCols.has('save_token') && piCols.has('save_seq'));

// ─── サムネイルプロキシの取得対象ガード (Codex R1 high: confused-deputy 防止) ───
const { isKnownImageFileId } = await import('../db.js');
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'thumb-known-test')`).run(rkId);
const wbRowBefore = db.prepare('SELECT white_bg_drive_file_id AS v FROM draft_rakuten WHERE draft_id = ?').get(rkId);
db.prepare(`INSERT INTO draft_rakuten (draft_id, white_bg_drive_file_id) VALUES (?, 'thumb-wb-test')
  ON CONFLICT(draft_id) DO UPDATE SET white_bg_drive_file_id = 'thumb-wb-test'`).run(rkId);
check('isKnownImageFileId: draft_images 登録済み → true', isKnownImageFileId(db, 'thumb-known-test'));
check('isKnownImageFileId: 白抜き背景 (draft_rakuten) → true', isKnownImageFileId(db, 'thumb-wb-test'));
check('isKnownImageFileId: 未登録IDは false', !isKnownImageFileId(db, 'not-registered-file-id'));
// 2026-08-08: サムネURLの版数は DB の期待値と照合する (任意の v でキャッシュを汚させない)
const { imageRefOfFileId } = await import('../db.js');
db.prepare(`UPDATE draft_images SET drive_modified_time = '2026-08-08T00:00:00.000Z' WHERE drive_file_id = 'thumb-known-test'`).run();
check('imageRefOfFileId: 登録済みは期待バージョンを返す / 未登録は null',
  imageRefOfFileId(db, 'thumb-known-test')?.modifiedTime === '2026-08-08T00:00:00.000Z'
  && imageRefOfFileId(db, 'not-registered-file-id') === null);
check('isKnownImageFileId: 空は false', !isKnownImageFileId(db, ''));
// フィクスチャを元の状態へ (後続の白抜き/payload テストの前提を変えない)
db.prepare(`DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id = 'thumb-known-test'`).run(rkId);
if (wbRowBefore === undefined) db.prepare('DELETE FROM draft_rakuten WHERE draft_id = ?').run(rkId);
else db.prepare('UPDATE draft_rakuten SET white_bg_drive_file_id = ? WHERE draft_id = ?').run(wbRowBefore.v, rkId);
db.prepare(`UPDATE draft_ai_outputs SET content = '特徴文' WHERE draft_id = ? AND kind = 'desc_features'`).run(rkId);

// 販売説明文単独の境界 (Codex R2 Low): ちょうど10240字は販売ガードにかからず、+1字でかかる
const salesLine1Len = listing.buildSalesDescriptionHtml(['/app-newitems/rk-smoke-1-1.jpg']).length;
const salesEmptyLen = listing.buildSalesDescriptionHtml(['']).length;
const exactLocLen = 10240 - salesLine1Len - 1 - salesEmptyLen; // -1 は行間の '\n'
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'gedge')`).run(rkId);
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gedge', ?)`).run(rkId, '/' + 'x'.repeat(exactLocLen - 1));
bLen = listing.buildItemPayload(db, rkId);
check('payload: 販売説明文ちょうど10240字は販売ガードにかからない (連結のスマホ用のみ)',
  bLen.ok === false && !bLen.reasons.some((r) => r.includes('画像HTML')) && bLen.reasons.some((r) => r.includes('スマホ用')),
  JSON.stringify(bLen.reasons));
db.prepare(`UPDATE draft_cabinet_images SET cabinet_location = ? WHERE draft_id = ? AND drive_file_id = 'gedge'`).run('/' + 'x'.repeat(exactLocLen), rkId);
bLen = listing.buildItemPayload(db, rkId);
check('payload: 販売説明文10241字は理由で止める', bLen.ok === false && bLen.reasons.some((r) => r.includes('画像HTML')), JSON.stringify(bLen.reasons));
db.prepare(`DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id = 'gedge'`).run(rkId);
db.prepare(`DELETE FROM draft_cabinet_images WHERE draft_id = ? AND drive_file_id = 'gedge'`).run(rkId);

// env の悪意ある値は既定URLへフォールバック (Codex R2 Low: R1修正の回帰テスト)
process.env.PH_CABINET_IMAGE_BASE = '"><script>alert(1)<' + '/script>';
check('salesDesc: 引用符/タグ入りenvは既定URLへフォールバック',
  listing.buildSalesDescriptionHtml(['/a/b.jpg']) === '<img src="https://image.rakuten.co.jp/b-faith/cabinet/a/b.jpg" width="100%" border="0"><br><br><br>');
process.env.PH_CABINET_IMAGE_BASE = 'http://evil.example/x';
check('salesDesc: http のenvは拒否して既定URL',
  listing.buildSalesDescriptionHtml(['/a/b.jpg']).startsWith('<img src="https://image.rakuten.co.jp/b-faith/cabinet/'));
delete process.env.PH_CABINET_IMAGE_BASE;
// ─── 末尾バナー自動追加 (2026-08-03 中原さん指定) ───
check('trailingBannerLocations: ネコポス(5) = 配送バナー + 共通3枚',
  JSON.stringify(listing.trailingBannerLocations('5'))
  === JSON.stringify(['/07722747/09610094/imgrc0104897185.jpg', '/coupon/imgrc0122590661.jpg', '/11720388/same_day.jpg', '/11720388/refund.jpg']));
check('trailingBannerLocations: 定形外(1)', listing.trailingBannerLocations('1')[0] === '/07722747/08581403/teikeigai_soryomuryo.jpg');
check('trailingBannerLocations: ゆうパケットパフ(9)', listing.trailingBannerLocations('9')[0] === '/07722747/09610098/rakutensouko.jpg');
check('trailingBannerLocations: 対応バナーの無い配送方法/未設定は共通3枚のみ',
  listing.trailingBannerLocations('4').length === 3 && listing.trailingBannerLocations(null).length === 3
  && listing.trailingBannerLocations('4')[0] === '/coupon/imgrc0122590661.jpg');
check('cabinetImageUrl: location → 公開URL',
  listing.cabinetImageUrl('/coupon/imgrc0122590661.jpg') === 'https://image.rakuten.co.jp/b-faith/cabinet/coupon/imgrc0122590661.jpg');

check('payload: 画像は CABINET location + 共通バナー3枚が末尾 (配送方法未確定)',
  pl.images.length === 4 && pl.images[0].type === 'CABINET' && pl.images[0].location === '/app-newitems/rk-smoke-1-1.jpg'
  && pl.images[1].location === '/coupon/imgrc0122590661.jpg'
  && pl.images[3].location === '/11720388/refund.jpg',
  JSON.stringify(pl.images));
check('payload: variants は ne_code キー + 属性 + 型番なし例外',
  pl.variants['rk-smoke-1'].standardPrice === 1980
  && pl.variants['rk-smoke-1'].articleNumber.exemptionReason === 5
  && pl.variants['rk-smoke-1'].attributes[0].name === 'ブランド名',
  JSON.stringify(pl.variants));

// メーカー型番があれば value で送る
db.prepare(`UPDATE draft_rakuten SET article_number = 'ABC-100' WHERE draft_id = ?`).run(rkId);
check('payload: 型番があれば value で送る',
  listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber.value === 'ABC-100');

// ─── 2026-07-27 出品仕様: 税率 / JAN / 配送 / 納期 / 白抜き / 画像20枚 ───
check('taxRateToPayment: 8% → payment.taxRate 0.08', listing.taxRateToPayment('8%')?.taxRate === 0.08);
check('taxRateToPayment: 10%/未設定も 0.1 を明示して送る (2026-08-05 平串実測: 送らないと税率未設定になる)',
  listing.taxRateToPayment('10%')?.taxRate === 0.1 && listing.taxRateToPayment(null)?.taxRate === 0.1);

check('isValidGtin: 正しいJAN-13を通す', listing.isValidGtin('4901234567894') === true);
check('isValidGtin: チェックデジット不一致/桁数違い/非数字を弾く',
  listing.isValidGtin('4901234567890') === false && listing.isValidGtin('49012') === false && listing.isValidGtin('4901234abc894') === false);

db.prepare(`UPDATE product_drafts SET jan_code = '4901234567894' WHERE id = ?`).run(rkId);
dbmod.upsertDraftYahoo(db, rkId, { tax_rate: '8%' });
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '5', postage_included = 1, normal_delivery_date_id = '1000' WHERE draft_id = ?`).run(rkId);
built = listing.buildItemPayload(db, rkId);
const rkVar = built.payload?.variants?.['rk-smoke-1'] || {};
// 2026-07-28 本番検証: 属性辞書はジャンルごとで「カタログID」が無いジャンル (111145実測) では
// IE1002 で登録自体が失敗する → **自動付与しない**。手入力した場合だけ送る (JAN欄との一致検証あり)
check('payload: JAN欄だけではカタログID属性を自動付与しない (IE1002対策)',
  !(rkVar.attributes || []).some((a) => a.name === 'カタログID'),
  JSON.stringify(rkVar.attributes));
check('payload: 8% は payment.taxRate で送る', built.payload?.payment?.taxRate === 0.08);

// Drive 上書き後の「転送済み」誤判定 (Codex R2 high)。ID が同じでも更新日時が違えば未転送扱い
check('freshCabinetMap/cabinetKeyOf: ID+更新日時で突合する',
  listing.freshCabinetMap([{ drive_file_id: 'f1', cabinet_location: '/a/1.jpg', drive_modified_time: 'T1' }])
    .get(listing.cabinetKeyOf({ drive_file_id: 'f1', drive_modified_time: 'T1' })) === '/a/1.jpg'
  && listing.freshCabinetMap([{ drive_file_id: 'f1', cabinet_location: '/a/1.jpg', drive_modified_time: 'T1' }])
    .has(listing.cabinetKeyOf({ drive_file_id: 'f1', drive_modified_time: 'T2' })) === false);
{
  // 転送履歴だけ古い更新日時にする → 未転送として登録が止まること。
  // fixture を壊さないよう、対象行はスナップショットして最後に元へ戻す
  const imgRow = db.prepare('SELECT drive_file_id, drive_modified_time FROM draft_images WHERE draft_id = ? ORDER BY sort, id LIMIT 1').get(rkId);
  const cabBefore = db.prepare('SELECT * FROM draft_cabinet_images WHERE draft_id = ? AND drive_file_id = ?')
    .get(rkId, imgRow.drive_file_id) || null;
  db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location, drive_modified_time)
    VALUES (?, ?, '/appnewitems/stale.jpg', 'OLD')
    ON CONFLICT(draft_id, drive_file_id) DO UPDATE SET cabinet_location = '/appnewitems/stale.jpg', drive_modified_time = 'OLD'`)
    .run(rkId, imgRow.drive_file_id);
  db.prepare(`UPDATE draft_images SET drive_modified_time = 'NEW' WHERE draft_id = ? AND drive_file_id = ?`)
    .run(rkId, imgRow.drive_file_id);
  const stale = listing.buildItemPayload(db, rkId);
  check('payload: Driveで上書きされた画像は「未転送」に落ちて登録が止まる',
    (stale.reasons || []).some((r) => r.includes('未転送')), JSON.stringify(stale.reasons));
  db.prepare(`UPDATE draft_cabinet_images SET drive_modified_time = 'NEW' WHERE draft_id = ? AND drive_file_id = ?`)
    .run(rkId, imgRow.drive_file_id);
  const okAgain = listing.buildItemPayload(db, rkId);
  check('payload: 再転送 (更新日時が一致) すれば未転送の理由は消える',
    !(okAgain.reasons || []).some((r) => r.includes('未転送')), JSON.stringify(okAgain.reasons));
  // 復元
  db.prepare('DELETE FROM draft_cabinet_images WHERE draft_id = ? AND drive_file_id = ?').run(rkId, imgRow.drive_file_id);
  if (cabBefore) {
    db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location, cabinet_file_id, drive_modified_time)
      VALUES (?, ?, ?, ?, ?)`).run(rkId, cabBefore.drive_file_id, cabBefore.cabinet_location, cabBefore.cabinet_file_id ?? null, cabBefore.drive_modified_time ?? null);
  }
  db.prepare(`UPDATE draft_images SET drive_modified_time = ? WHERE draft_id = ? AND drive_file_id = ?`)
    .run(imgRow.drive_modified_time ?? null, rkId, imgRow.drive_file_id);
}

// TOP画像 (商品画像1 = <商品コード>_top) のゲート (2026-08-08 スタッフ指摘)。
// 枠1 (sort 0) が空のまま登録すると別画像が繰り上がって TOP になるので止める
{
  const sortsBefore = db.prepare('SELECT id, sort FROM draft_images WHERE draft_id = ?').all(rkId);
  db.prepare('UPDATE draft_images SET sort = sort + 1 WHERE draft_id = ?').run(rkId);
  const noTop = listing.buildItemPayload(db, rkId);
  check('payload: 枠1 (_top) が空なら登録を止める',
    (noTop.reasons || []).some((r) => r.includes('TOP画像がありません')), JSON.stringify(noTop.reasons));
  const restore = db.prepare('UPDATE draft_images SET sort = ? WHERE id = ?');
  for (const r of sortsBefore) restore.run(r.sort, r.id);
  const withTop = listing.buildItemPayload(db, rkId);
  check('payload: 枠1が埋まっていれば TOP画像の理由は出ない',
    !(withTop.reasons || []).some((r) => r.includes('TOP画像がありません')), JSON.stringify(withTop.reasons));
}
check('payload: 配送方法グループ + 送料込み', rkVar.shipping?.shippingMethodGroup === '5' && rkVar.shipping?.postageIncluded === true);
check('payload: 納期情報ID は数値で送る', rkVar.normalDeliveryDateId === 1000);
check('payload: ネコポス指定でバナー→共通3枚を商品画像の後ろに自動追加',
  JSON.stringify(built.payload.images.slice(-4).map((i) => i.location))
  === JSON.stringify(['/07722747/09610094/imgrc0104897185.jpg', '/coupon/imgrc0122590661.jpg', '/11720388/same_day.jpg', '/11720388/refund.jpg'])
  && built.payload.images[0].location === '/app-newitems/rk-smoke-1-1.jpg',
  JSON.stringify(built.payload.images));
check('payload: 販売説明文 (画像HTML) にはバナーを入れない (商品画像のみ)',
  !built.payload.salesDescription.includes('coupon') && !built.payload.salesDescription.includes('07722747'));

// NE配送方法フォールバック (Codex R1 low: 統合レベルで検証)。
// アプリ指定なし + NE配送方法「定形外」→ 名前一致で楽天グループ1 → 定形外バナー
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = NULL WHERE draft_id = ?`).run(rkId);
db.prepare(`INSERT OR REPLACE INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 原価, 送料, 配送方法, 消費税率, updated_at)
  VALUES (99401, 'rk-smoke-1', '出品smoke', '1', '取扱中', 'ok', 660, 120, '定形外', 0.1, '2026-08-03T00:00:00Z')`).run();
let bNe = listing.buildItemPayload(db, rkId);
check('payload: アプリ指定なしは NE配送方法から定形外バナーを選ぶ',
  bNe.ok === true && bNe.payload.images.some((i) => i.location === '/07722747/08581403/teikeigai_soryomuryo.jpg')
  && !bNe.payload.images.some((i) => i.location === '/07722747/09610094/imgrc0104897185.jpg'),
  JSON.stringify(bNe.payload?.images || bNe.reasons));
check('effectiveShippingForDraft: アプリ指定(9)が NE(定形外=1) より優先 / 指定なしはNE',
  listing.effectiveShippingForDraft(db, 'rk-smoke-1', '9').group === '9'
  && listing.effectiveShippingForDraft(db, 'rk-smoke-1', null).group === '1');
// 不正な明示指定は NE に隠さない (プレビュー=配送バナーなし / payload=「配送方法の指定が不正です」
// で停止 — payload 経路は後段の「配送方法/納期の不正値を弾く」チェックが検証している)
check('effectiveShippingForDraft: 不正指定(99)はフォールバックせず未解決を返す',
  listing.effectiveShippingForDraft(db, 'rk-smoke-1', '99').group === null
  && listing.effectiveShippingForDraft(db, 'rk-smoke-1', '99').invalid === true);
// 複合選択肢 (2026-08-06): 楽天側は定形外(1) として振る舞い、Yahoo!配送だけ差し替わる
check('effectiveShippingForDraft: 複合(1y8/1y5)は楽天=定形外 + yahooDelivery付き',
  listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y8').group === '1'
  && listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y8').label === '定形外'
  && listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y8').yahooDelivery === '宅急便50サイズ以上'
  && listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y5').yahooDelivery === 'ネコポス');
check('複合選択肢の末尾バナーは定形外と同一',
  JSON.stringify(listing.trailingBannerLocations(listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y8').group))
  === JSON.stringify(listing.trailingBannerLocations('1')));
// SKU画像 (2026-08-07): ファイル名→SKUコード照合キー
check('skuImageKeyOfFileName: 拡張子除去+小文字化+trim',
  listing.skuImageKeyOfFileName('Sueders-DB.JPG') === 'sueders-db'
  && listing.skuImageKeyOfFileName(' sueders-db.png ') === 'sueders-db'
  && listing.skuImageKeyOfFileName('sueders-db') === 'sueders-db'
  && listing.skuImageKeyOfFileName('sueders-db.backup.jpg') === 'sueders-db.backup');
// Cabinet ファイルパス: 情報が落ちる置換はハッシュ付与で一意化 (Codex High-3)
check('skuCabinetFilePath: 素直なコードはそのまま / 特殊文字はハッシュ付与で衝突しない',
  listing.skuCabinetFilePath('sueders-db') === 'sueders-db-sku.jpg'
  && listing.skuCabinetFilePath('a_b') !== listing.skuCabinetFilePath('a.b')
  && /^[a-z0-9][a-z0-9\-]{0,30}\.jpg$/.test(listing.skuCabinetFilePath('a_b'))
  && /^[a-z0-9][a-z0-9\-]{0,30}\.jpg$/.test(listing.skuCabinetFilePath('とても長い日本語のSKUコード仮に置いたもの'))
  && /^[a-z0-9][a-z0-9\-]{0,30}\.jpg$/.test(listing.skuCabinetFilePath('x'.repeat(96))));
check('skuCabinetFilePath: 切り詰めが必要な長い同接頭辞SKUも衝突しない (R2 High)',
  listing.skuCabinetFilePath('a'.repeat(40)) !== listing.skuCabinetFilePath('a'.repeat(40) + 'b'));
db.prepare(`DELETE FROM mirror_products WHERE product_id = 99401`).run();
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '5' WHERE draft_id = ?`).run(rkId);

// 20枚上限は自動追加バナーを含めて判定する
const capIns = db.prepare('INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, ?)');
const capCab = db.prepare('INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, ?, ?)');
for (let i = 1; i <= 16; i++) { capIns.run(rkId, `gcap${i}`); capCab.run(rkId, `gcap${i}`, `/app-newitems/rk-smoke-1-cap${i}.jpg`); }
let bCap = listing.buildItemPayload(db, rkId); // 商品17枚 + バナー4枚 = 21
check('payload: 商品画像+自動追加バナーで20枚超は理由で止める',
  bCap.ok === false && bCap.reasons.some((r) => r.includes('自動追加バナー')), JSON.stringify(bCap.reasons));
db.prepare(`DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id LIKE 'gcap%'`).run(rkId);
db.prepare(`DELETE FROM draft_cabinet_images WHERE draft_id = ? AND drive_file_id LIKE 'gcap%'`).run(rkId);

dbmod.upsertDraftYahoo(db, rkId, { tax_rate: '10%' });
check('payload: 10% も payment.taxRate 0.1 を明示して送る (2026-08-05〜)',
  listing.buildItemPayload(db, rkId).payload?.payment?.taxRate === 0.1);

// 白抜き背景: 未転送なら理由を返し、転送済みなら whiteBgImage 別枠 (images には入れない)
db.prepare(`UPDATE draft_rakuten SET white_bg_drive_file_id = 'gwhite', white_bg_drive_url = 'https://drive.google.com/file/d/gwhite/view' WHERE draft_id = ?`).run(rkId);
let b27 = listing.buildItemPayload(db, rkId);
check('payload: 白抜き未転送は理由を返す', b27.ok === false && b27.reasons.some((r) => r.includes('白抜き')), JSON.stringify(b27.reasons));
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gwhite', '/app-newitems/rk-smoke-1-white.jpg')`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: whiteBgImage は images と別枠',
  b27.ok === true
  && b27.payload.whiteBgImage?.location === '/app-newitems/rk-smoke-1-white.jpg'
  && b27.payload.images.every((i) => i.location !== '/app-newitems/rk-smoke-1-white.jpg'),
  JSON.stringify(b27.reasons || b27.payload?.images));
check('payload: 販売説明文にも白抜き画像は入れない', !b27.payload.salesDescription.includes('white'));

// 不正値は理由で弾く
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '99', normal_delivery_date_id = 'abc' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 配送方法/納期の不正値を弾く',
  b27.ok === false && b27.reasons.some((r) => r.includes('配送方法')) && b27.reasons.some((r) => r.includes('納期')),
  JSON.stringify(b27.reasons));
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = NULL, normal_delivery_date_id = NULL WHERE draft_id = ?`).run(rkId);

// 税率の不正値は fail-closed (Codex R1 Medium-1)
dbmod.upsertDraftYahoo(db, rkId, { tax_rate: '9.6%' });
b27 = listing.buildItemPayload(db, rkId);
check('payload: 税率の不正値を弾く (8/10/空欄のみ)', b27.ok === false && b27.reasons.some((r) => r.includes('税率')), JSON.stringify(b27.reasons));
dbmod.upsertDraftYahoo(db, rkId, { tax_rate: '10%' });

// 手入力のカタログID属性と JAN欄の不一致は止める (Codex R1 Medium-4)
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"カタログID","values":["4999999999999"]}]' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: カタログID属性とJAN欄の不一致を弾く', b27.ok === false && b27.reasons.some((r) => r.includes('一致しません')), JSON.stringify(b27.reasons));

// R2: 複数のカタログID属性で不一致検査を迂回できない
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"カタログID","values":["4901234567894"]},{"name":"カタログID","values":["4999999999999"]}]' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: カタログID属性の複数記述を弾く', b27.ok === false && b27.reasons.some((r) => r.includes('複数')), JSON.stringify(b27.reasons));

// R2: JAN欄が空でも手入力カタログID自体を GTIN 検証する
db.prepare(`UPDATE product_drafts SET jan_code = NULL WHERE id = ?`).run(rkId);
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"カタログID","values":["4901234567890"]}]' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 手入力カタログIDの不正値を弾く (JAN欄が空でも)',
  b27.ok === false && b27.reasons.some((r) => r.includes('カタログID') && r.includes('不正')), JSON.stringify(b27.reasons));
db.prepare(`UPDATE product_drafts SET jan_code = '4901234567894' WHERE id = ?`).run(rkId);
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["ノーブランド品"]}]' WHERE draft_id = ?`).run(rkId);

// 転送後に削除した画像は送らない (Codex R1 Medium-2: draft_images との JOIN)
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gstale', '/app-newitems/rk-smoke-1-stale.jpg')`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 削除済み画像 (転送履歴のみ) は送らない',
  b27.ok === true && b27.payload.images.every((i) => !i.location.includes('stale')), JSON.stringify(b27.payload?.images));

// ─── 商品ページ表記の統合 (Codex R1 high: import だけで未統合だった回帰) ───
db.prepare(`INSERT INTO draft_page_info (draft_id, product_type, content_volume) VALUES (?, 'cosmetics', '50ml')`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 化粧品の必須記載不足は登録をブロック',
  b27.ok === false && b27.reasons.some((r) => r.includes('商品ページ表記')), JSON.stringify(b27.reasons));
db.prepare(`UPDATE draft_page_info SET seller_name = 'メーカーA', origin_type = '日本製', category_label = '化粧品' WHERE draft_id = ?`).run(rkId);
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '5' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 充足すると説明文末尾に表を連結 (発送方法=アプリ指定グループ名)',
  b27.ok === true
  && b27.payload.productDescription.pc.includes('<table')
  && b27.payload.productDescription.pc.includes('メーカーA')
  && b27.payload.productDescription.pc.includes('ネコポス'),
  JSON.stringify(b27.reasons || null));
check('payload: 表は説明文の末尾に付く', b27.payload.productDescription.pc.trim().endsWith('</table>'));
// 仕様表とページ表記の同名ラベルはページ表記が正 (Codex R1 Medium: 重複行を作らない)
db.prepare(`UPDATE draft_page_info SET size_text = '約W5cm' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 仕様表とページ表記の同名ラベルはページ表記が正 (サイズ行は1つ)',
  b27.ok === true
  && (b27.payload.productDescription.pc.match(/<b>サイズ<\/b>/g) || []).length === 1
  && b27.payload.productDescription.pc.includes('約W5cm')
  && !b27.payload.productDescription.pc.includes('W10cm'),
  JSON.stringify(b27.reasons || null));
db.prepare(`UPDATE draft_page_info SET size_text = NULL WHERE draft_id = ?`).run(rkId);
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = NULL WHERE draft_id = ?`).run(rkId);
db.prepare(`DELETE FROM draft_page_info WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: page_info 未保存でも説明は表形式 (表記の行だけ載らない)',
  b27.ok === true
  && b27.payload.productDescription.pc.includes('<b>説明</b>')
  && !b27.payload.productDescription.pc.includes('メーカーA'),
  JSON.stringify(b27.reasons || null));

// 未転送の画像があれば止める
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'gnotyet')`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 未転送の商品画像があれば止める', b27.ok === false && b27.reasons.some((r) => r.includes('未転送')), JSON.stringify(b27.reasons));
db.prepare(`DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id = 'gnotyet'`).run(rkId);

// 画像は 20 枚まで (白抜きはカウント外)
const insImg21 = db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, ?)`);
const insCab = db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, ?, ?)`);
for (let i = 2; i <= 21; i++) { insImg21.run(rkId, `gfile${i}`); insCab.run(rkId, `gfile${i}`, `/app-newitems/rk-smoke-1-${i}.jpg`); }
b27 = listing.buildItemPayload(db, rkId);
check('payload: 画像は20枚まで', b27.ok === false && b27.reasons.some((r) => r.includes('20')), JSON.stringify(b27.reasons));

// 公開切替は「アプリから登録済み」のドラフト限定 (registered_at 無しは RMS に接続せず拒否)
const visGuard = await listing.setItemVisibility(rkId, { hide: false });
check('visibility: 未登録ドラフトは拒否 (fail-closed)', visGuard.ok === false && String(visGuard.error).includes('登録'), JSON.stringify(visGuard));

// バリエーションページ (複数SKU) は未対応として弾く
db.prepare(`DELETE FROM product_drafts WHERE ne_code = 'rk-smoke-1'`).run();
insProd.run(9301, 'rkv-a', 'バリA', 'rkv');
insProd.run(9302, 'rkv-b', 'バリB', 'rkv');
const rkvId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, price) VALUES ('rkv', 'バリエページ', 1000)`).run().lastInsertRowid);
db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id) VALUES (?, '1')`).run(rkvId);
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'g2')`).run(rkvId);
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'g2', '/x/y.jpg')`).run(rkvId);
const bv = listing.buildItemPayload(db, rkvId);
check('payload: バリエーションページは未対応と明示',
  bv.ok === false && bv.reasons.some((r) => r.includes('バリエーション')), JSON.stringify(bv.reasons));
db.prepare(`DELETE FROM product_drafts WHERE ne_code = 'rkv'`).run();
db.prepare(`DELETE FROM mirror_products WHERE product_id IN (9301, 9302)`).run();

// toCabinetJpeg: 実画像で 2MB 以下 JPEG になる
const bigPng = await (await import('sharp')).default({
  create: { width: 3000, height: 3000, channels: 3, background: { r: 200, g: 100, b: 50 } },
}).png().toBuffer();
const jpg = await listing.toCabinetJpeg(bigPng);
check('toCabinetJpeg: 2MB以下のJPEGに変換', jpg.length <= 2 * 1024 * 1024 && jpg[0] === 0xFF && jpg[1] === 0xD8, `${jpg.length} bytes`);

// extractRmsErrors: RMS 400 の形
check('extractRmsErrors: errors[] を整形',
  listing.extractRmsErrors({ errors: [{ code: 'IE0418', message: 'Invalid attribute' }] }).includes('IE0418'));

// ─── 店舗内カテゴリ (マスタ貼り付け取り込み + ドラフト選択) ───
const shopCat = await import('../lib/shop-categories.js');

const catParsed = shopCat.parseShopCategoryText(
  '犬用品 > おやつ\n123456,猫用品 ＞ ケア用品\n\n犬用品>おやつ\n789\tその他');
check('cat parse: 4行から重複1を除いて3件', catParsed.ok && catParsed.rows.length === 3 && catParsed.duplicates === 1,
  JSON.stringify(catParsed));
check('cat parse: パスのみの行は ID なし', catParsed.rows[0].categoryId === null && catParsed.rows[0].path === '犬用品 > おやつ');
check('cat parse: 「ID,パス」+ 全角＞の正規化', catParsed.rows[1].categoryId === '123456' && catParsed.rows[1].path === '猫用品 > ケア用品');
check('cat parse: タブ区切りも受ける', catParsed.rows[2].categoryId === '789' && catParsed.rows[2].path === 'その他');
check('cat parse: 行数超過は弾く',
  shopCat.parseShopCategoryText(Array.from({ length: shopCat.MAX_SHOP_CATEGORY_LINES + 1 }, () => 'x').join('\n')).ok === false);
check('cat parse: 長すぎるカテゴリ名は弾く', shopCat.parseShopCategoryText('あ'.repeat(301)).ok === false);
check('cat parse: 空入力 → 0件', shopCat.parseShopCategoryText('  \n \n').rows.length === 0);

const catR1 = shopCat.replaceShopCategories(db, catParsed.rows);
check('cat import: 3件が有効', catR1.active === 3 && shopCat.countActiveShopCategories(db) === 3);

// 再取り込み (全置き換え): 消えたカテゴリは is_active=0 に落ち、既知の ID は保持される
const catParsed2 = shopCat.parseShopCategoryText('猫用品 > ケア用品\n新カテゴリ');
const catR2 = shopCat.replaceShopCategories(db, catParsed2.rows);
check('cat import: 全置き換えで有効2 / 非活性2', catR2.active === 2 && catR2.deactivated === 2, JSON.stringify(catR2));
check('cat import: ID無しで再取り込みしても既知のカテゴリIDを保持',
  db.prepare(`SELECT category_id FROM ph_shop_categories WHERE path = '猫用品 > ケア用品'`).get().category_id === '123456');

// ドラフトへの割り当て
const catDraftId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name) VALUES ('cat-smoke-1', '棚スモーク')`).run().lastInsertRowid);
const activeCat = db.prepare(`SELECT id FROM ph_shop_categories WHERE path = '猫用品 > ケア用品'`).get().id;
const inactiveCat = db.prepare(`SELECT id FROM ph_shop_categories WHERE path = '犬用品 > おやつ'`).get().id;
shopCat.setDraftShopCategories(db, catDraftId, [activeCat, inactiveCat]);
const catList = shopCat.listShopCategoriesForDraft(db, catDraftId);
check('cat list: 有効カテゴリ + 選択中の非活性カテゴリが出る (未選択の非活性は出ない)',
  catList.length === 3
  && catList.filter((c) => c.selected).length === 2
  && catList.some((c) => c.id === inactiveCat && c.selected === 1 && c.is_active === 0),
  JSON.stringify(catList));
check('cat paths: 選択中のパスを返す',
  JSON.stringify(shopCat.selectedShopCategoryPaths(db, catDraftId).sort())
  === JSON.stringify(['犬用品 > おやつ', '猫用品 > ケア用品']));
shopCat.setDraftShopCategories(db, catDraftId, [activeCat]);
check('cat set: 入れ替えで1件になる', shopCat.selectedShopCategoryPaths(db, catDraftId).length === 1);
db.prepare('DELETE FROM product_drafts WHERE id = ?').run(catDraftId);
check('cat cascade: ドラフト削除で選択も消える',
  db.prepare('SELECT COUNT(*) AS c FROM draft_shop_categories WHERE draft_id = ?').get(catDraftId).c === 0);

// id の厳密検証 (Codex R1 Medium-3)
check('cat ids: 整数と数字文字列だけ通す',
  JSON.stringify(shopCat.sanitizeShopCategoryIds([1, '2', 1]).ids) === '[1,2]'
  && shopCat.sanitizeShopCategoryIds(['12abc']).error === 'invalid_id'
  && shopCat.sanitizeShopCategoryIds([1.5]).error === 'invalid_id'
  && shopCat.sanitizeShopCategoryIds('x').error === 'not_array'
  && shopCat.sanitizeShopCategoryIds(Array.from({ length: shopCat.MAX_DRAFT_SHOP_CATEGORIES + 1 }, (_, i) => i + 1)).error === 'too_many');

// 全置き換えの激減ガード (Codex R1 High-4)
const catMany = shopCat.parseShopCategoryText(Array.from({ length: 12 }, (_, i) => `棚${i}`).join('\n'));
shopCat.replaceShopCategories(db, catMany.rows);
const catFew = shopCat.parseShopCategoryText('棚0\n棚1');
check('cat import: 半分未満への激減は force なしで拒否',
  shopCat.replaceShopCategories(db, catFew.rows).error === 'too_few');
check('cat import: force 指定なら激減置き換えを実行',
  shopCat.replaceShopCategories(db, catFew.rows, { force: true }).active === 2);

db.prepare('DELETE FROM ph_shop_categories').run();

// ─── Genre API: 辞書の正規化 / キャッシュ / 事前検証 / カタログID自動付与 ───
// RMS 実応答の形 (2026-07-28 プローブ実測) を fixture にする
const RMS_GENRE_FIXTURE = {
  version: { id: 132, fixedAt: '2026-07-28T07:52:18+09:00' },
  genre: {
    genreId: 900001, nameJa: 'テストジャンル', nameJaPath: ['大分類', '中分類', 'テストジャンル'],
    level: 3, lowest: true, properties: { itemRegisterFlg: true },
    attributes: [
      { id: 3, nameJa: 'ブランド名', dataType: 'STRING', maxLength: 100, unit: null,
        properties: { rmsMandatoryFlg: true, rmsMandatoryType: 'MANDATORY', rmsMultiValueLimit: 3, rmsInputMethod: 'DESCRIPTIVE' } },
      { id: 8, nameJa: '代表カラー', dataType: 'STRING', maxLength: 50, unit: null,
        properties: { rmsMandatoryFlg: true, rmsMandatoryType: 'MANDATORY', rmsMultiValueLimit: 1, rmsInputMethod: 'SELECTIVE' } },
      { id: 99, nameJa: 'カタログID', dataType: 'STRING', maxLength: 14, unit: null,
        properties: { rmsMandatoryFlg: true, rmsMandatoryType: 'MANDATORY', rmsMultiValueLimit: 1, rmsInputMethod: 'DESCRIPTIVE' } },
      { id: 50, nameJa: '総枚数', dataType: 'NUMBER', maxLength: null, unit: '枚',
        properties: { rmsMandatoryFlg: false, rmsMandatoryType: 'OPTIONAL_NAVIGATION', rmsMultiValueLimit: 1, rmsInputMethod: 'DESCRIPTIVE' } },
    ],
  },
};

const gnorm = listing.normalizeGenreAttributes(RMS_GENRE_FIXTURE);
check('genre: 正規化 (名前/必須/選択式/上限)',
  gnorm.genreId === '900001' && gnorm.genrePath === '大分類 > 中分類 > テストジャンル'
  && gnorm.attributes.length === 4
  && gnorm.attributes[0].mandatory === true && gnorm.attributes[0].multiValueLimit === 3
  && gnorm.attributes[1].inputMethod === 'SELECTIVE'
  && gnorm.attributes[3].mandatory === false && gnorm.attributes[3].unit === '枚',
  JSON.stringify(gnorm).slice(0, 300));
check('genre: 壊れた応答は null', listing.normalizeGenreAttributes({}) === null && listing.normalizeGenreAttributes(null) === null);

// キャッシュ保存 → 取得
db.prepare(`INSERT OR REPLACE INTO ph_genre_attributes (genre_id, genre_name, genre_path, payload_json, fixed_at)
  VALUES ('900001', ?, ?, ?, ?)`).run(gnorm.genreName, gnorm.genrePath, JSON.stringify(gnorm.attributes), gnorm.fixedAt);
const gc = listing.getCachedGenreAttributes(db, '900001');
check('genre: キャッシュ取得', gc && gc.genreName === 'テストジャンル' && gc.attributes.length === 4);
check('genre: 未キャッシュは null', listing.getCachedGenreAttributes(db, '999999') === null);

// ─── 辞書ありでの buildItemPayload 事前検証 ───
const gdId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, price, jan_code) VALUES ('gd-smoke-1', '辞書検証商品', 2980, '4999999999999')`).run().lastInsertRowid);
db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json)
  VALUES (?, '900001', '[{"name":"ブランド名","values":["テストブランド"]},{"name":"代表カラー","values":["ブラック"]}]')`).run(gdId);
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gd1', '/x/gd1.jpg')`).run(gdId);
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'gd1')`).run(gdId);
// 画像トラック: 画像ありの初回推定で TOP は done、詳細は「対象外」にしてゲートを通す
// (ここの主題はジャンル辞書の検証。画像ゲート自体は専用ブロックで検証している)
db.prepare(`UPDATE product_drafts SET detail_images_excluded = 1 WHERE id = ?`).run(gdId);

let gb = listing.buildItemPayload(db, gdId);
check('genre: 必須が揃っていれば通り、カタログIDはJAN欄から自動付与 (辞書にあるジャンル)',
  gb.ok === true
  && gb.payload.variants['gd-smoke-1'].attributes.some((a) => a.name === 'カタログID' && a.values[0] === '4999999999999'),
  JSON.stringify(gb.ok ? gb.payload.variants['gd-smoke-1'].attributes : gb.reasons));

// 辞書に無い属性名は登録前に止める (IE1002 の事前検知)
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["x"]},{"name":"代表カラー","values":["黒"]},{"name":"存在しない属性","values":["y"]}]' WHERE draft_id = ?`).run(gdId);
gb = listing.buildItemPayload(db, gdId);
check('genre: 辞書に無い属性名を事前に止める (IE1002対策)',
  gb.ok === false && gb.reasons.some((r) => r.includes('存在しない属性') && r.includes('IE1002')), JSON.stringify(gb.reasons));

// 必須属性の欠落を事前に止める
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["x"]}]' WHERE draft_id = ?`).run(gdId);
gb = listing.buildItemPayload(db, gdId);
check('genre: 必須属性の欠落を事前に止める',
  gb.ok === false && gb.reasons.some((r) => r.includes('必須属性「代表カラー」')), JSON.stringify(gb.reasons));

// multiValueLimit / maxLength
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["a","b","c","d"]},{"name":"代表カラー","values":["黒"]}]' WHERE draft_id = ?`).run(gdId);
gb = listing.buildItemPayload(db, gdId);
check('genre: 値の個数上限を事前に止める', gb.ok === false && gb.reasons.some((r) => r.includes('最大 3 個')), JSON.stringify(gb.reasons));

// 辞書が無いジャンルでは検証もカタログID付与もしない (従来どおり RMS に任せる)
db.prepare(`UPDATE draft_rakuten SET genre_id = '999999', attributes_json = '[{"name":"何でも属性","values":["z"]}]' WHERE draft_id = ?`).run(gdId);
gb = listing.buildItemPayload(db, gdId);
check('genre: 辞書未取得ジャンルは検証スキップ + カタログID付与なし',
  gb.ok === true
  && !gb.payload.variants['gd-smoke-1'].attributes.some((a) => a.name === 'カタログID'),
  JSON.stringify(gb.ok ? gb.payload.variants['gd-smoke-1'].attributes : gb.reasons));

// JAN欄が空 + 辞書のカタログID必須 → 必須欠落として止まる
db.prepare(`UPDATE draft_rakuten SET genre_id = '900001', attributes_json = '[{"name":"ブランド名","values":["x"]},{"name":"代表カラー","values":["黒"]}]' WHERE draft_id = ?`).run(gdId);
db.prepare(`UPDATE product_drafts SET jan_code = NULL WHERE id = ?`).run(gdId);
gb = listing.buildItemPayload(db, gdId);
check('genre: JAN欄が空だと辞書必須のカタログIDは欠落エラーになる',
  gb.ok === false && gb.reasons.some((r) => r.includes('カタログID')), JSON.stringify(gb.reasons));

// 鮮度切れ辞書は検証に使わない (Codex R1 High-1: 古い辞書で正しい属性を弾かない)
db.prepare(`UPDATE product_drafts SET jan_code = '4999999999999' WHERE id = ?`).run(gdId);
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"存在しない属性","values":["z"]}]' WHERE draft_id = ?`).run(gdId);
db.prepare(`UPDATE ph_genre_attributes SET fetched_at = '2026-01-01T00:00:00.000Z' WHERE genre_id = '900001'`).run();
gb = listing.buildItemPayload(db, gdId);
check('genre: 鮮度切れ辞書は検証スキップ + カタログID付与なし (RMSに任せる)',
  gb.ok === true
  && !gb.payload.variants['gd-smoke-1'].attributes.some((a) => a.name === 'カタログID'),
  JSON.stringify(gb.ok ? gb.payload.variants['gd-smoke-1'].attributes : gb.reasons));
check('genre: getCachedGenreAttributes は maxAgeMs で鮮度切れを null',
  listing.getCachedGenreAttributes(db, '900001') !== null
  && listing.getCachedGenreAttributes(db, '900001', { maxAgeMs: 24 * 60 * 60 * 1000 }) === null);

// 404 (ジャンル廃止) で旧キャッシュ行が消える (Codex R1 High-2)
const del404 = await listing.fetchGenreAttributes(db, '900001', { force: true, fetcher: async () => ({ status: 404, data: null }) });
check('genre: 404 で notFound + 旧キャッシュ行を削除',
  del404.ok === false && del404.notFound === true
  && listing.getCachedGenreAttributes(db, '900001') === null);

// fetcher 注入で 200 → 保存されキャッシュから返る
const fetch200 = await listing.fetchGenreAttributes(db, '900001', { force: true, fetcher: async () => ({ status: 200, data: RMS_GENRE_FIXTURE }) });
check('genre: fetch 200 → 正規化して保存', fetch200.ok === true && fetch200.genre.genreName === 'テストジャンル');
const fetchCached = await listing.fetchGenreAttributes(db, '900001', { fetcher: async () => { throw new Error('should not fetch'); } });
check('genre: 24h以内はキャッシュから返す (通信しない)', fetchCached.ok === true && fetchCached.cached === true);

db.prepare(`DELETE FROM product_drafts WHERE id = ?`).run(gdId);
db.prepare(`DELETE FROM ph_genre_attributes WHERE genre_id = '900001'`).run();

// ─── ジャンルツリー (IchibaGenre/Search) の正規化 / パス組み立て ───
const ichiba = await import('../lib/ichiba-genre.js');
const ICHIBA_FIXTURE = {
  current: { genreId: 111145, genreName: '付箋紙', genreLevel: 5 },
  parents: [
    { parent: { genreId: 215783, genreName: '日用品雑貨・文房具・手芸', genreLevel: 1 } },
    { parent: { genreId: 100901, genreName: '文房具・事務用品', genreLevel: 2 } },
  ],
  children: [],
};
const ig = ichiba.normalizeGenreSearch(ICHIBA_FIXTURE);
check('ichiba: 正規化 (current/parents/children)',
  ig.current.genreId === '111145' && ig.parents.length === 2 && ig.children.length === 0,
  JSON.stringify(ig));
check('ichiba: フルパス組み立て',
  ichiba.genrePathOf(ig) === '日用品雑貨・文房具・手芸 > 文房具・事務用品 > 付箋紙');
const igRoot = ichiba.normalizeGenreSearch({
  current: { genreId: 0, genreName: 'ルート', genreLevel: 0 },
  children: [{ child: { genreId: 100371, genreName: 'レディースファッション', genreLevel: 1 } }],
});
check('ichiba: root は path に含めない・子は child ラップを剥がす',
  ichiba.genrePathOf(igRoot) === '' && igRoot.children[0].genreId === '100371'
  && igRoot.children[0].name === 'レディースファッション');
// env 未設定時の fail-closed (ツリー=設定案内 / 提案=資格情報エラー)
delete process.env.RAKUTEN_WS_APP_ID;
delete process.env.RAKUTEN_APP_ID;
delete process.env.RAKUTEN_ACCESS_KEY;
const treeNoEnv = await ichiba.fetchGenreChildren('0');
check('ichiba: 資格情報が無ければツリーはセットアップ案内 (API を叩かない)',
  treeNoEnv.ok === false && treeNoEnv.needsSetup === true && /RAKUTEN_APP_ID/.test(treeNoEnv.error));

// 新システム ichibagt 20260701 の形 (genre/ancestors/children + id/jaName/level) を許容
const NEW_GENRE_FIXTURE = {
  genre: { id: 100901, jaName: '文房具・事務用品', level: 2 },
  ancestors: [{ id: 215783, jaName: '日用品雑貨・文房具・手芸', level: 1 }],
  children: [
    { id: 111142, jaName: '手帳・ノート・紙製品', level: 3 },
    { id: 216057, jaName: '筆記具', level: 3 },
  ],
};
const gNew = ichiba.normalizeGenreSearch(NEW_GENRE_FIXTURE);
check('ichiba: 新形式 (genre/ancestors/id/jaName) を正規化',
  gNew.current.genreId === '100901' && gNew.current.name === '文房具・事務用品'
  && gNew.parents.length === 1 && gNew.children.length === 2 && gNew.children[1].name === '筆記具',
  JSON.stringify(gNew));
check('ichiba: 新形式のフルパス組み立て',
  ichiba.genrePathOf(gNew) === '日用品雑貨・文房具・手芸 > 文房具・事務用品');
check('ichiba: root (children のみ) も新形式で通る',
  ichiba.normalizeGenreSearch({ genre: { id: 0, jaName: 'root', level: 0 }, children: [{ id: 100371, jaName: 'レディースファッション', level: 1 }] }).children[0].genreId === '100371');

check('ichiba: 壊れた応答も落ちない',
  JSON.stringify(ichiba.normalizeGenreSearch({})) === JSON.stringify({ current: null, parents: [], children: [] }));

// ─── 利益シミュレーション (Notion 数式の移植) ───
const profit = await import('../lib/profit.js');
check('profit: Notion式どおり (1280円/原価660/税10%/送料237 → 189円 14.8%)',
  JSON.stringify(profit.computeProfit({ price: 1280, costExTax: 660, taxPercent: 10, shippingCost: 237 }))
  === JSON.stringify({ profit: 189, marginPct: 14.8, costIncTax: 726 }));
check('profit: 税率8%', profit.computeProfit({ price: 1000, costExTax: 500, taxPercent: 8, shippingCost: 100 }).profit === Math.round(900 - 540 - 100));
check('profit: 税率null は計算しない (フォールバックは呼び出し側の責務)',
  profit.computeProfit({ price: 1000, costExTax: 500, taxPercent: null, shippingCost: 100 }) === null);
check('profit: 赤字も計算できる', profit.computeProfit({ price: 500, costExTax: 600, taxPercent: 10, shippingCost: 200 }).profit < 0);
check('profit: 欠損は null (売価0/原価null/送料null/税率異常)',
  profit.computeProfit({ price: 0, costExTax: 1, taxPercent: 10, shippingCost: 1 }) === null
  && profit.computeProfit({ price: 100, costExTax: null, taxPercent: 10, shippingCost: 1 }) === null
  && profit.computeProfit({ price: 100, costExTax: 1, taxPercent: 10, shippingCost: null }) === null
  && profit.computeProfit({ price: 100, costExTax: 1, taxPercent: 999, shippingCost: 1 }) === null);
check('profit: TAKE_RATE=0.9 (Notion式の手数料控除)', profit.TAKE_RATE === 0.9);

// getNeCost: mirror から原価/送料/配送方法/税率
insProd.run(9401, 'cost-smoke', '原価検証', null);
db.prepare(`UPDATE mirror_products SET 原価 = 660, 送料 = 237, 配送方法 = 'ネコポス', 消費税率 = 0.1 WHERE product_id = 9401`).run();
const nc = vari.getNeCost(db, 'COST-SMOKE');
check('getNeCost: 原価/送料/配送方法/税率 (大小ゆらぎ吸収)',
  nc && nc.costExTax === 660 && nc.shippingCost === 237 && nc.shippingMethod === 'ネコポス' && nc.taxPercent === 10,
  JSON.stringify(nc));
check('getNeCost: NEに無ければ null', vari.getNeCost(db, 'NO-SUCH-COST') === null);
db.prepare(`DELETE FROM mirror_products WHERE product_id = 9401`).run();

// getNeCost バリエーション対応 (2026-08-24 中原さん要望): 代表コードの行は93%実在しないので、
// 子SKUの行から集計する。全SKU同じ原価 → 採用 / SKUで違う → costVaries=true で採用しない
insProd.run(9410, 'vc-a', 'バリ原価A', 'vcost');
insProd.run(9411, 'vc-b', 'バリ原価B', 'vcost');
db.prepare(`UPDATE mirror_products SET 原価 = 300, 送料 = 237, 配送方法 = 'ネコポス', 消費税率 = 0.1 WHERE product_id IN (9410, 9411)`).run();
const ncSame = vari.getNeCost(db, 'vcost'); // 'vcost' 自体は mirror に行が無い
check('getNeCost: バリエーションは子SKUから集計 (全SKU同じ原価はその値を採用)',
  ncSame && ncSame.source === 'members' && ncSame.costExTax === 300 && ncSame.costVaries === false
  && ncSame.shippingCost === 237 && ncSame.taxPercent === 10 && ncSame.skuCosts.length === 2,
  JSON.stringify(ncSame));
db.prepare(`UPDATE mirror_products SET 原価 = 500 WHERE product_id = 9411`).run();
const ncVary = vari.getNeCost(db, 'vcost');
check('getNeCost: SKUで原価が違えば costVaries=true で原価は採用しない (skuCosts に個別値)',
  ncVary && ncVary.costVaries === true && ncVary.costExTax === null
  && JSON.stringify(ncVary.skuCosts.map((s) => s.costExTax)) === '[300,500]',
  JSON.stringify(ncVary));
check('getNeCost: 子SKUコードで引いても同じグループ集計になる',
  vari.getNeCost(db, 'vc-a')?.costVaries === true);
// 「500円 / 未設定」の混在も「全SKU同じ」とは扱わない
db.prepare(`UPDATE mirror_products SET 原価 = NULL WHERE product_id = 9410`).run();
check('getNeCost: 原価未設定と設定済みの混在も costVaries=true',
  vari.getNeCost(db, 'vcost')?.costVaries === true && vari.getNeCost(db, 'vcost')?.costExTax === null);
db.prepare(`UPDATE mirror_products SET 原価 = 300 WHERE product_id = 9410`).run();
// draftId を渡すと、そのドラフトで外した SKU は集計から除かれる
{
  const idVc = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('vcost', 'バリ原価', 'smoke')
  `).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_variation_exclusions (draft_id, ne_code, actor) VALUES (?, 'vc-b', 'smoke')`).run(idVc);
  const ncEx = vari.getNeCost(db, 'vcost', { draftId: idVc });
  check('getNeCost: 外したSKU (500円) は集計から除かれ残り (300円) で一致する',
    ncEx && ncEx.costExTax === 300 && ncEx.costVaries === false && ncEx.skuCosts.length === 1,
    JSON.stringify(ncEx));
  db.prepare('DELETE FROM draft_variation_exclusions WHERE draft_id = ?').run(idVc);
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idVc);
}

// ─── 商品ページ表記 (xlsm移植) ───
const pinfo = await import('../lib/page-info.js');

// validatePageInfo: 化粧品/健康食品の楽天必須記載
check('page-info: general は必須チェック対象外',
  pinfo.validatePageInfo({ product_type: 'general' }).length === 0
  && pinfo.validatePageInfo(null).length === 0);
check('page-info: 食品(一般) も対象外 (食品表示は推奨に留める)',
  pinfo.validatePageInfo({ product_type: 'food' }).length === 0);
check('page-info: 化粧品の空は 発売元/製造国/商品区分 の3件不足',
  pinfo.validatePageInfo({ product_type: 'cosmetics' }).length === 3);
check('page-info: 化粧品の充足でゼロ',
  pinfo.validatePageInfo({ product_type: 'cosmetics', seller_name: 'X社', origin_type: '日本製', category_label: '化粧品' }).length === 0);
check('page-info: 化粧品の海外製は原産国なしでも通る (原則記載だが必須ではない)',
  pinfo.validatePageInfo({ product_type: 'cosmetics', seller_name: 'X社', origin_type: '海外製', importer_name: '輸入X', category_label: '化粧品' }).length === 0);
check('page-info: 海外製 (輸入品) は輸入者名が必須 (メーカー名との両記載)',
  pinfo.validatePageInfo({ product_type: 'cosmetics', seller_name: 'X社', origin_type: '海外製', category_label: '化粧品' })
    .some((r) => r.includes('輸入者名')));
check('page-info: 健康食品の海外製は原産国名が必須',
  pinfo.validatePageInfo({ product_type: 'health_food', seller_name: 'X社', origin_type: '海外製', importer_name: '輸入X', category_label: '健康食品' })
    .some((r) => r.includes('原産国')));
// 健康食品は加工食品の必須記載 (名称/原材料名/内容量/賞味期限/保存方法) も必須 (2026-07-29 追加)
{
  const hfBase = { product_type: 'health_food', seller_name: 'X社', origin_type: '日本製', category_label: '健康食品' };
  const missing = pinfo.validatePageInfo(hfBase);
  check('page-info: 健康食品は食品表示5項目 (名称/原材料名/内容量/賞味期限/保存方法) が必須',
    ['名称', '原材料名', '内容量', '賞味期限', '保存方法'].every((k) => missing.some((r) => r.includes(k))));
  check('page-info: 健康食品の充足でゼロ',
    pinfo.validatePageInfo({ ...hfBase, food_name: 'サプリ', food_ingredients: 'アマニ',
      content_volume: '90粒', food_expiry: 'ラベルに記載', food_storage: '常温保存' }).length === 0);
  check('page-info: 化粧品には食品表示チェックは掛からない',
    pinfo.validatePageInfo({ product_type: 'cosmetics', seller_name: 'X社', origin_type: '日本製', category_label: '化粧品' }).length === 0);
}
check('page-info: 商品タイプと商品区分の不整合を弾く (化粧品×雑貨)',
  pinfo.validatePageInfo({ product_type: 'cosmetics', seller_name: 'X社', origin_type: '日本製', category_label: '雑貨' })
    .some((r) => r.includes('整合しない'))
  && pinfo.validatePageInfo({ product_type: 'health_food', seller_name: 'X社', origin_type: '日本製', category_label: '化粧品' })
    .some((r) => r.includes('整合しない')));

// 広告文責 env は <br> 以外を全部エスケープ (Codex R1 medium: env経由のXSS)
{
  const prev = process.env.PH_AD_RESPONSIBILITY;
  process.env.PH_AD_RESPONSIBILITY = 'X社<br><script>alert(1)</script>';
  check('page-info: 広告文責envは<br>だけ許可して他はエスケープ',
    pinfo.adResponsibility() === 'X社<br>&lt;script&gt;alert(1)&lt;/script&gt;',
    pinfo.adResponsibility());
  if (prev === undefined) delete process.env.PH_AD_RESPONSIBILITY;
  else process.env.PH_AD_RESPONSIBILITY = prev;
}

// buildPageInfoHtml: 表の生成・空行の省略・エスケープ
{
  const html = pinfo.buildPageInfoHtml({
    productName: 'テスト<商品> & "A"',
    info: {
      product_type: 'cosmetics', content_volume: '50ml', ingredients: '水\nグリセリン',
      origin_type: '海外製', origin_country: 'フランス', category_label: '化粧品',
      seller_name: 'メーカーA', importer_name: '輸入者B',
    },
    shippingLabel: 'ネコポス',
  });
  check('page-info html: table + エスケープ + 改行→<br>',
    html.startsWith('<table') && html.includes('テスト&lt;商品&gt; &amp; &quot;A&quot;') && html.includes('水<br>グリセリン'),
    html.slice(0, 200));
  check('page-info html: 製造国は「海外製（フランス）」形式', html.includes('海外製（フランス）'));
  check('page-info html: 発売元 + 輸入者の両記載 (楽天ルール: 輸入品)', html.includes('メーカーA<br>輸入者: 輸入者B'));
  check('page-info html: 発送方法/広告文責/注意事項が載る',
    html.includes('ネコポス') && html.includes(pinfo.adResponsibility()) && html.includes(pinfo.FIXED_NOTES));
  check('page-info html: 空欄の行は出さない (サイズ/使用上の注意なし)',
    !html.includes('サイズ') && !html.includes('使用上の注意'));
  const foodHtml = pinfo.buildPageInfoHtml({
    productName: 'アマニ', info: {
      product_type: 'food', food_name: '有機亜麻仁シード', food_ingredients: '有機アマニ',
      food_expiry: 'ラベルに記載', food_storage: '常温',
    }, shippingLabel: null,
  });
  check('page-info html: 食品は 名称/原材料名/賞味期限/保存方法',
    ['名称', '原材料名', '賞味期限', '保存方法'].every((k) => foodHtml.includes(k))
    && !foodHtml.includes('発送方法'));
  check('page-info html: 全空でも固定行 (商品名/広告文責/注意事項) だけの表になる',
    pinfo.buildPageInfoHtml({ productName: 'X', info: null, shippingLabel: null }).includes('広告文責'));
  check('page-info html: 商品名すら無ければ固定行のみ (空文字にはならない)',
    pinfo.buildPageInfoHtml({ productName: '', info: null, shippingLabel: null }).includes('注意事項'));
}

// mapNeShippingToRakuten: 保存済み > 完全一致 > 部分一致 > null
insProd.run(9402, 'ship-smoke', '配送検証', null);
db.prepare(`UPDATE mirror_products SET 配送方法 = 'ネコポス' WHERE product_id = 9402`).run();
insProd.run(9403, 'ship-smoke2', '配送検証2', null);
db.prepare(`UPDATE mirror_products SET 配送方法 = '宅急便コンパクト' WHERE product_id = 9403`).run();
{
  const exact = pinfo.mapNeShippingToRakuten(db, 'ネコポス');
  check('shipmap: 名前完全一致は推測で当たる (ネコポス→5)', exact.group === '5' && exact.guessed === true, JSON.stringify(exact));
  const partial = pinfo.mapNeShippingToRakuten(db, '宅急便コンパクト');
  check('shipmap: 部分一致は推測しない (宅急便コンパクト≠宅急便。誤マッピング防止)',
    partial.guessed === false && partial.group === null, JSON.stringify(partial));
  check('shipmap: 一致なしは null', pinfo.mapNeShippingToRakuten(db, '謎の配送方法').group === null);
  check('shipmap: 空/null は null', pinfo.mapNeShippingToRakuten(db, '').group === null && pinfo.mapNeShippingToRakuten(db, null).group === null);

  const saved = pinfo.saveShippingMethodMap(db, [
    { ne_label: 'ネコポス', rakuten_group: '6' },        // 推測(5)を明示上書き
    { ne_label: '謎の配送方法', rakuten_group: '99' },   // 不正グループ → 保存されない
    { ne_label: '  ', rakuten_group: '1' },              // 空ラベル → 無視
    { ne_label: '廃止された方法', rakuten_group: '1' },  // NEに現存しないが保存はできる
  ]);
  check('shipmap: 保存は正当な2件のみ', saved === 2, String(saved));
  check('shipmap: 保存済みが推測より優先 (ネコポス→6)',
    pinfo.mapNeShippingToRakuten(db, 'ネコポス').group === '6'
    && pinfo.mapNeShippingToRakuten(db, 'ネコポス').guessed === false);
  const rows = pinfo.listShippingMethodMap(db);
  const neko = rows.find((r) => r.neLabel === 'ネコポス');
  const orphan = rows.find((r) => r.neLabel === '廃止された方法');
  check('shipmap 一覧: 保存済み + 件数', neko && neko.saved === true && neko.rakutenGroup === '6' && neko.count >= 1, JSON.stringify(neko));
  check('shipmap 一覧: NEに現存しない保存済みも出る (棚卸し用)', orphan && orphan.count === 0 && orphan.saved === true);
  const compact = rows.find((r) => r.neLabel === '宅急便コンパクト');
  check('shipmap 一覧: 完全一致しない未保存ラベルは未割当のまま出る',
    compact && compact.saved === false && compact.rakutenGroup === null, JSON.stringify(compact));
  // upsert 再保存の冪等
  pinfo.saveShippingMethodMap(db, [{ ne_label: 'ネコポス', rakuten_group: '5' }]);
  check('shipmap: 再保存で上書き (6→5)', pinfo.mapNeShippingToRakuten(db, 'ネコポス').group === '5');
  // 未割当 (空) 保存 → 推測に戻る
  pinfo.saveShippingMethodMap(db, [{ ne_label: 'ネコポス', rakuten_group: '' }]);
  check('shipmap: 空で保存すると未割当 → 名前一致の推測に戻る',
    pinfo.mapNeShippingToRakuten(db, 'ネコポス').guessed === true);
}
db.prepare(`DELETE FROM mirror_products WHERE product_id IN (9402, 9403)`).run();
db.prepare(`DELETE FROM ph_shipping_method_map`).run();

// ─── ワークフロー: 担当者 / 役割 / 工程 (2026-08-23) ───
const wf = await import('../lib/workflow.js');
{
  const roles0 = wf.listRoles();
  check('役割シード 5件', roles0.length === 5, `= ${roles0.length}`);
  check('役割シード: 画像作成承認者', roles0.some((r) => r.code === 'image_approver' && r.label === '画像作成承認者'));
  check('役割シード: 商品登録者', roles0.some((r) => r.code === 'registrar' && r.label === '商品登録者'));
  const steps0 = wf.listSteps();
  check('工程シード: 本流6 + 画像9 (TOP 4段階 + 詳細 5段階 = 撮影依頼中は詳細のみ)',
    steps0.filter((s) => s.track === 'main').length === 6 && steps0.filter((s) => s.track === 'image').length === 14,
    `main=${steps0.filter((s) => s.track === 'main').length} image=${steps0.filter((s) => s.track === 'image').length}`);
  {
    const expectedImgOrder = {
      top: 'img_request_top,img_production_top,img_register_top,img_approve_top',
      detail: 'imgd_request,imgd_compose,imgd_material,imgd_ai,imgd_design,imgd_review_1,imgd_review_2,imgd_amazon,imgd_rakuten,imgd_aplus',
    };
    for (const kind of ['top', 'detail']) {
      const img = steps0.filter((s) => s.track === 'image' && s.image_kind === kind).map((s) => s.code);
      check(`工程シード: ${kind} の段階の並び (詳細は v2 ①〜⑨)`,
        img.join(',') === expectedImgOrder[kind], img.join(','));
    }
    check('工程シード: 画像承認 (TOP) と 社内確認 (中原) の担当は画像作成承認者',
      steps0.find((s) => s.code === 'img_approve_top').role_code === 'image_approver'
      && steps0.find((s) => s.code === 'imgd_review_2').role_code === 'image_approver');
    check('工程シード: image_stage が入る (ボードの列まとめの安定キー。⑥の2工程は同じ review)',
      steps0.find((s) => s.code === 'img_request_top').image_stage === 'request'
      && steps0.find((s) => s.code === 'imgd_review_1').image_stage === 'review'
      && steps0.find((s) => s.code === 'imgd_review_2').image_stage === 'review');
    check('工程シード: skippable / listing_gate (TOP と ①〜⑥ は対象外不可・⑦⑧⑨ は出品ゲート外)',
      steps0.find((s) => s.code === 'img_request_top').skippable === 0
      && steps0.find((s) => s.code === 'imgd_request').skippable === 0 && steps0.find((s) => s.code === 'imgd_request').listing_gate === 1
      && steps0.find((s) => s.code === 'imgd_amazon').skippable === 1 && steps0.find((s) => s.code === 'imgd_amazon').listing_gate === 0
      && steps0.find((s) => s.code === 'imgd_rakuten').listing_gate === 0 && steps0.find((s) => s.code === 'imgd_aplus').listing_gate === 0);
    check('工程シード: 旧詳細 v1 工程はシードされない (新規DB)', !steps0.some((s) => dbmod.LEGACY_DETAIL_V1_CODES.includes(s.code)));
    check('工程シード: 旧一本トラック工程はシードされない', !steps0.some((s) => ['img_request', 'img_production', 'img_register', 'img_approve'].includes(s.code)));
  }
  check('工程シード: AI待ちはシステム工程 (担当ロールなし)', steps0.find((s) => s.code === 'ai_generate').role_code === null);
  check('工程シード: 先頭は基本情報入力', steps0[0].code === 'basic_info');

  // 管理画面で改名したものが再 init で巻き戻らない (INSERT OR IGNORE の意図)
  wf.updateStep('basic_info', { label: '基本情報入力 (改)' });
  dbmod._resetInitForTest();
  dbmod.getDB();
  check('工程の改名が再initで戻らない', wf.listSteps().find((s) => s.code === 'basic_info').label === '基本情報入力 (改)');
  wf.updateStep('basic_info', { label: '基本情報入力' });

  // 担当者の登録
  const okawa = wf.createStaff({ name: '大川さん', kind: 'outsource' });
  const tanaka = wf.createStaff({ name: '田中美祐', kind: 'internal', portal_email: 'Tanaka@B-Faith.biz' });
  check('担当者に色が自動で付く', /^#[0-9a-f]{6}$/i.test(wf.getStaff(okawa).color || ''));
  check('ポータルメールは小文字で保存', wf.getStaff(tanaka).portal_email === 'tanaka@b-faith.biz');
  check('ポータルメールで担当者を引ける', wf.staffByPortalEmail('TANAKA@b-faith.biz')?.id === tanaka);

  let dup = null;
  try { wf.createStaff({ name: ' 大川さん ' }); } catch (e) { dup = e; }
  check('同名 (前後空白違い) は登録できない', dup?.status === 400, dup?.message || '例外が出ていない');
  let dupMail = null;
  try { wf.createStaff({ name: '別人さん', portal_email: 'tanaka@b-faith.biz' }); } catch (e) { dupMail = e; }
  check('同じポータルメールを2人に付けられない', !!dupMail, '例外が出ていない');
  let badMail = null;
  try { wf.createStaff({ name: '書式違いさん', portal_email: 'not-an-email' }); } catch (e) { badMail = e; }
  check('メール形式を検証する', badMail?.status === 400);

  // 役割の割り当てと既定担当
  wf.setStaffRoles(okawa, [{ code: 'image', isDefault: true }]);
  wf.setStaffRoles(tanaka, [{ code: 'registrar', isDefault: true }, { code: 'image' }]);
  check('既定担当が工程に出る', wf.listSteps().find((s) => s.code === 'img_request_top').default_staff_name === '大川さん');
  check('工程に紐づく人数が出る', wf.listSteps().find((s) => s.code === 'img_request_top').member_count === 2);
  // 既定は役割ごとに 1 人 = 後から立てた方に付け替わる (UNIQUE 違反でエラーにしない)
  wf.setStaffRoles(tanaka, [{ code: 'registrar', isDefault: true }, { code: 'image', isDefault: true }]);
  check('既定は役割ごとに1人 (前任は外れる)',
    wf.getStaff(okawa).roles.find((r) => r.code === 'image').isDefault === false);
  check('付け替え後の既定が反映される', wf.listSteps().find((s) => s.code === 'img_request_top').default_staff_name === '田中美祐');

  // 無効化 = 退職者に自動割り当てしない。ただし役割の紐付け自体は履歴として残す
  wf.setStaffActive(tanaka, false);
  check('無効化で既定が外れる', wf.listSteps().find((s) => s.code === 'basic_info').default_staff_name == null);
  check('無効化しても役割の紐付けは残る', wf.getStaff(tanaka).roles.length === 2);
  check('無効化した人は一覧の既定から外れる', wf.listStaff().every((s) => s.id !== tanaka));
  wf.setStaffActive(tanaka, true);

  // builtin (工程が参照する定義) は無効化させない。改名は許す
  let roleErr = null;
  try { wf.updateRole('registrar', { active: false }); } catch (e) { roleErr = e; }
  check('工程が使う役割は無効化できない', roleErr?.status === 400, roleErr?.message || '例外が出ていない');
  check('役割の改名はできる', wf.updateRole('registrar', { label: '商品登録者' }) === true);
  let stepErr = null;
  try { wf.updateStep('basic_info', { active: false }); } catch (e) { stepErr = e; }
  check('本流の工程は無効化できない', stepErr?.status === 400, stepErr?.message || '例外が出ていない');

  // 運用で足した工程は無効化できる。画像トラックは種別 (TOP/詳細) の指定が必須
  let kindErr = null;
  try { wf.createStep({ label: '撮影', track: 'image', role_code: 'image' }); } catch (e) { kindErr = e; }
  check('画像トラックの工程は種別なしでは追加できない', kindErr?.status === 400, kindErr?.message || '例外が出ていない');
  const extra = wf.createStep({ label: '撮影', track: 'image', image_kind: 'top', role_code: 'image' });
  check('工程を追加できる (種別つき)', wf.listSteps({ track: 'image' }).find((s) => s.code === extra)?.image_kind === 'top');
  check('追加した工程は無効化できる', wf.updateStep(extra, { active: false }) === true);
  check('無効化した工程は既定の一覧に出ない', !wf.listSteps().some((s) => s.code === extra));

  // 画像トラックの組み込み工程はラベル・並び順・滞留日数が TOP/詳細 で対 — 片方を変えると両方そろう
  wf.updateStep('img_production_top', { label: '画像制作 (改)', stall_days: 9 });
  const sib = wf.listSteps({ includeInactive: true }).find((s) => s.code === 'img_production_detail') || { label: '画像制作 (改)', stall_days: 9 };
  check('対の工程にラベルが伝播する', sib.label === '画像制作 (改)', sib.label);
  check('対の工程に滞留日数が伝播する', sib.stall_days === 9, `= ${sib.stall_days}`);
  wf.updateStep('img_production_top', { label: '画像制作', stall_days: 7 });
  // 担当ロールは伝播しない (TOP と詳細で承認者を分けたい場合があるため)
  wf.updateStep('img_approve_top', { role_code: 'approver' });
  check('担当ロールは対に伝播しない',
    (wf.listSteps({ includeInactive: true }).find((s) => s.code === 'img_approve_detail')?.role_code ?? 'image_approver') === 'image_approver');
  wf.updateStep('img_approve_top', { role_code: 'image_approver' });

  // 滞留日数
  let stallErr = null;
  try { wf.updateStep('ai_generate', { stall_days: 0 }); } catch (e) { stallErr = e; }
  check('滞留日数 0 は弾く', stallErr?.status === 400);
  check('滞留日数を空にできる (警告しない)', wf.updateStep('ai_generate', { stall_days: '' }) === true);
  wf.updateStep('ai_generate', { stall_days: 3 });

  // 担当ロールを外してシステム工程にできる (将来の自動化に備える)
  check('工程をシステム工程に変えられる', wf.updateStep('set_review', { role_code: '' }) === true);
  check('システム工程は担当ロールが空', wf.listSteps().find((s) => s.code === 'set_review').role_code === null);
  wf.updateStep('set_review', { role_code: 'set_planner' });

  // 「担当者が 1 人もいない工程」の検知 (画面の警告バナー)
  const ov = wf.workflowOverview();
  check('担当者0人の役割を検知する', ov.unassignedRoles.some((u) => u.role === '商品承認者'), JSON.stringify(ov.unassignedRoles));
  check('overview は本流と画像に分かれる', ov.main.length === 6 && ov.image.length === 14,
    `main=${ov.main.length} image=${ov.image.length}`);
}

// ─── ワークフロー: 商品 × 工程の進捗 (2026-08-23) ───
const wfp = await import('../lib/workflow-progress.js');
// 工程操作の権限文脈。admin は全部できる (一般ユーザーの制限は下の権限ブロックで検証する)
const ADMIN = { isAdmin: true, actorStaffId: null };
const wfTanakaId = wf.listStaff().find((s) => s.name === '田中美祐').id;
const wfOkawaId = wf.listStaff().find((s) => s.name === '大川さん').id;
// 上のブロックで一度無効化したため既定が外れている (無効化で既定を落とす仕様)。
// 有効化しても既定は自動で戻らない = 管理画面で付け直す運用なので、ここでも付け直す
wf.setStaffRoles(wfTanakaId, [{ code: 'registrar', isDefault: true }, { code: 'image', isDefault: true }]);
let wfDraftId = null;
{
  const mk = (code, status) => Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES (?, ?, ?, 'smoke')`
  ).run(code, `工程テスト ${code}`, status).lastInsertRowid);

  // 旧 status からの初期化 (既存ドラフトが全部先頭列に固まらないようにする推定)
  const idReview = mk('WF-REVIEW', 'review');
  wfDraftId = idReview;
  const p = wfp.progressOf(idReview, { db });
  check('初期化: review なら基本情報とAI待ちが done',
    p.main.find((s) => s.step_code === 'basic_info').state === 'done'
    && p.main.find((s) => s.step_code === 'ai_generate').state === 'done');
  check('初期化: 現在工程は商品説明確認', p.current?.step_code === 'desc_review');
  check('初期化: 進捗カウント 2/6', p.doneCount === 2 && p.totalCount === 6);
  check('初期化: 画像が無ければ画像トラックは未着手', p.image.every((s) => s.state === 'todo'));
  check('既定担当が自動で入る', p.current.assignee_id === wfTanakaId);
  check('画像工程の担当は画像登録者の既定', p.image[0].assignee_id === wfTanakaId);
  check('システム工程には担当者を置かない', p.main.find((s) => s.step_code === 'ai_generate').assignee_id === null);

  // 画像が登録済みなら画像トラックは終わっているとみなす (外注への二重依頼を防ぐ)
  // status=listed は楽天登録済みの根拠 (v2) なので、ここは approved で「画像があるだけ」を再現する
  const idListed = mk('WF-LISTED', 'approved');
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'wf-img-1', 0)`).run(idListed);
  const pl = wfp.progressOf(idListed, { db });
  check('初期化: 画像があれば TOP側は done (二重依頼の防止)', pl.imageTop.done === true);
  check('初期化: 画像があっても詳細側は todo (詳細画像が揃っている根拠にならない)',
    pl.imageDetail.rows.every((s) => s.state === 'todo') && pl.imageDone === false);
  check('初期化: approved なら残りはセット検討から', pl.current?.step_code === 'set_review');
  // 楽天登録済みなら詳細側も done で入る (出品済みに「承認して」を出さない)
  const idListedRk0 = mk('WF-LISTED-RK', 'listed');
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'wf-img-2', 0)`).run(idListedRk0);
  db.prepare(`INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-08-20T00:00:00Z')`).run(idListedRk0);
  check('初期化: 楽天登録済みは詳細側も done', wfp.progressOf(idListedRk0, { db }).imageDone === true);

  // 工程を進める
  const r1 = wfp.setStepState(idReview, 'desc_review', { state: 'done' }, 'smoke@b-faith.biz', ADMIN);
  check('工程を完了にできる', r1.changed === true);
  const p2 = wfp.progressOf(idReview, { db });
  check('完了すると次の工程にボールが移る', p2.current?.step_code === 'title_approve');
  check('完了者と完了日時が残る',
    p2.main.find((s) => s.step_code === 'desc_review').done_by === 'smoke@b-faith.biz'
    && !!p2.main.find((s) => s.step_code === 'desc_review').done_at);
  check('工程の変更が監査ログに残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'step_changed'`).get(idReview).c > 0);

  // 差し戻し
  wfp.setStepState(idReview, 'desc_review', { state: 'todo' }, 'smoke@b-faith.biz', ADMIN);
  const p3 = wfp.progressOf(idReview, { db });
  check('差し戻すと完了の痕跡が消える',
    p3.main.find((s) => s.step_code === 'desc_review').done_at === null
    && p3.main.find((s) => s.step_code === 'desc_review').done_by === null);
  check('差し戻すとボールが戻る', p3.current?.step_code === 'desc_review');

  // 「対象外」はボールを止めない (セット検討が不要な商品など)
  wfp.setStepState(idReview, 'desc_review', { state: 'done' }, 'smoke', ADMIN);
  wfp.setStepState(idReview, 'title_approve', { state: 'skip' }, 'smoke', ADMIN);
  check('対象外にした工程は飛ばされる', wfp.progressOf(idReview, { db }).current?.step_code === 'set_review');

  // 担当者の付け替え
  check('担当者を変えられる', wfp.setStepState(idReview, 'set_review', { assignee_id: wfOkawaId }, 'smoke', ADMIN).changed === true);
  check('未割り当てに戻せる', wfp.setStepState(idReview, 'set_review', { assignee_id: '' }, 'smoke', ADMIN).changed === true);
  let asgErr = null;
  try { wfp.setStepState(idReview, 'set_review', { assignee_id: 999999 }, 'smoke', ADMIN); } catch (e) { asgErr = e; }
  check('存在しない担当者は弾く', asgErr?.status === 400);
  // 無効化した人を新たに割り当てさせない (退職者に仕事を振らない)
  const ghost = wf.createStaff({ name: '退職者さん' });
  wf.setStaffActive(ghost, false);
  let offErr = null;
  try { wfp.setStepState(idReview, 'set_review', { assignee_id: ghost }, 'smoke', ADMIN); } catch (e) { offErr = e; }
  check('無効化した担当者は割り当てられない', offErr?.status === 400, offErr?.message || '例外が出ていない');

  // 期限・メモ
  let dueErr = null;
  try { wfp.setStepState(idReview, 'set_review', { due_date: '2026/09/01' }, 'smoke', ADMIN); } catch (e) { dueErr = e; }
  check('期限の形式を検証する', dueErr?.status === 400);
  wfp.setStepState(idReview, 'set_review', { due_date: '2026-09-01', note: 'セット候補あり' }, 'smoke', ADMIN);
  const p4 = wfp.progressOf(idReview, { db });
  check('期限とメモを保存できる',
    p4.main.find((s) => s.step_code === 'set_review').due_date === '2026-09-01'
    && p4.main.find((s) => s.step_code === 'set_review').note === 'セット候補あり');

  // 滞留の検知 (AI待ちは 3 日で警告)。前工程の完了日時を 5 日前にずらして再現する
  const idStall = mk('WF-STALL', 'ready_for_ai');
  wfp.progressOf(idStall, { db });
  db.prepare(`UPDATE draft_step_progress SET done_at = ? WHERE draft_id = ? AND step_code = 'basic_info'`)
    .run(new Date(Date.now() - 5 * 86400000).toISOString(), idStall);
  const ps = wfp.progressOf(idStall, { db });
  check('滞留日数が出る (AI待ち5日)', ps.stalledDays === 5, `= ${ps.stalledDays}`);
  check('滞留していない工程は null', wfp.progressOf(idListed, { db }).stalledDays === null);

  // 一括自己修復: 足りないものだけ直す
  db.prepare('DELETE FROM draft_step_progress WHERE draft_id = ?').run(idStall);
  const fixed = wfp.ensureProgressForMany(db, [idReview, idListed, idStall]);
  check('不足しているドラフトだけ修復する', fixed === 1, `= ${fixed}`);
  check('修復後も工程が揃う', wfp.progressOf(idStall, { db }).totalCount === 6);

  // 工程を後から足しても既存ドラフトに行き渡る (推定 done は初回だけ = 過去分を勝手に完了にしない)
  const added = wf.createStep({ label: '検品', track: 'main', role_code: 'registrar' });
  wfp.ensureProgressForMany(db, [idReview]);
  const p5 = wfp.progressOf(idReview, { db });
  check('後から足した工程が既存ドラフトに追加される', p5.main.some((s) => s.step_code === added));
  check('後から足した工程は未着手で入る (勝手にdoneにしない)',
    p5.main.find((s) => s.step_code === added).state === 'todo');
  wf.updateStep(added, { active: false });
  check('工程を無効化すると進捗からも消える', !wfp.progressOf(idReview, { db }).main.some((s) => s.step_code === added));
}

// ─── マスタ更新の原子性と参照ガード (Codex R1 medium 対応) ───
{
  // 担当者本体と役割は 1 トランザクション: 役割で失敗したら担当者も作られない
  const before = wf.listStaff({ includeInactive: true }).length;
  let txErr = null;
  try {
    wf.createStaffWithRoles({ name: 'ロールバックさん', roles: [{ code: 'no_such_role' }] });
  } catch (e) { txErr = e; }
  check('役割が不正なら担当者も作られない (ロールバック)',
    txErr?.status === 400 && wf.listStaff({ includeInactive: true }).length === before,
    `err=${txErr?.message} count=${wf.listStaff({ includeInactive: true }).length}/${before}`);

  // 工程が参照しているカスタム役割は無効化させない (builtin でなくても同じ事故が起きる)
  const tmpRole = wf.createRole({ label: '検品担当' });
  const tmpStep = wf.createStep({ label: '検品ライン', track: 'main', role_code: tmpRole });
  let usedErr = null;
  try { wf.updateRole(tmpRole, { active: false }); } catch (e) { usedErr = e; }
  check('工程が使っているカスタム役割は無効化できない', usedErr?.status === 400, usedErr?.message || '例外が出ていない');
  wf.updateStep(tmpStep, { active: false });
  check('工程を外せばカスタム役割も無効化できる', wf.updateRole(tmpRole, { active: false }) === true);
}

// ─── 工程操作の権限 (Codex R1 high 対応) ───
// 2026-08-23 中原さん: 外注は契約終了済みで担当者は全員ログインする → 本人 + admin に限定
{
  const id = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-PERM', '権限テスト', 'draft', 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(id, { db });
  // basic_info は商品登録者 = 田中美祐が既定で入る
  const OTHER = { isAdmin: false, actorStaffId: wfOkawaId };
  const OWNER = { isAdmin: false, actorStaffId: wfTanakaId };
  const NOBODY = { isAdmin: false, actorStaffId: null };

  let e1 = null;
  try { wfp.setStepState(id, 'basic_info', { state: 'done' }, 'okawa', OTHER); } catch (e) { e1 = e; }
  check('他人の担当工程は動かせない', e1?.status === 403, e1?.message || '例外が出ていない');
  check('担当者本人は動かせる', wfp.setStepState(id, 'basic_info', { state: 'doing' }, 'tanaka', OWNER).changed === true);

  let e2 = null;
  try { wfp.setStepState(id, 'basic_info', { state: 'skip' }, 'tanaka', OWNER); } catch (e) { e2 = e; }
  check('本人でも「対象外」にはできない (admin だけ)', e2?.status === 403, e2?.message || '例外が出ていない');

  let e3 = null;
  try { wfp.setStepState(id, 'basic_info', { assignee_id: wfOkawaId }, 'tanaka', OWNER); } catch (e) { e3 = e; }
  check('他人への付け替えは admin だけ', e3?.status === 403, e3?.message || '例外が出ていない');

  // 未割り当ての工程は「引き受け」だけ許す (状態をいきなり変えさせない)
  wfp.setStepState(id, 'set_review', { assignee_id: null }, 'admin', ADMIN);
  let e4a = null;
  try { wfp.setStepState(id, 'set_review', { state: 'doing' }, 'okawa', OTHER); } catch (e) { e4a = e; }
  check('未割り当てでもいきなり状態は変えられない', e4a?.status === 403, e4a?.message || '例外が出ていない');
  check('未割り当ての工程は自分で引き受けられる',
    wfp.setStepState(id, 'set_review', { assignee_id: wfOkawaId }, 'okawa', OTHER).changed === true);
  check('引き受けたあとは自分で動かせる',
    wfp.setStepState(id, 'set_review', { state: 'doing' }, 'okawa', OTHER).changed === true);
  let e4 = null;
  try { wfp.setStepState(id, 'set_review', { state: 'done' }, 'tanaka', OWNER); } catch (e) { e4 = e; }
  check('引き受けたあとは他の人が動かせない', e4?.status === 403);
  let e4b = null;
  try { wfp.setStepState(id, 'title_approve', { assignee_id: wfTanakaId }, 'okawa', OTHER); } catch (e) { e4b = e; }
  check('他人を担当に指名はできない (自分の引き受けだけ)', e4b?.status === 403, e4b?.message || '例外が出ていない');

  // システム工程 (AI待ち) は admin だけが手で進められる
  let e5a = null;
  try { wfp.setStepState(id, 'ai_generate', { state: 'done' }, 'nobody', NOBODY); } catch (e) { e5a = e; }
  check('システム工程は一般ユーザーが進められない', e5a?.status === 403, e5a?.message || '例外が出ていない');
  check('システム工程は admin なら進められる',
    wfp.setStepState(id, 'ai_generate', { state: 'done' }, 'admin', ADMIN).changed === true);

  // 楽観ロック: 画面が読んだ version を送る → 古い画面からの操作を弾く。
  // updated_at (ミリ秒) をトークンにすると同一ミリ秒の連続更新をすり抜ける (Codex R3)
  const beforeRow = db.prepare(
    `SELECT version FROM draft_step_progress WHERE draft_id = ? AND step_code = 'desc_review'`
  ).get(id);
  // 別の人が先に動かした状況を作る
  const r0 = wfp.setStepState(id, 'desc_review', { state: 'doing' }, 'admin', ADMIN);
  check('更新すると版数が上がる', r0.version === beforeRow.version + 1, `${beforeRow.version} → ${r0.version}`);
  let e5 = null;
  try {
    // 古い画面が「まだ未着手のはず」と思って done を送る
    wfp.setStepState(id, 'desc_review', { state: 'done', expected_version: beforeRow.version }, 'admin', ADMIN);
  } catch (e) { e5 = e; }
  check('古い画面からの後勝ちを 409 で弾く', e5?.status === 409, e5?.message || '例外が出ていない');
  // メモだけの更新でも版数が合わなければ弾く (状態が変わらない更新の抜け道を塞ぐ)
  let e5b = null;
  try {
    wfp.setStepState(id, 'desc_review', { note: '古い画面から', expected_version: beforeRow.version }, 'admin', ADMIN);
  } catch (e) { e5b = e; }
  check('メモだけの更新も版数不一致なら弾く', e5b?.status === 409);
  check('最新の版数を送れば通る',
    wfp.setStepState(id, 'desc_review', { state: 'done', expected_version: r0.version }, 'admin', ADMIN).changed === true);
  // 画面から来る操作 (requireVersion) はトークン省略を許さない
  let e5c = null;
  try {
    wfp.setStepState(id, 'desc_review', { state: 'todo' }, 'admin', { ...ADMIN, requireVersion: true });
  } catch (e) { e5c = e; }
  check('版数を省略した画面操作は受け付けない', e5c?.status === 409, e5c?.message || '例外が出ていない');

  // 日付の実在検証
  let e6 = null;
  try { wfp.setStepState(id, 'set_review', { due_date: '2026-02-31' }, 'admin', ADMIN); } catch (e) { e6 = e; }
  check('存在しない日付を弾く (2026-02-31)', e6?.status === 400, e6?.message || '例外が出ていない');
  let e7 = null;
  try { wfp.setStepState(id, 'set_review', { due_date: '2026-99-99' }, 'admin', ADMIN); } catch (e) { e7 = e; }
  check('存在しない日付を弾く (2026-99-99)', e7?.status === 400);
  check('実在する日付は通る (うるう年)',
    wfp.setStepState(id, 'set_review', { due_date: '2028-02-29' }, 'admin', ADMIN).changed === true);
}

// ─── 楽天出品ゲート: 画像トラック (画像承認まで) が終わるまで出さない (2026-08-23) ───
{
  // 画像の無い商品を専用に作る (WF-REVIEW を使うと、後ろのボードのテストで画像列に出なくなる)
  const idGate = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-IMG-GATE', '画像ゲート', 'approved', 'smoke')`
  ).run().lastInsertRowid);
  const blocked = listing.buildItemPayload(db, idGate);
  check('画像トラック未完了なら出品を止める',
    (blocked.reasons || []).some((x) => /画像トラックが終わっていません/.test(x)), JSON.stringify(blocked.reasons || []).slice(0, 200));
  check('止める理由に種別といまの工程が出る',
    (blocked.reasons || []).some((x) => /TOP画像: 画像制作の依頼/.test(x) && /詳細画像: 画像制作の依頼/.test(x)),
    JSON.stringify(blocked.reasons || []).slice(0, 300));
  // TOP画像の工程は admin でも「対象外」にできない (サムネイル無しの楽天出品はありえない)
  let topSkipErr = null;
  try { wfp.setStepState(idGate, 'img_request_top', { state: 'skip' }, 'admin', ADMIN); } catch (e) { topSkipErr = e; }
  check('TOP画像の工程は対象外にできない', topSkipErr?.status === 400, topSkipErr?.message || '例外が出ていない');
  let d1SkipErr = null;
  try { wfp.setStepState(idGate, 'imgd_request', { state: 'skip' }, 'admin', ADMIN); } catch (e) { d1SkipErr = e; }
  check('v2: 詳細 ①〜⑥ は対象外にできない (skippable=0)', d1SkipErr?.status === 400, d1SkipErr?.message || '例外が出ていない');
  check('v2: 詳細 ⑦ Amazon登録依頼 は admin なら対象外にできる',
    wfp.setStepState(idGate, 'imgd_amazon', { state: 'skip' }, 'admin', ADMIN).changed === true);
  wfp.setStepState(idGate, 'imgd_amazon', { state: 'todo' }, 'admin', ADMIN);
  // ガードをすり抜けて TOP が全部 skip になってもゲートは開かない (直 SQL で再現)
  db.prepare(`
    UPDATE draft_step_progress SET state = 'skip'
    WHERE draft_id = ? AND step_code IN ('img_request_top','img_production_top','img_register_top','img_approve_top')
  `).run(idGate);
  check('TOP全対象外でもゲートは開かない (防御)',
    /TOP画像.*必須/.test(wfp.imageTrackBlockReason(db, idGate) || ''), wfp.imageTrackBlockReason(db, idGate));
  db.prepare(`
    UPDATE draft_step_progress SET state = 'todo'
    WHERE draft_id = ? AND step_code LIKE 'img_%_top'
  `).run(idGate);
  // fail-closed: TOP側の工程が 1 つも無い (設定壊れ・移行漏れ) なら出品を通さない
  db.prepare(`UPDATE ph_steps SET active = 0 WHERE image_kind = 'top'`).run();
  check('TOP工程が無い設定壊れではゲートを閉じる (fail-closed)',
    /TOP画像.*見つかりません/.test(wfp.imageTrackBlockReason(db, idGate) || ''), wfp.imageTrackBlockReason(db, idGate));
  db.prepare(`UPDATE ph_steps SET active = 1 WHERE image_kind = 'top' AND builtin = 1`).run();
  // 詳細側の工程 0 件も同じく fail-closed (対象外にしていない限り — Codex R2 high)
  db.prepare(`UPDATE ph_steps SET active = 0 WHERE image_kind = 'detail'`).run();
  check('詳細工程が無い設定壊れでもゲートを閉じる (fail-closed)',
    /詳細画像.*見つかりません/.test(wfp.imageTrackBlockReason(db, idGate) || ''), wfp.imageTrackBlockReason(db, idGate));
  db.prepare(`UPDATE ph_steps SET active = 1 WHERE image_kind = 'detail' AND builtin = 1`).run();

  // 「詳細画像は対象外」(商品単位フラグ): TOP の承認だけでゲートが開く
  wfp.setDetailImagesExcluded(idGate, true, 'admin', ADMIN);
  check('対象外の切り替えがイベントに残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'detail_images_excluded'`).get(idGate).c === 1);
  check('二重送信は no-op', wfp.setDetailImagesExcluded(idGate, true, 'admin', ADMIN).changed === false);
  for (const s of wfp.progressOf(idGate, { db }).imageTop.rows) {
    wfp.setStepState(idGate, s.step_code, { state: 'done' }, 'admin', ADMIN);
  }
  check('詳細対象外なら TOP 承認だけで画像の理由が消える',
    !(listing.buildItemPayload(db, idGate).reasons || []).some((x) => /画像トラック/.test(x)),
    JSON.stringify((listing.buildItemPayload(db, idGate).reasons || []).filter((x) => /画像/.test(x))));
  // 対象外を解除すると詳細側が未完了なのでまたブロックされる
  wfp.setDetailImagesExcluded(idGate, false, 'admin', ADMIN);
  check('対象外を解除すると詳細側でまたブロック',
    /詳細画像/.test(wfp.imageTrackBlockReason(db, idGate) || ''), wfp.imageTrackBlockReason(db, idGate));
  // 権限: admin か詳細画像「依頼」工程の担当者本人だけ
  let exErr = null;
  try { wfp.setDetailImagesExcluded(idGate, true, 'okawa', { isAdmin: false, actorStaffId: wfOkawaId }); } catch (e) { exErr = e; }
  check('対象外の切り替えは担当外の人はできない', exErr?.status === 403, exErr?.message || '例外が出ていない');
  wfp.setStepState(idGate, 'imgd_request', { assignee_id: wfOkawaId }, 'admin', ADMIN);
  check('詳細画像の依頼担当なら対象外にできる',
    wfp.setDetailImagesExcluded(idGate, true, 'okawa', { isAdmin: false, actorStaffId: wfOkawaId }).changed === true);
  wfp.setDetailImagesExcluded(idGate, false, 'admin', ADMIN);

  // 画像トラックを全部完了にすると理由が消える (他の理由は残ってよい)
  for (const s of wfp.progressOf(idGate, { db }).image) {
    // ⑧ 楽天登録は出品なしに done にできない → 対象外で決着させる
    if (s.state !== 'done') wfp.setStepState(idGate, s.step_code, { state: s.step_code === 'imgd_rakuten' ? 'skip' : 'done' }, 'admin', ADMIN);
  }
  check('画像承認まで終われば画像の理由は消える',
    !(listing.buildItemPayload(db, idGate).reasons || []).some((x) => /画像トラック/.test(x)));

  // 後から足した画像工程: 既に楽天へ登録済みの商品だけ done で入る (承認者に「出品済みの承認」をさせない)
  const idListedRk = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-IMG-LISTED', '出品済み', 'listed', 'smoke')`
  ).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-08-20T00:00:00Z')`).run(idListedRk);
  const idNotListed = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-IMG-OPEN', '未出品', 'review', 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(idListedRk, { db });
  wfp.progressOf(idNotListed, { db });
  const lateImg = wf.createStep({ label: '画像の最終チェック', track: 'image', image_kind: 'top', role_code: 'image_approver' });
  wfp.ensureProgressForMany(db, [idListedRk, idNotListed]);
  check('後から足した画像工程: 楽天登録済みの商品は done で入る',
    wfp.progressOf(idListedRk, { db }).image.find((s) => s.step_code === lateImg)?.state === 'done');
  check('後から足した画像工程: 未出品の商品は todo で入る',
    wfp.progressOf(idNotListed, { db }).image.find((s) => s.step_code === lateImg)?.state === 'todo');
  const lateMain = wf.createStep({ label: '本流の追加工程', track: 'main', role_code: 'registrar' });
  wfp.ensureProgressForMany(db, [idListedRk]);
  check('後から足した本流工程は楽天登録済みでも todo (例外は画像トラックだけ)',
    wfp.progressOf(idListedRk, { db }).main.find((s) => s.step_code === lateMain)?.state === 'todo');
  wf.updateStep(lateImg, { active: false });
  wf.updateStep(lateMain, { active: false });
}

// ─── モール別の展開状況 (2026-08-23 中原さん: 出品・展開はモールごと) ───
const ms = await import('../lib/mall-status.js');
{
  check('モールは6つ (Amazon は含めない)', ms.MALLS.length === 6, ms.MALLS.map((m) => m.code).join(','));
  check('LINEギフトが入っている', ms.MALLS.some((m) => m.code === 'linegift'));
  check('Amazon は入れない', !ms.MALLS.some((m) => m.code === 'amazon'));

  const id = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-MALL', 'モールテスト', 'approved', 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(id, { db });
  // 出品・展開工程に担当を置くと、モールの初期担当がそこから入る
  wfp.setStepState(id, 'listing', { assignee_id: wfOkawaId }, 'admin', ADMIN);

  const st = ms.mallStatusOf(id, { db });
  check('モール行が自己修復で作られる', st.list.length === 6);
  check('初期状態は未着手', st.list.every((m) => m.state === 'todo'));
  check('モールの担当は出品・展開工程の担当を引き継ぐ', st.list.every((m) => m.assignee_id === wfOkawaId));
  check('並びは MALLS 定義どおり', st.list[0].code === 'rakuten' && st.list[5].code === 'linegift');

  // 楽天出品の成功で楽天モールが自動で完了になる
  check('楽天出品を反映できる', ms.markRakutenListed(db, id, { itemUrl: 'https://item.rakuten.co.jp/b-faith/wf-mall/', actor: 'smoke' }) === true);
  const st2 = ms.mallStatusOf(id, { db });
  check('楽天が掲載済になる', st2.list.find((m) => m.code === 'rakuten').state === 'done');
  check('掲載日が入る', !!st2.list.find((m) => m.code === 'rakuten').listed_at);
  check('まだ工程は完了しない (他モールが残る)',
    wfp.progressOf(id, { db }).main.find((s) => s.step_code === 'listing').state !== 'done');

  // URL の検証
  let urlErr = null;
  try { ms.setMallState(id, 'yahoo', { item_url: 'javascript:alert(1)' }, 'admin', ADMIN); } catch (e) { urlErr = e; }
  check('掲載URLは http(s) だけ通す', urlErr?.status === 400, urlErr?.message || '例外が出ていない');
  check('http(s) の掲載URLは保存できる',
    ms.setMallState(id, 'yahoo', { item_url: 'https://store.shopping.yahoo.co.jp/b-faith/wf-mall.html' }, 'admin', ADMIN).changed === true);

  let mallErr = null;
  try { ms.setMallState(id, 'amazon', { state: 'done' }, 'admin', ADMIN); } catch (e) { mallErr = e; }
  check('未知のモールコードは弾く', mallErr?.status === 400);

  // 権限 (工程と同じ約束)
  let permErr = null;
  try { ms.setMallState(id, 'yahoo', { state: 'done' }, 'tanaka', { isAdmin: false, actorStaffId: wfTanakaId }); } catch (e) { permErr = e; }
  check('他人が担当のモールは動かせない', permErr?.status === 403, permErr?.message || '例外が出ていない');
  check('担当者本人は動かせる',
    ms.setMallState(id, 'yahoo', { state: 'done' }, 'okawa', { isAdmin: false, actorStaffId: wfOkawaId }).changed === true);

  // 楽観ロック
  const v0 = ms.mallStatusOf(id, { db }).list.find((m) => m.code === 'aupay').version;
  const r1 = ms.setMallState(id, 'aupay', { state: 'doing' }, 'admin', ADMIN);
  check('モールも版数が上がる', r1.version === v0 + 1);
  let mlConflict = null;
  try { ms.setMallState(id, 'aupay', { state: 'done', expected_version: v0 }, 'admin', ADMIN); } catch (e) { mlConflict = e; }
  check('古い版数のモール更新を 409 で弾く', mlConflict?.status === 409);

  // 全モールが決着すると工程「出品・展開」が自動で完了になる
  ms.setMallState(id, 'aupay', { state: 'done' }, 'admin', ADMIN);
  ms.setMallState(id, 'mercari', { state: 'done' }, 'admin', ADMIN);
  ms.setMallState(id, 'qoo10', { state: 'skip' }, 'admin', ADMIN);
  const last = ms.setMallState(id, 'linegift', { state: 'done' }, 'admin', ADMIN);
  check('最後のモールで工程完了が返る', last.listingCompleted === true);
  check('工程「出品・展開」が完了になる',
    wfp.progressOf(id, { db }).main.find((s) => s.step_code === 'listing').state === 'done');
  // セット検討がまだ残っているので、この時点では全工程完了ではない
  check('出品が終わってもセット検討が残る', wfp.progressOf(id, { db }).current?.step_code === 'set_review');
  wfp.setStepState(id, 'set_review', { state: 'done' }, 'admin', ADMIN);
  check('全工程が終わってボードの完了列へ', wfp.progressOf(id, { db }).mainDone === true);
  check('対象外は掲載済に数えない', ms.mallStatusOf(id, { db }).doneCount === 5);
  check('決着済みの判定', ms.mallStatusOf(id, { db }).allSettled === true);

  // モール側が正。決着が崩れたら工程も開き直す (「完了なのに未展開が残る」を作らない)
  ms.setMallState(id, 'linegift', { state: 'todo' }, 'admin', ADMIN);
  check('モールを戻すと出品・展開の完了も取り消される',
    wfp.progressOf(id, { db }).main.find((s) => s.step_code === 'listing').state === 'todo');
  check('取り消しがイベントに残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND detail LIKE '%完了を取り消し%'`).get(id).c === 1);
  // 状態セレクトから直接完了にはできない (モール側が正)
  let directDone = null;
  try { wfp.setStepState(id, 'listing', { state: 'done' }, 'admin', ADMIN); } catch (e) { directDone = e; }
  check('未展開モールがあるうちは工程を直接完了にできない', directDone?.status === 400, directDone?.message || '例外が出ていない');
  // モール行がまだ作られていないドラフトでも直接完了させない (Codex R3: 未完了0件の穴)
  const idFresh = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-MALL-FRESH', 'モール未初期化', 'approved', 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(idFresh, { db });
  check('モール行が無い商品も直接完了にできない',
    db.prepare('SELECT COUNT(*) AS c FROM draft_mall_status WHERE draft_id = ?').get(idFresh).c === 0);
  let freshDone = null;
  try { wfp.setStepState(idFresh, 'listing', { state: 'done' }, 'admin', ADMIN); } catch (e) { freshDone = e; }
  check('未初期化でも「6件残っています」で止まる',
    freshDone?.status === 400 && /6 件/.test(freshDone.message), freshDone?.message || '例外が出ていない');
  // 廃止済みのモールコードで行数だけ埋めても通らない (Codex R4)
  for (const code of ['old_mall_a', 'old_mall_b', 'old_mall_c', 'old_mall_d', 'old_mall_e', 'old_mall_f']) {
    db.prepare(`INSERT INTO draft_mall_status (draft_id, mall, state) VALUES (?, ?, 'done')`).run(idFresh, code);
  }
  let staleDone = null;
  try { wfp.setStepState(idFresh, 'listing', { state: 'done' }, 'admin', ADMIN); } catch (e) { staleDone = e; }
  check('廃止済みモールの行で件数を埋めても完了にできない',
    staleDone?.status === 400 && /6 件/.test(staleDone.message), staleDone?.message || '例外が出ていない');
  db.prepare(`DELETE FROM draft_mall_status WHERE draft_id = ? AND mall LIKE 'old_mall_%'`).run(idFresh);
  ms.setMallState(id, 'linegift', { state: 'done' }, 'admin', ADMIN);
}

// ─── セット商品の派生ドラフト (2026-08-23) ───
const sd = await import('../services/set-derive.js');
let wfSetDraftId = null;
let wfSetParentId = null;
{
  const parentId = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, price, jan_code, official_url, own_brand, created_by)
     VALUES ('WF-SET-P', 'セット元商品', 'approved', 1980, '4901234567894', 'https://example.com/p', 1, 'smoke')`
  ).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, 'サイズ', '10cm', 0)`).run(parentId);
  db.prepare(`INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, '素材', 'ステンレス', 1)`).run(parentId);
  db.prepare(`INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, '内容量', '50ml', 2)`).run(parentId);
  db.prepare(`INSERT INTO draft_reference_urls (draft_id, url, sort) VALUES (?, 'https://example.com/ref', 0)`).run(parentId);
  db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json) VALUES (?, '565004', '[]')`).run(parentId);
  db.prepare(`INSERT INTO draft_ai_outputs (draft_id, kind, content) VALUES (?, 'rakuten_title', '単品のタイトル')`).run(parentId);
  // 商品ページ表記: セットで変わるもの (内容量・サイズ・食品表示) が引き継がれないことを見る
  db.prepare(`
    INSERT INTO draft_page_info (draft_id, product_type, content_volume, size_text, ingredients,
      seller_name, food_name, food_ingredients, food_expiry, food_storage)
    VALUES (?, 'food', '50ml', '10cm×5cm', '水、香料', '株式会社B-Faith', '清涼飲料水', '果糖ぶどう糖液糖', 'ラベルに記載', '直射日光を避けて保存')
  `).run(parentId);
  wfp.progressOf(parentId, { db });

  // 親の「セット商品作成検討」を閉じる操作なので、版数と権限文脈が要る
  const setReviewVersion = () => wfp.progressOf(parentId, { db }).main.find((x) => x.step_code === 'set_review').version;
  let noPermErr = null;
  try {
    sd.createSetDraft(parentId, { mode: 'ai', parent_step_version: setReviewVersion() }, 'smoke',
      { isAdmin: false, actorStaffId: null });
  } catch (e) { noPermErr = e; }
  check('担当者でない人はセットを作れない', noPermErr?.status === 403, noPermErr?.message || '例外が出ていない');
  check('作れなかったときは派生ドラフトも残らない',
    db.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE parent_draft_id = ?`).get(parentId).c === 0);

  const r = sd.createSetDraft(parentId, { mode: 'ai', members: [{ ne_code: 'WF-SET-P', qty: 2 }], parent_step_version: setReviewVersion() }, 'smoke', ADMIN);
  wfSetDraftId = r.draftId; wfSetParentId = parentId;
  check('仮コードで作られる', r.neCode === 'SET-WF-SET-P-01', r.neCode);
  const set1 = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(r.draftId);
  check('仮コードのフラグが立つ', set1.provisional_code === 1);
  check('親が記録される', set1.parent_draft_id === parentId);
  check('商品名が自動で付く', /セット/.test(set1.name), set1.name);

  // コピーする/しないの切り分け (ここを間違えると直し忘れが一番危ない)
  check('売価はコピーしない', set1.price == null);
  check('JANはコピーしない', set1.jan_code == null);
  check('公式URLはコピーする', set1.official_url === 'https://example.com/p');
  check('自社商品フラグはコピーする', set1.own_brand === 1);
  check('仕様表: 数量に依存しない行はコピーする',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_specs WHERE draft_id = ? AND spec_key = '素材'`).get(r.draftId).c === 1);
  check('仕様表: サイズ・内容量はコピーしない (2個セットに単品の値が残る)',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_specs WHERE draft_id = ? AND spec_key IN ('サイズ','内容量')`).get(r.draftId).c === 0);
  check('落とした仕様表の件数をイベントに残す',
    /仕様表 2 行/.test(db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'created_from_parent'`).get(r.draftId)?.detail || ''));
  check('参考URLはコピーする',
    db.prepare('SELECT COUNT(*) AS c FROM draft_reference_urls WHERE draft_id = ?').get(r.draftId).c === 1);
  check('楽天ジャンルはコピーする',
    db.prepare('SELECT genre_id FROM draft_rakuten WHERE draft_id = ?').get(r.draftId)?.genre_id === '565004');
  check('画像はコピーしない',
    db.prepare('SELECT COUNT(*) AS c FROM draft_images WHERE draft_id = ?').get(r.draftId).c === 0);
  check('AIモードでは説明文をコピーしない',
    db.prepare('SELECT COUNT(*) AS c FROM draft_ai_outputs WHERE draft_id = ?').get(r.draftId).c === 0);

  // 工程の開始位置
  const sp = wfp.progressOf(r.draftId, { db });
  check('AIモードは AI情報入力待ち から', sp.current?.step_code === 'ai_generate');
  check('セットの画像トラックは未着手', sp.image.every((s) => s.state === 'todo'));
  check('構成が保存される',
    db.prepare('SELECT qty FROM draft_set_members WHERE set_draft_id = ?').get(r.draftId).qty === 2);
  check('親のセット検討工程が完了する',
    wfp.progressOf(parentId, { db }).main.find((s) => s.step_code === 'set_review').state === 'done');
  check('親のイベントに記録が残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'set_draft_created'`).get(parentId).c === 1);

  // copy モードは説明文を持って商品説明確認から
  const r2 = sd.createSetDraft(parentId, { mode: 'copy', members: [{ ne_code: 'WF-SET-P', qty: 3 }], parent_step_version: setReviewVersion() }, 'smoke', ADMIN);
  check('2件目は連番になる', r2.neCode === 'SET-WF-SET-P-02', r2.neCode);
  check('copyモードは説明文をコピーする',
    db.prepare('SELECT content FROM draft_ai_outputs WHERE draft_id = ? AND kind = ?').get(r2.draftId, 'rakuten_title')?.content === '単品のタイトル');
  check('copyモードは 商品説明確認 から', wfp.progressOf(r2.draftId, { db }).current?.step_code === 'desc_review');

  // 検証
  let modeErr = null;
  try { sd.createSetDraft(parentId, { mode: 'unknown' }, 'smoke'); } catch (e) { modeErr = e; }
  check('作り方の指定を検証する', modeErr?.status === 400);
  let qtyErr = null;
  try { sd.createSetDraft(parentId, { mode: 'ai', members: [{ ne_code: 'X', qty: 0 }] }, 'smoke'); } catch (e) { qtyErr = e; }
  check('個数を検証する', qtyErr?.status === 400);
  let fracErr = null;
  try { sd.createSetDraft(parentId, { mode: 'ai', members: [{ ne_code: 'X', qty: 1.5 }] }, 'smoke'); } catch (e) { fracErr = e; }
  check('小数の個数は黙って丸めず弾く', fracErr?.status === 400, fracErr?.message || '例外が出ていない');
  let nestErr = null;
  try { sd.createSetDraft(r.draftId, { mode: 'ai' }, 'smoke'); } catch (e) { nestErr = e; }
  check('セットからさらにセットは作れない', nestErr?.status === 400);

  // 出品ゲート: 仮コードのままでは出品できない
  const payload = listing.buildItemPayload(db, r.draftId);
  check('仮コードのままでは出品を止める',
    (payload.reasons || []).some((x) => /商品コードが仮のまま/.test(x)),
    JSON.stringify(payload.reasons || []).slice(0, 200));

  // 商品ページ表記のコピー範囲 (Codex R1 high: 数量で変わるものと食品表示は引き継がない)
  const spi = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(r.draftId);
  check('ページ表記: 区分はコピーする', spi?.product_type === 'food');
  check('ページ表記: 販売者はコピーする', spi?.seller_name === '株式会社B-Faith');
  check('ページ表記: 成分はコピーする', spi?.ingredients === '水、香料');
  check('ページ表記: 内容量はコピーしない (50mlの2個セットが50mlになる)', spi?.content_volume == null);
  check('ページ表記: サイズはコピーしない', spi?.size_text == null);
  check('ページ表記: 食品表示はコピーしない (法定表示は人が確認する)',
    spi?.food_name == null && spi?.food_ingredients == null && spi?.food_expiry == null && spi?.food_storage == null);

  // 仮コードのまま外に漏れない (Notion カード / モール完了 / 工程の自動完了)
  const setDraftRow = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(r.draftId);
  check('仮コードは Notion カードを作らせない', dbmod.canWriteToNotion(setDraftRow) === false);
  let mallProv = null;
  try { ms.setMallState(r.draftId, 'yahoo', { state: 'done' }, 'admin', ADMIN); } catch (e) { mallProv = e; }
  check('仮コードのままモールを掲載済にできない', mallProv?.status === 400, mallProv?.message || '例外が出ていない');
  // 全部 skip にしても工程は閉じない (skip 経由の抜け道を塞ぐ)
  for (const m of ms.MALLS) ms.setMallState(r.draftId, m.code, { state: 'skip' }, 'admin', ADMIN);
  check('仮コードのままでは出品・展開工程が閉じない',
    wfp.progressOf(r.draftId, { db }).main.find((s) => s.step_code === 'listing').state !== 'done');

  // 本コードへの差し替え: NE に無ければ仮フラグは残る → NE に現れたら自動で確定する
  db.prepare(`UPDATE product_drafts SET ne_code = 'WF-SET-REAL' WHERE id = ?`).run(r.draftId);
  const notInNe = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(r.draftId);
  check('NE未確認なら仮フラグは残る', notInNe.provisional_code === 1);
  check('NE未確認でも出品は止まる',
    (listing.buildItemPayload(db, r.draftId).reasons || []).some((x) => /NE商品マスタに見つかりません/.test(x)));
  check('NEに無いうちは自動確定しない', sd.reconcileProvisionalCode(db, { ...notInNe }) === false);
  // NE 商品マスタに現れたら確定する
  db.prepare(`
    INSERT INTO mirror_products
      (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
    VALUES (99401, 'WF-SET-REAL', 'セット商品 実コード', '1', '取扱中', '1', 'WF-SET-REAL', '2026-08-23T00:00:00Z')
  `).run();
  check('NEに現れたら自動で確定する', sd.reconcileProvisionalCode(db, { ...notInNe }) === true);
  check('確定後は出品ゲートが開く',
    !(listing.buildItemPayload(db, r.draftId).reasons || []).some((x) => /商品コード/.test(x)));
  check('確定後は Notion カードも作れる',
    dbmod.canWriteToNotion(db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(r.draftId)) === true);
  db.prepare(`DELETE FROM mirror_products WHERE product_id = 99401`).run();

  // 親側・セット側の表示用データ
  check('親からセット一覧が引ける', sd.setDraftsOf(db, parentId).length === 2);
  const info = sd.setInfoOf(db, r.draftId);
  check('セットから親が引ける', info?.parent?.id === parentId);
  check('セットの構成が引ける', info.members[0].qty === 2);
  check('単品では setInfo が null', sd.setInfoOf(db, parentId) === null);
}

// ─── かんばんボード (2026-08-23) ───
{
  const idHold = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-HOLD', '保留テスト', 'on_hold', 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(idHold, { db });

  // 停滞の再現: WF-STALL は上の一括修復テストで作り直したので、改めて前工程の完了日時をずらす
  db.prepare(`
    UPDATE draft_step_progress SET done_at = ?
    WHERE step_code = 'basic_info'
      AND draft_id = (SELECT id FROM product_drafts WHERE ne_code = 'WF-STALL')
  `).run(new Date(Date.now() - 5 * 86400000).toISOString());

  const b = wfp.boardData(db, {});
  check('ボード(本流): 6列', b.view === 'main' && b.columns.length === 6, `view=${b.view} cols=${b.columns.length}`);
  const onBoard = new Set(b.columns.flatMap((c) => c.cards.map((x) => x.id)).concat(b.doneCards.map((x) => x.id)));
  check('ボード: 保留の商品は載せない', !onBoard.has(idHold));
  check('ボード: 工程テストの商品が載る', onBoard.has(wfDraftId));
  check('ボード: カードは1商品につき本流の1列だけ',
    b.columns.reduce((n, c) => n + c.cards.filter((x) => x.id === wfDraftId).length, 0) === 1);
  {
    // 本流カードに画像 (TOP/詳細) の進捗が常時付く = ボードを行き来せずに読める (2重管理の解消)
    const card = b.columns.flatMap((c) => c.cards).find((x) => x.id === wfDraftId);
    check('ボード: カードに TOP/詳細 のステッパーが付く (詳細は v2 の 10 点)',
      card.image?.top?.steps?.length === 4 && card.image?.detail?.steps?.length === 10,
      JSON.stringify(card.image || null).slice(0, 120));
  }

  // 画像ビュー: 列は段階 (依頼→撮影依頼中→制作→登録→承認) でまとまり、カードは 商品×種別
  const bi = wfp.boardData(db, { view: 'image' });
  check('ボード(画像): 12列 (依頼は TOP/詳細 共通・TOP の制作/登録はデザイン修正の後・承認は社内確認の後)',
    bi.view === 'image' && bi.columns.length === 12
    && bi.columns.map((c) => c.key).join(',') === 'request,compose,material,ai,design,production,register,review,approve,amazon,rakuten,aplus',
    `cols=${bi.columns.map((c) => c.key).join(',')}`);
  {
    const kindsOf = (id) => bi.columns.flatMap((c) => c.cards.filter((x) => x.id === id).map((x) => x.kind));
    check('ボード(画像): 同じ商品が TOP と詳細で別カードになる',
      kindsOf(wfDraftId).sort().join(',') === 'detail,top', kindsOf(wfDraftId).join(','));
    // 詳細対象外の商品は詳細カードを出さない
    const idNoDetail = Number(db.prepare(
      `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-BOARD-EX', '詳細なし', 'review', 'smoke')`
    ).run().lastInsertRowid);
    wfp.progressOf(idNoDetail, { db });
    wfp.setDetailImagesExcluded(idNoDetail, true, 'admin', ADMIN);
    const bi2 = wfp.boardData(db, { view: 'image' });
    const kinds2 = bi2.columns.flatMap((c) => c.cards.filter((x) => x.id === idNoDetail).map((x) => x.kind));
    check('ボード(画像): 詳細対象外の商品は TOP カードだけ', kinds2.join(',') === 'top', kinds2.join(','));
    // TOP全skip (ガードすり抜け) を完了列に見せない — 出品ゲートは拒否するので表示と食い違う
    wfp.setDetailImagesExcluded(idNoDetail, true, 'admin', ADMIN);
    db.prepare(`UPDATE draft_step_progress SET state = 'skip' WHERE draft_id = ? AND step_code LIKE 'img_%_top'`).run(idNoDetail);
    const bi3 = wfp.boardData(db, { view: 'image' });
    check('ボード(画像): TOP全対象外は完了列に出さない',
      !bi3.doneCards.some((c) => c.id === idNoDetail)
      && !bi3.columns.some((c) => c.cards.some((x) => x.id === idNoDetail)));
    db.prepare(`UPDATE draft_step_progress SET state = 'todo' WHERE draft_id = ? AND step_code LIKE 'img_%_top'`).run(idNoDetail);
  }

  // 停滞しているカードを先頭に出す (打ち手が要るものを埋もれさせない)
  const aiCol = b.columns.find((c) => c.code === 'ai_generate');
  check('ボード: 停滞カードが列の先頭', aiCol.cards.length > 0 && aiCol.cards[0].stalledDays >= 3,
    JSON.stringify(aiCol.cards.map((c) => c.stalledDays)));

  // 担当者で絞る (本流ビュー: 本流か画像のどこかがその人のボールなら残す)
  const mine = wfp.boardData(db, { assigneeId: wfTanakaId });
  const mineCards = mine.columns.flatMap((c) => c.cards);
  check('ボード: 担当者で絞ると全部その人のボールになる',
    mineCards.length > 0 && mineCards.every((c) => c.current?.assignee_id === wfTanakaId
      || c.image?.top?.current?.assignee_id === wfTanakaId
      || c.image?.detail?.current?.assignee_id === wfTanakaId),
    `件数=${mineCards.length}`);
  check('ボード: 絞り込みは全件より少ない', mineCards.length < onBoard.size * 2, `${mineCards.length} / ${onBoard.size}`);
  // 画像ビューの絞り込みは**その種別の**現在工程が基準 (他トラック一致で無関係な列に出さない)
  const mineImg = wfp.boardData(db, { view: 'image', assigneeId: wfTanakaId });
  check('ボード(画像): 絞り込みは種別自身の担当だけ',
    mineImg.columns.flatMap((c) => c.cards).every((c) => c.kindCurrent?.assignee_id === wfTanakaId));

  // 未割り当てだけ (システム工程は「未割り当て」に数えない)
  const un = wfp.boardData(db, { unassignedOnly: true });
  check('ボード: 未割り当て絞り込みでAI待ちを拾わない',
    !un.columns.find((c) => c.code === 'ai_generate').cards.some((c) => c.current?.role_code == null));

  check('ボード: 件数が返る', b.total >= 3 && b.truncated === false);
}

// ─── 画像トラック TOP/詳細 分割の移行 (2026-08-24) ───
{
  // #888/#890 デプロイ済み環境を再現: 旧一本トラックの工程 + 進捗行を作って移行を走らせる
  db.prepare(`
    INSERT OR IGNORE INTO ph_steps (code, label, track, role_code, sort, builtin, active)
    VALUES ('img_request', '画像制作の依頼', 'image', 'image', 10, 1, 1),
           ('img_approve', '画像承認', 'image', 'image_approver', 40, 1, 1)
  `).run();
  const mkMig = (code) => Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES (?, ?, 'review', 'smoke')`
  ).run(code, `移行 ${code}`).lastInsertRowid);
  const idHuman = mkMig('WF-MIG-HUMAN');   // 人が旧トラックを done にした
  const idEst = mkMig('WF-MIG-EST');       // 初回推定で done (工程導入前から画像あり)
  const idRk = mkMig('WF-MIG-RK');         // 楽天登録済み
  const idOpen = mkMig('WF-MIG-OPEN');     // 作業中
  db.prepare(`INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-08-20T00:00:00Z')`).run(idRk);
  const insOld = db.prepare(`
    INSERT INTO draft_step_progress (draft_id, step_code, state, done_at, done_by)
    VALUES (?, 'img_request', ?, ?, ?)
  `);
  insOld.run(idHuman, 'done', '2026-08-23T00:00:00Z', 'okawa@b-faith.biz');
  insOld.run(idEst, 'done', '2026-08-23T00:00:00Z', 'migration');
  insOld.run(idRk, 'done', '2026-08-23T00:00:00Z', 'okawa@b-faith.biz');
  insOld.run(idOpen, 'doing', null, null);
  check('移行が走る (旧工程が active のとき)', dbmod.migrateImageKindSplit(db) === true);
  const st = (id, code) => db.prepare(
    'SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(id, code)?.state;
  check('移行: TOP側は旧状態を引き継ぐ', st(idHuman, 'img_request_top') === 'done' && st(idOpen, 'img_request_top') === 'doing');
  // 詳細 v1 工程は v2 以降シードされないので、分割移行は詳細側をコピーしない (FK)。詳細は v2 の初回推定に委ねる
  wfpEarly.ensureProgress(db, idHuman); wfpEarly.ensureProgress(db, idEst); wfpEarly.ensureProgress(db, idRk);
  check('移行: 人が done にした商品の詳細側 (v2) は todo (作っていない詳細画像を承認済みにしない)',
    st(idHuman, 'img_request_detail') === undefined && st(idHuman, 'imgd_request') === 'todo');
  check('移行: 初回推定 done (画像が既にあっただけ) も詳細側は todo (Codex R1 critical)',
    st(idEst, 'imgd_request') === 'todo');
  check('移行: 楽天登録済みだけ詳細側も done', st(idRk, 'imgd_request') === 'done');
  check('移行: 旧工程は無効化される',
    db.prepare(`SELECT COUNT(*) AS c FROM ph_steps WHERE code IN ('img_request','img_approve') AND active = 1`).get().c === 0);
  check('移行: 再実行は no-op (冪等)', dbmod.migrateImageKindSplit(db) === false);
  // 旧工程・旧進捗行は本番と同じく残置する (active=0 で全クエリから見えない)。
  // DELETE しないのは draft_step_progress → ph_steps の FK を壊さないため
}

// ─── status の工程からの導出 (PR4 2026-08-24) ───
{
  const wfp = wfpEarly;
  const ms2 = await import('../lib/mall-status.js');
  const ADMIN = { isAdmin: true };
  const statusOf = (id) => db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(id).status;

  // 材料が揃った draft (基本情報の完了ゲートを通れる)
  const mkFull = (code) => {
    const id = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, official_url, created_by)
      VALUES (?, ?, 'https://example.com/item', 'smoke')
    `).run(code, `導出 ${code}`).lastInsertRowid);
    db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, ?)`).run(id, `drv-${code}`);
    return id;
  };

  const idD = mkFull('DRV-1');
  check('導出: 工程行なし = draft', wfp.deriveDraftStatus(db, idD) === 'draft');

  // 材料が無い draft は「基本情報入力」を完了にできない (AIキュー入口のゲート)
  const idBare = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-BARE', '材料なし', 'smoke')`
  ).run().lastInsertRowid);
  let gateErr = null;
  try { wfp.setStepState(idBare, 'basic_info', { state: 'done' }, 'smoke', ADMIN); } catch (e) { gateErr = e; }
  check('導出: 材料なしでは基本情報を完了にできない (400+理由)',
    gateErr?.status === 400 && String(gateErr.message).includes('足りません'), gateErr?.message || '例外が出ていない');
  check('導出: ゲートで止まった draft は draft のまま', statusOf(idBare) === 'draft');

  // 本流工程を進めると status が導出で進む
  wfp.setStepState(idD, 'basic_info', { state: 'done' }, 'smoke', ADMIN);
  check('導出: 基本情報 done → ready_for_ai (AIキュー入り)', statusOf(idD) === 'ready_for_ai');
  check('導出: AIキューにも載る', dbmod.listGenerationQueue(db).some((q) => q.id === idD));
  wfp.setStepState(idD, 'ai_generate', { state: 'done' }, 'smoke', ADMIN);
  check('導出: AI生成 done → review', statusOf(idD) === 'review');
  wfp.setStepState(idD, 'desc_review', { state: 'done' }, 'smoke', ADMIN);
  check('導出: 説明確認だけでは review のまま (タイトル確認待ち)', statusOf(idD) === 'review');
  wfp.setStepState(idD, 'title_approve', { state: 'done' }, 'smoke', ADMIN);
  check('導出: 説明+タイトル確認 done → approved', statusOf(idD) === 'approved');
  check('導出: set_review は status を左右しない (approved のまま検討中)',
    db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_review'`).get(idD).state === 'todo');

  // 楽天出品 (モール done) → listed
  ms2.markRakutenListed(db, idD, { actor: 'smoke' });
  check('導出: 楽天モール done → listed', statusOf(idD) === 'listed');

  // 全モール決着 → 工程「出品・展開」が閉じて expanded
  for (const m of ms2.MALLS.map((x) => x.code).filter((c) => c !== 'rakuten')) {
    ms2.setMallState(idD, m, { state: 'done' }, 'smoke', ADMIN);
  }
  check('導出: 全モール決着 → expanded', statusOf(idD) === 'expanded');

  // モールの決着が崩れる → 工程が開き直り listed へ戻る (モール側が正)
  ms2.setMallState(idD, 'yahoo', { state: 'todo' }, 'smoke', ADMIN);
  check('導出: モール決着が崩れると listed へ戻る (楽天は done のまま)', statusOf(idD) === 'listed');

  // AI生成をやり直す = 工程を開けるとキューへ戻る
  wfp.setStepState(idD, 'ai_generate', { state: 'todo' }, 'smoke', ADMIN);
  check('導出: AI生成を開け直すと ready_for_ai (再生成キュー入り)', statusOf(idD) === 'ready_for_ai');
  wfp.setStepState(idD, 'ai_generate', { state: 'done' }, 'smoke', ADMIN);
  check('導出: 閉じ直すと listed へ復帰 (楽天 done の実態が残っている)', statusOf(idD) === 'listed');

  // 保留中は導出が触らない (再開だけが戻す)
  db.prepare(`UPDATE product_drafts SET status = 'on_hold' WHERE id = ?`).run(idD);
  wfp.setStepState(idD, 'desc_review', { state: 'todo' }, 'smoke', ADMIN);
  check('導出: 保留中は工程を動かしても status を書き換えない', statusOf(idD) === 'on_hold');
  check('導出: 保留中の deriveDraftStatus は再開後の値を返す (review)', wfp.deriveDraftStatus(db, idD) === 'review');
  wfp.setStepState(idD, 'desc_review', { state: 'done' }, 'smoke', ADMIN);
  db.prepare(`UPDATE product_drafts SET status = 'listed' WHERE id = ?`).run(idD);

  // ─── 切替バックフィル (一回きり・遅延) ───
  db.prepare(`DELETE FROM ph_intake_state WHERE key = 'status_derive_backfilled'`).run();
  // 食い違い例1: 工程は AI 済みなのに status が古いまま (旧・二重管理のずれ)
  const idLag = mkFull('DRV-LAG');
  wfp.ensureProgress(db, idLag);
  db.prepare(`
    UPDATE draft_step_progress SET state = 'done', done_at = '2026-08-23T00:00:00Z', done_by = 'smoke'
    WHERE draft_id = ? AND step_code IN ('basic_info', 'ai_generate')
  `).run(idLag);
  // 食い違い例2: 旧・手動遷移で listed (draft_rakuten もモール行も無い)
  const idOldListed = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('DRV-OLDLS', '旧listed', 'listed', 'smoke')
  `).run().lastInsertRowid);
  const changed = wfp.maybeBackfillDerivedStatus(db);
  check('バックフィル: 工程が進んでいた draft は status が追いつく (review)', statusOf(idLag) === 'review');
  check('バックフィル: 旧 listed は楽天モール行に根拠が残り listed のまま',
    statusOf(idOldListed) === 'listed'
    && db.prepare(`SELECT state FROM draft_mall_status WHERE draft_id = ? AND mall = 'rakuten'`).get(idOldListed)?.state === 'done',
    `status=${statusOf(idOldListed)}`);
  check('バックフィル: 変更件数が返る', changed >= 1, String(changed));
  check('バックフィル: 2回目は no-op (フラグ済み)', wfp.maybeBackfillDerivedStatus(db) === 0);

  // ─── Codex R1 対応 (退避・再開まわりの穴) ───

  // M1: skip でゲートを迂回できる工程は skip 禁止
  const idSkip = mkFull('DRV-SKIP');
  let skipErr = null;
  try { wfp.setStepState(idSkip, 'basic_info', { state: 'skip' }, 'smoke', ADMIN); } catch (e) { skipErr = e; }
  check('R1対応: basic_info は skip 不可 (材料チェックの迂回防止)', skipErr?.status === 400, skipErr?.message || '例外が出ていない');
  let listSkipErr = null;
  try { wfp.setStepState(idSkip, 'listing', { state: 'skip' }, 'smoke', ADMIN); } catch (e) { listSkipErr = e; }
  check('R1対応: listing は skip 不可 (モール別の対象外を使う)', listSkipErr?.status === 400, listSkipErr?.message || '例外が出ていない');

  // H2 (lib 単体): 材料が壊れた draft は deriveWithGateCheck が basic_info を差し戻す
  const idGate = mkFull('DRV-GATE2');
  wfp.setStepState(idGate, 'basic_info', { state: 'done' }, 'smoke', ADMIN);
  db.prepare(`DELETE FROM draft_images WHERE draft_id = ?`).run(idGate);
  db.prepare(`UPDATE product_drafts SET official_url = NULL WHERE id = ?`).run(idGate);
  check('R1対応: deriveWithGateCheck は材料が無ければ draft へ差し戻す',
    wfp.deriveWithGateCheck(db, idGate, 'smoke') === 'draft'
    && db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'basic_info'`).get(idGate).state === 'todo');

  // M2: 退避中で工程未生成の商品は、監査ログの退避前 status から seed される
  const idHold = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('DRV-HOLD', '退避中', 'on_hold', 'smoke')
  `).run().lastInsertRowid);
  dbmod.logEvent(db, idHold, 'status_changed', 'approved -> on_hold', 'smoke');
  wfp.ensureProgress(db, idHold);
  check('R1対応: 退避中の初回 seed は退避前 status (approved) から推定する',
    db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'title_approve'`).get(idHold).state === 'done'
    && wfp.deriveDraftStatus(db, idHold) === 'approved');
}

// ─── 退避・再開の HTTP ルート (PR4: claim 無効化 / ゲート再検証 / CAS) ───
{
  const wfp = wfpEarly;
  const express = (await import('express')).default;
  const routerMod = await import('../router.js');
  const app = express();
  // セッションを偽装して直接マウント (本番は server.js の requireAppAccess を通る)
  // 一部のテストは一般ユーザーとして叩く (smokeSession を差し替える)
  let smokeSession = { email: 'smoke@b-faith.biz', displayName: 'smoke', role: 'admin' };
  app.use((req, res, next) => { req.session = smokeSession; next(); });
  app.use('/ph', routerMod.default);
  const server = app.listen(0);
  const base = `http://127.0.0.1:${server.address().port}/ph`;
  const call = async (method, p, body) => {
    const res = await fetch(base + p, {
      method, headers: { 'Content-Type': 'application/json' },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    return { status: res.status, json: await res.json() };
  };

  const idE = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, official_url, created_by)
    VALUES ('DRV-ESC', '退避テスト', 'https://example.com/item', 'smoke')
  `).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'drv-esc-img')`).run(idE);
  wfp.setStepState(idE, 'basic_info', { state: 'done' }, 'smoke', { isAdmin: true });
  // AI ランナーが claim 中という状況を再現
  db.prepare(`UPDATE product_drafts SET generation_claim_run_id = 'runOld', generation_claim_until = '2999-01-01T00:00:00Z' WHERE id = ?`).run(idE);

  let r = await call('POST', `/api/drafts/${idE}/status`, { to: 'on_hold' });
  const afterHold = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(idE);
  check('退避: on_hold になり AI claim が無効化される (Codex R1 High)',
    r.status === 200 && afterHold.status === 'on_hold'
    && afterHold.generation_claim_run_id == null && afterHold.generation_claim_until == null,
    JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idE}/status`, { to: 'on_hold' });
  check('退避: 二重の保留は 400', r.status === 400);

  // 退避中に材料を壊す → 再開でゲート再検証されて draft へ (Codex R1 High)
  db.prepare(`DELETE FROM draft_images WHERE draft_id = ?`).run(idE);
  db.prepare(`UPDATE product_drafts SET official_url = NULL WHERE id = ?`).run(idE);
  r = await call('POST', `/api/drafts/${idE}/status`, { to: 'resume' });
  check('再開: 材料が壊れていれば ready_for_ai でなく draft に着地する',
    r.status === 200 && r.json.status === 'draft'
    && db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(idE).status === 'draft',
    JSON.stringify(r.json));

  r = await call('POST', `/api/drafts/${idE}/status`, { to: 'resume' });
  check('再開: 退避中でなければ 400', r.status === 400);
  r = await call('POST', `/api/drafts/${idE}/status`, { to: 'review' });
  check('手動遷移の廃止: 導出ステータスへの直接遷移は 400', r.status === 400
    && String(r.json.error || '').includes('自動で決まります'), JSON.stringify(r.json));

  // ─── かんばん D&D 移動 + TOP画像の重要度 (2026-08-24) ───
  const ADMIN2 = { isAdmin: true };
  const statusOf2 = (id) => db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(id).status;
  const stepOf = (id, code) => db.prepare(
    'SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(id, code)?.state;
  const idM = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, official_url, created_by)
    VALUES ('DRV-MOVE', 'D&Dテスト', 'https://example.com/item', 'smoke')
  `).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'drv-move-img')`).run(idM);

  // 前方移動: 基本情報の列 → 商品説明確認の列 (basic_info と ai_generate をまとめて done に)
  wfpEarly.moveBoardCard(idM, { view: 'main', to: 'desc_review', expectedCurrent: 'basic_info' }, 'smoke', ADMIN2);
  check('D&D 前方: 途中の工程がまとめて done になり status も導出される',
    stepOf(idM, 'basic_info') === 'done' && stepOf(idM, 'ai_generate') === 'done' && statusOf2(idM) === 'review');

  // CAS: 掴んだ時点の現在工程がズレていたら 409
  let dndCas = null;
  try { wfpEarly.moveBoardCard(idM, { view: 'main', to: 'title_approve', expectedCurrent: 'basic_info' }, 'smoke', ADMIN2); } catch (e) { dndCas = e; }
  check('D&D CAS: ボードが古ければ 409', dndCas?.status === 409, dndCas?.message || '例外が出ていない');

  // 後方移動: AI情報入力待ちの列へ戻す = ai_generate を開け直す → ready_for_ai (キュー再入)
  wfpEarly.moveBoardCard(idM, { view: 'main', to: 'ai_generate', expectedCurrent: 'desc_review' }, 'smoke', ADMIN2);
  check('D&D 後方: 工程が開き直り status が巻き戻る',
    stepOf(idM, 'ai_generate') === 'todo' && statusOf2(idM) === 'ready_for_ai');

  // 完了列 (本流) はモール未決着なら listing の完了チェックで止まる (全体ロールバック)
  wfpEarly.moveBoardCard(idM, { view: 'main', to: 'set_review', expectedCurrent: 'ai_generate' }, 'smoke', ADMIN2);
  let dndDone = null;
  try { wfpEarly.moveBoardCard(idM, { view: 'main', to: 'done', expectedCurrent: 'set_review' }, 'smoke', ADMIN2); } catch (e) { dndDone = e; }
  check('D&D 完了列 (本流): モール未決着なら 400 で全体ロールバック',
    dndDone?.status === 400 && stepOf(idM, 'set_review') !== 'done', dndDone?.message || '例外が出ていない');

  // ゲート: 材料の無い商品は基本情報の列を跨げない
  const idM2 = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-MOVE2', '材料なし', 'smoke')
  `).run().lastInsertRowid);
  let dndGate = null;
  try { wfpEarly.moveBoardCard(idM2, { view: 'main', to: 'desc_review', expectedCurrent: 'basic_info' }, 'smoke', ADMIN2); } catch (e) { dndGate = e; }
  check('D&D ゲート: 材料なしでは基本情報を跨げない (400)', dndGate?.status === 400, dndGate?.message || '例外が出ていない');

  // 権限: 一般ユーザー (未紐付け) はシステム工程 (AI待ち) を跨げない
  let dndPerm = null;
  try { wfpEarly.moveBoardCard(idM, { view: 'main', to: 'title_approve', expectedCurrent: 'set_review' }, 'x@b-faith.biz', { isAdmin: false, actorStaffId: null }); } catch (e) { dndPerm = e; }
  check('D&D 権限: 後方移動でも他所の工程は権限チェックが効く', dndPerm?.status === 403 || dndPerm?.status === 400, dndPerm?.message || '例外が出ていない');

  // 未割り当て工程の自動引き受け (2026-08-27 田中さん改善案): 一般ユーザーが D&D で跨ぐ工程が
  // 未割り当てなら、本人に担当を付けてから done にする (詳細画面で全工程に「自分が担当する」を押さなくてよい)
  const idM5 = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, official_url, created_by)
    VALUES ('DRV-MOVE5', '自動引き受け', 'https://example.com/item5', 'smoke')
  `).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'drv-move5-img')`).run(idM5);
  wfpEarly.ensureProgress(db, idM5);
  db.prepare(`UPDATE draft_step_progress SET assignee_id = NULL WHERE draft_id = ?`).run(idM5);
  const TANAKA = { isAdmin: false, actorStaffId: wfTanakaId };
  const assigneeOf = (id, code) => db.prepare(
    'SELECT assignee_id FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(id, code)?.assignee_id;
  const versionOf = (id, code) => db.prepare(
    'SELECT version FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(id, code)?.version;
  const eventsOf = (id) => db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'step_changed' ORDER BY id`).all(id).map((r) => r.detail);

  // 一般ユーザーは基本情報 → 商品説明確認 へ直接は動かせない (間の AI待ち = システム工程は admin のみ。特例は作らない)
  let dndSys = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'desc_review', expectedCurrent: 'basic_info' }, 'tanaka', TANAKA); } catch (e) { dndSys = e; }
  check('D&D 自動引き受け: システム工程 (AI待ち) は一般ユーザーでは跨げない (403) + 先行の basic_info もロールバック',
    dndSys?.status === 403 && stepOf(idM5, 'basic_info') === 'todo' && assigneeOf(idM5, 'basic_info') == null, dndSys?.message || '例外が出ていない');
  // 基本情報 → AI待ちの列 (隣) へは動かせる = 未割り当ての basic_info を引き受けて完了 (version は 1 だけ増える・イベント 1 件)
  const v0 = versionOf(idM5, 'basic_info');
  const ev0 = eventsOf(idM5).length;
  let dndClaim = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'ai_generate', expectedCurrent: 'basic_info' }, 'tanaka', TANAKA); } catch (e) { dndClaim = e; }
  check('D&D 自動引き受け: 未割り当て工程を一般ユーザーが跨げる (基本情報 → AI待ち)', dndClaim === null, dndClaim?.message || '');
  check('D&D 自動引き受け: 跨いだ工程は本人担当 + done + version は 1 増',
    stepOf(idM5, 'basic_info') === 'done' && assigneeOf(idM5, 'basic_info') === wfTanakaId && versionOf(idM5, 'basic_info') === v0 + 1);
  const evs = eventsOf(idM5);
  check('D&D 自動引き受け: イベントは 1 件で「自動引き受け」と明記', evs.length === ev0 + 1 && /自動引き受け/.test(evs[evs.length - 1]), JSON.stringify(evs.slice(ev0)));
  check('D&D 自動引き受け: 移動先がシステム工程なら担当は付けない', assigneeOf(idM5, 'ai_generate') == null);

  // AI待ちが済んだ体にして、商品説明確認 → セット検討 へ (title_approve を跨ぐ)
  wfpEarly.setStepState(idM5, 'ai_generate', { state: 'done' }, 'smoke', ADMIN2);
  dndClaim = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'set_review', expectedCurrent: 'desc_review' }, 'tanaka', TANAKA); } catch (e) { dndClaim = e; }
  check('D&D 自動引き受け: 通過工程 2 つを一度に引き受けて done', dndClaim === null
    && stepOf(idM5, 'desc_review') === 'done' && assigneeOf(idM5, 'desc_review') === wfTanakaId
    && stepOf(idM5, 'title_approve') === 'done' && assigneeOf(idM5, 'title_approve') === wfTanakaId, dndClaim?.message || '');
  check('D&D 自動引き受け: 移動先 (いまやる番) も未割り当てなら移動者の担当になる (todo のまま)',
    stepOf(idM5, 'set_review') === 'todo' && assigneeOf(idM5, 'set_review') === wfTanakaId);

  // 後方移動 (開け直し): 本当に未割り当ての工程で検証する
  db.prepare(`UPDATE draft_step_progress SET assignee_id = NULL WHERE draft_id = ? AND step_code = 'title_approve'`).run(idM5);
  dndClaim = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'title_approve', expectedCurrent: 'set_review' }, 'tanaka', TANAKA); } catch (e) { dndClaim = e; }
  check('D&D 自動引き受け: 後方移動も本人の担当で開け直す', dndClaim === null
    && stepOf(idM5, 'title_approve') === 'todo' && assigneeOf(idM5, 'title_approve') === wfTanakaId, dndClaim?.message || '');

  // 途中に他人の担当工程があれば 403 (先行して引き受けた工程もロールバック = 担当を奪わない・中途半端に進めない)
  db.prepare(`UPDATE draft_step_progress SET assignee_id = NULL WHERE draft_id = ? AND step_code = 'title_approve'`).run(idM5);
  db.prepare(`UPDATE draft_step_progress SET assignee_id = ? WHERE draft_id = ? AND step_code = 'set_review'`).run(wfOkawaId, idM5);
  let dndOther = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'listing', expectedCurrent: 'title_approve' }, 'tanaka', TANAKA); } catch (e) { dndOther = e; }
  check('D&D 自動引き受け: 途中に他人の担当工程があれば 403 で全体ロールバック',
    dndOther?.status === 403 && stepOf(idM5, 'title_approve') === 'todo' && assigneeOf(idM5, 'title_approve') == null
    && assigneeOf(idM5, 'set_review') === wfOkawaId,
    dndOther?.message || '例外が出ていない');
  // 担当者に紐づいていないログインユーザーは従来どおり引き受けできない
  let dndNoStaff = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'set_review', expectedCurrent: 'title_approve' }, 'x@b-faith.biz', { isAdmin: false, actorStaffId: null }); } catch (e) { dndNoStaff = e; }
  check('D&D 自動引き受け: 担当者未紐付けのユーザーは 403', dndNoStaff?.status === 403 && stepOf(idM5, 'title_approve') === 'todo', dndNoStaff?.message || '例外が出ていない');
  // boardClaim は D&D 経路の内部オプション。通常の工程 API に body で送っても効かない (Codex R2)
  const adminSession = smokeSession;
  smokeSession = { email: 'tanaka@b-faith.biz', displayName: '田中', role: 'user' };
  try {
    r = await call('POST', `/api/drafts/${idM5}/steps/title_approve`, {
      assignee_id: wfTanakaId, state: 'done', boardClaim: true, expected_version: versionOf(idM5, 'title_approve'),
    });
    check('工程API: body の boardClaim は無視され、未割り当て工程の引き受け+完了は 403', r.status === 403
      && stepOf(idM5, 'title_approve') === 'todo' && assigneeOf(idM5, 'title_approve') == null, JSON.stringify(r.json));
  } finally { smokeSession = adminSession; }

  // 画像ビュー: TOP を 依頼 → 登録 へ (依頼・制作が done)、完了列で承認まで閉じる。
  // idM は画像ありのため初回 seed で TOP側が done 済み (仕様) → 画像なしの別ドラフトで検証する
  const idM3 = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-MOVE3', '画像D&D', 'smoke')
  `).run().lastInsertRowid);
  wfpEarly.ensureProgress(db, idM3);
  wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'top', to: 'register', expectedCurrent: 'img_request_top' }, 'smoke', ADMIN2);
  check('D&D 画像ビュー前方: 段階キーで移動できる',
    stepOf(idM3, 'img_request_top') === 'done' && stepOf(idM3, 'img_production_top') === 'done'
    && stepOf(idM3, 'img_register_top') === 'todo');
  wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'top', to: 'done', expectedCurrent: 'img_register_top' }, 'smoke', ADMIN2);
  check('D&D 画像ビュー完了列: 残り (登録・承認) がまとめて done',
    stepOf(idM3, 'img_register_top') === 'done' && stepOf(idM3, 'img_approve_top') === 'done');
  // 完了列からの差し戻し (Codex R1 medium): expectedCurrent=null が「全工程決着」の CAS 値
  wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'top', to: 'approve', expectedCurrent: null }, 'smoke', ADMIN2);
  check('D&D 完了列から差し戻し: 承認が開き直る', stepOf(idM3, 'img_approve_top') === 'todo');
  let doneCas = null;
  try { wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'top', to: 'production', expectedCurrent: null }, 'smoke', ADMIN2); } catch (e) { doneCas = e; }
  check('D&D 完了カードの CAS: もう完了でなければ 409', doneCas?.status === 409, doneCas?.message || '例外が出ていない');

  // 片方の種別だけ完了した商品がボードから消えない (Codex R2 medium):
  // TOP だけ完了 → TOP カードは完了列・詳細カードは依頼の列に居る
  const idM4 = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-MOVE4', '片側完了', 'smoke')
  `).run().lastInsertRowid);
  wfpEarly.ensureProgress(db, idM4);
  wfpEarly.moveBoardCard(idM4, { view: 'image', kind: 'top', to: 'done', expectedCurrent: 'img_request_top' }, 'smoke', ADMIN2);
  const ib = wfpEarly.boardData(db, { view: 'image' });
  check('画像ビュー: TOPだけ完了 → 完了列に TOP カードが出る (差し戻し可能)',
    ib.doneCards.some((c) => c.id === idM4 && c.kind === 'top'));
  check('画像ビュー: 同じ商品の詳細カードは依頼の列に残る',
    ib.columns.find((c) => c.key === 'request')?.cards.some((c2) => c2.id === idM4 && c2.kind === 'detail') === true);

  // 種別の絞り込み (2026-08-24 中原さん要望): kind 指定で段階列・完了列とも片方だけになる
  const ibTop = wfpEarly.boardData(db, { view: 'image', imageKind: 'top' });
  check('画像ビュー: kind=top は TOP カードだけ (完了列も対象)',
    ibTop.columns.every((c) => c.cards.every((x) => x.kind === 'top'))
    && ibTop.doneCards.every((x) => x.kind === 'top')
    && ibTop.doneCards.some((c) => c.id === idM4));
  const ibDet = wfpEarly.boardData(db, { view: 'image', imageKind: 'detail' });
  check('画像ビュー: kind=detail は商品詳細カードだけ (idM4 の詳細は依頼列に居る)',
    ibDet.columns.every((c) => c.cards.every((x) => x.kind === 'detail'))
    && ibDet.doneCards.every((x) => x.kind === 'detail')
    && ibDet.columns.find((c) => c.key === 'request')?.cards.some((c2) => c2.id === idM4) === true);
  // R1対応: 詳細対象外の商品は kind=detail の候補 (total/LIMIT) を消費しない
  const idKfx = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-KFEX', '詳細対象外の絞り込み', 'smoke')
  `).run().lastInsertRowid);
  wfpEarly.ensureProgress(db, idKfx);
  db.prepare('UPDATE product_drafts SET detail_images_excluded = 1 WHERE id = ?').run(idKfx);
  const ibDet2 = wfpEarly.boardData(db, { view: 'image', imageKind: 'detail' });
  check('画像ビュー: kind=detail は詳細対象外の商品を候補に数えない',
    ibDet2.total === ibDet.total && !ibDet2.columns.some((c) => c.cards.some((x) => x.id === idKfx)));
  check('画像ビュー: 詳細対象外でも kind=top には出る',
    wfpEarly.boardData(db, { view: 'image', imageKind: 'top' }).columns.some((c) => c.cards.some((x) => x.id === idKfx)));
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idKfx);

  // ─── 画像工程 v2 (2026-08-26): 詳細系列 ①〜⑨ ───
  {
    const ibV2 = wfpEarly.boardData(db, { view: 'image' });
    const keys = ibV2.columns.map((c) => c.key);
    check('v2: ボード列は 依頼→構成→素材待ち→AI制作→デザイン修正→社内確認→Amazon→楽天→A+ (TOP の制作/登録/承認も混在)',
      keys.indexOf('request') < keys.indexOf('compose') && keys.indexOf('compose') < keys.indexOf('material')
      && keys.indexOf('material') < keys.indexOf('ai') && keys.indexOf('ai') < keys.indexOf('design')
      && keys.indexOf('design') < keys.indexOf('review') && keys.indexOf('review') < keys.indexOf('amazon')
      && keys.indexOf('amazon') < keys.indexOf('rakuten') && keys.indexOf('rakuten') < keys.indexOf('aplus'), JSON.stringify(keys));
    check('v2: 社内確認の列は 2 工程を束ねる', (ibV2.columns.find((c) => c.key === 'review')?.stepCodes || []).length === 2);
    check('v2: 旧詳細 v1 の列 (shoot/production…) は詳細絞り込みに出ない',
      !wfpEarly.boardData(db, { view: 'image', imageKind: 'detail' }).columns.some((c) => c.key === 'shoot' || c.key === 'production'));

    // 自社商品 (v2 切替後に作成) の ① 完了条件 = 撮影・素材 + 商品情報
    const idV2 = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by, own_brand, image_priority) VALUES ('DRV-V2', 'v2テスト', 'smoke', 1, '自社商品（重要度：高）')
    `).run().lastInsertRowid);
    wfpEarly.ensureProgress(db, idV2);
    const stV2 = (code) => db.prepare('SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(idV2, code)?.state;
    let reqErr = null;
    try { wfpEarly.setStepState(idV2, 'imgd_request', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { reqErr = e; }
    check('v2: ① は撮影・素材ステータス未設定では完了できない', reqErr?.status === 400 && /撮影・素材/.test(reqErr.message), reqErr?.message);
    dbmod.upsertImageProduction(db, idV2, { material_status: 'internal_prep' });
    reqErr = null;
    try { wfpEarly.setStepState(idV2, 'imgd_request', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { reqErr = e; }
    check('v2: ① は商品情報 (1.5) が空では完了できない (v2 切替後の新規商品)', reqErr?.status === 400 && /商品情報/.test(reqErr.message), reqErr?.message);
    dbmod.upsertImageProduction(db, idV2, { product_info_text: 'テスト商品の情報' });
    check('v2: 撮影・素材 + 商品情報 が揃えば ① 完了', wfpEarly.setStepState(idV2, 'imgd_request', { state: 'done' }, 'smoke', ADMIN2).changed === true);
    // D&D: 依頼 → 素材待ち (構成をまとめて done)
    wfpEarly.moveBoardCard(idV2, { view: 'image', kind: 'detail', to: 'material', expectedCurrent: 'imgd_compose' }, 'smoke', ADMIN2);
    check('v2: D&D で構成を飛ばして素材待ちへ', stV2('imgd_compose') === 'done' && stV2('imgd_material') === 'todo');
    let matErr = null;
    try { wfpEarly.setStepState(idV2, 'imgd_material', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { matErr = e; }
    check('v2: ③ 素材待ちは 社内準備 のままでは完了できない', matErr?.status === 400 && /素材完了/.test(matErr.message), matErr?.message);
    dbmod.upsertImageProduction(db, idV2, { material_status: 'ready' });
    check('v2: 素材完了なら ③ 完了', wfpEarly.setStepState(idV2, 'imgd_material', { state: 'done' }, 'smoke', ADMIN2).changed === true);
    wfpEarly.setStepState(idV2, 'imgd_ai', { state: 'done' }, 'smoke', ADMIN2);
    // ⑤ デザイン修正 done → TOP の 依頼/制作/登録 が自動 done
    check('v2: ⑤ 前は TOP 側が todo', stV2('img_request_top') === 'todo');
    wfpEarly.setStepState(idV2, 'imgd_design', { state: 'done' }, 'smoke', ADMIN2);
    check('v2: ⑤ デザイン修正 done で TOP の 依頼/制作/登録 が自動 done (承認は残る)',
      stV2('img_request_top') === 'done' && stV2('img_production_top') === 'done' && stV2('img_register_top') === 'done'
      && stV2('img_approve_top') === 'todo', JSON.stringify([stV2('img_request_top'), stV2('img_production_top'), stV2('img_register_top'), stV2('img_approve_top')]));
    // ⑥ 順序: 中原確認は田中確認の後
    let ordErr = null;
    try { wfpEarly.setStepState(idV2, 'imgd_review_2', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { ordErr = e; }
    check('v2: ⑥-2 (中原) は ⑥-1 (田中) の前に完了できない', ordErr?.status === 400 && /田中/.test(ordErr.message), ordErr?.message);
    const pV2 = wfpEarly.progressOf(idV2, { db });
    check('v2: 確認者 = 未完了の方 (田中)', pV2.imageDetail.current?.step_code === 'imgd_review_1');
    const gateBefore = wfpEarly.imageTrackBlockReason(db, idV2);
    check('v2: ⑥ 未完了は出品ゲートが閉じる (詳細画像: 社内確認)', typeof gateBefore === 'string' && /社内確認/.test(gateBefore), gateBefore);
    wfpEarly.setStepState(idV2, 'imgd_review_1', { state: 'done' }, 'smoke', ADMIN2);
    check('v2: 田中確認 done → 確認者 = 中原', wfpEarly.progressOf(idV2, { db }).imageDetail.current?.step_code === 'imgd_review_2');
    wfpEarly.setStepState(idV2, 'imgd_review_2', { state: 'done' }, 'smoke', ADMIN2);
    check('v2: ⑥-2 done で TOP 承認も自動 done', stV2('img_approve_top') === 'done');
    check('v2: ⑥ 両方 done で出品ゲートが開く (⑦⑧⑨ は前提にしない)', wfpEarly.imageTrackBlockReason(db, idV2) === null, wfpEarly.imageTrackBlockReason(db, idV2));
    const pV2b = wfpEarly.progressOf(idV2, { db });
    check('v2: gateDone は true・done (全工程) は false (⑦⑧⑨ が残る)', pV2b.imageDetail.gateDone === true && pV2b.imageDetail.done === false);
    // ⑧ 楽天登録は人が done にできない (admin でも)。出品の根拠があれば可・system は可
    let rkErr = null;
    try { wfpEarly.setStepState(idV2, 'imgd_rakuten', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { rkErr = e; }
    check('v2: ⑧ 楽天登録は出品なしに done にできない (admin でも)', rkErr?.status === 400 && /自動で完了/.test(rkErr.message), rkErr?.message);
    let rkMove = null;
    try { wfpEarly.moveBoardCard(idV2, { view: 'image', kind: 'detail', to: 'done', expectedCurrent: 'imgd_amazon' }, 'smoke', ADMIN2); } catch (e) { rkMove = e; }
    check('v2: 完了列への D&D も ⑧で止まる (途中の ⑦ もロールバック)', !!rkMove && stV2('imgd_amazon') === 'todo', rkMove?.message);
    check('v2: ⑧ は「対象外」にはできる', wfpEarly.setStepState(idV2, 'imgd_rakuten', { state: 'skip' }, 'smoke', ADMIN2).changed === true);
    wfpEarly.setStepState(idV2, 'imgd_rakuten', { state: 'todo' }, 'smoke', ADMIN2);
    check('v2: 出品処理 (systemActor) なら ⑧ done', wfpEarly.setStepState(idV2, 'imgd_rakuten', { state: 'done' }, 'system', { isAdmin: true, systemActor: true }).changed === true);
    // カード情報
    const cardV2 = [...wfpEarly.boardData(db, { view: 'image', imageKind: 'detail' }).columns.flatMap((c) => c.cards)].find((c) => c.id === idV2);
    check('v2: 詳細カードに 撮影・素材 / 商品情報あり が乗る',
      cardV2 && cardV2.materialStatus === 'ready' && cardV2.materialLabel === '素材完了' && cardV2.hasProductInfo === true && cardV2.ownBrand === true,
      JSON.stringify(cardV2 && { m: cardV2.materialStatus, l: cardV2.materialLabel, i: cardV2.hasProductInfo }));
    // 楽天登録済みの既存商品には詳細 v2 も自動 done で入る
    const idV2Rk = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('DRV-V2-RK', 'v2・登録済み', 'listed', 'smoke')
    `).run().lastInsertRowid);
    db.prepare(`INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-08-01T00:00:00Z')`).run(idV2Rk);
    wfpEarly.ensureProgress(db, idV2Rk);
    check('v2: 楽天登録済みには ①〜⑨ 全部 done で入る',
      db.prepare("SELECT COUNT(*) c FROM draft_step_progress WHERE draft_id = ? AND step_code LIKE 'imgd_%' AND state = 'done'").get(idV2Rk).c === 10);

    // 旧 v1 → v2 の一回きり移行 (フラグを外して再実行)
    const mk = (code, listed) => {
      const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES (?, ?, 'smoke')`).run(code, code).lastInsertRowid);
      if (listed) db.prepare(`INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-08-01T00:00:00Z')`).run(id);
      return id;
    };
    // 本番 DB には v1 の詳細工程行が残っている (v2 以降は seed されない) → 再現のため inactive で入れる
    const insV1Step = db.prepare(`INSERT OR IGNORE INTO ph_steps (code, label, track, image_kind, image_stage, sort, active, builtin) VALUES (?, ?, 'image', 'detail', ?, ?, 1, 1)`);
    [['img_request_detail', '画像制作の依頼', 'request', 10], ['img_shoot_detail', '撮影依頼中', 'shoot', 15], ['img_production_detail', '画像制作', 'production', 20],
      ['img_register_detail', '画像登録', 'register', 30], ['img_approve_detail', '画像承認', 'approve', 40]].forEach((r) => insV1Step.run(...r));
    const insV1 = db.prepare('INSERT OR REPLACE INTO draft_step_progress (draft_id, step_code, state) VALUES (?, ?, ?)');
    const idM1 = mk('DRV-M-REQ', false); insV1.run(idM1, 'img_request_detail', 'done'); insV1.run(idM1, 'img_shoot_detail', 'skip');
    const idM2 = mk('DRV-M-PROD', false); insV1.run(idM2, 'img_request_detail', 'done'); insV1.run(idM2, 'img_production_detail', 'done');
    const idM3 = mk('DRV-M-APR', false); ['img_request_detail', 'img_production_detail', 'img_register_detail', 'img_approve_detail'].forEach((c) => insV1.run(idM3, c, 'done'));
    const idM4 = mk('DRV-M-RK', true); insV1.run(idM4, 'img_request_detail', 'done');
    const idM5 = mk('DRV-M-TODO', false); insV1.run(idM5, 'img_request_detail', 'todo');
    db.prepare('DELETE FROM ph_intake_state WHERE key = ?').run(dbmod.IMAGE_TRACK_V2_KEY);
    const mig = dbmod.migrateDetailTrackV2(db);
    const stM = (id, code) => db.prepare('SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(id, code)?.state;
    check('v2 移行: 実行される (5 件)', mig.skipped === false && mig.migrated >= 5, JSON.stringify(mig));
    check('v2 移行: 旧 依頼 done → ① done・② todo (撮影 skip → 撮影不要)',
      stM(idM1, 'imgd_request') === 'done' && stM(idM1, 'imgd_compose') === 'todo'
      && db.prepare('SELECT material_status FROM draft_image_production WHERE draft_id = ?').get(idM1)?.material_status === 'not_required');
    check('v2 移行: 旧 制作 done → ④まで done・⑤ todo', stM(idM2, 'imgd_ai') === 'done' && stM(idM2, 'imgd_design') === 'todo');
    check('v2 移行: 旧 承認 done でも ⑤まで done・⑥-1 (田中) から再確認',
      stM(idM3, 'imgd_design') === 'done' && stM(idM3, 'imgd_review_1') === 'todo' && stM(idM3, 'imgd_review_2') === 'todo');
    check('v2 移行: 楽天登録済みは全部 done', stM(idM4, 'imgd_aplus') === 'done');
    check('v2 移行: 旧 未着手は全部 todo', stM(idM5, 'imgd_request') === 'todo');
    check('v2 移行: イベントに元工程を記録',
      db.prepare("SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'image_track_v2_migrated' AND detail LIKE '%img_approve_detail=done%'").get(idM3).c === 1);
    check('v2 移行: 二度目は写す商品が無い (ドラフト単位で冪等)', dbmod.migrateDetailTrackV2(db).skipped === true);
    // 途中デプロイ等で写し損ねた商品 (旧行あり・移行イベント無し) は次回起動で拾う。v2 行が todo で先にあっても done に揃える
    const idM6 = mk('DRV-M-LATE', false); insV1.run(idM6, 'img_request_detail', 'done'); insV1.run(idM6, 'img_production_detail', 'done');
    insV1.run(idM6, 'imgd_request', 'todo');
    const mig2 = dbmod.migrateDetailTrackV2(db);
    check('v2 移行: 写し損ねた商品を後から拾い、todo の v2 行も期待状態へ揃える',
      mig2.migrated === 1 && stM(idM6, 'imgd_request') === 'done' && stM(idM6, 'imgd_ai') === 'done' && stM(idM6, 'imgd_design') === 'todo', JSON.stringify(mig2));
    // 楽天登録済みの根拠は registered_at 以外 (status listed / モール別状況) でも拾う
    const idM7 = mk('DRV-M-LISTED', false); insV1.run(idM7, 'img_request_detail', 'done');
    db.prepare("UPDATE product_drafts SET status = 'listed' WHERE id = ?").run(idM7);
    dbmod.migrateDetailTrackV2(db);
    check('v2 移行: status=listed も楽天登録済みとみなして全部 done', stM(idM7, 'imgd_aplus') === 'done');
    // 旧 撮影依頼中 done だけ (依頼行なし) → ① done・素材完了
    const idM8 = mk('DRV-M-SHOOT', false); insV1.run(idM8, 'img_shoot_detail', 'done');
    dbmod.migrateDetailTrackV2(db);
    check('v2 移行: 旧 撮影依頼中 done → ① done・②から・素材完了',
      stM(idM8, 'imgd_request') === 'done' && stM(idM8, 'imgd_compose') === 'todo'
      && db.prepare('SELECT material_status FROM draft_image_production WHERE draft_id = ?').get(idM8)?.material_status === 'ready');
    // 工程行がまだ無い既存商品: status=listed なら初回生成で画像側が全部 done
    const idM9 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('DRV-M-NOROWS', 'listed・行なし', 'listed', 'smoke')`).run().lastInsertRowid);
    wfpEarly.ensureProgress(db, idM9);
    check('v2: 工程行の無い出品済み商品 (status=listed) は初回生成で詳細 v2 も全部 done', stM(idM9, 'imgd_aplus') === 'done' && stM(idM9, 'img_approve_top') === 'done');
    db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?, ?, ?)').run(idM6, idM7, idM8, idM9);
    check('v2 移行: 旧詳細工程は active=0',
      db.prepare(`SELECT COUNT(*) c FROM ph_steps WHERE code IN ('img_shoot_detail','img_request_detail') AND active = 1`).get().c === 0);
    for (const id of [idV2, idV2Rk, idM1, idM2, idM3, idM4, idM5]) db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }

  // TOP画像の重要度 (HTTP)
  r = await call('POST', `/api/drafts/${idM}/image-priority`, { value: '自社商品（重要度：高）' });
  check('重要度: 保存できる', r.status === 200
    && db.prepare('SELECT image_priority FROM product_drafts WHERE id = ?').get(idM).image_priority === '自社商品（重要度：高）');
  r = await call('POST', `/api/drafts/${idM}/image-priority`, { value: '存在しない値' });
  check('重要度: 不正な値は 400', r.status === 400);
  r = await call('POST', `/api/drafts/${idM}/image-priority`, { value: '' });
  check('重要度: 空でクリアできる', r.status === 200
    && db.prepare('SELECT image_priority FROM product_drafts WHERE id = ?').get(idM).image_priority == null);

  // ─── SKU別売価 (2026-08-24: 原価がSKUで異なる場合に売価もSKU別に) ───
  // mirror の vc-a/vc-b (代表 'vcost') は getNeCost のテストで投入済み
  const idP = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('vcost', 'SKU売価テスト', 'smoke')
  `).run().lastInsertRowid);
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: 1480 });
  check('SKU売価: 保存できる', r.status === 200
    && db.prepare(`SELECT price FROM draft_sku_prices WHERE draft_id = ? AND sku_code = 'vc-b'`).get(idP)?.price === 1480,
    JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: 2480 });
  check('SKU売価: 上書きできる (UPSERT)', r.status === 200
    && db.prepare(`SELECT price FROM draft_sku_prices WHERE draft_id = ? AND sku_code = 'vc-b'`).get(idP)?.price === 2480);
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: 0 });
  check('SKU売価: 範囲外 (0円) は 400', r.status === 400);
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: 1.5 });
  check('SKU売価: 小数は 400', r.status === 400);
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'not-in-group', price: 1000 });
  check('SKU売価: グループ外のSKUは 409', r.status === 409);
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: null });
  check('SKU売価: 空 (null) で解除 = 行が消えてページ代表の売価に戻る', r.status === 200
    && !db.prepare(`SELECT 1 FROM draft_sku_prices WHERE draft_id = ? AND sku_code = 'vc-b'`).get(idP));
  r = await call('POST', `/api/drafts/${idE}/sku-prices`, { ne_code: 'vc-b', price: 1000 });
  check('SKU売価: バリエーションでないドラフトは 400', r.status === 400);
  // R1対応: SKUを外すと売価行も掃除される
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: 1980 });
  check('SKU売価: 除外前は保存できる', r.status === 200, JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/variation/exclude`, { ne_code: 'vc-b' });
  check('SKU売価: SKUを外すと売価行も消える (孤児行を残さない)', r.status === 200
    && !db.prepare(`SELECT 1 FROM draft_sku_prices WHERE draft_id = ? AND sku_code = 'vc-b'`).get(idP));
  // R2対応: 既に除外済みへの再除外 (冪等) でも掃除は走る (修正前データの残存救済)
  db.prepare(`INSERT INTO draft_sku_prices (draft_id, sku_code, price) VALUES (?, 'vc-b', 777)`).run(idP);
  r = await call('POST', `/api/drafts/${idP}/variation/exclude`, { ne_code: 'vc-b' });
  check('SKU売価: 除外済みSKUへの再除外 (冪等) でも売価行が掃除される', r.status === 200
    && !db.prepare(`SELECT 1 FROM draft_sku_prices WHERE draft_id = ? AND sku_code = 'vc-b'`).get(idP));
  db.prepare(`DELETE FROM draft_variation_exclusions WHERE LOWER(TRIM(ne_code)) = 'vc-b'`).run();
  // R1対応: 原価が全SKU共通なら保存は 400 (UIと同じ条件をサーバーでも強制)・解除はいつでもできる
  db.prepare(`UPDATE mirror_products SET 原価 = 300 WHERE product_id IN (9410, 9411)`).run();
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: 1480 });
  check('SKU売価: 原価が全SKU共通なら保存は 400', r.status === 400, JSON.stringify(r.json));
  db.prepare(`INSERT INTO draft_sku_prices (draft_id, sku_code, price) VALUES (?, 'vc-b', 999)`).run(idP);
  r = await call('POST', `/api/drafts/${idP}/sku-prices`, { ne_code: 'vc-b', price: '' });
  check('SKU売価: 残った行の解除はいつでもできる (原価が同一に変わった後でも)', r.status === 200
    && !db.prepare(`SELECT 1 FROM draft_sku_prices WHERE draft_id = ? AND sku_code = 'vc-b'`).get(idP));
  db.prepare(`UPDATE mirror_products SET 原価 = 500 WHERE product_id = 9411`).run();
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idP);

  // ─── 工程ボードをデフォルトに (2026-08-24 中原さん要望) ───
  const rawGet = async (p) => {
    const res = await fetch(base + p, { redirect: 'manual' });
    return { status: res.status, location: res.headers.get('location') || '' };
  };
  let rg = await rawGet('/');
  check('ルート: / は工程ボードへリダイレクト',
    rg.status === 302 && rg.location.endsWith('/apps/product-hub/board'), JSON.stringify(rg));
  rg = await rawGet('/?status=review');
  check('ルート: 旧 ?status= 付きブックマークは一覧へ引き継ぐ',
    rg.status === 302 && rg.location.endsWith('/apps/product-hub/list?status=review'), JSON.stringify(rg));
  rg = await rawGet('/?status=bogus');
  check('ルート: 不正な status は素の一覧へ',
    rg.status === 302 && rg.location.endsWith('/apps/product-hub/list'), JSON.stringify(rg));
  const listRes = await fetch(base + '/list');
  check('ルート: /list が一覧を返す', listRes.status === 200 && (await listRes.text()).includes('新規登録'));

  // ─── 自社商品チェック ⇄ 画像の重要度「自社商品（重要度：高）」の連動 (2026-08-24 中原さん要望) ───
  // 不変条件: own_brand=1 ⟺ image_priority='自社商品（重要度：高）'
  const idOb = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-OWNBRAND', '連動テスト', 'smoke')
  `).run().lastInsertRowid);
  const obRow = () => db.prepare('SELECT own_brand, image_priority FROM product_drafts WHERE id = ?').get(idOb);
  r = await call('POST', `/api/drafts/${idOb}/image-priority`, { value: '自社商品（重要度：高）' });
  check('連動: 重要度「自社商品」を選ぶと own_brand=1',
    r.status === 200 && r.json.own_brand === 1 && obRow().own_brand === 1, JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idOb}/image-priority`, { value: '仕入商品（重要度：高）' });
  check('連動: 他の重要度を選ぶと own_brand=0',
    r.json.own_brand === 0 && obRow().own_brand === 0 && obRow().image_priority === '仕入商品（重要度：高）');
  await call('POST', `/api/drafts/${idOb}/image-priority`, { value: '自社商品（重要度：高）' });
  r = await call('POST', `/api/drafts/${idOb}/image-priority`, { value: '' });
  check('連動: 重要度を未設定に戻すと own_brand=0 (不整合を正式な操作で作らせない — Codex R1)',
    r.json.own_brand === 0 && obRow().own_brand === 0 && obRow().image_priority == null);
  // 専用エンドポイント (チェックボックスの即保存。Notion同期・ゲート再判定なし — Codex R1)
  r = await call('POST', `/api/drafts/${idOb}/own-brand`, { value: true });
  check('連動: チェックON → 重要度が「自社商品（重要度：高）」になる',
    r.json.own_brand === 1 && r.json.image_priority === '自社商品（重要度：高）'
    && obRow().own_brand === 1 && obRow().image_priority === '自社商品（重要度：高）', JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idOb}/own-brand`, { value: false });
  check('連動: チェックOFF (重要度が自社商品) → 重要度は未設定に戻す',
    r.json.own_brand === 0 && obRow().own_brand === 0 && obRow().image_priority == null);
  await call('POST', `/api/drafts/${idOb}/image-priority`, { value: '取扱先限定商品（重要度：高）' });
  r = await call('POST', `/api/drafts/${idOb}/own-brand`, { value: false });
  check('連動: チェックOFFでも自社商品以外の重要度は残す',
    obRow().own_brand === 0 && obRow().image_priority === '取扱先限定商品（重要度：高）');
  // 基本情報の保存経路でも同じ不変条件が強制される
  r = await call('POST', `/api/drafts/${idOb}`, { own_brand: true });
  check('連動: 基本情報保存のチェックONでも重要度が自社商品になる',
    r.json.image_priority === '自社商品（重要度：高）' && obRow().image_priority === '自社商品（重要度：高）' && obRow().own_brand === 1,
    JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idOb}`, { name: '連動テスト2' });
  check('連動: own_brand を送らない保存は状態を変えない',
    obRow().own_brand === 1 && obRow().image_priority === '自社商品（重要度：高）');
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idOb);

  // 起動時バックフィル (連動導入前の既存データの整合化。重要度が設定済みなら重要度が正)
  const idBf = (ob, pr) => Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, own_brand, image_priority, created_by)
    VALUES ('DRV-BF-' || abs(random() % 100000), '整合化', ?, ?, 'smoke')
  `).run(ob, pr).lastInsertRowid);
  const bf1 = idBf(0, '自社商品（重要度：高）');   // 重要度=自社 → own_brand=1 へ
  const bf2 = idBf(1, '仕入商品（重要度：高）');   // 重要度=他社 → own_brand=0 へ
  const bf3 = idBf(1, null);                        // own_brand=1 のみ → 重要度を自社へ
  dbmod.syncOwnBrandImagePriority(db);
  const bfRow = (id) => db.prepare('SELECT own_brand, image_priority FROM product_drafts WHERE id = ?').get(id);
  check('整合化: 重要度=自社なのに own_brand=0 → 1 に直す', bfRow(bf1).own_brand === 1);
  check('整合化: 重要度=他社なのに own_brand=1 → 0 に直す', bfRow(bf2).own_brand === 0);
  check('整合化: own_brand=1 で重要度未設定 → 自社商品を入れる', bfRow(bf3).image_priority === '自社商品（重要度：高）');
  for (const id of [bf1, bf2, bf3]) db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);

  server.close();
}

// ─── EJS 実 render (RYS教訓: 全分岐を実データで) ───
const views = path.join(__dirname, '..', 'views');
const statuses = dbmod.DRAFT_STATUSES;
const statusLabels = dbmod.STATUS_LABELS;
const counts = Object.fromEntries(statuses.map((s) => [s, 1]));
// 商品ページ表記カードの render 変数 (全 detail fixture に必要)
const pageInfoVars = {
  pageInfo: null, pageInfoHtml: '',
  neShipping: { group: null, label: null, guessed: false },
  productTypes: pinfo.PRODUCT_TYPES, categoryLabels: pinfo.CATEGORY_LABELS,
  categoryLabelsByType: pinfo.CATEGORY_LABELS_BY_TYPE,
  adResponsibilityText: pinfo.adResponsibility(),
};
const draftRow = {
  ...after, thumb: 'https://drive.google.com/thumbnail?id=x&sz=w160', first_image_id: 'x',
  variation: { kind: 'single', groupKey: after.ne_code, memberCount: 0, isChild: false },
  // 一覧の「工程 / 担当」列。担当者あり・停滞ありの分岐まで描かせる
  workflow: {
    ...wfp.progressSummaryFor(db, [wfDraftId]).get(wfDraftId),
    stalledDays: 9,
  },
};
// 詳細画面のバリエーションカードは 3 分岐 × 子SKU有無を全部描かせる
const variationFixtures = {
  child: vari.resolveVariationGroup(db, 'rooms-l-bk'),
  rep: vari.resolveVariationGroup(db, 'rooms'),
  single: vari.resolveVariationGroup(db, 'SOLO-1'),
  unknown: vari.resolveVariationGroup(db, 'NOT-IN-NE-XYZ'),
  // 外したSKUがある状態 (除外欄の「単独ページあり」「未割当」両分岐)
  withExcluded: {
    ...vari.resolveVariationGroup(db, 'rooms'),
    excludedMembers: [
      { 商品コード: 'rooms-m-bk', 商品名: 'ルームズ M 黒', 取扱区分: '取扱中', 在庫数: 3, ownDraftId: null },
      { 商品コード: 'rooms-s-bk', 商品名: 'ルームズ S 黒', 取扱区分: '取扱中', 在庫数: 1, ownDraftId: 42 },
    ],
  },
  detached: { kind: 'detached', groupKey: 'rooms-m-bk', repCode: null, isChild: false, memberCount: 0, members: [], excludedMembers: [], found: true },
};

// ─── applyFolderImport (フォルダ取込のDB反映、2026-08-01) ───
db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url)
  VALUES ('FOLDER-1', 'フォルダ取込テスト', 'smoke', 'https://drive.google.com/drive/folders/OLDFOLDER')`).run();
const fdraft = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'FOLDER-1'`).get();
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'hand-placed-old', 0)`).run(fdraft.id);
dbmod.applyFolderImport(db, fdraft.id, {
  whiteBg: { id: 'wb1', name: 'FOLDER-1_00.jpg' },
  slots: [{ slot: 1, id: 'f1', name: 'FOLDER-1_01.jpg' }, { slot: 3, id: 'f3', name: 'FOLDER-1_03.jpg' }],
  skipped: [], conflicts: [],
}, { folderUrl: 'https://drive.google.com/drive/folders/NEWFOLDER', currentFolderUrl: fdraft.drive_folder_url });
const fimgs = db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ? ORDER BY sort').all(fdraft.id);
check('取込で手貼り画像が置き換わる (sort = スロット番号-1)',
  fimgs.length === 2 && fimgs[0].drive_file_id === 'f1' && fimgs[0].sort === 0
  && fimgs[1].drive_file_id === 'f3' && fimgs[1].sort === 2, JSON.stringify(fimgs));
const frk = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(fdraft.id);
check('白抜きが draft_rakuten に upsert される',
  frk?.white_bg_drive_file_id === 'wb1' && frk.white_bg_drive_url.includes('wb1'), JSON.stringify(frk));
check('フォルダURLが基本情報に保存される',
  db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(fdraft.id).drive_folder_url.includes('NEWFOLDER'));
db.prepare(`UPDATE draft_rakuten SET genre_id = '123456' WHERE draft_id = ?`).run(fdraft.id);
dbmod.applyFolderImport(db, fdraft.id,
  { whiteBg: { id: 'wb2', name: 'n' }, slots: [], skipped: [], conflicts: [] }, {});
const fimgs2 = db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ? ORDER BY sort').all(fdraft.id);
const frk2 = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(fdraft.id);
check('白抜きのみの取込は商品画像を触らず、draft_rakuten の他カラムも保持',
  fimgs2.length === 2 && frk2.white_bg_drive_file_id === 'wb2' && frk2.genre_id === '123456',
  JSON.stringify({ fimgs2, frk2 }));

// 店舗内カテゴリ AI 自動適用の「一度だけ」判定 (router の everSaved と同じイベント名・同じクエリ)
const everSavedQuery = (id) => db.prepare(
  `SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'shop_categories_saved'`
).get(id).c > 0;
check('店舗内カテゴリ: 未保存のドラフトは everSaved=false (AI自動適用の対象)', everSavedQuery(fdraft.id) === false);

// 紐付け可能判定 = 有効 or そのドラフトが既に選択中 (Codex R1 medium)
const { countSelectableShopCategories, shopCategoriesNeverSaved } = await import('../lib/shop-categories.js');
check('店舗内カテゴリ: shopCategoriesNeverSaved は未保存で true', shopCategoriesNeverSaved(db, fdraft.id) === true);
db.prepare(`INSERT INTO ph_shop_categories (category_id, path, path_key, is_active, sort_order)
  VALUES ('900','有効な棚','有効な棚',1,0), ('901','無効な棚','無効な棚',0,1), ('902','無効だが選択中','無効だが選択中',0,2)`).run();
const scActive = db.prepare(`SELECT id FROM ph_shop_categories WHERE path_key = '有効な棚'`).get().id;
const scInactive = db.prepare(`SELECT id FROM ph_shop_categories WHERE path_key = '無効な棚'`).get().id;
const scInactiveSel = db.prepare(`SELECT id FROM ph_shop_categories WHERE path_key = '無効だが選択中'`).get().id;
db.prepare('INSERT INTO draft_shop_categories (draft_id, shop_category_id) VALUES (?, ?)').run(fdraft.id, scInactiveSel);
check('店舗内カテゴリ: 有効な棚は紐付け可', countSelectableShopCategories(db, fdraft.id, [scActive]) === 1);
check('店舗内カテゴリ: 無効な棚への新規紐付けは弾く', countSelectableShopCategories(db, fdraft.id, [scInactive]) === 0);
check('店舗内カテゴリ: 無効でも既に選択中なら維持できる',
  countSelectableShopCategories(db, fdraft.id, [scInactiveSel]) === 1);
check('店舗内カテゴリ: 空配列は 0', countSelectableShopCategories(db, fdraft.id, []) === 0);

// 枠 (slot) の保存と順序 (2026-08-02: RMSの「表示先カテゴリ1〜5」対応)
const { setDraftShopCategories: setCats, selectedShopCategoriesInOrder, MAX_DRAFT_SHOP_CATEGORIES: MAXC }
  = await import('../lib/shop-categories.js');
check('店舗内カテゴリ: 上限はRMSと同じ5枠', MAXC === 5);
setCats(db, fdraft.id, [scInactiveSel, scActive]);   // わざと sort_order と逆順で保存
const inOrder = selectedShopCategoriesInOrder(db, fdraft.id);
check('店舗内カテゴリ: 配列順がそのまま枠番になる (マスタの並び順に引きずられない)',
  inOrder.length === 2 && inOrder[0].id === scInactiveSel && inOrder[0].slot === 1
  && inOrder[1].id === scActive && inOrder[1].slot === 2, JSON.stringify(inOrder));
const { sanitizeShopCategoryIds: sanitize6 } = await import('../lib/shop-categories.js');
check('店舗内カテゴリ: 6件は sanitize で too_many (5枠上限)',
  sanitize6([1, 2, 3, 4, 5, 6]).error === 'too_many');
setCats(db, fdraft.id, []);

// 旧上限 (30) 時代のDBからの移行: slot 列を落として6件選択の旧形を作り、再init で
// ①slot 採番 ②6件目以降の切り詰め+イベント記録 が走ることを確認 (Codex R1 high)
db.exec('ALTER TABLE draft_shop_categories DROP COLUMN slot');
for (let i = 1; i <= 6; i++) {
  db.prepare(`INSERT INTO ph_shop_categories (category_id, path, path_key, is_active, sort_order)
    VALUES (?, ?, ?, 1, ?)`).run(String(8000 + i), `移行棚${i}`, `移行棚${i}`, 200 + i);
}
const migIds = db.prepare(`SELECT id FROM ph_shop_categories WHERE path LIKE '移行棚%' ORDER BY sort_order`).all();
for (const r of migIds) {
  db.prepare('INSERT INTO draft_shop_categories (draft_id, shop_category_id) VALUES (?, ?)').run(fdraft.id, r.id);
}
// 途中失敗はロールバックされ、再実行で回復できる (Codex R2 high: 非アトミックだと
// 「slot列はあるが未採番」の中途半端な状態が残り、二度と移行が走らない)
db.exec('ALTER TABLE draft_events RENAME TO draft_events_bk');   // trim のINSERTを故意に失敗させる
let migThrew = false;
try { dbmod.migrateShopCategorySlots(db); } catch (e) { migThrew = true; }
check('slot移行: 途中失敗でロールバック (slot列ごと消え、再実行可能な状態に戻る)',
  migThrew === true
  && !db.prepare('PRAGMA table_info(draft_shop_categories)').all().some((c) => c.name === 'slot'));
db.exec('ALTER TABLE draft_events_bk RENAME TO draft_events');

check('slot移行: 旧形DBで migrate が走る', dbmod.migrateShopCategorySlots(db) === true);
check('slot移行: 2回目は no-op (冪等)', dbmod.migrateShopCategorySlots(db) === false);
const migRows = selectedShopCategoriesInOrder(db, fdraft.id);
check('slot移行: 6件が5件に切り詰められ slot 1..5 で採番される',
  migRows.length === 5 && migRows.every((r, i) => r.slot === i + 1)
  && migRows[0].path === '移行棚1' && migRows[4].path === '移行棚5', JSON.stringify(migRows));
check('slot移行: 外した棚は draft_events に記録される (黙って消さない)',
  (db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'shop_categories_trimmed_to_5'`)
    .get(fdraft.id)?.detail || '').includes('移行棚6'));
setCats(db, fdraft.id, []);

// Amazon URL だけでゲート通過した draft の参照元が generation queue から欠けない (Codex R1 high)
db.prepare(`UPDATE product_drafts SET official_url = NULL, amazon_url = 'https://www.amazon.co.jp/dp/B0GENQ', asin = 'B0GENQ',
  status = 'ready_for_ai' WHERE id = ?`).run(fdraft.id);
const q = dbmod.listGenerationQueue(db).find((d) => d.id === fdraft.id);
check('generation queue: amazon_url / asin が材料に含まれる',
  q && q.amazon_url === 'https://www.amazon.co.jp/dp/B0GENQ' && q.asin === 'B0GENQ' && q.official_url == null,
  JSON.stringify(q));
db.prepare(`UPDATE product_drafts SET status = 'draft', amazon_url = NULL, asin = NULL WHERE id = ?`).run(fdraft.id);

// Category API 2.0 のツリー展開 (2026-08-02 実測形。miniPC /shop-categories/tree の trees)
const { flattenCategoryTrees } = await import('../lib/shop-categories.js');
const REAL_TREE = [{
  categorySetId: '0',
  rootNode: {
    children: [
      { category: { categoryId: '115', title: '精油・アロマ・ハーブ' }, children: [
        { category: { categoryId: '145', title: '精油（エッセンシャルオイル）' }, children: [
          { category: { categoryId: '146', title: '精油 ア行' }, children: [
            { category: { categoryId: '176', title: '青森ひば' } },
          ] },
        ] },
      ] },
      { category: { categoryId: '120', title: '生活雑貨・日用品' }, children: [
        { category: { categoryId: '1270', title: '洗濯用品' } },
      ] },
    ],
  },
}];
const flat = flattenCategoryTrees(REAL_TREE);
check('カテゴリツリー展開: 全階層が行になる (中間ノードにも商品を割り当てられる)',
  flat.rows.length === 6, JSON.stringify(flat.rows.map((r) => r.path)));
check('カテゴリツリー展開: パスが「 > 」連結・categoryIdを保持',
  flat.rows.some((r) => r.path === '生活雑貨・日用品 > 洗濯用品' && r.categoryId === '1270')
  && flat.rows.some((r) => r.path === '精油・アロマ・ハーブ > 精油（エッセンシャルオイル） > 精油 ア行 > 青森ひば'),
  JSON.stringify(flat.rows));
check('カテゴリツリー展開: pathKey は小文字', flat.rows.every((r) => r.pathKey === r.path.toLowerCase()));
check('カテゴリツリー展開: 空・壊れた入力でも落ちない',
  flattenCategoryTrees(null).rows.length === 0 && flattenCategoryTrees([{}]).rows.length === 0
  && flattenCategoryTrees([{ rootNode: { children: [{}] } }]).rows.length === 0);
check('カテゴリツリー展開: タイトル内の「>」はパス区切りと衝突しないよう潰す',
  flattenCategoryTrees([{ rootNode: { children: [{ category: { categoryId: '9', title: 'A > B' } }] } }])
    .rows[0].path === 'A ／ B');
// 上限・形式検証 (貼り付け取り込みと同じ基準を API 側にも適用)
const deepTree = (depth) => {
  let node = { category: { categoryId: String(depth), title: 'L' + depth } };
  for (let i = depth - 1; i >= 1; i--) node = { category: { categoryId: String(i), title: 'L' + i }, children: [node] };
  return [{ rootNode: { children: [node] } }];
};
check('カテゴリツリー展開: 深すぎる階層は打ち切って skipped に出す',
  (() => { const d = flattenCategoryTrees(deepTree(30)); return d.skipped.length > 0 && d.rows.length <= 20; })());
check('カテゴリツリー展開: categoryId が数字でない行は skipped',
  (() => {
    const d = flattenCategoryTrees([{ rootNode: { children: [{ category: { categoryId: 'abc', title: 'X' } }] } }]);
    return d.rows.length === 0 && d.skipped.length === 1 && d.skipped[0].reason.includes('数字');
  })());
check('カテゴリツリー展開: 長すぎるパスは skipped',
  (() => {
    const d = flattenCategoryTrees([{ rootNode: { children: [{ category: { categoryId: '1', title: 'あ'.repeat(400) } }] } }]);
    return d.rows.length === 0 && d.skipped.length === 1;
  })());
check('カテゴリツリー展開: 件数上限を超えたら truncated (部分取り込みしない合図)',
  (() => {
    const many = Array.from({ length: 1200 }, (_, i) => ({ category: { categoryId: String(i + 1), title: 'C' + i } }));
    const d = flattenCategoryTrees([{ rootNode: { children: many } }]);
    return d.truncated === true;
  })());
check('カテゴリツリー展開: 正常データでは truncated=false・skipped 空',
  flat.truncated === false && flat.skipped.length === 0);

// item-mappings で棚を反映するための列 (2026-08-02)
const rkColsNow = new Set(db.prepare('PRAGMA table_info(draft_rakuten)').all().map((c) => c.name));
check('draft_rakuten に反映状態の列がある (冪等ALTER)',
  rkColsNow.has('shop_categories_synced_at') && rkColsNow.has('shop_categories_error'),
  [...rkColsNow].join(','));
const listing2 = await import('../services/rakuten-listing.js');
check('syncShopCategoriesToRms がエクスポートされている', typeof listing2.syncShopCategoriesToRms === 'function');
// 未登録ドラフトは反映できない (先に「楽天に登録」が必要)
const unreg = await listing2.syncShopCategoriesToRms(fdraft.id, { actor: 'smoke' });
check('未登録ドラフトの棚反映は拒否される',
  unreg.ok === false && String(unreg.error).includes('登録した商品だけ'), JSON.stringify(unreg));
// 登録済みだが棚が未選択 → 明示エラー (RMS を叩かない)
db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ?').run(fdraft.id); // 他テストの残りを掃除
db.prepare(`UPDATE draft_rakuten SET registered_at = '2026-08-02T00:00:00Z' WHERE draft_id = ?`).run(fdraft.id);
const noCat = await listing2.syncShopCategoriesToRms(fdraft.id, { actor: 'smoke' });
check('棚が未選択なら RMS を叩かずエラー',
  noCat.ok === false && String(noCat.error).includes('選択されていません'), JSON.stringify(noCat));
// categoryId を持たない棚 (貼り付け取り込み由来) だけのときも叩かない
db.prepare(`INSERT INTO ph_shop_categories (category_id, path, path_key, is_active, sort_order)
  VALUES (NULL, 'ID無しの棚', 'id無しの棚', 1, 99)`).run();
const noIdCat = db.prepare(`SELECT id FROM ph_shop_categories WHERE path_key = 'id無しの棚'`).get().id;
db.prepare('INSERT INTO draft_shop_categories (draft_id, shop_category_id) VALUES (?, ?)').run(fdraft.id, noIdCat);
const idless = await listing2.syncShopCategoriesToRms(fdraft.id, { actor: 'smoke' });
check('カテゴリIDが無い棚が混ざったら反映しない (部分反映で画面とRMSがズレるため)',
  idless.ok === false && String(idless.error).includes('カテゴリIDが分からない棚'), JSON.stringify(idless));
check('失敗理由は shop_categories_error に残る (リロードしても原因が分かる)',
  (db.prepare('SELECT shop_categories_error FROM draft_rakuten WHERE draft_id = ?').get(fdraft.id)
    ?.shop_categories_error || '').includes('カテゴリIDが分からない棚'));
// IDありの棚を1件足しても、ID無しが残っていれば止まる (Codex R1 medium: 部分反映の禁止)
db.prepare(`INSERT INTO ph_shop_categories (category_id, path, path_key, is_active, sort_order)
  VALUES ('1270', 'ID有りの棚(テスト)', 'id有りの棚(てすと)', 1, 50)`).run();
const okCat = db.prepare(`SELECT id FROM ph_shop_categories WHERE path_key = 'id有りの棚(てすと)'`).get();
db.prepare('INSERT INTO draft_shop_categories (draft_id, shop_category_id) VALUES (?, ?)').run(fdraft.id, okCat.id);
const mixed = await listing2.syncShopCategoriesToRms(fdraft.id, { actor: 'smoke' });
check('ID有りと無しが混在しても反映しない (全体置換APIなので部分送信しない)',
  mixed.ok === false && (mixed.missingPaths || []).length === 1, JSON.stringify(mixed));

// 反映状態の判定 (反映後に棚を変えたら「未反映」に戻る)
// ID無しの棚だけ選択から外す (マスタ行は draft_shop_categories から参照されているので消さない)
db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ? AND shop_category_id = ?').run(fdraft.id, noIdCat);
const rkNow = () => db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(fdraft.id);
db.prepare(`UPDATE draft_rakuten SET shop_categories_synced_at = NULL, shop_categories_synced_key = NULL,
  shop_categories_error = NULL WHERE draft_id = ?`).run(fdraft.id);
check('同期状態: 未反映は pending', listing2.shopCategorySyncState(db, fdraft.id, rkNow()) === 'pending');
db.prepare(`UPDATE draft_rakuten SET shop_categories_synced_at = '2026-08-02T00:00:00Z',
  shop_categories_synced_key = '1270' WHERE draft_id = ?`).run(fdraft.id);
check('同期状態: 反映した棚と一致すれば synced',
  listing2.shopCategorySyncState(db, fdraft.id, rkNow()) === 'synced');
db.prepare(`UPDATE draft_rakuten SET shop_categories_synced_key = '9999' WHERE draft_id = ?`).run(fdraft.id);
check('同期状態: 反映後に棚を変えたら stale (✅表示のままにしない)',
  listing2.shopCategorySyncState(db, fdraft.id, rkNow()) === 'stale');
db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ?').run(fdraft.id);
check('同期状態: 棚が空なら none', listing2.shopCategorySyncState(db, fdraft.id, rkNow()) === 'none');
check('同期状態: 未登録商品は null (判定対象外)',
  listing2.shopCategorySyncState(db, fdraft.id, { registered_at: null }) === null);
db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ?').run(fdraft.id);
db.prepare(`UPDATE draft_rakuten SET registered_at = NULL, shop_categories_synced_at = NULL,
  shop_categories_synced_key = NULL, shop_categories_error = NULL WHERE draft_id = ?`).run(fdraft.id);

// 承認 (too_few → force) を「確認した内容」に固定する指紋 (Codex R2 high)
const { shopCategorySnapshotHash } = await import('../lib/shop-categories.js');
const snapA = shopCategorySnapshotHash(flat.rows);
check('同期スナップショット: 同じ内容なら同じ指紋', snapA === shopCategorySnapshotHash(flat.rows.slice()));
check('同期スナップショット: 1件減ると変わる (部分取得のすり替えを検知)',
  snapA !== shopCategorySnapshotHash(flat.rows.slice(0, flat.rows.length - 1)));
check('同期スナップショット: 件数が同じでも中身が違えば変わる',
  snapA !== shopCategorySnapshotHash(flat.rows.map((r, i) => (i === 0 ? { ...r, categoryId: '99999' } : r))));
check('同期スナップショット: 先頭に件数が入る (人が読める形)',
  snapA.startsWith(String(flat.rows.length) + '-'), snapA);
check('同期スナップショット: 空配列でも落ちない', typeof shopCategorySnapshotHash([]) === 'string');

check('カテゴリツリー展開: 同一パスの重複は1件だけ採用し duplicates で報告',
  (() => {
    const d = flattenCategoryTrees([{ rootNode: { children: [
      { category: { categoryId: '1', title: '同じ' } },
      { category: { categoryId: '2', title: '同じ' } },
    ] } }]);
    return d.rows.length === 1 && d.duplicates === 1;
  })());
// replaceShopCategories にそのまま渡せる形か (貼り付け取り込みと同じ経路に載る)
const { replaceShopCategories: applyRows } = await import('../lib/shop-categories.js');
const applied = applyRows(db, flat.rows, { force: true });
check('カテゴリツリー展開: replaceShopCategories にそのまま渡せる (categoryIdごと保存される)',
  applied.active >= 6
  && db.prepare(`SELECT category_id FROM ph_shop_categories WHERE path = '生活雑貨・日用品 > 洗濯用品'`).get()?.category_id === '1270',
  JSON.stringify(applied));
db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ?').run(fdraft.id);
dbmod.logEvent(db, fdraft.id, 'shop_categories_saved', '0件', 'smoke'); // 人が全部外した保存
check('店舗内カテゴリ: 0件保存でも everSaved=true (人が外した意思を尊重し再挿入しない)',
  everSavedQuery(fdraft.id) === true);
check('店舗内カテゴリ: 保存後は shopCategoriesNeverSaved=false (AI自動適用をtx内で拒否できる)',
  shopCategoriesNeverSaved(db, fdraft.id) === false);

// ─── P2: AI生成の claim/lease + service-api の書き込みガード (2026-08-03、HTTPレベル) ───
{
  process.env.PH_SERVICE_TOKEN = 'smoke-token-1234567890';
  const express = (await import('express')).default;
  const { serviceApiRouter } = await import('../router.js');
  const app = express();
  app.use('/svc', serviceApiRouter);
  const server = app.listen(0);
  const base = `http://127.0.0.1:${server.address().port}/svc`;
  const call = async (method, path, body) => {
    const res = await fetch(base + path, {
      method,
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer smoke-token-1234567890' },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    return { status: res.status, json: await res.json() };
  };

  // 生成待ちの draft を用意 (材料 = amazon_url + 画像)
  db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, amazon_url, status)
    VALUES ('GEN-1', '生成テスト商品', 'smoke', 'https://www.amazon.co.jp/dp/B0GEN1', 'ready_for_ai')`).run();
  const gdraft = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'GEN-1'`).get();
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'gen-img-1')`).run(gdraft.id);

  // claim: 取得と排他が1回で行われる
  let r = await call('POST', '/generation-queue/claim', { run_id: 'runA', limit: 5 });
  check('P2 claim: ready_for_ai を材料付きで claim できる',
    r.status === 200 && r.json.drafts.some((d) => d.id === gdraft.id)
    && r.json.drafts.find((d) => d.id === gdraft.id).amazon_url === 'https://www.amazon.co.jp/dp/B0GEN1',
    JSON.stringify(r.json).slice(0, 300));
  r = await call('POST', '/generation-queue/claim', { run_id: 'runB', limit: 5 });
  check('P2 claim: lease 中は別 run が取れない (二重生成防止)',
    r.status === 200 && !r.json.drafts.some((d) => d.id === gdraft.id), JSON.stringify(r.json).slice(0, 200));

  // 書き込みガード
  const SIX = {
    rakuten_title: 'タイトル', yahoo_title: 'Yahooタイトル', desc_catch: 'キャッチ',
    desc_features: '特徴', desc_spec: '仕様', desc_notes: '注意',
  };
  r = await call('POST', `/drafts/${gdraft.id}/ai-outputs`, { outputs: SIX, advance: true });
  check('P2 書き込み: run_id なしは 409 (claim していない実行は書けない)', r.status === 409, JSON.stringify(r.json));
  r = await call('POST', `/drafts/${gdraft.id}/ai-outputs`, { run_id: 'runB', outputs: SIX, advance: true });
  check('P2 書き込み: 別 run の claim 中は 409', r.status === 409, JSON.stringify(r.json));
  r = await call('POST', `/drafts/${gdraft.id}/ai-outputs`,
    { run_id: 'runA', outputs: { rakuten_title: 'だけ' }, advance: true });
  check('P2 書き込み: advance は6項目そろわないと 400 (部分生成で review に進めない)',
    r.status === 400 && r.json.error.includes('不足'), JSON.stringify(r.json));

  // 人編集済みの項目は AI が上書きしない
  db.prepare(`INSERT INTO draft_ai_outputs (draft_id, kind, content, edited_by_human)
    VALUES (?, 'desc_notes', '人が書いた注意書き', 1)`).run(gdraft.id);
  r = await call('POST', `/drafts/${gdraft.id}/ai-outputs`, { run_id: 'runA', outputs: SIX, advance: true });
  check('P2 書き込み: 6項目+正しいrun_idで review へ進む',
    r.status === 200 && r.json.advanced === true
    && db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(gdraft.id).status === 'review',
    JSON.stringify(r.json));
  check('P2 書き込み: 人編集済み項目は上書きされない (skipped_human_edited)',
    (r.json.skipped_human_edited || []).includes('desc_notes')
    && db.prepare(`SELECT content FROM draft_ai_outputs WHERE draft_id = ? AND kind = 'desc_notes'`).get(gdraft.id).content === '人が書いた注意書き',
    JSON.stringify(r.json));
  check('P2 書き込み: 完了後 claim は解放される',
    db.prepare('SELECT generation_claim_run_id FROM product_drafts WHERE id = ?').get(gdraft.id).generation_claim_run_id == null);

  // review 中への再書き込みは 409 (人のレビュー中を守る)
  r = await call('POST', `/drafts/${gdraft.id}/ai-outputs`, { run_id: 'runA', outputs: SIX });
  check('P2 書き込み: review 中の draft へは 409 (人のレビューを上書きしない)', r.status === 409, JSON.stringify(r.json));

  // lease 切れの回収 + release
  db.prepare(`UPDATE product_drafts SET status = 'ready_for_ai' WHERE id = ?`).run(gdraft.id);
  await call('POST', '/generation-queue/claim', { run_id: 'runC', limit: 5 });
  db.prepare(`UPDATE product_drafts SET generation_claim_until = '2000-01-01T00:00:00Z' WHERE id = ?`).run(gdraft.id);
  r = await call('POST', '/generation-queue/claim', { run_id: 'runD', limit: 5 });
  check('P2 claim: lease が切れたらハングした実行から取り戻せる',
    r.status === 200 && r.json.drafts.some((d) => d.id === gdraft.id), JSON.stringify(r.json).slice(0, 200));
  r = await call('POST', `/drafts/${gdraft.id}/release`, { run_id: 'runD', reason: 'smoke' });
  check('P2 release: run_id 一致で解放できる', r.status === 200 && r.json.released === true, JSON.stringify(r.json));
  r = await call('POST', '/generation-queue/claim', { run_id: 'runE', limit: 5 });
  check('P2 release 後: 次の実行がすぐ拾える', r.json.drafts.some((d) => d.id === gdraft.id));

  // 書き込み直前のレース (Codex R2 high): 事前チェック後に claim が奪われた状況を
  // acquireGenerationWriteLock 直叩きで再現 — 条件を満たさない限り false = 1バイトも書かれない
  // (gdraft は直前のテストで runE が claim 済み)
  check('P2 原子性: 正しい run は書き込み権を取れて lease も延長される',
    dbmod.acquireGenerationWriteLock(db, gdraft.id, 'runE') === true);
  db.prepare(`UPDATE product_drafts SET generation_claim_run_id = 'runG' WHERE id = ?`).run(gdraft.id);
  check('P2 原子性: claim を奪われた旧 run は書き込み権を取れない (旧runが新runの結果を上書きできない)',
    dbmod.acquireGenerationWriteLock(db, gdraft.id, 'runE') === false);
  db.prepare(`UPDATE product_drafts SET generation_claim_until = '2000-01-01T00:00:00Z' WHERE id = ?`).run(gdraft.id);
  check('P2 原子性: lease 失効後も書き込み権を取れない',
    dbmod.acquireGenerationWriteLock(db, gdraft.id, 'runG') === false);

  // claim 応答の欠落防止 (Codex R2 medium): 他 run が lease 中の draft が多数あっても、
  // 新しく claim できた draft は必ず応答に含まれる (一覧の LIMIT に依存しない ids 直接取得)
  db.prepare(`UPDATE product_drafts SET status = 'ready_for_ai', generation_claim_run_id = 'runH',
    generation_claim_until = '2999-01-01T00:00:00Z' WHERE id = ?`).run(gdraft.id);
  db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, amazon_url, status)
    VALUES ('GEN-2', '生成テスト2', 'smoke', 'https://www.amazon.co.jp/dp/B0GEN2', 'ready_for_ai')`).run();
  const gdraft2 = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'GEN-2'`).get();
  r = await call('POST', '/generation-queue/claim', { run_id: 'runI', limit: 5 });
  check('P2 claim応答: 他runがlease中でも、自分がclaimしたdraftは必ず材料付きで返る',
    r.json.drafts.some((d) => d.id === gdraft2.id) && !r.json.drafts.some((d) => d.id === gdraft.id),
    JSON.stringify(r.json).slice(0, 200));

  // 後始末
  db.prepare(`UPDATE product_drafts SET status = 'draft', generation_claim_run_id = NULL, generation_claim_until = NULL WHERE id IN (?, ?)`).run(gdraft.id, gdraft2.id);
  server.close();
}

// ─── SP広告マニュアルKW: join ロジック (2026-08-04。実測: keywords/list はオートの
// プレースホルダ "(_targeting_auto_)" を含む → マニュアルKWだけ残すのが要点) ───
{
  const { joinSpKeywordsByAsin } = await import('../../keyword-researcher/ads-api.js');
  const byAsin = joinSpKeywordsByAsin(
    [
      { adGroupId: 'g1', asin: 'B0AAAAAAA1', sku: 's1', state: 'ENABLED' },
      { adGroupId: 'g2', asin: 'B0AAAAAAA1', state: 'ENABLED' },   // 同一ASINが複数adGroup
      { adGroupId: 'g3', asin: 'B0BBBBBBB2', state: 'ENABLED' },   // KWなしadGroup
      { adGroupId: 'gX', asin: '', state: 'ENABLED' },
    ],
    [
      { adGroupId: 'g1', keywordText: '平串 30cm', matchType: 'BROAD' },
      { adGroupId: 'g1', keywordText: '(_targeting_auto_)', matchType: 'BROAD' },  // オート→除外
      { adGroupId: 'g2', keywordText: '竹串 業務用', matchType: 'EXACT' },
      { adGroupId: 'g2', keywordText: '平串 30cm', matchType: 'PHRASE' },          // 重複→1つに
      { adGroupId: 'g9', keywordText: '孤児KW' },                                   // 広告なしadGroup
    ],
  );
  const a1 = [...(byAsin.get('B0AAAAAAA1') || [])];
  check('SP広告KW join: 同一ASINの複数adGroupを統合し重複除去',
    a1.length === 2 && a1.includes('平串 30cm') && a1.includes('竹串 業務用'), JSON.stringify(a1));
  check('SP広告KW join: オートのプレースホルダ (_targeting_auto_) は除外', !a1.includes('(_targeting_auto_)'));
  check('SP広告KW join: KWの無いASINは含まれない', !byAsin.has('B0BBBBBBB2'), JSON.stringify([...byAsin.keys()]));
  check('SP広告KW join: 空入力で落ちない', joinSpKeywordsByAsin(null, null).size === 0);
}

// ─── SP広告KWスナップショット (2026-08-04: 全件取得5〜10分→DBに保存して次回claimは即時) ───
{
  const m = new Map([['B0AAAAAAA1', new Set(['平串 30cm', '竹串'])], ['B0BBBBBBB2', new Set(['のぼり'])]]);
  dbmod.saveSpKeywordSnapshot(db, m);
  const loaded = dbmod.loadSpKeywordSnapshot(db);
  check('SP広告KWスナップショット: 保存→読込で ASIN→KW配列が往復する',
    loaded && Array.isArray(loaded.get('B0AAAAAAA1')) && loaded.get('B0AAAAAAA1').includes('平串 30cm')
    && loaded.get('B0BBBBBBB2').length === 1, JSON.stringify([...(loaded || new Map())]));
  // TTL 切れは null (古いKWで生成しない)
  const row = db.prepare('SELECT value FROM ph_intake_state WHERE key = ?').get(dbmod.SP_KW_SNAPSHOT_KEY);
  const stale = JSON.parse(row.value); stale.fetchedAt = '2020-01-01T00:00:00Z';
  db.prepare('UPDATE ph_intake_state SET value = ? WHERE key = ?').run(JSON.stringify(stale), dbmod.SP_KW_SNAPSHOT_KEY);
  check('SP広告KWスナップショット: TTL (7日) 切れは null', dbmod.loadSpKeywordSnapshot(db) === null);
  check('SP広告KWスナップショット: 壊れたJSONでも落ちず null',
    (() => { db.prepare('UPDATE ph_intake_state SET value = ? WHERE key = ?').run('{broken', dbmod.SP_KW_SNAPSHOT_KEY);
             return dbmod.loadSpKeywordSnapshot(db) === null; })());
  db.prepare('DELETE FROM ph_intake_state WHERE key = ?').run(dbmod.SP_KW_SNAPSHOT_KEY);
}

// ─── extractAsin (2026-08-04: 広告KWの材料化に使う ASIN 解決) ───
check('extractAsin: asin列を優先', dbmod.extractAsin({ asin: 'b0abc12345', amazon_url: 'https://www.amazon.co.jp/dp/B0ZZZZZZZZ' }) === 'B0ABC12345');
check('extractAsin: amazon_url の /dp/ から抽出', dbmod.extractAsin({ amazon_url: 'https://www.amazon.co.jp/dp/B0H5QKKD18?th=1' }) === 'B0H5QKKD18');
check('extractAsin: どちらも無ければ null', dbmod.extractAsin({}) === null && dbmod.extractAsin({ asin: '短い', amazon_url: 'https://example.com/x' }) === null);

// ─── 説明文3欄プレビュー (2026-08-03: 楽天の入力欄と同じ最終形をアプリで見る) ───
{
  const { buildDescriptionPreview, composeDescriptions } = await import('../services/rakuten-listing.js');
  const pdraft = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'GEN-1'`).get();
  db.prepare(`INSERT INTO draft_ai_outputs (draft_id, kind, content, edited_by_human) VALUES (?, 'desc_features', '・特徴A\n・特徴B', 0)
    ON CONFLICT(draft_id, kind) DO UPDATE SET content = excluded.content`).run(pdraft.id);
  const pv = buildDescriptionPreview(db, pdraft.id);
  check('3欄プレビュー: 登録要件未達 (ジャンル無し・画像未転送) でも組み立てられる',
    pv.ok === true && typeof pv.pc === 'string' && pv.pc.includes('特徴A'), JSON.stringify(pv).slice(0, 200));
  check('3欄プレビュー: 画像未転送なら販売説明文は空 + 件数が分かる',
    pv.sales === '' && pv.imageCount >= 1 && pv.transferredCount === 0,
    JSON.stringify({ sales: pv.sales, i: pv.imageCount, t: pv.transferredCount }));
  check('3欄プレビュー: スマホ用 = 販売+PC の連結 (販売が空ならPCと同じ)', pv.sp === pv.pc);
  check('3欄プレビュー: 存在しないdraftはok:false', buildDescriptionPreview(db, 999999).ok === false);
  // composeDescriptions が buildItemPayload と同じ入力形で pc/sales/sp を返す (共通化の回帰)
  const comp = composeDescriptions({
    title: 'T', ai: { desc_features: 'F', desc_spec: 'S', desc_notes: 'N' },
    specs: [], pageInfo: null, shippingLabel: null, cabinetLocations: ['/x/a.jpg'],
  });
  check('composeDescriptions: pc に特徴/仕様が入り sales に画像HTML・sp が連結',
    comp.pc.includes('F') && comp.pc.includes('S') && comp.sales.includes('/x/a.jpg')
    && comp.sp === comp.sales + '\n' + comp.pc, JSON.stringify(comp).slice(0, 200));

  // XSS境界 (Codexレビュー提案): 3欄は画面で innerHTML レンダリングされるため、
  // 素材にHTML/属性注入が混ざっても生成関数がすべてエスケープすることを固定する
  const evil = composeDescriptions({
    title: '<script>alert(1)</script>',
    ai: { desc_features: '<img src=x onerror=alert(2)>', desc_notes: '</td><script>alert(3)</script>' },
    specs: [{ spec_key: '<b>鍵</b>', spec_value: '"onmouseover="alert(4)' }],
    pageInfo: null, shippingLabel: null,
    cabinetLocations: ['/dir/"onerror="alert(5)/a.jpg'],
  });
  check('XSS境界: PC欄で素材の生タグが実行形で出ない (全てエスケープ)',
    !evil.pc.includes('<script>') && !evil.pc.includes('<img src=x')
    && !evil.pc.includes('<b>鍵</b>') && evil.pc.includes('&lt;script&gt;')
    && evil.pc.includes('&lt;img src=x onerror=alert(2)&gt;'),
    evil.pc.slice(0, 300));
  check('XSS境界: 販売説明文の画像URLは属性エスケープされ src が閉じない',
    !evil.sales.includes('"onerror="'), evil.sales.slice(0, 300));
}

const renders = [
  ['index.ejs (banner+rows+import panel)', 'index.ejs', {
    title: 't', displayName: 'smoke',
    // ポータル起点と取り込み由来の両方の行を描かせる (出自列の全分岐)
    drafts: [
      draftRow,
      { ...draftRow, id: 999, source: 'notion_import', ne_code: 'IMP-1' },
      { ...draftRow, id: 998, ne_code: 'rooms-l-bk', variation: { kind: 'variation', groupKey: 'rooms', memberCount: 3, isChild: true } },
      { ...draftRow, id: 997, ne_code: 'ZZZ', variation: { kind: 'unknown', groupKey: 'ZZZ', memberCount: 0, isChild: false } },
    ],
    counts, statusFilter: null,
    statuses, statusLabels, notionPending: 1, maxImportCodes: imp.MAX_IMPORT_CODES,
    maxRegisterCodes: intake.MAX_REGISTER_CODES, intake: intake.intakeStatus(), notionCardEnabled: false,
    isAdmin: true, shopCategoryCount: 12, maxShopCategoryLines: shopCat.MAX_SHOP_CATEGORY_LINES,
  }],
  ['index.ejs (empty)', 'index.ejs', {
    title: 't', displayName: 'smoke', drafts: [], counts, statusFilter: 'draft',
    statuses, statusLabels, notionPending: 0, maxImportCodes: imp.MAX_IMPORT_CODES,
    maxRegisterCodes: intake.MAX_REGISTER_CODES, intake: intake.intakeStatus(), notionCardEnabled: true,
    isAdmin: false, shopCategoryCount: 0, maxShopCategoryLines: shopCat.MAX_SHOP_CATEGORY_LINES,
  }],
  ['new.ejs', 'new.ejs', { title: 't', displayName: 'smoke' }],
  ['detail.ejs (full/own_brand)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, own_brand: 1, asin: 'B0TEST', amazon_url: 'https://www.amazon.co.jp/dp/B0TEST' },
    refs: [{ id: 1, url: 'https://example.com/ref' }],
    images: [{ id: 1, drive_file_id: 'x', thumb: 'https://x', view_url: 'https://x' }],
    specs: [{ id: 1, spec_key: 'サイズ', spec_value: 'W10' }],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, k === 'desc_catch' ? { content: 'AI文', generated_at: '2026-07-04', edited_by_human: 0 } : null])),
    events: [{ created_at: '2026-07-04T00:00:00Z', event: 'created', detail: 'x', actor: 'smoke' }],
    gate: ['公式ページURLが未入力です'],
    nextStatuses: ['ready_for_ai', 'on_hold', 'excluded'],
    statusLabels,
    aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.child, hasVariation: { value: true, source: 'ne' },
    regroup: null,
    rakuten: { genre_id: '205761', attributes_json: '[{"name":"ブランド名","values":["x"]}]', article_number: null, registered_at: null, last_error: null, shipping_method_group: '5', postage_included: 1, normal_delivery_date_id: '1000', white_bg_drive_file_id: 'gw', white_bg_drive_url: 'https://drive.google.com/file/d/gw/view', published_at: null }, cabinetImages: [],
    genreDict: { genreId: '205761', genreName: '入浴剤', genrePath: '美容・コスメ > 入浴剤', fixedAt: null, fetchedAt: '2026-07-28T00:00:00Z', attributes: [{ name: 'ブランド名', mandatory: true, inputMethod: 'DESCRIPTIVE', multiValueLimit: 3, maxLength: 100, unit: null, dataType: 'STRING', mandatoryType: 'MANDATORY' }] },
    neCost: { costExTax: 660, shippingCost: 237, shippingMethod: 'ネコポス', taxPercent: 10 }, profitSim: { profit: 189, marginPct: 14.8, costIncTax: 726 }, simTaxPercent: 10, profitTakeRate: 0.9,
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    // 商品ページ表記: 化粧品 + NE推測の配送で全分岐を描かせる
    pageInfo: { product_type: 'cosmetics', content_volume: '50ml', size_text: null, ingredients: '水', usage_notes: null, origin_type: '海外製', origin_country: 'フランス', category_label: '化粧品', seller_name: 'メーカーA', importer_name: '輸入者B', food_name: null, food_ingredients: null, food_expiry: null, food_storage: null },
    pageInfoHtml: '<table><tr><td>x</td></tr></table>',
    neShipping: { group: '5', label: 'ネコポス', guessed: true },
    // 店舗内カテゴリの選択リスト分岐 (有効/選択済み/一覧から外れた選択済み)
    shopCategories: [
      { id: 1, category_id: '100', path: '犬用品 > おやつ', is_active: 1, selected: 1 },
      { id: 2, category_id: null, path: '猫用品 > ケア用品', is_active: 1, selected: 0 },
      { id: 3, category_id: null, path: '廃止された棚', is_active: 0, selected: 1 },
    ],
    yahoo: { yahoo_price: 1980, yahoo_price_sagawa: null, delivery_label: 'ネコポス', tax_rate: '10%', yahoo_category_id: 43494, yahoo_path: 'おもちゃ' },
    imageProduction: { status: '参考画像収集', importance_tier: 'そこそこ力を入れる（6〜8枚）', production_type: null, aplus_content: null, aplus_related: null, camera_instruction_url: null, shipping_status: null, reference_collection: null, designer: '外注_大川さん', page_composer: null, request_text: '依頼文' },
  }],
  ['detail.ejs (rakuten registered + shop categories)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, own_brand: 0, asin: null, amazon_url: null },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['ready_for_ai'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.single, hasVariation: { value: false, source: 'ne' }, regroup: null,
    rakuten: { genre_id: '1', attributes_json: null, article_number: null, registered_at: '2026-07-27T00:00:00Z', last_error: 'IE0418: attr', shipping_method_group: null, postage_included: null, normal_delivery_date_id: null, white_bg_drive_file_id: 'gw', white_bg_drive_url: 'https://x', published_at: null,
      shop_categories_synced_at: '2026-08-02T01:00:00Z', shop_categories_synced_key: '1270', shop_categories_error: null },
    shopCatSyncState: 'stale',   // 反映後に棚を変えた表示の分岐
    cabinetImages: [{ id: 1, drive_file_id: 'gw' }],
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    shopCategories: [
      { id: 1, category_id: null, path: '犬用品 > おやつ', is_active: 1, selected: 1 },
      { id: 2, category_id: null, path: '猫用品', is_active: 1, selected: 1 },
    ],
    yahoo: null, imageProduction: null,
  }],
  ['detail.ejs (rakuten published)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, own_brand: 0, asin: null, amazon_url: null },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['ready_for_ai'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.single, hasVariation: { value: false, source: 'ne' }, regroup: null,
    rakuten: { genre_id: '1', attributes_json: null, article_number: null, registered_at: '2026-07-27T00:00:00Z', last_error: null, shipping_method_group: '7', postage_included: 0, normal_delivery_date_id: null, white_bg_drive_file_id: null, white_bg_drive_url: null, published_at: '2026-07-27T01:00:00Z' },
    cabinetImages: [{ id: 1, drive_file_id: 'g1' }],
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    shopCategories: [],
    yahoo: { yahoo_price: null, yahoo_price_sagawa: null, delivery_label: null, tax_rate: '8%', yahoo_category_id: null, yahoo_path: null },
    imageProduction: null,
  }],
  ['detail.ejs (created notion / non-own-brand)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, status: 'review', notion_card_status: 'created', notion_page_id: 'abcd-ef', has_variation: 1, own_brand: 0, asin: null, amazon_url: null, memo: 'm', price: 1980, jan_code: '49', drive_folder_url: 'https://drive.google.com/drive/folders/x', official_url: 'https://x' },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['approved', 'draft', 'on_hold', 'excluded'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.single, hasVariation: { value: false, source: 'ne' }, regroup: null,
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    yahoo: null, imageProduction: null,
  }],
  ['detail.ejs (notion_import banner + delete)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: {
      ...after, status: 'draft', source: 'notion_import', source_notion_status: '⓪新規商品_高島',
      notion_card_status: 'created', notion_page_id: 'abcd-ef', own_brand: 0, asin: null, amazon_url: null,
    },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: ['AIが参照できるURLがありません (公式ページURL / 参考URL / Amazon URL のどれか1つ)'],
    nextStatuses: ['ready_for_ai', 'on_hold', 'excluded'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.unknown, hasVariation: { value: false, source: 'manual' }, regroup: null,
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    yahoo: null, imageProduction: null,
  }],
  // 子SKU: まとめボタンが出る形 / まとめられない理由が出る形 の両方
  ['detail.ejs (child SKU + regroup button)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, ne_code: 'rooms-l-bk', source: 'portal', notion_page_id: null },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['ready_for_ai'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.child, hasVariation: { value: true, source: 'ne' }, regroup: null,
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    yahoo: null, imageProduction: null,
  }],
  ['detail.ejs (excluded SKU section)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, ne_code: 'rooms', source: 'portal' },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['ready_for_ai'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.withExcluded, hasVariation: { value: true, source: 'ne' }, regroup: null,
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    yahoo: null, imageProduction: null,
  }],
  ['detail.ejs (detached SKU)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, ne_code: 'rooms-m-bk', source: 'portal' },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['ready_for_ai'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.detached, hasVariation: { value: false, source: 'ne' }, regroup: null,
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    yahoo: null, imageProduction: null,
  }],
  ['detail.ejs (child SKU + regroup blocked)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, ne_code: 'rooms-l-bk', source: 'notion_import', notion_page_id: 'p1' },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['ready_for_ai'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.child, hasVariation: { value: true, source: 'ne' },
    regroup: 'Notionから取り込んだ商品はNotion側が正のため、ここでは商品コードを変更できません',
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, ...pageInfoVars,
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    yahoo: null, imageProduction: null,
  }],
];
// 担当者・工程の設定画面。admin / 閲覧のみ / 担当者ゼロ (初回起動時) の 3 分岐を実データで
const staffBase = {
  title: '担当者・工程の設定', displayName: '中原 大輔',
  staff: wf.listStaff({ includeInactive: true }),
  roles: wf.listRoles({ includeInactive: true }),
  steps: wf.listSteps({ includeInactive: true }),
  overview: wf.workflowOverview(),
  staffKinds: dbmod.STAFF_KINDS, staffColors: dbmod.STAFF_COLORS,
  myEmail: 'tanaka@b-faith.biz',
};
renders.push(
  ['staff.ejs (admin)', 'staff.ejs', { ...staffBase, isAdmin: true }],
  ['staff.ejs (閲覧のみ)', 'staff.ejs', { ...staffBase, isAdmin: false }],
  ['staff.ejs (担当者ゼロ)', 'staff.ejs', { ...staffBase, isAdmin: true, staff: [] }],
);
// 工程パネルの権限分岐: 一般ユーザー (担当外なので操作不可) の見た目も描かせる
{
  const d0 = renders.find((r) => r[1] === 'detail.ejs');
  if (d0) {
    renders.push(['detail.ejs (一般ユーザー・担当外)', 'detail.ejs',
      { ...d0[2], isAdmin: false, myStaffId: null }]);
    renders.push(['detail.ejs (一般ユーザー・本人)', 'detail.ejs',
      { ...d0[2], isAdmin: false, myStaffId: wfTanakaId }]);
  }
}
// セット商品まわり: 親 (作成済みセットの一覧あり) と セット側 (仮コード警告あり)
{
  const d0 = renders.find((r) => r[1] === 'detail.ejs');
  if (d0 && wfSetDraftId) {
    renders.push(['detail.ejs (セットの親・作成済み一覧)', 'detail.ejs',
      { ...d0[2], setDrafts: sd.setDraftsOf(db, wfSetParentId), setInfo: null }]);
    renders.push(['detail.ejs (セット商品・仮コード警告)', 'detail.ejs',
      { ...d0[2], setDrafts: [], setInfo: sd.setInfoOf(db, wfSetDraftId) }]);
  }
  // SKU別原価・売価 (2026-08-24): 原価がSKUで異なる分岐 (原価列 + SKU別売価入力 + 保存済み値)
  if (d0) {
    renders.push(['detail.ejs (SKU別原価・売価: costVaries)', 'detail.ejs', {
      ...d0[2],
      draft: { ...d0[2].draft, ne_code: 'rooms', price: 1980 },
      variation: variationFixtures.rep,
      neCost: {
        costExTax: null, shippingCost: 237, shippingMethod: 'ネコポス', taxPercent: 10,
        source: 'members', costVaries: true,
        skuCosts: [
          { code: 'rooms-l-bk', costExTax: 300, shippingCost: 237 },
          { code: 'rooms-l-wh', costExTax: 500, shippingCost: 237 },
          { code: 'rooms-m-bk', costExTax: null, shippingCost: null }, // 原価未設定SKUの「—」分岐
        ],
      },
      profitSim: null,
      skuPrices: { 'rooms-l-bk': 1480 },
    }]);
  }
}
// かんばん。カードあり / 自分の担当者が未紐付け / 空ボード の 3 分岐
const boardBase = {
  title: '工程ボード', displayName: '中原 大輔',
  board: wfp.boardData(db, { mallSummary: ms.mallSummaryFor }), staff: wf.listStaff(),
  me: null, assigneeId: null, assigneeParam: '', unassignedOnly: false, imageKind: null,
  stepStateLabels: wfp.STEP_STATE_LABELS,
  boardView: 'main', imageKindLabels: wfp.IMAGE_KIND_LABELS,
};
renders.push(
  ['board.ejs', 'board.ejs', boardBase],
  ['board.ejs (画像ビュー)', 'board.ejs', {
    ...boardBase, boardView: 'image',
    board: wfp.boardData(db, { view: 'image' }),
  }],
  ['board.ejs (画像ビュー・TOPのみ)', 'board.ejs', {
    ...boardBase, boardView: 'image', imageKind: 'top',
    board: wfp.boardData(db, { view: 'image', imageKind: 'top' }),
  }],
  ['board.ejs (自分のボール・担当者未紐付け)', 'board.ejs', { ...boardBase, assigneeParam: 'me' }],
  ['board.ejs (空)', 'board.ejs', {
    ...boardBase,
    board: { view: 'main', columns: [], doneCards: [], doneTotal: 0, total: 0, truncated: false },
  }],
);
for (const [name, file, data] of renders) {
  try {
    // router が常に渡す共通 locals (画像スロットグリッド・棚の反映状態・自動追加バナー)
    const html = await ejs.renderFile(path.join(views, file),
      {
        thumbnailUrl, fileViewUrl, shopCatSyncState: null,
        rakutenItemUrl: 'https://item.rakuten.co.jp/b-faith/rk-smoke-1/',
        skuImages: [],
        imagePriorities: dbmod.IMAGE_PRIORITIES,
        materialStatuses: dbmod.MATERIAL_STATUSES,
        promptTemplates: { available: true, reason: null, initialJudge: '【入力】<x>', productAnalysis: 'LP {{SUPPLEMENT}}' },
        // 工程パネル (detail.ejs)。fixture 側で上書きできるよう ...data より前に置く
        workflow: wfp.progressOf(wfDraftId, { db }),
        workflowStaff: wf.listStaff(),
        stepStateLabels: wfp.STEP_STATE_LABELS,
        isAdmin: true, myStaffId: null,
        mallStatus: ms.mallStatusOf(wfDraftId, { db }),
        setDrafts: [], setInfo: null,
        skuPrices: {},
        trailingBanners: [
          { location: listing.SHIPPING_BANNER_LOCATIONS['5'], label: '配送: ネコポス', url: listing.cabinetImageUrl(listing.SHIPPING_BANNER_LOCATIONS['5']) },
          ...listing.COMMON_TRAILING_BANNERS.map((b) => ({ ...b, url: listing.cabinetImageUrl(b.location) })),
        ],
        ...data,
      });
    check(`render ${name}`, html.length > 500);
  } catch (e) {
    check(`render ${name}`, false, e.message);
  }
}

// ─── EJS 内クライアントJSの構文チェック ───
// レンダリングは通っても <script> 内の構文エラー (例: 文字列リテラル内の生改行) は検出できず、
// ボタン全滅の形で本番に出る (#700 の confirm 事故)。EJS タグを無害値に置換して構文だけ検証する。
// ─── Notion 画像DB (商品ページ商品画像登録) の移植 (2026-08-26) ───
{
  const iimp = await import('../services/notion-image-import.js');
  const wfp2 = await import('../lib/workflow-progress.js');
  const takashima = wf.createStaff({ name: '高島さん', kind: 'internal' });

  const sel = (name) => ({ type: 'select', select: name ? { name } : null });
  const url = (u) => ({ type: 'url', url: u });
  const rtx = (t) => ({ type: 'rich_text', rich_text: t ? [{ plain_text: t }] : [] });
  const imgPage = (code, status, extra = {}) => ({
    id: `img-page-${code}-${status}`,
    properties: {
      Name: { type: 'title', title: [{ plain_text: `画像商品 ${code}` }] },
      '商品コード': rtx(code),
      Status: sel(status),
      'グーグルドライブURL': url('https://drive.google.com/drive/folders/1ABC'),
      AmazonURL: url('https://www.amazon.co.jp/dp/B0IMG1'),
      ASIN: rtx('B0IMG1'),
      '重要商品区分': sel('そこそこ力を入れる（6〜8枚）'),
      '撮影商品発送': sel('社内準備'),
      'カメラ撮影指示URL': url('https://docs.google.com/spreadsheets/d/xyz'),
      Canva: url('https://canva.link/abc'),
      '依頼文': { type: 'formula', formula: { type: 'string', string: 'お世話になっています。画像作成お願いします' } },
      '画像作成担当者': sel(null),
      ...extra,
    },
  });

  // buildImageRecord
  const r0 = iimp.buildImageRecord(imgPage('IMG-REC', '構成作成中', { Canva: url('javascript:alert(1)') }));
  check('画像DB: レコード変換 (コード・名前・URL・区分・依頼文)',
    r0.ne_code === 'IMG-REC' && r0.name === '画像商品 IMG-REC' && r0.status === '構成作成中'
    && r0.drive_folder_url === 'https://drive.google.com/drive/folders/1ABC' && r0.asin === 'B0IMG1'
    && r0.importance_tier === 'そこそこ力を入れる（6〜8枚）' && r0.shipping_status === '社内準備'
    && r0.request_text.startsWith('お世話になっています') && typeof r0.source_hash === 'string' && r0.source_hash.length === 32,
    JSON.stringify(r0));
  check('画像DB: http(s) 以外の URL は捨てる', r0.canva_url === null);
  check('画像DB: 商品コード無しは null', iimp.buildImageRecord({ id: 'x', properties: { Name: { type: 'title', title: [] } } }) === null);

  // planStepsFor
  const p1 = iimp.planStepsFor('構成作成中');
  check('画像DB: 構成作成中 → ① done・② 構成 doing (TOP 側は書かない)',
    p1.steps.some((s) => s.code === 'imgd_request' && s.state === 'done')
    && p1.steps.some((s) => s.code === 'imgd_compose' && s.state === 'doing')
    && !p1.steps.some((s) => s.code === 'imgd_material') && !p1.steps.some((s) => s.code.endsWith('_top')) && !p1.hold, JSON.stringify(p1));
  const p2 = iimp.planStepsFor('画像作成中（高島）');
  check('画像DB: 画像作成中 → ①〜④ done・⑤ デザイン修正 doing・担当=高島',
    p2.steps.filter((s) => s.state === 'done').length === 4
    && p2.steps.some((s) => s.code === 'imgd_design' && s.state === 'doing')
    && p2.assigneeName === '高島' && p2.assigneeStepCode === 'imgd_design', JSON.stringify(p2));
  const p3 = iimp.planStepsFor('画像確認（田中）');
  check('画像DB: 画像確認 → ①〜⑤ done・⑥-1 社内確認 (田中) doing・担当=田中',
    p3.steps.filter((s) => s.state === 'done').length === 5
    && p3.steps.some((s) => s.code === 'imgd_review_1' && s.state === 'doing')
    && !p3.steps.some((s) => s.code === 'imgd_review_2')
    && p3.assigneeName === '田中' && p3.assigneeStepCode === 'imgd_review_1', JSON.stringify(p3));
  const p4 = iimp.planStepsFor('保留', {});
  check('画像DB: 保留 → 工程なし・画像制作だけ保留', p4.hold && p4.steps.length === 0);
  check('画像DB: 対象外ステータスは何も書かない', iimp.planStepsFor('完了', {}).steps.length === 0);

  // 担当者の名寄せ
  check('画像DB: 担当者名寄せ 高島 → 高島さん (完全一致)', iimp.findStaffByName(db, '高島')?.staff?.id === takashima);
  const tanakaId = db.prepare("SELECT id FROM ph_staff WHERE name = '田中美祐'").get()?.id;
  check('画像DB: 担当者名寄せ 田中 → 田中美祐 (部分一致)', iimp.findStaffByName(db, '田中')?.staff?.id === tanakaId);
  check('画像DB: 見つからない名前は未割当', iimp.findStaffByName(db, '存在しない').staff === null);

  // 既存ドラフト (portal 起点・重要度未設定) と、重要度が自社以外のドラフト
  const insDraft = db.prepare(`INSERT INTO product_drafts (ne_code, name, status, source, image_priority, own_brand)
    VALUES (?, ?, 'draft', 'portal', ?, ?)`);
  const existId = Number(insDraft.run('IMG-EXIST-1', '既存商品', null, 0).lastInsertRowid);
  const confId = Number(insDraft.run('IMG-CONF-1', '重要度衝突', '仕入商品（重要度：低）', 0).lastInsertRowid);
  const conf2Id = Number(insDraft.run('IMG-CONF-2', '逆方向の不整合', '自社商品（重要度：高）', 0).lastInsertRowid);
  insProd.run(9101, 'IMG-VCONF', '表記ゆれ A', null);
  insProd.run(9102, 'img-vconf', '表記ゆれ B', null);
  check('画像DB: 前提 = mirror 表記ゆれは conflict', vari.resolveVariationGroup(db, 'IMG-VCONF').kind === 'conflict');
  db.prepare("UPDATE product_drafts SET amazon_url = 'https://www.amazon.co.jp/dp/B0EXIST' WHERE id = ?").run(existId);

  const schemaImg = { properties: { Status: { type: 'select', select: { options: [
    { name: '構成作成中' }, { name: '画像作成中（高島）' }, { name: '画像確認（田中）' }, { name: '保留' }, { name: '完了' }, { name: 'A+コンテンツ作成' },
  ] } } } };
  const pagesImg = [
    imgPage('IMG-NEW-1', '構成作成中'),
    imgPage('IMG-EXIST-1', '画像作成中（高島）', { '撮影商品発送': sel('撮影依頼不要') }),
    imgPage('IMG-MASTER-1', '画像確認（田中）'),
    imgPage('rooms-l-wh', '保留'),
    imgPage('IMG-CONF-1', '構成作成中'),
    imgPage('IMG-DUP-1', '構成作成中'),
    imgPage('IMG-DUP-1', '保留'),
    imgPage('IMG-VCONF', '構成作成中'),   // NE 側に同じコードが複数 (mirror の表記ゆれ) → 止める
    imgPage('IMG-CONF-2', '構成作成中'),
    { id: 'img-page-nocode', properties: { Name: { type: 'title', title: [{ plain_text: 'コード無し' }] } } },
  ];
  let capImg = null;
  const diImg = {
    config: () => ({ databaseId: 'img-db-test' }),
    request: async () => schemaImg,
    query: async (opts) => { capImg = opts; return { pages: pagesImg }; },
    masterFinder: async (code) => (code === 'IMG-MASTER-1'
      ? { id: 'master-1', properties: { Status: { type: 'select', select: { name: '②商品タイトル_大輔' } } } } : null),
    runId: 'img-smoke-1',
  };
  const beforeDrafts = db.prepare('SELECT COUNT(*) c FROM product_drafts').get().c;
  const prevImg = await iimp.importImageDbByStatus({ actor: 'smoke', ...diImg });
  check('画像DB: フィルタは対象4ステータスだけ (完了・A+ を含まない)',
    capImg.filter.or.length === 4 && capImg.filter.or.every((f) => iimp.IMAGE_MIGRATE_STATUSES.includes(f.select.equals)),
    JSON.stringify(capImg.filter));
  const byCode = (res, code, status) => res.results.find((r) => r.ne_code === code && (!status || r.notion_status === status));
  check('画像DB: dryRun は書き込まず分類だけ返す',
    prevImg.summary.would_create === 2 && prevImg.summary.would_update === 1
    && prevImg.summary.needs_master_import === 1 && prevImg.summary.brand_priority_conflict === 2
    && prevImg.summary.duplicate === 2 && prevImg.summary.variation_conflict === 1
    && prevImg.summary.failed === 1 && prevImg.total === 10
    && db.prepare('SELECT COUNT(*) c FROM product_drafts').get().c === beforeDrafts
    && db.prepare('SELECT COUNT(*) c FROM draft_image_notion_imports').get().c === 0,
    JSON.stringify(prevImg.summary));
  check('画像DB: 重複コードは全カード止める・NE 表記ゆれは variation_conflict・own_brand=0×重要度自社も止める',
    prevImg.results.filter((r) => r.ne_code === 'IMG-DUP-1').every((r) => r.outcome === 'duplicate')
    && byCode(prevImg, 'IMG-VCONF').outcome === 'variation_conflict'
    && byCode(prevImg, 'IMG-CONF-2').outcome === 'brand_priority_conflict');
  check('画像DB: 商品マスターに居る商品は本体取り込みを先に (ブロック)',
    byCode(prevImg, 'IMG-MASTER-1').outcome === 'needs_master_import' && byCode(prevImg, 'IMG-MASTER-1').master_status === '②商品タイトル_大輔');
  check('画像DB: 子SKU は detach の予告が出る',
    byCode(prevImg, 'rooms-l-wh').outcome === 'would_create'
    && byCode(prevImg, 'rooms-l-wh').warnings.some((w) => w.includes('独立ページ')));
  check('画像DB: 既存の Amazon URL は上書きせず、空欄 (Drive/ASIN) だけ補完予定',
    byCode(prevImg, 'IMG-EXIST-1').plan_summary.includes('drive_folder_url')
    && byCode(prevImg, 'IMG-EXIST-1').plan_summary.includes('asin')
    && !byCode(prevImg, 'IMG-EXIST-1').plan_summary.includes('amazon_url'));

  // 実行: snapshot 無し・不一致は止める
  let mis = null;
  try { await iimp.importImageDbByStatus({ actor: 'smoke', dryRun: false, ...diImg }); } catch (e) { mis = e; }
  check('画像DB: snapshot 無しの実行は拒否 (書き込みなし)',
    mis?.code === 'snapshot_mismatch' && db.prepare('SELECT COUNT(*) c FROM draft_image_notion_imports').get().c === 0);

  const runImg = await iimp.importImageDbByStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: prevImg.snapshot, ...diImg });
  check('画像DB: 実行 → 新規2・補完1 (衝突・重複は書かない)',
    runImg.summary.created === 2 && runImg.summary.updated === 1 && runImg.summary.failed === 1
    && !db.prepare('SELECT 1 FROM product_drafts WHERE ne_code IN (?, ?)').get('IMG-DUP-1', 'IMG-VCONF')
    && db.prepare('SELECT own_brand FROM product_drafts WHERE id = ?').get(conf2Id).own_brand === 0,
    JSON.stringify(runImg.results.map((r) => [r.ne_code, r.outcome, r.error, r.warnings])));
  const newRow = db.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get('IMG-NEW-1');
  check('画像DB: 新規は最小情報 + 自社商品 + 画像フォルダ',
    newRow && newRow.own_brand === 1 && newRow.image_priority === '自社商品（重要度：高）' && newRow.source === 'notion_import'
    && newRow.drive_folder_url === 'https://drive.google.com/drive/folders/1ABC' && newRow.asin === 'B0IMG1' && newRow.price == null,
    JSON.stringify(newRow));
  const newIp = db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(newRow.id);
  check('画像DB: 画像制作情報 (区分・撮影指示・Canva・依頼文・原文ステータス)',
    newIp && newIp.importance_tier === 'そこそこ力を入れる（6〜8枚）' && newIp.canva_url === 'https://canva.link/abc'
    && newIp.camera_instruction_url === 'https://docs.google.com/spreadsheets/d/xyz' && newIp.status === '構成作成中'
    && newIp.workflow_state === 'active', JSON.stringify(newIp));
  const stepOf = (id, code) => db.prepare('SELECT state, assignee_id FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(id, code);
  check('画像DB: 構成作成中 → ① done・② doing・撮影・素材=社内準備 (新規)',
    stepOf(newRow.id, 'imgd_request')?.state === 'done' && stepOf(newRow.id, 'imgd_compose')?.state === 'doing'
    && stepOf(newRow.id, 'imgd_material')?.state === 'todo' && stepOf(newRow.id, 'img_request_top')?.state === 'todo'
    && newIp.material_status === 'internal_prep');
  const exRow = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(existId);
  check('画像DB: 既存は空欄だけ補完 (Amazon URL は据え置き・自社商品に)',
    exRow.amazon_url === 'https://www.amazon.co.jp/dp/B0EXIST' && exRow.drive_folder_url === 'https://drive.google.com/drive/folders/1ABC'
    && exRow.own_brand === 1 && exRow.image_priority === '自社商品（重要度：高）' && exRow.name === '既存商品' && exRow.source === 'portal');
  check('画像DB: 画像作成中 → ①〜④ done・⑤ doing・担当=高島・撮影・素材=撮影不要 (既存)',
    stepOf(existId, 'imgd_request')?.state === 'done' && stepOf(existId, 'imgd_ai')?.state === 'done'
    && stepOf(existId, 'imgd_design')?.state === 'doing' && stepOf(existId, 'imgd_design')?.assignee_id === takashima
    && db.prepare('SELECT material_status FROM draft_image_production WHERE draft_id = ?').get(existId)?.material_status === 'not_required',
    JSON.stringify([stepOf(existId, 'imgd_request'), stepOf(existId, 'imgd_design')]));
  check('画像DB: 重要度が自社以外の商品は触らない',
    db.prepare('SELECT image_priority, own_brand FROM product_drafts WHERE id = ?').get(confId).image_priority === '仕入商品（重要度：低）'
    && !db.prepare('SELECT 1 FROM draft_image_notion_imports WHERE draft_id = ?').get(confId));
  const roomsRow = db.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get('rooms-l-wh');
  check('画像DB: 子SKU は独立ページ (detach 記録) + 画像制作だけ保留 (商品 status は draft のまま)',
    roomsRow && roomsRow.status === 'draft'
    && vari.resolveVariationGroup(db, 'rooms-l-wh').kind === 'detached'
    && db.prepare('SELECT workflow_state, hold_note FROM draft_image_production WHERE draft_id = ?').get(roomsRow.id)?.workflow_state === 'on_hold'
    && db.prepare("SELECT COUNT(*) c FROM draft_step_progress WHERE draft_id = ? AND state != 'todo'").get(roomsRow.id).c === 0,
    JSON.stringify(roomsRow));
  check('画像DB: 台帳に 1 カード 1 行 (成功分だけ)',
    db.prepare("SELECT COUNT(*) c FROM draft_image_notion_imports WHERE import_run_id = 'img-smoke-1'").get().c === 3
    && db.prepare('SELECT source_status FROM draft_image_notion_imports WHERE notion_page_id = ?').get('img-page-rooms-l-wh-保留')?.source_status === '保留');
  check('画像DB: イベントが残る', db.prepare("SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'image_db_imported'").get(newRow.id).c === 1
    && db.prepare("SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'image_hold'").get(roomsRow.id).c === 1);

  // 画像制作の保留 = 楽天出品ゲートが閉じる。解除で開く (工程の理由に戻る)
  const gateHold = wfp2.imageTrackBlockReason(db, roomsRow.id);
  check('画像DB: 保留中は出品ゲートが「保留」で止まる', typeof gateHold === 'string' && gateHold.includes('保留'), gateHold);
  const rel = dbmod.setImageWorkflowState(db, roomsRow.id, 'active', { actor: 'smoke' });
  const gateAfter = wfp2.imageTrackBlockReason(db, roomsRow.id);
  check('画像DB: 保留解除 → ゲートは工程の理由に戻る (冪等)',
    rel.changed === true && typeof gateAfter === 'string' && !gateAfter.includes('保留中')
    && dbmod.setImageWorkflowState(db, roomsRow.id, 'active', { actor: 'smoke' }).changed === false, gateAfter);
  let badState = null;
  try { dbmod.setImageWorkflowState(db, roomsRow.id, 'bogus', {}); } catch (e) { badState = e; }
  check('画像DB: 不正な状態は拒否', !!badState);

  // 再実行: 移植済みはスキップ、内容が変わったカードは報告のみ (追従しない)
  const prev2 = await iimp.importImageDbByStatus({ actor: 'smoke', ...diImg });
  check('画像DB: 再プレビューは移植済み 3 (書き込み予定 0)',
    prev2.summary.already_migrated === 3 && prev2.summary.would_create === 0 && prev2.summary.would_update === 0, JSON.stringify(prev2.summary));
  const changedPages = pagesImg.map((p) => (p.id === 'img-page-IMG-NEW-1-構成作成中'
    ? { ...p, properties: { ...p.properties, Status: sel('画像作成中（高島）') } } : p));
  const prev3 = await iimp.importImageDbByStatus({ actor: 'smoke', ...diImg, query: async () => ({ pages: changedPages }) });
  check('画像DB: 移植後に Notion 側が変わっても追従せず報告だけ',
    byCode(prev3, 'IMG-NEW-1', '画像作成中（高島）')?.outcome === 'source_changed_after_migration'
    && stepOf(newRow.id, 'imgd_design')?.state === 'todo');

  // 人が動かした工程は上書きしない
  const untouchedId = Number(insDraft.run('IMG-TOUCHED-1', '触った商品', null, 0).lastInsertRowid);
  wfp2.setStepState(untouchedId, 'img_request_top', { note: '手で準備中' }, 'smoke', ADMIN);
  const prev4 = await iimp.importImageDbByStatus({
    actor: 'smoke', ...diImg, query: async () => ({ pages: [imgPage('IMG-TOUCHED-1', '画像確認（田中）')] }),
  });
  check('画像DB: 担当・メモが入っている画像工程は書かない (警告)',
    byCode(prev4, 'IMG-TOUCHED-1').outcome === 'would_update'
    && byCode(prev4, 'IMG-TOUCHED-1').warnings.some((w) => w.includes('工程は書きません')));

  // 保留: プレビュー後に人が保留状態を変えたら実行は止まる (snapshot 不一致)
  {
    const holdId = Number(insDraft.run('IMG-HOLD-1', '保留テスト', null, 0).lastInsertRowid);
    const diHold = { ...diImg, query: async () => ({ pages: [imgPage('IMG-HOLD-1', '保留')] }) };
    const ph = await iimp.importImageDbByStatus({ actor: 'smoke', ...diHold });
    dbmod.setImageWorkflowState(db, holdId, 'on_hold', { note: '人が保留', actor: 'smoke' });
    let holdMis = null;
    try { await iimp.importImageDbByStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: ph.snapshot, ...diHold }); } catch (e) { holdMis = e; }
    check('画像DB: プレビュー後に保留状態が変わったら実行を止める', holdMis?.code === 'snapshot_mismatch');
    const ph2 = await iimp.importImageDbByStatus({ actor: 'smoke', ...diHold });
    check('画像DB: 既に保留中なら「台帳に記録だけ」',
      byCode(ph2, 'IMG-HOLD-1').outcome === 'would_ledger_only' || byCode(ph2, 'IMG-HOLD-1').outcome === 'would_update',
      byCode(ph2, 'IMG-HOLD-1').outcome);
    const ph3 = await iimp.importImageDbByStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: ph2.snapshot, ...diHold });
    check('画像DB: 保留中のまま実行 → 保留を維持 (人の理由を上書きしない)',
      (ph3.summary.ledger_only + ph3.summary.updated) === 1
      && db.prepare('SELECT hold_note FROM draft_image_production WHERE draft_id = ?').get(holdId).hold_note === '人が保留');
  }

  // 対象ステータスの一部が Notion に無い → プレビューは通るが実行は止める
  {
    const partial = { properties: { Status: { type: 'select', select: { options: [{ name: '構成作成中' }, { name: '完了' }] } } } };
    const diPart = { ...diImg, request: async () => partial, query: async () => ({ pages: [imgPage('IMG-PART-1', '構成作成中')] }) };
    const pp = await iimp.importImageDbByStatus({ actor: 'smoke', ...diPart });
    let partErr = null;
    try { await iimp.importImageDbByStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: pp.snapshot, ...diPart }); } catch (e) { partErr = e; }
    check('画像DB: 選択肢が一部欠けていたらプレビューで報告し実行は止める',
      pp.missingStatuses.length === 3 && partErr?.code === 'missing_statuses'
      && !db.prepare('SELECT 1 FROM product_drafts WHERE ne_code = ?').get('IMG-PART-1'));
  }

  // Status の選択肢が全部消えていたら fail-closed
  let noOpt = null;
  try {
    await iimp.importImageDbByStatus({ actor: 'smoke', ...diImg, request: async () => ({ properties: { Status: { type: 'select', select: { options: [{ name: '完了' }] } } } }) });
  } catch (e) { noOpt = e; }
  check('画像DB: 対象ステータスが Notion に無ければ止める (0件成功にしない)', !!noOpt && /見つかりません/.test(noOpt.message));

  // 画像制作 upsert に canva_url が乗る (部分更新で他列を消さない)
  dbmod.upsertImageProduction(db, newRow.id, { canva_url: 'https://canva.link/new' });
  const ip2 = db.prepare('SELECT canva_url, importance_tier, workflow_state FROM draft_image_production WHERE draft_id = ?').get(newRow.id);
  check('画像DB: canva_url の部分更新で他列・保留状態を消さない',
    ip2.canva_url === 'https://canva.link/new' && ip2.importance_tier === 'そこそこ力を入れる（6〜8枚）' && ip2.workflow_state === 'active');

  // 保留中は画像工程を動かせない (本流は動く)。解除で動く
  dbmod.setImageWorkflowState(db, roomsRow.id, 'on_hold', { note: 'テスト保留', actor: 'smoke' });
  let holdStep = null;
  try { wfp2.setStepState(roomsRow.id, 'img_request_top', { state: 'doing' }, 'smoke', ADMIN); } catch (e) { holdStep = e; }
  check('画像DB: 保留中は画像工程を動かせない (400)', holdStep?.status === 400 && /保留中/.test(holdStep.message)
    && stepOf(roomsRow.id, 'img_request_top')?.state === 'todo', holdStep?.message);
  let holdMove = null;
  try { wfp2.moveBoardCard(roomsRow.id, { view: 'image', kind: 'top', to: 'production', expectedCurrent: 'img_request_top' }, 'smoke', ADMIN); } catch (e) { holdMove = e; }
  check('画像DB: 保留中はボード D&D も止まる', !!holdMove && /保留中/.test(holdMove.message), holdMove?.message);
  check('画像DB: 保留中でも本流工程は動く', wfp2.setStepState(roomsRow.id, 'basic_info', { note: '本流メモ' }, 'smoke', ADMIN).changed === true);
  dbmod.setImageWorkflowState(db, roomsRow.id, 'active', { actor: 'smoke' });
  check('画像DB: 解除後は画像工程が動く', wfp2.setStepState(roomsRow.id, 'img_request_top', { state: 'doing' }, 'smoke', ADMIN).changed === true);
  wfp2.setStepState(roomsRow.id, 'img_request_top', { state: 'todo' }, 'smoke', ADMIN);

  // プレビュー後に人が空欄を埋めた → 実行はそのカードだけ失敗し台帳に載らない
  {
    const raceId = Number(insDraft.run('IMG-RACE-1', '競合テスト', null, 0).lastInsertRowid);
    const diRace = { ...diImg, query: async () => ({ pages: [imgPage('IMG-RACE-1', '構成作成中')] }) };
    const pr = await iimp.importImageDbByStatus({ actor: 'smoke', ...diRace });
    db.prepare("UPDATE product_drafts SET drive_folder_url = 'https://drive.google.com/drive/folders/HUMAN' WHERE id = ?").run(raceId);
    let raceRun = null;
    try { raceRun = await iimp.importImageDbByStatus({ actor: 'smoke', dryRun: false, expectedSnapshot: pr.snapshot, ...diRace }); } catch (e) { raceRun = { err: e }; }
    check('画像DB: プレビュー後に人が入力した項目があれば snapshot 不一致か、そのカードだけ失敗 (台帳に載らない)',
      (raceRun.err?.code === 'snapshot_mismatch' || byCode(raceRun, 'IMG-RACE-1')?.outcome === 'failed')
      && !db.prepare('SELECT 1 FROM draft_image_notion_imports WHERE draft_id = ?').get(raceId)
      && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(raceId).drive_folder_url.endsWith('HUMAN'),
      JSON.stringify(raceRun.err ? raceRun.err.code : raceRun.results));
  }

  // ボード: 保留カードにフラグ (画像ビュー)
  dbmod.setImageWorkflowState(db, roomsRow.id, 'on_hold', { note: 'テスト保留', actor: 'smoke' });
  const bImg = wfp2.boardData(db, { view: 'image' });
  const holdCard = [...bImg.columns.flatMap((c) => c.cards), ...bImg.doneCards].find((c) => c.id === roomsRow.id);
  check('画像DB: 画像ビューのカードに保留フラグと理由が乗る (滞留日数は付けない)',
    holdCard && holdCard.imageOnHold === true && holdCard.imageHoldNote === 'テスト保留' && holdCard.kindStalledDays == null,
    JSON.stringify(holdCard && { imageOnHold: holdCard.imageOnHold, note: holdCard.imageHoldNote, stalled: holdCard.kindStalledDays }));
  dbmod.setImageWorkflowState(db, roomsRow.id, 'active', { actor: 'smoke' });
}

{
  const fs = await import('node:fs');
  const vm = await import('node:vm');
  for (const f of fs.readdirSync(views).filter((n) => n.endsWith('.ejs'))) {
    const src = fs.readFileSync(path.join(views, f), 'utf8');
    const blocks = [...src.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
    if (blocks.length === 0) continue;
    const js = blocks.join('\n').replace(/<%[-=]?[\s\S]*?%>/g, '0');
    try {
      // vm.Script はコンパイルのみで実行しない = 構文チェック専用
      new vm.Script(js, { filename: f });
      check(`client-js syntax ${f}`, true);
    } catch (e) {
      check(`client-js syntax ${f}`, false, e.message);
    }
  }
}

console.log(failed === 0 ? '\nSMOKE: ALL PASS' : `\nSMOKE: ${failed} FAILED`);
process.exit(failed === 0 ? 0 : 1);
