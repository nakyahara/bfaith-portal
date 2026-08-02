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
check('thumbnail url', thumbnailUrl('abc', 160).includes('sz=w160'));

// ─── folder-import (画像フォルダ→スロット割当、2026-08-01) ───
const { assignImageSlots, parseNumberedName, MAX_IMAGE_SLOTS } = await import('../lib/folder-import.js');
check('MAX_IMAGE_SLOTS = 楽天の登録口20', MAX_IMAGE_SLOTS === 20);
check('parseNumberedName 基本形', JSON.stringify(parseNumberedName('abc_01.jpg')) === '{"base":"abc","num":1}');
check('parseNumberedName 番号なし', parseNumberedName('abc.jpg') === null);
check('parseNumberedName 拡張子なし', parseNumberedName('abc_01') === null);
check('parseNumberedName ゼロ埋めなし _5', parseNumberedName('abc_5.png')?.num === 5);

const fim = (name, mimeType = 'image/jpeg') => ({ id: 'id-' + name, name, mimeType });
let asn = assignImageSlots([fim('code_02.png'), fim('code_00.jpg'), fim('code_01.jpg')], 'code');
check('_00→白抜き / _01,_02→スロット1,2 (番号順)',
  asn.whiteBg?.id === 'id-code_00.jpg' && asn.slots.length === 2
  && asn.slots[0].slot === 1 && asn.slots[0].id === 'id-code_01.jpg' && asn.slots[1].slot === 2,
  JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('メモ.jpg'), fim('code_21.jpg'), fim('code_02.txt', 'text/plain')], 'code');
check('番号なし/上限超え/非画像 → skipped',
  asn.slots.length === 1 && asn.skipped.length === 3
  && asn.skipped.some((s) => s.name === 'code_21.jpg'), JSON.stringify(asn.skipped));

asn = assignImageSlots([fim('code_01.jpg'), fim('other_01.jpg'), fim('other_02.jpg')], 'code');
check('商品コード一致だけを採用 (他コードは除外・conflictにしない)',
  asn.slots.length === 1 && asn.slots[0].id === 'id-code_01.jpg' && asn.conflicts.length === 0
  && asn.skipped.length === 2, JSON.stringify(asn));

asn = assignImageSlots([fim('xxx_01.jpg'), fim('xxx_03.jpg')], 'code');
check('コード一致ゼロは fail-closed (codeMatched=false・何も採用しない)',
  asn.codeMatched === false && asn.slots.length === 0 && asn.whiteBg === null
  && asn.skipped.length === 2, JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('code_04.jpg')], 'code');
check('欠番は missingNums で報告 (_02,_03)',
  JSON.stringify(asn.missingNums) === '[2,3]' && asn.slots.length === 2, JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('code_01.png')], 'code');
check('同一番号の重複 → conflicts (セットしない)',
  asn.slots.length === 0 && asn.conflicts.length === 1 && asn.conflicts[0].num === 1, JSON.stringify(asn));

asn = assignImageSlots([fim('code_01.jpg'), fim('code_21.jpg'), fim('code_21.png')], 'code');
check('上限超え番号は重複でも conflicts にしない (skippedで_01は取り込める)',
  asn.slots.length === 1 && asn.conflicts.length === 0 && asn.skipped.length === 2, JSON.stringify(asn));

asn = assignImageSlots([fim('CODE_01.JPG')], 'code');
check('大文字小文字は同一視', asn.slots.length === 1, JSON.stringify(asn));

asn = assignImageSlots([fim('code_0.jpg'), fim('code_20.jpg')], 'code');
check('_0 は白抜き / _20 は最終スロット', asn.whiteBg?.id === 'id-code_0.jpg' && asn.slots[0]?.slot === 20, JSON.stringify(asn));

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

let reasons = dbmod.gateReasons(db, draft);
check('gate blocks (no url/image)', reasons.length === 2, JSON.stringify(reasons));

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

// ─── 自動差し戻し (Codex R1 high: ゲートすり抜け防止) ───
db.prepare(`UPDATE product_drafts SET status = 'ready_for_ai' WHERE id = ?`).run(draft.id);
check('demote no-op while gate ok', dbmod.demoteIfGateBroken(db, draft.id, 'smoke') === null);
db.prepare(`UPDATE product_drafts SET official_url = NULL WHERE id = ?`).run(draft.id);
const demoteReasons = dbmod.demoteIfGateBroken(db, draft.id, 'smoke');
check('demote fires when url cleared', Array.isArray(demoteReasons) && demoteReasons.length === 1);
check('demoted back to draft', db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(draft.id).status === 'draft');
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

built = listing.buildItemPayload(db, rkId);
check('payload: 組み立て成功', built.ok === true, JSON.stringify(built.reasons || null));
const pl = built.payload;
check('payload: hideItem=true 固定 (非公開登録のみ)', pl.hideItem === true);
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
check('payload: 画像は CABINET location', pl.images.length === 1 && pl.images[0].type === 'CABINET' && pl.images[0].location === '/app-newitems/rk-smoke-1-1.jpg');
check('payload: variants は ne_code キー + 属性 + 型番なし例外',
  pl.variants['rk-smoke-1'].standardPrice === 1980
  && pl.variants['rk-smoke-1'].articleNumber.exemptionReason === 1
  && pl.variants['rk-smoke-1'].attributes[0].name === 'ブランド名',
  JSON.stringify(pl.variants));

// メーカー型番があれば value で送る
db.prepare(`UPDATE draft_rakuten SET article_number = 'ABC-100' WHERE draft_id = ?`).run(rkId);
check('payload: 型番があれば value で送る',
  listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber.value === 'ABC-100');

// ─── 2026-07-27 出品仕様: 税率 / JAN / 配送 / 納期 / 白抜き / 画像20枚 ───
check('taxRateToPayment: 8% → payment.taxRate 0.08', listing.taxRateToPayment('8%')?.taxRate === 0.08);
check('taxRateToPayment: 10%/未設定/変値は送らない (店舗デフォルト)',
  listing.taxRateToPayment('10%') === null && listing.taxRateToPayment(null) === null && listing.taxRateToPayment('9.6') === null);

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
check('payload: 配送方法グループ + 送料込み', rkVar.shipping?.shippingMethodGroup === '5' && rkVar.shipping?.postageIncluded === true);
check('payload: 納期情報ID は数値で送る', rkVar.normalDeliveryDateId === 1000);

dbmod.upsertDraftYahoo(db, rkId, { tax_rate: '10%' });
check('payload: 10% は payment を送らない', !('payment' in listing.buildItemPayload(db, rkId).payload));

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
// 未登録ドラフトは反映できない (先に「非公開で登録」が必要)
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
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    events: [], gate: ['商品画像 (Driveリンク) が1枚もありません'],
    nextStatuses: ['ready_for_ai', 'on_hold', 'excluded'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.unknown, hasVariation: { value: false, source: 'manual' }, regroup: null,
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, ...pageInfoVars,
    genreDict: null,
    neCost: null, profitSim: null, simTaxPercent: 10, profitTakeRate: 0.9,
    yahoo: null, imageProduction: null,
  }],
];
for (const [name, file, data] of renders) {
  try {
    // router が常に渡す共通 locals (画像スロットグリッド・棚の反映状態)
    const html = await ejs.renderFile(path.join(views, file),
      { thumbnailUrl, fileViewUrl, shopCatSyncState: null, ...data });
    check(`render ${name}`, html.length > 500);
  } catch (e) {
    check(`render ${name}`, false, e.message);
  }
}

console.log(failed === 0 ? '\nSMOKE: ALL PASS' : `\nSMOKE: ${failed} FAILED`);
process.exit(failed === 0 ? 0 : 1);
