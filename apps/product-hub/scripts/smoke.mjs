/**
 * product-hub P1 smoke テスト (DB分離・Notion API 非接続)。
 * 実行: DATA_DIR に一時ディレクトリを指定して node apps/product-hub/scripts/smoke.mjs
 *   例 (bash): DATA_DIR=/tmp/ph-smoke node apps/product-hub/scripts/smoke.mjs
 * 検証: DB冪等init / CRUD / ゲート / append-onlyトリガー / drive-link / EJS実render / Notion fail-closed
 */
import path from 'path';
import fs from 'fs';
import { execFileSync } from 'child_process';
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

/**
 * 描画済み HTML から「ブラウザがその場で実行する JS」を script 要素ごとに取り出す。
 * 🚨 属性を許す (`<script defer>` `<script nonce>` にしただけで検査から外れないように)、
 *    src 付き (中身が無い) と JS でない type (application/json のデータ塊) は除く、
 *    **連結しない** (別々の script を繋ぐと、片方が構文エラーでも通ってしまう) — Codex R1
 * @returns {{code: string, isModule: boolean}[]}
 */
function inlineScriptsOf(html) {
  // 🚨 属性は**名前を取り出してから**見る。`/\bsrc=/` だと `data-src` の `-` の後ろでも
  //    語境界が成立して一致し、壊れた JS が検査から外れる (Codex R2)
  const attrsOf = (tag) => {
    const attrs = {};
    for (const a of String(tag).matchAll(/([^\s=/>]+)\s*(?:=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g)) {
      attrs[a[1].toLowerCase()] = a[2] ?? a[3] ?? a[4] ?? '';
    }
    return attrs;
  };
  const out = [];
  for (const m of String(html).matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
    const attrs = attrsOf(m[1] || '');
    if ('src' in attrs) continue; // 外部ファイル = ここに中身は無い
    const type = attrs.type || '';
    const isModule = /^module$/i.test(type);
    // 型なし = JS。JSON のデータ塊など JS でない型は対象外
    if (type && !isModule && !/^(text|application)\/javascript$/i.test(type)) continue;
    out.push({ code: m[2], isModule });
  }
  return out;
}

/**
 * 上で取り出した script を 1 つずつ構文検査する。名前は「構文」までしか保証しない
 * (実行時エラーはこれでは分からない)。
 * @returns {{ok: boolean, detail: string}}
 */
function checkInlineScriptSyntax(vm, html, label) {
  // 取り出し自体が壊れていないか (開きタグの数 = 取り出せた要素の数)。
  // これが合わないと「1 つも拾えなかったので緑」という素通りが起きる
  const opens = [...String(html).matchAll(/<script\b/gi)].length;
  const matched = [...String(html).matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].length;
  if (opens !== matched) return { ok: false, detail: `script 要素 ${opens} 個のうち ${matched} 個しか取り出せていない` };
  const blocks = inlineScriptsOf(html);
  if (blocks.length === 0) return { ok: true, detail: 'その場で実行する script は無い' };
  // ES module は vm.Script では検査できない。黙って外れないよう、現れたら落として気づく
  const mods = blocks.filter((b) => b.isModule).length;
  if (mods > 0) return { ok: false, detail: `type="module" の script が ${mods} 個 — この検査は未対応` };
  for (const [i, b] of blocks.entries()) {
    try { new vm.Script(b.code, { filename: `${label}#script${i + 1}` }); } catch (e) {
      return { ok: false, detail: `script[${i + 1}]: ${e.message}` };
    }
  }
  return { ok: true, detail: `${blocks.length} 個` };
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

// Notion カード作成の経路は撤去済み (2026-09-10)。API 非接続のまま env だけ外しておく
delete process.env.RYS_NOTION_TOKEN;
// この時点のドラフト行 (後段の描画 fixture が土台にする)
const after = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draft.id);

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

// 中身が変わらない再取り込みで「更新時刻」は進めない (Company DB は updated_at を観測時刻に使うので、
// 進めると JAN が変わっていないのに採用順が動き、観測が毎回まるごと増える。AI_reference CompanyDB構想 07 §9)
{
  const OLD = '2020-01-01T00:00:00.000Z';
  db.prepare("UPDATE product_drafts SET updated_at = ?, imported_at = ? WHERE ne_code = 'IMP-1'").run(OLD, OLD);
  const rSame = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: fakeFinder });
  const same = db.prepare("SELECT updated_at, imported_at FROM product_drafts WHERE ne_code = 'IMP-1'").get();
  check('re-import with no change keeps updated_at (imported_at だけ進む)',
    rSame.summary.updated === 1 && same.updated_at === OLD && same.imported_at !== OLD, JSON.stringify(same));

  db.prepare("UPDATE product_drafts SET updated_at = ? WHERE ne_code = 'IMP-1'").run(OLD);
  const janChanged = fakePage('IMP-1');
  janChanged.properties['JANコード'] = { type: 'number', number: 4909999999999 };
  const rJan = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: async () => janChanged });
  const moved = db.prepare("SELECT updated_at, jan_code FROM product_drafts WHERE ne_code = 'IMP-1'").get();
  check('JAN が変わったら updated_at は進む',
    rJan.summary.updated === 1 && moved.jan_code === '4909999999999' && moved.updated_at !== OLD, JSON.stringify(moved));
  await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: fakeFinder });   // 元の JAN に戻す

  // 子テーブルだけの変更 (税率・Yahoo 項目・AI の文言) も「中身が変わった」に数える。
  // 数えないと、派生セットの「親が更新されました」のお知らせが出なくなる (services/set-derive.js)
  db.prepare("UPDATE product_drafts SET updated_at = ? WHERE ne_code = 'IMP-1'").run(OLD);
  const taxChanged = fakePage('IMP-1');
  taxChanged.properties['税率'] = { type: 'select', select: { name: '8%' } };
  const rTax = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: async () => taxChanged });
  const afterTax = db.prepare("SELECT updated_at FROM product_drafts WHERE ne_code = 'IMP-1'").get();
  check('子テーブルだけの変更 (税率) でも updated_at は進む',
    rTax.summary.updated === 1
    && db.prepare("SELECT tax_rate FROM draft_yahoo WHERE draft_id = ?").get(imported.id).tax_rate === '8%'
    && afterTax.updated_at !== OLD, JSON.stringify(afterTax));

  db.prepare("UPDATE product_drafts SET updated_at = ? WHERE ne_code = 'IMP-1'").run(OLD);
  const aiChanged = fakePage('IMP-1');
  aiChanged.properties['税率'] = { type: 'select', select: { name: '8%' } };       // 税率は据え置き
  aiChanged.properties['キャッチコピー'] = rt('新しいキャッチ本文');
  const rAi = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: async () => aiChanged });
  check('AI の文言だけの変更でも updated_at は進む',
    rAi.summary.updated === 1
    && db.prepare("SELECT updated_at FROM product_drafts WHERE ne_code = 'IMP-1'").get().updated_at !== OLD);

  db.prepare("UPDATE product_drafts SET updated_at = ? WHERE ne_code = 'IMP-1'").run(OLD);
  const rNoop = await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: async () => aiChanged });
  check('親も子も変わらない再取り込みでは進まない (念のため、子を見るようにしても止まっている)',
    rNoop.summary.updated === 1
    && db.prepare("SELECT updated_at FROM product_drafts WHERE ne_code = 'IMP-1'").get().updated_at === OLD);
  await imp.importFromNotion(['IMP-1'], { actor: 'smoke', finder: fakeFinder });   // 元に戻す

  // 🚨 SQLite は「文字列の '4901234567890'」と「数値の 4901234567890」を別物として比べる。
  //    JAN は今は文字列で来るが、出どころが変わって数値になっても「毎回変わった」にならないよう型を揃える
  const numeric = imp.syncValues({ name: 'n', price: '1980', jan_code: 4901234567890, has_variation: '1',
    official_url: null, amazon_url: null, notion_page_id: 'p', notion_status: null });
  check('syncValues は列の型に合わせて揃える (JAN は文字列・価格と有無は数値・空は null のまま)',
    typeof numeric[2] === 'string' && numeric[2] === '4901234567890'
    && numeric[1] === 1980 && numeric[3] === 1
    && numeric[4] === null && numeric[7] === null, JSON.stringify(numeric));
}

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

// source の allow-list (取り込み由来・未知の値は portal 扱いにしない — deny-list だと通ってしまう)
check('canWriteToNotion allow-list',
  dbmod.canWriteToNotion({ source: 'portal' }) === true
  && dbmod.canWriteToNotion({ source: 'notion_import' }) === false
  && dbmod.canWriteToNotion({ source: 'unknown' }) === false
  && dbmod.canWriteToNotion(null) === false);
db.prepare(`UPDATE product_drafts SET official_url = 'https://example.com/item', amazon_url = NULL, notion_page_id = NULL WHERE id = ?`).run(draft.id);

// 削除は取り込み由来だけ (children は CASCADE、draft_events は append-only なので孤児で残る)
const impId = imported.id;
db.prepare('DELETE FROM product_drafts WHERE id = ?').run(impId);
check('delete cascades children',
  db.prepare('SELECT COUNT(*) c FROM draft_yahoo WHERE draft_id = ?').get(impId).c === 0
  && db.prepare('SELECT COUNT(*) c FROM draft_ai_outputs WHERE draft_id = ?').get(impId).c === 0);

// ─── バリエーション判定 (NE 代表商品コード) ───
const vari = await import('../lib/variation.js');
const existingPageMod = await import('../lib/existing-page.js');
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

// ─── 既存の楽天ページへの追加か (2026-09-25 スタッフ要望「既存ページラベル」) ───
// 自動判定 = NE の同じ代表商品コードのグループに、アプリ導入前からある商品 (初回シード = draft_id NULL) があるか
{
  const epIns = (code, rep) => insProd.run(9100 + epIns.n++, code, code, rep);
  epIns.n = 0;
  const seen = db.prepare('INSERT OR REPLACE INTO ph_ne_seen_codes (code_key, ne_code, draft_id) VALUES (?, ?, ?)');
  const mkDraft = (code, source = 'portal') => Number(db.prepare(
    'INSERT INTO product_drafts (ne_code, name, source) VALUES (?, ?, ?)').run(code, code, source).lastInsertRowid);
  // A: 既存ページ epx (青は前からある) に赤を足した = 典型のカラバリ追加。カードの商品コードは大文字でも同じ
  epIns('epx-blue', 'epx'); epIns('epx-red', 'epx');
  const dA = mkDraft('EPX');
  seen.run('epx-blue', 'epx-blue', null); seen.run('epx-red', 'epx-red', dA);
  // B: 新商品 epn (2 色とも今回はじめて入った)
  epIns('epn-blue', 'epn'); epIns('epn-red', 'epn');
  const dB = mkDraft('epn');
  seen.run('epn-blue', 'epn-blue', dB); seen.run('epn-red', 'epn-red', dB);
  // C: 前からある単品 (グループなし) = ページがまだ無いこともあるので自動では付けない
  epIns('eps', null);
  const dC = mkDraft('eps');
  seen.run('eps', 'eps', null);
  // D: 単品ページ epy に色を足し、元の商品の代表商品コードが空のまま = カード自身がシード済み + グループあり
  epIns('epy', null); epIns('epy-2', 'epy');
  const dD = mkDraft('epy');
  seen.run('epy', 'epy', null); seen.run('epy-2', 'epy-2', dD);
  // E: Notion から取り込んだ商品 (導入前から進めていた新商品 = シード済みになる) は誤検知なので付けない
  epIns('epz-1', 'epz');
  const dE = mkDraft('epz', 'notion_import');
  seen.run('epz-1', 'epz-1', null);
  // F: このアプリから楽天に出品した商品 = ページを作ったのはアプリ (新規ページ)
  epIns('epw-1', 'epw');
  const dF = mkDraft('epw');
  seen.run('epw-1', 'epw-1', null);
  db.prepare("INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-09-01T00:00:00Z')").run(dF);

  const epm = existingPageMod;
  const of = (id) => epm.existingPageOfDraft(db, id);
  check('既存ページ: 前からある色のグループに足した商品 = 自動で既存ページ',
    of(dA).existingPage === true && of(dA).auto === true && of(dA).choice === '', JSON.stringify(of(dA)));
  check('既存ページ: 全色はじめての新商品は付かない', of(dB).existingPage === false, JSON.stringify(of(dB)));
  check('既存ページ: 前からある単品 (グループなし) は自動では付けない', of(dC).existingPage === false, JSON.stringify(of(dC)));
  check('既存ページ: 元の商品の代表コードが空でも、カード自身が前からあってグループがあれば付く',
    of(dD).existingPage === true, JSON.stringify(of(dD)));
  check('既存ページ: Notion 取り込み由来は自動では付けない', of(dE).existingPage === false, JSON.stringify(of(dE)));
  check('既存ページ: アプリから楽天に出品した商品は自動では付けない', of(dF).existingPage === false, JSON.stringify(of(dF)));
  // 人が決めた値が自動判定より優先 (自動の誤りを直せる / 自動で分からないものを付けられる)
  db.prepare('UPDATE product_drafts SET existing_page = 0 WHERE id = ?').run(dA);
  db.prepare('UPDATE product_drafts SET existing_page = 1 WHERE id = ?').run(dB);
  check('既存ページ: 人が「新規ページ」と決めたら自動判定より優先',
    of(dA).existingPage === false && of(dA).auto === false && of(dA).choice === '0', JSON.stringify(of(dA)));
  check('既存ページ: 人が「既存ページに追加」と決めたら付く',
    of(dB).existingPage === true && of(dB).auto === false && of(dB).choice === '1', JSON.stringify(of(dB)));
  // まとめて引く (ボード) も 1 件ずつ (詳細) と同じ答え
  const all = epm.existingPageOf(db, db.prepare(`
    SELECT d.id, d.ne_code, d.existing_page, d.source,
      (SELECT registered_at FROM draft_rakuten r WHERE r.draft_id = d.id) AS rakuten_registered_at
    FROM product_drafts d WHERE d.id IN (?, ?, ?, ?, ?, ?)`).all(dA, dB, dC, dD, dE, dF));
  check('既存ページ: まとめて引いても 1 件ずつと同じ',
    [dA, dB, dC, dD, dE, dF].every((id) => all.get(id).existingPage === of(id).existingPage),
    JSON.stringify([...all]));
  let epCheckErr = null;
  try { db.prepare('UPDATE product_drafts SET existing_page = 2 WHERE id = ?').run(dC); } catch (e) { epCheckErr = e; }
  check('既存ページ: 列は NULL / 0 / 1 だけ (CHECK)', /CHECK/i.test(String(epCheckErr?.message)), epCheckErr?.message || '通ってしまった');

  // 後片付け (後段の自動取込の試験が mirror の未知コードを新商品として拾わないように)
  const ids = [dA, dB, dC, dD, dE, dF];
  db.prepare(`DELETE FROM draft_rakuten WHERE draft_id = ?`).run(dF);
  db.prepare(`DELETE FROM product_drafts WHERE id IN (${ids.map(() => '?').join(',')})`).run(...ids);
  db.prepare("DELETE FROM mirror_products WHERE product_id BETWEEN 9100 AND 9199").run();
  const epCodes = ['epx-blue', 'epx-red', 'epn-blue', 'epn-red', 'eps', 'epy', 'epy-2', 'epz-1', 'epw-1'];
  db.prepare(`DELETE FROM ph_ne_seen_codes WHERE code_key IN (${epCodes.map(() => '?').join(',')})`).run(...epCodes);
}

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

db.prepare(`DELETE FROM product_drafts WHERE ne_code IN ('rooms-l-bk','rooms')`).run();
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
check('一括登録: 上限を超えたら弾く',
  intake.registerByCodes(Array.from({ length: intake.MAX_REGISTER_CODES + 1 }, (_, i) => `X-${i}`), {}).error === 'too_many_codes');
check('一括登録: 空入力は弾く', intake.registerByCodes([], {}).error === 'no_codes');

// ── 出品済みのページに後から色が足された (2026-09-25) ──
// 以前は既存のドラフトへ黙ってまとめるだけで、カードが出なかった = 楽天ページに色を足す作業が誰にも見えない
{
  const sgsId = db.prepare(`SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'sgs'`).get().id;
  const cardOf = (code) => db.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code);
  const seenOf = (code) => db.prepare('SELECT draft_id FROM ph_ne_seen_codes WHERE code_key = ?').get(code)?.draft_id;
  // まだページを作っている途中なら、新しい色はそのページにそのまま入る (従来どおりまとめる)
  insP.run(9207, 'sgs-wh', 'メガネストラップ ホワイト', 'sgs', 0.1);
  const r0 = intake.syncNewProducts({ dryRun: true });
  check('色追加: 出品前のページならまとめる (カードを出さない)',
    r0.created === 0 && r0.merged === 1 && intake.pagePublished(db, sgsId) === false, JSON.stringify(r0));
  // アプリから楽天に出品した = ページが出ている
  db.prepare(`INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-09-01T00:00:00Z')
    ON CONFLICT(draft_id) DO UPDATE SET registered_at = excluded.registered_at`).run(sgsId);
  check('色追加: 楽天に出品済みならページが出ている扱い', intake.pagePublished(db, sgsId) === true);
  insP.run(9208, 'sgs-rd', 'メガネストラップ レッド', 'sgs', 0.1);
  const dryAdd = intake.syncNewProducts({ dryRun: true });
  check('色追加 dry-run: 同じページへの 2 色は 1 枚のカードとして数える',
    dryAdd.created === 1 && dryAdd.merged === 1 && dryAdd.drafts[0]?.addedTo === sgsId, JSON.stringify(dryAdd));
  const runAdd = intake.syncNewProducts({});
  const card = cardOf('sgs-rd') || cardOf('sgs-wh');
  check('色追加: 出品済みページのグループに新しい色が来たらカードを 1 枚出す',
    runAdd.created === 1 && runAdd.merged === 1 && !!card && runAdd.drafts[0]?.addedTo === sgsId, JSON.stringify(runAdd));
  check('色追加: カードは 📄既存ページ + 追加先のページを持つ (セットではない)',
    card.existing_page === 1 && card.added_to_draft_id === sgsId && card.parent_draft_id == null, JSON.stringify(card));
  check('色追加: 2 色目は同じカードにまとまる (色ごとにカードが乱立しない)',
    seenOf('sgs-wh') === card.id && seenOf('sgs-rd') === card.id);
  check('色追加: カードの札 = 既存ページ・追加先が読める',
    existingPageMod.existingPageOfDraft(db, card.id).existingPage === true
    && existingPageMod.existingPageOfDraft(db, card.id).addedTo?.ne_code === 'sgs');
  check('色追加: 追加先のページにも記録が残る',
    db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'variation_added'`).get(sgsId).c === 1);
  // 楽天出品は止める (商品コードが新しい色の SKU なので、出すと別の新しいページができる)
  const listingEarly = await import('../services/rakuten-listing.js');
  const built = listingEarly.buildItemPayload(db, card.id);
  check('色追加: カードからは楽天に出品しない (理由に直すページが出る)',
    built.ok === false && built.reasons.some((x) => /既存の楽天ページ「sgs」に追加する商品です/.test(x)), JSON.stringify(built.reasons));
  // 画像フォルダは既存ページのものを使うので自動では作らない
  const dif0 = await import('../services/drive-image-folder.js');
  const fold = await dif0.attemptImageFolderCreationBatch([card.id], { actor: 'smoke' });
  check('色追加: 画像フォルダを自動で作らない', fold.skipped === 1 && fold.created === 0 && fold.failed === 0, JSON.stringify(fold));
  // 前からある色を一括登録し直しても、色追加のカードは出さない (見たことのあるコード = まとめる)
  const regOld = intake.registerByCodes(['sgs-or'], { actor: 'smoke' });
  check('色追加: 前からある色の登録し直しではカードを出さない',
    regOld.summary.created === 0 && regOld.summary.merged === 1, JSON.stringify(regOld.summary));
  // 色追加のカードが済んだあと、さらに色が足されたら新しいカードを出す
  db.prepare(`UPDATE product_drafts SET status = 'expanded' WHERE id = ?`).run(card.id);
  insP.run(9209, 'sgs-pk', 'メガネストラップ ピンク', 'sgs', 0.1);
  const runAdd2 = intake.syncNewProducts({});
  check('色追加: 前の色追加カードが済んでいたら新しいカードを出す',
    runAdd2.created === 1 && cardOf('sgs-pk')?.added_to_draft_id === sgsId, JSON.stringify(runAdd2));
  // モール別の状況で楽天を手で「完了」にした商品 (アプリ以前に手で出した) もページが出ている扱い
  db.prepare(`DELETE FROM draft_rakuten WHERE draft_id = ?`).run(sgsId);
  db.prepare(`INSERT INTO draft_mall_status (draft_id, mall, state) VALUES (?, 'rakuten', 'done')
    ON CONFLICT(draft_id, mall) DO UPDATE SET state = 'done'`).run(sgsId);
  check('色追加: 楽天を手で完了にした商品もページが出ている扱い', intake.pagePublished(db, sgsId) === true);
  db.prepare(`DELETE FROM draft_mall_status WHERE draft_id = ?`).run(sgsId);
  db.prepare(`DELETE FROM product_drafts WHERE added_to_draft_id = ?`).run(sgsId);
}

// 後片付け (後続の render fixture に影響させない)
db.prepare(`DELETE FROM product_drafts WHERE LOWER(TRIM(ne_code)) IN ('sgs','flaxseed','notaxprod','newitem1')`).run();
db.prepare(`DELETE FROM mirror_products WHERE product_id BETWEEN 9201 AND 9209`).run();
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
// 配送方法は出品の必須条件 (2026-09-05 中原さん判断: 選んでいないものは出せない)。
// ここは「payload が組み立つか」を見るテストなので条件を満たしておく。
// **バナーの無い配送方法 (4 = ゆうパック)** を選ぶ — 下の画像テストは共通3枚だけの並びを見ているため
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '4' WHERE draft_id = ?`).run(rkId);

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
// 2026-08-31 中原さん: 楽天タイトルは検索用に語を並べたもので、説明として読ませる文ではない。
// 表の先頭に丸ごと出ると SEO 語の羅列がそのまま載るので、説明行には入れない
check('payload: PC説明文は表形式 — 説明行は AI 特徴から始まる (楽天タイトルを入れない)',
  pl.productDescription.pc.startsWith('<table')
  && pl.productDescription.pc.includes('<b>説明</b>')
  && !pl.productDescription.pc.includes('楽天用タイトル')
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
check('draft_page_info に brand_name (冪等ALTER)', piCols.has('brand_name'));
// SKU別JAN の一意制約 (テーブル定義 + 後付けインデックスの両方で担保)
{
  const ix = db.prepare('PRAGMA index_list(draft_sku_jans)').all().some((i) => {
    if (!i.unique) return false;
    const cols = db.prepare(`PRAGMA index_info(${JSON.stringify(i.name)})`).all().map((c) => c.name);
    return cols.length === 2 && cols.includes('draft_id') && cols.includes('jan_code');
  });
  check('draft_sku_jans に UNIQUE(draft_id, jan_code)', ix);
}

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

check('payload: 画像は CABINET location + 共通バナー3枚が末尾 (配送バナーの無い配送方法)',
  pl.images.length === 4 && pl.images[0].type === 'CABINET' && pl.images[0].location === '/app-newitems/rk-smoke-1-1.jpg'
  && pl.images[1].location === '/coupon/imgrc0122590661.jpg'
  && pl.images[3].location === '/11720388/refund.jpg',
  JSON.stringify(pl.images));
check('payload: variants は ne_code キー + 属性 + 型番なし例外',
  pl.variants['rk-smoke-1'].standardPrice === 1980
  && pl.variants['rk-smoke-1'].articleNumber.exemptionReason === 5
  && pl.variants['rk-smoke-1'].attributes[0].name === 'ブランド名',
  JSON.stringify(pl.variants));

// 🚨 articleNumber = RMS 画面の「カタログID」= JAN。メーカー型番を入れると IE0228 で登録が落ちる
// (2026-09-02 shaganshi で実証)。型番を入れても articleNumber は免除理由のまま、が正しい
db.prepare(`UPDATE draft_rakuten SET article_number = 'ABC-100' WHERE draft_id = ?`).run(rkId);
check('payload: メーカー型番は articleNumber に入れない (IE0228 の再発防止)',
  listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber.exemptionReason === 5
  && listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber.value === undefined,
  JSON.stringify(listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber));

// カタログIDなしの理由は選んだ値で送る (未選択なら 5)
db.prepare(`UPDATE draft_rakuten SET catalog_id_exemption_reason = 3 WHERE draft_id = ?`).run(rkId);
check('payload: カタログIDなしの理由は選んだ値で送る',
  listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber.exemptionReason === 3);
// 1 (セット商品) は articleNumberForSet が要るのでまだ送れない → 止める
db.prepare(`UPDATE draft_rakuten SET catalog_id_exemption_reason = 1 WHERE draft_id = ?`).run(rkId);
check('payload: 理由1 (セット商品) は未対応として止める',
  listing.buildItemPayload(db, rkId).ok === false);
db.prepare(`UPDATE draft_rakuten SET catalog_id_exemption_reason = NULL WHERE draft_id = ?`).run(rkId);

// JAN があればそれをカタログIDとして送る (免除理由より優先)
db.prepare(`UPDATE product_drafts SET jan_code = '4901234567894' WHERE id = ?`).run(rkId);
check('payload: JANがあればカタログIDとして value で送る',
  listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber.value === '4901234567894',
  JSON.stringify(listing.buildItemPayload(db, rkId).payload.variants['rk-smoke-1'].articleNumber));
db.prepare(`UPDATE product_drafts SET jan_code = NULL WHERE id = ?`).run(rkId);

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
// 🚨 RMS へ送る配送方法は **セットだけ** NE へフォールバックする (§4.4 決⑥)。
// 単品まで NE に落とすと、いま店舗デフォルトで出ている商品の配送方法が黙って変わるため。
// では単品はどうするか → **選んでいないなら出品を止める** (中原さん判断 2026-09-05)。
// 送らないまま出すと、帯 (バナー) は NE の「定形外」なのに楽天の設定は店舗デフォルト、
// という食い違いが誰にも気づかれないまま世に出る
check('🚨 単品は配送方法を選んでいないと出品できない (帯と楽天の設定が食い違うのを防ぐ)',
  bNe.ok === false
  && (bNe.reasons || []).some((x) => /配送方法を選んでください/.test(x)),
  JSON.stringify(bNe.reasons || []));
// 止めるだけで、画面のプレビューは従来どおり NE の配送方法で帯を描く
// (「NE ではこうなる」が見えないと、何を選べばよいか分からない)
check('アプリ指定なしでも、帯は NE の配送方法 (定形外) から選ばれる',
  listing.trailingBannerLocations(listing.effectiveShippingForDraft(db, 'rk-smoke-1', null).group)[0]
    === '/07722747/08581403/teikeigai_soryomuryo.jpg',
  JSON.stringify(listing.trailingBannerLocations(listing.effectiveShippingForDraft(db, 'rk-smoke-1', null).group)));
// 配送方法を戻す。ここから下のテストは「配送方法は選んである」前提で payload の中身を見る
// (2026-09-05 から未選択は出品ゲートで止まるので、戻さないと以降が全部止まる)
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '5' WHERE draft_id = ?`).run(rkId);
check('🚨 payload の配送方法: セットだけ NE へフォールバックする (単品は従来どおり)',
  listing.payloadShippingGroup({ parent_draft_id: 7 }, { group: '1' }, { group: null }) === '1'
  && listing.payloadShippingGroup({ parent_draft_id: null }, { group: '1' }, { group: null }) === ''
  // アプリで明示指定されていれば、セットでも単品でもその値 (effective も同じ値を返す)
  && listing.payloadShippingGroup({ parent_draft_id: 7 }, { group: '9' }, { group: '9' }) === '9'
  && listing.payloadShippingGroup({ parent_draft_id: null }, { group: '9' }, { group: '9' }) === '9',
  JSON.stringify([
    listing.payloadShippingGroup({ parent_draft_id: 7 }, { group: '1' }, { group: null }),
    listing.payloadShippingGroup({ parent_draft_id: null }, { group: '1' }, { group: null }),
  ]));
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
  && listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y8').yahooDelivery === '宅急便50サイズ以下'
  && listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y5').yahooDelivery === 'ネコポス');
check('複合選択肢の末尾バナーは定形外と同一',
  JSON.stringify(listing.trailingBannerLocations(listing.effectiveShippingForDraft(db, 'rk-smoke-1', '1y8').group))
  === JSON.stringify(listing.trailingBannerLocations('1')));

// ─── 配送方法の値の意味 (2026-09-03 #1152) ───────────────────────
// 楽天グループID と 画面の複合選択肢キー を1つの列に混ぜていたため、読む側の判断漏れで
// 「複合選択肢を選ぶと楽天に出品できない」不具合が出た (#725)。値の解釈は
// lib/shipping-groups.js の 1 関数だけが持ち、保存 (router) と出品 (buildItemPayload) の
// 両方がそこを通る。**画面に出る選択肢は全部、出品まで通らなければならない**
const shipGroups = await import('../lib/shipping-groups.js');
{
  // 🚨 配送方法の名前は price-update/shipping-labels.js が正本 (楽天の配送方法セット一覧)。
  // product-hub で別に書いていたため 2026-09-04 まで実物と食い違っていた
  // (4 = 「宅急便」→実物は**ゆうパック** / 8 = 「宅急便50サイズ以上」→実物は**以下**)。
  // 送る値は ID なので楽天側の設定は正しかったが、人はラベルを見て選ぶので実害がある
  const labels = await import('../../price-update/shipping-labels.js');
  const src = labels.RAKUTEN_SHIPPING_METHODS;
  check('配送方法の名前: 正本 (price-update の一覧) と食い違わない',
    Object.entries(shipGroups.SHIPPING_METHOD_GROUPS)
      .every(([id, name]) => src[id] === name),
    JSON.stringify(shipGroups.SHIPPING_METHOD_GROUPS));
  check('配送方法の名前: 実物どおり (4=ゆうパック / 8=宅急便50サイズ以下)',
    shipGroups.SHIPPING_METHOD_GROUPS['4'] === 'ゆうパック'
    && shipGroups.SHIPPING_METHOD_GROUPS['8'] === '宅急便50サイズ以下'
    && shipGroups.SHIPPING_METHOD_GROUPS['1'] === '定形外');
  check('配送方法の名前: 「現在使用不可」は選ばせない (楽天側で使えない)',
    !shipGroups.SHIPPING_METHOD_GROUPS['2']
    && src[2].includes('現在使用不可')
    && Object.values(shipGroups.SHIPPING_METHOD_GROUPS).every((n) => !n.includes('現在使用不可')));
  // 🚨 選択肢から外すだけ。既に保存されている値を「不正」にはしない (Codex R2):
  //    出品の検証で弾くと、その商品はもう出品できなくなる
  check('配送方法の名前: 選択肢に無い値でも、保存済みなら検証は通す',
    shipGroups.toRakutenShippingGroup('2').ok === true
    && shipGroups.toRakutenShippingGroup('2').group === '2'
    && !!shipGroups.ALL_SHIPPING_METHOD_GROUPS['2']);
  check('配送方法の名前: 楽天が持たない番号はこれまでどおり不正',
    shipGroups.toRakutenShippingGroup('99').ok === false
    && shipGroups.toRakutenShippingGroup('0').ok === false);
  check('配送方法の名前: 使える配送方法は正本から漏らさない',
    Object.keys(src).filter((id) => !src[id].includes('現在使用不可'))
      .every((id) => !!shipGroups.SHIPPING_METHOD_GROUPS[id]));
  // 複合選択肢がヤフー欄に初期セットする値も、選択肢 (楽天の表を流用) に実在すること
  check('配送方法の名前: 複合選択肢のヤフー初期値が選択肢に実在する',
    Object.values(shipGroups.YAHOO_OVERRIDE_SHIPPING_GROUPS)
      .every((ov) => Object.values(shipGroups.SHIPPING_METHOD_GROUPS).includes(ov.yahooDelivery)),
    JSON.stringify(Object.values(shipGroups.YAHOO_OVERRIDE_SHIPPING_GROUPS).map((o) => o.yahooDelivery)));
}
check('toRakutenShippingGroup: 楽天IDはそのまま / 複合は楽天IDへ分解 + ヤフー配送を返す',
  shipGroups.toRakutenShippingGroup('5').group === '5'
  && shipGroups.toRakutenShippingGroup('5').yahooOverride === null
  && shipGroups.toRakutenShippingGroup('1y5').group === '1'
  && shipGroups.toRakutenShippingGroup('1y5').yahooOverride?.yahooDelivery === 'ネコポス'
  && shipGroups.toRakutenShippingGroup('1y8').group === '1');
check('toRakutenShippingGroup: 未指定は空 (店舗デフォルト) / 選択肢に無い値だけ ok=false',
  shipGroups.toRakutenShippingGroup('').ok === true && shipGroups.toRakutenShippingGroup('').group === ''
  && shipGroups.toRakutenShippingGroup(null).ok === true
  && shipGroups.toRakutenShippingGroup('99').ok === false
  && shipGroups.toRakutenShippingGroup('1y9').ok === false);
check('shippingSelectValueOf: ヤフー別扱いのときだけ複合キーへ逆引きする',
  shipGroups.shippingSelectValueOf('1', { shipping_override: 1, delivery_label: 'ネコポス' }) === '1y5'
  && shipGroups.shippingSelectValueOf('1', { shipping_override: 1, delivery_label: '宅急便50サイズ以下' }) === '1y8'
  // 別扱いでない / ヤフー配送を複合の既定値から変えた → 楽天IDのまま (プルダウンは定形外を指す)
  && shipGroups.shippingSelectValueOf('1', { shipping_override: 0, delivery_label: 'ネコポス' }) === '1'
  && shipGroups.shippingSelectValueOf('1', { shipping_override: 1, delivery_label: '宅急便' }) === '1'
  && shipGroups.shippingSelectValueOf('5', null) === '5');
{
  // 総当たり: プルダウンに出る全選択肢で「出品が止まらない」「RMS へ出るのは楽天IDだけ」
  const setGroup = db.prepare('UPDATE draft_rakuten SET shipping_method_group = ? WHERE draft_id = ?');
  const blocked = [];
  const leaked = [];
  for (const choice of [...Object.keys(listing.SHIPPING_METHOD_GROUPS), ...Object.keys(listing.YAHOO_OVERRIDE_SHIPPING_GROUPS)]) {
    setGroup.run(choice, rkId);
    const b = listing.buildItemPayload(db, rkId);
    if ((b.reasons || []).some((r) => r.includes('配送方法'))) blocked.push(choice);
    const sent = b.payload?.variants?.['rk-smoke-1']?.shipping?.shippingMethodGroup;
    if (sent !== undefined && !listing.SHIPPING_METHOD_GROUPS[sent]) leaked.push(`${choice}→${sent}`);
  }
  check('payload: 画面の配送方法の選択肢は全部そのまま出品できる (複合選択肢も含む)',
    blocked.length === 0, `止まった選択肢: ${blocked.join(',')}`);
  check('payload: RMS へ送る shippingMethodGroup は必ず楽天グループID (複合キーは漏らさない)',
    leaked.length === 0, leaked.join(','));
  // 複合を選んだときは楽天=定形外として出る (ページ表記・バナーと同じ扱い)
  setGroup.run('1y5', rkId);
  const bOv = listing.buildItemPayload(db, rkId);
  check('payload: 複合(1y5)は楽天グループ1 (定形外) で送る',
    bOv.ok === true && bOv.payload.variants['rk-smoke-1'].shipping.shippingMethodGroup === '1',
    JSON.stringify(bOv.payload?.variants?.['rk-smoke-1']?.shipping || bOv.reasons));
  setGroup.run(null, rkId);
}
{
  // マイグレーション: 旧データ (複合キーが DB に残っている) を分解する
  db.prepare('UPDATE draft_rakuten SET shipping_method_group = ? WHERE draft_id = ?').run('1y5', rkId);
  db.prepare('UPDATE draft_yahoo SET delivery_label = NULL, shipping_override = 0 WHERE draft_id = ?').run(rkId);
  const moved = dbmod.migrateCompositeShippingGroups(db);
  const rkAfter = db.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(rkId);
  const yhAfter = db.prepare('SELECT delivery_label, shipping_override FROM draft_yahoo WHERE draft_id = ?').get(rkId);
  check('migration: 複合キーは 楽天ID + ヤフー別扱いフラグ に分解される',
    moved >= 1 && rkAfter.shipping_method_group === '1'
    && yhAfter.shipping_override === 1 && yhAfter.delivery_label === 'ネコポス',
    JSON.stringify({ moved, rkAfter, yhAfter }));
  check('migration: 冪等 (複合キーが無ければ何もしない)', dbmod.migrateCompositeShippingGroups(db) === 0);
  // 人が変えたヤフー配送は上書きしない
  db.prepare('UPDATE draft_rakuten SET shipping_method_group = ? WHERE draft_id = ?').run('1y8', rkId);
  db.prepare('UPDATE draft_yahoo SET delivery_label = ? WHERE draft_id = ?').run('宅急便', rkId);
  dbmod.migrateCompositeShippingGroups(db);
  check('migration: 既に入っているヤフーの配送方法は上書きしない',
    db.prepare('SELECT delivery_label FROM draft_yahoo WHERE draft_id = ?').get(rkId).delivery_label === '宅急便');
  db.prepare('UPDATE draft_rakuten SET shipping_method_group = NULL WHERE draft_id = ?').run(rkId);
  db.prepare('UPDATE draft_yahoo SET delivery_label = NULL, shipping_override = 0 WHERE draft_id = ?').run(rkId);
}
// SKU画像 (2026-08-07): ファイル名→SKUコード照合キー
check('skuImageKeyOfFileName: 拡張子除去+小文字化+trim',
  listing.skuImageKeyOfFileName('Sueders-DB.JPG') === 'sueders-db'
  && listing.skuImageKeyOfFileName(' sueders-db.png ') === 'sueders-db'
  && listing.skuImageKeyOfFileName('sueders-db') === 'sueders-db'
  && listing.skuImageKeyOfFileName('sueders-db.backup.jpg') === 'sueders-db.backup');
// ─── R-Cabinet の filePath は 20文字以内 (2026-09-04 実測 / #1163) ─────────
// 🚨 21文字は楽天が resultCode 3001 で拒否する。商品コードが 15 文字以上の商品は
// 白抜き (`<コード>-white` = 21文字) が必ず転送できず、理由の消えた 502 だけが出ていた。
// **miniPC の入力チェックと同じ形** (先頭英数 + 20文字以内 + .jpg) を守ること
const CAB_OK = /^[a-z0-9][a-z0-9-]{0,19}\.jpg$/;
// この形の検証が「末尾の改行や前後の空白を通さない」こと。JS の $ は (m フラグが無ければ)
// 文字列の終端だけに一致するので通らない — Perl/Python の $ とは違う。将来 m フラグが
// 付いたり検証の形が変わったら落ちるように、実際の値で確かめておく (Codex R4)
check('cabinetFilePath: 改行や前後空白を含む値は検証を通らない',
  !CAB_OK.test('ok-1.jpg\n') && !CAB_OK.test('ok-1.jpg\r\n') && !CAB_OK.test(' ok-1.jpg')
  && !CAB_OK.test('foo\nbar.jpg') && CAB_OK.test('ok-1.jpg'));
check('cabinetFilePath: 20文字に収まる商品コードは従来どおりの名前 (転送済みを作り直さない)',
  listing.cabinetFilePath('plastic', 'white') === 'plastic-white.jpg'
  && listing.cabinetFilePath('plastic', '3') === 'plastic-3.jpg'
  && listing.cabinetFilePath('siratamaishi', 'white') === 'siratamaishi-white.jpg'   // 18文字 (実績)
  && listing.cabinetFilePath('PLASTIC', '1') === 'plastic-1.jpg');                   // 小文字化
check('cabinetFilePath: 21文字になる商品コードは短縮する (silicateclay800-white が落ちていた実例)',
  listing.cabinetFilePath('silicateclay800', 'white') !== 'silicateclay800-white.jpg'
  && CAB_OK.test(listing.cabinetFilePath('silicateclay800', 'white'))
  // 商品画像側 (17文字) は 20文字に収まるので従来のまま = 転送済みの 8 枚を作り直さない
  && listing.cabinetFilePath('silicateclay800', '8') === 'silicateclay800-8.jpg');
{
  // 総当たり: 実在しうる商品コードの長さ × 枠 (white / 1〜20) が全部 20文字以内に収まること。
  // 1つでも溢れると、その商品はその枠だけ転送できず出品が止まる
  const suffixes = ['white', ...Array.from({ length: 20 }, (_, i) => String(i + 1))];
  const codes = ['a', 'plastic', 'silicateclay800', 'x'.repeat(30), 'とても長い日本語の商品コード', 'a_b.c', '---leading', '9start'];
  const bad = [];
  for (const code of codes) {
    for (const s of suffixes) {
      const p = listing.cabinetFilePath(code, s);
      if (!CAB_OK.test(p)) bad.push(`${code}/${s}→${p}`);
    }
  }
  check('cabinetFilePath: どの商品コード × どの枠でも 20文字以内・先頭英数に収まる', bad.length === 0, bad.slice(0, 5).join(' '));
  // 切り詰めても別商品と衝突しない (overWrite=true なので衝突すると他商品の画像を静かに壊す)
  const long1 = 'a'.repeat(25);
  check('cabinetFilePath: 先頭が同じ長い商品コード同士でも衝突しない',
    listing.cabinetFilePath(long1, 'white') !== listing.cabinetFilePath(`${long1}b`, 'white')
    && listing.cabinetFilePath(long1, '1') !== listing.cabinetFilePath(long1, '2'));
}
{
  // 🚨 上限は product-hub (名前を作る側) と miniPC (受け取って検証する側) の2箇所にある。
  // ここがズレていたのが #1163 の遠因 (miniPC=31文字 / 楽天の実際=20文字) なので、
  // ズレたら気づけるようにソースから読んで突き合わせる
  const svc = fs.readFileSync(path.join(__dirname, '..', '..', 'warehouse', 'rakuten-rms-service.js'), 'utf8');
  const m = svc.match(/\{0,(\d+)\}\\\.jpg\$/);
  check('cabinetFilePath: miniPC 側の filePath 検証と上限が一致している (20文字)',
    !!m && Number(m[1]) + 1 === listing.CABINET_PATH_MAX && listing.CABINET_PATH_MAX === 20,
    `miniPC=${m ? Number(m[1]) + 1 : '(読めず)'} / product-hub=${listing.CABINET_PATH_MAX}`);
}
// Cabinet ファイルパス: 情報が落ちる置換はハッシュ付与で一意化 (Codex High-3)
// ─── RMS の商品編集ページURL (2026-09-04 中原さんから実URLを受領) ──────────
//   https://item.rms.rakuten.co.jp/rms-sku/shops/373343/item/edit/silicateclay800
//   セッションやトークンは含まれず、店舗ID + 商品管理番号だけで開ける
{
  // 既定値を確かめるので env は必ず外してから測る (本番相当の env が設定された環境でも
  // このテストが落ちないように。最後に必ず戻す — Codex R1 medium)
  const beforeShopId = process.env.PH_RAKUTEN_SHOP_ID;
  try {
    delete process.env.PH_RAKUTEN_SHOP_ID;
    check('RMSリンク: 受け取った実URLと同じ形を組み立てる',
      listing.rakutenRmsItemUrl('silicateclay800')
        === 'https://item.rms.rakuten.co.jp/rms-sku/shops/373343/item/edit/silicateclay800');
    check('RMSリンク: 商品管理番号は小文字にする (RMS 仕様。registerItem と揃える)',
      listing.rakutenRmsItemUrl('SilicateClay800').endsWith('/item/edit/silicateclay800'));
    check('RMSリンク: 商品コードが無ければリンクを作らない',
      listing.rakutenRmsItemUrl('') === null && listing.rakutenRmsItemUrl(null) === null
      && listing.rakutenRmsItemUrl('   ') === null);
    process.env.PH_RAKUTEN_SHOP_ID = '999999';
    check('RMSリンク: 店舗IDは env で上書きできる',
      listing.rakutenRmsItemUrl('x').includes('/shops/999999/'));
    for (const bad of ['not-a-number', '', '   ', '12a']) {
      process.env.PH_RAKUTEN_SHOP_ID = bad;
      check(`RMSリンク: 店舗IDの設定ミス (${JSON.stringify(bad)}) では壊れたURLを出さない`,
        listing.rakutenRmsItemUrl('x') === null);
    }
  } finally {
    if (beforeShopId === undefined) delete process.env.PH_RAKUTEN_SHOP_ID;
    else process.env.PH_RAKUTEN_SHOP_ID = beforeShopId;
  }
}
check('skuCabinetFilePath: 素直なコードはそのまま / 特殊文字はハッシュ付与で衝突しない',
  listing.skuCabinetFilePath('sueders-db') === 'sueders-db-sku.jpg'
  && listing.skuCabinetFilePath('a_b') !== listing.skuCabinetFilePath('a.b')
  && CAB_OK.test(listing.skuCabinetFilePath('a_b'))
  && CAB_OK.test(listing.skuCabinetFilePath('とても長い日本語のSKUコード仮に置いたもの'))
  && CAB_OK.test(listing.skuCabinetFilePath('x'.repeat(96))));
check('skuCabinetFilePath: 切り詰めが必要な長い同接頭辞SKUも衝突しない (R2 High)',
  listing.skuCabinetFilePath('a'.repeat(40)) !== listing.skuCabinetFilePath('a'.repeat(40) + 'b'));
check('skuCabinetFilePath: 実績のある短いSKUの名前は変わらない (転送済みを作り直さない)',
  listing.skuCabinetFilePath('plastic-ki') === 'plastic-ki-sku.jpg');
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
// 後始末は NULL ではなく妥当な値に (2026-09-05 から未選択は出品ゲートで止まる)
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '5', normal_delivery_date_id = NULL WHERE draft_id = ?`).run(rkId);

// 税率の不正値は fail-closed (Codex R1 Medium-1)
dbmod.upsertDraftYahoo(db, rkId, { tax_rate: '9.6%' });
b27 = listing.buildItemPayload(db, rkId);
check('payload: 税率の不正値を弾く (8/10/空欄のみ)', b27.ok === false && b27.reasons.some((r) => r.includes('税率')), JSON.stringify(b27.reasons));
dbmod.upsertDraftYahoo(db, rkId, { tax_rate: '10%' });

// 商品属性の行に「カタログID」を手入力する経路は廃止 (2026-09-02: 入口は基本情報タブだけ)。
// JAN欄と一致していても弾く (旧データの掃除を促す)
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"カタログID","values":["4901234567894"]}]' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 属性行のカタログIDは弾く (入口は基本情報タブだけ)',
  b27.ok === false && b27.reasons.some((r) => r.includes('属性の行に「カタログID」')), JSON.stringify(b27.reasons));

// JAN欄の不正値 (チェックデジット違い) は止める
db.prepare(`UPDATE product_drafts SET jan_code = '4901234567890' WHERE id = ?`).run(rkId);
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["ノーブランド品"]}]' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: JAN欄の不正値を弾く',
  b27.ok === false && b27.reasons.some((r) => r.includes('JANコード') && r.includes('不正')), JSON.stringify(b27.reasons));
db.prepare(`UPDATE product_drafts SET jan_code = '4901234567894' WHERE id = ?`).run(rkId);
db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["ノーブランド品"]}]' WHERE draft_id = ?`).run(rkId);

// 転送後に削除した画像は送らない (Codex R1 Medium-2: draft_images との JOIN)
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gstale', '/app-newitems/rk-smoke-1-stale.jpg')`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 削除済み画像 (転送履歴のみ) は送らない',
  b27.ok === true && b27.payload.images.every((i) => !i.location.includes('stale')), JSON.stringify(b27.reasons || b27.payload?.images));

// ─── 商品ページ表記の統合 (Codex R1 high: import だけで未統合だった回帰) ───
db.prepare(`INSERT INTO draft_page_info (draft_id, product_type, content_volume) VALUES (?, 'cosmetics', '50ml')`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
check('payload: 化粧品の必須記載不足は登録をブロック',
  b27.ok === false && b27.reasons.some((r) => r.includes('商品ページ表記')), JSON.stringify(b27.reasons));
db.prepare(`UPDATE draft_page_info SET seller_name = 'メーカーA', origin_type = '日本製', category_label = '化粧品' WHERE draft_id = ?`).run(rkId);
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '5' WHERE draft_id = ?`).run(rkId);
b27 = listing.buildItemPayload(db, rkId);
// 発送方法の行は出さない (2026-08-31 中原さん: 表には不要。配送方法は画像末尾のバナーで見せている)
check('payload: 充足すると説明文末尾に表を連結 (発送方法の行は出さない)',
  b27.ok === true
  && b27.payload.productDescription.pc.includes('<table')
  && b27.payload.productDescription.pc.includes('メーカーA')
  && !b27.payload.productDescription.pc.includes('発送方法')
  && !b27.payload.productDescription.pc.includes('ネコポス'),
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
// 発送方法の行を出さない状態を作る (バナーの付かない配送方法。NULL にすると出品ゲートで止まる)
db.prepare(`UPDATE draft_rakuten SET shipping_method_group = '4' WHERE draft_id = ?`).run(rkId);
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
db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, shipping_method_group) VALUES (?, '1', '4')`).run(rkvId);
db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, 'g2')`).run(rkvId);
db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'g2', '/x/y.jpg')`).run(rkvId);
// 出品できる状態にする (TOP画像 sort=0 + 詳細画像は対象外) → 残る不足は項目選択肢だけ
db.prepare('UPDATE draft_images SET sort = 0 WHERE draft_id = ?').run(rkvId);
db.prepare('UPDATE product_drafts SET detail_images_excluded = 1, jan_code = NULL WHERE id = ?').run(rkvId);
let bv = listing.buildItemPayload(db, rkvId);
check('カラバリ: 項目選択肢の見出しと値が無ければ止める',
  bv.ok === false
  && bv.reasons.some((r) => r.includes('項目名'))
  && bv.reasons.some((r) => r.includes('rkv-a'))
  && bv.reasons.some((r) => r.includes('rkv-b')), JSON.stringify(bv.reasons));

// 見出しと値を入れると payload が組める
db.prepare("UPDATE draft_rakuten SET variant_selector_name = 'カラー' WHERE draft_id = ?").run(rkvId);
const insSel = db.prepare('INSERT INTO draft_sku_selector_values (draft_id, sku_code, value) VALUES (?, ?, ?)');
insSel.run(rkvId, 'rkv-a', 'ブラック');
insSel.run(rkvId, 'rkv-b', 'ホワイト');
db.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)').run(rkvId, 'rkv-a', '4901234567894');
// SKU別売価 (画面入力) が最優先。NE の標準売価より強い
db.prepare('INSERT INTO draft_sku_prices (draft_id, sku_code, price) VALUES (?, ?, ?)').run(rkvId, 'rkv-a', 2480);
bv = listing.buildItemPayload(db, rkvId);
check('カラバリ: SKU別売価 (画面入力) が NE の標準売価より優先される',
  bv.ok === true && bv.payload.variants['rkv-a'].standardPrice === 2480,
  JSON.stringify(bv.ok ? bv.payload.variants['rkv-a'] : bv.reasons));
check('カラバリ: variantSelectors は key/displayName/values[].displayValue の形',
  bv.ok === true && Array.isArray(bv.payload.variantSelectors)
  && bv.payload.variantSelectors.length === 1
  && bv.payload.variantSelectors[0].key === 'カラー'
  && bv.payload.variantSelectors[0].displayName === 'カラー'
  && bv.payload.variantSelectors[0].values.map((v) => v.displayValue).join(',') === 'ブラック,ホワイト',
  JSON.stringify(bv.ok ? bv.payload.variantSelectors : bv.reasons));
check('カラバリ: variants は SKU ごと + selectorValues + SKU別カタログID',
  bv.ok === true && Object.keys(bv.payload.variants).sort().join(',') === 'rkv-a,rkv-b'
  && bv.payload.variants['rkv-a'].selectorValues['カラー'] === 'ブラック'
  && bv.payload.variants['rkv-a'].merchantDefinedSkuId === 'rkv-a'
  && bv.payload.variants['rkv-a'].articleNumber.value === '4901234567894'
  && bv.payload.variants['rkv-b'].articleNumber.exemptionReason === 5,
  JSON.stringify(bv.ok ? bv.payload.variants : bv.reasons));

// 同じ値は DB の UNIQUE が拒否する (buildItemPayload 側の重複チェックは取込など別経路のバックストップ)
let selDupErr = null;
try {
  db.prepare("UPDATE draft_sku_selector_values SET value = 'ブラック' WHERE draft_id = ? AND sku_code = 'rkv-b'").run(rkvId);
} catch (e) { selDupErr = e; }
check('カラバリ: 同じ項目選択肢を2つのSKUに付けられない (DBのUNIQUE)',
  selDupErr !== null && /UNIQUE/i.test(String(selDupErr.message)), String(selDupErr && selDupErr.message));
db.prepare('DELETE FROM draft_sku_selector_values WHERE draft_id = ?').run(rkvId);
db.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ?').run(rkvId);
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
db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, shipping_method_group)
  VALUES (?, '900001', '[{"name":"ブランド名","values":["テストブランド"]},{"name":"代表カラー","values":["ブラック"]}]', '4')`).run(gdId);
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

// 数値の属性 (総容量・総重量 など。fixture では「総枚数」= NUMBER・単位 枚) — 2026-09-14 cassisp30 の IE0418
// 楽天は values に数値だけ + unit を求める。「30枚」「３０」「30」はどれも {values:['30'], unit:'枚'} で送る
{
  const attrOf = (b) => (b.ok ? b.payload.variants['gd-smoke-1'].attributes.find((a) => a.name === '総枚数') : null);
  const base = '[{"name":"ブランド名","values":["x"]},{"name":"代表カラー","values":["黒"]},{"name":"総枚数","values":[VALUE]}]';
  const put = (v) => db.prepare('UPDATE draft_rakuten SET attributes_json = ? WHERE draft_id = ?').run(base.replace('VALUE', JSON.stringify(v)), gdId);
  const cases = [['30枚', '30'], ['３０', '30'], ['30', '30'], [' 1,000 枚 ', '1000'], ['2.5', '2.5']];
  for (const [input, want] of cases) {
    put(input);
    const b = listing.buildItemPayload(db, gdId);
    const a = attrOf(b);
    check(`数値の属性: 「${input}」→ values ['${want}'] + unit '枚'`,
      !!a && a.values.length === 1 && a.values[0] === want && a.unit === '枚', JSON.stringify(b.ok ? a : b.reasons));
  }
  put('三十枚');
  let b = listing.buildItemPayload(db, gdId);
  check('数値の属性: 数値で始まらない値は送る前に止める (単位の例つき)',
    b.ok === false && b.reasons.some((r) => r.includes('総枚数') && r.includes('数値で入れて') && r.includes('30枚')), JSON.stringify(b.reasons));
  put('1234567890');
  b = listing.buildItemPayload(db, gdId);
  check('数値の属性: 上限 (999999999) を超える値は止める', b.ok === false && b.reasons.some((r) => r.includes('総枚数') && r.includes('大きすぎる')), JSON.stringify(b.reasons));
  put('1.12345678');
  b = listing.buildItemPayload(db, gdId);
  check('数値の属性: 小数 8 桁は止める', b.ok === false && b.reasons.some((r) => r.includes('総枚数') && r.includes('7 桁')), JSON.stringify(b.reasons));
  put('30枚');
  b = listing.buildItemPayload(db, gdId);
  check('数値の属性: 文字の属性 (ブランド名) と カタログID は変えない (unit を付けない)',
    b.ok === true && b.payload.variants['gd-smoke-1'].attributes.filter((a) => a.name !== '総枚数').every((a) => !('unit' in a))
    && b.payload.variants['gd-smoke-1'].attributes.find((a) => a.name === 'ブランド名').values[0] === 'x',
    JSON.stringify(b.ok ? b.payload.variants['gd-smoke-1'].attributes : b.reasons));
}
// 分け方そのもの (表記ゆれ)
check('splitNumberWithUnit: 30g / ３０ｇ / 1.5kg / 500ｍｌ / 2l / 2ℓ / 10cc / 3キロ',
  JSON.stringify([
    listing.splitNumberWithUnit('30g', 'g'), listing.splitNumberWithUnit('３０ｇ', 'g'), listing.splitNumberWithUnit('1.5kg', 'g'),
    listing.splitNumberWithUnit('500ｍｌ', 'ml'), listing.splitNumberWithUnit('2l', 'ml'), listing.splitNumberWithUnit('2ℓ', 'ml'),
    listing.splitNumberWithUnit('10cc', 'ml'), listing.splitNumberWithUnit('3キロ', 'g'),
  ].map((p) => p.value + p.unit)) === JSON.stringify(['30g', '30g', '1.5kg', '500ml', '2L', '2L', '10ml', '3kg']));
check('splitNumberWithUnit: 単位を書かなければ基準単位 / 基準単位の大文字小文字は揃える',
  listing.splitNumberWithUnit('30', 'ml').unit === 'ml' && listing.splitNumberWithUnit('4l', 'L').unit === 'L');
check('splitNumberWithUnit: 数値で始まらない・空は ok:false',
  listing.splitNumberWithUnit('約30g', 'g').ok === false && listing.splitNumberWithUnit('', 'g').ok === false && listing.splitNumberWithUnit(null, 'g').ok === false);
// cassisp30 で実際に返ってきたエラー (2026-09-14) が、次にやることの分かる文になる
{
  const msg = listing.translateRmsError('IE0418', 'Invalid attribute or genreId is set.', {
    details: [
      { code: 'invalidNumberValue', message: 'attributes.values is invalid.', properties: { attributeName: '総容量', minValue: 0, maxValue: 999999999, decimalPlaceLimit: 7 } },
      { code: 'invalidNoUnitAndValues', message: 'attributes.unit and attributes.values are mandatory when the target attribute has a base unit.', properties: { attributeName: '総容量' } },
    ],
    propertyPath: 'variants.cassisp30.attributes[5]',
  });
  check('translateRmsError: 数値・単位の IE0418 は「数値と単位で」と案内する', !!msg && msg.includes('「総容量」') && msg.includes('数値と単位') && msg.includes('attributes[5]'), msg);
}

// #1343 Codex R1: 複数値の単位・単位なしの数値・文字の属性・上限ちょうど (ジャンル 900002 = 単位つき複数値の属性を持つテスト用辞書)
{
  const dict2 = [
    { name: 'ブランド名', dataType: 'STRING', mandatory: false, mandatoryType: 'OPTIONAL_NAVIGATION', multiValueLimit: 3, inputMethod: 'DESCRIPTIVE', maxLength: 100, unit: null },
    { name: '総重量', dataType: 'NUMBER', mandatory: false, mandatoryType: 'OPTIONAL_NAVIGATION', multiValueLimit: 3, inputMethod: 'DESCRIPTIVE', maxLength: null, unit: 'g' },
    { name: '個数', dataType: 'NUMBER', mandatory: false, mandatoryType: 'OPTIONAL_NAVIGATION', multiValueLimit: 1, inputMethod: 'DESCRIPTIVE', maxLength: null, unit: null },
    { name: 'メモ', dataType: 'STRING', mandatory: false, mandatoryType: 'OPTIONAL_NAVIGATION', multiValueLimit: 1, inputMethod: 'DESCRIPTIVE', maxLength: 100, unit: 'g' },
  ];
  db.prepare("INSERT OR REPLACE INTO ph_genre_attributes (genre_id, genre_name, genre_path, payload_json, fixed_at) VALUES ('900002', 'テスト2', 'A > B', ?, NULL)").run(JSON.stringify(dict2));
  const put2 = (attrs) => db.prepare("UPDATE draft_rakuten SET genre_id = '900002', attributes_json = ? WHERE draft_id = ?").run(JSON.stringify(attrs), gdId);
  const find = (b, name) => (b.ok ? b.payload.variants['gd-smoke-1'].attributes.find((a) => a.name === name) : null);
  put2([{ name: 'ブランド名', values: ['x'] }, { name: '総重量', values: ['1kg', '2kg'] }]);
  let b2 = listing.buildItemPayload(db, gdId);
  check('数値の属性 (複数値): 単位がそろっていれば {values:[1,2], unit:kg}',
    JSON.stringify(find(b2, '総重量')) === JSON.stringify({ name: '総重量', values: ['1', '2'], unit: 'kg' }),
    JSON.stringify(b2.ok ? find(b2, '総重量') : b2.reasons));
  for (const mixed of [['1kg', '500g'], ['1kg', '500']]) {
    put2([{ name: 'ブランド名', values: ['x'] }, { name: '総重量', values: mixed }]);
    b2 = listing.buildItemPayload(db, gdId);
    check('数値の属性 (複数値): 単位が違えば送る前に止める (' + mixed.join(' / ') + ')',
      b2.ok === false && b2.reasons.some((r) => r.includes('総重量') && r.includes('そろっていません')), JSON.stringify(b2.reasons));
  }
  check('toRmsAttribute: 単位がそろっていない複数値は変えない (換算しない)',
    JSON.stringify(listing.toRmsAttribute({ name: '総重量', values: ['1kg', '500g'] }, dict2[1])) === JSON.stringify({ name: '総重量', values: ['1kg', '500g'] }));
  put2([{ name: 'ブランド名', values: ['x'] }, { name: '個数', values: ['３'] }]);
  b2 = listing.buildItemPayload(db, gdId);
  check('数値の属性 (単位なし): 数値だけにそろえ unit は付けない',
    JSON.stringify(find(b2, '個数')) === JSON.stringify({ name: '個数', values: ['3'] }), JSON.stringify(b2.ok ? find(b2, '個数') : b2.reasons));
  put2([{ name: 'ブランド名', values: ['x'] }, { name: '個数', values: ['3個'] }]);
  b2 = listing.buildItemPayload(db, gdId);
  check('数値の属性 (単位なし): 単位を書いたら止める',
    b2.ok === false && b2.reasons.some((r) => r.includes('個数') && r.includes('単位を付けずに')), JSON.stringify(b2.reasons));
  put2([{ name: 'ブランド名', values: ['x'] }, { name: 'メモ', values: ['30g'] }]);
  b2 = listing.buildItemPayload(db, gdId);
  check('文字の属性は辞書に単位があっても変えない',
    JSON.stringify(find(b2, 'メモ')) === JSON.stringify({ name: 'メモ', values: ['30g'] }), JSON.stringify(b2.ok ? find(b2, 'メモ') : b2.reasons));
  put2([{ name: 'ブランド名', values: ['x'] }, { name: '総重量', values: ['999999999'] }, { name: '個数', values: ['1.1234567'] }]);
  b2 = listing.buildItemPayload(db, gdId);
  check('数値の属性: 上限ちょうど・小数 7 桁は通る',
    b2.ok === true && find(b2, '総重量').values[0] === '999999999' && find(b2, '総重量').unit === 'g' && find(b2, '個数').values[0] === '1.1234567',
    JSON.stringify(b2.ok ? b2.payload.variants['gd-smoke-1'].attributes : b2.reasons));
  db.prepare("UPDATE draft_rakuten SET genre_id = '900001' WHERE draft_id = ?").run(gdId);
}
check('splitNumberWithUnit: 数値の続きに見える書き方・知らない英字の単位は ok:false (1.2.3g / 1..5g / 1e3g / 30oz)',
  ['1.2.3g', '1..5g', '1e3g', '30oz'].every((v) => listing.splitNumberWithUnit(v, 'g').ok === false));
check('splitNumberWithUnit: 大文字の単位・空白入りは通る (30 G / 1.5 KG)',
  listing.splitNumberWithUnit('30 G', 'g').unit === 'g' && listing.splitNumberWithUnit('1.5 KG', 'g').unit === 'kg');
// #1343 Codex R2: 基準単位も同じ規則でそろえて比べ、同じ単位なら辞書の表記で送る
check('splitNumberWithUnit: 全角の基準単位 (Ｗ) でも 30Ｗ・30W・30w は同じ単位 (辞書の表記で返す)',
  ['30Ｗ', '30W', '30w'].every((v) => { const p = listing.splitNumberWithUnit(v, 'Ｗ'); return p.ok && p.value === '30' && p.unit === 'Ｗ'; }));
check('splitNumberWithUnit: 日本語の基準単位 (グラム) でも 2g・2グラム・2 は同じ単位',
  ['2g', '2グラム', '2'].every((v) => { const p = listing.splitNumberWithUnit(v, 'グラム'); return p.ok && p.value === '2' && p.unit === 'グラム'; }));
check('toRmsAttribute: 日本語の基準単位で単位なしと 2g が混ざっても同じ単位として送る',
  JSON.stringify(listing.toRmsAttribute({ name: '総重量', values: ['1', '2g'] }, { dataType: 'NUMBER', unit: 'グラム' }))
    === JSON.stringify({ name: '総重量', values: ['1', '2'], unit: 'グラム' }));
check('splitNumberWithUnit: 基準単位と別の知っている単位はそろえた表記 (基準 グラム に 1.5kg → kg)',
  listing.splitNumberWithUnit('1.5kg', 'グラム').unit === 'kg' && listing.splitNumberWithUnit('30oz', 'グラム').ok === false);

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
  gb.ok === false && gb.reasons.some((r) => r.includes('カタログID') && r.includes('「カタログID」の行')), JSON.stringify(gb.reasons));

// ─── バリエーション + 辞書にカタログIDあり: 属性は SKU ごとに自分の JAN (2026-09-02 根本対策) ───
// それまではページ代表の jan_code を全 SKU の「カタログID」属性に付けていて、SKU の articleNumber と食い違っていた。
// ページ代表の jan_code (ここでは 4999999999999) は楽天には使わない
{
  insProd.run(9311, 'gdv-a', 'バリA', 'gdv');
  insProd.run(9312, 'gdv-b', 'バリB', 'gdv');
  const gdvId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, price, jan_code) VALUES ('gdv', '辞書バリエ', 1500, '4999999999999')`).run().lastInsertRowid);
  db.prepare(`UPDATE product_drafts SET detail_images_excluded = 1 WHERE id = ?`).run(gdvId);
  db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, variant_selector_name, shipping_method_group)
    VALUES (?, '900001', '[{"name":"ブランド名","values":["x"]},{"name":"代表カラー","values":["黒"]}]', 'カラー', '4')`).run(gdvId);
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'gdv1', 0)`).run(gdvId);
  db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gdv1', '/x/gdv1.jpg')`).run(gdvId);
  const insSelV = db.prepare('INSERT INTO draft_sku_selector_values (draft_id, sku_code, value) VALUES (?, ?, ?)');
  insSelV.run(gdvId, 'gdv-a', '黒');
  insSelV.run(gdvId, 'gdv-b', '白');
  db.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)').run(gdvId, 'gdv-a', '4901234567894');
  let gv = listing.buildItemPayload(db, gdvId);
  check('genre×バリエーション: JAN の無い SKU があると SKU 名つきで止まる (ページ代表の jan_code は見ない)',
    gv.ok === false
    && gv.reasons.some((r) => r.includes('gdv-b') && r.includes('カタログID') && r.includes('SKU表'))
    && !gv.reasons.some((r) => r.includes('gdv-a') && r.includes('カタログID')),
    JSON.stringify(gv.reasons));
  db.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)').run(gdvId, 'gdv-b', '4999999999999');
  gv = listing.buildItemPayload(db, gdvId);
  const catOf = (sku) => ((gv.ok && gv.payload.variants[sku].attributes) || []).filter((a) => a.name === 'カタログID').map((a) => a.values[0]);
  check('genre×バリエーション: カタログID属性は SKU ごとに自分の JAN (articleNumber と一致)',
    gv.ok === true
    && catOf('gdv-a').join() === '4901234567894' && gv.payload.variants['gdv-a'].articleNumber.value === '4901234567894'
    && catOf('gdv-b').join() === '4999999999999' && gv.payload.variants['gdv-b'].articleNumber.value === '4999999999999',
    JSON.stringify(gv.ok ? gv.payload.variants : gv.reasons));
  // SKU 表の数値の属性も SKU ごとに {values:[数値], unit} で送る (#1343 Codex R1 low)
  const insSkuAttr = db.prepare('INSERT INTO draft_sku_attributes (draft_id, sku_code, name, value) VALUES (?, ?, ?, ?)');
  insSkuAttr.run(gdvId, 'gdv-a', '総枚数', '30枚');
  insSkuAttr.run(gdvId, 'gdv-b', '総枚数', '５');
  gv = listing.buildItemPayload(db, gdvId);
  const numOf = (sku) => ((gv.ok && gv.payload.variants[sku].attributes) || []).find((a) => a.name === '総枚数') || null;
  check('genre×バリエーション: SKU 表の数値の属性も SKU ごとに {values:[数値], unit}',
    gv.ok === true && JSON.stringify(numOf('gdv-a')) === JSON.stringify({ name: '総枚数', values: ['30'], unit: '枚' })
    && JSON.stringify(numOf('gdv-b')) === JSON.stringify({ name: '総枚数', values: ['5'], unit: '枚' }),
    JSON.stringify(gv.ok ? gv.payload.variants : gv.reasons));
  db.prepare("UPDATE draft_sku_attributes SET value = '五枚' WHERE draft_id = ? AND sku_code = 'gdv-b' AND name = '総枚数'").run(gdvId);
  gv = listing.buildItemPayload(db, gdvId);
  check('genre×バリエーション: 数値で読めない SKU の値は SKU 名つきで止める',
    gv.ok === false && gv.reasons.some((r) => r.includes('gdv-b') && r.includes('総枚数') && r.includes('数値で入れて')), JSON.stringify(gv.reasons));
  db.prepare('DELETE FROM draft_sku_attributes WHERE draft_id = ?').run(gdvId);
  // 辞書に無いジャンルでは SKU にもカタログID属性を付けない (IE1002 対策はバリエーションでも同じ)
  db.prepare(`UPDATE draft_rakuten SET genre_id = '999999', attributes_json = '[]' WHERE draft_id = ?`).run(gdvId);
  gv = listing.buildItemPayload(db, gdvId);
  check('genre×バリエーション: 辞書未取得ジャンルでは SKU にカタログID属性を付けない',
    gv.ok === true && catOf('gdv-a').length === 0 && gv.payload.variants['gdv-a'].articleNumber.value === '4901234567894',
    JSON.stringify(gv.ok ? gv.payload.variants : gv.reasons));
  db.prepare('DELETE FROM draft_sku_selector_values WHERE draft_id = ?').run(gdvId);
  db.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ?').run(gdvId);
  db.prepare('DELETE FROM draft_cabinet_images WHERE draft_id = ?').run(gdvId);
  db.prepare('DELETE FROM draft_images WHERE draft_id = ?').run(gdvId);
  db.prepare('DELETE FROM draft_rakuten WHERE draft_id = ?').run(gdvId);
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(gdvId);
  db.prepare('DELETE FROM mirror_products WHERE product_id IN (9311, 9312)').run();
}

// ─── SKU ごとの商品仕様 (2026-09-03 中原さん: RMS と同じ SKU 列 × 項目行の表) ───
// ページ共通の attributes_json / article_number を既定値に、draft_sku_attributes が SKU ごとに上書き ('' = 明示的に空)。
// payload は SKU ごとに検証・送信する
{
  insProd.run(9321, 'gsa-a', 'バリA', 'gsa');
  insProd.run(9322, 'gsa-b', 'バリB', 'gsa');
  const gsaId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, price) VALUES ('gsa', '仕様バリエ', 1500)`).run().lastInsertRowid);
  db.prepare(`UPDATE product_drafts SET detail_images_excluded = 1 WHERE id = ?`).run(gsaId);
  db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, article_number, variant_selector_name, shipping_method_group)
    VALUES (?, '900001', '[{"name":"ブランド名","values":["共通ブランド"]}]', 'M-COMMON', 'カラー', '4')`).run(gsaId);
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'gsa1', 0)`).run(gsaId);
  db.prepare(`INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location) VALUES (?, 'gsa1', '/x/gsa1.jpg')`).run(gsaId);
  const insSelA = db.prepare('INSERT INTO draft_sku_selector_values (draft_id, sku_code, value) VALUES (?, ?, ?)');
  insSelA.run(gsaId, 'gsa-a', '黒'); insSelA.run(gsaId, 'gsa-b', '白');
  const insJanA = db.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)');
  insJanA.run(gsaId, 'gsa-a', '4901234567894'); insJanA.run(gsaId, 'gsa-b', '4999999999999');
  const insAttr = db.prepare('INSERT INTO draft_sku_attributes (draft_id, sku_code, name, value) VALUES (?, ?, ?, ?)');
  insAttr.run(gsaId, 'gsa-a', '代表カラー', '黒');
  insAttr.run(gsaId, 'gsa-b', '代表カラー', '白');
  insAttr.run(gsaId, 'gsa-b', listing.MODEL_ATTR_NAME, 'M-B');
  const members = vari.resolveVariationGroup(db, 'gsa', { draftId: gsaId, withMembers: true }).members;
  const grid = listing.skuAttributeGrid(db, gsaId, db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(gsaId), members);
  check('skuAttributeGrid: ページ共通が既定値・SKU 行が上書き (名前の並び = 共通 → SKU)',
    grid.names.join(',') === 'ブランド名,' + listing.MODEL_ATTR_NAME + ',代表カラー'
    && grid.bySku.get('gsa-a').get('ブランド名').join() === '共通ブランド' && grid.bySku.get('gsa-a').get('代表カラー').join() === '黒'
    && grid.bySku.get('gsa-a').get(listing.MODEL_ATTR_NAME).join() === 'M-COMMON' && grid.bySku.get('gsa-b').get(listing.MODEL_ATTR_NAME).join() === 'M-B'
    && grid.explicit.get('gsa-b').has(listing.MODEL_ATTR_NAME) && !grid.explicit.get('gsa-a').has(listing.MODEL_ATTR_NAME),
    JSON.stringify({ names: grid.names, a: [...grid.bySku.get('gsa-a').entries()], b: [...grid.bySku.get('gsa-b').entries()] }));
  // 共通の多値属性は配列のまま (Codex R1 medium)。SKU 行は「|」区切りで複数値 (RMS と同じ)
  {
    const rk2 = { attributes_json: '[{"name":"ブランド名","values":["A","B"]}]', article_number: null };
    insAttr.run(gsaId, 'gsa-b', 'ブランド名', 'X | Y | Z');
    const g2 = listing.skuAttributeGrid(db, gsaId, rk2, members);
    check('skuAttributeGrid: 共通の多値は配列のまま・SKU 行は | 区切りで配列に',
      g2.bySku.get('gsa-a').get('ブランド名').join(',') === 'A,B' && g2.bySku.get('gsa-b').get('ブランド名').join(',') === 'X,Y,Z',
      JSON.stringify([...g2.bySku.get('gsa-b').entries()]));
    db.prepare(`DELETE FROM draft_sku_attributes WHERE draft_id = ? AND sku_code = 'gsa-b' AND name = 'ブランド名'`).run(gsaId);
    // 旧データ: メーカー型番が属性側だけにある → 既定値に引き上げる / 欄と食い違い → 既定値にせず conflict
    const g3 = listing.skuAttributeGrid(db, gsaId, { attributes_json: '[{"name":"メーカー型番","values":["OLD"]}]', article_number: null }, members);
    const g4 = listing.skuAttributeGrid(db, gsaId, { attributes_json: '[{"name":"メーカー型番","values":["OLD"]}]', article_number: 'NEW' }, members);
    check('skuAttributeGrid: 属性側だけの旧メーカー型番は既定値に / 欄と食い違えば conflict (欄の値を既定値)',
      g3.bySku.get('gsa-a').get(listing.MODEL_ATTR_NAME).join() === 'OLD' && g3.legacyModelConflict === false
      && g4.legacyModelConflict === true && g4.bySku.get('gsa-a').get(listing.MODEL_ATTR_NAME).join() === 'NEW',
      JSON.stringify({ g3: g3.legacyModels, g4: g4.legacyModels }));
  }
  let gs = listing.buildItemPayload(db, gsaId);
  const attrsOfSku = (sku) => ((gs.ok && gs.payload.variants[sku].attributes) || []).map((a) => a.name + '=' + a.values.join('|')).sort().join(',');
  check('payload×SKU仕様: SKU ごとに違う代表カラー + 共通ブランド名 + SKU 別カタログID (辞書に無いメーカー型番は送らない)',
    gs.ok === true
    && attrsOfSku('gsa-a') === 'カタログID=4901234567894,ブランド名=共通ブランド,代表カラー=黒'
    && attrsOfSku('gsa-b') === 'カタログID=4999999999999,ブランド名=共通ブランド,代表カラー=白',
    JSON.stringify(gs.ok ? gs.payload.variants : gs.reasons));
  // '' の行 = 明示的に空 (共通の既定値を打ち消す) → その SKU だけ必須欠落
  insAttr.run(gsaId, 'gsa-a', 'ブランド名', '');
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: 空の行は既定値を打ち消し、その SKU だけ SKU 名つきで必須欠落',
    gs.ok === false
    && gs.reasons.some((r) => r.includes('gsa-a') && r.includes('必須属性「ブランド名」'))
    && !gs.reasons.some((r) => r.includes('gsa-b') && r.includes('ブランド名')),
    JSON.stringify(gs.reasons));
  db.prepare(`DELETE FROM draft_sku_attributes WHERE draft_id = ? AND sku_code = 'gsa-a' AND name = 'ブランド名'`).run(gsaId);
  // 辞書に無い属性名は SKU 名つきで止める
  insAttr.run(gsaId, 'gsa-b', '存在しない属性', 'z');
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: 辞書に無い属性名は SKU 名つきで止める (IE1002 対策)',
    gs.ok === false && gs.reasons.some((r) => r.includes('gsa-b') && r.includes('存在しない属性') && r.includes('IE1002')), JSON.stringify(gs.reasons));
  db.prepare(`DELETE FROM draft_sku_attributes WHERE draft_id = ? AND name = '存在しない属性'`).run(gsaId);
  // SKU 行に「カタログID」は入れさせない (JAN は専用行)
  insAttr.run(gsaId, 'gsa-a', 'カタログID', '4901234567894');
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: SKU 行の「カタログID」は止める', gs.ok === false && gs.reasons.some((r) => r.includes('gsa-a') && r.includes('「カタログID」の行')), JSON.stringify(gs.reasons));
  db.prepare(`DELETE FROM draft_sku_attributes WHERE draft_id = ? AND name = 'カタログID'`).run(gsaId);
  // SKU ごとの「IDなしの理由」: 辞書の無いジャンルで b の JAN を外し、b だけ理由 3
  db.prepare(`UPDATE draft_rakuten SET genre_id = '999999', attributes_json = '[]' WHERE draft_id = ?`).run(gsaId);
  db.prepare(`DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code = 'gsa-b'`).run(gsaId);
  db.prepare('INSERT INTO draft_sku_catalog_exemptions (draft_id, sku_code, reason) VALUES (?, ?, 3)').run(gsaId, 'gsa-b');
  db.prepare(`UPDATE draft_rakuten SET catalog_id_exemption_reason = 4 WHERE draft_id = ?`).run(gsaId);
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: カタログIDなしの理由は SKU ごと (無い SKU はページ共通の理由)',
    gs.ok === true
    && gs.payload.variants['gsa-a'].articleNumber.value === '4901234567894'
    && gs.payload.variants['gsa-b'].articleNumber.exemptionReason === 3,
    JSON.stringify(gs.ok ? gs.payload.variants : gs.reasons));
  db.prepare(`DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code = 'gsa-a'`).run(gsaId);
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: SKU の理由が無ければページ共通の理由で送る',
    gs.ok === true && gs.payload.variants['gsa-a'].articleNumber.exemptionReason === 4, JSON.stringify(gs.ok ? gs.payload.variants : gs.reasons));
  // 理由 1 (セット商品) は SKU 単位で未対応チェック
  db.prepare(`UPDATE draft_sku_catalog_exemptions SET reason = 1 WHERE draft_id = ? AND sku_code = 'gsa-b'`).run(gsaId);
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: SKU の理由 1 (セット商品) は未対応で止める', gs.ok === false && gs.reasons.some((r) => r.includes('セット商品')), JSON.stringify(gs.reasons));
  db.prepare(`UPDATE draft_sku_catalog_exemptions SET reason = 3 WHERE draft_id = ? AND sku_code = 'gsa-b'`).run(gsaId);
  // 共通の多値属性はそのまま複数値で送る (Codex R1 medium)。辞書 900001 の ブランド名 は multiValueLimit 3
  insJanA.run(gsaId, 'gsa-a', '4901234567894'); insJanA.run(gsaId, 'gsa-b', '4999999999999');
  db.prepare(`UPDATE draft_rakuten SET genre_id = '900001', attributes_json = '[{"name":"ブランド名","values":["A","B"]}]' WHERE draft_id = ?`).run(gsaId);
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: 共通の多値属性は values 配列のまま送る',
    gs.ok === true && (gs.payload.variants['gsa-a'].attributes.find((a) => a.name === 'ブランド名') || {}).values.join(',') === 'A,B',
    JSON.stringify(gs.ok ? gs.payload.variants['gsa-a'].attributes : gs.reasons));
  insAttr.run(gsaId, 'gsa-a', 'ブランド名', 'P | Q | R | S');
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: SKU 行の | 区切りも複数値として上限 (3 個) を検査する',
    gs.ok === false && gs.reasons.some((r) => r.includes('gsa-a') && r.includes('最大 3 個')), JSON.stringify(gs.reasons));
  db.prepare(`DELETE FROM draft_sku_attributes WHERE draft_id = ? AND name = 'ブランド名'`).run(gsaId);
  // 旧データでメーカー型番が食い違っているバリエーション: SKU 表の行が全 SKU に入るまで止める (Codex R1 high)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["A"]},{"name":"メーカー型番","values":["OLD"]}]', article_number = 'NEW' WHERE draft_id = ?`).run(gsaId);
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: 旧メーカー型番の食い違いは SKU 表への入力を促して止める (黙って捨てない)',
    gs.ok === false && gs.reasons.some((r) => r.includes('食い違って') && r.includes('OLD') && r.includes('NEW')), JSON.stringify(gs.reasons));
  insAttr.run(gsaId, 'gsa-a', listing.MODEL_ATTR_NAME, 'M-A');
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: 全 SKU に SKU 表の値が入れば旧データは使わず通る',
    gs.ok === true, JSON.stringify(gs.ok ? gs.payload.variants : gs.reasons));
  // 旧データの「カタログID」属性が残るバリエーションは、警告ボタンでの削除を促して止める (SKU 表には展開しない)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["A"]},{"name":"カタログID","values":["4901234567894"]}]', article_number = 'NEW' WHERE draft_id = ?`).run(gsaId);
  gs = listing.buildItemPayload(db, gsaId);
  check('payload×SKU仕様: 旧データの「カタログID」属性は削除を促して止める (SKU ごとの行にはしない)',
    gs.ok === false && gs.reasons.some((r) => r.includes('旧データ') && r.includes('カタログID') && r.includes('削除'))
    && !gs.reasons.some((r) => r.includes('SKU「gsa-a」の商品仕様に「カタログID」')),
    JSON.stringify(gs.reasons));
  db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"ブランド名","values":["A"]}]' WHERE draft_id = ?`).run(gsaId);
  db.prepare(`DELETE FROM draft_sku_attributes WHERE draft_id = ? AND name = ?`).run(gsaId, listing.MODEL_ATTR_NAME);
  insAttr.run(gsaId, 'gsa-b', listing.MODEL_ATTR_NAME, 'M-B');
  db.prepare('DELETE FROM draft_sku_catalog_exemptions WHERE draft_id = ?').run(gsaId);
  db.prepare('DELETE FROM draft_sku_attributes WHERE draft_id = ?').run(gsaId);
  db.prepare('DELETE FROM draft_sku_selector_values WHERE draft_id = ?').run(gsaId);
  db.prepare('DELETE FROM draft_cabinet_images WHERE draft_id = ?').run(gsaId);
  db.prepare('DELETE FROM draft_images WHERE draft_id = ?').run(gsaId);
  db.prepare('DELETE FROM draft_rakuten WHERE draft_id = ?').run(gsaId);
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(gsaId);
  db.prepare('DELETE FROM mirror_products WHERE product_id IN (9321, 9322)').run();
}

// ─── メーカー型番は「メーカー型番」欄が唯一の入口 (2026-08-31 中原さん指摘) ───
// RMS でも入力項目は 1 つなのに、画面が「メーカー型番」欄と商品属性の 2 箇所に入れさせていた。
// 入口を article_number だけにして、辞書に メーカー型番 があるジャンルでは送信時に自動で積む
{
  const MODEL = listing.MODEL_ATTR_NAME;
  // 辞書に メーカー型番 を足す (必須にして「欄に入っていれば必須欠落にならない」ことも見る)
  const withModel = JSON.parse(JSON.stringify(gnorm.attributes));
  withModel.push({ name: MODEL, mandatory: true, inputMethod: 'DESCRIPTIVE', multiValueLimit: 1, maxLength: 100, unit: null, dataType: 'STRING', mandatoryType: 'MANDATORY' });
  db.prepare(`INSERT OR REPLACE INTO ph_genre_attributes (genre_id, genre_name, genre_path, payload_json, fixed_at)
    VALUES ('900002', ?, ?, ?, ?)`).run(gnorm.genreName, gnorm.genrePath, JSON.stringify(withModel), gnorm.fixedAt);
  db.prepare(`UPDATE product_drafts SET jan_code = '4999999999999' WHERE id = ?`).run(gdId);
  const OK_ATTRS = '[{"name":"ブランド名","values":["x"]},{"name":"代表カラー","values":["黒"]}]';

  // ① 欄に入れれば、属性に メーカー型番 が無くても必須欠落にならず、payload には積まれる
  db.prepare(`UPDATE draft_rakuten SET genre_id = '900002', attributes_json = ?, article_number = 'toys3pen' WHERE draft_id = ?`).run(OK_ATTRS, gdId);
  let bm = listing.buildItemPayload(db, gdId);
  const attrsOf = (r) => (r.ok ? r.payload.variants['gd-smoke-1'].attributes || [] : []);
  check('メーカー型番: 欄に入れれば属性行が無くても通り、属性として自動で積まれる',
    bm.ok === true && attrsOf(bm).some((a2) => a2.name === MODEL && a2.values[0] === 'toys3pen'),
    JSON.stringify(bm.ok ? attrsOf(bm) : bm.reasons));
  // この draft は JAN 付き → articleNumber は JAN になる。型番 (toys3pen) が混ざらないことが要点
  check('メーカー型番: articleNumber (= カタログID) には入れない (2026-09-02 IE0228 の再発防止)',
    bm.ok === true && bm.payload.variants['gd-smoke-1'].articleNumber
    && bm.payload.variants['gd-smoke-1'].articleNumber.value !== 'toys3pen'
    && bm.payload.variants['gd-smoke-1'].articleNumber.exemptionReason === undefined,
    JSON.stringify(bm.ok ? bm.payload.variants['gd-smoke-1'].articleNumber : bm.reasons));

  // ② 欄が空なら、辞書必須の メーカー型番 は今までどおり欠落エラー (黙って通さない)
  db.prepare(`UPDATE draft_rakuten SET article_number = NULL WHERE draft_id = ?`).run(gdId);
  bm = listing.buildItemPayload(db, gdId);
  check('メーカー型番: 欄が空なら辞書必須の欠落として止まる',
    bm.ok === false && bm.reasons.some((r) => r.includes(MODEL)), JSON.stringify(bm.reasons));

  // ③ 旧データで属性側にも残っていて、欄と食い違うときは止める (どちらが正か分からない)
  db.prepare(`UPDATE draft_rakuten SET article_number = 'toys3pen',
    attributes_json = '[{"name":"ブランド名","values":["x"]},{"name":"代表カラー","values":["黒"]},{"name":"メーカー型番","values":["別の型番"]}]'
    WHERE draft_id = ?`).run(gdId);
  bm = listing.buildItemPayload(db, gdId);
  check('メーカー型番: 属性側の旧値と欄が食い違ったら止める',
    bm.ok === false && bm.reasons.some((r) => r.includes('一致しません')), JSON.stringify(bm.reasons));

  // ④ 同じ値なら通り、**二重に積まない** (属性が 2 つになると RMS 側で弾かれる)
  db.prepare(`UPDATE draft_rakuten SET attributes_json =
    '[{"name":"ブランド名","values":["x"]},{"name":"代表カラー","values":["黒"]},{"name":"メーカー型番","values":["toys3pen"]}]'
    WHERE draft_id = ?`).run(gdId);
  bm = listing.buildItemPayload(db, gdId);
  check('メーカー型番: 属性側と同じ値なら通り、属性は 1 つだけ (二重に積まない)',
    bm.ok === true && attrsOf(bm).filter((a2) => a2.name === MODEL).length === 1,
    JSON.stringify(bm.ok ? attrsOf(bm) : bm.reasons));

  // ⑤ 辞書に メーカー型番 が無いジャンルでは属性に積まない (IE1002 になる)
  db.prepare(`UPDATE draft_rakuten SET genre_id = '900001', attributes_json = ?, article_number = 'toys3pen' WHERE draft_id = ?`).run(OK_ATTRS, gdId);
  bm = listing.buildItemPayload(db, gdId);
  check('メーカー型番: 辞書に無いジャンルでは属性に積まない (IE1002 対策)',
    bm.ok === true && !attrsOf(bm).some((a2) => a2.name === MODEL),
    JSON.stringify(bm.ok ? attrsOf(bm) : bm.reasons));
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = NULL WHERE draft_id = ?`).run(OK_ATTRS, gdId);
}

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

// ─── 選択式の属性の選択肢 (2026-09-20): 属性ごとの dictionaryValues から取り、取れなければ null ───
{
  const dictOf = (values) => ({ genre: { genreId: 900001, attributes: [{ id: 8, nameJa: '代表カラー', dictionaryValues: values.map((v, i) => ({ id: i + 1, nameJa: v })) }] } });
  const colors = (from, n) => Array.from({ length: n }, (_, i) => `色${from + i}`);
  const makeFetcher = (dictHandler, calls) => async (path) => {
    calls.push(path);
    if (!path.includes('/dictionaryValues')) return { status: 200, data: RMS_GENRE_FIXTURE };
    return dictHandler(path);
  };
  const colorOf = (r) => r.genre.attributes.find((a) => a.name === '代表カラー');

  // 実応答 (2026-09-20 ジャンル 205761・代表カラー) と同じ振る舞いをする偽の楽天:
  //   page と limit は両方必須 (片方だけ = 400 invalidPageAndLimit) / limit は効く /
  //   最後のページの先は 404 notDictionaryValueFound
  const NO_MORE = { status: 404, data: { errors: [{ code: 'notDictionaryValueFound', message: 'Not dictionaryValue found.' }] } };
  const rakutenLike = (all) => (p) => {
    const page = Number((p.match(/[?&]page=(\d+)/) || [])[1] || 0);
    const limit = Number((p.match(/[?&]limit=(\d+)/) || [])[1] || 0);
    if (!page || !limit) return { status: 400, data: { errors: [{ code: 'invalidPageAndLimit', message: 'Both page and limit parameters are required.' }] } };
    const slice = all.slice((page - 1) * limit, page * limit);
    return slice.length === 0 ? NO_MORE : { status: 200, data: dictOf(slice) };
  };
  const dictCallsOf = (list) => list.filter((p) => p.includes('/dictionaryValues'));

  let calls = [];
  const one = await listing.fetchGenreAttributes(db, '900001', { force: true,
    fetcher: makeFetcher(rakutenLike(['-', 'ホワイト', 'ブラック', ' レッド ']), calls) });
  check('genre: 選択式だけ選択肢を取りに行く (属性ID 8・page と limit を両方送る・1 回で終わり)',
    one.ok && JSON.stringify(colorOf(one).options) === JSON.stringify(['-', 'ホワイト', 'ブラック', 'レッド'])
    && dictCallsOf(calls).length === 1
    && /^\/service-api\/rakuten-rms\/genres\/900001\/attributes\/8\/dictionaryValues\?page=1&limit=1000/.test(dictCallsOf(calls)[0])
    && one.genre.attributes.find((x) => x.name === 'ブランド名').options === undefined,
    JSON.stringify(calls));
  check('genre: 人が押した取り直し (force) は miniPC の失敗のキャッシュも通り越す (refresh=1)',
    dictCallsOf(calls).every((p) => p.includes('refresh=1')), JSON.stringify(calls));
  check('genre: 選択肢は保存され、次はキャッシュから返る',
    JSON.stringify(colorOf({ genre: listing.getCachedGenreAttributes(db, '900001') }).options) === JSON.stringify(['-', 'ホワイト', 'ブラック', 'レッド']));

  calls = [];
  const full = await listing.fetchGenreAttributes(db, '900001', { force: true, fetcher: makeFetcher(rakutenLike(colors(1, 1000)), calls) });
  check('genre: ちょうど満杯のページの次が 404 notDictionaryValueFound なら「そろった」(1000 件)',
    colorOf(full).options.length === 1000 && dictCallsOf(calls).length === 2 && dictCallsOf(calls)[1].includes('page=2&limit=1000'), JSON.stringify(dictCallsOf(calls)));

  const tooMany = await listing.fetchGenreAttributes(db, '900001', { force: true, fetcher: makeFetcher(rakutenLike(colors(1, 1001)), []) });
  check('genre: 多すぎる選択肢は持たない ([] = セレクトにしない・取り直さない)', Array.isArray(colorOf(tooMany).options) && colorOf(tooMany).options.length === 0);

  const noValues = await listing.fetchGenreAttributes(db, '900001', { force: true, fetcher: makeFetcher(rakutenLike([]), []) });
  check('genre: 1 ページ目から notDictionaryValueFound = 選択肢が無い属性 ([]・失敗ではない)', Array.isArray(colorOf(noValues).options) && colorOf(noValues).options.length === 0);

  const samePage = await listing.fetchGenreAttributes(db, '900001', { force: true,
    fetcher: makeFetcher(() => ({ status: 200, data: dictOf(colors(1, 1000)) }), []) });
  check('🚨 genre: page を無視して同じ一覧を返す応答は「そろった」と言えない (null)', colorOf(samePage).options === null);

  const plain404 = await listing.fetchGenreAttributes(db, '900001', { force: true,
    fetcher: makeFetcher((p) => (p.includes('page=2') ? { status: 404, data: null } : { status: 200, data: dictOf(colors(1, 1000)) }), []) });
  check('🚨 genre: 本文の無い 404 (miniPC に口が無い等) は「その先は無い」の証拠にしない (null)', colorOf(plain404).options === null);

  const brokenMid = await listing.fetchGenreAttributes(db, '900001', { force: true,
    fetcher: makeFetcher((p) => (p.includes('page=2') ? { status: 503, data: null } : { status: 200, data: dictOf(colors(1, 1000)) }), []) });
  check('🚨 genre: 途中で落ちたら途中までの一覧を出さない (null = 自由入力のまま)', brokenMid.ok && colorOf(brokenMid).options === null);

  const blankName = await listing.fetchGenreAttributes(db, '900001', { force: true,
    fetcher: makeFetcher((p) => (p.includes('page=1&') ? { status: 200, data: dictOf([...colors(1, 999), '']) } : NO_MORE), calls = []) });
  check('genre: 満杯かどうかは整形前の件数で数える (空の名前が混ざっても次のページを見る)',
    colorOf(blankName).options.length === 999 && dictCallsOf(calls).length === 2, String(dictCallsOf(calls).length));

  // 締切はページごとに見る: 時計を進めるフェッチャで、20 ページぶん待ち続けないこと (Codex R1 medium)
  {
    const realNow = Date.now; let skew = 0; let dictCalls = 0;
    Date.now = () => realNow() + skew;
    try {
      // 毎回 29 秒かかり、いつまでも満杯のページが続く (新しい値は 1 ページ 40 件 = 上限 1,000 件には届かない)
      const endless = (p) => {
        dictCalls += 1; skew += 29_000;
        const page = Number((p.match(/page=(\d+)/) || [])[1]);
        return { status: 200, data: dictOf(Array.from({ length: 1000 }, (_, i) => `色${page}-${i % 40}`)) };
      };
      const slow = await listing.fetchGenreAttributes(db, '900001', { force: true, fetcher: makeFetcher(endless, []) });
      check('genre: 合計 60 秒の締切を超えたら、ページの途中でもやめて出さない (null・通信は 3 回まで)',
        colorOf(slow).options === null && dictCalls === 3, JSON.stringify({ options: colorOf(slow).options, dictCalls }));
    } finally { Date.now = realNow; }
  }

  // miniPC が古い版 (口が無い = 404) でも辞書そのものは使える
  const noRoute = await listing.fetchGenreAttributes(db, '900001', { force: true,
    fetcher: makeFetcher(() => ({ status: 404, data: null }), []) });
  check('genre: 選択肢が取れなくても辞書は保存される', noRoute.ok && colorOf(noRoute).options === null
    && listing.getCachedGenreAttributes(db, '900001').attributes.length === 4);
  calls = [];
  const keep = await listing.fetchGenreAttributes(db, '900001', { fetcher: makeFetcher(() => ({ status: 404, data: null }), calls) });
  check('genre: 取れなかった印 (null) は、出品・プレビューの経路では取り直さない', keep.cached === true && calls.length === 0);
  const retry = await listing.fetchGenreAttributes(db, '900001', { retryOptions: true,
    fetcher: makeFetcher(rakutenLike(['ホワイト']), calls) });
  check('genre: 「ジャンル情報を取得」(retryOptions) では取り直す (miniPC の失敗のキャッシュも通り越す)',
    retry.cached === false && colorOf(retry).options.length === 1
    && calls.filter((p) => p.includes('/dictionaryValues')).every((p) => p.includes('refresh=1')), JSON.stringify(calls));

  // 選択肢を取る前の版で保存された辞書 (options が無い) は 1 回だけ取り直す
  const oldPayload = listing.getCachedGenreAttributes(db, '900001').attributes.map(({ options, id, ...rest }) => rest);
  db.prepare(`UPDATE ph_genre_attributes SET payload_json = ? WHERE genre_id = '900001'`).run(JSON.stringify(oldPayload));
  const migrated = await listing.fetchGenreAttributes(db, '900001', { fetcher: makeFetcher(rakutenLike(['ホワイト', 'ブラック']), []) });
  check('genre: 旧形式のキャッシュは取り直して選択肢が入る', migrated.cached === false && colorOf(migrated).options.length === 2);

  check('genre: pickDictionaryValues は形が違えば null・件数は整形前で数える',
    listing.pickDictionaryValues({ genre: { attributes: [{ id: 8, dictionaryValues: [{ id: 1, nameJa: '' }, { id: 2, nameJa: '白' }] }] } }, 8).rawCount === 2
    // 属性 ID が違う一覧は、1 件だけでも採用しない (Codex R2)
    && listing.pickDictionaryValues({ genre: { attributes: [{ id: 99, dictionaryValues: [{ id: 1, nameJa: '白' }] }] } }, 8) === null
    && listing.pickDictionaryValues({ genre: { attributes: [{ dictionaryValues: [{ id: 1, nameJa: '白' }] }] } }, 8) === null
    && listing.pickDictionaryValues(null, 8) === null && listing.pickDictionaryValues({ genre: { attributes: [{ id: 8 }] } }, 8) === null
    && listing.pickDictionaryValues({ genre: { attributes: [{ id: 1, dictionaryValues: [] }, { id: 2, dictionaryValues: [] }] } }, 8) === null);
}

db.prepare(`DELETE FROM product_drafts WHERE id = ?`).run(gdId);
db.prepare(`DELETE FROM ph_genre_attributes WHERE genre_id = '900001'`).run();

// ─── ジャンルツリー (IchibaGenre/Search) の正規化 / パス組み立て ───
const ichiba = await import('../lib/ichiba-genre.js');
// 商品ページからジャンルIDを読む処理は別モジュール (取得は miniPC 側で動く)
const itemPage = await import('../../../lib/rakuten-item-page.js');
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
// ─── 利益の配送費を配送方法で試算し直す (2026-09-04 中原さん報告) ──────────
// 「配送方法を変えても利益額が変わらない」= 配送費が NE の送料の固定値だったため。
// 選択肢は **NE の配送方法** で持つ (楽天のグループ8種は粒度が粗く、
// 「宅急便50サイズ以下」は NE の 50〜120 サイズ 479〜945円 のどれにもなって一意に決まらない)
{
  const vari = await import('../lib/variation.js');
  db.prepare('DELETE FROM mirror_products WHERE product_id BETWEEN 99500 AND 99599').run();
  const ins = db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 原価, 送料, 配送方法, 消費税率, updated_at)
    VALUES (?, ?, 'x', '1', ?, 'ok', 100, ?, ?, 0.1, '2026-09-04T00:00:00Z')`);
  // ネコポス=237 が最多 / 宅急便60サイズは 538 が3件・544 が1件 (最頻値=538) / 取扱中でないものは除く
  ins.run(99500, 'sp-1', '取扱中', 237, 'ネコポス');
  ins.run(99501, 'sp-2', '取扱中', 237, 'ネコポス');
  ins.run(99502, 'sp-3', '取扱中', 538, '宅急便60サイズ');
  ins.run(99503, 'sp-4', '取扱中', 538, '宅急便60サイズ');
  ins.run(99504, 'sp-5', '取扱中', 538, '宅急便60サイズ');
  ins.run(99505, 'sp-6', '取扱中', 544, '宅急便60サイズ');
  ins.run(99506, 'sp-7', '取扱終了', 999, 'まぼろし便');
  const opts = vari.listNeShippingOptions(db);
  const byMethod = Object.fromEntries(opts.map((o) => [o.method, o]));
  check('配送費の試算: 配送方法ごとの代表送料を出す (端数違いは最頻値)',
    byMethod['ネコポス']?.cost === 237 && byMethod['宅急便60サイズ']?.cost === 538,
    JSON.stringify(opts));
  check('配送費の試算: 取扱中でない商品の配送方法は選択肢に出さない',
    !byMethod['まぼろし便'], JSON.stringify(opts.map((o) => o.method)));
  check('配送費の試算: 送料の安い順に並ぶ (2026-09-04 中原さん要望: 金額がバラバラだと探しにくい)',
    opts[0].method === 'ネコポス' && opts[0].cost === 237
    && opts[1].method === '宅急便60サイズ' && opts[1].cost === 538
    && byMethod['宅急便60サイズ'].count === 4,
    JSON.stringify(opts));
  {
    // 🚨 開いただけで利益額が変わってはいけない (Codex R1 P1)。
    // この商品の実送料が 544 で、同じ配送方法の代表値 (最頻値) が 538 でも、初期表示は 544
    const choices = vari.profitShipChoices(opts, '宅急便60サイズ', 544);
    const cur = choices.find((c) => c.isCurrent);
    check('配送費の試算: いま使っている配送方法は代表値でなく実際の NE 送料を使う',
      cur?.method === '宅急便60サイズ' && cur.cost === 544
      && choices.find((c) => c.method === 'ネコポス').cost === 237,
      JSON.stringify(choices));
    // 一覧に無い配送方法 (その商品しか使っていない) でも、実送料で選べるようにする
    const solo = vari.profitShipChoices(opts, 'この商品だけの便', 1234);
    check('配送費の試算: 一覧に無い配送方法も実送料で先頭に出す',
      solo[0].method === 'この商品だけの便' && solo[0].cost === 1234 && solo[0].isCurrent === true,
      JSON.stringify(solo.slice(0, 2)));
    // 送料が分からない商品は、その行を出さない (0円扱いで利益を過大に見せない)
    const noCost = vari.profitShipChoices(opts, 'この商品だけの便', null);
    check('配送費の試算: 送料が不明な配送方法は選択肢に出さない',
      !noCost.some((c) => c.method === 'この商品だけの便'), JSON.stringify(noCost.map((c) => c.method)));
    check('配送費の試算: NE の配送方法が未設定でも他の選択肢は出る',
      vari.profitShipChoices(opts, null, null).length === opts.length);
    // 🚨 配送方法名が未設定でも送料があれば、その実値の行を先頭に置く。置かないと画面が
    // 先頭候補を初期選択して、開いただけで利益額が代表送料に変わる (Codex R3 P1)。
    // バリエーションで「送料は全SKU一致・配送方法名だけ割れている」ときに起きる
    const noMethod = vari.profitShipChoices(opts, null, 544);
    check('配送費の試算: 配送方法名が無くても送料があれば実値の行を先頭に置く',
      noMethod[0].isCurrent === true && noMethod[0].cost === 544
      && noMethod.length === opts.length + 1, JSON.stringify(noMethod.slice(0, 2)));
    check('配送費の試算: 実値の行はちょうど1つ (初期選択が決まる)',
      vari.profitShipChoices(opts, 'ネコポス', 237).filter((c) => c.isCurrent).length === 1
      && noMethod.filter((c) => c.isCurrent).length === 1);
  }
  {
    // 同数のときは高い方を代表にする (利益を実際より良く見せない側)
    db.prepare('DELETE FROM mirror_products WHERE product_id BETWEEN 99600 AND 99699').run();
    const ins2 = db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 原価, 送料, 配送方法, 消費税率, updated_at)
      VALUES (?, ?, 'x', '1', '取扱中', 'ok', 100, ?, ?, 0.1, '2026-09-04T00:00:00Z')`);
    ins2.run(99600, 'tie-1', 400, '同数便');
    ins2.run(99601, 'tie-2', 500, '同数便');
    // 前後に空白のある表記は同じ配送方法として数える (SQL 側で TRIM して集計 — Codex R1 P2)
    ins2.run(99602, 'tie-3', 237, ' ネコポス ');
    ins2.run(99603, 'tie-4', 237, 'ネコポス');
    const o2 = Object.fromEntries(vari.listNeShippingOptions(db).map((o) => [o.method, o]));
    check('配送費の試算: 代表送料が同数なら高い方を採る', o2['同数便']?.cost === 500, JSON.stringify(o2['同数便']));
    // 送料が同額のものは名前順。**挿入順と逆**に入れて、並べ替えが効いていることを確かめる
    // (実データを同じ比較関数で並べ直して比べるだけだと、何を入れても通ってしまう — Codex R1 P2)
    ins2.run(99604, 'tie-5', 777, 'い便');
    ins2.run(99605, 'tie-6', 777, 'あ便');
    const same = vari.listNeShippingOptions(db).filter((o) => o.cost === 777).map((o) => o.method);
    check('配送費の試算: 送料が同額なら配送方法名で並ぶ (入れた順に引きずられない)',
      JSON.stringify(same) === JSON.stringify(['あ便', 'い便']), JSON.stringify(same));
    // 前段の 2 件 + ここの ' ネコポス ' と 'ネコポス' = 4 件が 1 つに合算される
    check('配送費の試算: 配送方法名の前後の空白は同じものとして数える',
      o2['ネコポス']?.count === 4 && o2[' ネコポス '] === undefined, JSON.stringify(o2['ネコポス']));
    db.prepare('DELETE FROM mirror_products WHERE product_id BETWEEN 99600 AND 99699').run();
  }
  check('配送費の試算: 楽天グループ→NE配送方法の目安 (候補を上に集めるためだけ)',
    vari.RAKUTEN_GROUP_NE_HINTS['8'].includes('宅急便')
    && vari.RAKUTEN_GROUP_NE_HINTS['5'].includes('ネコポス')
    && vari.RAKUTEN_GROUP_NE_HINTS['1'].includes('定形'));
  db.prepare('DELETE FROM mirror_products WHERE product_id BETWEEN 99500 AND 99599').run();
}
// ─── 他社の商品ページURL → ジャンルID (2026-09-04) ────────────────────────
// 🚨 商品検索API では引けない (itemCode は `<店舗>:<数字>` の内部IDで、URL 末尾の
// 商品管理番号とは別物 — 2026-09-04 実測)。公開ページの埋め込み JSON から読む
check('genre-from-url: 楽天の商品ページURLだけ受け付け、クエリを落として正規化する',
  itemPage.parseRakutenItemUrl('https://item.rakuten.co.jp/ideshokai/pui016/')?.url === 'https://item.rakuten.co.jp/ideshokai/pui016/'
  && itemPage.parseRakutenItemUrl('https://item.rakuten.co.jp/ideshokai/pui016/?rafcid=wsc_i_is_123')?.url === 'https://item.rakuten.co.jp/ideshokai/pui016/'
  && itemPage.parseRakutenItemUrl('https://item.rakuten.co.jp/ideshokai/pui016')?.itemCode === 'pui016'
  && itemPage.parseRakutenItemUrl('https://item.rakuten.co.jp/ideshokai/pui016/#tab')?.shopCode === 'ideshokai');
{
  // 🚨 利用者が貼った URL を取りに行くので SSRF を作らない。item.rakuten.co.jp 以外は全部拒否
  const rejected = [
    'http://item.rakuten.co.jp/shop/item/',            // https 以外
    'https://item.rakuten.co.jp.evil.example/shop/it/', // ホストの後方一致すり抜け
    'https://evil.example/shop/item/',
    'https://item.rakuten.co.jp:8080/shop/item/',       // 別ポート
    'https://user:pw@item.rakuten.co.jp/shop/item/',    // 認証情報つき
    'https://127.0.0.1/shop/item/',
    'http://169.254.169.254/latest/meta-data/',         // クラウドのメタデータ
    'https://item.rakuten.co.jp/shop/',                 // 商品部分が無い
    'https://item.rakuten.co.jp/shop/item/extra/path',  // 余分なパス (黙って別URLにしない)
    'https://item.rakuten.co.jp/../etc/passwd/',
    'https://a.r10.to/xxxxx',                           // 短縮URL (転送先を保証できない)
    'file:///etc/passwd', 'javascript:alert(1)', '', null, undefined,
  ];
  const leaked = rejected.filter((u) => itemPage.parseRakutenItemUrl(u) !== null);
  check('genre-from-url: 楽天の商品ページ以外の URL は全部拒否する (SSRF を作らない)',
    leaked.length === 0, JSON.stringify(leaked));
}
check('genre-from-url: 壊れた percent encoding は例外にせず不正URL扱い (Codex R1 low)',
  itemPage.parseRakutenItemUrl('https://item.rakuten.co.jp/shop/%/') === null
  && itemPage.parseRakutenItemUrl('https://item.rakuten.co.jp/%E4%B8%8D%E6%AD%A3/%/') === null);
check('genre-from-url: 埋め込み JSON の genreId を一意に取り出す',
  itemPage.extractGenreIdFromHtml('x &quot;data&quot;: { &quot;genreId&quot;: 568908, &quot;price&quot;: } y') === '568908');
check('genre-from-url: 緩い一致で拾えるノイズ (全ページに出る 100005) は混ぜない',
  itemPage.extractGenreIdFromHtml('genre_id=100005 ... &quot;genreId&quot;: 215261,') === '215261');
check('genre-from-url: 一意に決まらなければ null (誤ったジャンルを返さない)',
  itemPage.extractGenreIdFromHtml('&quot;genreId&quot;: 111, &quot;genreId&quot;: 222') === null
  && itemPage.extractGenreIdFromHtml('ジャンルの手がかりが無いページ') === null
  && itemPage.extractGenreIdFromHtml('') === null);
{
  // 楽天の商品ページは EUC-JP。ヘッダ/meta の charset に従い、不明なら EUC-JP で読む
  const eucBytes = Buffer.from([0xa4, 0xb3, 0xa4, 0xf3]); // 「こん」(EUC-JP)
  check('genre-from-url: charset 指定が無ければ EUC-JP として読む',
    itemPage.decodeItemPage(eucBytes, null) === 'こん', JSON.stringify(itemPage.decodeItemPage(eucBytes, null)));
  check('genre-from-url: charset が UTF-8 ならそれに従う',
    itemPage.decodeItemPage(Buffer.from('こん', 'utf8'), 'text/html; charset=UTF-8') === 'こん');
}
{
  // URL が不正なら miniPC を呼ばずに断る (往復を無駄にしない・ネットワークに出ない)
  let calls = 0;
  const countingFetcher = async () => { calls += 1; return { status: 200, data: {} }; };
  const bad = await listing.genreIdFromItemUrl('https://evil.example/x/y/', { fetcher: countingFetcher });
  check('genre-from-url: 不正な URL は取りに行かずに断る',
    bad.ok === false && calls === 0 && /item\.rakuten\.co\.jp/.test(bad.error), JSON.stringify({ bad, calls }));
}
{
  // miniPC の口を差し替えて、実際に通して確かめる (ソース検査だけだと受け渡しを守れない)
  let seen = null;
  const okFetcher = async (p, opts) => {
    seen = { path: p, body: opts?.body, method: opts?.method };
    return { status: 200, data: { ok: true, genreId: 507768, shopCode: 'x', itemCode: 'y' } };
  };
  const r = await listing.genreIdFromItemUrl('https://item.rakuten.co.jp/fujiwarayohojo/b0czr58swk/?rafcid=zz', { fetcher: okFetcher });
  check('genre-from-url: miniPC の口へ、クエリを落とした URL を POST する',
    seen?.path === '/service-api/rakuten-rms/item-page/genre' && seen.method === 'POST'
    && seen.body?.url === 'https://item.rakuten.co.jp/fujiwarayohojo/b0czr58swk/', JSON.stringify(seen));
  check('genre-from-url: 応答の genreId を文字列で返す (画面と DB は文字列で扱う)',
    r.ok === true && r.genreId === '507768' && typeof r.genreId === 'string'
    && r.shopCode === 'fujiwarayohojo' && r.itemCode === 'b0czr58swk', JSON.stringify(r));
  // miniPC は読めなかった理由を 400 + message で返す。その文言をそのまま画面に出す
  const ng = await listing.genreIdFromItemUrl('https://item.rakuten.co.jp/a/b/', {
    fetcher: async () => ({ status: 400, data: { ok: false, error: 'ITEM_PAGE_GENRE_NOT_FOUND', message: 'このページからは読み取れませんでした' } }),
  });
  check('genre-from-url: miniPC が返した理由をそのまま画面へ出す',
    ng.ok === false && ng.error === 'このページからは読み取れませんでした', JSON.stringify(ng));
  // 本文が取れないとき (CF が差し替えた等) でも、状況が分かる文言にする
  const noBody = await listing.genreIdFromItemUrl('https://item.rakuten.co.jp/a/b/', {
    fetcher: async () => ({ status: 502, data: null }),
  });
  check('genre-from-url: 理由が取れないときは HTTP を添えて返す',
    noBody.ok === false && /HTTP 502/.test(noBody.error), JSON.stringify(noBody));
  // 通信そのものが落ちたときも画面を壊さない
  const boom = await listing.genreIdFromItemUrl('https://item.rakuten.co.jp/a/b/', {
    fetcher: async () => { throw new Error('fetch failed'); },
  });
  check('genre-from-url: 通信エラーでも例外にせずメッセージで返す',
    boom.ok === false && /fetch failed/.test(boom.error), JSON.stringify(boom));
}
{
  // 🚨 取得は **miniPC 経由**にした (2026-09-04)。Render (海外IP) から楽天の公開ページを
  // 取ると読み取れなかったため。miniPC = 日本のIP からは同じ実装で必ず取れる。
  // ここでは miniPC の口を差し替えて、呼び先と受け渡しを確かめる
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'rakuten-listing.js'), 'utf8');
  const fn = src.slice(src.indexOf('export async function genreIdFromItemUrl'));
  check('genre-from-url: 取得は miniPC の口を呼ぶ (Render から直接取りに行かない)',
    fn.includes("'/service-api/rakuten-rms/item-page/genre'") && !/\bfetch\(/.test(fn.slice(0, 1400)));
  check('genre-from-url: miniPC へは組み直した URL を渡す (クエリを持ち込まない)',
    /body: \{ url: parsed\.url \}/.test(fn));
  // miniPC 側の口が、共用モジュールの処理をそのまま使っていること (検証を二重に書かない)
  const svc = fs.readFileSync(path.join(__dirname, '..', '..', 'warehouse', 'rakuten-rms-service.js'), 'utf8');
  check('genre-from-url: miniPC 側は共用モジュールで取得する (検証を書き直さない)',
    svc.includes("from '../../lib/rakuten-item-page.js'")
    && svc.includes("router.post('/item-page/genre'"));
  // 🚨 待ち行列を二重に持たない (Codex R1/R2):
  //    RMS API のセマフォに載せると業務側の処理を止める。かといって専用セマフォを重ねると、
  //    そちらの待ちは無制限で最悪待ち時間が読めず、呼び出し側のタイムアウトを超える。
  //    待ち行列は lib/rakuten-item-page.js の1箇所だけにする
  check('genre-from-url: miniPC の口にセマフォを重ねない (待ち行列は1箇所)',
    /router\.post\('\/item-page\/genre', async \(req, res\)/.test(svc)
    && !/item-page\/genre', rateLimitMiddleware/.test(svc));
  const lib = fs.readFileSync(path.join(__dirname, '..', '..', '..', 'lib', 'rakuten-item-page.js'), 'utf8');
  check('genre-from-url: 最悪待ち時間が呼び出し側のタイムアウトに収まる',
    /MAX_PAGE_QUEUE = 2;/.test(lib) && /PAGE_MIN_GAP_MS = 2000;/.test(lib)
    && /AbortSignal\.timeout\(15_000\)/.test(lib)   // 2*(15+2)=34s < 40s
    && /timeoutMs: 40_000/.test(fn));
  check('genre-from-url: miniPC は読めなかった理由を 400 で返す (CF が本文を差し替えない)',
    /ITEM_PAGE_GENRE_NOT_FOUND[\s\S]{0,200}status: 400|status: 400[\s\S]{0,200}ITEM_PAGE_GENRE_NOT_FOUND/.test(svc));
}

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
  });
  check('page-info html: table + エスケープ + 改行→<br>',
    html.startsWith('<table') && html.includes('テスト&lt;商品&gt; &amp; &quot;A&quot;') && html.includes('水<br>グリセリン'),
    html.slice(0, 200));
  check('page-info html: 製造国は「海外製（フランス）」形式', html.includes('海外製（フランス）'));
  check('page-info html: 発売元 + 輸入者の両記載 (楽天ルール: 輸入品)', html.includes('メーカーA<br>輸入者: 輸入者B'));
  check('page-info html: 広告文責/注意事項が載る',
    html.includes(pinfo.adResponsibility()) && html.includes(pinfo.FIXED_NOTES));
  check('page-info html: 発送方法の行は出さない (2026-08-31 中原さん: 表には不要)',
    !html.includes('発送方法') && !html.includes('ネコポス'), html.slice(0, 300));
  check('page-info html: 空欄の行は出さない (サイズ/使用上の注意なし)',
    !html.includes('サイズ') && !html.includes('使用上の注意'));
  const foodHtml = pinfo.buildPageInfoHtml({
    productName: 'アマニ', info: {
      product_type: 'food', food_name: '有機亜麻仁シード', food_ingredients: '有機アマニ',
      food_expiry: 'ラベルに記載', food_storage: '常温',
    },
  });
  check('page-info html: 食品は 名称/原材料名/賞味期限/保存方法',
    ['名称', '原材料名', '賞味期限', '保存方法'].every((k) => foodHtml.includes(k))
    && !foodHtml.includes('発送方法'));
  check('page-info html: 全空でも固定行 (商品名/広告文責/注意事項) だけの表になる',
    pinfo.buildPageInfoHtml({ productName: 'X', info: null }).includes('広告文責'));
  check('page-info html: 商品名すら無ければ固定行のみ (空文字にはならない)',
    pinfo.buildPageInfoHtml({ productName: '', info: null }).includes('注意事項'));
  // ブランド名・容量 (ml/g) — 2026-08-28 中原さん要望
  {
    const bh = pinfo.buildPageInfoHtml({
      productName: 'X',
      info: { product_type: 'general', brand_name: 'B-Faith<x>', content_volume: '200g' },
    });
    check('page-info html: ブランド名の行が載る (エスケープあり)',
      bh.includes('<b>ブランド名</b>') && bh.includes('B-Faith&lt;x&gt;'), bh.slice(0, 300));
    check('page-info html: ブランド名は商品名の次・サイズより前',
      bh.indexOf('商品名') < bh.indexOf('ブランド名') && bh.indexOf('ブランド名') < bh.indexOf('内容量'));
    check('page-info html: 雑貨でも容量 (ml/g) が内容量として載る', bh.includes('<b>内容量</b>') && bh.includes('200g'));
    check('page-info html: ブランド名が空なら行を出さない',
      !pinfo.buildPageInfoHtml({ productName: 'X', info: { product_type: 'general' } })
        .includes('ブランド名'));
  }
  // その他注意事項 (2026-09-13 スタッフ要望)。「箱から出して配送します」など商品ごとのお知らせを表の行で出す
  {
    const oh = pinfo.buildPageInfoHtml({
      productName: 'X',
      info: { product_type: 'general', usage_notes: '直射日光を避ける', other_notes: '箱から出して<配送>します\n2行目' },
    });
    check('page-info html: その他注意事項の行が載る (エスケープ・改行→<br>)',
      oh.includes('<b>その他注意事項</b>') && oh.includes('箱から出して&lt;配送&gt;します<br>2行目'), oh.slice(0, 400));
    check('page-info html: その他注意事項は 使用上の注意 の直後',
      oh.indexOf('使用上の注意') < oh.indexOf('その他注意事項') && oh.indexOf('その他注意事項') < oh.indexOf('広告文責'));
    check('page-info html: その他注意事項が空なら行を出さない',
      !pinfo.buildPageInfoHtml({ productName: 'X', info: { product_type: 'general' } }).includes('その他注意事項'));
  }
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
  // 2026-08-31: 画像の工程は商品詳細 (LP) の 10 段階に一本化。TOP の 4 工程は廃止した
  // (LP と TOP は同時進行で作るので、工程を分けて管理する必要性が低い — 中原さん)
  check('工程シード: 本流6 + 画像10 (詳細 v2 のみ。TOP の 4 工程は廃止)',
    steps0.filter((s) => s.track === 'main').length === 6 && steps0.filter((s) => s.track === 'image').length === 10,
    `main=${steps0.filter((s) => s.track === 'main').length} image=${steps0.filter((s) => s.track === 'image').length}`);
  check('工程シード: TOP の工程は 1 つも有効になっていない',
    steps0.filter((s) => s.track === 'image' && s.image_kind !== 'detail').length === 0,
    steps0.filter((s) => s.track === 'image' && s.image_kind !== 'detail').map((s) => s.code).join(','));
  {
    const expectedImgOrder = {
      detail: 'imgd_request,imgd_compose,imgd_material,imgd_ai,imgd_design,imgd_review_1,imgd_review_2,imgd_amazon,imgd_rakuten,imgd_aplus',
    };
    for (const kind of ['detail']) {
      const img = steps0.filter((s) => s.track === 'image' && s.image_kind === kind).map((s) => s.code);
      check(`工程シード: ${kind} の段階の並び (詳細は v2 ①〜⑨)`,
        img.join(',') === expectedImgOrder[kind], img.join(','));
    }
    check('工程シード: 社内確認 (中原) の担当は画像作成承認者',
      steps0.find((s) => s.code === 'imgd_review_2').role_code === 'image_approver');
    check('工程シード: image_stage が入る (ボードの列まとめの安定キー。⑥の2工程は同じ review)',
      steps0.find((s) => s.code === 'imgd_request').image_stage === 'request'
      && steps0.find((s) => s.code === 'imgd_review_1').image_stage === 'review'
      && steps0.find((s) => s.code === 'imgd_review_2').image_stage === 'review');
    check('工程シード: skippable / listing_gate (①〜⑥ は対象外不可・⑦⑧⑨ は出品ゲート外)',
      steps0.find((s) => s.code === 'imgd_request').skippable === 0 && steps0.find((s) => s.code === 'imgd_request').listing_gate === 1
      && steps0.find((s) => s.code === 'imgd_amazon').skippable === 1 && steps0.find((s) => s.code === 'imgd_amazon').listing_gate === 0
      && steps0.find((s) => s.code === 'imgd_rakuten').listing_gate === 0 && steps0.find((s) => s.code === 'imgd_aplus').listing_gate === 0);
    check('工程シード: 旧詳細 v1 工程はシードされない (新規DB)', !steps0.some((s) => dbmod.LEGACY_DETAIL_V1_CODES.includes(s.code)));
    check('工程シード: 旧一本トラック工程はシードされない', !steps0.some((s) => ['img_request', 'img_production', 'img_register', 'img_approve'].includes(s.code)));
    // 2026-08-31: TOP の 4 工程は廃止。新規 DB ではシードされず、既存 DB では active=0 になる
    check('工程シード: 廃止した TOP 工程はシードされない',
      !steps0.some((s) => dbmod.RETIRED_TOP_STEP_CODES.includes(s.code)),
      steps0.filter((s) => dbmod.RETIRED_TOP_STEP_CODES.includes(s.code)).map((s) => s.code).join(','));
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
  check('既定担当が工程に出る', wf.listSteps().find((s) => s.code === 'imgd_request').default_staff_name === '大川さん');
  check('工程に紐づく人数が出る', wf.listSteps().find((s) => s.code === 'imgd_request').member_count === 2);
  // 既定は役割ごとに 1 人 = 後から立てた方に付け替わる (UNIQUE 違反でエラーにしない)
  wf.setStaffRoles(tanaka, [{ code: 'registrar', isDefault: true }, { code: 'image', isDefault: true }]);
  check('既定は役割ごとに1人 (前任は外れる)',
    wf.getStaff(okawa).roles.find((r) => r.code === 'image').isDefault === false);
  check('付け替え後の既定が反映される', wf.listSteps().find((s) => s.code === 'imgd_request').default_staff_name === '田中美祐');

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
  // TOP画像の工程は 2026-08-31 に廃止 (LP に一本化)。作ろうとすると 400
  let topKindErr = null;
  try { wf.createStep({ label: '撮影', track: 'image', image_kind: 'top', role_code: 'image' }); } catch (e) { topKindErr = e; }
  check('工程追加: TOP画像の工程は作れない (廃止済み)', topKindErr?.status === 400, topKindErr?.message || '例外が出ていない');
  const extra = wf.createStep({ label: '撮影', track: 'image', image_kind: 'detail', role_code: 'image' });
  check('工程を追加できる (種別つき)', wf.listSteps({ track: 'image' }).find((s) => s.code === extra)?.image_kind === 'detail');
  check('追加した工程は無効化できる', wf.updateStep(extra, { active: false }) === true);
  // 廃止した TOP 工程は元に戻せない (戻すとボードに TOP 列・カードが復活する)。
  // 旧版の管理画面から足せた**カスタム TOP 工程**も同じ扱いにする (Codex R3 high)。
  // 新規 DB には TOP 工程が無いので、直 SQL で「旧 DB に残っている状態」を作って検証する
  {
    db.prepare(`INSERT INTO ph_steps (code, label, track, image_kind, image_stage, role_code, sort, builtin, active)
      VALUES ('step_legacy_top', '旧カスタムTOP', 'image', 'top', 'request', 'image', 999, 0, 1)`).run();
    db.prepare(`INSERT INTO ph_steps (code, label, track, image_kind, image_stage, role_code, sort, builtin, active)
      VALUES ('step_legacy_nokind', '旧カスタム画像 (種別なし)', 'image', NULL, NULL, 'image', 998, 0, 1)`).run();
    const activeOf = (code) => db.prepare('SELECT active FROM ph_steps WHERE code = ?').get(code)?.active;
    // 既存 DB に残った「TOP側工程も自動完了」の説明も、この起動処理で直す (シードは
    // INSERT OR IGNORE なので説明だけ古いまま残り、もう起きない動作を案内してしまう — Codex R5 low)
    db.prepare(`UPDATE ph_steps SET description = '⑤ AI 画像を修正 + TOP画像制作 (完了で TOP 側の 依頼/制作/登録 も自動で済みになる)' WHERE code = 'imgd_design'`).run();
    check('起動時: 詳細以外の画像工程 (カスタム TOP・種別なし) は無効化される',
      dbmod.retireTopImageSteps(db) >= 2 && activeOf('step_legacy_top') === 0 && activeOf('step_legacy_nokind') === 0,
      `top=${activeOf('step_legacy_top')} nokind=${activeOf('step_legacy_nokind')}`);
    check('起動時: 古い工程説明 (TOP側も自動完了) を直す',
      !/TOP 側の/.test(db.prepare(`SELECT description FROM ph_steps WHERE code = 'imgd_design'`).get()?.description || ''),
      db.prepare(`SELECT description FROM ph_steps WHERE code = 'imgd_design'`).get()?.description);
    // 管理画面で書き換えた独自の説明は消さない (Codex R6 low: 旧文言に一致するときだけ置換)
    db.prepare(`UPDATE ph_steps SET description = '現場で書き換えた説明' WHERE code = 'imgd_design'`).run();
    dbmod.retireTopImageSteps(db);
    check('起動時: 管理画面で書き換えた説明は上書きしない',
      db.prepare(`SELECT description FROM ph_steps WHERE code = 'imgd_design'`).get()?.description === '現場で書き換えた説明');
    for (const code of ['step_legacy_top', 'step_legacy_nokind']) {
      let revive = null;
      try { wf.updateStep(code, { active: true }); } catch (e) { revive = e; }
      check(`廃止した画像工程は再有効化できない (${code})`,
        revive?.status === 400 && /TOP画像の工程/.test(revive.message || '')
        && activeOf(code) === 0, revive?.message || '例外が出ていない');
    }
    db.prepare(`DELETE FROM ph_steps WHERE code IN ('step_legacy_top', 'step_legacy_nokind')`).run();
  }
  check('無効化した工程は既定の一覧に出ない', !wf.listSteps().some((s) => s.code === extra));

  // 画像トラックの組み込み工程のラベル・滞留日数は管理画面から変えられる。
  // 2026-08-31 に TOP/詳細 の対を廃止したので「対へ伝播する」検査は無くなり、
  // 更新した工程そのものが変わることを見る (Codex R7 low: 対が無い状態では恒真だった)
  wf.updateStep('imgd_design', { label: 'デザイン修正 (改)', stall_days: 9 });
  {
    const edited = wf.listSteps({ includeInactive: true }).find((s) => s.code === 'imgd_design');
    check('工程のラベル・滞留日数を変えられる',
      edited?.label === 'デザイン修正 (改)' && edited?.stall_days === 9,
      `${edited?.label} / ${edited?.stall_days}`);
  }
  wf.updateStep('imgd_design', { label: 'デザイン修正', stall_days: 7 });
  check('工程のラベルを戻せる',
    wf.listSteps({ includeInactive: true }).find((s) => s.code === 'imgd_design')?.label === 'デザイン修正');
  // 担当ロールも工程ごとに変えられる
  wf.updateStep('imgd_review_2', { role_code: 'approver' });
  check('工程の担当ロールを変えられる',
    wf.listSteps({ includeInactive: true }).find((s) => s.code === 'imgd_review_2')?.role_code === 'approver');
  wf.updateStep('imgd_review_2', { role_code: 'image_approver' });

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
  check('overview は本流・セット・画像に分かれる', ov.main.length === 6 && ov.image.length === 10 && ov.set.length === 5,
    `main=${ov.main.length} set=${ov.set.length} image=${ov.image.length}`);
  check('overview: セットの流れは構成決定から始まり、出品・展開は単品と共用',
    ov.set[0].code === 'set_compose' && ov.set[ov.set.length - 1].code === 'listing',
    ov.set.map((s) => s.code).join(','));
  check('overview: 単品の流れにセット工程を混ぜない', !ov.main.some((s) => s.track === 'set'));
}

// ─── ワークフロー: 商品 × 工程の進捗 (2026-08-23) ───
const wfp = await import('../lib/workflow-progress.js');
const backLink = await import('../lib/back-link.js');
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
  // 2026-08-31: TOP の工程は廃止したので imageTop は常に空。TOP の状態は画像の有無で見る
  check('初期化: TOP側の工程は無い (画像の有無で見る)', pl.imageTop.rows.length === 0);
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
  // TOP画像は工程でなく「画像が登録されているか」で見る (2026-08-31) ので、
  // ここでは画像を入れて**詳細 (LP) の工程が終わっていない**ことだけをゲートの理由にする
  db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'gate-img-1', 0)`).run(idGate);
  const blocked = listing.buildItemPayload(db, idGate);
  check('画像の工程が未完了なら出品を止める',
    (blocked.reasons || []).some((x) => /画像の工程が終わっていません/.test(x)), JSON.stringify(blocked.reasons || []).slice(0, 200));
  check('止める理由にいまの工程が出る',
    (blocked.reasons || []).some((x) => /商品詳細画像: 画像制作の依頼/.test(x)),
    JSON.stringify(blocked.reasons || []).slice(0, 300));
  let d1SkipErr = null;
  try { wfp.setStepState(idGate, 'imgd_request', { state: 'skip' }, 'admin', ADMIN); } catch (e) { d1SkipErr = e; }
  check('v2: 詳細 ①〜⑥ は対象外にできない (skippable=0)', d1SkipErr?.status === 400, d1SkipErr?.message || '例外が出ていない');
  check('v2: 詳細 ⑦ Amazon登録依頼 は admin なら対象外にできる',
    wfp.setStepState(idGate, 'imgd_amazon', { state: 'skip' }, 'admin', ADMIN).changed === true);
  wfp.setStepState(idGate, 'imgd_amazon', { state: 'todo' }, 'admin', ADMIN);
  // TOP画像は工程でなく**画像が登録されているか**で見る (2026-08-31 中原さん決定 A)。
  // サムネイル無しの楽天出品はありえないので、画像が 1 枚も無ければゲートは開かない
  {
    const imgs = db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ?').all(idGate);
    db.prepare('DELETE FROM draft_images WHERE draft_id = ?').run(idGate);
    check('画像が 1 枚も無ければゲートは開かない (TOP画像は楽天出品に必須)',
      /TOP画像 \(サムネイル\) が登録されていません/.test(wfp.imageTrackBlockReason(db, idGate) || ''),
      wfp.imageTrackBlockReason(db, idGate));
    // 枠1 (sort=0 = <商品コード>_top) が無ければ通さない (Codex R6 high):
    // _top を入れ忘れて _01 だけ取り込んだ商品が素通りすると、別の画像がサムネイルになる
    db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'gate-img-nz', 1)`).run(idGate);
    check('枠1 (_top) が無ければゲートは開かない (_01 だけでは通さない)',
      /TOP画像 \(サムネイル\) が登録されていません/.test(wfp.imageTrackBlockReason(db, idGate) || ''),
      wfp.imageTrackBlockReason(db, idGate));
    db.prepare(`DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id = 'gate-img-nz'`).run(idGate);
    const ins = db.prepare('INSERT INTO draft_images (draft_id, drive_file_id) VALUES (?, ?)');
    for (const i of imgs) ins.run(idGate, i.drive_file_id);
  }
  db.prepare(`
    UPDATE draft_step_progress SET state = 'todo'
    WHERE draft_id = ? AND step_code LIKE 'img_%_top'
  `).run(idGate);
  // 詳細側の工程 0 件は fail-closed (対象外にしていない限り — Codex R2 high)
  db.prepare(`UPDATE ph_steps SET active = 0 WHERE image_kind = 'detail'`).run();
  check('詳細工程が無い設定壊れでもゲートを閉じる (fail-closed)',
    /詳細画像.*見つかりません/.test(wfp.imageTrackBlockReason(db, idGate) || ''), wfp.imageTrackBlockReason(db, idGate));
  db.prepare(`UPDATE ph_steps SET active = 1 WHERE image_kind = 'detail' AND builtin = 1`).run();

  // 「詳細画像は対象外」(商品単位フラグ): 画像が登録されていればゲートが開く
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
  // 権限: admin か、詳細画像「依頼」工程の担当者本人か、その役割 (画像登録者) がある人。
  // 2026-09-13 から画像の工程は役割でも見るので、ここでは役割の無い人で「担当外 = 不可 / 担当 = 可」を見る
  // (大川さんは画像登録者の役割を持つので担当外でも切り替えられる。役割での判定は下のブロックで見る)
  const exOwnerId = wf.createStaff({ name: '依頼担当スモーク', kind: 'internal' });
  let exErr = null;
  try { wfp.setDetailImagesExcluded(idGate, true, 'owner', { isAdmin: false, actorStaffId: exOwnerId }); } catch (e) { exErr = e; }
  check('対象外の切り替えは 担当外で役割も無い人はできない', exErr?.status === 403, exErr?.message || '例外が出ていない');
  wfp.setStepState(idGate, 'imgd_request', { assignee_id: exOwnerId }, 'admin', ADMIN);
  check('詳細画像の依頼担当なら (役割が無くても) 対象外にできる',
    wfp.setDetailImagesExcluded(idGate, true, 'owner', { isAdmin: false, actorStaffId: exOwnerId }).changed === true);
  wfp.setDetailImagesExcluded(idGate, false, 'admin', ADMIN);
  // 後片付け: 担当を大川さんに戻してから、テスト用の担当者を消す (以降のテストは従来どおり大川さん担当の前提)
  wfp.setStepState(idGate, 'imgd_request', { assignee_id: wfOkawaId }, 'admin', ADMIN);
  db.prepare('DELETE FROM ph_staff WHERE id = ?').run(exOwnerId);

  // 画像の工程は担当者で縛らない (2026-09-13 スタッフ要望: 担当が付くと他の人がボードで動かせなかった)。
  // その工程の役割を持つ人なら担当に関わらず動かせる。役割の無い人は従来どおり。⑥-2 は承認者の役割が要る
  {
    const imgStaffId = wf.createStaff({ name: '画像係スモーク', kind: 'internal' });
    const noRoleId = wf.createStaff({ name: '役割なしスモーク', kind: 'internal' });
    db.prepare(`INSERT INTO ph_staff_roles (staff_id, role_code) VALUES (?, 'image')`).run(imgStaffId);
    const IMG = { isAdmin: false, actorStaffId: imgStaffId };
    const NOROLE = { isAdmin: false, actorStaffId: noRoleId };
    const idR = Number(db.prepare(
      "INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('IMG-ROLE', '画像の役割テスト', 'draft', 'smoke')",
    ).run().lastInsertRowid);
    wfp.ensureProgress(db, idR);
    const stR = (code) => db.prepare('SELECT state, assignee_id FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(idR, code) || {};
    wfp.setStepState(idR, 'imgd_request', { assignee_id: wfOkawaId }, 'admin', ADMIN);
    let e1 = null; let r1 = null;
    try { r1 = wfp.setStepState(idR, 'imgd_request', { state: 'done' }, 'img', IMG); } catch (e) { e1 = e; }
    check('画像の工程: 画像登録者の役割があれば、他人の担当でも動かせる', r1?.changed === true, e1?.message);
    let e2 = null;
    try { wfp.setStepState(idR, 'imgd_request', { state: 'todo' }, 'norole', NOROLE); } catch (e) { e2 = e; }
    check('画像の工程: 役割が無い人は他人の担当を動かせない (403)', e2?.status === 403, e2?.message || '例外が出ていない');
    let e2b = null;
    try { wfp.setStepState(idR, 'imgd_compose', { assignee_id: wfOkawaId }, 'img', IMG); } catch (e) { e2b = e; }
    check('画像の工程: 役割があっても他人への付け替えは管理者だけ (403)', e2b?.status === 403, e2b?.message || '例外が出ていない');
    const composeBefore = stR('imgd_compose').assignee_id ?? null;
    const materialBefore = stR('imgd_material').assignee_id ?? null;
    wfp.moveBoardCard(idR, { view: 'image', kind: 'detail', to: 'material', expectedCurrent: 'imgd_compose' }, 'img', IMG);
    check('画像の工程: ボードで動かしても担当を付けない (通過した ②仮構成・移動先の ③素材待ち)',
      stR('imgd_compose').state === 'done' && (stR('imgd_compose').assignee_id ?? null) === composeBefore
      && (stR('imgd_material').assignee_id ?? null) === materialBefore && stR('imgd_compose').assignee_id !== imgStaffId,
      JSON.stringify([stR('imgd_compose'), stR('imgd_material')]));
    for (const code of ['imgd_material', 'imgd_ai', 'imgd_design', 'imgd_review_1']) {
      wfp.setStepState(idR, code, { state: 'done' }, 'admin', ADMIN);
    }
    let e3 = null;
    try { wfp.setStepState(idR, 'imgd_review_2', { state: 'done' }, 'img', IMG); } catch (e) { e3 = e; }
    check('画像の工程: ⑥-2 中原確認は 画像登録者 の役割では進められない (承認者の役割が要る)',
      e3?.status === 403 && stR('imgd_review_2').state !== 'done', e3?.message || '例外が出ていない');
    let e4 = null;
    try { wfp.setDetailImagesExcluded(idR, true, 'norole', NOROLE); } catch (e) { e4 = e; }
    check('詳細画像は対象外: 役割が無い人は切り替えられない (403)', e4?.status === 403, e4?.message || '例外が出ていない');
    check('詳細画像は対象外: 画像登録者の役割があれば担当者でなくても切り替えられる',
      wfp.setDetailImagesExcluded(idR, true, 'img', IMG).changed === true);
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idR);
    db.prepare('DELETE FROM ph_staff_roles WHERE staff_id IN (?, ?)').run(imgStaffId, noRoleId);
    db.prepare('DELETE FROM ph_staff WHERE id IN (?, ?)').run(imgStaffId, noRoleId);
  }

  // カードの画像は _00 (白抜き) に統一 (2026-09-13 スタッフ要望)。白抜きがまだ無い商品は 枠1 (TOP)
  {
    const idT = Number(db.prepare(
      "INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('IMG-THUMB', 'カード画像テスト', 'draft', 'smoke')",
    ).run().lastInsertRowid);
    wfp.ensureProgress(db, idT);
    db.prepare("INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'thumb-top', 0), (?, 'thumb-01', 1)").run(idT, idT);
    const cardT = () => wfp.boardData(db, {}).columns.flatMap((c) => c.cards).find((x) => x.id === idT) || {};
    check('カードの画像: 白抜き (_00) が無ければ 枠1 (TOP)', cardT().card_image_id === 'thumb-top', JSON.stringify(cardT().card_image_id));
    db.prepare(`INSERT INTO draft_rakuten (draft_id, white_bg_drive_file_id, white_bg_modified_time)
                VALUES (?, 'thumb-00', '2026-09-13T00:00:00Z')`).run(idT);
    check('カードの画像: 白抜き (_00) があればそれを出す (更新日時もそちら)',
      cardT().card_image_id === 'thumb-00' && cardT().card_image_mtime === '2026-09-13T00:00:00Z', JSON.stringify(cardT().card_image_id));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idT);
  }

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
  const lateImg = wf.createStep({ label: '画像の最終チェック', track: 'image', image_kind: 'detail', role_code: 'image_approver' });
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
  // ⑤は判断を記録してからでないと閉じられない (2026-09-04)。ここでは「作らない」と決めて閉じる
  // (set-derive の import はこの下なので、記録そのものを直接入れる)
  db.prepare(`INSERT INTO draft_set_decisions (draft_id, decision, reason_code, decided_by) VALUES (?, 'none', 'single_enough', 'admin')`).run(id);
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
// セットの画像の引き継ぎ計画の語彙 (§4.7)
const sip = await import('../lib/set-image-plan.js');
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
  // 配送方法は**セットにコピーされない**ことを見るために、親には入れておく (2026-09-04 §4.4 決⑥)
  db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, article_number,
      shipping_method_group, postage_included, normal_delivery_date_id)
    VALUES (?, '565004', '[]', 'parent-model-1', '5', 1, '1000')`).run(parentId);
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
  // 売価は「コピー」ではなく**構成から計算した初期値**を入れる (2026-09-04 §4.4 決⑦)。
  // 親の 1,980 円をそのまま入れると 2 個セットが単品と同じ値段で出る
  check('売価は親の値をコピーせず「単品売価 × 個数」を初期値として入れる (1,980 × 2 = 3,960)',
    set1.price === 3960, String(set1.price));
  check('売価の初期値の由来をイベントに残す (人が「誰かが入れた値」に見えて触れなくならないように)',
    /1,980円 × 2 = 3,960円/.test(db.prepare(
      `SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'price_prefilled'`).get(r.draftId)?.detail || ''),
    db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'price_prefilled'`).get(r.draftId)?.detail || '(イベントが無い)');
  // 🚨 配送方法はコピーしない (決⑥)。セットは個数で重さも箱も変わる。
  //    空なら effectiveShippingForDraft が NE の配送方法へ落ちるので、本コード確定後に自動で正しくなる
  const setRk = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(r.draftId);
  check('配送方法は親からコピーしない (個数で変わるので NE 確定後に NE の値が入る)',
    setRk != null && setRk.shipping_method_group == null && setRk.postage_included == null
    && setRk.normal_delivery_date_id == null,
    JSON.stringify({ g: setRk?.shipping_method_group, p: setRk?.postage_included, d: setRk?.normal_delivery_date_id }));
  check('配送方法をコピーしていない理由をイベントに残す',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'shipping_not_copied'`).get(r.draftId).c === 1);
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
  // メーカー型番の入口を article_number に統一したので、ここを抜かすと派生したセットから型番が消える
  // (2026-08-31 / Codex R1 high)
  check('メーカー型番 (article_number) もコピーする',
    db.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(r.draftId)?.article_number === 'parent-model-1',
    String(db.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(r.draftId)?.article_number));
  check('画像はコピーしない',
    db.prepare('SELECT COUNT(*) AS c FROM draft_images WHERE draft_id = ?').get(r.draftId).c === 0);
  check('AIモードでは説明文をコピーしない',
    db.prepare('SELECT COUNT(*) AS c FROM draft_ai_outputs WHERE draft_id = ?').get(r.draftId).c === 0);

  // 工程の開始位置
  const sp = wfp.progressOf(r.draftId, { db });
  // セットはセット専用の工程を持つ (2026-09-04)。単品の基本情報・AI待ちは無く、構成決定から始まる
  check('セットはセット工程の「構成決定」から始まる (単品の①②は持たない)',
    sp.current?.step_code === 'set_compose'
    && sp.main.every((x) => x.step_code === 'set_compose' || x.step_code === 'set_ne_register'
      || x.step_code === 'set_content' || x.step_code === 'set_prep' || x.step_code === 'listing'),
    JSON.stringify(sp.main.map((x) => x.step_code)));
  check('セットの画像トラックは未着手', sp.image.every((s) => s.state === 'todo'));
  check('構成が保存される',
    db.prepare('SELECT qty FROM draft_set_members WHERE set_draft_id = ?').get(r.draftId).qty === 2);
  check('親のセット検討工程が完了する',
    wfp.progressOf(parentId, { db }).main.find((s) => s.step_code === 'set_review').state === 'done');
  check('親のイベントに記録が残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'set_draft_created'`).get(parentId).c === 1);

  // ─── 売価の初期値 (2026-09-04 §4.4 決⑦) ──────────────────────────────
  // 「単品売価 × 個数」の和。単価は ①アプリのドラフト ②NE mirror の標準売価 の順で引く。
  // 🚨 1 件でも引けなければ **合計を作らない** — 欠けた合計は「2個セットが1個ぶんの値段」になる
  {
    const sp = await import('../lib/set-price.js');
    db.prepare(`INSERT OR REPLACE INTO mirror_products
      (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 標準売価, 原価, 送料, 配送方法, 消費税率, updated_at)
      VALUES (99710, 'setprice-ne', 'NEにだけある商品', '1', '取扱中', 'ok', 500, 200, 120, '定形外', 0.1, '2026-09-04T00:00:00Z')`).run();
    check('売価の初期値: アプリのドラフトの売価を単価に使う',
      sp.unitPriceOf(db, 'WF-SET-P')?.value === 1980 && sp.unitPriceOf(db, 'WF-SET-P')?.source === 'draft',
      JSON.stringify(sp.unitPriceOf(db, 'WF-SET-P')));
    check('売価の初期値: アプリに無ければ NE mirror の標準売価を使う',
      sp.unitPriceOf(db, 'setprice-ne')?.value === 500 && sp.unitPriceOf(db, 'setprice-ne')?.source === 'ne',
      JSON.stringify(sp.unitPriceOf(db, 'setprice-ne')));
    check('売価の初期値: 商品コードの大小文字・前後空白は無視して引く',
      sp.unitPriceOf(db, '  SETPRICE-NE ')?.value === 500);
    check('売価の初期値: どこにも無い商品コードは null (0 円にしない)',
      sp.unitPriceOf(db, 'setprice-nowhere') === null);
    // 🚨 どの引き先でも「全行が一致するときだけ採用」(Codex high 2026-09-04)。
    //    ふだんは正規化 UNIQUE index (idx_product_drafts_ne_norm) が `ABC` と ` abc ` の同居を防ぐが、
    //    **既存データが衝突している DB ではこの index が張られない** (db.js: 事前検査して skip)。
    //    その劣化状態を再現する。ORDER BY id LIMIT 1 だと、どちらの単価が出るかが**運**になる
    db.exec('DROP INDEX IF EXISTS idx_product_drafts_ne_norm');
    db.prepare(`INSERT INTO product_drafts (ne_code, name, status, price, created_by)
      VALUES ('SETPRICE-DUP', '同じコードの別ドラフト1', 'draft', 700, 'smoke')`).run();
    db.prepare(`INSERT INTO product_drafts (ne_code, name, status, price, created_by)
      VALUES (' setprice-dup ', '同じコードの別ドラフト2', 'draft', 700, 'smoke')`).run();
    check('売価の初期値: 同じ商品コードのドラフトが複数でも、売価が一致していれば使う',
      sp.unitPriceOf(db, 'setprice-dup')?.value === 700, JSON.stringify(sp.unitPriceOf(db, 'setprice-dup')));
    db.prepare(`UPDATE product_drafts SET price = 900 WHERE ne_code = ' setprice-dup '`).run();
    check('🚨 売価の初期値: 同じ商品コードで売価が割れていたら採らない (どちらが正か分からない)',
      sp.unitPriceOf(db, 'setprice-dup') === null, JSON.stringify(sp.unitPriceOf(db, 'setprice-dup')));
    // 「有効値と未入力の混在」も割れている扱い (Codex medium 2巡目)。どちらのドラフトが正か
    // 分からないまま値段を入れてしまわない
    db.prepare(`UPDATE product_drafts SET price = NULL WHERE ne_code = ' setprice-dup '`).run();
    check('🚨 売価の初期値: 片方が売価未入力なら「一致」とみなさない (どちらが正か分からない)',
      sp.unitPriceOf(db, 'setprice-dup') === null, JSON.stringify(sp.unitPriceOf(db, 'setprice-dup')));
    // 全部が未入力なら「その引き先には値が無い」= 次の引き先へ落ちてよい (止めない)
    db.prepare(`UPDATE product_drafts SET price = NULL WHERE LOWER(TRIM(ne_code)) = 'setprice-dup'`).run();
    db.prepare(`INSERT OR REPLACE INTO mirror_products
      (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 標準売価, 原価, 送料, 配送方法, 消費税率, updated_at)
      VALUES (99715, 'setprice-dup', 'NEにもある', '1', '取扱中', 'ok', 222, 50, 120, '定形外', 0.1, '2026-09-04T00:00:00Z')`).run();
    check('売価の初期値: ドラフトが全部売価未入力なら NE の標準売価へ落ちてよい',
      sp.unitPriceOf(db, 'setprice-dup')?.value === 222 && sp.unitPriceOf(db, 'setprice-dup')?.source === 'ne',
      JSON.stringify(sp.unitPriceOf(db, 'setprice-dup')));
    db.prepare(`DELETE FROM mirror_products WHERE product_id = 99715`).run();
    db.prepare(`UPDATE product_drafts SET price = 700 WHERE ne_code = 'SETPRICE-DUP'`).run();
    db.prepare(`UPDATE product_drafts SET price = 900 WHERE ne_code = ' setprice-dup '`).run();
    check('🚨 売価の初期値: 売価が割れているとき NE の標準売価へ落ちない (人が入れた値を無視しない)',
      (() => {
        db.prepare(`INSERT OR REPLACE INTO mirror_products
          (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 標準売価, 原価, 送料, 配送方法, 消費税率, updated_at)
          VALUES (99711, 'setprice-dup', 'NEにもある', '1', '取扱中', 'ok', 111, 50, 120, '定形外', 0.1, '2026-09-04T00:00:00Z')`).run();
        return sp.unitPriceOf(db, 'setprice-dup') === null;
      })(), JSON.stringify(sp.unitPriceOf(db, 'setprice-dup')));
    db.prepare(`DELETE FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'setprice-dup'`).run();
    db.prepare(`DELETE FROM mirror_products WHERE 商品コード = 'setprice-dup'`).run();
    // 劣化状態の再現はここまで。以降のテストのために正規化 UNIQUE を戻す
    db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_product_drafts_ne_norm ON product_drafts(LOWER(TRIM(ne_code)))');
    // ①' バリエーションの子SKU に人が付けた売価 (draft_sku_prices) も「アプリの値」(Codex medium)
    db.prepare(`INSERT OR REPLACE INTO mirror_products
      (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 標準売価, 原価, 送料, 配送方法, 消費税率, updated_at)
      VALUES (99712, 'setprice-sku', '子SKU', '1', '取扱中', 'ok', 300, 100, 120, '定形外', 0.1, '2026-09-04T00:00:00Z')`).run();
    db.prepare('INSERT INTO draft_sku_prices (draft_id, sku_code, price) VALUES (?, ?, ?)')
      .run(parentId, 'setprice-sku', 880);
    check('売価の初期値: 子SKU に付けた売価は NE の標準売価より優先する',
      sp.unitPriceOf(db, 'setprice-sku')?.value === 880 && sp.unitPriceOf(db, 'setprice-sku')?.source === 'sku',
      JSON.stringify(sp.unitPriceOf(db, 'setprice-sku')));
    db.prepare('INSERT INTO draft_sku_prices (draft_id, sku_code, price) VALUES (?, ?, ?)')
      .run(r.draftId, 'setprice-sku', 990);
    check('🚨 売価の初期値: 子SKU の売価が複数のドラフトで割れていたら採らない',
      sp.unitPriceOf(db, 'setprice-sku') === null, JSON.stringify(sp.unitPriceOf(db, 'setprice-sku')));
    db.prepare(`DELETE FROM draft_sku_prices WHERE sku_code = 'setprice-sku'`).run();
    // NE mirror 側も同じ扱い: 正規化で複数行に当たって値が割れたら採らない / 0円・負数は「無い」
    db.prepare(`INSERT OR REPLACE INTO mirror_products
      (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 標準売価, 原価, 送料, 配送方法, 消費税率, updated_at)
      VALUES (99713, ' SETPRICE-SKU ', '同じコードの別行', '1', '取扱中', 'ok', 400, 100, 120, '定形外', 0.1, '2026-09-04T00:00:00Z')`).run();
    check('🚨 売価の初期値: NE の標準売価が正規化で割れていたら採らない',
      sp.unitPriceOf(db, 'setprice-sku') === null, JSON.stringify(sp.unitPriceOf(db, 'setprice-sku')));
    db.prepare(`DELETE FROM mirror_products WHERE product_id IN (99712, 99713)`).run();
    db.prepare(`INSERT OR REPLACE INTO mirror_products
      (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 標準売価, 原価, 送料, 配送方法, 消費税率, updated_at)
      VALUES (99714, 'setprice-zero', '売価0円', '1', '取扱中', 'ok', 0, 100, 120, '定形外', 0.1, '2026-09-04T00:00:00Z')`).run();
    check('売価の初期値: 標準売価 0 円は「無い」扱い (0 円のセットを作らない)',
      sp.unitPriceOf(db, 'setprice-zero') === null);
    db.prepare(`UPDATE mirror_products SET 標準売価 = -100 WHERE product_id = 99714`).run();
    check('売価の初期値: マイナスの標準売価も「無い」扱い', sp.unitPriceOf(db, 'setprice-zero') === null);
    db.prepare(`DELETE FROM mirror_products WHERE product_id = 99714`).run();
    const mix = sp.setPriceFromMembers(db, [{ ne_code: 'WF-SET-P', qty: 2 }, { ne_code: 'setprice-ne', qty: 3 }]);
    check('売価の初期値: 複数商品の混載は それぞれの単価×個数 の和 (1,980×2 + 500×3 = 5,460)',
      mix.total === 5460, JSON.stringify(mix));
    check('売価の初期値: 式を1行で説明できる (画面とイベントで同じ言葉)',
      sp.describeSetPrice(mix) === 'WF-SET-P 1,980円 × 2 + setprice-ne 500円 × 3 = 5,460円',
      sp.describeSetPrice(mix));
    const gap = sp.setPriceFromMembers(db, [{ ne_code: 'WF-SET-P', qty: 2 }, { ne_code: 'setprice-nowhere', qty: 1 }]);
    check('🚨 売価の初期値: 1件でも単価が引けなければ合計を作らない (欠けた合計を入れない)',
      gap.total === null && gap.missing.includes('setprice-nowhere'), JSON.stringify(gap));
    check('売価の初期値: 構成が空なら合計も無い', sp.setPriceFromMembers(db, []).total === null);
    // セットの売価を単価に拾うと「セットのセット」の値段になる。単品 (parent_draft_id IS NULL) だけ見る
    check('売価の初期値: セットのドラフトは単価に数えない (単品だけを見る)',
      sp.unitPriceOf(db, r.neCode) === null, JSON.stringify(sp.unitPriceOf(db, r.neCode)));
    // 実際に作ったセットにも入る (混載)
    const rMix = sd.createSetDraft(parentId,
      { mode: 'ai', members: [{ ne_code: 'WF-SET-P', qty: 2 }, { ne_code: 'setprice-ne', qty: 3 }], parent_step_version: setReviewVersion() },
      'smoke', ADMIN);
    check('売価の初期値: 混載セットを作ると和が入る',
      db.prepare('SELECT price FROM product_drafts WHERE id = ?').get(rMix.draftId).price === 5460);
    check('売価の初期値: 混載セットの構成が順番どおり保存される',
      db.prepare('SELECT member_ne_code FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort')
        .all(rMix.draftId).map((x) => x.member_ne_code).join(',') === 'WF-SET-P,setprice-ne');
    const rGap = sd.createSetDraft(parentId,
      { mode: 'ai', members: [{ ne_code: 'WF-SET-P', qty: 2 }, { ne_code: 'setprice-nowhere', qty: 1 }], parent_step_version: setReviewVersion() },
      'smoke', ADMIN);
    check('売価の初期値: 単価が引けない商品が混ざったら売価は空のまま (人が必ず気づく)',
      db.prepare('SELECT price FROM product_drafts WHERE id = ?').get(rGap.draftId).price == null);
    check('売価の初期値: 入れられなかったことと理由もイベントに残す',
      /setprice-nowhere/.test(db.prepare(
        `SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'price_prefilled'`).get(rGap.draftId)?.detail || ''),
      db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'price_prefilled'`).get(rGap.draftId)?.detail || '');
    // 画面に出す「由来」も同じ計算 (setInfoOf)。人が値付けした後は言い方が変わる
    const oMix = sd.setInfoOf(db, rMix.draftId);
    check('売価の由来: 画面に出す目安は作成時と同じ計算で作る',
      oMix.priceOrigin?.total === 5460 && oMix.priceOrigin.priceEmpty === false, JSON.stringify(oMix.priceOrigin));
    // 🚨 「初期値のまま」を今の売価と計算値の一致で判定しない (Codex medium)。偶然の一致・
    //    作成後の単品値上げ・構成の変更、のどれでも嘘になる。売価が空かどうかだけを言う
    db.prepare('UPDATE product_drafts SET price = NULL WHERE id = ?').run(rMix.draftId);
    check('売価の由来: 売価が空なら「まだ入っていない」と分かる',
      sd.setInfoOf(db, rMix.draftId).priceOrigin?.priceEmpty === true);
    db.prepare('UPDATE product_drafts SET price = 5460 WHERE id = ?').run(rMix.draftId);
    check('売価の由来: 人が偶然おなじ値を入れても「初期値のまま」とは言わない (言える根拠が無い)',
      !('untouched' in (sd.setInfoOf(db, rMix.draftId).priceOrigin || {})),
      JSON.stringify(sd.setInfoOf(db, rMix.draftId).priceOrigin));
    db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(rMix.draftId, rGap.draftId);
  }

  // copy モードは説明文を持って商品説明確認から
  const r2 = sd.createSetDraft(parentId, { mode: 'copy', members: [{ ne_code: 'WF-SET-P', qty: 3 }], parent_step_version: setReviewVersion() }, 'smoke', ADMIN);
  check('2件目は連番になる', r2.neCode === 'SET-WF-SET-P-02', r2.neCode);
  check('copyモードは説明文をコピーする',
    db.prepare('SELECT content FROM draft_ai_outputs WHERE draft_id = ? AND kind = ?').get(r2.draftId, 'rakuten_title')?.content === '単品のタイトル');
  // モードの違いは**説明文の初期値だけ**。どちらも工程は「構成決定」から始まる
  check('copyモードでも工程は 構成決定 から (違いは説明文を引き継ぐかどうか)',
    wfp.progressOf(r2.draftId, { db }).current?.step_code === 'set_compose');

  // 検証
  let modeErr = null;
  try { sd.createSetDraft(parentId, { mode: 'unknown' }, 'smoke'); } catch (e) { modeErr = e; }
  check('作り方の指定を検証する', modeErr?.status === 400);
  let qtyErr = null;
  try { sd.createSetDraft(parentId, { mode: 'ai', members: [{ ne_code: 'X', qty: 0 }] }, 'smoke'); } catch (e) { qtyErr = e; }
  check('個数を検証する', qtyErr?.status === 400);
  // 🚨 未指定 (親×2 の既定) と「空を明示」を区別する。API だけ黙って別の構成に化けさせない
  let emptyErr = null;
  try { sd.createSetDraft(parentId, { mode: 'ai', members: [] }, 'smoke'); } catch (e) { emptyErr = e; }
  check('構成に空配列を明示したら 400 (黙って「親×2」にしない)',
    emptyErr?.status === 400 && /構成/.test(emptyErr.message), emptyErr?.message || '例外が出ていない');
  let notArrErr = null;
  try { sd.createSetDraft(parentId, { mode: 'ai', members: 'WF-SET-P' }, 'smoke'); } catch (e) { notArrErr = e; }
  check('構成が配列でなければ 400', notArrErr?.status === 400, notArrErr?.message || '例外が出ていない');
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
  // 本コードが確定したら「NE登録」工程も閉じる。人は押せない工程なので、ここで閉じないと
  // カードが NE登録の列に残り続ける (2026-09-04 §4.1 / Codex R3 medium)
  check('本コードが確定したら NE登録の工程も自動で閉じる',
    db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`)
      .get(r.draftId)?.state === 'done',
    db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`)
      .get(r.draftId)?.state);
  check('自動完了は 2 度目には何もしない (冪等)', sd.closeNeStepIfConfirmed(db, r.draftId) === false);
  // 確定と工程完了が別々に落ちた場合の取り残しを、次に開いたときに直す (Codex R4 medium)
  db.prepare(`UPDATE draft_step_progress SET state = 'todo', done_at = NULL, done_by = NULL
    WHERE draft_id = ? AND step_code = 'set_ne_register'`).run(r.draftId);
  sd.reconcileProvisionalCode(db, db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(r.draftId));
  check('確定済みなのに工程が取り残されていたら次に開いたときに直す',
    db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`)
      .get(r.draftId)?.state === 'done');
  // 単品は NE登録工程を持たないが、移行の失敗などで行が残っても触らないこと
  db.prepare(`INSERT INTO draft_step_progress (draft_id, step_code, state) VALUES (?, 'set_ne_register', 'todo')`).run(parentId);
  check('自動完了は単品には効かない (行が残っていても閉じない)',
    sd.closeNeStepIfConfirmed(db, parentId) === false
    && db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`).get(parentId)?.state === 'todo');
  db.prepare(`DELETE FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`).run(parentId);
  check('確定後は出品ゲートが開く',
    !(listing.buildItemPayload(db, r.draftId).reasons || []).some((x) => /商品コード/.test(x)));

  // 配送方法 (§4.4 決⑥ + 2026-09-05 中原さん判断)。
  // セットは親からコピーしないので、**NE の配送方法が唯一の出どころ**。
  // NE にも無ければ「決まらない」ので出品を止める — 送らないまま出すと、
  // 商品ページの帯と楽天の設定が食い違ったまま世に出る
  check('セット: NE に配送方法が無ければ出品を止める',
    (listing.buildItemPayload(db, r.draftId).reasons || []).some((x) => /配送方法が決まりません/.test(x)),
    JSON.stringify(listing.buildItemPayload(db, r.draftId).reasons || []));
  db.prepare(`UPDATE mirror_products SET 配送方法 = 'ネコポス' WHERE 商品コード = 'WF-SET-REAL'`).run();
  check('セット: NE に配送方法が載れば、選ばなくても出品できる (単品と違うのはここ)',
    !(listing.buildItemPayload(db, r.draftId).reasons || []).some((x) => /配送方法/.test(x)),
    JSON.stringify(listing.buildItemPayload(db, r.draftId).reasons || []));
  // (送る値そのものは payloadShippingGroup の単体テストで見ている。
  //  ここは画像が未登録なので payload まで組み上がらない = 出品ゲートの他の理由が先に立つ)
  check('確定後は Notion カードも作れる',
    dbmod.canWriteToNotion(db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(r.draftId)) === true);
  db.prepare(`DELETE FROM mirror_products WHERE product_id = 99401`).run();

  // 親側・セット側の表示用データ
  check('親からセット一覧が引ける', sd.setDraftsOf(db, parentId).length === 2);
  const info = sd.setInfoOf(db, r.draftId);
  check('セットから親が引ける', info?.parent?.id === parentId);
  check('セットの構成が引ける', info.members[0].qty === 2);
  check('単品では setInfo が null', sd.setInfoOf(db, parentId) === null);

  // 「親が更新されました」のお知らせは親の updated_at で見ている。
  // 🚨 だから「中身が変わったときだけ updated_at を進める」の判定には、子テーブル (税率・Yahoo 項目・
  //    AI の文言) の変更も入れないといけない (services/notion-import.js の childrenFingerprint)
  check('親の updated_at が進むと「親が更新されました」が出る (この 2 つは繋がっている)', (() => {
    const before = sd.setInfoOf(db, r.draftId).parentChanged;
    db.prepare("UPDATE product_drafts SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?").run(parentId);
    const after = sd.setInfoOf(db, r.draftId).parentChanged;
    return before === false && after === true;
  })());
}

// ─── セットの画像の引き継ぎ計画 (2026-09-04 §4.7) ───
// 中原さん: 「詳細画像の既定は『作らない』ではなく、単品の詳細画像で**何枚目を修正**みたいな形で
// 指示を送れるように」。枠ごとに そのまま使う/直して使う/作り直す/使わない を決める
{
  const parentId = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, price, created_by)
    VALUES ('WF-IMG-P', '画像計画の親', 'approved', 1000, 'smoke')
  `).run().lastInsertRowid);
  // 親の画像: 白抜き (slot 0) + TOP (sort 0 = slot 1) + 商品画像2枚 (sort 1,2 = slot 2,3)
  db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, white_bg_drive_file_id, white_bg_drive_url)
    VALUES (?, '565004', 'pw-white', 'https://drive.google.com/file/d/pw-white/view')`).run(parentId);
  for (const [fid, sort] of [['pimg-top', 0], ['pimg-01', 1], ['pimg-02', 2]]) {
    db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, drive_url, sort) VALUES (?, ?, ?, ?)')
      .run(parentId, fid, `https://drive.google.com/file/d/${fid}/view`, sort);
  }
  wfp.progressOf(parentId, { db });
  const pv = () => wfp.progressOf(parentId, { db }).main.find((x) => x.step_code === 'set_review').version;
  const r = sd.createSetDraft(parentId, { mode: 'ai', members: [{ ne_code: 'WF-IMG-P', qty: 2 }], parent_step_version: pv() }, 'smoke', ADMIN);
  const setId = r.draftId;

  // ① 枠の数え方 (lib)。**既存の楽天スロット番号に合わせる** (1=TOP / 2=_01)。
  //    draft_images.sort とは 1 ずれるので、変換を1箇所に閉じ込めている
  check('枠: TOP は slot 1・_01 は slot 2 (draft_images.sort とは 1 ずれる)',
    sip.slotOfImageSort(0) === 1 && sip.slotOfImageSort(1) === 2
    && sip.imageSortOfSlot(1) === 0 && sip.imageSortOfSlot(2) === 1
    && sip.imageSortOfSlot(sip.WHITE_BG_SLOT) === null);
  check('枠: 呼び名は画面と依頼書で同じ',
    sip.slotLabel(0) === '白抜き' && sip.slotLabel(1) === 'TOP画像' && sip.slotLabel(3) === '商品画像 2');

  // ② 作成時に親の全枠ぶんの計画ができ、既定は「そのまま使う」
  const plans0 = sd.setImagePlansOf(db, setId);
  check('作成時: 親の全枠ぶんの計画ができる (白抜き + TOP + 商品画像2枚 = 4枠)',
    plans0.length === 4 && plans0.map((p) => p.slot).join(',') === '0,1,2,3',
    JSON.stringify(plans0.map((p) => p.slot)));
  check('作成時: 既定は「そのまま使う」', plans0.every((p) => p.action === 'reuse'));
  check('作成時: 親のどの画像かを覚えている (後から差し替わっても計画が読める)',
    plans0.find((p) => p.slot === 1)?.parent_drive_file_id === 'pimg-top'
    && plans0.find((p) => p.slot === 0)?.parent_drive_file_id === 'pw-white');

  // ③ 「そのまま使う」枠の画像はセットにコピーされる (白抜きは楽天の欄へ)
  const setImgs = db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ? ORDER BY sort').all(setId);
  check('そのまま使う: 画像がセットにコピーされ、枠の位置も保たれる',
    setImgs.map((i) => `${i.sort}:${i.drive_file_id}`).join(',') === '0:pimg-top,1:pimg-01,2:pimg-02',
    JSON.stringify(setImgs));
  check('そのまま使う: 白抜きは楽天の欄にコピーされる',
    db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(setId)?.white_bg_drive_file_id === 'pw-white');
  check('コピーは何度やっても増えない (計画を直すたびに走るため)',
    sd.applyReuseImages(db, setId).copied === 0
    && db.prepare('SELECT COUNT(*) AS c FROM draft_images WHERE draft_id = ?').get(setId).c === 3);

  // ④ 全部そのまま使うなら制作は要らない = 画像の工程は「対象外」で決着
  const detailSteps = () => db.prepare(`
    SELECT p.step_code, p.state FROM draft_step_progress p JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
    WHERE p.draft_id = ? AND s.track = 'image' AND s.image_kind = 'detail' ORDER BY s.sort
  `).all(setId);
  check('全部そのまま使う → 画像の制作工程は「対象外」で決着する',
    detailSteps().length > 0 && detailSteps().every((x) => x.state === 'skip'),
    JSON.stringify(detailSteps().map((x) => x.state)));

  // ⑤ 1枠でも「直して使う」にすると制作が動き出し、指示が依頼に載る
  const put = (items) => sd.replaceSetImagePlans(db, setId, items, 'smoke');
  put([
    { slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
    { slot: 2, action: 'modify', instruction: '2個並べた写真に' },
    { slot: 3, action: 'drop' },
  ]);
  check('直して使う: 1枠でも入れば制作工程が「対象外」から戻る',
    detailSteps().every((x) => x.state === 'todo'), JSON.stringify(detailSteps().map((x) => x.state)));
  const ins = sip.productionInstructions(sd.setImagePlansOf(db, setId));
  check('直して使う: 依頼に「どの枠を・どう直すか」が載る',
    ins.length === 1 && ins[0].text === '商品画像 1: 直して使う — 2個並べた写真に', JSON.stringify(ins));
  check('直して使う: 親からコピーしてあった画像は枠から外す (空で待つ)',
    !db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(setId, 'pimg-01'),
    JSON.stringify(db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ? ORDER BY sort').all(setId)));
  check('使わない: その枠も空になる',
    !db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(setId, 'pimg-02'));
  check('そのまま使う枠は残る (TOP・白抜き)',
    !!db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(setId, 'pimg-top')
    && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(setId)?.white_bg_drive_file_id === 'pw-white');
  check('使わない: 枠を落としても他の枠は残る',
    sd.setImagePlansOf(db, setId).find((p) => p.slot === 3)?.action === 'drop');

  // ⑥ 「直して使う」は指示が必須 (指示の無い依頼は作れない)
  let noIns = null;
  try { put([{ slot: 2, action: 'modify', instruction: '  ' }]); } catch (e) { noIns = e; }
  check('直して使う: どう直すかを書いていなければ 400',
    noIns?.status === 400 && /どう直すか/.test(noIns.message), noIns?.message || '通ってしまった');
  let badSlot = null;
  try { put([{ slot: 9, action: 'reuse' }]); } catch (e) { badSlot = e; }
  check('親に無い枠は受けない', badSlot?.status === 400, badSlot?.message || '通ってしまった');
  let badAct = null;
  try { put([{ slot: 1, action: 'unknown' }]); } catch (e) { badAct = e; }
  check('知らない指定は受けない', badAct?.status === 400, badAct?.message || '通ってしまった');
  let notSet = null;
  try { sd.replaceSetImagePlans(db, parentId, [{ slot: 1, action: 'reuse' }], 'smoke'); } catch (e) { notSet = e; }
  check('単品には計画を作らせない', notSet?.status === 400, notSet?.message || '通ってしまった');

  // ⑥' 🚨 人が入れ直した画像・制作の成果物は、計画を変えても消さない。
  //     消えるのは「親からコピーしたままのもの」だけ (届いた画像が消えるようでは使えない)
  db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, ?, ?)')
    .run(setId, 'made-by-designer', 1);
  put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'recreate' },
    { slot: 2, action: 'modify', instruction: '2個並べた写真に' }, { slot: 3, action: 'drop' }]);
  check('🚨 制作で入った画像は、計画を変えても消さない',
    !!db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(setId, 'made-by-designer'),
    JSON.stringify(db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ?').all(setId)));
  check('作り直す: 親からコピーしてあった TOP は外れる',
    !db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(setId, 'pimg-top'));
  db.prepare('DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').run(setId, 'made-by-designer');

  // ⑦ 戻せば制作工程もまた「対象外」になる (todo のものだけ。人が進めた done は触らない)
  put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' }, { slot: 2, action: 'reuse' }, { slot: 3, action: 'reuse' }]);
  check('全部そのまま使うに戻せば、制作工程はまた「対象外」になる',
    detailSteps().every((x) => x.state === 'skip'), JSON.stringify(detailSteps().map((x) => x.state)));
  const firstStep = detailSteps()[0].step_code;
  db.prepare(`UPDATE draft_step_progress SET state = 'done' WHERE draft_id = ? AND step_code = ?`).run(setId, firstStep);
  put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
    { slot: 2, action: 'recreate', instruction: '' }, { slot: 3, action: 'reuse' }]);
  check('🚨 人が進めた工程 (done) は巻き戻さない',
    detailSteps().find((x) => x.step_code === firstStep)?.state === 'done',
    JSON.stringify(detailSteps()));
  check('作り直す: 指示は無くてもよい (直して使う と違う)',
    sd.setImagePlansOf(db, setId).find((p) => p.slot === 2)?.action === 'recreate');

  // ⑧ 履歴に残る (何をどう変えたか)
  check('計画の変更は履歴に残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'set_image_plan_changed'`).get(setId).c >= 1);
  check('作成時の計画も履歴に残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'set_image_plan_seeded'`).get(setId).c === 1);

  // ⑧' 🚨 制作が全部 done のあとに「直して使う」を足しても、出品は止まる (Codex R1 high)。
  //     工程は人の作業の記録なので巻き戻さない。出品ゲートは**実際に画像があるか**で見る
  {
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
      { slot: 2, action: 'reuse' }, { slot: 3, action: 'reuse' }]);
    db.prepare(`
      UPDATE draft_step_progress SET state = 'done'
      WHERE draft_id = ? AND step_code IN (SELECT code FROM ph_steps WHERE track = 'image')
    `).run(setId);
    check('計画が全部そのまま使うなら、作る予定の枠は無い',
      sd.pendingImagePlanSlots(db, setId).length === 0);
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
      { slot: 2, action: 'modify', instruction: '2個並べた写真に' }, { slot: 3, action: 'reuse' }]);
    const pend = sd.pendingImagePlanSlots(db, setId);
    check('🚨 制作が done でも、あとから足した「直して使う」の枠は「まだ空」と分かる',
      pend.length === 1 && pend[0].slot === 2, JSON.stringify(pend.map((x) => x.slot)));
    check('🚨 その状態では出品ゲートが止める (工程が done でも)',
      (listing.buildItemPayload(db, setId).reasons || []).some((x) => /画像の計画で作ることにした枠/.test(x)),
      JSON.stringify(listing.buildItemPayload(db, setId).reasons || []));
    // 画像が届けば止まらない (計画より後に入った画像であること)
    db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, ?, ?)')
      .run(setId, 'made-01', sip.imageSortOfSlot(2));
    check('画像が届けば、計画の枠では止まらなくなる',
      sd.pendingImagePlanSlots(db, setId).length === 0
      && !(listing.buildItemPayload(db, setId).reasons || []).some((x) => /画像の計画で作ることにした枠/.test(x)),
      JSON.stringify(listing.buildItemPayload(db, setId).reasons || []));

    // 🚨 指示を変えたら、**前の指示で作った画像では満たされない** (Codex R2 high)。
    // 「2個並べて」の成果物が入っていても、「3個並べて」に変えたら作り直しが要る
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
      { slot: 2, action: 'modify', instruction: '3個並べた写真に' }, { slot: 3, action: 'reuse' }]);
    check('🚨 指示を変えたら、前の指示で作った画像では満たされない',
      sd.pendingImagePlanSlots(db, setId).map((x) => x.slot).join(',') === '2',
      JSON.stringify(sd.pendingImagePlanSlots(db, setId).map((x) => x.slot)));
    check('🚨 指示を変えたあとは出品も止まる',
      (listing.buildItemPayload(db, setId).reasons || []).some((x) => /画像の計画で作ることにした枠/.test(x)));

    // 🚨 指定を変えた枠は、変えた先が何であれ前の画像を残さない (Codex R3 high)
    db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, ?, ?)')
      .run(setId, 'made-02', sip.imageSortOfSlot(2));
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
      { slot: 2, action: 'drop' }, { slot: 3, action: 'reuse' }]);
    check('🚨 直して使う → 使わない: 前の成果物は枠から外れる (使わないのに出品に残らない)',
      !db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(setId, 'made-02'),
      JSON.stringify(db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ?').all(setId)));
    db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, ?, ?)')
      .run(setId, 'made-03', sip.imageSortOfSlot(2));
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
      { slot: 2, action: 'reuse' }, { slot: 3, action: 'reuse' }]);
    check('🚨 直して使う → そのまま使う: 前の成果物ではなく親の画像に戻る',
      !db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(setId, 'made-03')
      && db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ? AND sort = ?')
        .get(setId, sip.imageSortOfSlot(2))?.drive_file_id === 'pimg-01',
      JSON.stringify(db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ? ORDER BY sort').all(setId)));

    db.prepare('DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').run(setId, 'made-01');
  }

  // ⑧'' 🚨 同じ画像を別の枠へ移したあとに計画を変えても、移した先からは消さない (Codex R1 medium)
  {
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
      { slot: 2, action: 'reuse' }, { slot: 3, action: 'reuse' }]);
    // TOP の画像 (pimg-top) を人が商品画像2の枠へ移した
    db.prepare('UPDATE draft_images SET sort = ? WHERE draft_id = ? AND drive_file_id = ?')
      .run(sip.imageSortOfSlot(3), setId, 'pimg-top');
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'recreate' },
      { slot: 2, action: 'reuse' }, { slot: 3, action: 'reuse' }]);
    check('🚨 別の枠へ移した画像は、計画を変えても消さない',
      !!db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ? AND sort = ?')
        .get(setId, 'pimg-top', sip.imageSortOfSlot(3)),
      JSON.stringify(db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ?').all(setId)));
    // 🚨 その状態で元の枠を「そのまま使う」に戻しても、同じ画像は 1 商品に 1 回しか置けないので
    //    枠は空のまま。**使うつもりの枠が空**なら出品を止める (Codex R4 high)
    put([{ slot: 0, action: 'reuse' }, { slot: 1, action: 'reuse' },
      { slot: 2, action: 'reuse' }, { slot: 3, action: 'reuse' }]);
    check('🚨 「そのまま使う」なのに枠が空 (画像が別の枠にある) なら未達として数える',
      sd.pendingImagePlanSlots(db, setId).some((x) => x.slot === 1),
      JSON.stringify({
        pending: sd.pendingImagePlanSlots(db, setId).map((x) => x.slot),
        images: db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ?').all(setId),
      }));
    check('🚨 その状態では出品も止まる',
      (listing.buildItemPayload(db, setId).reasons || []).some((x) => /画像の計画/.test(x)));
    check('使わない枠は空でよい (未達に数えない)',
      !sd.pendingImagePlanSlots(db, setId).some((x) => x.action === 'drop'));
    db.prepare('DELETE FROM draft_images WHERE draft_id = ?').run(setId);
  }

  // ⑧''' 導入前に作られたセット (計画が無い) にも、あとから計画を作る (Codex R4 medium)
  {
    const oldSetId = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code)
      VALUES ('WF-IMG-OLD', '導入前に作られたセット', 'draft', 'smoke', ?, 1)
    `).run(parentId).lastInsertRowid);
    wfp.ensureProgress(db, oldSetId);
    // 人が入れた画像と、進行中の画像制作がある状態
    db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, ?, ?)')
      .run(oldSetId, 'hand-made', 0);
    const detailOf = (id) => db.prepare(`
      SELECT p.state FROM draft_step_progress p JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
      WHERE p.draft_id = ? AND s.track = 'image' AND s.image_kind = 'detail' ORDER BY s.sort
    `).all(id).map((x) => x.state);
    check('前提: 導入前のセットには計画が無い', sd.setImagePlansOf(db, oldSetId).length === 0);
    const before = detailOf(oldSetId).join(',');
    check('バックフィル: 親の枠ぶんの計画があとから作られる',
      sd.backfillSetImagePlans(db) >= 1 && sd.setImagePlansOf(db, oldSetId).length === 4,
      JSON.stringify(sd.setImagePlansOf(db, oldSetId).map((x) => x.slot)));
    check('🚨 バックフィルは画像も工程も触らない (動いている制作を止めない)',
      detailOf(oldSetId).join(',') === before
      && db.prepare('SELECT COUNT(*) AS c FROM draft_images WHERE draft_id = ?').get(oldSetId).c === 1,
      `${before} → ${detailOf(oldSetId).join(',')}`);
    check('バックフィル: 2 回目は何もしない (冪等)', sd.backfillSetImagePlans(db) === 0);
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(oldSetId);
  }

  // ⑨ 親に画像が 1 枚も無いセットを「制作不要」にしない (画像ゼロで出品ゲートを素通りさせない)
  const bareId = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-IMG-BARE', '画像の無い親', 'approved', 'smoke')
  `).run().lastInsertRowid);
  wfp.progressOf(bareId, { db });
  const bareVer = () => wfp.progressOf(bareId, { db }).main.find((x) => x.step_code === 'set_review').version;
  const rb = sd.createSetDraft(bareId, { mode: 'ai', parent_step_version: bareVer() }, 'smoke', ADMIN);
  check('🚨 親に画像が無ければ計画は空。制作工程を「対象外」にしない',
    sd.setImagePlansOf(db, rb.draftId).length === 0
    && db.prepare(`
      SELECT COUNT(*) AS c FROM draft_step_progress p JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
      WHERE p.draft_id = ? AND s.track = 'image' AND s.image_kind = 'detail' AND p.state = 'skip'
    `).get(rb.draftId).c === 0);
}

// ─── セット工程トラック (2026-09-04 §4.1/§4.6/§5.1) ───
{
  const parentId = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, created_by, price)
    VALUES ('WF-TRK-P', 'トラック検証の単品', 'draft', 'smoke', 1000)
  `).run().lastInsertRowid);
  wfp.ensureProgress(db, parentId);
  const setId = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code)
    VALUES ('SET-WF-TRK-P-01', 'トラック検証のセット', 'draft', 'smoke', ?, 1)
  `).run(parentId).lastInsertRowid);
  const codesOf = (id) => db.prepare(`
    SELECT p.step_code, s.track FROM draft_step_progress p JOIN ph_steps s ON s.code = p.step_code
    WHERE p.draft_id = ? ORDER BY s.track, s.sort
  `).all(id);

  // ① 工程テンプレートの振り分け
  wfp.ensureProgress(db, setId);
  const setSteps = codesOf(setId);
  check('セットは set + image の行だけを持つ',
    setSteps.filter((r) => r.track === 'set').length === 4
    && setSteps.filter((r) => r.track === 'main').length === 1     // listing は単品と共用
    && setSteps.find((r) => r.track === 'main').step_code === 'listing'
    && setSteps.some((r) => r.track === 'image'),
    JSON.stringify(setSteps.map((r) => r.step_code)));
  check('セットは単品の基本情報・AI待ち・セット展開判断を持たない',
    !setSteps.some((r) => ['basic_info', 'ai_generate', 'desc_review', 'title_approve', 'set_review'].includes(r.step_code)));
  check('単品はセット工程を持たない',
    !codesOf(parentId).some((r) => r.track === 'set'));

  // ② status の導出 (語彙は増やさず既存に写す)
  const st = () => wfp.deriveDraftStatus(db, setId);
  const doStep = (code, state) => db.prepare(
    `UPDATE draft_step_progress SET state = ? WHERE draft_id = ? AND step_code = ?`).run(state, setId, code);
  check('セット: 構成決定が未了なら draft', st() === 'draft', st());
  doStep('set_compose', 'done');
  check('セット: NE登録待ちは ready_for_ai', st() === 'ready_for_ai', st());
  doStep('set_ne_register', 'done');
  check('セット: 商品情報作成中は review', st() === 'review', st());
  doStep('set_content', 'done');
  check('セット: 出品準備が残っていれば review のまま', st() === 'review', st());
  doStep('set_prep', 'done');
  check('セット: 出品準備まで済めば approved', st() === 'approved', st());
  doStep('listing', 'done');
  check('セット: 出品・展開が閉じたら expanded', st() === 'expanded', st());
  for (const c of ['set_compose', 'set_ne_register', 'set_content', 'set_prep', 'listing']) doStep(c, 'todo');

  // ③ ボードのビュー
  const boardOf = (view) => wfp.boardData(db, { view, mallSummary: ms.mallSummaryFor });
  const bSet = boardOf('set');
  check('view=set の列はセット工程 + 出品・展開',
    bSet.columns.map((c) => c.code).join(',') === 'set_compose,set_ne_register,set_content,set_prep,listing',
    bSet.columns.map((c) => c.code).join(','));
  const idsIn = (b) => new Set(b.columns.flatMap((c) => c.cards.map((x) => x.id)).concat(b.doneCards.map((x) => x.id)));
  check('view=set にはセットだけが出る', idsIn(bSet).has(setId) && !idsIn(bSet).has(parentId));
  const bSingle = boardOf('single');
  check('view=single にはセットが出ない', idsIn(bSingle).has(parentId) && !idsIn(bSingle).has(setId));
  check('view=single の列は本流のまま', bSingle.columns.some((c) => c.code === 'basic_info'));
  const bAll = boardOf('main');
  check('view=all にはセットも単品も出る', idsIn(bAll).has(setId) && idsIn(bAll).has(parentId));
  check('all は main の別名', boardOf('all').columns.length === bAll.columns.length);

  // ④ 投影 (§4.6): セット工程は本流の列に写るが、カードは本当の工程名を持つ
  const colOfSet = bAll.columns.find((c) => c.cards.some((x) => x.id === setId));
  check('全体ビュー: 構成決定のセットは「基本情報入力」の列に投影される',
    colOfSet?.code === 'basic_info', colOfSet?.code);
  check('全体ビュー: カードは本当のセット工程を持つ (列名と違うことを隠さない)',
    colOfSet.cards.find((x) => x.id === setId)?.current?.step_code === 'set_compose');
  doStep('set_compose', 'done'); doStep('set_ne_register', 'done');
  const colContent = boardOf('main').columns.find((c) => c.cards.some((x) => x.id === setId));
  check('全体ビュー: 商品情報作成は「商品説明確認」の列に投影される',
    colContent?.code === 'desc_review', colContent?.code);
  for (const c of ['set_compose', 'set_ne_register']) doStep(c, 'todo');

  // ⑤ D&D (§4.6): 全体ビューで本流の列に落としても、動くのは本当のセット工程
  const ADMIN2 = { isAdmin: true, actorStaffId: null };
  let neSkip = null;
  try {
    wfp.moveBoardCard(setId, { view: 'main', to: 'desc_review', expectedCurrent: 'set_compose' }, 'smoke', ADMIN2);
  } catch (e) { neSkip = e; }
  check('D&D: NE登録は飛び越せない', neSkip?.status === 400 && /NE 本コード/.test(neSkip.message), neSkip?.message);
  // 逆に、本コードが確定していれば D&D でも通れる。止める理由は「仮コード」であって工程ではない
  // (D&D 側にも別の判断を置くと、確定後もボードだけ 400 になる — Codex R2 medium)
  db.prepare('UPDATE product_drafts SET provisional_code = 0 WHERE id = ?').run(setId);
  wfp.moveBoardCard(setId, { view: 'main', to: 'desc_review', expectedCurrent: 'set_compose' }, 'smoke', ADMIN2);
  check('D&D: 本コードが確定していれば NE登録を通過して先へ進める',
    wfp.progressOf(setId, { db }).current?.step_code === 'set_content',
    wfp.progressOf(setId, { db }).current?.step_code);
  for (const c of ['set_compose', 'set_ne_register', 'set_content']) doStep(c, 'todo');
  db.prepare('UPDATE product_drafts SET provisional_code = 1 WHERE id = ?').run(setId);
  // 構成決定と NE登録は同じ列 (基本情報入力) に写るので、その列に落としても動かない
  const noMove = wfp.moveBoardCard(setId, { view: 'main', to: 'basic_info', expectedCurrent: 'set_compose' }, 'smoke', ADMIN2);
  check('D&D: 同じ列に落としても工程は動かない', noMove.changed === false
    && wfp.progressOf(setId, { db }).current?.step_code === 'set_compose');
  // セットビューはセット工程コードそのままで動く
  wfp.moveBoardCard(setId, { view: 'set', to: 'set_ne_register', expectedCurrent: 'set_compose' }, 'smoke', ADMIN2);
  check('D&D: セットビューはセット工程コードで動く',
    wfp.progressOf(setId, { db }).current?.step_code === 'set_ne_register');
  // NE登録が済んでいれば、全体ビューの「商品説明確認」列に落として 商品情報作成 へ進める
  doStep('set_ne_register', 'done');
  wfp.moveBoardCard(setId, { view: 'main', to: 'desc_review', expectedCurrent: 'set_content' }, 'smoke', ADMIN2);
  check('D&D: 全体ビューの列は本当のセット工程に写して動かす',
    wfp.progressOf(setId, { db }).current?.step_code === 'set_content',
    wfp.progressOf(setId, { db }).current?.step_code);
  wfp.moveBoardCard(setId, { view: 'main', to: 'title_approve', expectedCurrent: 'set_content' }, 'smoke', ADMIN2);
  check('D&D: 「タイトル確認」の列は 出品準備 に写る',
    wfp.progressOf(setId, { db }).current?.step_code === 'set_prep',
    wfp.progressOf(setId, { db }).current?.step_code);
  wfp.moveBoardCard(setId, { view: 'main', to: 'basic_info', expectedCurrent: 'set_prep' }, 'smoke', ADMIN2);
  check('D&D: 差し戻しも投影先から本当の工程に戻る',
    wfp.progressOf(setId, { db }).current?.step_code === 'set_compose',
    wfp.progressOf(setId, { db }).current?.step_code);
  // セットが持たない列 (AI情報入力待ち・セット展開判断) には落とせない
  let noColErr = null;
  try {
    wfp.moveBoardCard(setId, { view: 'main', to: 'ai_generate', expectedCurrent: 'set_compose' }, 'smoke', ADMIN2);
  } catch (e) { noColErr = e; }
  check('D&D: セットが持たない列には落とせない', noColErr?.status === 400, noColErr?.message || '通ってしまった');
  // 「移動先の工程が見つかりません」だけだと何が悪いのか分からない (2026-09-25 スタッフ報告)
  check('D&D: セットが持たない列の理由と進め方を言う',
    /セット商品には「AI情報入力待ち」の工程がありません/.test(noColErr?.message || '') && /セット工程」タブ/.test(noColErr?.message || ''),
    noColErr?.message || '');
  check('D&D: セットが持たない列に落としても工程は動かない',
    wfp.progressOf(setId, { db }).current?.step_code === 'set_compose', wfp.progressOf(setId, { db }).current?.step_code);

  // ⑥ 権限 (§4.1): セット企画者はセット工程の全部を操作できる。単品には効かない
  const plannerId = Number(db.prepare(
    `INSERT INTO ph_staff (name, active) VALUES ('セット企画 太郎', 1)`).run().lastInsertRowid);
  db.prepare(`INSERT INTO ph_staff_roles (staff_id, role_code) VALUES (?, 'set_planner')`).run(plannerId);
  const asPlanner = { isAdmin: false, actorStaffId: plannerId };
  // 担当ロールが registrar の工程 (NE登録・商品情報) も、担当者未割り当てのまま動かせる
  db.prepare(`UPDATE draft_step_progress SET assignee_id = NULL WHERE draft_id = ?`).run(setId);
  wfp.setStepState(setId, 'set_compose', { state: 'done' }, 'planner', asPlanner);
  let contentErr = null;
  try { wfp.setStepState(setId, 'set_content', { state: 'done' }, 'planner', asPlanner); } catch (e) { contentErr = e; }
  check('セット企画者は担当ロールが別のセット工程も操作できる', contentErr === null, contentErr?.message);
  let mainErr = null;
  try { wfp.setStepState(parentId, 'basic_info', { state: 'done' }, 'planner', asPlanner); } catch (e) { mainErr = e; }
  check('セット企画者でも単品の工程には効かない', mainErr?.status === 403, mainErr?.message || '通ってしまった');
  let skipErr = null;
  try { wfp.setStepState(setId, 'set_content', { state: 'skip' }, 'planner', asPlanner); } catch (e) { skipErr = e; }
  check('セット企画者でも「対象外」は管理者だけ', skipErr?.status === 403, skipErr?.message || '通ってしまった');
  // 出品準備 (承認) は承認者の工程。企画者が自分の企画を自分で承認できてしまわないこと (Codex R1 high)
  let prepErr = null;
  try { wfp.setStepState(setId, 'set_prep', { state: 'done' }, 'planner', asPlanner); } catch (e) { prepErr = e; }
  check('セット企画者は出品準備 (承認) までは通せない', prepErr?.status === 403, prepErr?.message || '通ってしまった');
  // 本コードの差し替え (router の /set-code) も同じ物差しを使う。ここが食い違うと
  // 「工程は動かせるのに差し替えだけ 403」になる (Codex R2 high)
  check('セット企画者かどうかの判定は 1 箇所 (router の本コード差し替えも同じ)',
    wfp.canOperateSetStep(db, plannerId) === true && wfp.canOperateSetStep(db, null) === false);

  // NE登録は仮コードのままでは閉じられない。D&D だけでなく工程 API も塞ぐ (Codex R1 high)
  let neManual = null;
  try { wfp.setStepState(setId, 'set_ne_register', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { neManual = e; }
  check('NE登録: 仮コードのままでは管理者でも閉じられない',
    neManual?.status === 400 && /本コード/.test(neManual.message), neManual?.message || '通ってしまった');
  let neSkipManual = null;
  try { wfp.setStepState(setId, 'set_ne_register', { state: 'skip' }, 'smoke', ADMIN2); } catch (e) { neSkipManual = e; }
  check('NE登録: 「対象外」で迂回もできない', neSkipManual?.status === 400, neSkipManual?.message || '通ってしまった');
  db.prepare('UPDATE product_drafts SET provisional_code = 0 WHERE id = ?').run(setId);
  wfp.setStepState(setId, 'set_ne_register', { state: 'done' }, 'smoke', ADMIN2);
  check('NE登録: 本コードが確定していれば閉じられる',
    db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`)
      .get(setId)?.state === 'done');
  db.prepare('UPDATE product_drafts SET provisional_code = 1 WHERE id = ?').run(setId);

  // ⑦ 既存セットの移行 (§4.1)。PR2 以前に作られたセットは本流の進捗を持っている
  const legacyId = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code)
    VALUES ('SET-WF-TRK-P-99', '移行検証の旧セット', 'draft', 'smoke', ?, 1)
  `).run(parentId).lastInsertRowid);
  db.prepare('DELETE FROM draft_step_progress WHERE draft_id = ?').run(legacyId);
  const legacyIns = db.prepare(
    'INSERT INTO draft_step_progress (draft_id, step_code, state, note) VALUES (?, ?, ?, ?)');
  for (const [code, state] of [['basic_info', 'done'], ['ai_generate', 'done'], ['desc_review', 'done'],
    ['title_approve', 'todo'], ['set_review', 'todo'], ['listing', 'todo']]) {
    legacyIns.run(legacyId, code, state, code === 'desc_review' ? '説明文は確認済み' : null);
  }
  const moved = dbmod.migrateSetDraftsToSetTrack(db);
  const after = Object.fromEntries(db.prepare(
    'SELECT step_code, state FROM draft_step_progress WHERE draft_id = ?').all(legacyId).map((r) => [r.step_code, r.state]));
  check('移行: 旧セットを1件写した', moved === 1, String(moved));
  check('移行: 基本情報 + AI待ち → 構成決定 (done)', after.set_compose === 'done', JSON.stringify(after));
  check('移行: 先に進んでいれば NE登録も done (仮コードでは進めないため)', after.set_ne_register === 'done');
  check('移行: 商品説明確認 → 商品情報作成 (状態も引き継ぐ)', after.set_content === 'done');
  check('移行: タイトル確認 → 出品準備 (todo のまま)', after.set_prep === 'todo');
  check('移行: 出品・展開は単品と共用なので残す', after.listing === 'todo');
  check('移行: 旧の本流工程は消す (列が二重に並ばないように)',
    !['basic_info', 'ai_generate', 'desc_review', 'title_approve', 'set_review'].some((c) => after[c]),
    JSON.stringify(after));
  check('移行: note も引き継ぐ (作業の記録を捨てない)',
    db.prepare(`SELECT note FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_content'`)
      .get(legacyId)?.note === '説明文は確認済み');
  check('移行: 何を写したか履歴に残る',
    db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'set_steps_migrated'`).get(legacyId).c === 1);
  check('移行: 2回目は何もしない (冪等)', dbmod.migrateSetDraftsToSetTrack(db) === 0);
  check('移行: 単品には触らない',
    codesOf(parentId).some((r) => r.step_code === 'basic_info') && !codesOf(parentId).some((r) => r.track === 'set'));

  // ⑨ NE登録の進み (2026-09-04 §4.3/§5.5)。工程とは別に「どこで止まっているか」を持つ
  {
    const neId = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code, ne_registration_state)
      VALUES ('SET-WF-TRK-P-97', 'NE進み検証のセット', 'draft', 'smoke', ?, 1, 'not_requested')
    `).run(parentId).lastInsertRowid);
    wfp.ensureProgress(db, neId);

    check('NE: 初期は未要求', sd.setInfoOf(db, neId).neState === 'not_requested');
    sd.setNeRegistrationState(db, neId, { state: 'requested' }, '中原 実紀');
    const req1 = sd.setInfoOf(db, neId);
    check('NE: 依頼済みにすると日時と人が残る',
      req1.neState === 'requested' && !!req1.neRequestedAt && req1.neRequestedBy === '中原 実紀',
      JSON.stringify({ s: req1.neState, at: req1.neRequestedAt, by: req1.neRequestedBy }));

    let noReason = null;
    try { sd.setNeRegistrationState(db, neId, { state: 'needs_action' }, 'smoke'); } catch (e) { noReason = e; }
    check('NE: 要対応は理由が必須 (理由の無い要対応は誰も動かせない)',
      noReason?.status === 400, noReason?.message || '通ってしまった');
    sd.setNeRegistrationState(db, neId, { state: 'needs_action', reason: '商品コードが重複していた' }, 'smoke');
    check('NE: 要対応は理由まで持つ',
      sd.setInfoOf(db, neId).neError === '商品コードが重複していた');
    sd.setNeRegistrationState(db, neId, { state: 'requested' }, 'smoke');
    check('NE: 再要求すると理由は消える', sd.setInfoOf(db, neId).neError === null);

    let badState = null;
    try { sd.setNeRegistrationState(db, neId, { state: 'confirmed' }, 'smoke'); } catch (e) { badState = e; }
    check('NE: 「確定」は人が指定できない (provisional_code=0 が唯一の真)',
      badState?.status === 400, badState?.message || '通ってしまった');
    let notSet = null;
    try { sd.setNeRegistrationState(db, parentId, { state: 'requested' }, 'smoke'); } catch (e) { notSet = e; }
    check('NE: 単品には効かない (理由も「セットではない」と言う)',
      notSet?.status === 400 && /セット商品ではありません/.test(notSet.message),
      notSet?.message || '通ってしまった');

    // 表示上の「確定」は列ではなく provisional_code から出す
    db.prepare('UPDATE product_drafts SET provisional_code = 0 WHERE id = ?').run(neId);
    check('NE: 本コードが確定したら状態は「確定」と出す (列は requested のまま)',
      sd.setInfoOf(db, neId).neState === 'confirmed'
      && db.prepare('SELECT ne_registration_state FROM product_drafts WHERE id = ?').get(neId).ne_registration_state === 'requested');
    let afterFix = null;
    try { sd.setNeRegistrationState(db, neId, { state: 'requested' }, 'smoke'); } catch (e) { afterFix = e; }
    check('NE: 確定後はもう動かせない', afterFix?.status === 400, afterFix?.message || '通ってしまった');
    db.prepare('UPDATE product_drafts SET provisional_code = 1 WHERE id = ?').run(neId);

    // 要対応ビュー (§5.5)
    sd.setNeRegistrationState(db, neId, { state: 'needs_action', reason: 'コード重複' }, 'smoke');
    const rows = wfp.neRegistrationRows(db);
    const mine = rows.find((r) => r.id === neId);
    check('要対応ビュー: 仮コードのセットが並ぶ', !!mine, `rows=${rows.length}`);
    check('要対応ビュー: 状態・理由・次にやること が行に入る',
      mine.state === 'needs_action' && mine.error === 'コード重複' && /再要求/.test(mine.next),
      JSON.stringify(mine).slice(0, 200));
    check('要対応ビュー: 親と構成が分かる', mine.parentId === parentId);
    check('要対応ビュー: 単品は出さない', !rows.some((r) => r.id === parentId));
    check('要対応ビュー: 要対応が先頭 (人が動かないと止まったままのものから)',
      rows[0].state === 'needs_action', rows.map((r) => r.state).join(','));

    // 🚨 並び順は LIMIT の前に効かせる (Codex R1 high)。新しい「未要求」が上限を食い潰して、
    // いちばん見たい「要対応」が表から消えることがあってはいけない
    {
      const filler = [];
      for (let i = 0; i < 5; i++) {
        filler.push(Number(db.prepare(`
          INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code,
            ne_registration_state, updated_at)
          VALUES (?, ?, 'draft', 'smoke', ?, 1, 'not_requested', '2099-01-01T00:00:00.000Z')
        `).run(`SET-FILL-${i}`, `未要求の新しいセット ${i}`, parentId).lastInsertRowid));
      }
      const capped = wfp.neRegistrationRows(db, { limit: 3 });
      check('要対応ビュー: 上限で切っても「要対応」が残る (並びは SQL 側)',
        capped.some((r) => r.id === neId) && capped[0].state === 'needs_action',
        capped.map((r) => `${r.state}:${r.id}`).join(','));
      for (const id of filler) db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
    }

    // 表示のついでの reconcile は「確定しうる行」だけに通す (Codex R1 medium)。
    // 仮コードのまま (NE に無い) セットを毎回照合すると、1画面の GET が数千SQLになる
    {
      let called = 0;
      const spy = (dbx, d) => { called += 1; return sd.reconcileProvisionalCode(dbx, d); };
      wfp.neRegistrationRows(db, { reconcile: spy });
      check('要対応ビュー: NE に無いコードは照合しない (画面が同期SQLで詰まらない)',
        called === 0, `called=${called}`);
    }

    // 🚨 確定の取り込みは表示の LIMIT と切り離す (Codex R2 medium)。
    // ① 表示件数に穴が空かない ② 表示対象の外 (古い行) も拾われる
    {
      const olds = [];
      for (let i = 0; i < 3; i++) {
        olds.push(Number(db.prepare(`
          INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code,
            ne_registration_state, updated_at)
          VALUES (?, ?, 'draft', 'smoke', ?, 1, 'requested', '2000-01-01T00:00:00.000Z')
        `).run(`WF-OLD-CONF-${i}`, `古い確定待ちセット ${i}`, parentId).lastInsertRowid));
      }
      for (const id of olds) wfp.ensureProgress(db, id);
      // 3件のうち1件だけ NE に載せる (残り2件は仮のまま = 表に残るべき)
      db.prepare(`
        INSERT INTO mirror_products
          (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
        VALUES (99403, 'WF-OLD-CONF-0', '古い確定待ち 実コード', '1', '取扱中', '1', 'WF-OLD-CONF-0', '2026-09-04T00:00:00Z')
      `).run();
      // 新しい未要求で表示上限を埋めても、古い行の確定は拾われる
      const capped = wfp.neRegistrationRows(db, { reconcile: sd.reconcileProvisionalCode, limit: 2 });
      check('要対応ビュー: 表示の上限に関わらず古い行の確定も拾う',
        db.prepare('SELECT provisional_code FROM product_drafts WHERE id = ?').get(olds[0]).provisional_code === 0,
        '古い行が確定していない');
      check('要対応ビュー: 確定した行が抜けても上限まで行が埋まる (穴が空かない)',
        capped.length === 2 && !capped.some((r) => r.id === olds[0]),
        `len=${capped.length} ids=${capped.map((r) => r.id).join(',')}`);
      check('要対応ビュー: 確定した行は NE登録の工程も閉じている',
        db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`)
          .get(olds[0])?.state === 'done');
      db.prepare('DELETE FROM mirror_products WHERE product_id = 99403').run();
      for (const id of olds) db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
    }

    // ボードを開いたときも同じように拾う (§4.3)。表示は updated_at DESC の上限で切られるので、
    // 古いセットは表示対象に入らない — それでも確定は進む必要がある (Codex R2 medium)
    {
      const oldId = Number(db.prepare(`
        INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code,
          ne_registration_state, updated_at)
        VALUES ('WF-BOARD-CONF', 'ボードから確定するセット', 'draft', 'smoke', ?, 1, 'requested', '2000-01-01T00:00:00.000Z')
      `).run(parentId).lastInsertRowid);
      wfp.ensureProgress(db, oldId);
      db.prepare(`
        INSERT INTO mirror_products
          (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
        VALUES (99404, 'WF-BOARD-CONF', 'ボード確定 実コード', '1', '取扱中', '1', 'WF-BOARD-CONF', '2026-09-04T00:00:00Z')
      `).run();
      // limit=1 = この古い行は表示対象に入らない。それでも確定は進む
      // (確定すると updated_at が今になるので、結果としてボードにも載る = 取り込みが先に走った証拠)
      wfp.boardData(db, { view: 'main', limit: 1, reconcileSet: sd.reconcileProvisionalCode });
      check('ボード: 表示の上限に入っていなくても、NEに載ったセットは確定させる',
        db.prepare('SELECT provisional_code FROM product_drafts WHERE id = ?').get(oldId).provisional_code === 0,
        '確定していない');
      check('ボード: 確定したら NE登録の工程も閉じている',
        db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`)
          .get(oldId)?.state === 'done');
      db.prepare('DELETE FROM mirror_products WHERE product_id = 99404').run();
      db.prepare('DELETE FROM product_drafts WHERE id = ?').run(oldId);
    }

    // 🚨 確定できない行が上限を食い潰して、後ろの行が永久に処理されないことがあってはいけない
    // (Codex R3 medium)。バリエーションから外した (detached) コードは NE に載っていても
    // reconcile が必ず false を返すので、候補に残すと毎回それが先頭を占める
    {
      // 確定できない行を先に (古い updated_at で) 置き、そのあとに確定できる行を置く
      const stuck = Number(db.prepare(`
        INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code, updated_at)
        VALUES ('WF-DETACHED-CONF', '外してあって確定できないセット', 'draft', 'smoke', ?, 1, '1999-01-01T00:00:00.000Z')
      `).run(parentId).lastInsertRowid);
      const okId = Number(db.prepare(`
        INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code, updated_at)
        VALUES ('WF-AFTER-STUCK', '確定できない行の後ろにあるセット', 'draft', 'smoke', ?, 1, '1999-01-02T00:00:00.000Z')
      `).run(parentId).lastInsertRowid);
      wfp.ensureProgress(db, stuck); wfp.ensureProgress(db, okId);
      const insM = db.prepare(`
        INSERT INTO mirror_products
          (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
        VALUES (?, ?, ?, '1', '取扱中', '1', ?, '2026-09-04T00:00:00Z')
      `);
      insM.run(99405, 'WF-DETACHED-CONF', '外した商品', 'WF-DETACHED-CONF');
      insM.run(99406, 'WF-AFTER-STUCK', '後ろの行', 'WF-AFTER-STUCK');
      db.prepare(`INSERT INTO draft_variation_exclusions (draft_id, ne_code, actor) VALUES (?, 'WF-DETACHED-CONF', 'smoke')`)
        .run(stuck);
      // 上限1でも、確定できない行は候補に入らないので後ろの行が処理される
      wfp.reconcileConfirmableSets(db, sd.reconcileProvisionalCode, { max: 1 });
      check('確定できない行が上限を食い潰さない (後ろの行が永久に止まらない)',
        db.prepare('SELECT provisional_code FROM product_drafts WHERE id = ?').get(okId).provisional_code === 0,
        '後ろの行が処理されていない');
      check('バリエーションから外したコードは確定させない',
        db.prepare('SELECT provisional_code FROM product_drafts WHERE id = ?').get(stuck).provisional_code === 1);
      db.prepare(`DELETE FROM draft_variation_exclusions WHERE ne_code = 'WF-DETACHED-CONF'`).run();
      for (const pid of [99405, 99406]) db.prepare('DELETE FROM mirror_products WHERE product_id = ?').run(pid);
      for (const id of [stuck, okId]) db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
    }

    // 確定したものは表から消える (工程ビューで見る)
    db.prepare('UPDATE product_drafts SET provisional_code = 0 WHERE id = ?').run(neId);
    check('要対応ビュー: 本コードが確定したら消える',
      !wfp.neRegistrationRows(db).some((r) => r.id === neId));
    db.prepare('UPDATE product_drafts SET provisional_code = 1 WHERE id = ?').run(neId);

    // 表示のついでに取り込みを追いかける (mirror は毎時なので画面を開くだけで追いつく)
    db.prepare(`UPDATE product_drafts SET ne_code = 'WF-NE-REAL' WHERE id = ?`).run(neId);
    db.prepare(`
      INSERT INTO mirror_products
        (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
      VALUES (99402, 'WF-NE-REAL', 'NE進み検証 実コード', '1', '取扱中', '1', 'WF-NE-REAL', '2026-09-04T00:00:00Z')
    `).run();
    const afterReconcile = wfp.neRegistrationRows(db, { reconcile: sd.reconcileProvisionalCode });
    check('要対応ビュー: 表示のついでに NE の取り込みを拾って表から落とす',
      !afterReconcile.some((r) => r.id === neId)
      && db.prepare('SELECT provisional_code FROM product_drafts WHERE id = ?').get(neId).provisional_code === 0);
    check('要対応ビュー: 拾ったら NE登録の工程も閉じている',
      db.prepare(`SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'set_ne_register'`)
        .get(neId)?.state === 'done');
    db.prepare('DELETE FROM mirror_products WHERE product_id = 99402').run();
  }

  // ⑩ 親の更新の知らせ (§4.5)。自動追随はしない — 見て判断してもらう
  {
    const chId = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code, parent_snapshot_at)
      VALUES ('SET-WF-TRK-P-96', '親更新検証のセット', 'draft', 'smoke', ?, 1, ?)
    `).run(parentId, '2026-09-01T00:00:00.000Z').lastInsertRowid);
    db.prepare(`UPDATE product_drafts SET updated_at = '2026-09-02T00:00:00.000Z' WHERE id = ?`).run(parentId);
    check('親更新: 派生後に親が変わったら知らせる', sd.setInfoOf(db, chId).parentChanged === true);
    check('親更新: 「確認した」で覚え直す', sd.ackParentSnapshot(db, chId, 'smoke') === true);
    check('親更新: 確認したら知らせは消える', sd.setInfoOf(db, chId).parentChanged === false);
    check('親更新: 単品には効かない', sd.ackParentSnapshot(db, parentId, 'smoke') === false);
    check('親更新: 履歴に残る',
      db.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'parent_snapshot_ack'`).get(chId).c === 1);
    // 自動追随はしない = 親の値がセットに入ってこない
    check('親更新: 親の値をセットに写さない (自動追随しない)',
      db.prepare('SELECT price FROM product_drafts WHERE id = ?').get(chId).price == null);
  }

  // 移行先が既にある場合 (先行デプロイ・自己修復で todo だけ作られた)。
  // 決着している方を採らないと、進んでいた商品が未着手に巻き戻る (Codex R1 medium)
  const halfId = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code)
    VALUES ('SET-WF-TRK-P-98', '途中まで移行した旧セット', 'draft', 'smoke', ?, 1)
  `).run(parentId).lastInsertRowid);
  db.prepare('DELETE FROM draft_step_progress WHERE draft_id = ?').run(halfId);
  for (const [code, state] of [['set_compose', 'todo'], ['set_content', 'todo'],
    ['basic_info', 'done'], ['ai_generate', 'done'], ['desc_review', 'done'], ['listing', 'todo']]) {
    legacyIns.run(halfId, code, state, null);
  }
  dbmod.migrateSetDraftsToSetTrack(db);
  const half = Object.fromEntries(db.prepare(
    'SELECT step_code, state FROM draft_step_progress WHERE draft_id = ?').all(halfId).map((r) => [r.step_code, r.state]));
  check('移行: 既に todo で作られていた set 工程にも旧の done を写す (巻き戻さない)',
    half.set_compose === 'done' && half.set_content === 'done', JSON.stringify(half));
  check('移行: 途中まででも旧の本流工程は消える', !half.basic_info && !half.desc_review, JSON.stringify(half));

  // 一度移行したあとに旧コードが動いて main 行が戻っても、次の起動で直る (Codex R1 medium)。
  // イベントの有無で冪等を決めると、混ざったまま二度と直らない
  legacyIns.run(legacyId, 'basic_info', 'todo', null);
  check('移行: 移行済みでも旧行が残っていれば直す (自己修復)',
    dbmod.migrateSetDraftsToSetTrack(db) === 1
    && !db.prepare(`SELECT 1 FROM draft_step_progress WHERE draft_id = ? AND step_code = 'basic_info'`).get(legacyId));
  check('移行: 直したあとは何もしない', dbmod.migrateSetDraftsToSetTrack(db) === 0);

  // ⑧ 移行が**起動時に走る**こと。ここを呼び忘れると、本番に既にあるセットは旧工程のまま残り、
  //    ボードで列が二重に並ぶ (関数を直接呼ぶテストだけでは呼び忘れに気づけない)。
  //    初期化は 1 プロセス 1 回きり (initialized フラグ) なので、別プロセスで作った DB で確かめる
  {
    const bootDir = path.join(process.env.DATA_DIR, 'boot-migration');
    fs.rmSync(bootDir, { recursive: true, force: true });
    fs.mkdirSync(bootDir, { recursive: true });
    const script = [
      "process.env.DATA_DIR = process.argv[2];",
      "const mm = await import('../../warehouse-mirror/db.js');",
      "const m = await import('../db.js');",
      "mm.initMirrorDB(); const db = m.initProductHubDB();",
      // 旧形式 (本流工程を持つセット) を仕込む
      "const pid = db.prepare(`INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('BOOT-P','親','draft','smoke')`).run().lastInsertRowid;",
      "const sid = db.prepare(`INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code) VALUES ('BOOT-SET','セット','draft','smoke',?,1)`).run(pid).lastInsertRowid;",
      "db.prepare('DELETE FROM draft_step_progress WHERE draft_id = ?').run(sid);",
      "const ins = db.prepare('INSERT INTO draft_step_progress (draft_id, step_code, state) VALUES (?,?,?)');",
      "for (const c of ['basic_info','ai_generate','desc_review','title_approve','set_review','listing']) ins.run(sid, c, 'done');",
      // 画像の計画 (§4.7) も起動時に埋まること。親に画像を置き、セットの計画は消しておく
      "db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'boot-top', 0)`).run(pid);",
      "db.prepare('DELETE FROM draft_set_image_plans WHERE set_draft_id = ?').run(sid);",
      "db.close();",
      "console.log(JSON.stringify({ sid }));",
    ].join('\n');
    const boot = [
      "process.env.DATA_DIR = process.argv[2];",
      "const mm = await import('../../warehouse-mirror/db.js');",
      "const m = await import('../db.js');",
      "mm.initMirrorDB(); const db = m.initProductHubDB();",   // ← ここで移行が走るはず
      "const sid2 = db.prepare(`SELECT id FROM product_drafts WHERE ne_code = 'BOOT-SET'`).get().id;",
      "const rows = db.prepare(`SELECT p.step_code, s.track FROM draft_step_progress p JOIN ph_steps s ON s.code = p.step_code WHERE p.draft_id = ?`).all(sid2);",
      "const plans = db.prepare('SELECT slot, action FROM draft_set_image_plans WHERE set_draft_id = ?').all(sid2);",
      "console.log(JSON.stringify({ steps: rows.map((r) => r.step_code + ':' + r.track), plans: plans }));",
    ].join('\n');
    const runNode = (src) => {
      const f = path.join(bootDir, 'run.mjs');
      // db.js を相対 import するので scripts/ に置く (import 元と同じ深さ)
      const target = path.join(__dirname, `.boot-${Date.now()}.mjs`);
      fs.writeFileSync(target, src);
      try {
        return execFileSync(process.execPath, [target, bootDir], { encoding: 'utf-8' })
          .trim().split('\n').pop();
      } finally { fs.rmSync(target, { force: true }); fs.rmSync(f, { force: true }); }
    };
    runNode(script);
    const after = JSON.parse(runNode(boot));
    check('移行: 起動時の初期化で走る (呼び忘れるとボードの列が二重に並ぶ)',
      after.steps.some((x) => x.startsWith('set_compose:set')) && after.steps.includes('listing:main')
      && !after.steps.some((x) => x.startsWith('basic_info:')), JSON.stringify(after.steps));
    // 🚨 画像の計画も**初期化の中で同期的に**埋まること。待たずに始めると、再起動直後の
    //    最初のリクエストが出品だったとき計画 0 件で画像のゲートを素通りする (Codex R5 high)
    check('画像の計画: 起動時の初期化で埋まる (待たずに始めると出品ゲートを素通りする)',
      after.plans.length === 1 && after.plans[0].slot === 1 && after.plans[0].action === 'reuse',
      JSON.stringify(after.plans));
  }
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
    // 2026-08-31: TOP の工程は廃止。2026-09-01: TOP は「枠1 が登録されているか」でカードに出す
    check('ボード: カードに詳細のステッパー (v2 の 10 点) と TOP の登録状況が付く',
      card.image?.top?.registered === false && card.image?.detail?.steps?.length === 10,
      JSON.stringify(card.image || null).slice(0, 120));

    // TOP は枠1 (sort=0) だけを見る = 出品ゲート imageTrackBlockReason と同じ判定。
    // _01 だけ取り込まれた商品 (sort=1〜) を「済」にしてはいけない
    db.prepare("INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'smoke-img-01', 1)").run(wfDraftId);
    const cardOnly01 = wfp.boardData(db, {}).columns.flatMap((c) => c.cards).find((x) => x.id === wfDraftId);
    check('ボード: 枠1 以外の画像だけでは TOP は「済」にしない',
      cardOnly01.image?.top?.registered === false, JSON.stringify(cardOnly01.image?.top || null));
    db.prepare("INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'smoke-img-top', 0)").run(wfDraftId);
    const cardWithTop = wfp.boardData(db, {}).columns.flatMap((c) => c.cards).find((x) => x.id === wfDraftId);
    check('ボード: 枠1 (_top) が登録されると TOP が「済」になる',
      cardWithTop.image?.top?.registered === true, JSON.stringify(cardWithTop.image?.top || null));
    db.prepare("DELETE FROM draft_images WHERE draft_id = ? AND drive_file_id IN ('smoke-img-01','smoke-img-top')").run(wfDraftId);

    // 詳細画像は「作り終わったか」で 済/まだ を出す (2026-09-01 中原さん要望)。
    // ⑧楽天登録・⑨A+登録 は作った画像をモールに載せる後工程なので、そこに来ていれば made=true。
    // ⑦Amazon登録依頼 は最終デザイン確認を兼ねるので、まだ made ではない。
    // 他のボードテストの前提 (どの商品がどの列にいるか) を崩さないよう、専用の商品で試して消す
    {
      const madeId = Number(db.prepare(
        "INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-MADE', '画像 済 判定テスト', 'draft', 'smoke')"
      ).run().lastInsertRowid);
      wfp.ensureProgress(db, madeId);
      const madeOf = () => wfp.boardData(db, {}).columns.flatMap((c) => c.cards)
        .find((x) => x.id === madeId)?.image?.detail;
      check('ボード: 制作の途中は「まだ」 (made=false)',
        madeOf()?.made === false, JSON.stringify(madeOf()?.current?.step_code || null));
      for (const code of ['imgd_request', 'imgd_compose', 'imgd_material', 'imgd_ai', 'imgd_design', 'imgd_review_1', 'imgd_review_2']) {
        wfp.setStepState(madeId, code, { state: 'done' }, 'smoke', ADMIN);
      }
      check('ボード: ⑦Amazon登録依頼 で止まっている間は「まだ」',
        madeOf()?.made === false && madeOf()?.current?.step_code === 'imgd_amazon',
        JSON.stringify(madeOf()?.current?.step_code || null));
      wfp.setStepState(madeId, 'imgd_amazon', { state: 'done' }, 'smoke', ADMIN);
      check('ボード: ⑧楽天登録に移ったら「済」 (画像は作り終わっている・工程はまだ残る)',
        madeOf()?.made === true && madeOf()?.done === false
        && madeOf()?.current?.step_code === 'imgd_rakuten',
        JSON.stringify(madeOf()?.current?.step_code || null));
      check('ボード: 詳細画像が対象外の商品は made にしない (対象外のまま)', (() => {
        wfp.setDetailImagesExcluded(madeId, true, 'smoke', ADMIN);
        const t = madeOf();
        wfp.setDetailImagesExcluded(madeId, false, 'smoke', ADMIN);
        return t?.excluded === true && t?.made === false;
      })());
      db.prepare('DELETE FROM product_drafts WHERE id = ?').run(madeId);

      // 本番の構成の 済/まだ (2026-09-13 スタッフ要望)。縦列 ②仮構成 とは別の印で持ち、
      // 印が無くても ③素材待ちが決着していれば 済 とみなす (既存カードを軒並み まだ にしない)
      {
        const cId = Number(db.prepare(
          "INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-COMPOSE', '構成 済 判定テスト', 'draft', 'smoke')"
        ).run().lastInsertRowid);
        wfp.ensureProgress(db, cId);
        const composeOf = () => wfp.boardData(db, {}).columns.flatMap((c) => c.cards)
          .find((x) => x.id === cId)?.image?.compose;
        check('ボード 構成: 印が無く ③素材待ち も終わっていなければ「まだ」', composeOf()?.done === false, JSON.stringify(composeOf()));
        for (const code of ['imgd_request', 'imgd_compose']) wfp.setStepState(cId, code, { state: 'done' }, 'smoke', ADMIN);
        check('ボード 構成: ②仮構成 が終わっても、本番の構成の印が無ければ「まだ」',
          composeOf()?.done === false, JSON.stringify(composeOf()));
        db.prepare("INSERT INTO draft_image_production (draft_id, compose_status) VALUES (?, 'done')").run(cId);
        check('ボード 構成: 済にすると ③素材待ちの列にいても「済」',
          composeOf()?.done === true && composeOf()?.marked === true && composeOf()?.implied === false, JSON.stringify(composeOf()));
        db.prepare('UPDATE draft_image_production SET compose_status = NULL WHERE draft_id = ?').run(cId);
        wfp.setStepState(cId, 'imgd_material', { state: 'done' }, 'smoke', ADMIN);
        check('ボード 構成: 人が決めていなくても ③素材待ちが決着していれば「済」とみなす',
          composeOf()?.done === true && composeOf()?.implied === true && composeOf()?.marked === false, JSON.stringify(composeOf()));
        // Codex R1: 推定の 済 でも人が「まだ」にしたらそちらが勝つ (戻せないと誤操作を直せない)
        db.prepare("UPDATE draft_image_production SET compose_status = 'todo' WHERE draft_id = ?").run(cId);
        check('ボード 構成: 人が「まだ」にしたら ③素材待ちが済んでいても「まだ」 (推定より人の値)',
          composeOf()?.done === false && composeOf()?.marked === true && composeOf()?.implied === false, JSON.stringify(composeOf()));
        db.prepare('UPDATE draft_image_production SET compose_status = NULL WHERE draft_id = ?').run(cId);
        check('ボード 構成: 詳細画像が対象外なら 対象外 (済にしない)', (() => {
          wfp.setDetailImagesExcluded(cId, true, 'smoke', ADMIN);
          const t = composeOf();
          wfp.setDetailImagesExcluded(cId, false, 'smoke', ADMIN);
          return t?.excluded === true && t?.done === false;
        })());
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(cId);
      }

      // 順序が飛ぶケース (Codex R2 medium)。「いまの工程が楽天登録か」で見ると、
      // ⑦を対象外にした / 工程を並べ替えた だけで判定が崩れる
      const mk2 = (code) => {
        const id = Number(db.prepare(
          `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('${code}', '画像 済 判定 ${code}', 'draft', 'smoke')`
        ).run().lastInsertRowid);
        wfp.ensureProgress(db, id);
        for (const c of ['imgd_request', 'imgd_compose', 'imgd_material', 'imgd_ai', 'imgd_design', 'imgd_review_1', 'imgd_review_2']) {
          wfp.setStepState(id, c, { state: 'done' }, 'smoke', ADMIN);
        }
        return id;
      };
      const detailOf = (id) => wfp.boardData(db, {}).columns.flatMap((c) => c.cards)
        .find((x) => x.id === id)?.image?.detail;
      {
        // ⑦Amazon登録依頼 を「対象外」= Amazon に出さない商品。決着なので「済」
        const id = mk2('WF-MADE-SKIP7');
        wfp.setStepState(id, 'imgd_amazon', { state: 'skip' }, 'smoke', ADMIN);
        check('ボード: ⑦を対象外にしても「済」 (skip も決着)',
          detailOf(id)?.made === true, JSON.stringify(detailOf(id)?.current?.step_code || null));
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
      }
      {
        // 管理画面で ⑧楽天登録 を ⑥より前へ動かした状態。「いま楽天登録にいる」だけを見ると
        // ⑥⑦が終わっていないのに「済」になってしまう
        const id = mk2('WF-MADE-REORDER');
        wfp.setStepState(id, 'imgd_review_2', { state: 'todo' }, 'smoke', ADMIN);
        const origSort = db.prepare("SELECT sort FROM ph_steps WHERE code = 'imgd_rakuten'").get().sort;
        db.prepare("UPDATE ph_steps SET sort = 55 WHERE code = 'imgd_rakuten'").run();
        const t = detailOf(id);
        db.prepare('UPDATE ph_steps SET sort = ? WHERE code = ?').run(origSort, 'imgd_rakuten');
        check('ボード: 工程を並べ替えて楽天登録が先に来ても、前の工程が残っていれば「まだ」',
          t?.made === false && t?.current?.step_code === 'imgd_rakuten',
          JSON.stringify({ made: t?.made, cur: t?.current?.step_code }));
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
      }
      {
        // 境界工程 ⑦ そのものを ⑥ より前へ動かした状態 (Codex R3 medium)。
        // 「境界より前の行だけ見る」判定だと、後ろへ回った ⑥ が todo でも「済」になってしまう
        const id = mk2('WF-MADE-BOUNDARY-FIRST');
        wfp.setStepState(id, 'imgd_review_2', { state: 'todo' }, 'smoke', ADMIN);
        wfp.setStepState(id, 'imgd_amazon', { state: 'done' }, 'smoke', ADMIN);
        const origSort = db.prepare("SELECT sort FROM ph_steps WHERE code = 'imgd_amazon'").get().sort;
        db.prepare("UPDATE ph_steps SET sort = 5 WHERE code = 'imgd_amazon'").run();
        const t = detailOf(id);
        db.prepare('UPDATE ph_steps SET sort = ? WHERE code = ?').run(origSort, 'imgd_amazon');
        check('ボード: ⑦を先頭へ動かしても、後ろに残った ⑥ が未完了なら「まだ」',
          t?.made === false, JSON.stringify({ made: t?.made, cur: t?.current?.step_code }));
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
      }
      {
        // 境界工程 ⑦ を無効化した状態 (工程を消した)。決着の確認ができないので「済」と偽らない
        const id = mk2('WF-MADE-NO-BOUNDARY');
        db.prepare("UPDATE ph_steps SET active = 0 WHERE code = 'imgd_amazon'").run();
        const t = detailOf(id);
        db.prepare("UPDATE ph_steps SET active = 1 WHERE code = 'imgd_amazon'").run();
        check('ボード: 境界工程 (⑦) が無いときは「済」にしない',
          t?.made === false && t?.done === false, JSON.stringify({ made: t?.made, done: t?.done }));
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
      }
      {
        // 管理画面から画像工程を足した状態。**実際の作成経路 (createStep) を通す** (Codex R4 medium):
        // 直接 INSERT で listing_gate=0 を指定すると、既定値 1 で作られる本番と違う前提を固定してしまう。
        // 既定 1 = 「制作に必要な工程」扱い → 終わるまで「まだ」(fail-closed。楽天出品ゲートも同じ扱い)
        const id = mk2('WF-MADE-CUSTOM');
        const customCode = wf.createStep({ label: '追加工程 (smoke)', track: 'image', image_kind: 'detail' });
        wfp.ensureProgress(db, id);
        wfp.setStepState(id, 'imgd_amazon', { state: 'done' }, 'smoke', ADMIN);
        const tBefore = detailOf(id);
        wfp.setStepState(id, customCode, { state: 'done' }, 'smoke', ADMIN);
        const tAfter = detailOf(id);
        db.prepare('DELETE FROM draft_step_progress WHERE step_code = ?').run(customCode);
        db.prepare('DELETE FROM ph_steps WHERE code = ?').run(customCode);
        check('ボード: 管理画面で足した画像工程が未完了なら「まだ」 (既定で制作に数える)',
          tBefore?.made === false, JSON.stringify({ made: tBefore?.made, cur: tBefore?.current?.step_code }));
        check('ボード: その工程が終われば「済」',
          tAfter?.made === true, JSON.stringify({ made: tAfter?.made, cur: tAfter?.current?.step_code }));
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
      }
    }
  }

  // 画像ビュー: 列は商品詳細 (LP) の 10 段階。カードは 1 商品 1 枚 (2026-08-31 TOP工程の廃止)
  const bi = wfp.boardData(db, { view: 'image' });
  check('ボード(画像): 9列 (詳細 v2 の段階。⑥-1/⑥-2 は同じ review 列)',
    bi.view === 'image' && bi.columns.length === 9
    && bi.columns.map((c) => c.key).join(',') === 'request,compose,material,ai,design,review,amazon,rakuten,aplus',
    `cols=${bi.columns.map((c) => c.key).join(',')}`);
  {
    const kindsOf = (id) => bi.columns.flatMap((c) => c.cards.filter((x) => x.id === id).map((x) => x.kind));
    // 2026-08-31: TOP の工程を廃止し、カードは 1 商品 1 枚になった
    // (「実際の制作件数がぱっと見で分かりにくい」— 中原さん)
    check('ボード(画像): カードは 1 商品 1 枚 (TOP と詳細で分かれない)',
      kindsOf(wfDraftId).join(',') === 'detail', kindsOf(wfDraftId).join(','));
    // 詳細対象外の商品は詳細カードを出さない
    const idNoDetail = Number(db.prepare(
      `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES ('WF-BOARD-EX', '詳細なし', 'review', 'smoke')`
    ).run().lastInsertRowid);
    wfp.progressOf(idNoDetail, { db });
    wfp.setDetailImagesExcluded(idNoDetail, true, 'admin', ADMIN);
    const bi2 = wfp.boardData(db, { view: 'image' });
    const kinds2 = bi2.columns.flatMap((c) => c.cards.filter((x) => x.id === idNoDetail).map((x) => x.kind));
    // 2026-08-31: 画像の工程は詳細 (LP) だけ。詳細を対象外にした商品は画像の作業が無いので
    // 画像ボードには出ない (TOP画像そのものは出品ゲートが画像の登録で見る)
    check('ボード(画像): 詳細対象外の商品はカードが出ない (画像の作業が無い)',
      kinds2.length === 0, kinds2.join(','));
    // TOP全skip (ガードすり抜け) を完了列に見せない — 出品ゲートは拒否するので表示と食い違う
    wfp.setDetailImagesExcluded(idNoDetail, true, 'admin', ADMIN);
    const bi3 = wfp.boardData(db, { view: 'image' });
    check('ボード(画像): 詳細対象外は完了列にも出さない',
      !bi3.doneCards.some((c) => c.id === idNoDetail)
      && !bi3.columns.some((c) => c.cards.some((x) => x.id === idNoDetail)));
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
  // TOP 工程は 2026-08-31 に廃止したので、分割移行は写す先が無い (行は作られない)。
  // 旧一本トラックの進捗行は残る = 履歴として読める
  check('移行: 旧一本トラックの進捗は残る (TOP工程は廃止したので写し先は無い)',
    st(idHuman, 'img_request') === 'done' && st(idOpen, 'img_request') === 'doing',
    `${st(idHuman, 'img_request')} / ${st(idOpen, 'img_request')}`);
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

  // AI待ちは一般ユーザーも跨げる (2026-09-25 スタッフ要望) が、その先に他人の担当工程があれば
  // 全体ロールバック = 先行した basic_info・AI待ちも元に戻る (中途半端に進めない)
  db.prepare(`UPDATE draft_step_progress SET assignee_id = ? WHERE draft_id = ? AND step_code = 'desc_review'`).run(wfOkawaId, idM5);
  let dndSys = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'title_approve', expectedCurrent: 'basic_info' }, 'tanaka', TANAKA); } catch (e) { dndSys = e; }
  check('D&D: AI待ちを跨いだ先で他人の担当に当たったら 403 + basic_info・AI待ちもロールバック',
    dndSys?.status === 403 && stepOf(idM5, 'basic_info') === 'todo' && assigneeOf(idM5, 'basic_info') == null
    && stepOf(idM5, 'ai_generate') === 'todo', dndSys?.message || '例外が出ていない');
  db.prepare(`UPDATE draft_step_progress SET assignee_id = NULL WHERE draft_id = ? AND step_code = 'desc_review'`).run(idM5);
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

  // AI待ち → 商品説明確認: 一般ユーザーが AI を待たずに手で進められる (2026-09-25 スタッフ要望。
  // 既存ページへのカラバリ追加などで人が項目を入れた商品が、管理者に頼まないと進めなかった)
  const statusOfM5 = () => db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(idM5).status;
  check('前提: AI待ちの列 = ready_for_ai', statusOfM5() === 'ready_for_ai', statusOfM5());
  const evAi0 = eventsOf(idM5).length;
  dndClaim = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'desc_review', expectedCurrent: 'ai_generate' }, 'tanaka', TANAKA); } catch (e) { dndClaim = e; }
  check('D&D: 一般ユーザーが AI待ち → 商品説明確認 へ手で進められる',
    dndClaim === null && stepOf(idM5, 'ai_generate') === 'done' && statusOfM5() === 'review', dndClaim?.message || statusOfM5());
  check('D&D: AI待ちを手で進めても担当は付けない (システム工程のまま)', assigneeOf(idM5, 'ai_generate') == null);
  {
    // AI が生成中 (claim 済み) に人が手で進めたら、AI の結果は書き込ませない (Codex R1 要確認)。
    // 書き込み直前の再確認 acquireGenerationWriteLock が status=ready_for_ai を見るので拒否される
    db.prepare(`UPDATE product_drafts SET generation_claim_run_id = 'run-smoke-race',
      generation_claim_until = '2999-01-01T00:00:00Z' WHERE id = ?`).run(idM5);
    check('D&D: 生成中に手で進めた商品には AI の書き込み権を渡さない',
      dbmod.acquireGenerationWriteLock(db, idM5, 'run-smoke-race') === false);
    db.prepare('UPDATE product_drafts SET generation_claim_run_id = NULL, generation_claim_until = NULL WHERE id = ?').run(idM5);
  }
  check('D&D: AI待ちを手で進めたことがイベントで読み分けられる',
    eventsOf(idM5).slice(evAi0).some((e) => /AI情報入力待ち: .*完了 \(AI を待たずに手で進めた\)/.test(e)), JSON.stringify(eventsOf(idM5).slice(evAi0)));
  // 戻す (AI にもう一度書かせる) のも一般ユーザーができる = 夜間の AI キューに戻る
  dndClaim = null;
  try { wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'ai_generate', expectedCurrent: 'desc_review' }, 'tanaka', TANAKA); } catch (e) { dndClaim = e; }
  check('D&D: 一般ユーザーが AI待ちの列へ戻せる (ready_for_ai に戻る)',
    dndClaim === null && stepOf(idM5, 'ai_generate') === 'todo' && statusOfM5() === 'ready_for_ai', dndClaim?.message || statusOfM5());
  // 状態以外 (対象外・担当の付け替え) は従来どおり管理者だけ
  let aiSkip = null;
  try { wfpEarly.setStepState(idM5, 'ai_generate', { state: 'skip' }, 'tanaka', TANAKA); } catch (e) { aiSkip = e; }
  check('AI待ちの「対象外」は一般ユーザーにはできない', aiSkip?.status === 403, aiSkip?.message || '通ってしまった');
  let aiAssign = null;
  try { wfpEarly.setStepState(idM5, 'ai_generate', { state: 'done', assignee_id: wfTanakaId }, 'tanaka', TANAKA); } catch (e) { aiAssign = e; }
  check('AI待ちに担当を付けながら進めることは一般ユーザーにはできない', aiAssign?.status === 403 && stepOf(idM5, 'ai_generate') === 'todo', aiAssign?.message || '通ってしまった');
  wfpEarly.moveBoardCard(idM5, { view: 'main', to: 'desc_review', expectedCurrent: 'ai_generate' }, 'tanaka', TANAKA);

  // 商品説明確認 → セット検討 へ (title_approve を跨ぐ)
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

  // 画像ビュー: 依頼 → 素材待ち へ (依頼・構成が done)、完了列で残りをまとめて閉じる。
  // 2026-08-31: TOP の 4 工程は廃止したので、画像の工程は商品詳細 (LP) の 10 段階だけ
  const idM3 = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-MOVE3', '画像D&D', 'smoke')
  `).run().lastInsertRowid);
  wfpEarly.ensureProgress(db, idM3);
  wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'detail', to: 'material', expectedCurrent: 'imgd_request' }, 'smoke', ADMIN2);
  check('D&D 画像ビュー前方: 段階キーで移動できる',
    stepOf(idM3, 'imgd_request') === 'done' && stepOf(idM3, 'imgd_compose') === 'done'
    && stepOf(idM3, 'imgd_material') === 'todo');
  // さらに前へ: 途中 (AI制作) もまとめて done になる
  wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'detail', to: 'design', expectedCurrent: 'imgd_material' }, 'smoke', ADMIN2);
  check('D&D 画像ビュー: 飛ばした工程がまとめて done',
    stepOf(idM3, 'imgd_material') === 'done' && stepOf(idM3, 'imgd_ai') === 'done'
    && stepOf(idM3, 'imgd_design') === 'todo');
  // 後ろへ戻す (差し戻し)
  wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'detail', to: 'ai', expectedCurrent: 'imgd_design' }, 'smoke', ADMIN2);
  check('D&D 画像ビュー: 後ろへ戻すとその工程が開き直る', stepOf(idM3, 'imgd_ai') === 'todo');
  let moveCas = null;
  try {
    wfpEarly.moveBoardCard(idM3, { view: 'image', kind: 'detail', to: 'compose', expectedCurrent: 'imgd_aplus' }, 'smoke', ADMIN2);
  } catch (e) { moveCas = e; }
  check('D&D の CAS: 掴んだ時点の工程と違えば 409', moveCas?.status === 409, moveCas?.message || '例外が出ていない');

  // カードは 1 商品 1 枚 (2026-08-31 TOP工程の廃止。「制作件数がぱっと見で分かりにくい」の解消)
  const ib = wfpEarly.boardData(db, { view: 'image' });
  check('画像ビュー: カードは 1 商品 1 枚 (TOP/詳細で分かれない)',
    ib.columns.reduce((n, c) => n + c.cards.filter((x) => x.id === idM3).length, 0) === 1,
    JSON.stringify(ib.columns.flatMap((c) => c.cards.filter((x) => x.id === idM3).map((x) => x.kind))));

  // 種別の絞り込み (TOP/詳細) は 2026-08-31 に廃止。カードが 1 種類しかないので選ぶ意味がない。
  // 詳細対象外の商品はカードにならない (画像の作業が無い) ことだけ確かめる
  const imgTotalBefore = wfpEarly.boardData(db, { view: 'image' }).total;
  const mainTotalBefore = wfpEarly.boardData(db, { view: 'main' }).total;
  const idKfx = Number(db.prepare(`
    INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-KFEX', '詳細対象外の絞り込み', 'smoke')
  `).run().lastInsertRowid);
  wfpEarly.ensureProgress(db, idKfx);
  db.prepare('UPDATE product_drafts SET detail_images_excluded = 1 WHERE id = ?').run(idKfx);
  const ibEx = wfpEarly.boardData(db, { view: 'image' });
  check('画像ビュー: 詳細対象外の商品はカードにならない (画像の作業が無い)',
    !ibEx.columns.some((c) => c.cards.some((x) => x.id === idKfx))
    && !ibEx.doneCards.some((x) => x.id === idKfx));
  // 候補 (LIMIT) も食わないこと (Codex R1 medium: 食うと実際に作業がある商品がボードから欠ける)。
  // **挿入の前後**で比べる — 同条件で 2 回取って比べても恒真にしかならない (Codex R2 low)
  check('画像ビュー: 詳細対象外は候補 (total) を食わない (本流では数える)',
    ibEx.total === imgTotalBefore && wfpEarly.boardData(db, { view: 'main' }).total === mainTotalBefore + 1,
    `image ${imgTotalBefore}→${ibEx.total} / main ${mainTotalBefore}→${wfpEarly.boardData(db, { view: 'main' }).total}`);
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
    // TOP画像は工程でなく画像の登録で見る (2026-08-31) ので、ゲートの検証用に 1 枚入れておく
    db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, 'v2-img-1', 0)`).run(idV2);
    const stV2 = (code) => db.prepare('SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = ?').get(idV2, code)?.state;
    // 2026-09-13 スタッフ要望: ① は情報入力 (撮影・素材 / 商品情報) が済んでいなくても完了できる
    // (ボードで ②仮構成 へ動かせず、撮影依頼のための仮構成を先に作れなかった)
    let reqErr = null; let reqDone = null;
    try { reqDone = wfpEarly.setStepState(idV2, 'imgd_request', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { reqErr = e; }
    check('v2: ① は撮影・素材も商品情報も未入力のまま完了できる (自社商品・v2 切替後の新規商品でも)',
      reqDone?.changed === true, reqErr?.message);
    // 以降のテストの前提 (素材待ちの判定など) は従来どおり
    dbmod.upsertImageProduction(db, idV2, { material_status: 'internal_prep', product_info_text: 'テスト商品の情報' });
    // D&D: 依頼 → 素材待ち (構成をまとめて done)
    wfpEarly.moveBoardCard(idV2, { view: 'image', kind: 'detail', to: 'material', expectedCurrent: 'imgd_compose' }, 'smoke', ADMIN2);
    check('v2: D&D で構成を飛ばして素材待ちへ', stV2('imgd_compose') === 'done' && stV2('imgd_material') === 'todo');
    let matErr = null;
    try { wfpEarly.setStepState(idV2, 'imgd_material', { state: 'done' }, 'smoke', ADMIN2); } catch (e) { matErr = e; }
    check('v2: ③ 素材待ちは 社内準備 のままでは完了できない', matErr?.status === 400 && /素材完了/.test(matErr.message), matErr?.message);
    dbmod.upsertImageProduction(db, idV2, { material_status: 'ready' });
    check('v2: 素材完了なら ③ 完了', wfpEarly.setStepState(idV2, 'imgd_material', { state: 'done' }, 'smoke', ADMIN2).changed === true);
    wfpEarly.setStepState(idV2, 'imgd_ai', { state: 'done' }, 'smoke', ADMIN2);
    // 2026-08-31: TOP の 4 工程は廃止したので、⑤ からの自動追随は無い
    wfpEarly.setStepState(idV2, 'imgd_design', { state: 'done' }, 'smoke', ADMIN2);
    check('v2: ⑤ デザイン修正 done', stV2('imgd_design') === 'done');
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
    check('v2: ⑥-2 done', stV2('imgd_review_2') === 'done');
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
    check('v2: 工程行の無い出品済み商品 (status=listed) は初回生成で詳細 v2 も全部 done', stM(idM9, 'imgd_aplus') === 'done');
    db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?, ?, ?)').run(idM6, idM7, idM8, idM9);
    check('v2 移行: 旧詳細工程は active=0',
      db.prepare(`SELECT COUNT(*) c FROM ph_steps WHERE code IN ('img_shoot_detail','img_request_detail') AND active = 1`).get().c === 0);
    for (const id of [idV2, idV2Rk, idM1, idM2, idM3, idM4, idM5]) db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }

  // ─── 裏面情報 + ChatGPT 定型文 (2026-09-10 スタッフ要望) ───
  {
    const pt = await import('../lib/prompt-templates.js');
    const idBI = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by, own_brand, asin) VALUES ('DRV-BI', '裏面テスト商品', 'smoke', 1, 'B00TEST123')
    `).run().lastInsertRowid);
    const draftBI = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(idBI);

    // 定型文 (lib 単体)
    check('定型文: 商品情報も裏面情報も空なら使えない', pt.buildPromptTemplates(draftBI, {}).available === false);
    check('定型文: 裏面情報だけでも使える (裏面情報は任意・単独可)',
      pt.buildPromptTemplates(draftBI, { back_info_text: '原材料：小麦' }).available === true);
    const tplBoth = pt.buildPromptTemplates(draftBI, { product_info_text: 'PC用商品説明文の本文', back_info_text: '原材料：小麦' });
    check('定型文 初動判定: @GPT名 / 参照仕様書URL / Amazon URL / 裏面情報 / 商品情報 が入る',
      tplBoth.initialJudge.startsWith('@新商品初動判定\n')
      && tplBoth.initialJudge.includes('/spreadsheets/d/1u2Qg2BTc34bBCqbaaA75FUNG5SXOrvupZqseqZQ2IB8/edit?gid=11001#gid=11001')
      && tplBoth.initialJudge.includes('Amazon商品URL：https://www.amazon.co.jp/dp/B00TEST123')
      && tplBoth.initialJudge.includes('■裏面情報 (パッケージ裏面の表記)\n原材料：小麦')
      && tplBoth.initialJudge.includes('■商品情報\nPC用商品説明文の本文'), tplBoth.initialJudge);
    check('定型文 初動判定: Ver 表記は残っていない (最新仕様を使う文面になった 2026-09-13)', !tplBoth.initialJudge.includes('Ver1.0'));
    check('定型文 初動判定: 商品画像は空行 (ChatGPT へ直接貼るので本文に入れない)', /\n商品画像：\n\n/.test(tplBoth.initialJudge));
    check('定型文 初動判定: LP制作管理シートURL の貼り付け位置と逆算の指示が入る',
      tplBoth.initialJudge.includes('LP制作管理シートURL：\n（ここに作成したLP制作管理シートのURLを貼り付け）')
      && tplBoth.initialJudge.includes('各画像に必要な素材を逆算してください。')
      && tplBoth.initialJudge.includes('既存素材で足りるもの／図解・AIで作れるもの／追加撮影が必要なものを判定し、'));
    check('定型文 初動判定: 質問だけで止めない指示が入る', tplBoth.initialJudge.includes('不足情報がある場合も質問だけで止めず、\n確認できる範囲で判定結果まで出力してください。'));
    // カラバリ (2026-09-13)。商品情報の見出しの後・LP制作管理シートの前に入る
    check('定型文 初動判定: バリエーション情報を渡さなければ「なし (単品)」',
      tplBoth.initialJudge.includes('■商品情報\nPC用商品説明文の本文\n\n■カラバリ\nなし (単品)\n\nLP制作管理シートURL：'), tplBoth.initialJudge);
    const vari3 = {
      variation: { kind: 'variation', members: [
        { 商品コード: 'DRV-BI-RED', 商品名: '裏面テスト商品 レッド' },
        { 商品コード: 'DRV-BI-BLU', 商品名: '裏面テスト商品 ブルー' },
        { 商品コード: 'DRV-BI-GRN', 商品名: '' },
      ] },
      hasVariation: { value: true },
      selectorName: ' カラー ',
      selectorValues: { 'drv-bi-red': 'レッド' },
    };
    check('定型文 カラバリ: 選択肢の値を優先し、未入力は NE 商品名 → 商品コードで埋める',
      pt.composeColorVariations(vari3) === '■カラバリ (カラー・全3種)\n・レッド\n・裏面テスト商品 ブルー\n・DRV-BI-GRN',
      pt.composeColorVariations(vari3));
    check('定型文 カラバリ: 項目名が未設定なら見出しは件数だけ',
      pt.composeColorVariations({ ...vari3, selectorName: '' }).startsWith('■カラバリ (全3種)\n'));
    check('定型文 カラバリ: NE 未登録でも手入力の「バリエーションあり」は伝える',
      pt.composeColorVariations({ variation: { kind: 'unknown', members: [] }, hasVariation: { value: true } })
        === '■カラバリ\nあり (NE 未登録のため内訳は未確認)');
    // 商品分析にも同じカラバリが入る (2026-09-13 商品分析の差し替え)
    const tplVari3 = pt.buildPromptTemplates(draftBI, { product_info_text: 'あ' }, vari3);
    check('定型文 カラバリ: 初動判定にも商品分析にも入る',
      tplVari3.initialJudge.includes('■カラバリ (カラー・全3種)\n・レッド')
      && tplVari3.productAnalysis.includes('■カラバリ (カラー・全3種)\n・レッド'), tplVari3.productAnalysis);
    check('定型文 商品分析: @GPT名 / 参照仕様書URL / 商品名 / 裏面情報 が入る',
      tplBoth.productAnalysis.startsWith('@LP制作システム\n')
      && tplBoth.productAnalysis.includes('/spreadsheets/d/1CGQXKtz4E4Il-jkzYO3QL9oi2PAdS51-s4rStulHdYc/')
      && tplBoth.productAnalysis.includes('商品名：裏面テスト商品')
      && tplBoth.productAnalysis.includes('■裏面情報 (パッケージ裏面の表記)\n原材料：小麦'), tplBoth.productAnalysis);
    check('定型文 商品分析: 商品情報 (カラバリ込み) → 商品画像 (空) → 【実行】の順',
      tplBoth.productAnalysis.includes('■商品情報\nPC用商品説明文の本文\n\n■カラバリ\nなし (単品)\n\n商品画像：\n\n\n【実行】'),
      tplBoth.productAnalysis);
    check('定型文 商品分析: ⑦だけ出力させる指示と「仕様書内の最新形式」に従わせる指示が入る',
      tplBoth.productAnalysis.includes('最終回答は必ず⑦ AI画像生成プロンプトのみを出力してください。')
      && tplBoth.productAnalysis.includes('出力形式は仕様書内の最新のAI画像生成プロンプト出力形式に従ってください。')
      && tplBoth.productAnalysis.includes('- 冒頭は「# LP制作システム」→「## ⑦ AI画像生成プロンプト」\n')
      && tplBoth.productAnalysis.includes('# 共通生成後チェック'), tplBoth.productAnalysis);
    // 版の固定指示が残ると、仕様書を更新しても ChatGPT が古い形式で出す
    check('定型文 商品分析: 版の固定指示 (V2.1 / V2.2 / 共通生成条件) は残っていない',
      !/V2\.[12]/.test(tplBoth.productAnalysis) && !tplBoth.productAnalysis.includes('# 共通生成条件'));
    // 簡易LP (2026-09-13 仕入れ低の商品も LP を作る)。要望のテンプレートどおりの並び
    // 2026-09-17: 【入力】の先頭に「画像の重要度」(draftBI は重要度を入れていないので未設定)
    check('定型文 簡易LP: @GPT名 / gid 付き参照仕様書 / 画像の重要度 → 商品情報 (カラバリ込み) → 商品画像 (空) → 【実行】',
      tplBoth.simpleLp.startsWith('@仕入れ商品 簡易LP・サムネイル作成\n\n【参照仕様書】\n'
        + 'https://docs.google.com/spreadsheets/d/1PbX8e_aKUnzZeq7xjmJkPwDC5l2shb1VdVtivbtyxmo/edit?gid=1932534260#gid=1932534260'
        + '\n\n【入力】\n\n画像の重要度：未設定\n\n商品情報：\n■裏面情報 (パッケージ裏面の表記)\n原材料：小麦')
      && tplBoth.simpleLp.endsWith('■カラバリ\nなし (単品)\n\n商品画像：\n\n【実行】\n'
        + '上記をもとに「仕入れ商品 簡易LP・サムネイル作成」の最新仕様に従って作成してください。\n'
        + '最終回答はAI画像生成用文章のみ出力してください。'), `${draftBI.image_priority}\n${tplBoth.simpleLp}`);
    check('定型文 簡易LP: 商品情報も裏面情報も空なら作らない (他の 2 つと同じ)',
      pt.buildPromptTemplates(draftBI, {}).simpleLp === null && pt.buildPromptTemplates(draftBI, {}).simpleLpPriority === null);
    // 画像の重要度 (2026-09-17 スタッフ要望: 激低と低で LP の作り込みを変える)。基本情報タブの選択肢をそのまま入れる
    {
      const { IMAGE_PRIORITIES } = await import('../db.js');
      const lines = IMAGE_PRIORITIES.map((p) => pt.buildPromptTemplates({ ...draftBI, image_priority: p.value }, { product_info_text: 'あ' }));
      check('定型文 簡易LP: 重要度の選択肢がそのまま「画像の重要度：」の行に入る (激低と低を区別できる)',
        lines.every((t, i) => t.simpleLp.includes(`\n【入力】\n\n画像の重要度：${IMAGE_PRIORITIES[i].value}\n\n商品情報：\n`))
        && lines[0].simpleLp.includes('画像の重要度：仕入れ商品（重要度：激低_白抜）')
        && lines[1].simpleLp.includes('画像の重要度：仕入商品（重要度：低）'), lines.map((t) => t.simpleLp.split('\n')[7]).join(' / '));
      const tLow = lines[1];
      check('定型文 簡易LP: 画面が行を入れ直す材料 (本文に入れた行・見出し・未設定の書き方) を返す',
        tLow.simpleLpPriority?.line === '画像の重要度：仕入商品（重要度：低）'
        && tLow.simpleLp.includes(tLow.simpleLpPriority.line)
        && tLow.simpleLpPriority.prefix === '画像の重要度：' && tLow.simpleLpPriority.unset === '未設定'
        && tplBoth.simpleLpPriority?.line === '画像の重要度：未設定', JSON.stringify([tLow.simpleLpPriority, tplBoth.simpleLpPriority]));
      // 本文の最初に出てくる行を画面が差し替えるので、商品情報より前にあること
      check('定型文 簡易LP: 重要度の行は商品情報より前 (商品情報に同じ文字があっても画面が当て違えない)',
        tLow.simpleLp.indexOf(tLow.simpleLpPriority.line) < tLow.simpleLp.indexOf('商品情報：'));
      check('定型文: 初動判定・商品分析には重要度を入れない (要望は簡易LPだけ)',
        !tLow.initialJudge.includes('画像の重要度') && !tLow.productAnalysis.includes('画像の重要度'));
    }

    // 画像タブの商品情報の自動表示 (2026-09-13 スタッフ要望)。商品説明タブの PC用商品説明文を文字にする
    {
      const pia = await import('../lib/product-info-auto.js');
      const { FIXED_NOTES } = await import('../lib/page-info.js');
      const txt = pia.descriptionHtmlToText('<table><tr><td><b>説明</b></td><td>一行目<br>二行目 &amp; &lt;b&gt;</td></tr>'
        + `<tr><td><b>注意事項</b></td><td>AIの注意<br>${FIXED_NOTES}</td></tr>`
        + '<tr><td><b>サイズ</b></td><td>10cm</td></tr><tr><td><b>広告文責</b></td><td>B-Faith株式会社<br>TEL</td></tr></table>');
      check('自動の商品情報: 表を「見出し：値」の文字にする (<br>=改行・文字参照を戻す・広告文責と固定の注意書きは落とす)',
        txt === '説明：\n一行目\n二行目 & <b>\n\n注意事項：AIの注意\nサイズ：10cm', JSON.stringify(txt));
      const idAuto = Number(db.prepare(`
        INSERT INTO product_drafts (ne_code, name, created_by, own_brand) VALUES ('DRV-AUTO-INFO', '自動商品情報', 'smoke', 1)
      `).run().lastInsertRowid);
      check('自動の商品情報: AI の特徴・仕様がまだ無ければ空 (商品名だけの表を商品情報と呼ばない)',
        pia.autoProductInfoText(db, idAuto) === '' && pia.hasAutoDescription(db, idAuto) === false);
      const insAi = db.prepare('INSERT INTO draft_ai_outputs (draft_id, kind, content) VALUES (?, ?, ?)');
      insAi.run(idAuto, 'desc_features', '手になじむ木のスプーン');
      insAi.run(idAuto, 'desc_spec', '長さ 15cm');
      insAi.run(idAuto, 'desc_notes', '食洗機は使えません');
      db.prepare("INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, '素材', '天然木', 0)").run(idAuto);
      const auto = pia.autoProductInfoText(db, idAuto);
      check('自動の商品情報: 商品説明タブの PC用商品説明文と同じ中身を文字で出す',
        auto.startsWith('説明：\n手になじむ木のスプーン\n\n長さ 15cm\n') && auto.includes('注意事項：食洗機は使えません')
        && auto.includes('素材：天然木') && !auto.includes('広告文責') && !auto.includes('モニター画面') && !/<[a-z]/i.test(auto), auto);
      check('自動の商品情報: 定型文に使うのは 手入力 > 自動',
        pia.effectiveProductInfo('手入力', auto) === '手入力' && pia.effectiveProductInfo('  ', auto) === auto);
      // ボードの「商品情報 未入力」: 手入力が無くても自動の説明文があれば出さない
      // (① の完了条件は 2026-09-13 に外したので、ここではカードの表示だけ見る)
      wfpEarly.ensureProgress(db, idAuto);
      const cardAuto = wfpEarly.boardData(db, { view: 'image', imageKind: 'detail' }).columns.flatMap((c) => c.cards).find((c) => c.id === idAuto);
      check('自動の商品情報: ボードの「商品情報 未入力」も自動の説明文があれば出さない',
        cardAuto?.hasProductInfo === true, JSON.stringify(cardAuto && { i: cardAuto.hasProductInfo }));
      db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idAuto);
    }
    // 画面の補足情報欄は廃止した。差し込み口が残っていると {{SUPPLEMENT}} がそのまま ChatGPT へ行く
    check('定型文: {{SUPPLEMENT}} の差し込み口は残っていない',
      !tplBoth.initialJudge.includes('{{SUPPLEMENT}}') && !tplBoth.productAnalysis.includes('{{SUPPLEMENT}}'));
    check('定型文: 商品情報だけなら裏面情報の見出しは出ない',
      !pt.buildPromptTemplates(draftBI, { product_info_text: 'あ' }).initialJudge.includes('■裏面情報'));

    // 裏面情報の保存 (HTTP)。画像制作の保存は商品リンク台帳へ strict 同期するので、台帳の表も要る
    // (本番では server.js が起動時に作る。ここまでの smoke では未作成 = 保存が 500 になる)
    (await import('../../product-links/db.js')).initProductLinksDB();
    const ipBI = () => db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(idBI) || {};
    r = await call('POST', `/api/drafts/${idBI}/image-production`, { back_info_text: '  原材料：小麦、砂糖  ' });
    check('裏面情報: 保存できる (前後の空白は落ちる)',
      r.status === 200 && ipBI().back_info_text === '原材料：小麦、砂糖', JSON.stringify(r) + JSON.stringify(ipBI()));
    check('裏面情報: 更新日時・更新者が残る', !!ipBI().back_info_updated_at && !!ipBI().back_info_updated_by);
    const backAtBefore = ipBI().back_info_updated_at;
    r = await call('POST', `/api/drafts/${idBI}/image-production`, { status: 'メモだけ更新' });
    check('裏面情報: 送らなければ消えない (部分更新)',
      r.status === 200 && ipBI().back_info_text === '原材料：小麦、砂糖' && ipBI().back_info_updated_at === backAtBefore);
    // 同じ内容の送り直しで更新日時が動くと「誰がいつ直したか」が実態とズレる (Codex R1 の追加候補)
    r = await call('POST', `/api/drafts/${idBI}/image-production`, { back_info_text: '原材料：小麦、砂糖' });
    check('裏面情報: 同じ内容を送り直しても更新日時は動かない',
      r.status === 200 && ipBI().back_info_updated_at === backAtBefore);
    // 商品情報と裏面情報を同時に直したら両方に更新日時が入る (cur を 1 回引きに直したので両方を見る)
    r = await call('POST', `/api/drafts/${idBI}/image-production`, { product_info_text: '商品情報も同時に', back_info_text: '裏面も同時に' });
    check('裏面情報: 商品情報と同時に更新しても両方に更新日時・更新者が入る',
      r.status === 200 && ipBI().product_info_text === '商品情報も同時に' && ipBI().back_info_text === '裏面も同時に'
      && !!ipBI().product_info_updated_at && ipBI().back_info_updated_at !== backAtBefore
      && !!ipBI().product_info_updated_by && !!ipBI().back_info_updated_by, JSON.stringify(ipBI()));
    check('裏面情報: 操作履歴に「商品情報・裏面情報を更新」が残る',
      db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'image_production_updated' AND detail = '商品情報・裏面情報を更新'`).get(idBI).c === 1);
    r = await call('POST', `/api/drafts/${idBI}/image-production`, { product_info_text: '', back_info_text: '原材料：小麦、砂糖' });
    check('裏面情報: 商品情報だけ空に戻せる', r.status === 200 && ipBI().product_info_text === null && ipBI().back_info_text === '原材料：小麦、砂糖');
    r = await call('POST', `/api/drafts/${idBI}/image-production`, { back_info_text: '' });
    check('裏面情報: 空で消せる', r.status === 200 && ipBI().back_info_text === null);
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idBI);

    // 仕入商品 (重要度：低) でも保存・保留 API が重要度で弾かない (2026-09-13 画像制作を全商品に出す。
    // 以前は 自社商品 / 取扱先限定商品 以外を 400 にしていた)
    const idLowApi = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by, own_brand, image_priority)
      VALUES ('DRV-LOW-API', '仕入れ低API', 'smoke', 0, '仕入商品（重要度：低）')
    `).run().lastInsertRowid);
    r = await call('POST', `/api/drafts/${idLowApi}/image-production`, { product_info_text: '仕入れ低の説明文' });
    check('画像制作: 仕入商品 (重要度：低) でも商品情報を保存できる',
      r.status === 200 && db.prepare('SELECT product_info_text FROM draft_image_production WHERE draft_id = ?').get(idLowApi)?.product_info_text === '仕入れ低の説明文',
      JSON.stringify(r));
    r = await call('POST', `/api/drafts/${idLowApi}/image-hold`, { on_hold: true });
    const rUnhold = await call('POST', `/api/drafts/${idLowApi}/image-hold`, { on_hold: false });
    check('画像制作: 仕入商品でも保留をかけて外せる', r.status === 200 && rUnhold.status === 200, JSON.stringify([r, rUnhold]));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idLowApi);

    // 本番の構成の 済/まだ (2026-09-13 スタッフ要望)。工程は動かさず、印と履歴だけ残す
    const idCmp = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-COMPOSE-API', '構成API', 'smoke')
    `).run().lastInsertRowid);
    const cmpOf = () => db.prepare('SELECT compose_status, compose_updated_at, compose_updated_by FROM draft_image_production WHERE draft_id = ?').get(idCmp) || {};
    r = await call('POST', `/api/drafts/${idCmp}/compose`, { done: true });
    check('構成: 済にすると値と日時・担当が残る',
      r.status === 200 && r.json?.changed === true && cmpOf().compose_status === 'done' && !!cmpOf().compose_updated_at && !!cmpOf().compose_updated_by,
      JSON.stringify([r, cmpOf()]));
    const cmpAt = cmpOf().compose_updated_at;
    r = await call('POST', `/api/drafts/${idCmp}/compose`, { done: true });
    check('構成: 同じ値の送り直しでは日時を上書きしない', r.status === 200 && r.json?.changed === false && cmpOf().compose_updated_at === cmpAt);
    r = await call('POST', `/api/drafts/${idCmp}/compose`, { done: false });
    // NULL に戻すと ③素材待ちからの推定で 済 に戻ってしまう (Codex R1) → 'todo' として残す
    check('構成: まだにすると todo が残る (未設定には戻さない)', r.status === 200 && r.json?.changed === true && cmpOf().compose_status === 'todo',
      JSON.stringify([r, cmpOf()]));
    r = await call('POST', `/api/drafts/${idCmp}/compose`, { done: 'yes' });
    check('構成: done が boolean でなければ 400 (「まだ」に倒さない)', r.status === 400);
    check('構成: 操作履歴に 済にした / まだに戻した が残る',
      db.prepare("SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'compose_marked'").get(idCmp).c === 2);
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idCmp);

    // TOP画像の構成 (2026-09-13 スタッフ要望)。簡単なテキストの構成と参考・ラフの URL を画像制作に持つ
    const idTop = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-TOP-COMPOSE', 'TOP構成', 'smoke')
    `).run().lastInsertRowid);
    const topOf = () => db.prepare('SELECT top_compose_text, top_ref_url FROM draft_image_production WHERE draft_id = ?').get(idTop) || {};
    const ROUGH = 'https://drive.google.com/file/d/rough';
    r = await call('POST', `/api/drafts/${idTop}/image-production`, { top_compose_text: '  白背景に商品を大きく  ', top_ref_url: ROUGH });
    check('TOP画像の構成: テキストと参考・ラフの URL が保存される (前後の空白は落ちる)',
      r.status === 200 && topOf().top_compose_text === '白背景に商品を大きく' && topOf().top_ref_url === ROUGH, JSON.stringify([r, topOf()]));
    r = await call('POST', `/api/drafts/${idTop}/image-production`, { top_ref_url: 'javascript:alert(1)' });
    check('TOP画像の構成: 参考・ラフの URL は http(s) 以外なら 400 (保存済みの値は残る)',
      r.status === 400 && topOf().top_ref_url === ROUGH, JSON.stringify([r, topOf()]));
    r = await call('POST', `/api/drafts/${idTop}/image-production`, { status: 'メモだけ更新' });
    check('TOP画像の構成: 項目を送らない保存では消えない (部分更新)',
      r.status === 200 && topOf().top_compose_text === '白背景に商品を大きく' && topOf().top_ref_url === ROUGH);
    r = await call('POST', `/api/drafts/${idTop}/image-production`, { top_compose_text: '', top_ref_url: '' });
    check('TOP画像の構成: 空で送れば消える', r.status === 200 && topOf().top_compose_text == null && topOf().top_ref_url == null);
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idTop);

    // 縦列「構成」→「仮構成」(2026-09-13)。seed は既存行を変えないので一回きりの補正で直す
    {
      const stepOf = () => db.prepare("SELECT label, description FROM ph_steps WHERE code = 'imgd_compose'").get();
      check('仮構成: 新しい DB では最初から「仮構成」', stepOf().label === '仮構成', JSON.stringify(stepOf()));
      const rerun = (label) => {
        db.prepare("UPDATE ph_steps SET label = ?, description = '② 商品画像の構成を作る' WHERE code = 'imgd_compose'").run(label);
        db.prepare('DELETE FROM ph_intake_state WHERE key = ?').run(dbmod.COMPOSE_STEP_RENAMED_KEY);
        return dbmod.migrateComposeStepLabel(db);
      };
      const m1 = rerun('構成');
      check('仮構成: 既存 DB の「構成」は一度だけ「仮構成」に直す (説明も新しい文に)',
        m1.renamed === 1 && stepOf().label === '仮構成' && /仮の構成/.test(stepOf().description || ''), JSON.stringify([m1, stepOf()]));
      db.prepare("UPDATE ph_steps SET label = '構成' WHERE code = 'imgd_compose'").run();
      check('仮構成: 一度走ったら、管理画面で「構成」に戻しても巻き戻さない',
        dbmod.migrateComposeStepLabel(db).skipped === true && stepOf().label === '構成');
      const m2 = rerun('撮影前の構成');
      // Codex R1: 説明文だけ新しい意味に書き換わると、名前と説明が食い違う
      check('仮構成: 管理画面で別名にしてある場合は表示名も説明文も触らない',
        m2.renamed === 0 && stepOf().label === '撮影前の構成' && stepOf().description === '② 商品画像の構成を作る', JSON.stringify(stepOf()));
      db.prepare("UPDATE ph_steps SET label = '構成', description = '人が直した説明' WHERE code = 'imgd_compose'").run();
      db.prepare('DELETE FROM ph_intake_state WHERE key = ?').run(dbmod.COMPOSE_STEP_RENAMED_KEY);
      dbmod.migrateComposeStepLabel(db);
      check('仮構成: 表示名は直しても、人が直した説明文は残す',
        stepOf().label === '仮構成' && stepOf().description === '人が直した説明', JSON.stringify(stepOf()));
      // 後片付け: 他のテストが見る表示名に戻す
      db.prepare("UPDATE ph_steps SET label = '仮構成' WHERE code = 'imgd_compose'").run();
    }
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

  // ─── SKU別JAN (2026-08-28 中原さん要望: バリエーションありはSKUごとにJANを控える) ───
  // 有効な JAN (チェックデジット込み): 4901234567894 / 4912345678904
  const janOf = (code) => db.prepare(
    'SELECT jan_code FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').get(idP, code)?.jan_code;
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'vc-b', jan_code: '4901234567894' });
  check('SKU JAN: 保存できる', r.status === 200 && janOf('vc-b') === '4901234567894', JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'vc-b', jan_code: '4912345678904' });
  check('SKU JAN: 上書きできる (UPSERT)', r.status === 200 && janOf('vc-b') === '4912345678904');
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'vc-b', jan_code: '4901234567890' });
  check('SKU JAN: チェックデジットが合わない値は 400', r.status === 400, JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'vc-b', jan_code: '12345' });
  check('SKU JAN: 桁数が合わない値は 400', r.status === 400);
  check('SKU JAN: 弾かれた要求で既存値は書き換わらない', janOf('vc-b') === '4912345678904');
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'vc-a', jan_code: '4912345678904' });
  check('SKU JAN: 同じJANを別SKUに付けようとしたら 409 (別商品の同一JANはモールで弾かれる)',
    r.status === 409, JSON.stringify(r.json));
  {
    // API の事前チェックだけでなく DB 制約でも防ぐ (一括取込など別経路の backstop)
    let dupErr = null;
    try {
      db.prepare(`INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, 'vc-a', '4912345678904')`).run(idP);
    } catch (e) { dupErr = e; }
    check('SKU JAN: 同一ページ内のJAN重複は DB 制約でも弾く', /UNIQUE/i.test(String(dupErr && dupErr.message)),
      String(dupErr && dupErr.message));
  }
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'not-in-group', jan_code: '4901234567894' });
  check('SKU JAN: グループ外のSKUは 409', r.status === 409);
  r = await call('POST', `/api/drafts/${idE}/sku-jans`, { ne_code: 'vc-b', jan_code: '4901234567894' });
  check('SKU JAN: バリエーションでないドラフトは 400', r.status === 400);
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'vc-b', jan_code: '' });
  check('SKU JAN: 空で解除 = 行が消える', r.status === 200 && janOf('vc-b') === undefined);
  // SKU を外したら JAN 行も掃除される (残すと入力欄が消えて解除できない)
  r = await call('POST', `/api/drafts/${idP}/sku-jans`, { ne_code: 'vc-b', jan_code: '4901234567894' });
  check('SKU JAN: 除外前は保存できる', r.status === 200, JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/variation/exclude`, { ne_code: 'vc-b' });
  check('SKU JAN: SKUを外すとJAN行も消える (孤児行を残さない)', r.status === 200 && janOf('vc-b') === undefined);
  db.prepare(`DELETE FROM draft_variation_exclusions WHERE LOWER(TRIM(ne_code)) = 'vc-b'`).run();

  // ─── SKU別の商品仕様 / カタログIDなしの理由 (2026-09-03 中原さん: RMS と同じ SKU 列 × 項目行の表) ───
  const attrOf = (code, name) => db.prepare(
    'SELECT value FROM draft_sku_attributes WHERE draft_id = ? AND sku_code = ? AND name = ?').get(idP, code, name);
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { ne_code: 'vc-b', name: '代表カラー', value: '白' });
  check('SKU仕様: SKU ごとに保存される', r.status === 200 && attrOf('vc-b', '代表カラー')?.value === '白', JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { name: 'ブランド名', value: 'B-Faith', all: true });
  check('SKU仕様: 一括入力は全 SKU に同じ値 (1 リクエスト)', r.status === 200 && r.json.count === 2
    && attrOf('vc-a', 'ブランド名')?.value === 'B-Faith' && attrOf('vc-b', 'ブランド名')?.value === 'B-Faith', JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { ne_code: 'vc-b', name: '代表カラー', value: '' });
  check('SKU仕様: 空は「明示的に空」の行として残る (共通の既定値を打ち消す)', r.status === 200 && attrOf('vc-b', '代表カラー')?.value === '');
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { ne_code: 'vc-b', name: 'カタログID', value: '4901234567894' });
  check('SKU仕様: 「カタログID」は行として受け付けない (JAN は専用行)', r.status === 400, JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { ne_code: 'not-in-group', name: 'ブランド名', value: 'x' });
  check('SKU仕様: グループ外の SKU は 409', r.status === 409);
  r = await call('POST', `/api/drafts/${idE}/sku-attributes`, { ne_code: 'vc-b', name: 'ブランド名', value: 'x' });
  check('SKU仕様: バリエーションでないドラフトは 400', r.status === 400);
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { ne_code: 'vc-b', value: 'x' });
  check('SKU仕様: 項目名が無ければ 400', r.status === 400);
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { ne_code: 'vc-b', name: 'ブランド名', value: 'x'.repeat(301) });
  check('SKU仕様: 301 文字の値は 400 (黙って切り詰めない)', r.status === 400 && attrOf('vc-b', 'ブランド名')?.value === 'B-Faith', JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-attributes`, { ne_code: 'vc-b', name: 'n'.repeat(101), value: 'x' });
  check('SKU仕様: 101 文字の項目名は 400', r.status === 400);
  // /rakuten で article_number を送らない保存 (バリエーションの画面) は既存の型番を維持する (Codex R1 high)
  db.prepare(`INSERT INTO draft_rakuten (draft_id, genre_id, article_number) VALUES (?, '1', 'M-KEEP')
    ON CONFLICT(draft_id) DO UPDATE SET article_number = 'M-KEEP'`).run(idP);
  r = await call('POST', `/api/drafts/${idP}/rakuten`, { genre_id: '1' });
  check('メーカー型番: /rakuten で article_number を送らなければ既存を維持する', r.status === 200
    && db.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(idP)?.article_number === 'M-KEEP', JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/rakuten`, { genre_id: '1', article_number: '' });
  check('メーカー型番: 空文字は明示的な解除', r.status === 200
    && db.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(idP)?.article_number == null);
  // バリエーションは単品用の競合チェックを走らせず、旧データ (属性側の型番) も捨てない (Codex R2 high)
  db.prepare(`UPDATE draft_rakuten SET article_number = 'NEW', attributes_json = '[{"name":"メーカー型番","values":["OLD"]}]' WHERE draft_id = ?`).run(idP);
  r = await call('POST', `/api/drafts/${idP}/rakuten`, { genre_id: '1' });
  {
    const row = db.prepare('SELECT article_number, attributes_json FROM draft_rakuten WHERE draft_id = ?').get(idP);
    check('メーカー型番: バリエーションの /rakuten は旧型番の食い違いで 400 にせず、旧値も捨てない',
      r.status === 200 && row?.article_number === 'NEW' && String(row?.attributes_json).includes('OLD'), `${r.status} ${JSON.stringify(row)}`);
  }
  // 古い画面・旧クライアントが attributes を送ってきても、バリエーションでは旧メーカー型番の行を落とさない (Codex R3 high)
  r = await call('POST', `/api/drafts/${idP}/rakuten`, { genre_id: '1', attributes: [{ name: 'ブランド名', value: 'X' }] });
  {
    const row = db.prepare('SELECT attributes_json FROM draft_rakuten WHERE draft_id = ?').get(idP);
    check('メーカー型番: バリエーションの /rakuten は attributes を送られても旧型番の行を残す',
      r.status === 200 && String(row?.attributes_json).includes('OLD') && String(row?.attributes_json).includes('ブランド名'), `${r.status} ${JSON.stringify(row)}`);
  }
  // 旧データの「カタログID」属性は SKU 表に展開せず、警告ボタン (drop_legacy_catalog_attr) で削除する (Codex R3 high)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = '[{"name":"カタログID","values":["4901234567894"]},{"name":"ブランド名","values":["X"]}]' WHERE draft_id = ?`).run(idP);
  {
    const membersP = vari.resolveVariationGroup(db, 'vc', { draftId: idP, withMembers: true }).members;
    const gP = listing.skuAttributeGrid(db, idP, db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(idP), membersP);
    check('旧カタログID属性: SKU 表には展開せず legacyCatalogIds に出す',
      !gP.names.includes('カタログID') && gP.legacyCatalogIds.join() === '4901234567894' && gP.names.includes('ブランド名'), JSON.stringify(gP.names));
  }
  // 後続の検証で 400 なら属性も残り、削除ログも残らない (Codex R4 medium: ログが更新と同じトランザクションに)
  r = await call('POST', `/api/drafts/${idP}/rakuten`, { genre_id: '1', drop_legacy_catalog_attr: true, shipping_method_group: 'zz' });
  check('旧カタログID属性: 後続の検証で 400 なら属性は残り、削除ログも残らない', r.status === 400
    && String(db.prepare('SELECT attributes_json FROM draft_rakuten WHERE draft_id = ?').get(idP)?.attributes_json).includes('カタログID')
    && !db.prepare("SELECT 1 FROM draft_events WHERE draft_id = ? AND event = 'legacy_catalog_attr_dropped'").get(idP), JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/rakuten`, { genre_id: '1', drop_legacy_catalog_attr: true });
  {
    const row = db.prepare('SELECT attributes_json FROM draft_rakuten WHERE draft_id = ?').get(idP);
    check('旧カタログID属性: drop_legacy_catalog_attr で「カタログID」の行だけ消える',
      r.status === 200 && !String(row?.attributes_json).includes('カタログID') && String(row?.attributes_json).includes('ブランド名'), `${r.status} ${JSON.stringify(row)}`);
  }
  db.prepare(`UPDATE draft_rakuten SET article_number = NULL, attributes_json = NULL WHERE draft_id = ?`).run(idP);
  const exOf = (code) => db.prepare(
    'SELECT reason FROM draft_sku_catalog_exemptions WHERE draft_id = ? AND sku_code = ?').get(idP, code)?.reason;
  r = await call('POST', `/api/drafts/${idP}/sku-catalog-exemptions`, { ne_code: 'vc-b', reason: '3' });
  check('SKU理由: SKU ごとに保存される', r.status === 200 && exOf('vc-b') === 3, JSON.stringify(r.json));
  r = await call('POST', `/api/drafts/${idP}/sku-catalog-exemptions`, { ne_code: 'vc-b', reason: '9' });
  check('SKU理由: 範囲外は 400 (既存は維持)', r.status === 400 && exOf('vc-b') === 3);
  r = await call('POST', `/api/drafts/${idP}/sku-catalog-exemptions`, { ne_code: 'not-in-group', reason: '3' });
  check('SKU理由: グループ外の SKU は 409', r.status === 409);
  r = await call('POST', `/api/drafts/${idP}/sku-catalog-exemptions`, { ne_code: 'vc-b', reason: '' });
  check('SKU理由: 空で解除 = ページ共通の理由に戻る', r.status === 200 && exOf('vc-b') === undefined);
  // SKU を外したら商品仕様・理由の行も掃除される (孤児行を残さない)
  r = await call('POST', `/api/drafts/${idP}/sku-catalog-exemptions`, { ne_code: 'vc-b', reason: '2' });
  r = await call('POST', `/api/drafts/${idP}/variation/exclude`, { ne_code: 'vc-b' });
  check('SKU仕様/理由: SKUを外すと行も消える (孤児行を残さない)', r.status === 200
    && !attrOf('vc-b', 'ブランド名') && exOf('vc-b') === undefined && attrOf('vc-a', 'ブランド名')?.value === 'B-Faith');
  db.prepare(`DELETE FROM draft_variation_exclusions WHERE LOWER(TRIM(ne_code)) = 'vc-b'`).run();
  db.prepare('DELETE FROM draft_sku_attributes WHERE draft_id = ?').run(idP);

  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idP);

  // ─── 参考URL / 商品情報 (ブランド名・容量) / 掲載HTMLの商品名 (2026-08-28 中原さん要望) ───
  {
    const idB = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-BASIC28', '旧の商品名', 'smoke')
    `).run().lastInsertRowid);
    // ブランド名・容量が保存され、掲載HTMLに載る
    r = await call('POST', `/api/drafts/${idB}/page-info`, {
      product_type: 'general', brand_name: 'B-Faith', content_volume: '200g',
    });
    check('商品情報: ブランド名・容量が保存される', r.status === 200
      && db.prepare('SELECT brand_name, content_volume FROM draft_page_info WHERE draft_id = ?').get(idB)?.brand_name === 'B-Faith',
      JSON.stringify(r.json).slice(0, 200));
    check('商品情報: 掲載HTMLにブランド名と容量が載る (雑貨でも容量が出る)',
      r.json.html.includes('ブランド名') && r.json.html.includes('B-Faith')
      && r.json.html.includes('内容量') && r.json.html.includes('200g'));
    // 掲載HTMLの商品名は「基本情報にいま入っている商品名」を使う (保存はしない)
    check('掲載HTML: product_name 未指定なら DB の商品名', r.json.html.includes('旧の商品名'));
    r = await call('POST', `/api/drafts/${idB}/page-info`, {
      product_type: 'general', brand_name: 'B-Faith', content_volume: '200g', product_name: '打ち替えた新しい商品名',
    });
    check('掲載HTML: 基本情報にいま入っている商品名が使われる',
      r.json.html.includes('打ち替えた新しい商品名') && !r.json.html.includes('旧の商品名'),
      r.json.html.slice(0, 200));
    check('掲載HTML: 商品名のプレビュー指定では product_drafts.name を書き換えない',
      db.prepare('SELECT name FROM product_drafts WHERE id = ?').get(idB).name === '旧の商品名');
    // 画面で商品名を空にしたら「商品名」行も消える (DB の古い値を復活させない — Codex R1 中)
    r = await call('POST', `/api/drafts/${idB}/page-info`, {
      product_type: 'general', brand_name: 'B-Faith', content_volume: '200g', product_name: '',
    });
    check('掲載HTML: 画面で商品名が空なら「商品名」行を出さない (古い名前を復活させない)',
      !r.json.html.includes('旧の商品名') && !r.json.html.includes('<b>商品名</b>'), r.json.html.slice(0, 200));
    // その他注意事項 (2026-09-13 スタッフ要望)。保存されて掲載HTMLに載る / この欄を知らない古い画面の保存では消えない
    const otherOf = () => db.prepare('SELECT other_notes FROM draft_page_info WHERE draft_id = ?').get(idB)?.other_notes;
    r = await call('POST', `/api/drafts/${idB}/page-info`, {
      product_type: 'general', brand_name: 'B-Faith', content_volume: '200g', other_notes: '  箱から出して配送します  ',
    });
    check('その他注意事項: 保存され (前後の空白は落ちる)、掲載HTMLに行として載る',
      r.status === 200 && otherOf() === '箱から出して配送します'
      && r.json.html.includes('<b>その他注意事項</b>') && r.json.html.includes('箱から出して配送します'),
      JSON.stringify(r.json).slice(0, 200));
    r = await call('POST', `/api/drafts/${idB}/page-info`, { product_type: 'general', brand_name: 'B-Faith', content_volume: '200g' });
    check('その他注意事項: 項目を送らない保存 (デプロイ前から開いたままの画面の自動保存) では消えない',
      r.status === 200 && otherOf() === '箱から出して配送します', String(otherOf()));
    r = await call('POST', `/api/drafts/${idB}/page-info`, {
      product_type: 'general', brand_name: 'B-Faith', content_volume: '200g', other_notes: '',
    });
    check('その他注意事項: 空で送れば消え、行も出ない', r.status === 200 && otherOf() == null && !r.json.html.includes('その他注意事項'));
    // 参考URL: 追加ボタンでも自動反映でも通る経路は同じ (URL 検証はサーバー側が最終判定)
    r = await call('POST', `/api/drafts/${idB}/refs`, { url: 'https://example.com/ref-1' });
    check('参考URL: 追加できる', r.status === 200
      && db.prepare('SELECT COUNT(*) c FROM draft_reference_urls WHERE draft_id = ?').get(idB).c === 1,
      JSON.stringify(r.json));
    r = await call('POST', `/api/drafts/${idB}/refs`, { url: 'javascript:alert(1)' });
    check('参考URL: http(s) 以外は 400 (自動反映でも素通しにしない)', r.status === 400, JSON.stringify(r.json));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idB);
  }

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

  // ─── かんばんの手動並び順 + 詳細の戻り先 (2026-08-28 中原さん要望) ───
  {
    // 既定の並びは「停滞が長い順 → 登録順」。手で並べ替えたらその順が残る (読み直しても戻らない)
    const colOf = (b, code) => b.columns.find((c) => (c.code || c.key) === code);
    const b0 = wfp.boardData(db, {});
    const target = b0.columns.find((c) => c.cards.length >= 2);
    check('並び順: 2枚以上あるボード列がある (以降の検証の前提)', !!target,
      b0.columns.map((c) => `${c.code}:${c.cards.length}`).join(','));
    if (target) {
      const colCode = target.code || target.key;
      const before = target.cards.map((c) => c.id);
      // 先頭と末尾を入れ替えて保存 (画面が送るのと同じ「列まるごとの順番」)
      const wanted = [before[before.length - 1], ...before.slice(1, -1), before[0]];
      const rr = await call('POST', '/api/board/reorder', {
        view: 'main', col: colCode, items: wanted.map((id) => ({ id })),
      });
      check('並び順: API が保存を受け付ける', rr.status === 200 && rr.json.ok && rr.json.saved === wanted.length,
        JSON.stringify(rr.json));
      const b1 = wfp.boardData(db, {});
      check('並び順: 読み直しても手で並べた順のまま (作成順に戻らない)',
        colOf(b1, colCode).cards.map((c) => c.id).join(',') === wanted.join(','),
        `${colOf(b1, colCode).cards.map((c) => c.id).join(',')} != ${wanted.join(',')}`);

      // 別ビューの並びは独立 (本流で並べても画像ボードの順は変わらない)
      const ordRows = db.prepare(`SELECT COUNT(*) AS n FROM ph_board_order WHERE view = 'image'`).get().n;
      check('並び順: 本流の並べ替えは画像ビューに漏れない', ordRows === 0, `image rows=${ordRows}`);

      // 工程が変わって別の列に出たカードは、前の列で付けた順番を持ち込まない
      const movedId = wanted[0];
      db.prepare(`UPDATE ph_board_order SET col = 'ZZZ-OTHER' WHERE view = 'main' AND draft_id = ?`).run(movedId);
      const b2 = wfp.boardData(db, {});
      const cards2 = colOf(b2, colCode).cards.map((c) => c.id);
      check('並び順: 別の列に置いた記録はこの列に効かない (既定順に戻る)',
        cards2[0] !== movedId || wanted.length === 1, cards2.join(','));
      // 記録の列と実際の列が食い違う行は、ボードを開いたときに掃除される (Codex R2 中:
      // 残ると、あとでその列を並べ替えたとき見えないカードとして差し込み位置を押し下げる)
      check('並び順: 実際の列と食い違う記録はボード表示時に消える',
        db.prepare(`SELECT COUNT(*) AS n FROM ph_board_order WHERE col = 'ZZZ-OTHER'`).get().n === 0);
      // 単品タブで開いても同じ掃除が効く (2026-09-10 監査: 記録のキーは main なのに view='single' で
      // 消していて空振りし、食い違った行が全体タブを開くまで残っていた)
      {
        const singleId = b2.columns.flatMap((c) => c.cards).find((c) => !c.isSet)?.id;
        check('並び順: 掃除の検証に使う単品カードがある', !!singleId);
        if (singleId) {
          db.prepare(`INSERT INTO ph_board_order (view, draft_id, kind, col, sort, updated_at)
            VALUES ('main', ?, '', 'ZZZ-OTHER2', 0, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            ON CONFLICT (view, draft_id, kind) DO UPDATE SET col = 'ZZZ-OTHER2'`).run(singleId);
          wfp.boardData(db, { view: 'single' });
          check('並び順: 単品ビューで開いても食い違う記録が消える',
            db.prepare(`SELECT COUNT(*) AS n FROM ph_board_order WHERE col = 'ZZZ-OTHER2'`).get().n === 0);
        }
      }
      // NE要対応の件数 (2026-09-10 Codex R1 low): 表と同じ条件で数え、router がどのタブにも渡している
      {
        const parentId = b2.columns.flatMap((c) => c.cards).find((c) => !c.isSet)?.id;
        check('NE件数: 検証に使う親カードがある', !!parentId);
        if (parentId) {
          const before = wfp.neRegistrationCount(db);
          const mk = (code, status, provisional) => Number(db.prepare(
            `INSERT INTO product_drafts (ne_code, name, status, parent_draft_id, provisional_code, created_by)
             VALUES (?, ?, ?, ?, ?, 'smoke')`,
          ).run(code, `NE件数 ${code}`, status, parentId, provisional).lastInsertRowid);
          const ids = [
            mk('SET-NECNT-01', 'draft', 1),     // 数える
            mk('SET-NECNT-02', 'on_hold', 1),   // 保留は数えない
            mk('SET-NECNT-03', 'excluded', 1),  // 除外は数えない
            mk('necnt-04-2set', 'draft', 0),    // 本コード確定済みは数えない
          ];
          const cnt = wfp.neRegistrationCount(db);
          check('NE件数: 仮コードのまま動いているセットだけを数える (保留・除外・確定済みは除く) = 表の行数',
            cnt === before + 1 && cnt === wfp.neRegistrationRows(db).length,
            `count=${cnt} before=${before} rows=${wfp.neRegistrationRows(db).length}`);
          const html = await (await fetch(base + '/board')).text();
          const htmlImg = await (await fetch(base + '/board?view=image')).text();
          const badge = new RegExp(`kb-tab-n">${cnt}<`);
          check('NE件数: 全体タブと画像タブの NE要対応バッジに同じ件数が出る (router が全タブへ渡している)',
            badge.test(html) && badge.test(htmlImg), `count=${cnt} main=${badge.test(html)} image=${badge.test(htmlImg)}`);
          db.prepare(`DELETE FROM product_drafts WHERE id IN (${ids.map(() => '?').join(',')})`).run(...ids);
        }
      }

      // 確認中は手動順より上 (2026-08-31 / Codex R1)。「情報待ちが埋もれる」が要望の本体なので、
      // 以前その列で手で決めた位置より優先する。手で最後尾に置いたカードを確認中にして確かめる
      {
        const again = await call('POST', '/api/board/reorder', {
          view: 'main', col: colCode, items: wanted.map((id) => ({ id })),
        });
        check('確認中: 手動順の再保存 (前提)', again.status === 200);
        const lastId = wanted[wanted.length - 1];
        dbmod.setDraftChecking(db, lastId, { reasonCode: 'no_web_info', actor: 'smoke' });
        const bChk = wfp.boardData(db, {});
        const colChk = colOf(bChk, colCode);
        check('確認中: 手で最後尾に置いたカードでも、確認中にしたら列の先頭に来る',
          colChk && colChk.cards[0].id === lastId,
          colChk ? colChk.cards.map((c) => `${c.id}${c.checking ? '(確認中)' : ''}`).join(',') : '(列が無い)');
        dbmod.clearDraftChecking(db, lastId, { actor: 'smoke' });
        const bBack = wfp.boardData(db, {});
        check('確認中: 解除すると手で並べた順に戻る (手動順を壊さない)',
          colOf(bBack, colCode).cards.map((c) => c.id).join(',') === wanted.join(','),
          colOf(bBack, colCode).cards.map((c) => c.id).join(','));
      }

      // 完了列と画像ビューも同じ orderIn を通る (Codex R2 low: 本流の通常列でしか見ていなかった)
      {
        // 完了列の検証には完了カードが 2 枚要る。この時点では足りないので、既存 draft 2 件の
        // 本流工程をその場で done にして作り、検証後に元の state へ戻す
        // (「前提が無いので黙って skip」にすると、テストがあるのに何も検証していない状態になる)
        const mainCodes = db.prepare(`SELECT code FROM ph_steps WHERE track = 'main' AND active = 1`).all().map((x) => x.code);
        const donors = db.prepare(`SELECT id FROM product_drafts WHERE status NOT IN ('on_hold','excluded')
          ORDER BY id LIMIT 2`).all().map((x) => x.id);
        const savedStates = donors.length === 2
          ? db.prepare(`SELECT draft_id, step_code, state FROM draft_step_progress
              WHERE draft_id IN (${donors.join(',')})`).all()
          : [];
        if (donors.length === 2) {
          db.prepare(`UPDATE draft_step_progress SET state = 'done'
            WHERE draft_id IN (${donors.join(',')})
              AND step_code IN (${mainCodes.map(() => '?').join(',')})`).run(...mainCodes);
        }
        const bD = wfp.boardData(db, {});
        check('確認中: 完了列の検証の前提 (完了カードを 2 枚用意できた)', bD.doneCards.length >= 2,
          `done=${bD.doneCards.length}`);
        if (bD.doneCards.length >= 2) {
          const doneIds = bD.doneCards.map((c) => c.id);
          const wantedDone = [...doneIds.slice(1), doneIds[0]];
          await call('POST', '/api/board/reorder', {
            view: 'main', col: 'done', items: wantedDone.map((id) => ({ id })),
          });
          const lastDone = wantedDone[wantedDone.length - 1];
          dbmod.setDraftChecking(db, lastDone, { reasonCode: 'other', actor: 'smoke' });
          check('確認中: 完了列でも手動順より上に来る',
            wfp.boardData(db, {}).doneCards[0].id === lastDone,
            wfp.boardData(db, {}).doneCards.map((c) => `${c.id}${c.checking ? '(確認中)' : ''}`).join(','));
          dbmod.clearDraftChecking(db, lastDone, { actor: 'smoke' });
          check('確認中: 完了列も解除で手動順に戻る',
            wfp.boardData(db, {}).doneCards.slice(0, wantedDone.length).map((c) => c.id).join(',') === wantedDone.join(','),
            wfp.boardData(db, {}).doneCards.map((c) => c.id).join(','));
        }
        // 工程を元に戻す (以降のテストに「勝手に完了した商品」を持ち込まない)
        for (const s of savedStates) {
          db.prepare('UPDATE draft_step_progress SET state = ? WHERE draft_id = ? AND step_code = ?')
            .run(s.state, s.draft_id, s.step_code);
        }
        for (const id of donors) wfp.recomputeDraftStatus(db, id, { actor: 'smoke' });
        // 画像ビュー: カード = 商品×種別。同じ商品の TOP/詳細 が**それぞれの列で**先頭に来る
        const bI = wfp.boardData(db, { view: 'image' });
        const imgCol = bI.columns.find((c) => c.cards.length >= 2);
        // 前提も check にする (Codex R3): fixture が変わって「2枚以上ある画像列」が消えたとき、
        // 黙って未実行のまま ALL PASS になると、検証しているつもりで何も見ていない状態になる
        check('確認中: 画像ビューの検証の前提 (2枚以上あるカード列がある)', !!imgCol,
          bI.columns.map((c) => `${c.code || c.key}:${c.cards.length}`).join(','));
        if (imgCol) {
          const imgIds = imgCol.cards.map((c) => `${c.id}|${c.kind}`);
          const wantedImg = [...imgIds.slice(1), imgIds[0]];
          await call('POST', '/api/board/reorder', {
            view: 'image', col: imgCol.code || imgCol.key,
            items: wantedImg.map((k) => ({ id: Number(k.split('|')[0]), kind: k.split('|')[1] })),
          });
          const lastImg = wantedImg[wantedImg.length - 1];
          const lastImgId = Number(lastImg.split('|')[0]);
          dbmod.setDraftChecking(db, lastImgId, { reasonCode: 'other', actor: 'smoke' });
          const afterImg = wfp.boardData(db, { view: 'image' });
          const colAfter = afterImg.columns.find((c) => (c.code || c.key) === (imgCol.code || imgCol.key));
          // 確認中は**商品**に付くフラグなので、画像ビューでは同じ商品の TOP と詳細が
          // 両方とも確認中カードになる。「その 1 枚が先頭」ではなく
          // 「確認中のカードが全部、通常のカードより前にいて、そこに対象が居る」で見る
          check('確認中: 画像ビューでも手動順より上に来る (同じ商品の TOP/詳細 が揃って先頭グループ)', (() => {
            if (!colAfter) return false;
            const flags = colAfter.cards.map((c) => !!c.checking);
            const lastChk = flags.lastIndexOf(true);
            const firstNormal = flags.indexOf(false);
            if (lastChk < 0) return false;                                  // 確認中が 1 枚も無い
            if (firstNormal >= 0 && lastChk > firstNormal) return false;    // 通常カードに割り込まれている
            return colAfter.cards.map((c) => `${c.id}|${c.kind}`).indexOf(lastImg) <= lastChk;
          })(),
            colAfter ? colAfter.cards.map((c) => `${c.id}|${c.kind}${c.checking ? '(確認中)' : ''}`).join(',') : '(列が無い)');
          dbmod.clearDraftChecking(db, lastImgId, { actor: 'smoke' });
          const backImg = wfp.boardData(db, { view: 'image' })
            .columns.find((c) => (c.code || c.key) === (imgCol.code || imgCol.key));
          check('確認中: 画像ビューも解除で id|kind ごとの手動順に戻る',
            backImg.cards.map((c) => `${c.id}|${c.kind}`).join(',') === wantedImg.join(','),
            backImg.cards.map((c) => `${c.id}|${c.kind}`).join(','));
        }
      }
      // 後片付け (以降の並び検証に影響させない)
      db.prepare(`DELETE FROM ph_board_order`).run();
    }
    // 絞り込み中の並べ替えが、画面に出ていないカードの順番を巻き込まない (Codex R1 高)
    {
      const b = wfp.boardData(db, {});
      const t = b.columns.find((c) => c.cards.length >= 3);
      check('並び順(部分): 3枚以上あるボード列がある (以降の検証の前提)', !!t,
        b.columns.map((c) => `${c.code}:${c.cards.length}`).join(','));
      if (t) {
        const code = t.code;
        const all = t.cards.map((c) => c.id);
        // まず列全体を固定 (A,B,C,...)
        await call('POST', '/api/board/reorder', { view: 'main', col: code, items: all.map((id) => ({ id })) });
        // 次に「B だけ絞り込みで隠れている画面」から A と C を入れ替えて保存
        const visible = [all[2], all[0], ...all.slice(3)];
        const rp = await call('POST', '/api/board/reorder', { view: 'main', col: code, items: visible.map((id) => ({ id })) });
        check('並び順(部分): 一部だけ送っても受け付ける', rp.status === 200 && rp.json.ok, JSON.stringify(rp.json));
        const after2 = wfp.boardData(db, {}).columns.find((c) => c.code === code).cards.map((c) => c.id);
        check('並び順(部分): 送ったカードだけ入れ替わり、隠れていたカードは動かない',
          after2.join(',') === [all[2], all[1], all[0], ...all.slice(3)].join(','),
          `${after2.join(',')} != ${[all[2], all[1], all[0], ...all.slice(3)].join(',')}`);
        db.prepare(`DELETE FROM ph_board_order`).run();
      }
    }

    const rBad = await call('POST', '/api/board/reorder', { view: 'main', col: '', items: [{ id: 1 }] });
    check('並び順: 列の指定が無ければ拒否', rBad.status >= 400, JSON.stringify(rBad.json));
    const rNoItems = await call('POST', '/api/board/reorder', { view: 'main', col: 'basic_info' });
    check('並び順: items 無しは 400', rNoItems.status === 400, JSON.stringify(rNoItems.json));
    // 入力の検証 (黙って main / top に倒さない — Codex R1 高)
    const rCol = await call('POST', '/api/board/reorder', { view: 'main', col: 'ZZZ-NOPE', items: [{ id: wfDraftId }] });
    check('並び順: 存在しない列は 400', rCol.status === 400, JSON.stringify(rCol.json));
    const rView = await call('POST', '/api/board/reorder', { view: 'bogus', col: 'basic_info', items: [{ id: wfDraftId }] });
    check('並び順: 不正なビューは 400 (黙って本流にしない)', rView.status === 400, JSON.stringify(rView.json));
    const rKind = await call('POST', '/api/board/reorder', { view: 'image', col: 'request', items: [{ id: wfDraftId, kind: 'bogus' }] });
    check('並び順: 不正な画像種別は 400 (黙って TOP にしない)', rKind.status === 400, JSON.stringify(rKind.json));
    const rDup = await call('POST', '/api/board/reorder', {
      view: 'main', col: 'basic_info', items: [{ id: wfDraftId }, { id: wfDraftId }],
    });
    check('並び順: 同じカードの重複は 400', rDup.status === 400, JSON.stringify(rDup.json));
    const rGhost = await call('POST', '/api/board/reorder', { view: 'main', col: 'basic_info', items: [{ id: 999999 }] });
    check('並び順: 存在しない商品は 400', rGhost.status === 400, JSON.stringify(rGhost.json));
    check('並び順: 検証で弾かれた要求は 1 行も書かない',
      db.prepare('SELECT COUNT(*) AS n FROM ph_board_order').get().n === 0);

    // 戻り先: ボードから開いたら見ていたボードへ。不正な値は一覧に倒す (外部URLへ飛ばさない)
    check('戻り先: back なしは一覧', backLink.backLinkOf({}).url === '/apps/product-hub/list');
    check('戻り先: 画像ビュー + 種別 + 未割り当てを復元',
      backLink.backLinkOf({ back: 'v=board&view=image&kind=detail&filter=unassigned' }).url
      === '/apps/product-hub/board?view=image&filter=unassigned&kind=detail',
      backLink.backLinkOf({ back: 'v=board&view=image&kind=detail&filter=unassigned' }).url);
    check('戻り先: 本流ビューは素のボード',
      backLink.backLinkOf({ back: 'v=board' }).url === '/apps/product-hub/board');
    check('戻り先: 担当者は me か数字だけ通す',
      backLink.backLinkOf({ back: 'v=board&assignee=me' }).url === '/apps/product-hub/board?assignee=me'
      && backLink.backLinkOf({ back: 'v=board&assignee=12' }).url === '/apps/product-hub/board?assignee=12'
      && backLink.backLinkOf({ back: 'v=board&assignee=../../evil' }).url === '/apps/product-hub/board');
    check('戻り先: 外部URLを入れても board 以外へは行かない',
      backLink.backLinkOf({ back: 'https://evil.example.com/' }).url === '/apps/product-hub/list'
      && backLink.backLinkOf({ back: 'v=board&view=//evil.example.com' }).url === '/apps/product-hub/board');
    check('戻り先: 種別は画像ビューのときだけ効く',
      backLink.backLinkOf({ back: 'v=board&kind=detail' }).url === '/apps/product-hub/board');
    check('戻り先: filter=checking も復元される (確認中で絞った画面に戻す — 2026-08-31)',
      backLink.backLinkOf({ back: 'v=board&filter=checking' }).url === '/apps/product-hub/board?filter=checking',
      backLink.backLinkOf({ back: 'v=board&filter=checking' }).url);
    check('戻り先: 未知の filter は落とす',
      backLink.backLinkOf({ back: 'v=board&filter=whatever' }).url === '/apps/product-hub/board');
    // ボードのカードリンクに戻り先の印が付いている (これが無いと詳細から一覧に戻ってしまう)
    const boardHtml = await (await fetch(base + '/board?view=image')).text();
    check('戻り先: ボードのカードリンクに back= が付く',
      boardHtml.includes('back=v%3Dboard%26view%3Dimage'),
      boardHtml.slice(boardHtml.indexOf('kb-card-link'), boardHtml.indexOf('kb-card-link') + 200));
  }

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

  // ─── 既存の楽天ページへの追加か (2026-09-25 スタッフ要望「既存ページラベル」): 実ルート ───
  // router が詳細画面へ判定を渡し忘れると detail が 500 になる → 描画テストではなく実ルートで見る
  {
    const idEp = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DRV-EXISTPAGE', '既存ページテスト', 'smoke')
    `).run().lastInsertRowid);
    const epRow = () => db.prepare('SELECT existing_page FROM product_drafts WHERE id = ?').get(idEp).existing_page;
    const pg0 = await (await fetch(`${base}/detail/${idEp}`)).text();
    check('既存ページ: 詳細に選択欄が出る (既定 = 自動で判定)',
      pg0.includes('id="f-existing-page"') && /<option value=""\s+selected>自動で判定/.test(pg0), pg0.slice(pg0.indexOf('f-existing-page'), pg0.indexOf('f-existing-page') + 400));
    r = await call('POST', `/api/drafts/${idEp}/existing-page`, { value: '1' });
    check('既存ページ: 「既存ページに追加」を保存できる',
      r.status === 200 && r.json.existingPage === true && r.json.auto === false && epRow() === 1, JSON.stringify(r.json));
    const pg1 = await (await fetch(`${base}/detail/${idEp}`)).text();
    check('既存ページ: 保存した値が詳細の選択欄に出る', /<option value="1"\s+selected>/.test(pg1));
    const board = await (await fetch(`${base}/board`)).text();
    const at = board.indexOf(`data-draft="${idEp}"`);
    const cardHtml = at === -1 ? '' : board.slice(at, board.indexOf('kb-card-top', at));
    check('既存ページ: ボードのカードに「📄 既存ページ」の札が出る',
      cardHtml.includes('kb-tag existing-page') && cardHtml.includes('既存ページ'), at === -1 ? 'カードがボードに無い' : cardHtml.slice(0, 600));
    r = await call('POST', `/api/drafts/${idEp}/existing-page`, { value: 'x' });
    check('既存ページ: 不正な値は 400 で保存しない', r.status === 400 && epRow() === 1, JSON.stringify(r.json));
    r = await call('POST', `/api/drafts/${idEp}/existing-page`, { value: '' });
    check('既存ページ: 「自動で判定」に戻せる (NULL)', r.status === 200 && r.json.auto === true && epRow() == null, JSON.stringify(r.json));
    const board2 = await (await fetch(`${base}/board`)).text();
    const at2 = board2.indexOf(`data-draft="${idEp}"`);
    check('既存ページ: 自動判定で新規ならボードに札を出さない',
      at2 !== -1 && !board2.slice(at2, board2.indexOf('kb-card-top', at2)).includes('existing-page'));
    // 「出品・展開」の列: 既存ページのカードには出品ボタンを出さず、RMS で直す案内を出す。
    // 列に落としたときの出品の確認も出さない (data-rk=existing)
    await call('POST', `/api/drafts/${idEp}/existing-page`, { value: '1' });
    wfpEarly.ensureProgress(db, idEp);
    db.prepare(`UPDATE draft_step_progress SET state = 'done' WHERE draft_id = ?
      AND step_code IN ('basic_info', 'ai_generate', 'desc_review', 'title_approve', 'set_review')`).run(idEp);
    const board3 = await (await fetch(`${base}/board`)).text();
    const at3 = board3.indexOf(`data-draft="${idEp}"`);
    // 次のカードの頭 (<div class="kb-card ..."> / 列の終わり) まで。kb-card-link などカードの中身では切らない
    const next3 = at3 === -1 ? -1 : board3.slice(at3).search(/<div class="kb-card[ "]|class="kb-col[ "]/);
    const end3 = next3 === -1 ? -1 : at3 + next3;
    const card3 = at3 === -1 ? '' : board3.slice(at3, end3 === -1 ? undefined : end3);
    check('既存ページ: 出品・展開の列では出品ボタンの代わりに RMS で直す案内',
      card3.includes('既存ページに追加: 楽天は RMS でページ') && !card3.includes('⚡ 楽天に出品'), card3.slice(0, 1500));
    check('既存ページ: 列に落としても出品の確認を出さない印 (data-rk=existing)', card3.includes('data-rk="existing"'), card3.slice(0, 400));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idEp);
  }

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

  // ─── 画像の引き継ぎ計画: 実ルートで画面と API を通す (§4.7) ───
  // テンプレートの描画テストだけでは router の渡し忘れを検知できない (#1181 の教訓)
  {
    const pIm = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('HTTP-IMG-P', '画像計画HTTPの親', 'smoke')
    `).run().lastInsertRowid);
    db.prepare(`INSERT INTO draft_images (draft_id, drive_file_id, sort)
      VALUES (?, 'hp-top', 0), (?, 'hp-01', 1), (?, 'hp-02', 2)`).run(pIm, pIm, pIm);
    wfp.progressOf(pIm, { db });
    const ver = () => wfp.progressOf(pIm, { db }).main.find((x) => x.step_code === 'set_review').version;
    const made = sd.createSetDraft(pIm, { mode: 'ai', parent_step_version: ver() }, 'smoke',
      { isAdmin: true, actorStaffId: null });
    const sIm = made.draftId;

    const htmlIm = await (await fetch(`${base}/detail/${sIm}`)).text();
    check('画像の計画: セットの詳細を実ルートで開ける (200 で表が描ける)',
      htmlIm.includes('親の画像をどう使うか') && htmlIm.includes('TOP画像'), htmlIm.slice(0, 150));

    let rIm = await call('PUT', `/api/drafts/${sIm}/set-image-plan`, {
      items: [{ slot: 1, action: 'reuse' }, { slot: 2, action: 'modify', instruction: '2個並べた写真に' },
        { slot: 3, action: 'reuse' }],
    });
    check('画像の計画: API で置き換えられる',
      rIm.status === 200 && rIm.json.ok === true
      && rIm.json.plans.find((x) => x.slot === 2)?.instruction === '2個並べた写真に',
      JSON.stringify(rIm.json).slice(0, 200));
    check('画像の計画: 「直して使う」にすると制作工程が動き出す',
      rIm.json.needsProduction === true);
    check('画像の計画: その枠の画像は空に戻る (制作の成果物を待つ)',
      !db.prepare('SELECT 1 FROM draft_images WHERE draft_id = ? AND drive_file_id = ?').get(sIm, 'hp-01'));

    rIm = await call('PUT', `/api/drafts/${sIm}/set-image-plan`, {
      items: [{ slot: 1, action: 'reuse' }, { slot: 2, action: 'modify', instruction: '' },
        { slot: 3, action: 'reuse' }],
    });
    check('画像の計画: 「直して使う」で指示が空なら 400', rIm.status === 400, JSON.stringify(rIm.json));
    rIm = await call('PUT', `/api/drafts/${pIm}/set-image-plan`, { items: [{ slot: 1, action: 'reuse' }] });
    check('画像の計画: 単品には作らせない', rIm.status === 400, JSON.stringify(rIm.json));

    // 依頼の内容が画面に出る (作る人がここだけ見れば分かる)
    const htmlIm2 = await (await fetch(`${base}/detail/${sIm}`)).text();
    check('画像の計画: 依頼に載る指示が画面に出る',
      htmlIm2.includes('この画像を作ってください') && htmlIm2.includes('2個並べた写真に'), '');

    // 🚨 空けた枠に、画面から足した画像がそのまま入る (Codex R1 medium)。
    //    末尾に足すだけだと「商品画像1を直して」と空けた枠に届かない
    const addImg = await call('POST', `/api/drafts/${sIm}/images`,
      { url: 'https://drive.google.com/file/d/made-by-http/view' });
    check('画像の計画: 空いた枠に、画面から足した画像が入る (末尾ではなく途中の空きへ)',
      addImg.status === 200
      && db.prepare('SELECT sort FROM draft_images WHERE draft_id = ? AND drive_file_id = ?')
        .get(sIm, 'made-by-http')?.sort === sip.imageSortOfSlot(2),
      JSON.stringify(db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ? ORDER BY sort').all(sIm)));

    // 🚨 「使わない」で空けた枠が前にあっても、**作ることにした枠**に入る (Codex R2 medium)。
    //    単に最小の空きへ入れると、依頼した枠ではなく使わない枠が埋まってしまう
    await call('PUT', `/api/drafts/${sIm}/set-image-plan`, {
      // slot 1 を「使わない」で空け、slot 2 は指示を変える (= 作り直しが要る = 枠が空く)。
      // 空きが 2 つできるので、どちらへ入るかで判定できる
      items: [{ slot: 1, action: 'drop' }, { slot: 2, action: 'modify', instruction: '3個並べた写真に' },
        { slot: 3, action: 'reuse' }],
    });
    const add2 = await call('POST', `/api/drafts/${sIm}/images`,
      { url: 'https://drive.google.com/file/d/made-for-modify/view' });
    check('画像の計画: 「使わない」で空いた枠より、「作る」ことにした枠を先に埋める',
      add2.status === 200
      && db.prepare('SELECT sort FROM draft_images WHERE draft_id = ? AND drive_file_id = ?')
        .get(sIm, 'made-for-modify')?.sort === sip.imageSortOfSlot(2),
      JSON.stringify(db.prepare('SELECT drive_file_id, sort FROM draft_images WHERE draft_id = ? ORDER BY sort').all(sIm)));

    db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(sIm, pIm);
  }

  // ─── 配送方法: 「選んでいない」が保存で勝手に埋まらないこと (2026-09-05 / Codex high) ───
  // 画面は全項目をまとめて送る (collectRakutenFields) ので、未選択のとき NE の値を
  // **選択済みで描く**と、配送方法に触れずジャンルだけ保存した人が「選んだ」ことになってしまう。
  // 出品ゲートは「人が選んでいないと出せない」なので、そこが崩れる
  {
    const idSh = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('SHIP-PICK', '配送未選択テスト', 'smoke')
    `).run().lastInsertRowid);
    db.prepare(`
      INSERT OR REPLACE INTO mirror_products
        (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 配送方法, updated_at)
      VALUES (99450, 'SHIP-PICK', '配送未選択テスト', '1', '取扱中', 'ok', 'ネコポス', '2026-09-05T00:00:00Z')
    `).run();
    const shipOf = () => db.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(idSh);

    // 検査は**配送方法の select の中だけ**に限定する (画面には他にも select があり、
    // HTML 全体を正規表現で見ると別の select の状態で結果が変わる — Codex R2 medium)
    const shipSelect = (html) => (html.match(/<select id="rk-shipping-group"[\s\S]*?<\/select>/) || [''])[0];

    // ① NE には配送方法があるが、DB は未選択のまま
    const html0 = await (await fetch(`${base}/detail/${idSh}`)).text();
    const sel0 = shipSelect(html0);
    check('配送: 未選択の商品は「選んでください」が選ばれた状態で描く (NEの値を勝手に選ばない)',
      /<option value="" selected>— 選んでください<\/option>/.test(sel0)
      && !/<option value="5"[^>]*selected/.test(sel0),
      sel0.slice(0, 300));
    check('配送: NE の値は「候補」として出し、押して初めて入る',
      html0.includes('id="rk-ship-use-ne"') && html0.includes('未選択です') && html0.includes('ネコポス'),
      '');

    // ② 配送方法に触れずジャンルだけ保存 → 未選択のままであること
    let r0 = await call('POST', `/api/drafts/${idSh}/rakuten`, { genre_id: '565004', shipping_method_group: '' });
    check('配送: ジャンルだけ保存しても配送方法は未選択のまま (勝手に確定しない)',
      r0.status === 200 && !shipOf()?.shipping_method_group, JSON.stringify(shipOf()));
    check('配送: 未選択のままでは出品できない',
      (listing.buildItemPayload(db, idSh).reasons || []).some((x) => /配送方法を選んでください/.test(x)),
      JSON.stringify(listing.buildItemPayload(db, idSh).reasons || []));

    // ③ 人が選んで保存すれば入る (「これにする」= NE の値でも、選んだのは人)
    r0 = await call('POST', `/api/drafts/${idSh}/rakuten`, { genre_id: '565004', shipping_method_group: '5' });
    check('配送: 人が選んで保存すれば入り、その値で描かれる',
      r0.status === 200 && shipOf()?.shipping_method_group === '5', JSON.stringify(shipOf()));
    const html1 = await (await fetch(`${base}/detail/${idSh}`)).text();
    const sel1 = shipSelect(html1);
    check('配送: 選んだあとは選んだ値が選択状態になる',
      /<option value="5"[^>]*selected/.test(sel1)
      && !/<option value="" selected>— 選んでください/.test(sel1), sel1.slice(0, 300));
    check('配送: 選んだあとは未選択の警告を出さない', !html1.includes('未選択です'));

    // ④ セットは NE の値をそのまま使うので、選ばなくても出品できる。
    //    ここで単品と同じ「出品できません」を出すと嘘になる (Codex R2 medium)
    const idShSet = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, created_by, parent_draft_id, provisional_code)
      VALUES ('SHIP-PICK-SET', '配送未選択のセット', 'smoke', ?, 0)
    `).run(idSh).lastInsertRowid);
    db.prepare(`
      INSERT OR REPLACE INTO mirror_products
        (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 配送方法, updated_at)
      VALUES (99451, 'SHIP-PICK-SET', '配送未選択のセット', '1', '取扱中', 'ok', 'ネコポス', '2026-09-05T00:00:00Z')
    `).run();
    const htmlSet = await (await fetch(`${base}/detail/${idShSet}`)).text();
    check('配送: セットは「NE の配送方法をそのまま使う」と案内する (出品できない扱いにしない)',
      htmlSet.includes('セットは NE の配送方法') && !htmlSet.includes('未選択です（このままでは出品できません）'),
      htmlSet.includes('未選択です（このままでは出品できません）') ? '単品と同じ警告が出ている' : '案内が出ていない');
    check('配送: セットは選んでいなくても配送方法では止まらない',
      !(listing.buildItemPayload(db, idShSet).reasons || []).some((x) => /配送方法/.test(x)),
      JSON.stringify(listing.buildItemPayload(db, idShSet).reasons || []));
    db.prepare('DELETE FROM mirror_products WHERE product_id = 99451').run();
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idShSet);

    db.prepare('DELETE FROM mirror_products WHERE product_id = 99450').run();
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idSh);
  }

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

// ─── drive-image-folder (画像フォルダ「商品コード_商品名」の自動作成、2026-08-27) ───
{
  const dif = await import('../services/drive-image-folder.js');
  // フォルダ名: G: 同期される親フォルダなので Windows 禁止文字は全角へ寄せる
  check('フォルダ名: 商品コード_商品名', dif.buildImageFolderName('sgs-or', 'メガネストラップ') === 'sgs-or_メガネストラップ');
  check('フォルダ名: Windows禁止文字を全角へ', dif.buildImageFolderName('a/b', 'x:y*z?"<>|') === 'a／b_x：y＊z？”＜＞｜');
  check('フォルダ名: 空白圧縮+末尾ドット除去', dif.buildImageFolderName(' c1 ', ' 商品  名 .. ') === 'c1_商品 名');
  check('フォルダ名: 商品名が空ならコードのみ', dif.buildImageFolderName('code-1', '  ') === 'code-1');
  check('フォルダ名: 100字で切る', dif.buildImageFolderName('X', 'あ'.repeat(200)).length <= 100);

  check('単品判定: 通常ドラフトは対象', dif.isSingleProductDraft({ parent_draft_id: null, provisional_code: 0 }) === true);
  check('単品判定: セット派生 (仮コード) は対象外', dif.isSingleProductDraft({ parent_draft_id: 1, provisional_code: 1 }) === false);
  check('単品判定: コード確定後のセットも対象外', dif.isSingleProductDraft({ parent_draft_id: 1, provisional_code: 0 }) === false);

  const setDifId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, parent_draft_id, provisional_code, created_by)
    VALUES ('SET-DIF-01', 'セット派生', 999, 1, 'smoke')`).run().lastInsertRowid);
  check('セット派生はフォルダを作らない', (await dif.attemptImageFolderCreation(setDifId, {})).outcome === 'skipped_set');
  check('URL入力済みのカードは触らない', (await dif.attemptImageFolderCreation(fdraft.id, {})).outcome === 'skipped_has_url');

  // SA 鍵なし → disabled (fail-soft。カード作成は成功のまま)
  const difId = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-1', 'フォルダ自動作成', 'smoke')`).run().lastInsertRowid);
  delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  check('SA鍵なしは disabled', (await dif.attemptImageFolderCreation(difId, {})).outcome === 'disabled');

  // 成功パス (fake client 注入・API 非接続)
  const fakeDrive = (listFiles, createdId) => ({
    files: {
      list: async () => ({ data: { files: listFiles } }),
      create: async () => ({ data: { id: createdId } }),
    },
  });
  const r1 = await dif.attemptImageFolderCreation(difId, { actor: 'smoke', driveClient: fakeDrive([], 'NEW-FOLDER-ID') });
  check('新規作成で drive_folder_url が入る', r1.outcome === 'created' && r1.url.endsWith('NEW-FOLDER-ID')
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difId).drive_folder_url.endsWith('NEW-FOLDER-ID'),
    JSON.stringify(r1));
  check('drive_folder_created イベント記録',
    db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'drive_folder_created'`).get(difId).c === 1);
  check('2回目は URL 済みで skip', (await dif.attemptImageFolderCreation(difId, { driveClient: fakeDrive([], 'X') })).outcome === 'skipped_has_url');

  // 冪等: 親フォルダ直下に同名フォルダがあれば再利用 (二重作成しない)
  const difId2 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-2', '再利用', 'smoke')`).run().lastInsertRowid);
  const r2 = await dif.attemptImageFolderCreation(difId2, { driveClient: fakeDrive([{ id: 'EXIST-ID', name: 'x' }], 'unused') });
  check('同名フォルダは再利用', r2.outcome === 'reused' && r2.url.endsWith('EXIST-ID'), JSON.stringify(r2));

  // レース: Drive 作成中に人が URL を貼ったら人の入力を正とする (上書きしない)
  const difId3 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-3', 'レース', 'smoke')`).run().lastInsertRowid);
  const racingDrive = {
    files: {
      list: async () => {
        db.prepare(`UPDATE product_drafts SET drive_folder_url = 'https://drive.google.com/drive/folders/HUMAN2' WHERE id = ?`).run(difId3);
        return { data: { files: [] } };
      },
      create: async () => ({ data: { id: 'AUTO-ID' } }),
    },
  };
  const r3 = await dif.attemptImageFolderCreation(difId3, { driveClient: racingDrive });
  check('作成中の手入力 URL を上書きしない (url は残った方を返す)', r3.outcome === 'kept_manual_url'
    && r3.url.endsWith('HUMAN2') && r3.unusedFolderUrl.endsWith('AUTO-ID')
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difId3).drive_folder_url.endsWith('HUMAN2'),
    JSON.stringify(r3));

  // 同一 draft への同時呼び出しは1本にまとまる (list→create の隙間の二重作成防止)
  const difId5 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-5', '同時実行', 'smoke')`).run().lastInsertRowid);
  let createCalls = 0;
  const slowDrive = {
    files: {
      list: async () => { await new Promise((r) => setTimeout(r, 20)); return { data: { files: [] } }; },
      create: async () => { createCalls += 1; return { data: { id: 'ONCE-ID' } }; },
    },
  };
  const [c1, c2] = await Promise.all([
    dif.attemptImageFolderCreation(difId5, { driveClient: slowDrive }),
    dif.attemptImageFolderCreation(difId5, { driveClient: slowDrive }),
  ]);
  check('同時2回の呼び出しで create は1回', createCalls === 1 && c1.outcome === 'created' && c2.outcome === 'created',
    JSON.stringify({ createCalls, c1, c2 }));

  // 壊れた SA 鍵でも throw せず failed (登録 API を 500 にしない)
  const difIdBadKey = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-BADKEY', '鍵破損', 'smoke')`).run().lastInsertRowid);
  process.env.GOOGLE_SERVICE_ACCOUNT_KEY = 'not-base64-json!!';
  const rBad = await dif.attemptImageFolderCreation(difIdBadKey, {});
  delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  check('壊れた SA 鍵は failed (throw しない)', rBad.outcome === 'failed' && !!rBad.error
    && db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'drive_folder_failed'`).get(difIdBadKey).c === 1,
    JSON.stringify(rBad));

  // Drive 失敗は fail-soft + イベント記録
  const difId4 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-4', '失敗', 'smoke')`).run().lastInsertRowid);
  const r4 = await dif.attemptImageFolderCreation(difId4, { driveClient: { files: { list: async () => { throw new Error('boom'); } } } });
  check('Drive失敗は fail-soft + drive_folder_failed', r4.outcome === 'failed' && r4.error.includes('boom')
    && db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'drive_folder_failed'`).get(difId4).c === 1,
    JSON.stringify(r4));

  // バッチは途中の失敗で止まらない (1件目 throw → 2件目は作成される)
  const difId6 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-6', 'バッチ続行', 'smoke')`).run().lastInsertRowid);
  let batchCall = 0;
  const flakyDrive = {
    files: {
      list: async () => { batchCall += 1; if (batchCall === 1) throw new Error('flaky'); return { data: { files: [] } }; },
      create: async () => ({ data: { id: 'BATCH-ID' } }),
    },
  };
  const difId7 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-7', 'バッチ2件目', 'smoke')`).run().lastInsertRowid);
  const s = await dif.attemptImageFolderCreationBatch([difId6, difId7, difId2, 999999], { driveClient: flakyDrive });
  check('バッチは失敗で止まらない (失敗1/作成1/skip2)',
    s.failed === 1 && s.created === 1 && s.skipped === 2, JSON.stringify(s));

  // 失敗の回収: drive_folder_failed のまま URL 空の単品だけ再試行する (バックフィルはしない)。
  // 直近失敗 (backoff 中) と総試行上限超えは対象外 = 恒久失敗を毎回叩かない (Codex R2 high)
  const difId8 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-8', '再試行対象', 'smoke')`).run().lastInsertRowid);
  const insOldFail = db.prepare(`INSERT INTO draft_events (draft_id, event, detail, actor, created_at)
    VALUES (?, 'drive_folder_failed', 'old-fail', 'smoke', strftime('%Y-%m-%dT%H:%M:%fZ','now','-3 hours'))`);
  insOldFail.run(difId8);
  const difId9 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-9', '上限超え', 'smoke')`).run().lastInsertRowid);
  for (let i = 0; i < dif.MAX_RETRY_ATTEMPTS; i++) insOldFail.run(difId9);
  const rt1 = await dif.retryFailedImageFolders({ driveClient: fakeDrive([], 'RETRY-ID') });
  check('再試行: 古い失敗だけ回収 (直近失敗=backoff中・上限超えは対象外)',
    rt1.retried === 1 && rt1.created === 1 && rt1.failed === 0
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difId8).drive_folder_url.endsWith('RETRY-ID')
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difId9).drive_folder_url == null
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difId4).drive_folder_url == null,
    JSON.stringify(rt1));
  const rt2 = await dif.retryFailedImageFolders({ driveClient: fakeDrive([], 'X') });
  check('再試行: 回収済みなら対象なし', rt2.retried === 0, JSON.stringify(rt2));

  // limit 到達時は「古い失敗」から優先 (先頭 id 固定の飢餓にならない)
  const oldFailAt = (id, hoursAgo) => db.prepare(`INSERT INTO draft_events (draft_id, event, detail, actor, created_at)
    VALUES (?, 'drive_folder_failed', 'old-fail', 'smoke', strftime('%Y-%m-%dT%H:%M:%fZ','now','-' || ? || ' hours'))`).run(id, hoursAgo);
  const difIdOldA = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-OLD-A', '5時間前', 'smoke')`).run().lastInsertRowid);
  const difIdOldB = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-OLD-B', '4時間前', 'smoke')`).run().lastInsertRowid);
  const difIdOldC = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DIF-OLD-C', '3時間前', 'smoke')`).run().lastInsertRowid);
  oldFailAt(difIdOldC, 3); oldFailAt(difIdOldA, 5); oldFailAt(difIdOldB, 4);
  const rt3 = await dif.retryFailedImageFolders({ limit: 2, driveClient: fakeDrive([], 'OLDEST-ID') });
  check('再試行: limit 超過時は古い失敗から優先 (新しい方は次回へ)',
    rt3.retried === 2 && rt3.created === 2
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difIdOldA).drive_folder_url.endsWith('OLDEST-ID')
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difIdOldB).drive_folder_url.endsWith('OLDEST-ID')
    && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(difIdOldC).drive_folder_url == null,
    JSON.stringify(rt3));

  // 不正な PH_IMAGE_FOLDER_PARENT_ID は既定値へフォールバック (Drive クエリを壊さない)
  process.env.PH_IMAGE_FOLDER_PARENT_ID = "bad'id --";
  const pid = dif.imageFolderParentId();
  delete process.env.PH_IMAGE_FOLDER_PARENT_ID;
  check('不正な親フォルダIDは既定値へ', /^[A-Za-z0-9_-]+$/.test(pid) && pid !== "bad'id --", pid);
}

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

// ─── white-bg-inbox (受信箱の白抜き画像を _00 として登録 + 商品フォルダへ移動、2026-09-14) ───
{
  const wbi = await import('../services/white-bg-inbox.js');
  const { parseImageFileName } = await import('../lib/folder-import.js');
  const INBOX = '1rPzsWJaFqo4pW0JCm77ZjkXhpyG7fF4N';
  check('受信箱: 既定のフォルダ ID', wbi.whiteBgInboxFolderId() === INBOX && wbi.whiteBgInboxFolderUrl().endsWith(INBOX));
  process.env.PH_WHITE_BG_INBOX_FOLDER_ID = 'bad id!';
  check('受信箱: env が Drive ID の形式でなければ既定', wbi.whiteBgInboxFolderId() === INBOX);
  process.env.PH_WHITE_BG_INBOX_FOLDER_ID = 'CUSTOM-INBOX-1';
  check('受信箱: env で差し替えできる', wbi.whiteBgInboxFolderId() === 'CUSTOM-INBOX-1');
  delete process.env.PH_WHITE_BG_INBOX_FOLDER_ID;

  check('whiteBgFileName: 拡張子は元ファイルから (小文字化・jpeg→jpg)', wbi.whiteBgFileName('ABC-1', '20260904_x_商品.JPEG', 'image/jpeg') === 'ABC-1_00.jpg');
  check('whiteBgFileName: 拡張子が無ければ mimeType から', wbi.whiteBgFileName('abc', 'noext', 'image/png') === 'abc_00.png');
  check('whiteBgFileName: どちらも無ければ jpg', wbi.whiteBgFileName(' abc ', '', '') === 'abc_00.jpg');
  check('whiteBgFileName の結果は parseImageFileName が白抜きと読む', parseImageFileName(wbi.whiteBgFileName('abc', 'a.png', ''))?.kind === 'white');
  {
    const n = wbi.parkedName('abc_00.jpg', new Date('2026-09-14T01:30:00Z'));
    check('parkedName: _旧<JST日時> を付ける', n === 'abc_00_旧20260914-1030.jpg', n);
    check('parkedName: 退けた名前は枠として解釈されない (自動セットで拾わない)', parseImageFileName(n) === null);
    check('parkedName: 拡張子なしでも壊れない', wbi.parkedName('noext', new Date('2026-09-14T01:30:00Z')) === 'noext_旧20260914-1030');
  }
  {
    const m = wbi.parseMailDescription('From: ohata@am-craft.jp\nSubject: 新商品ASIN\nDate: Fri Sep 04 2026');
    check('parseMailDescription: From/Subject を取り出す', m.from === 'ohata@am-craft.jp' && m.subject === '新商品ASIN', JSON.stringify(m));
    check('parseMailDescription: 形式が違えば null', wbi.parseMailDescription('こんにちは').subject === null && wbi.parseMailDescription(null).from === null);
  }
  check('inboxDisplayName: 自動保存の「日時_ハッシュ_」を落とす',
    wbi.inboxDisplayName('20260904_160143_1a06b39759bc64af_ウィッグ用両面テープ60P.jpg') === 'ウィッグ用両面テープ60P.jpg'
    && wbi.inboxDisplayName('plain.jpg') === 'plain.jpg');
  check('jstDisplay: UTC → JST', wbi.jstDisplay('2026-09-04T15:19:08.569Z') === '2026/09/05 00:19' && wbi.jstDisplay('x') === '');

  // 偽 Drive: フォルダ/ファイルを Map で持ち、list/get/update/create を最小限に真似る
  function fakeDrive(seed) {
    const files = new Map();
    for (const f of seed) files.set(f.id, { trashed: false, parents: [], ...f });
    const calls = [];
    const parseQ = (q) => ({
      parent: (q.match(/'([^']+)' in parents/) || [])[1],
      folderOnly: q.includes("mimeType = 'application/vnd.google-apps.folder'"),
      imageOnly: q.includes("mimeType contains 'image/'"),
      name: (q.match(/name = '((?:[^'\\]|\\.)*)'/) || [])[1],
    });
    const drive = {
      failUpdate: null,
      files_: files,
      calls,
      files: {
        list: async (params) => {
          calls.push(['list', params]);
          const c = parseQ(params.q || '');
          const out = [...files.values()].filter((f) => !f.trashed
            && (!c.parent || f.parents.includes(c.parent))
            && (!c.folderOnly || f.mimeType === 'application/vnd.google-apps.folder')
            && (!c.imageOnly || String(f.mimeType).startsWith('image/'))
            && (c.name == null || f.name === c.name.replace(/\\(.)/g, '$1')));
          // ページ送りを真似る (pageSize / pageToken = offset)。orderBy は無視 = 並びはサービス側で揃える前提 (Codex R1 low)
          const size = Number(params.pageSize) || 100;
          const offset = Number(params.pageToken || 0);
          const page = out.slice(offset, offset + size);
          const next = offset + size < out.length ? String(offset + size) : undefined;
          return { data: { files: page.map((f) => ({ ...f })), nextPageToken: next } };
        },
        get: async (params) => {
          calls.push(['get', params]);
          const f = files.get(params.fileId);
          if (!f) { const e = new Error('File not found'); e.code = 404; throw e; }
          return { data: { ...f } };
        },
        update: async (params) => {
          calls.push(['update', params]);
          const f = files.get(params.fileId);
          if (!f) { const e = new Error('File not found'); e.code = 404; throw e; }
          if (typeof drive.failUpdate === 'function') drive.failUpdate(params, f);
          else if (drive.failUpdate) throw new Error(drive.failUpdate);
          if (params.removeParents) f.parents = f.parents.filter((p) => p !== params.removeParents);
          if (params.addParents) f.parents = [...f.parents, params.addParents];
          if (params.requestBody?.name) f.name = params.requestBody.name;
          f.modifiedTime = '2026-09-14T02:00:00.000Z';
          return { data: { ...f } };
        },
        create: async (params) => {
          calls.push(['create', params]);
          const id = 'NEW-FOLDER-' + String(files.size + 1).padStart(4, '0');
          files.set(id, { id, trashed: false, ...params.requestBody });
          return { data: { id } };
        },
      },
    };
    return drive;
  }
  const img = (id, name, parent, extra = {}) => ({ id, name, mimeType: 'image/jpeg', parents: [parent], createdTime: '2026-09-01T00:00:00Z', modifiedTime: '2026-09-01T00:00:01Z', ...extra });

  const wbDraftA = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-A', '受信箱A', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-A-0000001')`).run().lastInsertRowid);
  const wbDraftB = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('WBI-B', '受信箱B (フォルダ無し)', 'smoke')`).run().lastInsertRowid);
  db.prepare(`INSERT INTO draft_rakuten (draft_id, white_bg_drive_file_id, white_bg_drive_url) VALUES (?, 'inbox-file-old1', 'u')`).run(wbDraftB);
  // 一覧: 画像だけ・新しい順・登録済みの札
  {
    const d = fakeDrive([
      // 古い順に並べておく = 偽 Drive は orderBy を無視するので、サービス側の並べ替えが効いているかが分かる
      img('inbox-file-old1', 'old.jpg', INBOX, { createdTime: '2026-08-01T00:00:00Z' }),
      img('inbox-file-0002', '20260901_115148_1a05ae188216cb6a_パウダー18g.png', INBOX, { mimeType: 'image/png', createdTime: '2026-09-01T15:19:04Z' }),
      img('inbox-file-0001', '20260904_160143_1a06b39759bc64af_テープ60P.jpg', INBOX, { createdTime: '2026-09-04T15:19:08Z', description: 'From: ohata@am-craft.jp\nSubject: 新商品ASIN' }),
      { id: 'inbox-file-pdf1', name: 'x.pdf', mimeType: 'application/pdf', parents: [INBOX], createdTime: '2026-09-10T00:00:00Z' },
      img('elsewhere-file1', 'WBI-A_00.jpg', 'FOLDER-A-0000001'),
    ]);
    const r = await wbi.listWhiteBgInbox({ driveClient: d, db });
    check('受信箱の一覧: 受信箱の画像だけ・新しい順', r.files.map((f) => f.id).join(',') === 'inbox-file-0001,inbox-file-0002,inbox-file-old1' && r.truncated === false && r.folderUrl.endsWith(INBOX), JSON.stringify(r.files.map((f) => f.id)));
    const f1 = r.files[0];
    check('受信箱の一覧: 表示名・受信日時 (JST)・件名・差出人', f1.displayName === 'テープ60P.jpg' && f1.receivedAt === '2026/09/05 00:19' && f1.mailSubject === '新商品ASIN' && f1.mailFrom === 'ohata@am-craft.jp', JSON.stringify(f1));
    check('受信箱の一覧: どこかの商品の白抜きになっているファイルには商品コードの札', r.files[2].registeredFor === 'WBI-B' && f1.registeredFor === null);
    check('受信箱の一覧: 共有ドライブ対応で聞いている', d.calls[0][1].supportsAllDrives === true && d.calls[0][1].includeItemsFromAllDrives === true && d.calls[0][1].q.includes(`'${INBOX}' in parents`));
  }
  // SA 鍵なし・注入なし → 503 相当
  {
    delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
    let err = null;
    try { await wbi.listWhiteBgInbox({ db }); } catch (e) { err = e; }
    check('受信箱の一覧: 鍵が無ければ statusCode 503 で throw', err?.statusCode === 503, String(err));
    const r = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-0001', {});
    check('登録: 鍵が無ければ ok:false 503 (throw しない)', r.ok === false && r.status === 503, JSON.stringify(r));
  }
  // 登録の本線: 移動 + 改名 + DB + イベント
  {
    const d = fakeDrive([
      img('inbox-file-0001', '20260904_x_テープ.JPG', INBOX, { modifiedTime: '2026-09-04T15:19:10Z' }),
      img('wb-old-file-001', 'wbi-a_00.png', 'FOLDER-A-0000001', { mimeType: 'image/png' }), // 拡張子違い・大文字小文字違いでも同じ枠
      img('keep-top-file01', 'WBI-A_top.jpg', 'FOLDER-A-0000001'),
    ]);
    const r = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-0001', { actor: 'smoke', driveClient: d, now: () => new Date('2026-09-14T01:30:00Z') });
    const moveCall = d.calls.find((c) => c[0] === 'update' && c[1].fileId === 'inbox-file-0001')?.[1];
    check('登録: ok・移動済み・新しい名前', r.ok === true && r.moved === true && r.name === 'WBI-A_00.jpg' && r.originalName === '20260904_x_テープ.JPG' && r.folderUrl.endsWith('FOLDER-A-0000001'), JSON.stringify(r));
    check('登録: Drive の update は addParents=商品フォルダ / removeParents=受信箱 / name=商品コード_00',
      !!moveCall && moveCall.addParents === 'FOLDER-A-0000001' && moveCall.removeParents === INBOX && moveCall.requestBody.name === 'WBI-A_00.jpg' && moveCall.supportsAllDrives === true, JSON.stringify(moveCall));
    check('登録: 受信箱から消えて商品フォルダに入る (偽 Drive の状態)', d.files_.get('inbox-file-0001').parents.join(',') === 'FOLDER-A-0000001' && d.files_.get('inbox-file-0001').name === 'WBI-A_00.jpg');
    check('登録: 既にあった _00 (拡張子違い) は「_旧<日時>」に退け、他の枠は触らない',
      d.files_.get('wb-old-file-001').name === 'wbi-a_00_旧20260914-1030.png' && r.parked.join(',') === 'wbi-a_00_旧20260914-1030.png' && d.files_.get('keep-top-file01').name === 'WBI-A_top.jpg',
      JSON.stringify({ old: d.files_.get('wb-old-file-001').name, parked: r.parked }));
    const rk = db.prepare('SELECT white_bg_drive_file_id, white_bg_drive_url, white_bg_modified_time FROM draft_rakuten WHERE draft_id = ?').get(wbDraftA);
    check('登録: draft_rakuten に白抜きが入る (更新日時は移動後のもの)', rk.white_bg_drive_file_id === 'inbox-file-0001' && rk.white_bg_drive_url.includes('/file/d/inbox-file-0001/') && rk.white_bg_modified_time === '2026-09-14T02:00:00.000Z', JSON.stringify(rk));
    const ev = db.prepare(`SELECT detail, actor FROM draft_events WHERE draft_id = ? AND event = 'white_bg_set_from_inbox'`).all(wbDraftA);
    check('登録: イベントに 元の名前→新しい名前・退けたファイル', ev.length === 1 && ev[0].actor === 'smoke' && ev[0].detail.includes('20260904_x_テープ.JPG → WBI-A_00.jpg') && ev[0].detail.includes('wbi-a_00_旧20260914-1030.png'), JSON.stringify(ev));
    check('登録: 警告なし', r.warnings.length === 0, JSON.stringify(r.warnings));
    // 同じファイルをもう一度 (もう受信箱に無い) → 409
    const again = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-0001', { driveClient: d });
    check('登録: 受信箱に無いファイルは 409 (別の商品で登録済み・移動済みの横取り防止)', again.ok === false && again.status === 409, JSON.stringify(again));
    check('登録: 409 のときは DB を触らない', db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'white_bg_set_from_inbox'`).get(wbDraftA).c === 1);
  }
  // 入力の検証
  {
    const d = fakeDrive([
      img('trashed-file-01', 't.jpg', INBOX, { trashed: true }),
      { id: 'pdf-file-000001', name: 'x.pdf', mimeType: 'application/pdf', parents: [INBOX] },
    ]);
    check('登録: 存在しない商品は 404', (await wbi.registerWhiteBgFromInbox(999999, 'inbox-file-0001', { driveClient: d })).status === 404);
    check('登録: 不正な ID は 400', (await wbi.registerWhiteBgFromInbox(wbDraftA, "x' or 1", { driveClient: d })).status === 400);
    check('登録: Drive に無いファイルは 404', (await wbi.registerWhiteBgFromInbox(wbDraftA, 'nope-nope-nope', { driveClient: d })).status === 404);
    check('登録: ゴミ箱のファイルは 409', (await wbi.registerWhiteBgFromInbox(wbDraftA, 'trashed-file-01', { driveClient: d })).status === 409);
    check('登録: 画像でないファイルは 400', (await wbi.registerWhiteBgFromInbox(wbDraftA, 'pdf-file-000001', { driveClient: d })).status === 400);
  }
  // 🚨 移動先があるのに移動できなければ、登録もしない (2026-09-18)。
  // 以前は「移動は best-effort」で警告つきで登録していたが、SA が受信箱で閲覧者のままだった 9/14〜9/18 の間、
  // 画面には白抜きが入っているのに Drive には来ない、という食い違いだけが黙って積み上がった
  {
    const before = db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbDraftA).white_bg_drive_file_id;
    const evBefore = db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'white_bg_set_from_inbox'`).get(wbDraftA).c;
    const d = fakeDrive([
      img('inbox-file-0009', 'nine.jpg', INBOX, { modifiedTime: '2026-09-09T00:00:00Z' }),
      img('wb-keep-old-01', 'WBI-A_00.jpg', 'FOLDER-A-0000001'),  // 前の白抜き = 移動に失敗したら触られていないこと
    ]);
    d.failUpdate = 'insufficientFilePermissions';
    const r = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-0009', { driveClient: d });
    check('登録: 移動できなければ 502 で登録しない',
      r.ok === false && r.status === 502 && r.error.includes('登録していません') && r.error.includes('insufficientFilePermissions'), JSON.stringify(r));
    check('登録: 移動できなければ白抜きは前のまま (画面と Drive が食い違わない)',
      db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbDraftA).white_bg_drive_file_id === before);
    check('登録: 移動できなければ登録イベントも増えない',
      db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'white_bg_set_from_inbox'`).get(wbDraftA).c === evBefore);
    check('登録: 中止した理由は white_bg_inbox_failed に残る',
      !!db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'white_bg_inbox_failed' ORDER BY id DESC LIMIT 1`).get(wbDraftA)?.detail.includes('移動できず登録を中止'));
    check('登録: 退避は移動のあとなので、移動できなければ Drive は何も変わらない',
      d.files_.get('wb-keep-old-01').name === 'WBI-A_00.jpg' && d.files_.get('inbox-file-0009').parents.join(',') === INBOX,
      JSON.stringify({ old: d.files_.get('wb-keep-old-01').name, parents: d.files_.get('inbox-file-0009').parents }));
  }
  // 動かせないと Drive が言っているなら、何も触らずに止める (退避で旧ファイルの名前だけ変えてしまわない)
  {
    const d = fakeDrive([
      img('inbox-file-000c', 'c.jpg', INBOX, { capabilities: { canEdit: false, canMoveItemWithinDrive: false } }),
      img('wb-keep-old-02', 'WBI-A_00.jpg', 'FOLDER-A-0000001'),
    ]);
    const r = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-000c', { driveClient: d });
    check('登録: 受信箱から動かす権限が無ければ 403 (登録しない)',
      r.ok === false && r.status === 403 && r.error.includes('コンテンツ管理者'), JSON.stringify(r));
    check('登録: 403 のときは Drive に一切書き込まない',
      !d.calls.some((c) => c[0] === 'update') && d.files_.get('wb-keep-old-02').name === 'WBI-A_00.jpg');
    check('登録: 403 も white_bg_inbox_failed に残る',
      !!db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'white_bg_inbox_failed' ORDER BY id DESC LIMIT 1`).get(wbDraftA)?.detail.includes('権限が無いため'));
    const d2 = fakeDrive([img('inbox-file-000d', 'd.jpg', INBOX, { capabilities: { canEdit: true, canMoveItemWithinDrive: true } })]);
    const ok = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-000d', { driveClient: d2 });
    check('登録: 動かせると分かっていれば今までどおり移動して登録', ok.ok === true && ok.moved === true, JSON.stringify(ok));
  }
  // 一覧: 受信箱から画像を出せるか (画像を選ぶ前に権限不足へ気づけるように)
  {
    const inboxFolder = (cap) => ({ id: INBOX, name: '受信箱', mimeType: 'application/vnd.google-apps.folder', parents: [], capabilities: cap });
    const listWith = async (cap) => (await wbi.listWhiteBgInbox({
      driveClient: fakeDrive([inboxFolder(cap), img('inbox-file-000w', 'w.jpg', INBOX)]), db,
    })).writable;
    check('一覧: 受信箱が読み取り専用なら writable=false',
      (await listWith({ canEdit: false, canAddChildren: false, canMoveChildrenWithinDrive: false })) === false);
    // 🚨 共有ドライブの「投稿者」は追加も編集もできるが、フォルダから出せない (Codex R1 medium)
    check('一覧: 追加・編集はできても外へ出せなければ writable=false',
      (await listWith({ canEdit: true, canAddChildren: true, canMoveChildrenWithinDrive: false })) === false);
    check('一覧: 移動の可否が返らなければ writable=null (できると決めつけない)',
      (await listWith({ canEdit: true, canAddChildren: true })) === null);
    const dRw = fakeDrive([inboxFolder({ canEdit: true, canAddChildren: true, canMoveChildrenWithinDrive: true }), img('inbox-file-000w', 'w.jpg', INBOX)]);
    const rRw = await wbi.listWhiteBgInbox({ driveClient: dRw, db });
    check('一覧: 出せるなら writable=true (フォルダ自身は一覧に混ざらない)',
      rRw.writable === true && rRw.files.length === 1 && rRw.files[0].id === 'inbox-file-000w', JSON.stringify(rRw.files.map((f) => f.id)));
    const dUnknown = fakeDrive([img('inbox-file-000w', 'w.jpg', INBOX)]);
    check('一覧: 受信箱を読めなければ writable=null (一覧そのものは出す)', (await wbi.listWhiteBgInbox({ driveClient: dUnknown, db })).writable === null);
  }
  // 事前確認をすり抜けても、移動の失敗が権限不足なら 403 に分ける (やり直しても直らないため — Codex R1)
  {
    const d = fakeDrive([img('inbox-file-perm1', 'pm.jpg', INBOX, { capabilities: { canEdit: false, canMoveItemWithinDrive: false } })]);
    let gets = 0;
    const g = d.files.get;
    d.files.get = async (p) => { gets += 1; const r = await g(p); if (gets === 1) delete r.data.capabilities; return r; }; // 1 回目 = 事前確認は素通り
    d.failUpdate = 'insufficientFilePermissions';
    const r = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-perm1', { driveClient: d });
    check('登録: 移動失敗の原因が権限不足なら 403 (一時的な不調の 502 と分ける)',
      r.ok === false && r.status === 403 && r.error.includes('コンテンツ管理者'), JSON.stringify(r));
    check('登録: 403 の理由は white_bg_inbox_failed に「権限不足」と残る',
      !!db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'white_bg_inbox_failed' ORDER BY id DESC LIMIT 1`).get(wbDraftA)?.detail.includes('権限不足'));
  }
  // 受信箱側は動かせても移動先フォルダに足せないことがある。HTTP 403 だけで決めず reason で分ける (Codex R2 medium)
  {
    const wbU = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-U', '移動先の権限', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-U-0000001')`).run().lastInsertRowid);
    const ok = { canEdit: true, canMoveItemWithinDrive: true };   // 受信箱側は問題なし = capabilities では気づけない
    const driveErr = (message, reason, code = 403) => () => { const e = new Error(message); e.code = code; e.errors = [{ reason }]; throw e; };
    const d = fakeDrive([img('inbox-file-dest1', 'du.jpg', INBOX, { capabilities: ok })]);
    d.failUpdate = driveErr('The user does not have sufficient permissions for this file.', 'insufficientFilePermissions');
    const r = await wbi.registerWhiteBgFromInbox(wbU, 'inbox-file-dest1', { driveClient: d });
    check('登録: 移動先フォルダの権限不足も 403 + 受信箱と商品フォルダの両方を案内',
      r.ok === false && r.status === 403 && r.error.includes('両方') && r.error.includes('画像フォルダ'), JSON.stringify(r));
    // レート制限も 403 で来るが、こちらは待てば通る = 502 (権限の案内を出さない)
    const d2 = fakeDrive([img('inbox-file-dest2', 'du2.jpg', INBOX, { capabilities: ok })]);
    d2.failUpdate = driveErr('Rate Limit Exceeded', 'userRateLimitExceeded');
    const r2 = await wbi.registerWhiteBgFromInbox(wbU, 'inbox-file-dest2', { driveClient: d2 });
    check('登録: レート制限 (403 だが reason が別) は 502 でやり直しを促す', r2.ok === false && r2.status === 502, JSON.stringify(r2));
    check('登録: どちらも DB は変えない', db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbU) == null);
    // e.errors = [] (空配列) は truthy なので、応答本体の reason を隠さないこと (Codex R3 low)
    const d3 = fakeDrive([img('inbox-file-dest3', 'du3.jpg', INBOX, { capabilities: ok })]);
    d3.failUpdate = () => {
      const e = new Error('Insufficient permissions');
      e.code = 403; e.errors = [];
      e.response = { data: { error: { errors: [{ reason: 'insufficientFilePermissions' }] } } };
      throw e;
    };
    const r3 = await wbi.registerWhiteBgFromInbox(wbU, 'inbox-file-dest3', { driveClient: d3 });
    check('登録: reason が応答本体だけにあっても 403 と分かる (空の errors に隠されない)',
      r3.ok === false && r3.status === 403, JSON.stringify(r3));
  }
  // 動かせないと分かっているなら、画像フォルダも作らずに止める (使われないフォルダを Drive に残さない — Codex R3 medium)
  {
    const wbW = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('WBI-W', 'フォルダ未作成で権限不足', 'smoke')`).run().lastInsertRowid);
    const d = fakeDrive([img('inbox-file-ww001', 'w.jpg', INBOX, { capabilities: { canEdit: false, canMoveItemWithinDrive: false } })]);
    const r = await wbi.registerWhiteBgFromInbox(wbW, 'inbox-file-ww001', { driveClient: d });
    check('登録: 権限不足なら 403 で、画像フォルダの作成にも進まない',
      r.ok === false && r.status === 403 && !d.calls.some((c) => c[0] === 'create')
      && db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(wbW).drive_folder_url == null
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbW) == null,
      JSON.stringify({ r, calls: d.calls.map((c) => c[0]) }));
  }
  // セット派生は移動しない = 権限は関係ないので、読み取り専用でも登録だけは通る
  {
    const wbX = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, parent_draft_id, provisional_code) VALUES ('WBI-X', 'セット (権限なし)', 'smoke', ?, 1)`).run(wbDraftA).lastInsertRowid);
    const d = fakeDrive([img('inbox-file-xx001', 'x.jpg', INBOX, { capabilities: { canEdit: false, canMoveItemWithinDrive: false } })]);
    const r = await wbi.registerWhiteBgFromInbox(wbX, 'inbox-file-xx001', { driveClient: d });
    check('登録: セット派生 (移動先が無い) は権限に関係なく登録だけする',
      r.ok === true && r.moved === false && r.warnings.some((w) => w.includes('セット商品'))
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbX).white_bg_drive_file_id === 'inbox-file-xx001',
      JSON.stringify(r));
  }
  // 移動だけ届いて所在も確認できない = 商品フォルダに _00 が 2 枚ある。自動セットは重複で止まるので手順を案内する (Codex R2 medium)
  {
    const wbV = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-V', '届いたが確認不能', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-V-0000001')`).run().lastInsertRowid);
    const d = fakeDrive([img('inbox-file-vv001', 'v.jpg', INBOX), img('old-v-file-0001', 'WBI-V_00.jpg', 'FOLDER-V-0000001')]);
    let gets = 0;
    const g = d.files.get;
    d.files.get = async (p) => { gets += 1; if (gets >= 2) throw new Error('drive down'); return g(p); };
    d.failUpdate = (params, f) => { f.parents = ['FOLDER-V-0000001']; f.name = params.requestBody.name; throw new Error('socket hang up'); };
    const r = await wbi.registerWhiteBgFromInbox(wbV, 'inbox-file-vv001', { driveClient: d });
    const ev = db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'white_bg_inbox_failed' ORDER BY id DESC LIMIT 1`).get(wbV);
    check('R2: 移動は届いたが所在も確認できない → 502 + 選んだ画像のリンクと直す順番を案内',
      r.ok === false && r.status === 502 && r.error.includes('inbox-file-vv001') && r.error.includes('WBI-V_00.jpg') && r.error.includes('名前を変える'),
      JSON.stringify(r));
    check('R2: 失敗イベントにも選んだ画像の名前と ID が残る', !!ev && ev.detail.includes('v.jpg') && ev.detail.includes('inbox-file-vv001'), JSON.stringify(ev));
    check('R2: このとき旧 _00 はまだ退けられていない (だから先に名前を変えてもらう)',
      d.files_.get('old-v-file-0001').name === 'WBI-V_00.jpg'
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbV) == null);
  }
  // 単品でフォルダが無ければその場で作って移動する (drive-image-folder と同じ関数)
  {
    const d = fakeDrive([img('inbox-file-000b', 'b.jpg', INBOX)]);
    const r = await wbi.registerWhiteBgFromInbox(wbDraftB, 'inbox-file-000b', { driveClient: d });
    const created = d.calls.find((c) => c[0] === 'create')?.[1];
    const url = db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(wbDraftB).drive_folder_url;
    check('登録: 単品でフォルダが無ければ「商品コード_商品名」を作ってそこへ移動',
      r.ok === true && r.moved === true && !!created && created.requestBody.name === 'WBI-B_受信箱B (フォルダ無し)' && !!url && url.endsWith('NEW-FOLDER-0002') && d.files_.get('inbox-file-000b').parents.join(',') === 'NEW-FOLDER-0002',
      JSON.stringify({ r, created, url }));
  }
  // セット派生でフォルダが無ければ移動せず登録だけ (警告)
  {
    const wbSet = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, parent_draft_id, provisional_code) VALUES ('WBI-SET', 'セット', 'smoke', ?, 1)`).run(wbDraftA).lastInsertRowid);
    const d = fakeDrive([img('inbox-file-000s', 's.jpg', INBOX)]);
    const r = await wbi.registerWhiteBgFromInbox(wbSet, 'inbox-file-000s', { driveClient: d });
    check('登録: セット派生でフォルダが無ければ移動せず登録だけ + 警告 (フォルダは作らない)',
      r.ok === true && r.moved === false && r.warnings.length === 1 && r.warnings[0].includes('セット商品') && !d.calls.some((c) => c[0] === 'create') && d.files_.get('inbox-file-000s').parents.join(',') === INBOX
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbSet).white_bg_drive_file_id === 'inbox-file-000s', JSON.stringify(r));
  }
  // 一覧: ページ送り・500 件超の打ち切り (Codex R1 low)
  {
    const many = [];
    for (let i = 0; i < 501; i++) {
      many.push(img(`inbox-many-${String(i).padStart(4, '0')}`, `m${i}.jpg`, INBOX, { createdTime: new Date(Date.UTC(2026, 0, 1) + i * 60000).toISOString() }));
    }
    const d = fakeDrive(many);
    const r = await wbi.listWhiteBgInbox({ driveClient: d, db });
    const lists = d.calls.filter((c) => c[0] === 'list');
    check('受信箱の一覧: ページ送りして集め、上限 500 で打ち切り truncated=true',
      r.files.length === 500 && r.truncated === true && lists.length >= 3 && lists[1][1].pageToken != null,
      JSON.stringify({ n: r.files.length, t: r.truncated, lists: lists.length }));
    check('受信箱の一覧: 打ち切っても新しい順', r.files[0].id === 'inbox-many-0500' && r.files[499].id === 'inbox-many-0001',
      JSON.stringify([r.files[0].id, r.files[499].id]));
  }
  // 同時登録 (Codex R1 high): 同じ画像を 2 商品へ / 同じ商品へ 2 枚 — 1 本ずつ処理される
  {
    const wbC1 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-C1', '同時1', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-C1-000001')`).run().lastInsertRowid);
    const wbC2 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-C2', '同時2', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-C2-000001')`).run().lastInsertRowid);
    const d = fakeDrive([img('inbox-file-race1', 'race.jpg', INBOX)]);
    const slowGet = d.files.get;
    d.files.get = async (p) => { await new Promise((r) => setTimeout(r, 15)); return slowGet(p); };
    const [r1, r2] = await Promise.all([
      wbi.registerWhiteBgFromInbox(wbC1, 'inbox-file-race1', { driveClient: d }),
      wbi.registerWhiteBgFromInbox(wbC2, 'inbox-file-race1', { driveClient: d }),
    ]);
    check('同時登録: 同じ画像を 2 商品へ → 先勝ち・後は 409 (両方には登録されない)',
      r1.ok === true && r1.moved === true && r2.ok === false && r2.status === 409
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbC2) == null,
      JSON.stringify({ r1, r2 }));
    const d2 = fakeDrive([img('inbox-file-raceA', 'a.jpg', INBOX), img('inbox-file-raceB', 'b.jpg', INBOX)]);
    const g2 = d2.files.get;
    d2.files.get = async (p) => { await new Promise((r) => setTimeout(r, 15)); return g2(p); };
    const at = () => new Date('2026-09-14T03:00:00Z');
    const [s1, s2] = await Promise.all([
      wbi.registerWhiteBgFromInbox(wbC1, 'inbox-file-raceA', { driveClient: d2, now: at }),
      wbi.registerWhiteBgFromInbox(wbC1, 'inbox-file-raceB', { driveClient: d2, now: at }),
    ]);
    const inC1 = [...d2.files_.values()].filter((f) => f.parents.includes('FOLDER-C1-000001')).map((f) => f.name).sort();
    check('同時登録: 同じ商品へ 2 枚 → 後の登録が先の _00 を退けるので _00 は 1 枚',
      s1.ok === true && s2.ok === true && s2.parked.length === 1 && inC1.join(',') === 'WBI-C1_00.jpg,WBI-C1_00_旧20260914-1200.jpg'
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbC1).white_bg_drive_file_id === 'inbox-file-raceB',
      JSON.stringify({ s1, s2, inC1 }));
    // 前の登録が失敗 (throw) しても次の登録は動く
    const d3 = fakeDrive([img('inbox-file-after1', 'after.jpg', INBOX)]);
    const [f1, f2] = await Promise.all([
      wbi.registerWhiteBgFromInbox(wbC2, 'inbox-file-after1', { driveClient: { files: { get: async () => { throw new Error('boom'); } } } }),
      wbi.registerWhiteBgFromInbox(wbC2, 'inbox-file-after1', { driveClient: d3 }),
    ]);
    check('同時登録: 前の登録が失敗しても次は動く', f1.ok === false && f1.status === 502 && f2.ok === true && f2.moved === true, JSON.stringify({ f1, f2 }));
  }
  // 退避の途中失敗 (Codex R1 low): 移動は成功し、1 枚目は退けられ 2 枚目で失敗 → 成功分が結果とイベントに残り、警告が出る
  {
    const wbP = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-P', '退避', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-P-0000001')`).run().lastInsertRowid);
    const d = fakeDrive([
      img('inbox-file-park1', 'p.jpg', INBOX),
      img('old-park-file-01', 'WBI-P_00.jpg', 'FOLDER-P-0000001'),
      img('old-park-file-02', 'wbi-p_00.png', 'FOLDER-P-0000001', { mimeType: 'image/png' }),
    ]);
    d.failUpdate = (params) => { if (params.fileId === 'old-park-file-02') throw new Error('rename boom'); };
    const r = await wbi.registerWhiteBgFromInbox(wbP, 'inbox-file-park1', { driveClient: d, now: () => new Date('2026-09-14T01:30:00Z') });
    const ev = db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'white_bg_set_from_inbox'`).get(wbP);
    check('退避の途中失敗: 移動は済み・成功分 (1 枚) が parked とイベントに残り、退けられなかったことが警告に出る',
      r.ok === true && r.moved === true && r.parked.length === 1
      && r.warnings.length === 1 && r.warnings[0].includes('rename boom') && r.warnings[0].includes('_旧')
      && ev && ev.detail.includes(r.parked[0])
      // 退けられなかったファイルと理由も履歴に残る (画面の警告は 1 回きりなので — Codex R1 medium)
      && ev.detail.includes('rename boom') && ev.detail.includes('old-park-file-02') && ev.detail.includes('wbi-p_00.png')
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbP).white_bg_drive_file_id === 'inbox-file-park1',
      JSON.stringify({ r, ev }));
  }
  // 移動に失敗し、その間に誰かが受信箱から動かしていた → 登録しない 409 (Codex R1 high)
  {
    const d = fakeDrive([img('inbox-file-gone1', 'g.jpg', INBOX)]);
    d.failUpdate = (params, f) => { f.parents = ['SOMEWHERE-ELSE-0001']; throw new Error('moved by someone'); };
    const r = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-gone1', { driveClient: d });
    check('移動失敗 + もう受信箱に無い → 409 で登録しない', r.ok === false && r.status === 409
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbDraftA).white_bg_drive_file_id !== 'inbox-file-gone1',
      JSON.stringify(r));
  }
  // R2 high: 移動失敗 + 所在も確認できない → 登録しない 502 + 失敗イベント (退避は移動のあとなので旧ファイルは無傷)
  {
    const wbQ = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-Q', '所在不明', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-Q-0000001')`).run().lastInsertRowid);
    const d = fakeDrive([img('inbox-file-unkn1', 'u.jpg', INBOX), img('old-q-file-0001', 'WBI-Q_00.jpg', 'FOLDER-Q-0000001')]);
    let gets = 0;
    const g = d.files.get;
    d.files.get = async (p) => { gets += 1; if (gets >= 2) throw new Error('drive down'); return g(p); };
    d.failUpdate = (params) => { if (params.fileId === 'inbox-file-unkn1') throw new Error('move boom'); };
    const r = await wbi.registerWhiteBgFromInbox(wbQ, 'inbox-file-unkn1', { driveClient: d, now: () => new Date('2026-09-14T01:30:00Z') });
    const ev = db.prepare(`SELECT event, detail FROM draft_events WHERE draft_id = ? AND event LIKE 'white_bg_%' ORDER BY id`).all(wbQ);
    check('R2: 移動失敗 + 所在不明 → 502 で登録しない・前の _00 も触っていない',
      r.ok === false && r.status === 502 && r.error.includes('確認できませんでした')
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbQ) == null
      && ev.length === 1 && ev[0].event === 'white_bg_inbox_failed' && ev[0].detail.includes('drive down')
      && d.files_.get('old-q-file-0001').name === 'WBI-Q_00.jpg',
      JSON.stringify({ r, ev }));
  }
  // R2 medium: 移動は届いていて応答だけ落ちた → 移動済みとして登録 (409 にしない)
  {
    const wbR = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-R', '応答落ち', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-R-0000001')`).run().lastInsertRowid);
    const d = fakeDrive([img('inbox-file-lost1', 'l.jpg', INBOX)]);
    d.failUpdate = (params, f) => {
      f.parents = ['FOLDER-R-0000001']; f.name = params.requestBody.name; f.modifiedTime = '2026-09-14T05:00:00.000Z';
      throw new Error('socket hang up');
    };
    const r = await wbi.registerWhiteBgFromInbox(wbR, 'inbox-file-lost1', { driveClient: d });
    const rk = db.prepare('SELECT white_bg_drive_file_id, white_bg_modified_time FROM draft_rakuten WHERE draft_id = ?').get(wbR);
    check('R2: 移動済みで応答だけ落ちた → moved=true で登録 (更新日時は聞き直した値)',
      r.ok === true && r.moved === true && r.name === 'WBI-R_00.jpg' && r.warnings.length === 1 && r.warnings[0].includes('移動済みとして登録')
      && rk.white_bg_drive_file_id === 'inbox-file-lost1' && rk.white_bg_modified_time === '2026-09-14T05:00:00.000Z',
      JSON.stringify({ r, rk }));
  }
  // R2 low → 2026-09-18: 退避が移動のあとになったので、409 で終わるときは前の _00 も無傷のまま
  {
    const wbS = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-S', '退避後409', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-S-0000001')`).run().lastInsertRowid);
    const d = fakeDrive([img('inbox-file-s0001', 's.jpg', INBOX), img('old-s-file-00001', 'WBI-S_00.jpg', 'FOLDER-S-0000001')]);
    d.failUpdate = (params, f) => { if (params.fileId === 'inbox-file-s0001') { f.parents = ['SOMEWHERE-ELSE-0001']; throw new Error('moved by someone'); } };
    const r = await wbi.registerWhiteBgFromInbox(wbS, 'inbox-file-s0001', { driveClient: d, now: () => new Date('2026-09-14T01:30:00Z') });
    const ev = db.prepare(`SELECT event, detail FROM draft_events WHERE draft_id = ? AND event = 'white_bg_inbox_failed'`).get(wbS);
    check('R2: 移動中に誰かが動かした → 409 で登録せず、失敗イベントは残り、前の _00 は退けられていない',
      r.ok === false && r.status === 409 && !r.error.includes('_旧')
      && !!ev && ev.detail.includes('moved by someone')
      && d.files_.get('old-s-file-00001').name === 'WBI-S_00.jpg'
      && db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(wbS) == null,
      JSON.stringify({ r, ev }));
  }
  // R3 low: 親に受信箱と移動先の両方 + 名前だけ新しい (中途半端) → 移動済みとはみなさない。
  // 2026-09-18 から、受信箱に残っている側は登録もしない (やり直させる)
  {
    const wbT = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, drive_folder_url) VALUES ('WBI-T', '両方の親', 'smoke', 'https://drive.google.com/drive/folders/FOLDER-T-0000001')`).run().lastInsertRowid);
    const d = fakeDrive([img('inbox-file-both1', 'b.jpg', INBOX, { modifiedTime: '2026-09-14T06:00:00.000Z' })]);
    d.failUpdate = (params, f) => {
      f.parents = [INBOX, 'FOLDER-T-0000001']; f.name = params.requestBody.name; f.modifiedTime = '2026-09-14T06:30:00.000Z';
      throw new Error('partial');
    };
    const r = await wbi.registerWhiteBgFromInbox(wbT, 'inbox-file-both1', { driveClient: d });
    const rk = db.prepare('SELECT white_bg_drive_file_id, white_bg_modified_time FROM draft_rakuten WHERE draft_id = ?').get(wbT);
    check('R3: 受信箱と移動先の両方に親がある (中途半端) → 移動済みとみなさず 502 で登録しない',
      r.ok === false && r.status === 502 && r.error.includes('移動できなかった') && rk == null,
      JSON.stringify({ r, rk }));
  }
  // サムネイル (2026-09-14 中原さん「画像が見えない」): /api/thumb は登録済みの画像しか返さないため、
  // 受信箱の一覧で見せた画像だけを期限つきで許可する。ルートが実際にそれを使っているかを HTTP で確かめる
  {
    const d = fakeDrive([img('inbox-thumb-0001', 't.jpg', INBOX, { modifiedTime: '2026-09-14T07:00:00.000Z' })]);
    check('サムネイル: 一覧を出す前は許可しない', wbi.inboxThumbRef('inbox-thumb-0001') === null);
    await wbi.listWhiteBgInbox({ driveClient: d, db });
    const ref = wbi.inboxThumbRef('inbox-thumb-0001');
    check('サムネイル: 一覧で見せた画像は許可 (版数の期待値つき)', ref?.modifiedTime === '2026-09-14T07:00:00.000Z', JSON.stringify(ref));
    check('サムネイル: 期限切れは許可しない', wbi.inboxThumbRef('inbox-thumb-0001', Date.now() + wbi.INBOX_THUMB_TTL_MS + 1000) === null);
    await wbi.listWhiteBgInbox({ driveClient: d, db }); // 期限切れで消えたので見せ直す
    const express = (await import('express')).default;
    const routerMod = await import('../router.js');
    const app = express();
    app.use((req, res, next) => { req.session = { email: 'smoke@b-faith.biz', displayName: 'smoke', role: 'admin' }; next(); });
    app.use('/ph', routerMod.default);
    const server = app.listen(0);
    try {
      const base = `http://127.0.0.1:${server.address().port}/ph`;
      const unknown = await fetch(`${base}/api/thumb/inbox-never-seen-01?w=160`);
      const uj = await unknown.json().catch(() => ({}));
      const seen = await fetch(`${base}/api/thumb/inbox-thumb-0001?w=160`);
      const sj = await seen.json().catch(() => ({}));
      check('サムネイル (HTTP): 登録も一覧表示もされていない ID は 404 unknown image (任意の ID は覗けない)',
        unknown.status === 404 && uj.error === 'unknown image', JSON.stringify({ s: unknown.status, uj }));
      check('サムネイル (HTTP): 受信箱の一覧で見せた ID は許可を通って Drive の取得へ進む (smoke は鍵なしなので 502)',
        seen.status === 502 && sj.error === 'thumbnail unavailable', JSON.stringify({ s: seen.status, sj }));
    } finally {
      server.close();
    }
  }
  // Drive が throw しても reject しない
  {
    const d = { files: { get: async () => { throw new Error('boom'); } } };
    const r = await wbi.registerWhiteBgFromInbox(wbDraftA, 'inbox-file-0001', { driveClient: d });
    check('登録: Drive の想定外エラーは 502 (throw しない)', r.ok === false && r.status === 502 && r.error.includes('boom'), JSON.stringify(r));
  }
}

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

  // ─── 人の確認待ち (generation-block) + 文字数ガード (2026-08-28 夜間自動化) ───
  // 人用ルート (解除・手直し) も同じ app に載せる。session は偽装
  const routerModGB = await import('../router.js');
  app.use((req, res, next) => { req.session = { email: 'smoke@b-faith.biz', displayName: 'smoke', role: 'admin' }; next(); });
  app.use('/ph', routerModGB.default);
  const callPh = async (method, path, body) => {
    const res = await fetch(`http://127.0.0.1:${server.address().port}/ph` + path, {
      method, headers: { 'Content-Type': 'application/json' },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    return { status: res.status, json: await res.json() };
  };
  db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, amazon_url, status)
    VALUES ('GEN-3', 'アイスの棒 50本', 'smoke', 'https://www.amazon.co.jp/dp/B0GEN3', 'ready_for_ai')`).run();
  const gdraft3 = db.prepare(`SELECT * FROM product_drafts WHERE ne_code = 'GEN-3'`).get();
  r = await call('POST', '/generation-queue/claim', { run_id: 'runJ', limit: 10 });
  check('block 準備: runJ が GEN-3 を claim', r.json.drafts.some((d) => d.id === gdraft3.id));
  check('claim 応答に queue 内訳が付く', r.json.queue && typeof r.json.queue.claimable === 'number' && typeof r.json.queue.blocked === 'number', JSON.stringify(r.json.queue));

  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`, { code: 'PACK_COUNT_MISMATCH', reason: 'x' });
  check('block: run_id なしは 400', r.status === 400);
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`, { run_id: 'runX', code: 'PACK_COUNT_MISMATCH', reason: 'x' });
  check('block: claim を持たない run は 409 (他人の draft を止められない)', r.status === 409, JSON.stringify(r.json));
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`, { run_id: 'runJ', code: 'NOT_A_CODE', reason: 'x' });
  check('block: 未知の code は 400', r.status === 400);
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`, { run_id: 'runJ', code: 'PACK_COUNT_MISMATCH' });
  check('block: reason なしは 400', r.status === 400);
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`,
    { run_id: 'runJ', code: 'PACK_COUNT_MISMATCH', reason: 'draft は 50本、Amazon ページは 100本入り' });
  check('block: 有効な claim を持つ run は止められる', r.status === 200 && r.json.blocked === true && r.json.already === false, JSON.stringify(r.json));
  let rowB = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(gdraft3.id);
  check('block: 4列が揃って書かれ claim は解放される (status は ready_for_ai のまま)',
    rowB.generation_block_code === 'PACK_COUNT_MISMATCH' && rowB.generation_blocked_at && rowB.generation_blocked_by === 'ai:runJ'
    && rowB.generation_claim_run_id == null && rowB.generation_claim_until == null && rowB.status === 'ready_for_ai');
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`,
    { run_id: 'runJ', code: 'PACK_COUNT_MISMATCH', reason: 'draft は 50本、Amazon ページは 100本入り' });
  check('block: 同じ run・code・reason の再送は 200 already (通信断リトライを 409 にしない)', r.status === 200 && r.json.already === true, JSON.stringify(r.json));
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`,
    { run_id: 'runJ', code: 'PACK_COUNT_MISMATCH', reason: '理由が変わった' });
  check('block: 同じ run でも reason が違えば 409 (冪等は同一操作の再送だけ — Codex R1 medium)', r.status === 409);
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`,
    { run_id: 'runJ', code: 'OTHER', reason: '別の理由' });
  check('block: 別 code での上書きは 409 (人が解除するまで理由を固定)', r.status === 409);
  r = await call('POST', `/drafts/${gdraft3.id}abc/generation-block`, { run_id: 'runJ', code: 'OTHER', reason: 'x' });
  check('block: id に数字以外が混じると 400 (parseInt の緩さを許さない)', r.status === 400);
  r = await call('POST', `/drafts/${gdraft3.id}/generation-block`, { run_id: 'runJ', code: 'OTHER', reason: 'か'.repeat(1001) });
  check('block: reason 1001 字は切り詰めずに 400', r.status === 400, JSON.stringify(r.json));

  r = await call('POST', '/generation-queue/claim', { run_id: 'runK', limit: 10 });
  check('block 後: claim 候補から外れる', !r.json.drafts.some((d) => d.id === gdraft3.id));
  check('block 後: queue.blocked に数えられる', r.json.queue.blocked >= 1 && r.json.queue.blockedByCode.PACK_COUNT_MISMATCH >= 1, JSON.stringify(r.json.queue));
  r = await call('GET', '/generation-queue');
  check('block 後: GET 一覧からも消える (人待ちは AI に見せない)', !r.json.drafts.some((d) => d.id === gdraft3.id) && r.json.queue.blocked >= 1);

  // 書き込みロックのレース: block 済みなのに有効な claim が残っていても文章は入らない (Codex Critical)
  db.prepare(`UPDATE product_drafts SET generation_claim_run_id = 'runJ', generation_claim_until = '2999-01-01T00:00:00Z' WHERE id = ?`).run(gdraft3.id);
  check('block 済み: acquireGenerationWriteLock は書き込み権を渡さない', dbmod.acquireGenerationWriteLock(db, gdraft3.id, 'runJ') === false);
  r = await call('POST', `/drafts/${gdraft3.id}/ai-outputs`, { run_id: 'runJ', outputs: SIX, advance: true });
  check('block 済み: ai-outputs は 409 で 1 バイトも書かれない', r.status === 409
    && db.prepare('SELECT COUNT(*) AS c FROM draft_ai_outputs WHERE draft_id = ?').get(gdraft3.id).c === 0, JSON.stringify(r.json));
  db.prepare(`UPDATE product_drafts SET generation_claim_run_id = NULL, generation_claim_until = NULL WHERE id = ?`).run(gdraft3.id);

  // 人の解除 (通常ルート) — 楽観ロック付き
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/generation-block`, { clear: true, blocked_at: '1999-01-01T00:00:00.000Z' });
  check('解除: 画面が見ていた blocked_at と違えば 409', r.status === 409, JSON.stringify(r.json));
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/generation-block`, { clear: false });
  check('解除: clear:true 以外は 400 (人がこの画面から止めることはできない)', r.status === 400);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/generation-block`, { clear: true });
  check('解除: blocked_at 省略は 400 (楽観ロックを迂回させない — Codex R1 medium)', r.status === 400
    && db.prepare('SELECT generation_block_code FROM product_drafts WHERE id = ?').get(gdraft3.id).generation_block_code === 'PACK_COUNT_MISMATCH');
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/generation-block`, { clear: true, blocked_at: rowB.generation_blocked_at });
  check('解除: blocked_at 一致で解除できる', r.status === 200 && r.json.unblocked === true, JSON.stringify(r.json));
  rowB = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(gdraft3.id);
  check('解除後: 4列すべて NULL に戻る', rowB.generation_block_code == null && rowB.generation_block_reason == null
    && rowB.generation_blocked_at == null && rowB.generation_blocked_by == null);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/generation-block`, { clear: true, blocked_at: rowB.generation_blocked_at || '2000-01-01T00:00:00.000Z' });
  check('解除: 二重解除は 400', r.status === 400);
  r = await call('POST', '/generation-queue/claim', { run_id: 'runL', limit: 10 });
  check('解除後: 次の claim で拾える (解除 = キューに戻すだけ)', r.json.drafts.some((d) => d.id === gdraft3.id));
  const evGB = db.prepare(`SELECT event FROM draft_events WHERE draft_id = ? AND event IN ('generation_blocked','generation_unblocked') ORDER BY id`).all(gdraft3.id).map((e) => e.event);
  check('監査ログ: blocked → unblocked が残る', evGB.join(',') === 'generation_blocked,generation_unblocked', evGB.join(','));

  // 文字数ガード (service-api)。数え方はコードポイント (copy_lint.py の len() と一致)
  r = await call('POST', `/drafts/${gdraft3.id}/ai-outputs`, { run_id: 'runL', outputs: { ...SIX, rakuten_title: 'あ'.repeat(128) }, advance: true });
  check('文字数: 楽天タイトル 128 字は 400 (構造化エラー)', r.status === 400 && r.json.code === 'OUTPUT_TOO_LONG'
    && r.json.kind === 'rakuten_title' && r.json.limit === 127 && r.json.actual === 128, JSON.stringify(r.json));
  r = await call('POST', `/drafts/${gdraft3.id}/ai-outputs`, { run_id: 'runL', outputs: { yahoo_title: 'い'.repeat(66) } });
  check('文字数: Yahoo!タイトル 66 字は 400', r.status === 400 && r.json.kind === 'yahoo_title' && r.json.limit === 65);
  r = await call('POST', `/drafts/${gdraft3.id}/ai-outputs`, { run_id: 'runL', outputs: { desc_catch: 'う'.repeat(31) } });
  check('文字数: キャッチコピー 31 字は 400', r.status === 400 && r.json.kind === 'desc_catch' && r.json.limit === 30);
  r = await call('POST', `/drafts/${gdraft3.id}/ai-outputs`, { run_id: 'runL', outputs: { rakuten_title: '😀'.repeat(127) } });
  check('文字数: 絵文字 127 個 (UTF-16 では 254) はコードポイント数で 127 → 通る', r.status === 200 && r.json.written === 1, JSON.stringify(r.json));
  // 書き込み成功で claim は解放される仕様 → 次の書き込みは claim し直す
  r = await call('POST', '/generation-queue/claim', { run_id: 'runM', limit: 10 });
  check('文字数: 部分保存の後は claim が解放され、次の run が拾える', r.json.drafts.some((d) => d.id === gdraft3.id));
  r = await call('POST', `/drafts/${gdraft3.id}/ai-outputs`, { run_id: 'runM', outputs: { desc_features: 'え'.repeat(3000) } });
  check('文字数: 説明文3欄には上限を置かない', r.status === 200, JSON.stringify(r.json));
  // 人の手直し (通常ルート) にも同じ上限
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/ai-outputs`, { kind: 'yahoo_title', content: 'お'.repeat(66) });
  check('文字数 (人の手直し): 66 字は 400', r.status === 400 && r.json.code === 'OUTPUT_TOO_LONG');
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/ai-outputs`, { kind: 'yahoo_title', content: 'お'.repeat(65) });
  check('文字数 (人の手直し): 65 字は通る', r.status === 200);

  // ─── 確認中 (2026-08-31 スタッフ要望): 情報待ちの印。工程も status も動かさず、
  //     カードにラベルが出て AI 生成キューからだけ外れる ───
  db.prepare(`UPDATE product_drafts SET status = 'ready_for_ai', generation_claim_run_id = NULL,
    generation_claim_until = NULL WHERE id = ?`).run(gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { reason_code: 'package_label' });
  check('確認中: on の指定なしは 400 (欠落を「解除」に倒さない)', r.status === 400, JSON.stringify(r.json));
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: 'true', reason_code: 'package_label' });
  check('確認中: on が文字列なら 400', r.status === 400);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: true, reason_code: 'NOT_A_REASON' });
  check('確認中: 未知の理由コードは 400 (画面が説明できない状態を作らない)', r.status === 400, JSON.stringify(r.json));
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: true, reason_code: 'package_label', note: '成分表示を実物で確認' });
  check('確認中: 有効な理由なら立てられる', r.status === 200 && r.json.changed === true, JSON.stringify(r.json));
  let rowC = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(gdraft3.id);
  const checkingSince0 = rowC.checking_since;
  check('確認中: 4列が揃って書かれ status は ready_for_ai のまま (工程導出を壊さない)',
    rowC.checking_reason_code === 'package_label' && rowC.checking_note === '成分表示を実物で確認'
    && rowC.checking_since && rowC.checking_by && rowC.status === 'ready_for_ai', JSON.stringify(rowC).slice(0, 200));

  r = await call('POST', '/generation-queue/claim', { run_id: 'runN', limit: 10 });
  check('確認中: claim 候補から外れる (確認中に AI が原稿を書かない)', !r.json.drafts.some((d) => d.id === gdraft3.id));
  // 「数え漏れ」でなく「claimable から checking へ 1 件移った」ことを見る (Codex R2 low:
  // checking >= 1 だけだと claimable にも二重計上されている状態を見逃す)
  {
    const claimCols = `generation_claim_run_id = NULL, generation_claim_until = NULL`;
    db.prepare(`UPDATE product_drafts SET checking_since = NULL, checking_reason_code = NULL,
      checking_by = NULL, ${claimCols} WHERE id = ?`).run(gdraft3.id);
    const qOff = dbmod.generationQueueSummary(db);
    db.prepare(`UPDATE product_drafts SET checking_since = ?, checking_reason_code = 'package_label',
      checking_by = 'smoke', ${claimCols} WHERE id = ?`).run(checkingSince0, gdraft3.id);
    const qOn = dbmod.generationQueueSummary(db);
    check('確認中: queue の内訳が claimable → checking へ 1 件移る (二重計上しない)',
      qOn.checking === qOff.checking + 1 && qOn.claimable === qOff.claimable - 1 && qOn.blocked === qOff.blocked,
      `off=${JSON.stringify(qOff)} on=${JSON.stringify(qOn)}`);
  }
  r = await call('GET', '/generation-queue');
  check('確認中: GET 一覧からも消える', !r.json.drafts.some((d) => d.id === gdraft3.id));

  // 生成中の run が居るまま確認中にしたら claim も解放する (Codex R1 medium)。
  // 残すと、lease (30分) の内に人が解除した場合だけ古い run が結果を書き戻せる。
  // (理由を変える = changed:true の更新でも claim が落ちること。checking_since は維持されること)
  db.prepare(`UPDATE product_drafts SET generation_claim_run_id = 'runRace',
    generation_claim_until = '2999-01-01T00:00:00Z' WHERE id = ?`).run(gdraft3.id);
  await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: true, reason_code: 'share_info', note: '成分表示を実物で確認' });
  const rowRace = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(gdraft3.id);
  check('確認中: 立てると走っていた実行の claim も解放される (解除直後に古い結果が書かれない)',
    rowRace.generation_claim_run_id == null && rowRace.generation_claim_until == null
    && rowRace.checking_since === checkingSince0,
    `${rowRace.generation_claim_run_id} / ${rowRace.generation_claim_until} / ${rowRace.checking_since}`);

  // 書き込みロックのレース: claim 済みの生成が走っている最中に人が確認中にしたら結果は書かせない
  db.prepare(`UPDATE product_drafts SET generation_claim_run_id = 'runN', generation_claim_until = '2999-01-01T00:00:00Z' WHERE id = ?`).run(gdraft3.id);
  check('確認中: acquireGenerationWriteLock は書き込み権を渡さない',
    dbmod.acquireGenerationWriteLock(db, gdraft3.id, 'runN') === false);
  const aiCountBefore = db.prepare('SELECT COUNT(*) AS c FROM draft_ai_outputs WHERE draft_id = ?').get(gdraft3.id).c;
  r = await call('POST', `/drafts/${gdraft3.id}/ai-outputs`, { run_id: 'runN', outputs: SIX, advance: true });
  check('確認中: ai-outputs は 409 で 1 バイトも書かれない', r.status === 409
    && db.prepare('SELECT COUNT(*) AS c FROM draft_ai_outputs WHERE draft_id = ?').get(gdraft3.id).c === aiCountBefore,
    JSON.stringify(r.json));
  db.prepare(`UPDATE product_drafts SET generation_claim_run_id = NULL, generation_claim_until = NULL WHERE id = ?`).run(gdraft3.id);

  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: true, reason_code: 'share_info', note: '成分表示を実物で確認' });
  check('確認中: 同じ理由・同じ補足の再送は changed:false (ログを無駄に増やさない)',
    r.status === 200 && r.json.changed === false, JSON.stringify(r.json));
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: true, reason_code: 'no_web_info', note: 'メーカーに問い合わせ中' });
  rowC = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(gdraft3.id);
  check('確認中: 理由を変えても checking_since は進まない (何日待っているかを見失わない)',
    r.status === 200 && rowC.checking_reason_code === 'no_web_info' && rowC.checking_since === checkingSince0,
    `${rowC.checking_since} vs ${checkingSince0}`);

  // ボード表示: ラベル・並び順・絞り込み
  {
    const bC = wfp.boardData(db, {});
    const cardC = bC.columns.flatMap((c) => c.cards).concat(bC.doneCards).find((c) => c.id === gdraft3.id);
    check('確認中: ボードのカードに理由ラベルと経過日数が乗る',
      !!cardC?.checking && cardC.checking.label === 'ウェブに情報が無い' && typeof cardC.checking.days === 'number',
      JSON.stringify(cardC?.checking));
    check('確認中: checkingTotal に数えられる', bC.checkingTotal >= 1, String(bC.checkingTotal));
    const colOfC = bC.columns.find((c) => c.cards.some((x) => x.id === gdraft3.id));
    check('確認中: カードは列の先頭に並ぶ (他のカードに埋もれない)',
      !colOfC || colOfC.cards[0].id === gdraft3.id,
      colOfC ? colOfC.cards.map((x) => x.id).join(',') : '(完了列)');
    const bOnly = wfp.boardData(db, { checkingOnly: true });
    const idsOnly = bOnly.columns.flatMap((c) => c.cards).concat(bOnly.doneCards).map((c) => c.id);
    check('確認中: filter=checking で確認中だけに絞れる',
      idsOnly.includes(gdraft3.id) && idsOnly.length >= 1
      && db.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE id IN (${idsOnly.join(',') || 'NULL'}) AND checking_since IS NULL`).get().c === 0,
      idsOnly.join(','));
    // 絞り込み中でも総数を出す (0 と出ると確認中が無いように見えて入口が消える)
    const bUn = wfp.boardData(db, { unassignedOnly: true });
    check('確認中: 別の絞り込み中でも checkingTotal は総数のまま', bUn.checkingTotal >= 1, String(bUn.checkingTotal));
  }

  // 解除
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: false });
  check('確認中: 解除できる', r.status === 200 && r.json.changed === true, JSON.stringify(r.json));
  rowC = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(gdraft3.id);
  check('確認中: 解除で 4列すべて NULL に戻る',
    rowC.checking_reason_code == null && rowC.checking_note == null
    && rowC.checking_since == null && rowC.checking_by == null);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/checking`, { on: false });
  check('確認中: 二重解除は changed:false (エラーにしない — 押し直しで詰まらせない)',
    r.status === 200 && r.json.changed === false, JSON.stringify(r.json));
  r = await call('POST', '/generation-queue/claim', { run_id: 'runO', limit: 10 });
  check('確認中: 解除後は次の claim で拾える (解除 = キューに戻すだけ)', r.json.drafts.some((d) => d.id === gdraft3.id));
  const evCK = db.prepare(`SELECT event FROM draft_events WHERE draft_id = ?
    AND event IN ('checking_on','checking_updated','checking_off') ORDER BY id`).all(gdraft3.id).map((e) => e.event);
  check('確認中: 監査ログが on → updated (理由変更 2 回) → off で残る (誰が何を待たせたかを追える)',
    evCK.join(',') === 'checking_on,checking_updated,checking_updated,checking_off', evCK.join(','));

  // ─── メーカー型番は保存時にも属性から落とす (2026-08-31) ───
  // 画面は行を出さないが、旧い画面を開いたままのタブや直叩きからも入らないようにする
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, {
    attributes: [
      { name: 'ブランド名', value: 'テストブランド' },
      { name: 'メーカー型番', value: 'toys3pen' },
    ],
    article_number: 'toys3pen',
  });
  {
    const saved = db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id);
    const attrs = JSON.parse(saved?.attributes_json || '[]');
    check('メーカー型番: 保存時に属性から落とす (古い画面からの POST でも二重にならない)',
      r.status === 200 && !attrs.some((a2) => a2.name === 'メーカー型番')
      && attrs.some((a2) => a2.name === 'ブランド名') && saved.article_number === 'toys3pen',
      JSON.stringify(saved));
  }

  // 🚨 値を黙って消さないこと (Codex R1 high)。画面は属性側の メーカー型番 を隠すので、
  // 欄と違う値が残っていると人が気づかないまま保存で消える → サーバ側で 400 にする
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = 'toys3pen' WHERE draft_id = ?`)
    .run('[{"name":"ブランド名","values":["テストブランド"]},{"name":"メーカー型番","values":["別の型番XYZ"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, {
    attributes: [{ name: 'ブランド名', value: 'テストブランド' }],   // 画面は メーカー型番 を送らない
    article_number: 'toys3pen',
  });
  {
    const saved = db.prepare('SELECT attributes_json FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id);
    check('メーカー型番: DB に残った別の値は黙って消さず 400 で止める',
      r.status === 400 && String(r.json.error || '').includes('別の型番XYZ')
      && String(saved.attributes_json).includes('別の型番XYZ'),   // 保存されず DB もそのまま
      `${r.status} ${JSON.stringify(r.json)}`);
  }
  // 複数残っている場合も「どれが正か」を人に決めさせる
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ? WHERE draft_id = ?`)
    .run('[{"name":"メーカー型番","values":["AAA"]},{"name":"メーカー型番","values":["BBB"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, {
    attributes: [{ name: 'ブランド名', value: 'テストブランド' }], article_number: 'AAA',
  });
  check('メーカー型番: 属性側に複数あるときは 400 (どれかが黙って消えない)',
    r.status === 400 && String(r.json.error || '').includes('複数'), `${r.status} ${JSON.stringify(r.json)}`);
  // 🚨 競合を検出したあと、画面から**直せる**こと (Codex R2 high)。
  // 止めるだけだと「OLD が DB に残っているので何を送っても 400」のデッドロックになる
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = 'NEW' WHERE draft_id = ?`)
    .run('[{"name":"メーカー型番","values":["OLD"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { attributes: [], article_number: 'NEW' });
  check('メーカー型番: 競合中はフラグ無しの保存を止める (誤って消さない)', r.status === 400, `${r.status}`);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`,
    { attributes: [], article_number: 'NEW', resolve_model_conflict: true });
  check('メーカー型番: 解消に model_conflict_seen が無ければ 400 (楽観ロックを迂回させない)',
    r.status === 400 && String(r.json.error || '').includes('model_conflict_seen'), `${r.status} ${JSON.stringify(r.json)}`);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`,
    { attributes: [], article_number: 'NEW', resolve_model_conflict: true, model_conflict_seen: ['OLD'] });
  {
    const saved = db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id);
    check('メーカー型番: 人が「欄の値を残す」と決めたら解消できる (デッドロックにしない)',
      r.status === 200 && saved.article_number === 'NEW'
      && !String(saved.attributes_json).includes('OLD'), `${r.status} ${JSON.stringify(saved)}`);
  }
  // 「型番なしにする」も選べる (空にできないと直せない商品が出る)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = 'NEW' WHERE draft_id = ?`)
    .run('[{"name":"メーカー型番","values":["OLD"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`,
    { attributes: [], article_number: '', resolve_model_conflict: true, model_conflict_seen: ['OLD'] });
  {
    const saved = db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id);
    check('メーカー型番: 「型番なしにする」も選べる (空にできる)',
      r.status === 200 && !saved.article_number && !String(saved.attributes_json).includes('OLD'),
      `${r.status} ${JSON.stringify(saved)}`);
  }
  // カタログID (JAN) 本体は /rakuten では受けない (2026-09-02: 入力は基本情報タブだけ)。
  // 旧クライアントが catalog_jan を送ってきても jan_code は触らない
  db.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run('4901234567894', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { catalog_jan: '4999999999999', attributes: [] });
  check('カタログID: /rakuten の catalog_jan は無視され jan_code は変わらない (入口は基本情報タブだけ)',
    r.status === 200 && db.prepare('SELECT jan_code FROM product_drafts WHERE id = ?').get(gdraft3.id).jan_code === '4901234567894',
    `${r.status} ${JSON.stringify(r.json)}`);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { catalog_jan: '', attributes: [] });
  check('カタログID: catalog_jan の空文字でも jan_code は消えない',
    r.status === 200 && db.prepare('SELECT jan_code FROM product_drafts WHERE id = ?').get(gdraft3.id).jan_code === '4901234567894',
    `${r.status}`);
  // 「IDなしの理由」は /rakuten で保存する (JAN の無い SKU に使う)
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { catalog_id_exemption_reason: '3', attributes: [] });
  check('カタログID: IDなしの理由が保存される',
    r.status === 200 && db.prepare('SELECT catalog_id_exemption_reason FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id).catalog_id_exemption_reason === 3,
    `${r.status}`);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { catalog_id_exemption_reason: '9', attributes: [] });
  check('カタログID: 範囲外の理由は 400', r.status === 400, `${r.status} ${JSON.stringify(r.json)}`);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { attributes: [] });
  check('カタログID: 理由を送らない保存では既存の理由を維持する',
    r.status === 200 && db.prepare('SELECT catalog_id_exemption_reason FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id).catalog_id_exemption_reason === 3,
    `${r.status}`);
  db.prepare('UPDATE product_drafts SET jan_code = NULL WHERE id = ?').run(gdraft3.id);

  // 旧形式 {name, value} で残っている値も拾う (Codex R2 medium: values 配列だけ見ると消える)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = 'NEW' WHERE draft_id = ?`)
    .run('[{"name":"メーカー型番","value":"OLDFORM"}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { attributes: [], article_number: 'NEW' });
  check('メーカー型番: 旧形式 {name,value} の値も競合として拾う',
    r.status === 400 && String(r.json.error || '').includes('OLDFORM'), `${r.status} ${JSON.stringify(r.json)}`);
  // 壊れた JSON は上書きせず止める (中身が分からなくなる)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ? WHERE draft_id = ?`).run('{壊れ', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { attributes: [], article_number: 'NEW' });
  check('メーカー型番: 既存の属性JSONが壊れていたら上書きせず止める',
    r.status === 400 && String(r.json.error || '').includes('壊れています'), `${r.status} ${JSON.stringify(r.json)}`);
  // 同じ値が 2 行に重複した旧データでも解消できる (Codex R6 medium: 片側だけ集合化すると永久 409)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = 'NEW' WHERE draft_id = ?`)
    .run('[{"name":"メーカー型番","values":["DUP"]},{"name":"メーカー型番","values":["DUP"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`,
    { attributes: [], article_number: 'NEW', resolve_model_conflict: true, model_conflict_seen: ['DUP'] });
  check('メーカー型番: 同じ値が重複した旧データでも解消できる (件数差で 409 にしない)',
    r.status === 200
    && db.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id).article_number === 'NEW',
    `${r.status} ${JSON.stringify(r.json)}`);

  // 部分更新 (attributes 省略) でも競合を新しく作らせない (Codex R5 medium)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = NULL WHERE draft_id = ?`)
    .run('[{"name":"メーカー型番","values":["OLD"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { article_number: 'NEW' });
  check('メーカー型番: attributes を省いた部分更新でも競合を止める',
    r.status === 400 && String(r.json.error || '').includes('OLD'), `${r.status} ${JSON.stringify(r.json)}`);

  // 楽観ロック: 画面が見ていた旧値と DB が食い違うなら 409 (別タブで変わった値を捨てない)
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`,
    { attributes: [], article_number: 'NEW', resolve_model_conflict: true, model_conflict_seen: ['見ていない値'] });
  check('メーカー型番: 画面が見た旧値と DB が違えば 409 (見ていない値を捨てない)',
    r.status === 409 && String(r.json.error || '').includes('別の画面'), `${r.status} ${JSON.stringify(r.json)}`);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`,
    { attributes: [], article_number: 'NEW', resolve_model_conflict: true, model_conflict_seen: ['OLD'] });
  check('メーカー型番: 見た旧値が一致すれば解消できる',
    r.status === 200
    && db.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id).article_number === 'NEW',
    `${r.status} ${JSON.stringify(r.json)}`);

  // 部分更新でも メーカー型番 の行は残さない (次の保存でまた競合になる)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = NULL WHERE draft_id = ?`)
    .run('[{"name":"ブランド名","values":["B"]},{"name":"メーカー型番","values":["ONLY"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { genre_id: '565004' });
  {
    const saved = db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id);
    check('メーカー型番: 部分更新でも属性側から落として欄へ寄せる',
      r.status === 200 && saved.article_number === 'ONLY'
      && !String(saved.attributes_json).includes('メーカー型番')
      && String(saved.attributes_json).includes('ブランド名'), `${r.status} ${JSON.stringify(saved)}`);
  }

  // attributes を送ってこない POST で既存属性が全部消えないこと (Codex R3 medium)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = NULL WHERE draft_id = ?`)
    .run('[{"name":"ブランド名","values":["残るはず"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { article_number: 'only-model' });
  {
    const saved = db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id);
    check('属性: attributes を送らない POST で既存の属性が消えない',
      r.status === 200 && String(saved.attributes_json).includes('残るはず') && saved.article_number === 'only-model',
      `${r.status} ${JSON.stringify(saved)}`);
  }
  db.prepare(`UPDATE draft_rakuten SET attributes_json = '[]', article_number = NULL WHERE draft_id = ?`).run(gdraft3.id);

  // 欄が空で属性側にだけある旧データは、欄へ引き上げて保存できる (値を失わない)
  db.prepare(`UPDATE draft_rakuten SET attributes_json = ?, article_number = NULL WHERE draft_id = ?`)
    .run('[{"name":"メーカー型番","values":["legacy-123"]}]', gdraft3.id);
  r = await callPh('POST', `/api/drafts/${gdraft3.id}/rakuten`, { attributes: [] });
  {
    const saved = db.prepare('SELECT attributes_json, article_number FROM draft_rakuten WHERE draft_id = ?').get(gdraft3.id);
    check('メーカー型番: 欄が空の旧データは欄へ引き上げて保存される (値を失わない)',
      r.status === 200 && saved.article_number === 'legacy-123'
      && !String(saved.attributes_json).includes('メーカー型番'), JSON.stringify(saved));
  }

  // 後始末
  db.prepare(`UPDATE product_drafts SET status = 'draft', generation_claim_run_id = NULL, generation_claim_until = NULL,
    generation_block_code = NULL, generation_block_reason = NULL, generation_blocked_at = NULL, generation_blocked_by = NULL,
    checking_reason_code = NULL, checking_note = NULL, checking_since = NULL, checking_by = NULL
    WHERE id IN (?, ?, ?)`).run(gdraft.id, gdraft2.id, gdraft3.id);
  server.close();
}

// ─── SP広告KW の夜間 AI (PR3a・2026-09-23): service-api (miniPC の実行役が使う口) を HTTP で ───
// 1 件ずつ claim → reserve (AI を呼ぶ前に予約) → result (応答断の再送は同じ receipt) / fail / release。フラグ OFF は 503・トークン無しは 401
{
  process.env.PH_SERVICE_TOKEN = 'smoke-token-1234567890';
  const express = (await import('express')).default;
  const { serviceApiRouter } = await import('../router.js');
  const akm = await import('../lib/ad-keywords.js');
  const aim = await import('../lib/ad-kw-ai.js');
  const app = express();
  app.use('/svc', serviceApiRouter);
  const server = app.listen(0);
  const base = `http://127.0.0.1:${server.address().port}/svc`;
  const call = async (method, p, body, token = 'smoke-token-1234567890') => {
    const res = await fetch(base + p, { method, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) },
      body: body !== undefined ? JSON.stringify(body) : undefined });
    return { status: res.status, json: await res.json().catch(() => ({})) };
  };
  const savedFlag = process.env.AD_KW_AI_ENABLED;
  const aiDraft = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, own_brand) VALUES ('ADKW-AI-1', 'ハッカ油 AI', 'smoke', 1)`).run().lastInsertRowid);
  const draftRow = () => db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(aiDraft);
  const rq = akm.ensureRequest(db, draftRow(), { idempotencyKey: 'k-ai', actor: 'smoke' }).request;
  const ev = Number(db.prepare(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json, fetched_at) VALUES (?, 'suggest', 'ハッカ油', 'success', '{}', '[]', '2026-09-23T01:00:00Z')`).run(rq.id).lastInsertRowid);
  db.prepare(`INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key) VALUES (?, 'kw', 'ハッカ油 スプレー', 'ハッカ油 スプレー', 'observed', ?, ?, 1, 'a')`)
    .run(rq.id, ev, JSON.stringify([{ evidence_id: ev, seed: 'ハッカ油', source: 'base' }]));

  delete process.env.AD_KW_AI_ENABLED;
  let r = await call('POST', '/ad-kw-ai/claim', { runner_run_id: 'n1' });
  check('AI svc: フラグ OFF の claim は 503 (実行役の導入前は動かない)', r.status === 503 && r.json.code === 'ai_disabled', JSON.stringify(r));
  r = await call('GET', '/ad-kw-ai/queue', undefined, null);
  check('AI svc: トークン無しは 401', r.status === 401);
  process.env.AD_KW_AI_ENABLED = '1';
  const job = aim.requestAiJob(db, draftRow(), rq.id, { idempotencyKey: 'j', actor: 'smoke' }).job;
  r = await call('GET', '/ad-kw-ai/queue');
  check('AI svc: 要約に claimable 1', r.status === 200 && r.json.queue.claimable === 1 && r.json.queue.enabled === true, JSON.stringify(r.json));
  r = await call('POST', '/ad-kw-ai/claim', { runner_run_id: 'n1' });
  const lease = r.json.job && r.json.job.lease_token;
  check('AI svc: claim = lease と固定 packet (観測語つき)', r.status === 200 && r.json.job.job_id === job.id && lease && r.json.job.packet.observations[0].value === 'ハッカ油 スプレー'
    && r.json.job.packet_hash === job.packet_hash, JSON.stringify(r.json).slice(0, 300));
  r = await call('POST', `/ad-kw-ai/jobs/${job.id}/reserve`, { lease_token: 'bad', model: 'claude-sonnet-5', prompt_version: 'p1' });
  check('AI svc: 別の token の予約は 409', r.status === 409 && r.json.code === 'lease_lost');
  r = await call('POST', `/ad-kw-ai/jobs/${job.id}/reserve`, { lease_token: lease, model: 'claude-sonnet-5', prompt_version: 'p1' });
  const gid = r.json.generation_id;
  check('AI svc: 予約できる', r.status === 200 && gid > 0, JSON.stringify(r.json));
  r = await call('POST', `/ad-kw-ai/jobs/${job.id}/release`, { lease_token: lease });
  check('AI svc: 予約後の release は needs_review (成否不明)', r.status === 200 && r.json.status === 'needs_review', JSON.stringify(r.json));
  const out = { keywords: [{ keyword: 'ハッカ油 ルームスプレー', basis_obs_ids: ['o1'], reason: '用途を広げた' }, { keyword: 'ハッカ油 スプレー', basis_obs_ids: ['o1'] }] };
  r = await call('POST', `/ad-kw-ai/generations/${gid}/result`, { packet_hash: job.packet_hash, output: out });
  check('AI svc: 予約済みなら lease を失っても結果を受ける (復旧)', r.status === 200 && r.json.receipt.accepted === 2 && r.json.receipt.new_candidates === 1 && r.json.replay === false, JSON.stringify(r.json));
  const receipt1 = r.json.receipt;
  r = await call('POST', `/ad-kw-ai/generations/${gid}/result`, { packet_hash: job.packet_hash, output: out });
  check('AI svc: 同じ内容の再送 = 同じ receipt (replay)', r.status === 200 && r.json.replay === true && JSON.stringify(r.json.receipt) === JSON.stringify(receipt1));
  r = await call('POST', `/ad-kw-ai/generations/${gid}/result`, { packet_hash: job.packet_hash, output: { keywords: [] } });
  check('AI svc: 確定後に別の内容は 409', r.status === 409 && r.json.code === 'already_finalized');
  r = await call('POST', `/ad-kw-ai/generations/999999/result`, { packet_hash: 'x', output: out });
  check('AI svc: 無い予約は 404', r.status === 404);
  r = await call('POST', `/ad-kw-ai/jobs/${job.id}/fail`, { lease_token: lease, code: 'network' });
  check('AI svc: 終わった job への fail は 409 (巻き戻さない)', r.status === 409 && db.prepare('SELECT status FROM ph_ad_kw_ai_jobs WHERE id = ?').get(job.id).status === 'done');
  const st = akm.stateForDraft(db, draftRow(), { configured: true });
  const aiCand = st.candidates.find((c) => c.value === 'ハッカ油 ルームスプレー');
  check('AI svc: 画面の状態に AI の候補 (未採用) と提案 (AI だけ・理由・根拠) が載る',
    aiCand && aiCand.origin === 'ai' && aiCand.decision === null && st.ai.proposals[aiCand.id].observed === 'ai_only' && st.ai.proposals[aiCand.id].reason === '用途を広げた'
    && JSON.stringify(st.ai.proposals[aiCand.id].basis) === '["ハッカ油 スプレー"]' && st.ai.jobs[0].status === 'done', JSON.stringify(st.ai).slice(0, 300));
  if (savedFlag === undefined) delete process.env.AD_KW_AI_ENABLED; else process.env.AD_KW_AI_ENABLED = savedFlag;
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(aiDraft);
  server.close();
}

// ─── SP広告 検索KW PR1 (2026-09-23): 依頼 → サジェスト収集 (miniPC は差し替え) → 採否 (append-only) → コピー履歴 ───
// 守りたいこと (『Amazon_SP広告KW自動生成_設計方針_20260922.md』§5「検証で必須にすること」):
//   失敗 / 0 件 / 一部 / 未実行 を混ぜない・取消後と lease を失った結果を保存しない・同じ依頼で 2 つ同時に集めない・
//   採否は人の API だけ (append-only)・コピー本文は採否版を参照して固定・自社商品以外では使えない
{
  const express = (await import('express')).default;
  const routerMod = await import('../router.js');
  const kwClient = await import('../lib/keyword-suggest-client.js');
  const abaClient = await import('../lib/aba-client.js');
  const app = express();
  app.use((req, res, next) => { req.session = { email: 'smoke@b-faith.biz', displayName: 'smoke', role: 'admin' }; next(); });
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
  const getHtml = async (p) => { const res = await fetch(base + p); return { status: res.status, html: await res.text() }; };

  // 「miniPC を呼ぶ設定あり」にする。本物は呼ばない (fetcher を差し替える)
  const savedToken = process.env.WAREHOUSE_SERVICE_TOKEN;
  process.env.WAREHOUSE_SERVICE_TOKEN = 'smoke-token';
  // miniPC の応答の形 (prefix は 基本 1 + ひらがな 46 = 47 件。a〜z 指定なら +26)。summary は prefix から数える
  // (クライアントは prefix の件数と requested が合わない応答を受け取らない)
  const HIRA = 'あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん'.split('');
  const suggestResult = (seed, { alphabet = false } = {}) => {
    const prefixes = [{ prefix: seed, source: 'base', status: 'success', count: 3, fetchedAt: '2026-09-23T01:00:00.000Z' }];
    for (const h of HIRA) {
      const st = h === 'あ' || h === 'す' ? 'success' : h === 'い' || h === 'う' ? 'failed' : h === 'わ' ? 'unrun' : 'empty';
      prefixes.push({ prefix: `${seed} ${h}`, source: `hiragana:${h}`, status: st, count: st === 'success' ? 1 : 0,
        error: st === 'failed' ? 'timeout' : st === 'unrun' ? 'maxRequests に達したため未実行' : null, fetchedAt: st === 'unrun' ? null : '2026-09-23T01:00:01.000Z' });
    }
    if (alphabet) for (const a of 'abcdefghijklmnopqrstuvwxyz') prefixes.push({ prefix: `${seed} ${a}`, source: `alphabet:${a}`, status: a === 'a' ? 'success' : 'empty', count: a === 'a' ? 1 : 0, fetchedAt: '2026-09-23T01:00:02.000Z' });
    const summary = { requested: prefixes.length, success: 0, empty: 0, failed: 0, unrun: 0, requests: 0, stopped: null };
    for (const p of prefixes) { summary[p.status] += 1; if (p.status !== 'unrun') summary.requests += 1; }
    return {
      seed, total: 5,
      suggestions: [
        { keyword: `${seed} スプレー`, source: 'base', depth: 0 },
        { keyword: `${seed} 虫除け`, source: 'base', depth: 0 },
        { keyword: `${seed} あせも`, source: 'hiragana:あ', depth: 0 },
        { keyword: seed, source: 'base', depth: 0 },                        // 種そのもの → 候補にしない
        { keyword: `${seed}  スプレー `, source: 'hiragana:す', depth: 0 },   // 空白違いの同じ語 → 1 つ
      ],
      prefixes, summary, fetchedAt: '2026-09-23T01:00:00.000Z', options: { alphabet },
    };
  };
  // 内訳を指定して応答を作る (prefix 別の状態と summary を必ず一致させる。クライアントは食い違いを受け取らない)
  const resultWith = (seed, { success = 0, empty = 0, failed = 0, unrun = 0, stopped = null, suggestions = [], alphabet = false } = {}) => {
    const sources = ['base', ...HIRA.map((h) => `hiragana:${h}`), ...(alphabet ? 'abcdefghijklmnopqrstuvwxyz'.split('').map((a) => `alphabet:${a}`) : [])];
    const statuses = [...Array(success).fill('success'), ...Array(empty).fill('empty'), ...Array(failed).fill('failed'), ...Array(unrun).fill('unrun')];
    if (statuses.length !== sources.length) throw new Error(`fixture: 内訳 ${statuses.length} ≠ prefix ${sources.length}`);
    const prefixes = sources.map((source, i) => ({
      prefix: i === 0 ? seed : `${seed} ${source.split(':')[1]}`, source, status: statuses[i], count: statuses[i] === 'success' ? 1 : 0,
      error: statuses[i] === 'failed' ? 'HTTP 503' : statuses[i] === 'unrun' ? '未実行' : null, fetchedAt: statuses[i] === 'unrun' ? null : '2026-09-23T01:00:00.000Z',
    }));
    return { seed, total: suggestions.length, suggestions, prefixes,
      summary: { requested: prefixes.length, success, empty, failed, unrun, requests: prefixes.length - unrun, stopped },
      fetchedAt: '2026-09-23T01:00:00.000Z', options: { alphabet } };
  };
  let fetcherCalls = [];
  let fetcherImpl = async (body) => { fetcherCalls.push(body); return suggestResult(body.seed); };
  kwClient._setSuggestFetcher((body) => fetcherImpl(body));
  // miniPC の ABA 参照 (/service-api/aba/lookup・PR #1414) の応答の形。走査しない = 取込済みの週をそのまま返す
  const abaWeek = (ws, we, extra = {}) => ({ week_start: ws, week_end: we, ingested_at: `${we}T23:30:00Z`, term_count: 400000, row_count: 1300000, parsed_count: 1300000, mode: 'full', skipped_count: 0, pruned_at: null, ...extra });
  const abaTerm = (t, rank, pos, cs, conv) => ({ search_term: t, department: 'amazon.co.jp', search_frequency_rank: rank, click_position: pos, click_share: cs, conversion_share: conv });
  const abaResult = (asin, item, week = abaWeek('2026-09-13', '2026-09-19')) => ({
    week, requested_week: null, week_coverage: week ? 'complete' : 'unknown', registered: true, register_errors: [], invalid: [],
    items: [{ asin, proof: 'week_ingested', reason: null, ...item }],
  });
  let abaCalls = [];
  let abaImpl = async (body) => { abaCalls.push(body); return abaResult(body.asins[0], { status: 'found', coverage: 'complete', terms: [abaTerm('ハッカ油 スプレー', 1200, 2, 0.21, 0.15)] }); };
  abaClient._setAbaFetcher((body, path) => abaImpl(body, path));

  const idOwn = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, own_brand, asin)
    VALUES ('ADKW-1', 'ハッカ油スプレー', 'smoke', 1, 'B0ADKWOWN1')`).run().lastInsertRowid);
  const idOwn2 = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, own_brand)
    VALUES ('ADKW-2', '別の自社商品', 'smoke', 1)`).run().lastInsertRowid);
  const idOther = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, own_brand)
    VALUES ('ADKW-3', '他社商品', 'smoke', 0)`).run().lastInsertRowid);
  const P = (id) => `/api/drafts/${id}/ad-keywords`;

  // 自社商品でなければ使えない (受付・状態・画面)
  let r = await call('POST', `${P(idOther)}/requests`, { idempotency_key: 'k-other' });
  check('SP広告KW: 自社商品でないドラフトは受け付けない (400 not_own_brand)', r.status === 400 && r.json.code === 'not_own_brand', JSON.stringify(r.json));
  r = await call('GET', P(idOther));
  check('SP広告KW: 自社商品でないドラフトは状態も返さない', r.status === 400 && r.json.code === 'not_own_brand');
  {
    const pg = await getHtml(`/detail/${idOther}`);
    check('SP広告KW: 自社商品でない詳細画面にはタブが無い', pg.status === 200 && !pg.html.includes('data-tab="tab-adkw"') && !pg.html.includes('id="adkw-json"'));
  }
  r = await call('GET', P(idOwn));
  check('SP広告KW: 集める前の状態 = 依頼なし・設定あり', r.status === 200 && r.json.state.request === null && r.json.state.configured === true, JSON.stringify(r.json).slice(0, 200));

  // 依頼: 冪等キー
  r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k1' });
  const rid = r.json.request_id;
  check('SP広告KW: 依頼を作れる', r.status === 200 && Number.isInteger(rid) && r.json.reused === false, JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k1' });
  check('SP広告KW: 同じ冪等キーの再送は同じ依頼 (二重に作らない)', r.json.request_id === rid && r.json.reused === true, JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k2' });
  check('SP広告KW: 開いている依頼があれば別キーでもそれを返す (restart 無し)', r.json.request_id === rid && r.json.reused === true, JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests`, {});
  check('SP広告KW: 冪等キー無しは受け付けない', r.status === 400 && r.json.code === 'bad_key');

  // 収集
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: '  ハッカ油 ' });
  check('SP広告KW: 収集 → 候補 3 語 (種そのもの・空白違いの重複は除く)', r.status === 200 && r.json.collected === true && r.json.added === 3, JSON.stringify(r.json).slice(0, 300));
  check('SP広告KW: miniPC へは種 1 つ・ひらがな固定・深掘り無しで頼む',
    fetcherCalls.length === 1 && fetcherCalls[0].seed === 'ハッカ油' && fetcherCalls[0].hiragana === true && fetcherCalls[0].alphabet === false && !('depth' in fetcherCalls[0]),
    JSON.stringify(fetcherCalls));
  const st1 = r.json.state;
  check('SP広告KW: 取得範囲が 失敗/未実行 込みで残る (partial)',
    st1.seeds.length === 1 && st1.seeds[0].status === 'partial' && /2 回失敗/.test(st1.seeds[0].coverage_text) && /上限で 1 回未実行/.test(st1.seeds[0].coverage_text),
    JSON.stringify(st1.seeds));
  check('SP広告KW: 出典の文 (§4.8) = 観測・取得日・検索回数は不明', /Amazon サジェストで観測・取得日 9月23日。検索回数は不明/.test(st1.seeds[0].fetched_text), st1.seeds[0].fetched_text);
  check('SP広告KW: 候補は全件「未採用」から始まる', st1.candidates.length === 3 && st1.candidates.every((c) => c.decision === null));
  check('SP広告KW: 並びは出方の順 (そのまま → +あ)。総合点は無い',
    st1.candidates.map((c) => c.observed[0].source_label).join(',') === 'そのまま,そのまま,+あ' && !('score' in st1.candidates[0]),
    JSON.stringify(st1.candidates.map((c) => [c.value, c.observed[0].source_label])));
  check('SP広告KW: 収集が終わると依頼は review_ready に戻り lease が消える',
    st1.request.status === 'review_ready' && st1.request.collecting_seed === null
    && db.prepare('SELECT collecting_token FROM ph_ad_kw_requests WHERE id = ?').get(rid).collecting_token === null);
  check('SP広告KW: 材料 (evidence) に prefix ごとの状態がそのまま残る',
    JSON.parse(db.prepare(`SELECT raw_json FROM ph_ad_kw_evidence WHERE request_id = ? AND seed = 'ハッカ油'`).get(rid).raw_json).some((p) => p.status === 'unrun'));

  fetcherCalls = [];
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ハッカ油' });
  check('SP広告KW: 取得済みの種は miniPC を呼ばず「取得済み」を返す', r.status === 200 && r.json.reused === true && r.json.added === 0 && fetcherCalls.length === 0, JSON.stringify(r.json).slice(0, 200));

  fetcherImpl = async (body) => resultWith(body.seed, { success: 2, empty: 45, suggestions: [{ keyword: 'ハッカ油 スプレー', source: 'base' }, { keyword: 'はっか油 業務用', source: 'base' }] });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'はっか油' });
  check('SP広告KW: 別の種で同じ語が出たら候補は増えず観測が 2 つになる',
    r.json.added === 1 && r.json.merged === 1 && r.json.state.candidates.find((c) => c.value === 'ハッカ油 スプレー').observed_count === 2,
    JSON.stringify(r.json).slice(0, 300));
  check('SP広告KW: 失敗も未実行も無い種は success', r.json.state.seeds.find((s) => s.seed === 'はっか油').status === 'success');

  // miniPC が落ちている → 「取れなかった」を記録 (0 件と混ぜない)。依頼は閉じない
  fetcherImpl = async () => { const e = new Error('HTTP 502'); e.code = 'unreachable'; throw e; };
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ひば油' });
  check('SP広告KW: miniPC が落ちていれば「取れなかった」を記録して返す (200・collected=false・理由つき)',
    r.status === 200 && r.json.collected === false && r.json.evidence.status === 'failed' && /unreachable/.test(r.json.error || ''),
    JSON.stringify(r.json).slice(0, 300));
  check('SP広告KW: 失敗した種は候補 0・依頼は review_ready のまま',
    r.json.state.seeds.find((s) => s.seed === 'ひば油').candidate_count === 0 && r.json.state.request.status === 'review_ready');
  fetcherImpl = async (body) => resultWith(body.seed, { success: 1, empty: 46, suggestions: [{ keyword: 'ひば油 スプレー', source: 'base' }] });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ひば油' });
  check('SP広告KW: 失敗した種はもう一度集められ、種の表示は 1 つ (取得回は 2 回・いまの状態は success)',
    r.json.collected === true && r.json.state.seeds.filter((s) => s.seed === 'ひば油').length === 1
    && r.json.state.seeds.find((s) => s.seed === 'ひば油').status === 'success' && r.json.state.seeds.find((s) => s.seed === 'ひば油').fetch_count === 2
    && db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND seed = 'ひば油'`).get(rid).n === 2, JSON.stringify(r.json.state.seeds));
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: '' });
  check('SP広告KW: 空の種は受け付けない (何も記録しない)', r.status === 400 && r.json.code === 'bad_seed');
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'x'.repeat(61) });
  check('SP広告KW: 61 文字の種は受け付けない', r.status === 400 && r.json.code === 'bad_seed');

  // 排他: 進行中の収集がある依頼では 409。死んだ収集中 (3 分超) は奪える
  db.prepare(`UPDATE ph_ad_kw_requests SET status = 'collecting', collecting_seed = 'x', collecting_token = 't', collecting_since = ? WHERE id = ?`)
    .run(new Date().toISOString(), rid);
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ゆず' });
  check('SP広告KW: 別の収集が進行中なら 409 (同じ依頼で 2 つ同時に走らせない)', r.status === 409 && r.json.code === 'busy', JSON.stringify(r.json));
  db.prepare('UPDATE ph_ad_kw_requests SET collecting_since = ? WHERE id = ?').run(new Date(Date.now() - 10 * 60 * 1000).toISOString(), rid);
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ゆず' });
  check('SP広告KW: 死んだ収集中 (3 分超) は奪って集められる', r.status === 200 && r.json.collected === true, JSON.stringify(r.json).slice(0, 200));

  // lease を失った結果は保存しない: 収集の途中で死んだとみなされ、別の収集に奪われた
  {
    let inner = null;
    fetcherImpl = async (body) => {
      if (body.seed === 'ラベンダー') {
        db.prepare('UPDATE ph_ad_kw_requests SET collecting_since = ? WHERE id = ?').run(new Date(Date.now() - 10 * 60 * 1000).toISOString(), rid);
        inner = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ローズマリー' });   // 奪う側 (新しい token)
        return { ...suggestResult(body.seed), suggestions: [{ keyword: 'ラベンダー 香り', source: 'base' }] };
      }
      return resultWith(body.seed, { success: 1, empty: 46, suggestions: [{ keyword: `${body.seed} 精油`, source: 'base' }] });
    };
    r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ラベンダー' });
    check('SP広告KW: lease を奪われた古い収集の結果は保存しない (409 lost_lease・材料も候補も無い)',
      r.status === 409 && r.json.code === 'lost_lease' && inner && inner.json.collected === true
      && !db.prepare(`SELECT 1 FROM ph_ad_kw_evidence WHERE request_id = ? AND seed = 'ラベンダー'`).get(rid)
      && !db.prepare(`SELECT 1 FROM ph_ad_kw_candidates WHERE request_id = ? AND value = 'ラベンダー 香り'`).get(rid),
      JSON.stringify(r.json));
    check('SP広告KW: 奪った側の結果は保存され、依頼は review_ready に戻る',
      db.prepare(`SELECT status FROM ph_ad_kw_evidence WHERE request_id = ? AND seed = 'ローズマリー'`).get(rid)?.status === 'success'
      && db.prepare('SELECT status FROM ph_ad_kw_requests WHERE id = ?').get(rid).status === 'review_ready');
  }

  // 全部失敗 (通信はできたが 47 回とも失敗) は「取れなかった」だが取得範囲は残す (R1 #8)
  fetcherImpl = async (body) => ({
    seed: body.seed, total: 0, suggestions: [],
    prefixes: Array.from({ length: 47 }, (_, i) => ({ prefix: `${body.seed} ${i}`, source: i === 0 ? 'base' : `hiragana:${i}`, status: 'failed', error: 'HTTP 503' })),
    summary: { requested: 47, success: 0, empty: 0, failed: 47, unrun: 0, requests: 94, stopped: null }, fetchedAt: '2026-09-23T02:00:00.000Z',
  });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'すだち' });
  {
    const s = r.json.state.seeds.find((x) => x.seed === 'すだち');
    check('SP広告KW: 全 prefix 失敗は failed でも取得範囲 (47 回失敗) を残す', r.json.collected === true && s.status === 'failed' && /47 回失敗/.test(s.coverage_text) && s.candidate_count === 0, JSON.stringify(s));
    const evId = s.id;
    fetcherImpl = async (body) => resultWith(body.seed, { success: 1, empty: 46, suggestions: [{ keyword: 'すだち 果汁', source: 'base' }] });
    r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'すだち' });
    const s2 = r.json.state.seeds.find((x) => x.seed === 'すだち');
    check('SP広告KW: 失敗した種の取り直しは取得回の行を足す (前の失敗の記録は残る・いまの状態は新しい行)',
      r.json.collected === true && s2.id !== evId && s2.status === 'success' && s2.candidate_count === 1 && s2.fetch_count === 2
      && db.prepare('SELECT status FROM ph_ad_kw_evidence WHERE id = ?').get(evId).status === 'failed', JSON.stringify(s2));
  }
  // 全体の期限で打ち切られた収集 (miniPC 側 40 秒) は理由つきで残る
  fetcherImpl = async (body) => resultWith(body.seed, { success: 1, empty: 36, unrun: 10, stopped: 'deadline', suggestions: [{ keyword: 'かぼす ポン酢', source: 'base' }] });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'かぼす' });
  check('SP広告KW: 期限で打ち切られた収集は partial・理由「全体の期限で打ち切り」つき', r.json.state.seeds.find((x) => x.seed === 'かぼす').status === 'partial'
    && /全体の期限で打ち切り・10 回未実行/.test(r.json.state.seeds.find((x) => x.seed === 'かぼす').coverage_text), JSON.stringify(r.json.state.seeds.find((x) => x.seed === 'かぼす')));

  // 条件を広げる (a〜z を足す) と取り直す。取れていた語の観測は二重に足さない (R1 #5)
  fetcherCalls = [];
  fetcherImpl = async (body) => { fetcherCalls.push(body); return resultWith(body.seed, { alphabet: true, success: 3, empty: 70, suggestions: [{ keyword: 'ハッカ油 スプレー', source: 'base' }, { keyword: 'はっか油 業務用', source: 'base' }, { keyword: 'はっか油 amazon', source: 'alphabet:a' }] }); };
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'はっか油' });
  check('SP広告KW: 取得済みの種を同じ条件で頼んでも取り直さない', r.json.reused === true && fetcherCalls.length === 0);
  const gyomuBefore = (await call('GET', P(idOwn))).json.state.candidates.find((c) => c.value === 'はっか油 業務用');
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'はっか油', alphabet: true });
  {
    const s = r.json.state.seeds.find((x) => x.seed === 'はっか油');
    const spray = r.json.state.candidates.find((c) => c.value === 'ハッカ油 スプレー');
    const gyomu = r.json.state.candidates.find((c) => c.value === 'はっか油 業務用');
    check('SP広告KW: a〜z を足すと同じ種でも取り直す (miniPC に alphabet:true で頼み、いまの取得回の条件が a〜z 込みになる)',
      r.json.collected === true && fetcherCalls.length === 1 && fetcherCalls[0].alphabet === true && s.options.alphabet === true && s.fetch_count === 2, JSON.stringify([fetcherCalls, s.options, s.fetch_count]));
    check('SP広告KW: 取り直しで同じ語の観測を二重に足さない (観測 2 のまま)・新しく出た語だけ増える',
      spray.observed_count === 2 && r.json.added === 1 && r.json.merged === 0 && r.json.state.candidates.some((c) => c.value === 'はっか油 amazon'), JSON.stringify([spray.observed_count, r.json.added, r.json.merged]));
    check('SP広告KW: 前の取得回で観測した語の出典 (取得回・日付) は取り直しで書き換わらない (R2 #4)',
      gyomu.evidence_id === gyomuBefore.evidence_id && gyomu.first_fetched_at === gyomuBefore.first_fetched_at && gyomu.observed.length === 1
      && gyomu.evidence_id !== s.id, JSON.stringify([gyomuBefore.evidence_id, gyomu.evidence_id, s.id]));
    check('SP広告KW: 種ごとの数 = 観測した語 (別の種で先に出た語も含む) と この種で初めて出た語 (R1 #9)',
      s.candidate_count === 3 && s.new_count === 2, JSON.stringify([s.candidate_count, s.new_count]));
  }
  // 一部取得 (partial) は「取り直す」指定のときだけ取り直す。取り直しが失敗しても前の材料は消えない
  fetcherCalls = [];
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ハッカ油' });
  check('SP広告KW: 一部取得の種は指定が無ければ取り直さない (取得済み扱い)', r.json.reused === true && fetcherCalls.length === 0);
  fetcherImpl = async () => { const e = new Error('HTTP 502'); e.code = 'unreachable'; throw e; };
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ハッカ油', retake: true });
  {
    const s = r.json.state.seeds.find((x) => x.seed === 'ハッカ油');
    check('SP広告KW: 一部取得の取り直しが通信で失敗しても、前の取得回と候補はそのまま (collected=false・「前の取得回は残っています」)',
      r.json.collected === false && /unreachable/.test(r.json.error || '') && r.json.previous_ok === true && s.status === 'failed' && s.previous_ok === true
      && /前の取得回/.test(s.status_ja) && s.candidate_count === 3, JSON.stringify([r.json.error, s.status, s.status_ja, s.candidate_count]));
  }
  // HTTP は成功したが全 prefix 失敗 (取り直し) → 前の取得回を failed で上書きしない (R2 #4)
  fetcherImpl = async (body) => ({
    seed: body.seed, total: 0, suggestions: [],
    prefixes: Array.from({ length: 47 }, (_, i) => ({ prefix: `${body.seed} ${i}`, source: i === 0 ? 'base' : `hiragana:${i}`, status: 'failed', error: 'HTTP 503' })),
    summary: { requested: 47, success: 0, empty: 0, failed: 47, unrun: 0, requests: 94, stopped: null }, fetchedAt: '2026-09-23T03:00:00.000Z',
  });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ハッカ油' });
  {
    const s = r.json.state.seeds.find((x) => x.seed === 'ハッカ油');
    const mushi = r.json.state.candidates.find((c) => c.value === 'ハッカ油 虫除け');
    check('SP広告KW: 取り直しで全 prefix 失敗でも、前の取得回 (partial) は残り、候補の出典は前の取得回のまま',
      r.json.collected === true && s.status === 'failed' && s.previous_ok === true && /47 回失敗/.test(s.coverage_text) && s.candidate_count === 3
      && mushi.observed[0].fetched_at === '2026-09-23T01:00:00.000Z'
      && db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND seed = 'ハッカ油' AND status = 'partial'`).get(rid).n === 1,
      JSON.stringify([s.status, s.previous_ok, s.coverage_text, s.candidate_count, mushi.observed]));
  }
  fetcherImpl = async (body) => resultWith(body.seed, { success: 2, empty: 45, suggestions: suggestResult(body.seed).suggestions });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ハッカ油' });   // いまの状態は failed なので指定なしでも取り直せる
  {
    const s = r.json.state.seeds.find((x) => x.seed === 'ハッカ油');
    const mushi = r.json.state.candidates.find((c) => c.value === 'ハッカ油 虫除け');
    check('SP広告KW: 取り直しが成功すると success になり、既にあった語の観測は増えない (同じ種・同じ出方)',
      r.json.collected === true && s.status === 'success' && r.json.added === 0 && r.json.merged === 0 && mushi.observed_count === 1, JSON.stringify([s.status, r.json.added, r.json.merged, mushi.observed_count]));
  }
  // 20 種の上限は「取れた種」で数える。失敗しかない種を取り直して取れるときも数える (R2 #10)
  {
    const validSeeds = db.prepare(`SELECT COUNT(DISTINCT seed) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND status != 'failed'`).get(rid).n;
    fetcherImpl = async (body) => resultWith(body.seed, { empty: 47 });
    for (let i = validSeeds; i < 20; i++) await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: `埋め${i}` });
    check('SP広告KW: 取れた種が 20 になった', db.prepare(`SELECT COUNT(DISTINCT seed) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND status != 'failed'`).get(rid).n === 20);
    r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: '21 個目' });
    check('SP広告KW: 21 種目は too_many', r.status === 400 && r.json.code === 'too_many', JSON.stringify(r.json));
    // 失敗しかない種 (すだち は success になっているので別の種で作る): 上限に達している状態で失敗行を作ることはできないので、
    // 上限の 1 つ手前で失敗行を作ってから、別の種で 20 に到達させ、失敗行の取り直しが too_many になることを見る
    db.prepare(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json, error) VALUES (?, 'suggest', '失敗だけ', 'failed', '{}', '[]', 'x')`).run(rid);
    r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: '失敗だけ' });
    check('SP広告KW: 失敗しかない種の取り直しも上限を超えない (too_many)', r.status === 400 && r.json.code === 'too_many', JSON.stringify(r.json));
    db.prepare(`DELETE FROM ph_ad_kw_evidence WHERE request_id = ? AND (seed LIKE '埋め%' OR seed = '失敗だけ')`).run(rid);
  }

  // 採否 (人の API・append-only)
  r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
  check('SP広告KW: 採用が無ければ本文を固定できない (400 nothing)', r.status === 400 && r.json.code === 'nothing', JSON.stringify(r.json));
  r = await call('GET', P(idOwn));
  const cands = r.json.state.candidates;
  const cSpray = cands.find((c) => c.value === 'ハッカ油 スプレー');
  const cMushi = cands.find((c) => c.value === 'ハッカ油 虫除け');
  r = await call('POST', `${P(idOwn)}/candidates/${cSpray.id}/decisions`, { decision: 'adopt' });
  check('SP広告KW: 採用にはマッチタイプが要る (400)', r.status === 400 && r.json.code === 'bad_match_type', JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/candidates/${cSpray.id}/decisions`, { decision: 'adopt', match_type: 'exact' });
  check('SP広告KW: 採用 (完全一致) を記録できる。語は候補の語がそのまま入る',
    r.status === 200 && r.json.decision.decision === 'adopt' && r.json.decision.keyword === 'ハッカ油 スプレー' && r.json.decision.match_type === 'exact'
    && r.json.decision.actor === 'smoke@b-faith.biz', JSON.stringify(r.json));
  const d1 = r.json.decision.id;
  r = await call('POST', `${P(idOwn)}/candidates/${cSpray.id}/decisions`, { decision: 'adopt', match_type: 'phrase', keyword: ' ハッカ油  スプレー 携帯 ' });
  check('SP広告KW: 語を直して採用し直すと新しい行 (訂正元つき)。前の行は残る',
    r.json.decision.keyword === 'ハッカ油 スプレー 携帯' && r.json.decision.supersedes_decision_id === d1
    && db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_decisions WHERE candidate_id = ?').get(cSpray.id).n === 2, JSON.stringify(r.json));
  {
    const rejects = (sql, ...args) => { try { db.prepare(sql).run(...args); return false; } catch (e) { return /append-only/.test(e.message); } };
    check('SP広告KW: 採否は append-only (UPDATE / DELETE がトリガーで拒否される)',
      rejects('UPDATE ph_ad_kw_decisions SET decision = ? WHERE id = ?', 'reject', d1) && rejects('DELETE FROM ph_ad_kw_decisions WHERE id = ?', d1));
  }
  r = await call('POST', `${P(idOwn)}/candidates/${cSpray.id}/decisions`, { decision: 'bogus' });
  check('SP広告KW: 不正な採否は 400', r.status === 400 && r.json.code === 'bad_decision');
  r = await call('POST', `${P(idOwn)}/candidates/999999/decisions`, { decision: 'reject' });
  check('SP広告KW: 無い候補は 404', r.status === 404);
  r = await call('POST', `${P(idOwn2)}/candidates/${cSpray.id}/decisions`, { decision: 'reject' });
  check('SP広告KW: 別のドラフトから他人の候補を触れない (404)', r.status === 404 && r.json.code === 'not_found', JSON.stringify(r.json));

  // コピー本文 = 採否版を参照して固定。同じ採否なら履歴を増やさない
  r = await call('POST', `${P(idOwn)}/candidates/${cMushi.id}/decisions`, { decision: 'adopt', match_type: 'exact' });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
  const e1 = r.json.export;
  check('SP広告KW: 本文を固定できる (マッチタイプ別に分かれる・採否版つき)',
    r.status === 200 && r.json.reused === false && e1.body.total === 2
    && e1.body.blocks.map((b) => `${b.match_type}:${b.text}`).join('|') === 'exact:ハッカ油 虫除け|phrase:ハッカ油 スプレー 携帯'
    && e1.decision_version === db.prepare('SELECT MAX(id) AS m FROM ph_ad_kw_decisions WHERE request_id = ?').get(rid).m,
    JSON.stringify(r.json).slice(0, 400));
  r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
  check('SP広告KW: 採否が変わっていなければ同じ履歴を返す (二重に作らない)', r.json.reused === true && r.json.export.id === e1.id);
  r = await call('POST', `${P(idOwn)}/candidates/${cSpray.id}/decisions`, { decision: 'adopt', match_type: 'exact', keyword: 'ハッカ油 スプレー' });
  r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
  check('SP広告KW: 採否を変えたら新しい履歴 (前の履歴はそのまま残る)',
    r.json.reused === false && r.json.export.id !== e1.id
    && r.json.export.body.blocks.length === 1 && r.json.export.body.blocks[0].match_type === 'exact' && r.json.export.body.blocks[0].count === 2
    && db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_exports WHERE request_id = ?').get(rid).n === 2, JSON.stringify(r.json).slice(0, 300));
  const e2 = r.json.export;
  r = await call('POST', `${P(idOwn)}/exports/${e2.id}/copied`, { match_type: 'exact' });
  check('SP広告KW: コピーした印が付く', r.status === 200 && typeof r.json.copied.exact === 'string', JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/exports/${e2.id}/copied`, { match_type: 'bogus' });
  check('SP広告KW: 不正なマッチタイプの印は 400', r.status === 400);
  r = await call('POST', `${P(idOwn2)}/exports/${e2.id}/copied`, { match_type: 'exact' });
  check('SP広告KW: 別のドラフトからコピー履歴に印を付けられない', r.status === 404);
  r = await call('GET', P(idOwn));
  check('SP広告KW: 状態にコピー履歴 (最新が先・印つき) と採否版が載る',
    r.json.state.exports[0].id === e2.id && typeof r.json.state.exports[0].copied.exact === 'string'
    && r.json.state.decision_version === e2.decision_version && r.json.state.adopted_count === 2, JSON.stringify(r.json.state.exports).slice(0, 300));
  check('SP広告KW: 「Amazon 登録済み」を表す状態を持たない (コピー済みは登録済みではない)',
    !JSON.stringify(r.json.state).includes('registered') && !JSON.stringify(r.json.state).includes('登録済み'));
  check('SP広告KW: 操作履歴 (draft_events) に 依頼・収集・採否・固定 が残る',
    ['ad_kw_request', 'ad_kw_collected', 'ad_kw_collect_failed', 'ad_kw_decision', 'ad_kw_export', 'ad_kw_copied']
      .every((ev) => db.prepare('SELECT 1 FROM draft_events WHERE draft_id = ? AND event = ?').get(idOwn, ev)));

  // ─── 採用 = 完全一致＋フレーズ一致 (exact_phrase・2026-09-23 中原さん「基本、完全一致とフレーズ一致を全部かけている」) ───
  check('SP広告KW: 採否の選択肢は 完全一致＋フレーズ一致 が先頭 (既定)', JSON.stringify(r.json.state.match_types) === '["exact_phrase","exact","phrase","broad"]'
    && r.json.state.labels.match_type.exact_phrase === '完全一致＋フレーズ一致' && !('exact_phrase' in r.json.state.labels.copy_block), JSON.stringify(r.json.state.match_types));
  r = await call('POST', `${P(idOwn)}/candidates/${cMushi.id}/decisions`, { decision: 'adopt', match_type: 'exact_phrase' });
  check('SP広告KW: 完全一致＋フレーズ一致 で採用できる', r.status === 200 && r.json.decision.match_type === 'exact_phrase', JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
  check('SP広告KW: 完全一致＋フレーズ一致 の語は 完全一致 と フレーズ一致 の両方のブロックに載る',
    r.status === 200 && r.json.export.body.blocks.map((b) => `${b.match_type}:${b.text.replace(/\n/g, '/')}`).join('|') === 'exact:ハッカ油 スプレー/ハッカ油 虫除け|phrase:ハッカ油 虫除け',
    JSON.stringify(r.json.export?.body).slice(0, 300));
  r = await call('GET', P(idOwn));
  check('SP広告KW: 採用数は語の数 (両方に載せても 2 回と数えない)', r.json.state.adopted_count === 2, String(r.json.state.adopted_count));
  r = await call('POST', `${P(idOwn)}/candidates/${cMushi.id}/decisions`, { decision: 'adopt', match_type: 'exact' });   // 以降の検査の前提 (完全一致 2 語) に戻す

  // ─── PR2-C: 競合 ASIN (商品ターゲット) を人が入れる → 採否 → コピー本文の別ブロック ───
  r = await call('POST', `${P(idOwn)}/requests/${rid}/asins`, { asins: '' });
  check('SP広告KW/ASIN: 空は 400', r.status === 400 && r.json.code === 'bad_asin', JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests/${rid}/asins`, { asins: 'b0compet01, https://www.amazon.co.jp/dp/B0COMPET02/ref=x B0ADKWOWN1 not-asin B0COMPET01' });
  check('SP広告KW/ASIN: 大文字化・URL から抽出・自分の ASIN と重複と形式違いを弾く',
    r.status === 200 && JSON.stringify(r.json.added) === '["B0COMPET01","B0COMPET02"]'
    && r.json.skipped.some((s) => s.asin === 'B0ADKWOWN1' && /自分/.test(s.reason)) && JSON.stringify(r.json.invalid) === '["not-asin"]', JSON.stringify(r.json).slice(0, 300));
  check('SP広告KW/ASIN: 状態に asins が載り、検索語の候補とは別 (candidates に混ざらない)',
    r.json.state.asins.length === 2 && r.json.state.asins.every((a) => a.decision === null && a.added_by === 'smoke@b-faith.biz')
    && !r.json.state.candidates.some((c) => c.value === 'B0COMPET01'), JSON.stringify(r.json.state.asins));
  check('SP広告KW/ASIN: 材料は source=input・候補は kind=asin origin=input',
    db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'input'`).get(rid).n === 2
    && db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_candidates WHERE request_id = ? AND kind = 'asin' AND origin = 'input'`).get(rid).n === 2);
  r = await call('POST', `${P(idOwn)}/requests/${rid}/asins`, { asins: ['B0COMPET03', 'B0COMPET04', 'B0COMPET05', 'B0COMPET06'] });
  check('SP広告KW/ASIN: 6 件目も入る (上限は 20 件。2026-09-23 に 5 → 20)', r.json.added.length === 4 && r.json.skipped.length === 0 && r.json.state.limits.max_asins === 20, JSON.stringify(r.json).slice(0, 300));
  check('SP広告KW/ASIN: 種の表に ASIN は混ざらない (input は種ではない)', !r.json.state.seeds.some((s) => /^B0COMPET/.test(s.seed)));
  const aC1 = r.json.state.asins.find((a) => a.asin === 'B0COMPET01');
  r = await call('POST', `${P(idOwn)}/candidates/${aC1.id}/decisions`, { decision: 'adopt', match_type: 'exact' });
  check('SP広告KW/ASIN: 商品ターゲットにマッチタイプは付けられない (400)', r.status === 400 && r.json.code === 'bad_match_type', JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/candidates/${aC1.id}/decisions`, { decision: 'adopt' });
  check('SP広告KW/ASIN: マッチタイプ無しで採用できる (keyword = ASIN・match_type = null)',
    r.status === 200 && r.json.decision.keyword === 'B0COMPET01' && r.json.decision.match_type === null, JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
  {
    const blocks = r.json.export.body.blocks;
    const pt = blocks.find((b) => b.match_type === 'product_targets');
    check('SP広告KW/ASIN: 本文に「商品ターゲット」のブロックが別に出る (キーワードのブロックに混ざらない)',
      r.status === 200 && r.json.export.kind === 'ad_copy' && pt && pt.text === 'B0COMPET01' && pt.count === 1
      && !blocks.filter((b) => b.match_type !== 'product_targets').some((b) => /B0COMPETIT/.test(b.text))
      && r.json.export.body.target_total === 1 && r.json.export.body.keyword_total === 2, JSON.stringify(r.json.export.body));
    r = await call('POST', `${P(idOwn)}/exports/${r.json.export.id}/copied`, { match_type: 'product_targets' });
    check('SP広告KW/ASIN: 商品ターゲットのブロックにコピーの印が付く', r.status === 200 && typeof r.json.copied.product_targets === 'string', JSON.stringify(r.json));
  }
  r = await call('GET', P(idOwn));
  check('SP広告KW/ASIN: 状態に採用した商品ターゲットの数が別に載る', r.json.state.adopted_asin_count === 1 && r.json.state.adopted_count === 2, JSON.stringify([r.json.state.adopted_asin_count, r.json.state.adopted_count]));

  // 商品情報が変わったら「旧情報に基づく」
  db.prepare(`UPDATE product_drafts SET name = 'ハッカ油スプレー 100ml' WHERE id = ?`).run(idOwn);
  r = await call('GET', P(idOwn));
  check('SP広告KW: 依頼のあとで商品名が変わると stale=true (旧情報に基づく候補と出す)', r.json.state.stale === true && r.json.state.request.snapshot.name === 'ハッカ油スプレー');

  // 詳細画面: 自社商品ならタブと状態 JSON が載る (中身は JS が描く)
  {
    const pg = await getHtml(`/detail/${idOwn}`);
    const m = pg.html.match(/<script type="application\/json" id="adkw-json">([\s\S]*?)<\/script>/);
    const embedded = m ? JSON.parse(m[1]) : null;
    check('SP広告KW: 自社商品の詳細画面にタブと状態 JSON が載る',
      pg.status === 200 && pg.html.includes('data-tab="tab-adkw"') && embedded && embedded.draftId === idOwn
      && embedded.state.adopted_count === 2 && embedded.state.seeds.length >= 3, `${pg.status} ${m ? m[1].slice(0, 200) : pg.html.slice(0, 300)}`);
    check('SP広告KW: 画面の注意書き = Amazon に登録はされない・コピー済み≠登録済み・実績は未取得',
      /Amazon に登録はされません/.test(pg.html) && /「コピー済み」は「Amazon 登録済み」ではありません/.test(pg.html) && /ACOS は未取得/.test(pg.html));
    check('SP広告KW/ABA: 画面の文言 (§4.8) = クリック上位 3 商品に含まれた検索語・対象週・該当なし ≠ 注文なし・全注文語でも広告成果でもない',
      /クリック上位 3 商品に含まれた検索語/.test(pg.html) && /対象週/.test(pg.html) && /該当なし ≠ 注文なし/.test(pg.html) && /全注文語でも広告成果でも/.test(pg.html));
    // 描画後の JS が構文として通る (タブの JS は状態 JSON から DOM を組む。文字列を innerHTML に流さない)
    const vmMod = await import('node:vm');
    const js = checkInlineScriptSyntax(vmMod, pg.html, 'detail(ad-keywords)');
    const tabScript = (pg.html.match(/<script>\s*\/\/ SP広告KW タブ[\s\S]*?<\/script>/) || [''])[0];
    check('SP広告KW: 詳細画面の JS が構文として通り、タブの JS は innerHTML を使わない',
      js.ok && tabScript.includes('initAdKeywords') && !/\.innerHTML\b/.test(tabScript), js.detail + ` / タブ script ${tabScript.length} 文字`);
  }

  // ─── PR2-B2: 競合 ASIN の ABA 検索語を miniPC の aba.db (取込済みの週) から引く。走査しない・Render から Amazon を呼ばない ───
  //   守りたいこと: 該当なし ≠ 注文なし (証明の無い「該当なし」を出さない)・失敗と 0 件と「判定できない」を混ぜない・材料は取得回ごと・
  //   候補は先勝ちで観測を足す・取得済みは miniPC を呼ばない・取り直しは行を足す (前の材料と採否は残る)・閉じた依頼には保存しない
  const A = (cid) => `${P(idOwn)}/asins/${cid}/aba`;
  {
    abaCalls = [];
    abaImpl = async (body) => {
      abaCalls.push(body);
      return abaResult(body.asins[0], { status: 'found', coverage: 'complete', terms: [
        abaTerm('ハッカ油 スプレー', 1200, 2, 0.21, 0.15), abaTerm('はっか油 虫除け 最強', 8800, 1, 0.33, 0.30),
        abaTerm('  ハッカ油 スプレー ', 1200, 2, 0.21, 0.15), abaTerm('ハッカ油 スプレー 作り方', 25000, 3, 0.05, 0.02),
      ] });
    };
    r = await call('GET', P(idOwn));
    const asinsBefore = r.json.state.asins;
    check('SP広告KW/ABA: 引く前は asins[].aba が null・取得 0 回', asinsBefore.length === 6 && asinsBefore.every((a) => a.aba === null && a.aba_fetch_count === 0), JSON.stringify(asinsBefore).slice(0, 300));
    const aC2 = asinsBefore.find((a) => a.asin === 'B0COMPET02');
    const kwBefore = r.json.state.candidates.length;
    r = await call('POST', A(aC2.id), {});
    check('SP広告KW/ABA: 引くと found・対象週・語数が返り、miniPC には ASIN 1 つ・register=true で頼む',
      r.status === 200 && r.json.looked_up === true && r.json.reused === false && r.json.aba.status === 'found' && r.json.aba.week_start === '2026-09-13' && r.json.aba.term_count === 4
      && abaCalls.length === 1 && JSON.stringify(abaCalls[0].asins) === '["B0COMPET02"]' && abaCalls[0].register === true, JSON.stringify(r.json).slice(0, 400));
    check('SP広告KW/ABA: 候補 = 既出 1 語 (ハッカ油 スプレー = サジェストで先に出た) に観測を足し、新規 2 語 (空白違いの重複は 1 つ)',
      r.json.added === 2 && r.json.merged === 1 && r.json.state.candidates.length === kwBefore + 2, JSON.stringify([r.json.added, r.json.merged, kwBefore, r.json.state.candidates.length]));
    {
      const st = r.json.state;
      const spray = st.candidates.find((c) => c.value === 'ハッカ油 スプレー');
      const strongest = st.candidates.find((c) => c.value === 'はっか油 虫除け 最強');
      const howto = st.candidates.find((c) => c.value === 'ハッカ油 スプレー 作り方');
      check('SP広告KW/ABA: 先勝ち = サジェストで先に出た語は種の側に残り (seed は変わらない)、ABA の観測 (順位・位置・シェア・週) が足される',
        !!spray && spray.seed !== 'B0COMPET02' && spray.first_source !== 'aba' && spray.observed_count === spray.observed.length && spray.observed.length >= 2
        && spray.observed.some((o) => o.source === 'aba' && o.seed === 'B0COMPET02' && o.rank === 1200 && o.click_position === 2 && o.click_share === 0.21 && o.week_start === '2026-09-13' && o.source_label === 'ABA' && /9月13日〜9月19日/.test(o.week_text)),
        JSON.stringify(spray && spray.observed));
      check('SP広告KW/ABA: ABA で初めて出た語は seed=ASIN・first_source=aba・origin=observed で、並びは検索頻度順位 (小さい順)',
        !!strongest && !!howto && strongest.seed === 'B0COMPET02' && strongest.first_source === 'aba' && strongest.origin === 'observed'
        && st.candidates.indexOf(strongest) < st.candidates.indexOf(howto), JSON.stringify(st.candidates.map((c) => [c.value, c.seed])));
      const a2 = st.asins.find((a) => a.asin === 'B0COMPET02');
      check('SP広告KW/ABA: asins[].aba に 状態・対象週・§4.8 の文 (クリック上位 3 商品に含まれた検索語・対象週) が載る',
        !!a2.aba && a2.aba.status === 'found' && a2.aba.status_ja === '該当あり' && a2.aba.coverage === 'complete' && a2.aba.week_text === '9月13日〜9月19日'
        && /競合 ASIN B0COMPET02 がクリック上位 3 商品に含まれた検索語・対象週 9月13日〜9月19日/.test(a2.aba.observed_text) && a2.aba_fetch_count === 1 && a2.aba_candidate_count === 3,
        JSON.stringify(a2));
      check('SP広告KW/ABA: 材料は source=aba・seed=ASIN・status=success・coverage に aba_status/週/証明・raw に語がそのまま',
        (() => {
          const ev = db.prepare(`SELECT * FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = 'B0COMPET02'`).all(rid);
          const c = ev.length === 1 ? JSON.parse(ev[0].coverage_json) : {};
          return ev.length === 1 && ev[0].status === 'success' && c.aba_status === 'found' && c.week_start === '2026-09-13' && c.proof === 'week_ingested' && c.term_count === 4 && JSON.parse(ev[0].raw_json).length === 4;
        })());
      check('SP広告KW/ABA: 指標は原値のまま (換算しない)・成果スコアの項目が無い',
        !!strongest && !/score|volume|ボリューム/.test(JSON.stringify(strongest.observed)) && strongest.observed[0].conversion_share === 0.30);
    }
    abaCalls = [];
    r = await call('POST', A(aC2.id), {});
    check('SP広告KW/ABA: 取得済みの ASIN は miniPC を呼ばず reused', r.status === 200 && r.json.reused === true && r.json.added === 0 && abaCalls.length === 0, JSON.stringify(r.json).slice(0, 200));
    // 取り直し = 新しい週で行を足す (前の材料・候補は残る)。同じ語の同じ週の観測は二重にしない
    abaImpl = async (body) => { abaCalls.push(body); return abaResult(body.asins[0], { status: 'found', coverage: 'complete', terms: [abaTerm('はっか油 虫除け 最強', 7000, 1, 0.35, 0.31), abaTerm('ハッカ油 ゴキブリ', 30000, 3, 0.04, 0.01)] }, abaWeek('2026-09-20', '2026-09-26')); };
    r = await call('POST', A(aC2.id), { retake: true });
    check('SP広告KW/ABA: 取り直しは取得回の行を足す (2 行)・新しい週で、既出の語には新しい週の観測が足され、新規 1 語',
      r.status === 200 && r.json.reused === false && r.json.added === 1 && r.json.merged === 1 && r.json.aba.week_start === '2026-09-20'
      && db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = 'B0COMPET02'`).get(rid).n === 2
      && r.json.state.asins.find((a) => a.asin === 'B0COMPET02').aba_fetch_count === 2, JSON.stringify(r.json).slice(0, 300));
    {
      const strongest = r.json.state.candidates.find((c) => c.value === 'はっか油 虫除け 最強');
      check('SP広告KW/ABA: 最初の観測 (seed・週) は取り直しで書き換わらない (観測が 2 つ: 9/13 と 9/20)',
        !!strongest && strongest.seed === 'B0COMPET02' && strongest.observed_count === 2 && strongest.observed[0].week_start === '2026-09-13' && strongest.observed[1].week_start === '2026-09-20', JSON.stringify(strongest && strongest.observed));
      check('SP広告KW/ABA: 前の取得回でだけ出た語も候補に残る', !!r.json.state.candidates.find((c) => c.value === 'ハッカ油 スプレー 作り方'));
    }
    // 該当なし (証明つき)・判定できない・レポート無し・失敗 を混ぜない
    const aC3 = asinsBefore.find((a) => a.asin === 'B0COMPET03');
    abaImpl = async (body) => abaResult(body.asins[0], { status: 'none', coverage: 'complete', terms: [] });
    r = await call('POST', A(aC3.id), {});
    check('SP広告KW/ABA: 該当なし (full かつ捨てた行 0 の週) = empty・証明 week_ingested・候補 0',
      r.status === 200 && r.json.looked_up === true && r.json.aba.status === 'none' && r.json.aba.proof === 'week_ingested' && r.json.added === 0
      && db.prepare(`SELECT status FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = 'B0COMPET03'`).get(rid).status === 'empty'
      && /該当なし/.test(r.json.aba.status_ja), JSON.stringify(r.json.aba));
    const aC4 = asinsBefore.find((a) => a.asin === 'B0COMPET04');
    abaImpl = async (body) => abaResult(body.asins[0], { status: 'not_covered', coverage: 'unknown', proof: null, reason: 'incomplete_ingest', terms: [] }, abaWeek('2026-09-13', '2026-09-19', { skipped_count: 12 }));
    r = await call('POST', A(aC4.id), {});
    check('SP広告KW/ABA: 判定できない (取込が不完全) は「該当なし」にしない (partial・理由つき)',
      r.status === 200 && r.json.aba.status === 'not_covered' && /判定できない/.test(r.json.aba.status_ja) && /捨てた行/.test(r.json.aba.reason_ja)
      && db.prepare(`SELECT status FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = 'B0COMPET04'`).get(rid).status === 'partial', JSON.stringify(r.json.aba));
    abaCalls = [];
    r = await call('POST', A(aC4.id), {});
    check('SP広告KW/ABA: 判定できない は取得済み扱い (retake 無しでは呼び直さない)', r.json.reused === true && abaCalls.length === 0);
    const aC5 = asinsBefore.find((a) => a.asin === 'B0COMPET05');
    abaImpl = async (body) => abaResult(body.asins[0], { status: 'no_week', coverage: 'unknown', proof: null, reason: 'no_ingested_week', terms: [] }, null);
    r = await call('POST', A(aC5.id), {});
    check('SP広告KW/ABA: レポート無し (取込済みの週が無い) は failed 扱いで「もう一度」できる (状態は no_week)',
      r.status === 200 && r.json.looked_up === true && r.json.aba.status === 'no_week' && r.json.aba.week_start === null && /レポート無し/.test(r.json.aba.status_ja)
      && db.prepare(`SELECT status FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = 'B0COMPET05'`).get(rid).status === 'failed', JSON.stringify(r.json.aba));
    abaImpl = async () => { throw new Error('ECONNREFUSED'); };
    r = await call('POST', A(aC5.id), {});
    check('SP広告KW/ABA: miniPC が落ちていれば「取れなかった」を記録して返す (200・looked_up=false・理由つき・呼び直せる)',
      r.status === 200 && r.json.looked_up === false && /unreachable/.test(r.json.error) && r.json.aba.status === 'failed'
      && db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = 'B0COMPET05'`).get(rid).n === 2, JSON.stringify(r.json).slice(0, 300));
    abaImpl = async (body) => abaResult(body.asins[0], { status: 'none', coverage: 'partial', terms: [] });
    r = await call('POST', A(aC5.id), {});
    check('SP広告KW/ABA: 証明の無い「該当なし」(coverage≠complete) は受け取らず失敗として記録 (bad_response)',
      r.status === 200 && r.json.looked_up === false && /bad_response/.test(r.json.error), JSON.stringify(r.json).slice(0, 300));
    abaImpl = async (body) => abaResult(body.asins[0], { status: 'found', coverage: 'complete', terms: [abaTerm('ラベンダー スプレー', 500, 1, 0.5, 0.4)] });
    r = await call('POST', A(aC5.id), {});
    check('SP広告KW/ABA: 失敗のあとに引き直せる (取得回 4 行・いまの状態は found)',
      r.status === 200 && r.json.looked_up === true && r.json.reused === false && r.json.added === 1
      && r.json.state.asins.find((a) => a.asin === 'B0COMPET05').aba_fetch_count === 4 && r.json.state.asins.find((a) => a.asin === 'B0COMPET05').aba.status === 'found', JSON.stringify(r.json.aba));
    // 入口の守り
    r = await call('POST', A(cSpray.id), {});
    check('SP広告KW/ABA: 検索語の候補 (kind=kw) の id では引けない (404)', r.status === 404 && r.json.code === 'not_found', JSON.stringify(r.json));
    r = await call('POST', A(999999), {});
    check('SP広告KW/ABA: 無い候補は 404', r.status === 404);
    r = await call('POST', `${P(idOwn2)}/asins/${aC2.id}/aba`, {});
    check('SP広告KW/ABA: 別のドラフトからは引けない (404)', r.status === 404, JSON.stringify(r.json));
    // ABA の語も採用・コピーできる (キーワードのブロックに入る。商品ターゲットのブロックには混ざらない)
    r = await call('GET', P(idOwn));
    {
      const strongest = r.json.state.candidates.find((c) => c.value === 'はっか油 虫除け 最強');
      r = await call('POST', `${P(idOwn)}/candidates/${strongest.id}/decisions`, { decision: 'adopt', match_type: 'phrase' });
      check('SP広告KW/ABA: ABA で出た語も採用できる (マッチタイプつき)', r.status === 200 && r.json.decision.match_type === 'phrase', JSON.stringify(r.json));
      r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
      const ph = r.json.export.body.blocks.find((b) => b.match_type === 'phrase');
      const pt = r.json.export.body.blocks.find((b) => b.match_type === 'product_targets');
      check('SP広告KW/ABA: 本文のフレーズ一致に入る (商品ターゲットのブロックには混ざらない)',
        !!ph && /はっか油 虫除け 最強/.test(ph.text) && !!pt && !/はっか油/.test(pt.text), JSON.stringify(r.json.export.body));
      check('SP広告KW/ABA: 操作履歴に 引いた・失敗 が残る',
        ['ad_kw_aba_looked_up', 'ad_kw_aba_failed'].every((ev) => db.prepare('SELECT 1 FROM draft_events WHERE draft_id = ? AND event = ?').get(idOwn, ev)));
    }
    // 並行照会 (lease を取らない代わりの守り): 週A → 週B → 週A の順に保存が進んでも、同じ週の材料を二重に足さない (Codex R1 #1)
    {
      const adkw = await import('../lib/ad-keywords.js');
      const draftRow = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(idOwn);
      const aC1 = asinsBefore.find((a) => a.asin === 'B0COMPET01');
      const deferred = () => { let resolve; const p = new Promise((res) => { resolve = res; }); return { p, resolve }; };
      const d1 = deferred(), d2 = deferred(), d3 = deferred();
      const mk = (d, ws, we) => () => d.p.then(() => ({ ok: true, result: abaResult('B0COMPET01', { status: 'found', coverage: 'complete', terms: [abaTerm(`ハッカ油 ${ws}`, 100, 1, 0.1, 0.1)] }, abaWeek(ws, we)) }));
      const p1 = adkw.lookupAbaForAsin(db, draftRow, aC1.id, { actor: 'smoke', lookup: mk(d1, '2026-09-13', '2026-09-19') });
      const p2 = adkw.lookupAbaForAsin(db, draftRow, aC1.id, { actor: 'smoke', retake: true, lookup: mk(d2, '2026-09-20', '2026-09-26') });
      const p3 = adkw.lookupAbaForAsin(db, draftRow, aC1.id, { actor: 'smoke', retake: true, lookup: mk(d3, '2026-09-13', '2026-09-19') });
      d1.resolve(); const r1 = await p1;
      d2.resolve(); const r2 = await p2;
      d3.resolve(); const r3 = await p3;
      check('SP広告KW/ABA: 並行照会が 週A → 週B → 週A の順に保存へ進んでも、同じ週 (A) の材料は二重に足さない (3 つ目は最初の A を再利用)',
        r1.ok && r1.reused === false && r2.ok && r2.reused === false && r3.ok && r3.reused === true && r3.evidence.id === r1.evidence.id
        && db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = 'B0COMPET01'`).get(rid).n === 2,
        JSON.stringify([r1.reused, r2.reused, r3.reused, r3.evidence && r3.evidence.id, r1.evidence && r1.evidence.id]));
      r = await call('GET', P(idOwn));
      const a1 = r.json.state.asins.find((a) => a.asin === 'B0COMPET01');
      check('SP広告KW/ABA: 画面の最新週は 週B のまま (A に巻き戻らない)・取得 2 回', a1.aba.week_start === '2026-09-20' && a1.aba_fetch_count === 2, JSON.stringify(a1.aba));
    }
  }

  // 取消: 以後の収集・採否・固定は 409。収集の途中で取り消された結果は保存しない
  r = await call('POST', `${P(idOwn)}/requests/${rid}/cancel`);
  check('SP広告KW: 依頼を取り消せる', r.status === 200, JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests/${rid}/collect`, { seed: 'ゼラニウム' });
  check('SP広告KW: 取り消した依頼では集められない (409 closed)', r.status === 409 && r.json.code === 'closed');
  r = await call('POST', `${P(idOwn)}/candidates/${cSpray.id}/decisions`, { decision: 'reject' });
  check('SP広告KW: 取り消した依頼の採否は変えられない (409 closed)', r.status === 409 && r.json.code === 'closed');
  r = await call('POST', `${P(idOwn)}/requests/${rid}/exports`);
  check('SP広告KW: 取り消した依頼の本文は固定できない (409 closed)', r.status === 409 && r.json.code === 'closed');
  r = await call('POST', `${P(idOwn)}/requests/${rid}/cancel`);
  check('SP広告KW: 二度目の取消は 409', r.status === 409);
  r = await call('GET', P(idOwn));
  check('SP広告KW: 取り消すと画面の状態は「依頼なし」に戻る (候補・採否は DB に残る)',
    r.json.state.request === null && db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_candidates WHERE request_id = ?').get(rid).n > 0);

  r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k1' });
  check('SP広告KW: 取り消した依頼のキーを再送しても、閉じた依頼を返さない (409 closed。画面が同じキーで詰まらない)',
    r.status === 409 && r.json.code === 'closed', JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k3' });
  const rid3 = r.json.request_id;
  check('SP広告KW: 取消のあとは新しいキーで新しい依頼を作れる', r.status === 200 && rid3 !== rid && r.json.reused === false);
  {
    let cancelled = null;
    fetcherImpl = async (body) => {
      cancelled = await call('POST', `${P(idOwn)}/requests/${rid3}/cancel`);   // 収集の途中で人が取り消す
      return { ...suggestResult(body.seed), suggestions: [{ keyword: 'レモン 香り', source: 'base' }] };
    };
    r = await call('POST', `${P(idOwn)}/requests/${rid3}/collect`, { seed: 'レモン' });
    check('SP広告KW: 収集の途中で取り消された結果は保存しない (409 cancelled・材料なし)',
      r.status === 409 && r.json.code === 'cancelled' && cancelled && cancelled.status === 200
      && !db.prepare(`SELECT 1 FROM ph_ad_kw_evidence WHERE request_id = ?`).get(rid3)
      && db.prepare('SELECT status FROM ph_ad_kw_requests WHERE id = ?').get(rid3).status === 'cancelled', JSON.stringify(r.json));
  }
  {
    // ABA も同じ: 引いている途中で取り消された結果は保存しない
    r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k3b' });
    const rid3b = r.json.request_id;
    r = await call('POST', `${P(idOwn)}/requests/${rid3b}/asins`, { asins: 'B0COMPET07' });
    const c7 = r.json.state.asins[0];
    let cancelled = null;
    abaImpl = async (body) => {
      cancelled = await call('POST', `${P(idOwn)}/requests/${rid3b}/cancel`);
      return abaResult(body.asins[0], { status: 'found', coverage: 'complete', terms: [abaTerm('レモン スプレー', 10, 1, 0.1, 0.1)] });
    };
    r = await call('POST', A(c7.id), {});
    check('SP広告KW/ABA: 引いている途中で取り消された結果は保存しない (409 cancelled・材料なし)',
      r.status === 409 && r.json.code === 'cancelled' && cancelled && cancelled.status === 200
      && !db.prepare(`SELECT 1 FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba'`).get(rid3b), JSON.stringify(r.json));
    r = await call('POST', A(c7.id), {});
    check('SP広告KW/ABA: 閉じた依頼の ASIN では引けない (409 closed)', r.status === 409 && r.json.code === 'closed', JSON.stringify(r.json));
  }

  // 置き換え (restart): 以前の依頼は superseded。採否は消えないが変えられない
  fetcherImpl = async (body) => (resultWith(body.seed, { success: 1, empty: 46, suggestions: [{ keyword: `${body.seed} 精油`, source: 'base' }] }));
  r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k4' });
  const rid4 = r.json.request_id;
  r = await call('POST', `${P(idOwn)}/requests/${rid4}/collect`, { seed: 'ティーツリー' });
  const c4 = r.json.state.candidates[0];
  r = await call('POST', `${P(idOwn)}/candidates/${c4.id}/decisions`, { decision: 'adopt', match_type: 'broad' });
  r = await call('POST', `${P(idOwn)}/requests`, { idempotency_key: 'k5', restart: true });
  const rid5 = r.json.request_id;
  check('SP広告KW: restart で新しい依頼になり、前の依頼は superseded (採否の行はそのまま)',
    rid5 !== rid4 && r.json.reused === false
    && db.prepare('SELECT status, supersedes_request_id FROM ph_ad_kw_requests WHERE id = ?').get(rid5).supersedes_request_id === rid4
    && db.prepare('SELECT status FROM ph_ad_kw_requests WHERE id = ?').get(rid4).status === 'superseded'
    && db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_decisions WHERE request_id = ?').get(rid4).n === 1, JSON.stringify(r.json));
  r = await call('POST', `${P(idOwn)}/candidates/${c4.id}/decisions`, { decision: 'reject' });
  check('SP広告KW: 置き換えられた依頼の採否は変えられない (上書きしない)', r.status === 409 && r.json.code === 'closed');
  r = await call('GET', P(idOwn));
  check('SP広告KW: 新しい依頼は空から始まる (前の候補は持ち越さない)', r.json.state.request.id === rid5 && r.json.state.candidates.length === 0);

  // ─── 競合 ASIN を ABA から自動で出す (2026-09-23)。採用した語ごとのクリック上位 3 → 多く出た順に「採用 (自動)」→ 人が却下 ───
  {
    db.prepare(`UPDATE product_drafts SET asin = 'B0AUTOOWN1' WHERE id = ?`).run(idOwn2);
    r = await call('POST', `${P(idOwn2)}/requests`, { idempotency_key: 'k-auto' });
    const ridA = r.json.request_id;
    const AU = `${P(idOwn2)}/requests/${ridA}/auto-asins`;
    abaCalls = [];
    r = await call('POST', AU, {});
    check('SP広告KW/自動: 採用した検索 KW が無ければ 400 (miniPC を呼ばない)', r.status === 400 && r.json.code === 'no_adopted' && abaCalls.length === 0, JSON.stringify(r.json));
    fetcherImpl = async (body) => { fetcherCalls.push(body); return suggestResult(body.seed); };
    r = await call('POST', `${P(idOwn2)}/requests/${ridA}/collect`, { seed: 'ラベンダー' });
    const cs = r.json.state.candidates;
    const kSpray = cs.find((c) => c.value === 'ラベンダー スプレー'), kMushi = cs.find((c) => c.value === 'ラベンダー 虫除け');
    await call('POST', `${P(idOwn2)}/candidates/${kSpray.id}/decisions`, { decision: 'adopt', match_type: 'exact_phrase' });
    await call('POST', `${P(idOwn2)}/candidates/${kMushi.id}/decisions`, { decision: 'adopt', match_type: 'exact', keyword: 'ラベンダー 虫よけ' });

    // miniPC /aba/terms の応答 (語ごと・部門ごと)
    const tWeek = abaWeek('2026-09-13', '2026-09-19');
    const hit = (asin, pos, cs2) => ({ asin, click_position: pos, product_title: null, click_share: cs2, conversion_share: cs2 / 2 });
    const termsResult = (terms, byTerm, week = tWeek, wc = 'complete') => ({
      week, requested_week: null, week_coverage: wc, invalid: [],
      items: terms.map((t) => byTerm[t] ? { term: t, matched_term: t, variants: [t], status: 'found', coverage: wc, reason: null, departments: byTerm[t] }
        : { term: t, matched_term: null, variants: [t], status: 'none', coverage: 'complete', reason: null, departments: [] }),
    });
    let termsImpl = (terms) => termsResult(terms, {
      'ラベンダー スプレー': [{ department: 'amazon.co.jp', search_frequency_rank: 800, asins: [hit('B0AUTOAAA1', 1, 0.3), hit('B0AUTOOWN1', 2, 0.2), hit('B0AUTOBBB2', 3, 0.05)] }],
      'ラベンダー 虫よけ': [{ department: 'amazon.co.jp', search_frequency_rank: 2000, asins: [hit('B0AUTOAAA1', 2, 0.2), hit('B0AUTOCCC3', 3, 0.1)] }],
    });
    abaImpl = async (body, path) => { abaCalls.push({ body, path }); if (path !== '/terms') throw new Error('unexpected path ' + path); return termsImpl(body.terms); };
    r = await call('POST', AU, {});
    check('SP広告KW/自動: 採用した語 (直した語) を miniPC /aba/terms に送る', r.status === 200 && abaCalls.length === 1 && abaCalls[0].path === '/terms'
      && JSON.stringify(abaCalls[0].body.terms) === '["ラベンダー スプレー","ラベンダー 虫よけ"]', JSON.stringify(abaCalls));
    check('SP広告KW/自動: 多く出た順 (語の数 → 良い順位 → シェア) に入り、自分の ASIN は除く',
      JSON.stringify(r.json.added) === '["B0AUTOAAA1","B0AUTOCCC3","B0AUTOBBB2"]' && r.json.skipped.some((s) => s.asin === 'B0AUTOOWN1' && /自分/.test(s.reason)), JSON.stringify(r.json).slice(0, 400));
    const stA = r.json.state;
    const aA = stA.asins.find((a) => a.asin === 'B0AUTOAAA1');
    check('SP広告KW/自動: 最初から「採用」(記録者 = auto:aba)・候補は origin=observed',
      stA.asins.every((a) => a.auto && a.decision && a.decision.decision === 'adopt' && a.decision.actor === 'auto:aba' && a.origin === 'observed') && stA.adopted_asin_count === 3,
      JSON.stringify(stA.asins.map((a) => [a.asin, a.decision && a.decision.actor])));
    check('SP広告KW/自動: どの語で何位かを添える (原値)',
      aA.auto_term_count === 2 && JSON.stringify(aA.auto_hits.map((h) => [h.term, h.click_position, h.click_share])) === '[["ラベンダー スプレー",1,0.3],["ラベンダー 虫よけ",2,0.2]]'
      && aA.auto_week_text === '9月13日〜9月19日' && aA.added_by === null, JSON.stringify(aA));
    check('SP広告KW/自動: 前回の要約 (語 2 → 該当あり 2・出てきた商品 4 → 採用 3)',
      stA.auto_asins && stA.auto_asins.status === 'success' && stA.auto_asins.terms_sent === 2 && stA.auto_asins.found === 2 && stA.auto_asins.asins_seen === 4 && stA.auto_asins.added === 3,
      JSON.stringify(stA.auto_asins));
    check('SP広告KW/自動: 材料は source=aba・seed=*top_asins* (ASIN の ABA 検索語とは別)・操作履歴に残る',
      db.prepare(`SELECT COUNT(*) AS n FROM ph_ad_kw_evidence WHERE request_id = ? AND source = 'aba' AND seed = '*top_asins*'`).get(ridA).n === 1
      && stA.asins.every((a) => a.aba === null)
      && !!db.prepare(`SELECT 1 FROM draft_events WHERE draft_id = ? AND event = 'ad_kw_auto_asins'`).get(idOwn2));
    r = await call('POST', AU, {});
    check('SP広告KW/自動: もう一度押しても同じ ASIN は二重に入らない (入力済み)', r.json.added.length === 0 && r.json.skipped.filter((s) => s.reason === '入力済み').length === 3
      && r.json.state.asins.length === 3, JSON.stringify(r.json).slice(0, 300));
    // 人が却下 → 本文の商品ターゲットから外れる
    const aB = r.json.state.asins.find((a) => a.asin === 'B0AUTOBBB2');
    r = await call('POST', `${P(idOwn2)}/candidates/${aB.id}/decisions`, { decision: 'reject' });
    check('SP広告KW/自動: 自動で入った ASIN を人が却下できる', r.status === 200 && r.json.decision.decision === 'reject' && r.json.decision.actor === 'smoke@b-faith.biz');
    r = await call('POST', `${P(idOwn2)}/requests/${ridA}/exports`);
    {
      const pt = r.json.export.body.blocks.find((b) => b.match_type === 'product_targets');
      check('SP広告KW/自動: 本文の商品ターゲット = 採用 (自動) のうち却下していないもの', pt && pt.text === 'B0AUTOAAA1\nB0AUTOCCC3', JSON.stringify(r.json.export.body).slice(0, 300));
    }
    // 失敗の記録 (0 件と混ぜない)
    abaImpl = async () => { throw new Error('ECONNREFUSED'); };
    r = await call('POST', AU, {});
    check('SP広告KW/自動: miniPC に届かなければ looked_up=false・材料は failed・前回の要約も failed',
      r.status === 200 && r.json.looked_up === false && /unreachable|ECONNREFUSED/.test(r.json.error) && r.json.state.auto_asins.status === 'failed' && r.json.state.asins.length === 3, JSON.stringify(r.json).slice(0, 300));
    abaImpl = async (body) => ({ week: null, requested_week: null, week_coverage: 'unknown', invalid: [],
      items: body.terms.map((t) => ({ term: t, matched_term: null, variants: [t], status: 'no_week', coverage: 'unknown', reason: 'no_ingested_week', departments: [] })) });
    r = await call('POST', AU, {});
    check('SP広告KW/自動: 取込済みの週が無ければ failed (no_week)・何も入れない', r.json.looked_up === true && r.json.added.length === 0 && r.json.state.auto_asins.status === 'failed'
      && /no_week/.test(r.json.state.auto_asins.error || ''), JSON.stringify(r.json.state.auto_asins));
    abaImpl = async (body) => termsResult(body.terms, {}, tWeek, 'partial');
    r = await call('POST', AU, {});
    check('SP広告KW/自動: 証明の無い「該当なし」(none なのに週が partial) は受け取らない (bad_response)', r.json.looked_up === false && /bad_response/.test(r.json.error), JSON.stringify(r.json).slice(0, 300));
    abaImpl = async (body) => termsResult(body.terms, { [body.terms[0]]: [{ department: 'x', search_frequency_rank: 1, asins: [hit('NOT-AN-ASIN', 1, 0.1)] }] });
    r = await call('POST', AU, {});
    check('SP広告KW/自動: ASIN の形でない値は受け取らない (bad_response)', r.json.looked_up === false && /bad_response/.test(r.json.error), JSON.stringify(r.json).slice(0, 300));
    // 上限 20 件: 手入力で 19 件にしてから自動 → 1 件だけ入り、残りは「20 件まで」
    const fill = Array.from({ length: 16 }, (_, i) => 'B0FILL' + String(i).padStart(4, '0'));
    r = await call('POST', `${P(idOwn2)}/requests/${ridA}/asins`, { asins: fill });
    check('SP広告KW/ASIN: 手入力は 20 件まで入る', r.json.added.length === 16 && r.json.state.asins.length === 19, JSON.stringify(r.json).slice(0, 200));
    abaImpl = async (body) => termsResult(body.terms, { [body.terms[0]]: [{ department: 'amazon.co.jp', search_frequency_rank: 5, asins: [hit('B0AUTODDD4', 1, 0.5), hit('B0AUTOEEE5', 2, 0.3)] }] });
    r = await call('POST', AU, {});
    check('SP広告KW/自動: 1 依頼 20 件で止まる (入りきらない分は skip・理由つき)',
      JSON.stringify(r.json.added) === '["B0AUTODDD4"]' && r.json.skipped.some((s) => s.asin === 'B0AUTOEEE5' && /20 件/.test(s.reason)) && r.json.state.asins.length === 20, JSON.stringify(r.json).slice(0, 300));
    r = await call('POST', `${P(idOwn2)}/requests/${ridA}/asins`, { asins: 'B0FILLXXX1' });
    check('SP広告KW/ASIN: 20 件あれば手入力も skip', r.json.added.length === 0 && r.json.skipped.some((s) => /20 件/.test(s.reason)), JSON.stringify(r.json).slice(0, 200));
    // 閉じた依頼には保存しない
    r = await call('POST', `${P(idOwn2)}/requests/${ridA}/cancel`, {});
    abaCalls = [];
    r = await call('POST', AU, {});
    check('SP広告KW/自動: 閉じた依頼は 409 (miniPC を呼ばない)', r.status === 409 && r.json.code === 'closed' && abaCalls.length === 0, JSON.stringify(r.json));
    r = await call('POST', `${P(idOwn)}/requests/${ridA}/auto-asins`, {});
    check('SP広告KW/自動: 別のドラフトの依頼は 404', r.status === 404, JSON.stringify(r.json));
  }

  // 設定が無ければ集めない (採否・コピーはできる)
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  fetcherCalls = [];
  fetcherImpl = async (body) => { fetcherCalls.push(body); return suggestResult(body.seed); };
  r = await call('POST', `${P(idOwn)}/requests/${rid5}/collect`, { seed: 'ミント' });
  check('SP広告KW: WAREHOUSE_SERVICE_TOKEN が無ければ 503 で止まる (Render から代わりに叩かない・記録も残さない)',
    r.status === 503 && r.json.code === 'not_configured' && fetcherCalls.length === 0
    && !db.prepare('SELECT 1 FROM ph_ad_kw_evidence WHERE request_id = ?').get(rid5), JSON.stringify(r.json));
  r = await call('GET', P(idOwn));
  check('SP広告KW: 設定が無いことを状態で伝える (configured=false)', r.json.state.configured === false);
  abaCalls = [];
  abaImpl = async (body) => { abaCalls.push(body); return abaResult(body.asins[0], { status: 'none', coverage: 'complete', terms: [] }); };
  r = await call('POST', A(db.prepare(`SELECT id FROM ph_ad_kw_candidates WHERE kind = 'asin' AND value = 'B0COMPET02'`).get().id), {});
  check('SP広告KW/ABA: WAREHOUSE_SERVICE_TOKEN が無ければ 503 で止まる (記録も残さない)', r.status === 503 && r.json.code === 'not_configured' && abaCalls.length === 0, JSON.stringify(r.json));

  // 後始末
  if (savedToken === undefined) delete process.env.WAREHOUSE_SERVICE_TOKEN; else process.env.WAREHOUSE_SERVICE_TOKEN = savedToken;
  kwClient._setSuggestFetcher(null);
  abaClient._setAbaFetcher(null);
  server.close();
  db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?, ?)').run(idOwn, idOwn2, idOther);
  check('SP広告KW: ドラフトを消すと依頼・材料・候補・コピー履歴も消える (採否の行は監査として残る)',
    !db.prepare('SELECT 1 FROM ph_ad_kw_requests WHERE draft_id = ?').get(idOwn)
    && !db.prepare('SELECT 1 FROM ph_ad_kw_candidates WHERE request_id = ?').get(rid)
    && !db.prepare('SELECT 1 FROM ph_ad_kw_exports WHERE draft_id = ?').get(idOwn)
    && db.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_decisions WHERE request_id = ?').get(rid).n > 0);
}

// ─── SP広告KW PR2-C: PR1 の CHECK 制約を広げる作り直し (migrateAdKwCheckConstraints) ───
// 本番には PR1 (9/23 午前) の定義で行が入っているかもしれない。行と id を保ったまま作り直せること・二度目は何もしないことを、
// PR1 の DDL を写した別の DB で確かめる
{
  const Database = (await import('better-sqlite3')).default;
  const mpath = path.join(process.env.DATA_DIR, 'adkw-migration-test.db');
  try { fs.unlinkSync(mpath); } catch { /* 無ければ無視 */ }
  const mdb = new Database(mpath);
  mdb.pragma('foreign_keys = ON');
  mdb.exec(`
    CREATE TABLE product_drafts (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
    CREATE TABLE ph_ad_kw_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, draft_id INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      idempotency_key TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'review_ready', collecting_seed TEXT, collecting_token TEXT, collecting_since TEXT,
      product_snapshot_json TEXT NOT NULL, input_hash TEXT NOT NULL, supersedes_request_id INTEGER, requested_by TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
    CREATE TABLE ph_ad_kw_evidence (id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL REFERENCES ph_ad_kw_requests(id) ON DELETE CASCADE,
      source TEXT NOT NULL CHECK (source IN ('suggest')), seed TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('success', 'partial', 'empty', 'failed')),
      options_json TEXT NOT NULL DEFAULT '{}', coverage_json TEXT NOT NULL, raw_json TEXT NOT NULL, error TEXT, fetched_at TEXT, created_by TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
    CREATE INDEX idx_ph_ad_kw_evidence_seed ON ph_ad_kw_evidence(request_id, source, seed, id);
    CREATE TABLE ph_ad_kw_candidates (id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL REFERENCES ph_ad_kw_requests(id) ON DELETE CASCADE,
      kind TEXT NOT NULL CHECK (kind IN ('kw', 'negative', 'asin')), value TEXT NOT NULL, value_norm TEXT NOT NULL,
      origin TEXT NOT NULL CHECK (origin IN ('observed', 'ai')), evidence_id INTEGER NOT NULL REFERENCES ph_ad_kw_evidence(id),
      observed_json TEXT NOT NULL, observed_count INTEGER NOT NULL DEFAULT 1, sort_key TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
    CREATE UNIQUE INDEX uq_ph_ad_kw_candidates_value ON ph_ad_kw_candidates(request_id, kind, value_norm);
    CREATE TABLE ph_ad_kw_decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_id INTEGER NOT NULL, request_id INTEGER NOT NULL,
      decision TEXT NOT NULL, keyword TEXT, match_type TEXT, scope TEXT, supersedes_decision_id INTEGER, actor TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
    CREATE TABLE ph_ad_kw_exports (id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL REFERENCES ph_ad_kw_requests(id) ON DELETE CASCADE,
      draft_id INTEGER NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('search_keywords')), decision_version INTEGER NOT NULL,
      body_json TEXT NOT NULL, body_hash TEXT NOT NULL, copied_json TEXT NOT NULL DEFAULT '{}', created_by TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
    CREATE INDEX idx_ph_ad_kw_exports_request ON ph_ad_kw_exports(request_id, id);
    INSERT INTO product_drafts (id, name) VALUES (1, 'x');
    INSERT INTO ph_ad_kw_requests (id, draft_id, idempotency_key, product_snapshot_json, input_hash) VALUES (7, 1, 'k', '{}', 'h');
    INSERT INTO ph_ad_kw_evidence (id, request_id, source, seed, status, coverage_json, raw_json) VALUES (30, 7, 'suggest', 'ハッカ油', 'success', '{}', '[]');
    INSERT INTO ph_ad_kw_candidates (id, request_id, kind, value, value_norm, origin, evidence_id, observed_json, sort_key) VALUES (500, 7, 'kw', 'ハッカ油 スプレー', 'ハッカ油 スプレー', 'observed', 30, '[]', 's');
    INSERT INTO ph_ad_kw_decisions (id, candidate_id, request_id, decision, keyword, match_type) VALUES (9000, 500, 7, 'adopt', 'ハッカ油 スプレー', 'exact');
    INSERT INTO ph_ad_kw_exports (id, request_id, draft_id, kind, decision_version, body_json, body_hash) VALUES (42, 7, 1, 'search_keywords', 9000, '{}', 'hh');
    -- 消したドラフトの採否 (候補は CASCADE で消え、採否は監査として残る = 正常な孤立) と、消した行を含む採番の上限 (Codex #1413 R1 #1 #2)
    INSERT INTO ph_ad_kw_decisions (id, candidate_id, request_id, decision, keyword, match_type) VALUES (9001, 777, 8, 'adopt', '消した商品の語', 'exact');
    UPDATE sqlite_sequence SET seq = 900 WHERE name = 'ph_ad_kw_candidates';
    UPDATE sqlite_sequence SET seq = 88 WHERE name = 'ph_ad_kw_evidence';
  `);
  check('移行前: 正常な孤立採否が 1 件ある (前提の確認)', mdb.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_decisions d WHERE NOT EXISTS (SELECT 1 FROM ph_ad_kw_candidates c WHERE c.id = d.candidate_id)').get().n === 1);
  const rejects = (sql) => { try { mdb.exec(sql); return false; } catch (e) { return /CHECK/.test(e.message); } };
  check('移行前: PR1 の定義では source=input を受け付けない (前提の確認)', rejects(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json) VALUES (7, 'input', 'B0X', 'success', '{}', '[]')`));
  const m1 = dbmod.migrateAdKwCheckConstraints(mdb);
  check('移行: 3 表を作り直す', JSON.stringify(m1.migrated) === '["ph_ad_kw_evidence","ph_ad_kw_candidates","ph_ad_kw_exports"]', JSON.stringify(m1));
  check('移行: 行と id がそのまま (evidence 30 / candidate 500 / decision 9000 / export 42)',
    mdb.prepare('SELECT id FROM ph_ad_kw_evidence').get().id === 30 && mdb.prepare('SELECT id, evidence_id FROM ph_ad_kw_candidates').get().evidence_id === 30
    && mdb.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_candidates WHERE id = 500').get().n === 1
    && mdb.prepare('SELECT candidate_id FROM ph_ad_kw_decisions WHERE id = 9000').get().candidate_id === 500
    && mdb.prepare('SELECT kind FROM ph_ad_kw_exports WHERE id = 42').get().kind === 'search_keywords');
  check('移行後: source=input / origin=input / kind=ad_copy を受け付ける',
    !rejects(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json) VALUES (7, 'input', 'B0X', 'success', '{}', '[]')`)
    && !rejects(`INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, sort_key) VALUES (7, 'asin', 'B0X', 'B0X', 'input', 30, '[]', 'a')`)
    && !rejects(`INSERT INTO ph_ad_kw_exports (request_id, draft_id, kind, decision_version, body_json, body_hash) VALUES (7, 1, 'ad_copy', 1, '{}', 'x')`));
  check('移行後: 索引が作り直されている (UNIQUE が効く)',
    (() => { try { mdb.exec(`INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, sort_key) VALUES (7, 'asin', 'B0X', 'B0X', 'input', 30, '[]', 'a')`); return false; } catch (e) { return /UNIQUE/.test(e.message); } })()
    && mdb.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'index' AND name IN ('idx_ph_ad_kw_evidence_seed', 'uq_ph_ad_kw_candidates_value', 'idx_ph_ad_kw_exports_request')").get().n === 3);
  check('移行後: 外部キーは有効のまま・不整合なし', mdb.pragma('foreign_keys', { simple: true }) === 1 && mdb.pragma('foreign_key_check').length === 0);
  check('移行後: 採番が続く (新しい id が既存より大きい)', mdb.prepare('SELECT MAX(id) AS m FROM ph_ad_kw_candidates').get().m > 500);
  check('移行後: 消した行を含む採番の上限 (sqlite_sequence) が保たれ、過去の id を再利用しない (候補 900 超・材料 88 超)',
    mdb.prepare('SELECT MAX(id) AS m FROM ph_ad_kw_candidates').get().m > 900
    && (mdb.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'ph_ad_kw_evidence'").get()?.seq ?? 0) >= 88
    && mdb.prepare('SELECT MAX(id) AS m FROM ph_ad_kw_evidence').get().m > 88,
    JSON.stringify(mdb.prepare("SELECT name, seq FROM sqlite_sequence").all()));
  check('移行後: 正常な孤立採否 (消したドラフトの監査) はそのまま残り、移行を止めない', mdb.prepare('SELECT COUNT(*) AS n FROM ph_ad_kw_decisions WHERE id = 9001').get().n === 1);
  const m2 = dbmod.migrateAdKwCheckConstraints(mdb);
  check('移行: 二度目は何もしない (冪等)', JSON.stringify(m2.migrated) === '[]', JSON.stringify(m2));
  check('移行: 本番の init で作った (新しい定義の) DB でも何もしない', JSON.stringify(dbmod.migrateAdKwCheckConstraints(db).migrated) === '[]');
  mdb.close();
  try { fs.unlinkSync(mpath); } catch { /* 無ければ無視 */ }
}

// ─── 採否の match_type に exact_phrase を足す作り直し (2026-09-23)。本番の採否表 (PR1 の定義・append-only トリガーつき) を写して確かめる ───
// 🚨 トリガーは DROP TABLE で表と一緒に消える → 作り直しのあと付け直されていること
{
  const Database = (await import('better-sqlite3')).default;
  const mpath = path.join(process.env.DATA_DIR, 'adkw-migration-decisions-test.db');
  try { fs.unlinkSync(mpath); } catch { /* 無ければ無視 */ }
  const tdb = new Database(mpath);
  tdb.pragma('foreign_keys = ON');
  // 候補表は孤立の検証 (candidate_id の突き合わせ) にだけ使う。材料・コピー履歴の表は無い = 作り直しの対象外
  tdb.exec(`
    CREATE TABLE ph_ad_kw_candidates (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL);
    INSERT INTO ph_ad_kw_candidates (id, value) VALUES (500, 'ハッカ油 スプレー');
    CREATE TABLE ph_ad_kw_decisions (
      id                     INTEGER PRIMARY KEY AUTOINCREMENT,
      candidate_id           INTEGER NOT NULL,
      request_id             INTEGER NOT NULL,
      decision               TEXT NOT NULL CHECK (decision IN ('adopt', 'hold', 'reject', 'undecided')),
      keyword                TEXT,
      match_type             TEXT CHECK (match_type IN ('exact', 'phrase', 'broad')),
      scope                  TEXT,
      supersedes_decision_id INTEGER,
      actor                  TEXT,
      created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX idx_ph_ad_kw_decisions_candidate ON ph_ad_kw_decisions(candidate_id, id);
    CREATE INDEX idx_ph_ad_kw_decisions_request ON ph_ad_kw_decisions(request_id, id);
    INSERT INTO ph_ad_kw_decisions (id, candidate_id, request_id, decision, keyword, match_type, actor) VALUES (9000, 500, 7, 'adopt', 'ハッカ油 スプレー', 'exact', 'a@b');
    INSERT INTO ph_ad_kw_decisions (id, candidate_id, request_id, decision, keyword, match_type, supersedes_decision_id) VALUES (9005, 500, 7, 'adopt', 'ハッカ油 スプレー', 'phrase', 9000);
    UPDATE sqlite_sequence SET seq = 9100 WHERE name = 'ph_ad_kw_decisions';
    CREATE TRIGGER trg_ph_ad_kw_decisions_no_update BEFORE UPDATE ON ph_ad_kw_decisions BEGIN SELECT RAISE(ABORT, 'ph_ad_kw_decisions is append-only'); END;
    CREATE TRIGGER trg_ph_ad_kw_decisions_no_delete BEFORE DELETE ON ph_ad_kw_decisions BEGIN SELECT RAISE(ABORT, 'ph_ad_kw_decisions is append-only'); END;
  `);
  const rejects = (sql, re) => { try { tdb.exec(sql); return false; } catch (e) { return re.test(e.message); } };
  check('採否の移行前: PR1 の定義は exact_phrase を受け付けない (前提の確認)',
    rejects(`INSERT INTO ph_ad_kw_decisions (candidate_id, request_id, decision, keyword, match_type) VALUES (500, 7, 'adopt', 'x', 'exact_phrase')`, /CHECK/));
  const m1 = dbmod.migrateAdKwCheckConstraints(tdb);
  check('採否の移行: 採否表だけを作り直す', JSON.stringify(m1.migrated) === '["ph_ad_kw_decisions"]', JSON.stringify(m1));
  check('採否の移行: 行と id と訂正元がそのまま (9000 / 9005→9000)',
    JSON.stringify(tdb.prepare('SELECT id, match_type, supersedes_decision_id AS s, actor FROM ph_ad_kw_decisions ORDER BY id').all())
      === JSON.stringify([{ id: 9000, match_type: 'exact', s: null, actor: 'a@b' }, { id: 9005, match_type: 'phrase', s: 9000, actor: null }]));
  check('採否の移行後: exact_phrase を受け付け、知らない値は今も拒む',
    !rejects(`INSERT INTO ph_ad_kw_decisions (candidate_id, request_id, decision, keyword, match_type) VALUES (500, 7, 'adopt', 'x', 'exact_phrase')`, /CHECK/)
    && rejects(`INSERT INTO ph_ad_kw_decisions (candidate_id, request_id, decision, keyword, match_type) VALUES (500, 7, 'adopt', 'x', 'bogus')`, /CHECK/));
  check('採否の移行後: 採番の上限 (9100) を保つ (過去の id を再利用しない)', tdb.prepare('SELECT MAX(id) AS m FROM ph_ad_kw_decisions').get().m > 9100);
  check('採否の移行後: append-only のトリガーが付き直っている (UPDATE / DELETE を拒む)',
    rejects('UPDATE ph_ad_kw_decisions SET decision = \'reject\' WHERE id = 9000', /append-only/)
    && rejects('DELETE FROM ph_ad_kw_decisions WHERE id = 9000', /append-only/)
    && tdb.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'ph_ad_kw_decisions'").get().n === 2);
  check('採否の移行後: 索引が作り直されている', tdb.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'index' AND name IN ('idx_ph_ad_kw_decisions_candidate', 'idx_ph_ad_kw_decisions_request')").get().n === 2);
  check('採否の移行: 二度目は何もしない (冪等)', JSON.stringify(dbmod.migrateAdKwCheckConstraints(tdb).migrated) === '[]');
  tdb.close();
  try { fs.unlinkSync(mpath); } catch { /* 無ければ無視 */ }
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
  // 説明行が空 (AI 文が 1 つも無い) のときだけ productName がフォールバックで使われる。
  // ここに楽天タイトルを渡すと、外したはずの SEO 語がフォールバックで出てしまう
  {
    const fb = composeDescriptions({
      productName: 'NE商品名<X>', ai: {}, specs: [], pageInfo: null, cabinetLocations: [],
    });
    check('composeDescriptions: AI 文が空なら「商品名」行に NE 商品名が出る (楽天タイトルではない)',
      fb.pc.includes('<b>商品名</b>') && fb.pc.includes('NE商品名&lt;X&gt;')
      && !fb.pc.includes('<b>説明</b>'), fb.pc.slice(0, 220));
    check('composeDescriptions: フォールバックの商品名もエスケープされる',
      !fb.pc.includes('NE商品名<X>'), fb.pc.slice(0, 220));
  }
  const comp = composeDescriptions({
    productName: 'NE名', ai: { desc_features: 'F', desc_spec: 'S', desc_notes: 'N' },
    specs: [], pageInfo: null, cabinetLocations: ['/x/a.jpg'],
  });
  check('composeDescriptions: pc に特徴/仕様が入り sales に画像HTML・sp が連結',
    comp.pc.includes('F') && comp.pc.includes('S') && comp.sales.includes('/x/a.jpg')
    && comp.sp === comp.sales + '\n' + comp.pc, JSON.stringify(comp).slice(0, 200));

  // XSS境界 (Codexレビュー提案): 3欄は画面で innerHTML レンダリングされるため、
  // 素材にHTML/属性注入が混ざっても生成関数がすべてエスケープすることを固定する
  const evil = composeDescriptions({
    productName: '<script>alert(1)</script>',
    ai: { desc_features: '<img src=x onerror=alert(2)>', desc_notes: '</td><script>alert(3)</script>' },
    specs: [{ spec_key: '<b>鍵</b>', spec_value: '"onmouseover="alert(4)' }],
    pageInfo: null,
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
    statuses, statusLabels, maxImportCodes: imp.MAX_IMPORT_CODES,
    maxRegisterCodes: intake.MAX_REGISTER_CODES, intake: intake.intakeStatus(),
    isAdmin: true, shopCategoryCount: 12, maxShopCategoryLines: shopCat.MAX_SHOP_CATEGORY_LINES,
  }],
  ['index.ejs (empty)', 'index.ejs', {
    title: 't', displayName: 'smoke', drafts: [], counts, statusFilter: 'draft',
    statuses, statusLabels, maxImportCodes: imp.MAX_IMPORT_CODES,
    maxRegisterCodes: intake.MAX_REGISTER_CODES, intake: intake.intakeStatus(),
    isAdmin: false, shopCategoryCount: 0, maxShopCategoryLines: shopCat.MAX_SHOP_CATEGORY_LINES,
  }],
  ['new.ejs', 'new.ejs', { title: 't', displayName: 'smoke' }],
  ['detail.ejs (full/own_brand)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, own_brand: 1, asin: 'B0TEST', amazon_url: 'https://www.amazon.co.jp/dp/B0TEST' },
    // SP広告KW タブ (自社商品だけ)。JSON に埋めるだけなので中身は最小。</script> を含めても script を壊さないこと
    adKeywords: { configured: true, request: { id: 1, status: 'review_ready', collecting_seed: null, snapshot: { name: 'x' } }, stale: false,
      seeds: [{ id: 1, seed: '</script><script>alert(1)</script>', status: 'partial', coverage_text: '47 回中 1 回に候補あり', fetched_text: 'Amazon サジェストで観測・取得日 9月23日。検索回数は不明', candidate_count: 1 }],
      candidates: [{ id: 1, value: 'x y', evidence_id: 1, observed_count: 1, observed: [{ source: 'base', source_label: 'そのまま', seed: 'x' }], decision: null }],
      asins: [{ id: 2, asin: 'B0TESTTEST', added_by: 'smoke', added_at: '2026-09-23T00:00:00Z', decision: null }], adopted_asin_count: 0, own_asin: 'B0TEST',
      adopted_count: 0, exports: [], decision_version: 0, limits: { seed_max_len: 60, max_seeds: 20, max_asins: 5 },
      labels: { decision: {}, match_type: { exact: '完全一致' }, copy_block: { exact: '完全一致', product_targets: '商品ターゲット (ASIN)' }, evidence_status: {} }, match_types: ['exact', 'phrase', 'broad'] },
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
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [{ method: 'ネコポス', cost: 237, count: 3832 }, { method: '宅急便60サイズ', cost: 538, count: 417 }, { method: '</script><script>alert(1)</script>', cost: 999, count: 1 }], rakutenGroupNeHints: { '5': ['ネコポス'], '8': ['宅急便'] }, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '1y5', ...pageInfoVars,
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
    yahoo: { yahoo_price: 1980, yahoo_price_sagawa: null, delivery_label: 'ネコポス', shipping_override: 1, tax_rate: '10%', yahoo_category_id: 43494, yahoo_path: 'おもちゃ' },
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
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
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
    shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
    shopCategories: [],
    yahoo: { yahoo_price: null, yahoo_price_sagawa: null, delivery_label: null, tax_rate: '8%', yahoo_category_id: null, yahoo_path: null },
    imageProduction: null,
  }],
  ['detail.ejs (review / non-own-brand)', 'detail.ejs', {
    title: 't', displayName: 'smoke',
    draft: { ...after, status: 'review', has_variation: 1, own_brand: 0, asin: null, amazon_url: null, memo: 'm', price: 1980, jan_code: '49', drive_folder_url: 'https://drive.google.com/drive/folders/x', official_url: 'https://x' },
    refs: [], images: [], specs: [],
    aiOutputs: Object.fromEntries(dbmod.AI_OUTPUT_KINDS.map((k) => [k, null])),
    events: [], gate: [], nextStatuses: ['approved', 'draft', 'on_hold', 'excluded'],
    statusLabels, aiKinds: dbmod.AI_OUTPUT_KINDS,
    variation: variationFixtures.single, hasVariation: { value: false, source: 'ne' }, regroup: null,
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
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
    rakuten: null, cabinetImages: [], shopCategories: [], shippingGroups: listing.SHIPPING_METHOD_GROUPS, allShippingGroups: listing.ALL_SHIPPING_METHOD_GROUPS, setDecisionReasons: sd.SET_DECISION_REASONS, neShippingOptions: [], rakutenGroupNeHints: {}, yahooOverrideGroups: listing.YAHOO_OVERRIDE_SHIPPING_GROUPS, shippingSelectValue: '', ...pageInfoVars,
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
// 🆕 入荷のときに撮ったパッケージ裏面の写真 (2026-09-18)。写真がある商品の見え方を描かせる
{
  const d0 = renders.find((r) => r[1] === 'detail.ejs');
  if (d0) {
    const photos = [
      { id: 91, product_id: 'bl-child-a', product_name: '子A', status: 'uploaded', drive_url: 'https://drive.google.com/file/d/x/view', created_at: '2026-09-18T01:02:03Z', ar_no: 'AR900' },
      { id: 92, product_id: 'bl-child-b', product_name: '子B', status: 'stored', drive_url: null, created_at: '2026-09-18T01:03:04Z', ar_no: 'AR900' },
    ];
    renders.push(['detail.ejs (裏面の写真あり・AI文字起こし可)', 'detail.ejs',
      { ...d0[2], backLabelPhotos: photos, backLabelOcrEnabled: true }]);
    // AI が未設定でも壊れない (ボタンは出るが押せない)
    renders.push(['detail.ejs (裏面の写真あり・AI未設定)', 'detail.ejs',
      { ...d0[2], backLabelPhotos: photos, backLabelOcrEnabled: false }]);
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
    // 🚨 セット商品は**自分の工程 (set トラック)** で描く。親の workflow を使い回していたため、
    //    「main に set_review が無い」という本番の形が smoke に一度も現れず、
    //    画面の JS を丸ごと殺す埋め込み事故 (parent_step_version) を通していた (2026-09-07)
    renders.push(['detail.ejs (セット商品・セット工程の実データ)', 'detail.ejs',
      { ...d0[2], setDrafts: [], setInfo: sd.setInfoOf(db, wfSetDraftId),
        draft: { ...d0[2].draft, id: wfSetDraftId, parent_draft_id: wfSetParentId },
        workflow: wfp.progressOf(wfSetDraftId, { db }) }]);
    // 判断済みの見え方 (2026-09-06)。札に理由まで出て、ボタンは「見直す」に変わる。
    // 🚨 create / existing / hold も描く — none だけだと「判断済みなのに『作らない』と
    //    書いてあるボタン」のような分岐漏れを検出できない (Codex R1)
    for (const [tag, row] of [
      ['作らない', { decision: 'none', reason_code: 'shipping_loss' }],
      ['既存あり', { decision: 'existing', linked_set_draft_id: wfSetDraftId }],
      ['保留', { decision: 'hold', reason_text: '売れ行きを見てから' }],
      ['作成済み', { decision: 'create', linked_set_draft_id: wfSetDraftId }],
    ]) {
      renders.push([`detail.ejs (セットの親・判断=${tag})`, 'detail.ejs',
        { ...d0[2], setDrafts: [], setInfo: null,
          setDecision: row, setDecisionText: sd.describeSetDecision(row) }]);
    }
    // 画像の引き継ぎ計画 (§4.7): 枠ごとの表と、依頼に載る指示
    renders.push(['detail.ejs (セット商品・画像の計画)', 'detail.ejs', {
      ...d0[2], setDrafts: [], setInfo: sd.setInfoOf(db, wfSetDraftId),
      setImagePlans: [
        { slot: 0, parent_drive_file_id: 'pw-white', action: 'reuse', instruction: null, label: '白抜き' },
        { slot: 1, parent_drive_file_id: 'pimg-top', action: 'reuse', instruction: null, label: 'TOP画像' },
        { slot: 2, parent_drive_file_id: 'pimg-01', action: 'modify', instruction: '2個並べた写真に', label: '商品画像 1' },
        { slot: 3, parent_drive_file_id: 'pimg-02', action: 'drop', instruction: null, label: '商品画像 2' },
      ],
      setImageInstructions: sip.productionInstructions([
        { slot: 2, action: 'modify', instruction: '2個並べた写真に' },
      ]),
    }]);
    // NE の進み (§5.3): 要対応で止まっていて、親も派生後に更新された状態。
    // 配送方法は空 = セットはコピーしないので「NE確定待ち」が出る
    renders.push(['detail.ejs (セット商品・NE要対応)', 'detail.ejs', {
      ...d0[2], setDrafts: [], rakuten: { ...d0[2].rakuten, shipping_method_group: null },
      neShipping: { group: null, label: null }, neCost: { ...d0[2].neCost, shippingMethod: null },
      setInfo: {
        ...sd.setInfoOf(db, wfSetDraftId),
        provisional: true,   // 本コードを入れる前 (この画面の主役の状態)
        neState: 'needs_action', neError: '商品コード「plastic-set」は既に使われています',
        neRequestedAt: '2026-09-01T09:30:00.000Z', neRequestedBy: '中原 実紀',
        parentChanged: true,
        // 🚨 priceOrigin は**手で作らない** — setInfoOf が返す本物を使う (spread した値のまま)。
        //    フィクスチャで作ると計算側を壊しても画面テストが通ってしまう (#1181 の教訓)
      },
    }]);
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
      // SKU別JAN (2026-08-28): 保存済み / 未入力 の両方を描かせる
      skuJans: { 'rooms-l-bk': '4901234567894' },
      skuSelectorValues: { 'rooms-l-bk': 'ブラック' },
    }]);
    // バリエーションあり (原価は全SKU共通) — SKU別JANの列だけが出る分岐
    renders.push(['detail.ejs (バリエーション: SKU別JANのみ)', 'detail.ejs', {
      ...d0[2],
      draft: { ...d0[2].draft, ne_code: 'rooms', price: 1980, jan_code: '4901234567894' },
      variation: variationFixtures.rep,
      skuJans: { 'rooms-l-wh': '4912345678904' },
      skuSelectorValues: { 'rooms-l-wh': 'ホワイト' },
    }]);
    // SKU 表の商品仕様 (2026-09-03): 辞書の必須行 + 値のある行、SKU ごとの値、b は IDなし (理由 3)
    renders.push(['detail.ejs (バリエーション: SKU表の商品仕様)', 'detail.ejs', {
      ...d0[2],
      draft: { ...d0[2].draft, ne_code: 'rooms', price: 1980 },
      variation: variationFixtures.rep,
      genreDict: { genreName: 'テスト', genrePath: 'A > B', attributes: [
        { name: 'ブランド名', mandatory: true, inputMethod: 'DESCRIPTIVE' },
        { name: '代表カラー', mandatory: true, inputMethod: 'SELECTIVE' },
        { name: 'カタログID', mandatory: true },
        { name: '総枚数', mandatory: false },
      ] },
      skuJans: { 'rooms-l-bk': '4901234567894' },
      skuSelectorValues: { 'rooms-l-bk': 'ブラック', 'rooms-l-wh': 'ホワイト' },
      skuAttrGrid: { names: ['ブランド名', '代表カラー'], bySku: {
        'rooms-l-bk': { 'ブランド名': 'B-Faith', '代表カラー': 'ブラック' },
        'rooms-l-wh': { 'ブランド名': 'B-Faith', '代表カラー': 'ホワイト' },
      } },
      skuExemptions: { 'rooms-l-wh': 3 },
    }]);
    // 除外で実効 1 SKU になったバリエーション (2026-09-02 Codex R1 medium): サーバーは単品扱い
    // (memberCount > 1 でない) なので、画面も JAN 欄を出し SKU 表には JAN 入力欄を出さない
    renders.push(['detail.ejs (バリエーション: 実効1SKU)', 'detail.ejs', {
      ...d0[2],
      draft: { ...d0[2].draft, ne_code: 'rooms', price: 1980, jan_code: '4901234567894' },
      variation: { ...variationFixtures.rep, members: variationFixtures.rep.members.slice(0, 1), memberCount: 1 },
      skuJans: {}, skuSelectorValues: {},
    }]);
    // 商品情報: ブランド名・容量が入っている分岐
    renders.push(['detail.ejs (商品情報: ブランド名・容量あり)', 'detail.ejs', {
      ...d0[2],
      pageInfo: { ...(d0[2].pageInfo || {}), product_type: 'general', brand_name: 'B-Faith', content_volume: '200g' },
    }]);
    // 取扱先限定商品 (own_brand=0) でも画像制作の管理項目が使えること (2026-08-31 中原さん:
    // 栃木レザー等。自社商品だけに閉じていると撮影の設定や LP の重要度を決められなかった)
    renders.push(['detail.ejs (取扱先限定商品・画像制作あり)', 'detail.ejs', {
      ...d0[2],
      draft: { ...d0[2].draft, own_brand: 0, image_priority: '取扱先限定商品（重要度：高）' },
    }]);
    // 仕入商品 (重要度：低) でも出る (2026-09-13 スタッフ要望: 仕入れ低の商品も簡易LPを作る)
    renders.push(['detail.ejs (仕入商品・画像制作あり)', 'detail.ejs', {
      ...d0[2],
      draft: { ...d0[2].draft, own_brand: 0, image_priority: '仕入商品（重要度：低）' },
    }]);
    // メーカー型番 (2026-08-31): 旧データで商品属性側に入っている状態。
    // 上の「メーカー型番」欄へ引き上げて表示し、属性テーブルには行を出さない
    renders.push(['detail.ejs (メーカー型番が属性側にある旧データ)', 'detail.ejs', {
      ...d0[2],
      // 単品の描画 (メーカー型番の固定行は単品の表にある。バリエーションは SKU 表の行)
      variation: variationFixtures.single, hasVariation: { value: false, source: 'ne' },
      rakuten: { ...d0[2].rakuten, article_number: null,
        attributes_json: '[{"name":"ブランド名","values":["テストブランド"]},{"name":"メーカー型番","values":["toys3pen"]}]' },
    }]);
    // メーカー型番が 2 つあって競合している状態 (人がどれを残すか選ぶ画面)。
    // 型番に script 終了タグを混ぜ、画面へ埋め込むときのエスケープが効いていることも同時に見る
    renders.push(['detail.ejs (メーカー型番が競合)', 'detail.ejs', {
      ...d0[2],
      // 単品の描画 (メーカー型番の固定行は単品の表にある。バリエーションは SKU 表の行)
      variation: variationFixtures.single, hasVariation: { value: false, source: 'ne' },
      rakuten: { ...d0[2].rakuten, article_number: 'toys3pen',
        attributes_json: JSON.stringify([{ name: 'ブランド名', values: ['テストブランド'] },
          { name: 'メーカー型番', values: ['x' + '</scr' + 'ipt><img src=x onerror=alert(1)>'] }]) },
    }]);
    // 確認中 (2026-08-31): 立っているとき = 青い帯 + 経過日数 + 解除ボタン、
    // 立っていないとき = 理由ボタンが並ぶ帯 (既定の detail fixture 側で描かれる)
    renders.push(['detail.ejs (確認中)', 'detail.ejs', {
      ...d0[2],
      draft: {
        ...d0[2].draft,
        checking_reason_code: 'package_label', checking_note: '裏面の成分表示を確認',
        checking_since: '2026-08-25T00:00:00.000Z', checking_by: 'smoke@b-faith.biz',
      },
      checkingDays: 6,
    }]);
  }
}
// かんばん。カードあり / 自分の担当者が未紐付け / 空ボード の 3 分岐
// 確認中 (2026-08-31) のラベルを実際に描かせるため、ボード用の fixture を作る間だけ 1 件立てる
dbmod.setDraftChecking(db, wfDraftId, { reasonCode: 'package_label', note: '裏面の成分表示を確認', actor: 'smoke' });
const boardBase = {
  title: '工程ボード', displayName: '中原 大輔',
  board: wfp.boardData(db, { mallSummary: ms.mallSummaryFor }), staff: wf.listStaff(),
  me: null, assigneeId: null, assigneeParam: '', unassignedOnly: false, checkingOnly: false, imageKind: null,
  stepStateLabels: wfp.STEP_STATE_LABELS,
  boardView: 'main', imageKindLabels: wfp.IMAGE_KIND_LABELS,
  // 出品・展開の列で楽天の商品ページを組み立てる (2026-09-01)
  rakutenItemPageUrl: (mn) => `https://item.rakuten.co.jp/b-faith/${String(mn).toLowerCase()}/`,
  rakutenRmsItemUrl: (mn) => `https://item.rms.rakuten.co.jp/rms-sku/shops/373343/item/edit/${String(mn).toLowerCase()}`,
  // セット商品の表示 (2026-09-04)。router が渡しているものと同じ
  NE_STATE_LABELS: sd.NE_STATE_LABELS,
  // カードから「作らない」を選ぶときの理由 (2026-09-07)。詳細画面と同じ辞書
  setDecisionReasons: sd.SET_DECISION_REASONS,
  isAdmin: true,
  // NE要対応の件数バッジ (2026-09-10: どのタブでも出す)
  neCount: 2,
};
renders.push(
  ['board.ejs', 'board.ejs', boardBase],
  ['board.ejs (確認中で絞り込み)', 'board.ejs', {
    ...boardBase, checkingOnly: true,
    board: wfp.boardData(db, { checkingOnly: true, mallSummary: ms.mallSummaryFor }),
  }],
  ['board.ejs (画像ビュー)', 'board.ejs', {
    ...boardBase, boardView: 'image',
    board: wfp.boardData(db, { view: 'image' }),
  }],
  ['board.ejs (画像ビュー・TOPのみ)', 'board.ejs', {
    ...boardBase, boardView: 'image', imageKind: 'top',
    board: wfp.boardData(db, { view: 'image', imageKind: 'top' }),
  }],
  ['board.ejs (自分のボール・担当者未紐付け)', 'board.ejs', { ...boardBase, assigneeParam: 'me' }],
  // 完了列のカードにも画像の状況を出す (2026-09-01)。実データでは完了が 0 件のこともあるので、
  // 進行中のカードを 1 枚借りて必ず描かせる
  ['board.ejs (完了列にカード)', 'board.ejs', {
    ...boardBase,
    board: {
      ...boardBase.board,
      // 名前付き + 楽天「対象外」の完了カード (2026-09-10 Codex R1 low: 属性が空でも通る検査にしない)
      doneCards: boardBase.board.columns.flatMap((c) => c.cards).slice(0, 1)
        .map((c) => ({ ...c, name: '完了列テスト商品', rakutenRegisteredAt: null,
          malls: [{ code: 'rakuten', label: '楽天', state: 'skip' }] })),
      doneTotal: 1,
    },
  }],
  // セット商品のカード (2026-09-04 §5.2)。単品と見分けられるか・親と構成・NEの進み・⑤の判断
  ['board.ejs (セット商品のカード)', 'board.ejs', (() => {
    const base = boardBase.board.columns.flatMap((c) => c.cards)[0];
    const mk = (id, over) => ({ ...base, id, ne_code: `SETCARD-${id}`, name: `セット表示テスト ${id}`, ...over });
    return {
      ...boardBase,
      board: {
        ...boardBase.board,
        columns: boardBase.board.columns.map((c, i) => (i !== 0 ? c : {
          ...c,
          cards: [
            // 仮コードのまま NE 反映待ち
            mk(95001, {
              ne_code: 'SET-silicateclay800-01', name: '有機 珪酸塩白土 800g 2個セット',
              isSet: true, setChildrenCount: 0, setDecision: null,
              // 全体ビューは「基本情報入力」の列に投影して置くので、本当の工程名がカードに要る (§4.6)
              current: { ...base.current, step_code: 'set_ne_register', label: 'NE登録' },
              set: { parentId: 900, parentNeCode: 'silicateclay800', members: 'silicateclay800 × 2',
                provisional: true, neState: 'requested', neError: null, parentChanged: false },
            }),
            // NE 登録に失敗して人の対応待ち + 親が更新された
            mk(95002, {
              ne_code: 'SET-plastic-01', name: 'プラスチックシール 2種セット',
              isSet: true, setChildrenCount: 0, setDecision: null,
              set: { parentId: 901, parentNeCode: 'plastic', members: 'plastic-ki × 1 + plastic-ks × 1',
                provisional: true, neState: 'needs_action', neError: '商品コード「plastic-set」は既に使われています',
                parentChanged: true },
            }),
            // 本コードが確定したセット
            mk(95003, {
              ne_code: 'silicateclay800-3set', name: '有機 珪酸塩白土 800g 3個セット',
              isSet: true, setChildrenCount: 0, setDecision: null,
              set: { parentId: 900, parentNeCode: 'silicateclay800', members: 'silicateclay800 × 3',
                provisional: false, neState: 'confirmed', neError: null, parentChanged: false },
            }),
            // 親 (派生2件を持つ単品)
            mk(95004, { ne_code: 'silicateclay800', name: '有機 珪酸塩白土 800g', isSet: false, set: null, setChildrenCount: 2, setDecision: { decision: 'create', label: sd.describeSetDecision({ decision: 'create' }) } }),
            // ⑤で「作らない」と判断した単品
            mk(95005, { ne_code: 'kansho-yp100', name: 'ゆうパケット用 緩衝材 100枚', isSet: false, set: null, setChildrenCount: 0,
              setDecision: { decision: 'none', label: sd.describeSetDecision({ decision: 'none', reason_code: 'shipping_loss' }) } }),
            // ⑤で保留にした単品
            mk(95006, { ne_code: 'hoyu-1', name: '保留テスト', isSet: false, set: null, setChildrenCount: 0,
              setDecision: { decision: 'hold', label: sd.describeSetDecision({ decision: 'hold', reason_text: '売れ行きを見てから' }) } }),
          ],
        })),
      },
    };
  })()],
  // セットの判断をカードから決める (2026-09-07)。⑤の列では 作る/作らない/保留、
  // 判断済みならどの列でも「取り消す」。他人の担当ならボタンを出さず誰の担当かを書く
  ['board.ejs (セット判断のボタン)', 'board.ejs', (() => {
    const base = boardBase.board.columns.flatMap((c) => c.cards)[0];
    const mk = (id, over) => ({
      ...base, id, ne_code: `SETBTN-${id}`, name: `判断ボタン ${id}`,
      isSet: false, set: null, setChildrenCount: 0, setDecision: null,
      setReview: { version: 3, state: 'todo', assigneeId: null, assigneeName: null, roleCode: 'planner' },
      ...over,
    });
    return {
      ...boardBase,
      // admin ではない一般ユーザー (担当者 id=1 に紐づいている) として描く
      isAdmin: false, me: { id: 1, name: '中原 大輔' },
      board: {
        ...boardBase.board,
        columns: boardBase.board.columns.map((c) => {
          if (c.code === 'set_review') {
            return { ...c, cards: [
              // 未判断・未割り当て → 押した人が引き受ける (claim=1)
              mk(96001, {}),
              // 判断済み (保留) → ⑤は開いたままなのでボタンと取り消しが両方出る
              mk(96002, {
                setReview: { version: 5, state: 'todo', assigneeId: 1, assigneeName: '中原 大輔', roleCode: 'planner' },
                setDecision: { decision: 'hold', label: sd.describeSetDecision({ decision: 'hold' }) },
              }),
              // 他人の担当 → ボタンを出さない (403 で弾いてから理由を知らせない)
              mk(96003, {
                setReview: { version: 2, state: 'todo', assigneeId: 9, assigneeName: '高島 和美', roleCode: 'planner' },
              }),
            ] };
          }
          if (c.code === 'listing') {
            // ⑤の列を離れたあとのカード。判断済みなので「取り消す」だけが出る
            return { ...c, cards: [mk(96004, {
              setReview: { version: 7, state: 'done', assigneeId: 1, assigneeName: '中原 大輔', roleCode: 'planner' },
              setDecision: { decision: 'none', label: sd.describeSetDecision({ decision: 'none', reason_code: 'low_demand' }) },
            })] };
          }
          return { ...c, cards: [] };
        }),
        // 完了列のカードでも取り消せる (判断が済むとカードはやがてここに来る)
        doneCards: [mk(96005, {
          setReview: { version: 9, state: 'done', assigneeId: 1, assigneeName: '中原 大輔', roleCode: 'planner' },
          setDecision: { decision: 'create', label: sd.describeSetDecision({ decision: 'create' }) },
        })],
        doneTotal: 1,
      },
    };
  })()],
  // 出品・展開の列 (2026-09-01 楽天出品ボタン/結果)。未出品・失敗・出品済み の 3 枚を並べる
  ['board.ejs (出品・展開にカード)', 'board.ejs', (() => {
    const base = boardBase.board.columns.flatMap((c) => c.cards)[0];
    const malls = (st, itemUrl = null) => wf.listSteps ? [
      { code: 'rakuten', label: '楽天', state: st, itemUrl },
      { code: 'yahoo', label: 'Yahoo', state: 'todo', itemUrl: null },
    ] : [];
    const mk = (id, over) => ({ ...base, id, ne_code: `LST-${id}`, name: `出品テスト ${id}`, ...over });
    return {
      ...boardBase,
      board: {
        ...boardBase.board,
        columns: boardBase.board.columns.map((c) => c.code !== 'listing' ? c : {
          ...c,
          cards: [
            mk(90001, { malls: malls('todo'), rakutenRegisteredAt: null, rakutenLastError: null }),
            mk(90002, { malls: malls('todo'), rakutenRegisteredAt: null, rakutenLastError: 'ジャンルIDが未入力か数字ではありません' }),
            mk(90003, { malls: malls('done', 'https://item.rakuten.co.jp/x/lst-90003/'), rakutenRegisteredAt: '2026-09-01T00:00:00Z', rakutenLastError: null }),
            // 結果不明 (やり直し禁止・管理者だけ再実行) / 実行中 / 途中で止まった / 登録済みだが後処理が失敗
            mk(90004, { malls: malls('todo'), rakutenRegisteredAt: null, rakutenLastError: '楽天への登録結果が確認できませんでした (fetch failed)', rakutenListingOutcome: 'unknown' }),
            mk(90005, { malls: malls('todo'), rakutenRegisteredAt: null, rakutenLastError: null, rakutenListingOutcome: 'running', rakutenListingAttemptAt: new Date().toISOString() }),
            mk(90006, { malls: malls('todo'), rakutenRegisteredAt: null, rakutenLastError: null, rakutenListingOutcome: 'running', rakutenListingAttemptAt: '2026-08-01T00:00:00Z' }),
            mk(90007, { malls: malls('todo'), rakutenRegisteredAt: '2026-09-01T00:00:00Z', rakutenLastError: null }),
          ],
        }),
      },
    };
  })()],
  // セット工程ビュー (2026-09-04 §5.1)。列がセット工程になり、投影のタグは出さない
  ['board.ejs (セット工程ビュー)', 'board.ejs', (() => {
    const base = boardBase.board.columns.flatMap((c) => c.cards)[0];
    const card = {
      ...base, id: 95101, ne_code: 'SET-silicateclay800-01', name: '有機 珪酸塩白土 800g 2個セット',
      isSet: true, setChildrenCount: 0, setDecision: null,
      current: { ...base.current, step_code: 'set_compose', label: '構成決定' },
      set: { parentId: 900, parentNeCode: 'silicateclay800', members: 'silicateclay800 × 2',
        provisional: true, neState: 'requested', neError: null, parentChanged: false },
    };
    return {
      ...boardBase, boardView: 'set',
      board: {
        ...boardBase.board, view: 'set', doneCards: [], doneTotal: 0, total: 1,
        columns: [
          { code: 'set_compose', label: '構成決定', track: 'set', sort: 10, role_code: 'set_planner', stall_days: null, cards: [card] },
          { code: 'set_ne_register', label: 'NE登録', track: 'set', sort: 20, role_code: 'registrar', stall_days: 3, cards: [] },
          { code: 'set_content', label: '商品情報作成・確認', track: 'set', sort: 30, role_code: 'registrar', stall_days: null, cards: [] },
          { code: 'set_prep', label: '出品準備', track: 'set', sort: 40, role_code: 'approver', stall_days: null, cards: [] },
          { code: 'listing', label: '出品・展開', track: 'main', sort: 60, role_code: null, stall_days: null, cards: [] },
        ],
      },
    };
  })()],
  // NE要対応ビュー (2026-09-04 §5.5)。列ではなく表なので、行が読めるかを見る
  ['board.ejs (NE要対応)', 'board.ejs', {
    ...boardBase, boardView: 'ne',
    board: { view: 'ne', columns: [], doneCards: [], doneTotal: 0, total: 3, truncated: false, checkingTotal: 0 },
    neCount: 3, // router と同じ = 表の行数と一致する件数 (バッジはこの値を読む)
    neRows: [
      { id: 96001, neCode: 'SET-plastic-01', name: 'プラスチックシール 2種セット', status: 'draft',
        updatedAt: '2026-09-04T00:00:00Z', parentId: 901, parentNeCode: 'plastic', parentName: 'プラスチックシール',
        members: 'plastic-ki × 1 + plastic-ks × 1', state: 'needs_action',
        stateLabel: sd.NE_STATE_LABELS.needs_action, error: '商品コード「plastic-set」は既に使われています',
        requestedAt: '2026-09-01T00:00:00Z', requestedBy: '中原 実紀', waitingDays: 3,
        next: '理由を直して再要求する', stillTemporary: true },
      { id: 96002, neCode: 'silicateclay800-2set', name: '有機 珪酸塩白土 800g 2個セット', status: 'draft',
        updatedAt: '2026-09-03T00:00:00Z', parentId: 900, parentNeCode: 'silicateclay800', parentName: '珪酸塩白土',
        members: 'silicateclay800 × 2', state: 'requested', stateLabel: sd.NE_STATE_LABELS.requested,
        error: null, requestedAt: '2026-09-03T00:00:00Z', requestedBy: '中原 実紀', waitingDays: 1,
        next: '本コードが決まったら入力する', stillTemporary: false },
      { id: 96003, neCode: 'SET-shaganshi-01', name: '遮眼子 3個セット', status: 'draft',
        updatedAt: '2026-09-04T00:00:00Z', parentId: 902, parentNeCode: 'shaganshi', parentName: '遮眼子',
        members: 'shaganshi × 3', state: 'not_requested', stateLabel: sd.NE_STATE_LABELS.not_requested,
        error: null, requestedAt: null, requestedBy: null, waitingDays: null,
        next: 'ネクストエンジンに登録を依頼する', stillTemporary: true },
    ],
  }],
  ['board.ejs (空)', 'board.ejs', {
    ...boardBase,
    board: { view: 'main', columns: [], doneCards: [], doneTotal: 0, total: 0, truncated: false, checkingTotal: 0 },
  }],
);
// 🆕 工程ボードのカードに「裏面あり」バッジが出るか (全 fixture が出そろってから足す)
{
  const b0 = renders.find((r) => r[1] === 'board.ejs' && (r[2].board?.columns || []).some((col) => (col.cards || []).length));
  if (b0) {
    // 全カードぶんの枚数を 1 つの Map で渡す (カードごとに引かない)
    const counts = new Map();
    for (const col of (b0[2].board?.columns || [])) {
      for (const c of (col.cards || [])) counts.set(String(c.ne_code || '').trim().toLowerCase(), 2);
    }
    renders.push(['board.ejs (裏面の写真ありバッジ)', 'board.ejs', { ...b0[2], backLabelCounts: counts }]);
  } else {
    console.log('  !!  board.ejs にカードのある fixture が見つからないため、裏面バッジの描画は確かめられていません');
  }
}

const renderedHtml = new Map();
let dumpSeq = 0;   // PH_SMOKE_DUMP で書き出すときの通し番号
for (const [name, file, data] of renders) {
  try {
    // router が常に渡す共通 locals (画像スロットグリッド・棚の反映状態・自動追加バナー)
    const html = await ejs.renderFile(path.join(views, file),
      {
        thumbnailUrl, fileViewUrl, shopCatSyncState: null,
        // 🆕 入荷のときに撮ったパッケージ裏面の写真 (2026-09-18)。既定 = 無し。
        //    「ある」ときの見え方は fixture 側で上書きする
        backLabelPhotos: [], backLabelOcrEnabled: false,
        backLabelCounts: new Map(),
        // 既存の楽天ページへの追加か (2026-09-25)。既定 = 自動判定で新規ページ
        existingPage: { existingPage: false, auto: true, choice: '', addedTo: null },
        existingPageChoices: existingPageMod.EXISTING_PAGE_CHOICES,
        // SP広告 検索KW (2026-09-23)。router は own_brand のときだけ状態を渡す。既定 = 無し (タブを出さない)
        adKeywords: null,
        // 詳細画面の「← 戻る」の戻り先 (router の backLinkOf 相当。既定 = 一覧)
        backLink: { url: '/apps/product-hub/list', label: '← 一覧に戻る' },
        rakutenItemUrl: 'https://item.rakuten.co.jp/b-faith/rk-smoke-1/',
        rakutenRmsUrl: 'https://item.rms.rakuten.co.jp/rms-sku/shops/373343/item/edit/rk-smoke-1',
        skuImages: [],
        skuJans: {}, skuSelectorValues: {},
        imagePriorities: dbmod.IMAGE_PRIORITIES,
        materialStatuses: dbmod.MATERIAL_STATUSES,
        // 確認中 (2026-08-31)。detail は理由リスト、board は絞り込みの状態を使う
        checkingReasons: dbmod.CHECKING_REASONS,
        checkingNoteMax: dbmod.CHECKING_NOTE_MAX,
        checkingDays: null,
        // セット展開判断のいまの値 (2026-09-06)。既定 = まだ決めていない。
        // 判断済みの見え方は「セットの親」の fixture 側で上書きする
        setDecision: null, setDecisionText: '',
        // メーカー型番の属性名 (画面はこの属性行を出さない — 入口はメーカー型番欄だけ)
        modelAttrName: listing.MODEL_ATTR_NAME,
        checkingOnly: false,
        promptTemplates: { available: true, reason: null, initialJudge: '【入力】<x>', productAnalysis: '@LP制作システム' },
        // 本番の構成の 済/まだ (2026-09-13)。router が composeStateOf で作って渡す
        composeState: { excluded: false, done: false, marked: false, implied: false },
        // 工程パネル (detail.ejs)。fixture 側で上書きできるよう ...data より前に置く
        workflow: wfp.progressOf(wfDraftId, { db }),
        workflowStaff: wf.listStaff(),
        stepStateLabels: wfp.STEP_STATE_LABELS,
        isAdmin: true, myStaffId: null,
        // 自分の役割 (2026-09-13)。画像の工程は役割で「押せるか」を出し分ける
        myRoleCodes: [],
        mallStatus: ms.mallStatusOf(wfDraftId, { db }),
        setDrafts: [], setInfo: null,
        // NE登録の進みの表示名 (2026-09-04)。router が board にも detail にも渡している
        NE_STATE_LABELS: sd.NE_STATE_LABELS,
        neRows: [],
        // セットの画像の引き継ぎ計画 (2026-09-04 §4.7)。router が detail に渡している
        setImagePlans: [], setImageInstructions: [],
        setImageActions: sip.SET_IMAGE_ACTIONS, setImageActionLabels: sip.SET_IMAGE_ACTION_LABELS,
        skuPrices: {},
        // 画像タブの商品情報の自動表示 (2026-09-13)。router が product-info-auto / composeColorVariations で作る
        autoProductInfo: '', colorVariationsText: '■カラバリ\nなし (単品)',
        trailingBanners: [
          { location: listing.SHIPPING_BANNER_LOCATIONS['5'], label: '配送: ネコポス', url: listing.cabinetImageUrl(listing.SHIPPING_BANNER_LOCATIONS['5']) },
          ...listing.COMMON_TRAILING_BANNERS.map((b) => ({ ...b, url: listing.cabinetImageUrl(b.location) })),
        ],
        ...data,
      });
    check(`render ${name}`, html.length > 500);
    renderedHtml.set(name, html);
    // 見た目を目視するときだけ、描いた HTML をそのまま書き出す (サーバーを起こさずに確認できる)。
    // PH_SMOKE_DUMP=<dir> で有効。ファイル名に使えない文字は落とす
    if (process.env.PH_SMOKE_DUMP) {
      fs.mkdirSync(process.env.PH_SMOKE_DUMP, { recursive: true });
      // 日本語のラベルは _ に潰れて名前がぶつかるので、通し番号を頭に付ける
      dumpSeq += 1;
      fs.writeFileSync(path.join(process.env.PH_SMOKE_DUMP,
        `${String(dumpSeq).padStart(3, '0')}-${name.replace(/[^\w.()\-]+/g, '_')}.html`), html);
    }
  } catch (e) {
    check(`render ${name}`, false, e.message);
  }
}

// ─── 🆕 入荷のときに撮ったパッケージ裏面の写真 (2026-09-18 中原さん指示) ───
{
  const withPhotos = renderedHtml.get('detail.ejs (裏面の写真あり・AI文字起こし可)') || '';
  const noAi = renderedHtml.get('detail.ejs (裏面の写真あり・AI未設定)') || '';
  const plain = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  check('基本情報タブに裏面の写真が出る (実物を見ないと埋まらない項目をここから読む)',
    withPhotos.includes('パッケージ裏面の写真') && withPhotos.includes('/back-label/91') && withPhotos.includes('/back-label/92'));
  check('写真は product-hub 経由で出す (入荷受付チェックの権限を要求しない)',
    withPhotos.includes('/apps/product-hub/drafts/') && !withPhotos.includes('/apps/inbound-check/api/back-label/'));
  check('裏面情報の欄のそばにも写真と「AIで文字起こし」が出る',
    withPhotos.includes('id="back-info-ocr-btn"') && withPhotos.includes('id="back-info-ocr-msg"')
    && withPhotos.includes('back-info/transcribe'));
  check('AI文字起こしは「保存しません」と書いてある (AI の読み違いを黙って正本に入れない)',
    withPhotos.includes('保存はしません'));
  check('AI が未設定ならボタンは押せない + 理由が出る',
    noAi.includes('id="back-info-ocr-btn"') && noAi.includes('disabled') && noAi.includes('OPENAI_API_KEY'));
  check('写真が無ければ何も出さない (欄だけが増えない)',
    !plain.includes('パッケージ裏面の写真') && !plain.includes('id="back-info-ocr-btn"'));
  const board = renderedHtml.get('board.ejs (裏面の写真ありバッジ)') || '';
  const boardPlain = renderedHtml.get('board.ejs') || '';
  check('工程ボードのカードに「📷 裏面あり」が出る', board.includes('裏面あり') && board.includes('kb-tag backlabel'));
  check('写真が無いボードにはバッジを出さない', !boardPlain.includes('裏面あり'));
}

// ─── 白抜き画像の受信箱 (2026-09-14): 画像タブの白抜きの枠にボタン + 選択画面。JS への値は data 属性で渡す ───
{
  const full = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  check('画像タブ: 「受信箱から選ぶ」ボタン (白抜きあり = data-has-white-bg="1") と選択画面がある',
    full.includes('id="wb-inbox-btn"') && full.includes('data-has-white-bg="1"') && full.includes('data-code="')
    && full.includes('id="wb-inbox-modal"') && full.includes('id="wb-inbox-grid"') && full.includes('id="wb-inbox-filter"')
    && full.includes('登録の結果を確認できませんでした'));
  check('画像タブ: 白抜き未設定の描画では data-has-white-bg が空',
    [...renderedHtml.entries()].some(([n, h]) => n.startsWith('detail.ejs') && h.includes('data-has-white-bg=""')));
}

// ─── カタログID・商品仕様は「SKU 列 × 項目行」の表 1 箇所で入力 (2026-09-03 中原さん: RMS と同じ構成で
//     SKU ごとに IDあり/IDなし。基本情報タブには入力欄を置かない = 入口は 1 つ) ───
{
  // 単品の描画 = variationFixtures.single を使う fixture (「detail.ejs (full/own_brand)」は子SKUの描画)
  const single = renderedHtml.get('detail.ejs (rakuten registered + shop categories)') || '';
  const multi = renderedHtml.get('detail.ejs (バリエーション: SKU別JANのみ)') || '';
  const one = renderedHtml.get('detail.ejs (バリエーション: 実効1SKU)') || '';
  const gridFx = renderedHtml.get('detail.ejs (バリエーション: SKU表の商品仕様)') || '';
  const singleConds = {
    janInTable: single.includes('id="f-jan"') && single.includes('name="rk-catalog-mode"') && single.includes('id="rk-attrs"'),
    noGrid: !single.includes('id="rk-sku-grid"') && !single.includes('class="sku-jan-input"'),
    exemption: single.includes('id="rk-catalog-exemption"'),
    basicTabGuides: single.includes('class="jump-tab"'),
  };
  check('SKU表画面: 単品は #rk-attrs の 1 行目がカタログID (IDあり/IDなし + JAN + 理由)、SKU 表は無い',
    Object.values(singleConds).every(Boolean), JSON.stringify(singleConds));
  const multiConds = {
    grid: multi.includes('id="rk-sku-grid"'),
    perSkuCatalog: (multi.match(/class="sku-catalog-mode"/g) || []).length === 2 * 3 // 3 SKU × (IDあり/IDなし)
      && (multi.match(/class="sku-jan-input"/g) || []).length === 3
      && (multi.match(/class="sku-exemption-select"/g) || []).length === 3,
    noPageJan: !multi.includes('id="f-jan"') && !multi.includes('id="rk-attrs"') && !multi.includes('id="rk-article"'),
    janValueShown: multi.includes('value="4912345678904"'),
  };
  check('SKU表画面: バリエーションは SKU 列 × 項目行の表 (SKU ごとに IDあり/IDなし・JAN・理由)、ページ代表の欄は無い',
    Object.values(multiConds).every(Boolean), JSON.stringify(multiConds));
  check('SKU表画面: 実効 1 SKU は単品と同じ表 (SKU 表ではない) = サーバーの判定と一致',
    one.includes('id="f-jan"') && one.includes('id="rk-attrs"') && !one.includes('id="rk-sku-grid"'));
  const gridConds = {
    rows: gridFx.includes('data-name="代表カラー"') && gridFx.includes('data-name="ブランド名"'),
    perSkuValue: gridFx.includes('value="ブラック"') && gridFx.includes('value="ホワイト"'),
    bulk: (gridFx.match(/class="btn btn-sm sku-grid-bulk"/g) || []).length >= 2,
    exemptionSelected: /class="sku-exemption-select"[^>]*>[\s\S]*?value="3" selected/.test(gridFx),
  };
  check('SKU表画面: 商品仕様の行 (辞書の必須 + 値のある項目) に SKU ごとの値と一括入力ボタンが出る',
    Object.values(gridConds).every(Boolean), JSON.stringify(gridConds));
}

// ─── 画面から消した UI が戻ってこないこと (2026-08-28 中原さん指摘) ───
{
  // 参考URL: 「追加」ボタンは無い (入力したら反映されるので押す必要が無い)
  const anyDetail = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  check('参考URL: 「追加」ボタンを出さない (入力欄だけ)',
    anyDetail.includes('id="new-ref-url"') && !anyDetail.includes('id="add-ref-btn"'));
  // Notionカード: 「⏳ 未作成」「再作成」は出さない
  const noCard = renderedHtml.get('detail.ejs (child SKU + regroup button)') || '';
  check('Notionカード: 未作成のとき何も出さない (⏳未作成・再作成ボタンなし)',
    noCard.length > 500 && !noCard.includes('未作成') && !noCard.includes('notion-retry-btn')
    && !noCard.includes('Notionカード:'),
    noCard.includes('Notionカード:') ? 'Notionカード: が残っている' : '未作成/再作成が残っている');
  // レビュー待ちの商品でも Notion への導線は出さない (2026-08-28 中原さん。カード作成の経路は 2026-09-10 に撤去)
  const hasCard = renderedHtml.get('detail.ejs (review / non-own-brand)') || '';
  check('Notionカード: 「Notionで開く」リンクを出さない',
    hasCard.length > 500 && !hasCard.includes('Notionで開く') && !hasCard.includes('notion.so')
    && !hasCard.includes('Notionカード'),
    hasCard.includes('Notionで開く') ? 'リンクが残っている' : 'Notionカード の文言が残っている');
  // 一覧も同じ表示 (Notion列の ⏳ 未作成) をやめる。列を消したのでヘッダごと無い
  const list = renderedHtml.get('index.ejs (banner+rows+import panel)') || '';
  check('一覧: Notion列 (⏳ 未作成) を出さない',
    list.length > 500 && !list.includes('>Notion</th>') && !list.includes('⏳ 未作成'),
    list.includes('>Notion</th>') ? 'Notion列が残っている' : '⏳ 未作成 が残っている');
  // 「未作成 n件」バナーと「まとめて再作成」も出さない (カード作成の経路ごと撤去済み 2026-09-10)
  check('一覧: Notionカード未作成バナー・まとめて再作成を出さない',
    !list.includes('retry-all-btn') && !list.includes('まとめて再作成')
    && !list.includes('カード未作成') && !list.includes('notion-retry-all'));
  check('一覧: ヘッダとデータ行の列数が合っている (列削除で崩れていない)', (() => {
    const head = (list.match(/<thead>[\s\S]*?<\/thead>/) || [''])[0];
    const body = (list.match(/<tbody>[\s\S]*?<\/tbody>/) || [''])[0];
    const th = (head.match(/<th[\s>]/g) || []).length;
    const firstRow = (body.match(/<tr[^>]*>[\s\S]*?<\/tr>/) || [''])[0];
    const td = (firstRow.match(/<td[\s>]/g) || []).length;
    return th > 0 && th === td;
  })());
}

// ─── 確認中が画面に出ていること (2026-08-31 スタッフ要望の本体は「カードに表示」) ───
{
  const bh = renderedHtml.get('board.ejs') || '';
  // 2026-09-01 中原さん要望: TOP画像 / 商品詳細画像 が 済 か まだ かをカードで読めること。
  // TOP は工程を持たない (2026-08-31 廃止) ので、行の中身は画像の登録有無から作る。
  // 「どこか 1 つでもバッジがあれば OK」では、片方の行だけ描けなくなった退行を拾えない (Codex R1 low)
  const IMG_ROW_TEXT = { done: ['済'], todo: ['まだ'], off: ['対象外', '—'] };
  const krowsOf = (html) => html.match(/<div class="kb-krow"[^>]*>[\s\S]*?<\/div>/g) || [];
  const badgeOf = (row) => {
    const m = row.match(/<span class="kb-state (done|todo|off)">([^<]+)<\/span>/);
    return m ? { state: m[1], text: m[2] } : null;
  };
  const rowsOk = (rows) => rows.length > 0 && rows.every((r) => {
    const b = badgeOf(r);
    return b && IMG_ROW_TEXT[b.state].includes(b.text);
  });
  const rows = krowsOf(bh);
  check('ボード: カードの トップ画像 行に 済/まだ のバッジが付く',
    rowsOk(rows.filter((r) => r.includes('>トップ画像<'))),
    rows.filter((r) => r.includes('>トップ画像<')).slice(0, 2).join(' | ') || '(TOP の行が無い)');
  check('ボード: カードの 詳細画像 行に 済/まだ/対象外 のバッジが付く',
    rowsOk(rows.filter((r) => r.includes('>詳細画像<'))),
    rows.filter((r) => r.includes('>詳細画像<')).slice(0, 2).join(' | ') || '(詳細の行が無い)');
  // 構成 (2026-09-13 スタッフ要望)。トップ画像・詳細画像と同じ形で 済/まだ/対象外 を出す
  check('ボード: カードの 構成 行に 済/まだ/対象外 のバッジが付く',
    rowsOk(rows.filter((r) => r.includes('>構成<'))),
    rows.filter((r) => r.includes('>構成<')).slice(0, 2).join(' | ') || '(構成の行が無い)');
  // まとめて移動 (2026-09-13 スタッフ要望)。カードごとに選択のチェックがあり、リンクの外に置く (押しても詳細へ飛ばない)
  check('ボード: カードにまとめて移動の選択チェックがあり、リンクの外にある',
    /<label class="kb-pick"[^>]*><input type="checkbox" class="kb-pick-box"[^>]*><\/label>\s*<a class="kb-card-link"/.test(bh));
  // 画像ビューは担当者で絞らない (2026-09-13 中原さん決定: 画像の工程は担当者を置かない)。全体ビューは従来どおり
  {
    const bhImg = renderedHtml.get('board.ejs (画像ビュー)') || '';
    check('ボード: 画像ビューには「自分のボール / 未割り当て / 担当者で絞る」を出さない (すべて・確認中は残す)',
      !bhImg.includes('id="assignee-select"') && !/>\s*未割り当て<\/a>/.test(bhImg) && !bhImg.includes('自分のボール')
      && />すべて<\/a>/.test(bhImg) && bhImg.includes('🔍 確認中'));
    check('ボード: 全体ビューには 担当者で絞る・未割り当て が従来どおり出る',
      bh.includes('id="assignee-select"') && />\s*未割り当て<\/a>/.test(bh));
  }
  {
    // 完了列にも同じ 2 行が出る (本流を D&D で完了にすると TOP画像が未登録のまま完了列に入りうる)
    const bhDone = renderedHtml.get('board.ejs (完了列にカード)') || '';
    const doneCol = bhDone.split('data-col="done"')[1] || '';
    const doneRows = krowsOf(doneCol);
    check('ボード: 完了列のカードにも トップ画像 / 詳細画像 の状況が出る',
      rowsOk(doneRows.filter((r) => r.includes('>トップ画像<'))) && rowsOk(doneRows.filter((r) => r.includes('>詳細画像<'))),
      doneRows.slice(0, 2).join(' | ') || '(完了列に画像の行が無い)');
    // 完了列のカードにも楽天の状態と商品名を持たせる (2026-09-10 監査: 差し戻し時の出品確認が
    // 商品名なし・対象外でも出ていた)
    const doneCard = (doneCol.match(/<div class="kb-card[^>]*>/) || [''])[0];
    check('ボード: 完了列のカードに 楽天の状態 (data-rk=skip) と商品名 (data-name) が入る (差し戻し時の出品確認に使う)',
      /\sdata-rk="skip"/.test(doneCard) && /\sdata-name="完了列テスト商品"/.test(doneCard), doneCard.slice(0, 300));
  }
  {
    const bhMain = renderedHtml.get('board.ejs') || '';
    // NE要対応の件数バッジは、NE タブを開いていなくても出る (2026-09-10 監査)
    check('ボード: NE要対応の件数バッジが全体タブでも出る', /kb-tab-n">2</.test(bhMain));
    // 「出品・展開」に落としたときの楽天出品の確認は、本流の列を持つビュー全部で出す (画像ビュー以外)
    check('ボード: 出品の確認は画像ビュー以外のどのタブでも出る (全体タブ限定にしない)',
      bhMain.includes("BOARD_VIEW !== 'image' && to === 'listing'") && !bhMain.includes("BOARD_VIEW === 'main' && to === 'listing'"));
    // 4xx (登録済み・実行中・対象外) の失敗に「やり直せる」を重ねない
    check('ボード: 失敗アラートの「やり直せる」案内は retryable=true のときだけ',
      bhMain.includes("j.retryable === true ?") && !bhMain.includes("j.retryable === false ?"));
  }
  {
    // 出品・展開の列のカード (2026-09-01): 未出品=ボタン / 失敗=理由+やり直しボタン / 出品済み=商品ページ
    const bhL = renderedHtml.get('board.ejs (出品・展開にカード)') || '';
    // カード 1 枚分 = <div class="kb-card …> から次のカードまで (ボタンにも data-draft が付くので、それでは切れない)
    const cardOf = (id) => {
      const seg = bhL.split('<div class="kb-card ').find((s) => s.includes(`data-draft="${id}"`)) || '';
      // 次のカード (完了列は class="kb-card" と空白無しで始まる) と <script> の手前で切る —
      // 最後のカードは末尾まで伸びて、スクリプト内の '.kb-rk-btn' を拾ってしまう
      // (kb-card-top / kb-card-link は同じカードの中身なので切らない = 直後が '"' か空白のときだけ)
      return seg.split(/<div class="kb-card[" ]/)[0].split('<script>')[0];
    };
    const c1 = cardOf(90001), c2 = cardOf(90002), c3 = cardOf(90003);
    check('ボード(出品): 未出品のカードに「⚡ 楽天に出品」ボタンが出る',
      /class="kb-rk-btn"[^>]*data-draft="90001"/.test(c1) && c1.includes('楽天に出品') && !c1.includes('やり直す'),
      c1.slice(0, 200));
    check('ボード(出品): 失敗したカードに理由と「やり直す」ボタンが出る',
      c2.includes('kb-rk-fail') && c2.includes('ジャンルIDが未入力') && c2.includes('やり直す'),
      c2.slice(0, 200));
    check('ボード(出品): 出品済みのカードは商品ページへのリンク (ボタンは出さない)',
      c3.includes('kb-rk-done') && c3.includes('https://item.rakuten.co.jp/x/lst-90003/') && !c3.includes('kb-rk-btn'),
      c3.slice(0, 200));
    check('ボード(出品): ボタン・結果はカードのリンク (<a>) の外にある (入れ子リンクにしない)', (() => {
      // <a class="kb-card-link"> … </a> の内側に kb-rk が無いこと
      const inLinks = [...bhL.matchAll(/<a class="kb-card-link"[\s\S]*?<\/a>/g)].map((m) => m[0]);
      return inLinks.length > 0 && inLinks.every((a) => !a.includes('kb-rk'));
    })());
    check('ボード(出品): カードに data-rk (楽天の状態) と data-name が付く',
      /data-rk="todo"[\s\S]{0,80}data-name="出品テスト 90001"/.test(bhL) && bhL.includes('data-rk="done"'),
      (bhL.match(/data-rk="[^"]*"/g) || []).slice(0, 4).join(' '));
    // RMS の商品編集ページへのリンク (2026-09-04 中原さん要望)。出品済みのカードだけに出す
    check('RMSリンク: 出品済みのカードに RMS へのリンクが出る',
      c3.includes('/rms-sku/shops/373343/item/edit/lst-90003') && c3.includes('RMS ↗'), c3.slice(-260));
    check('RMSリンク: まだ出品していない・失敗したカードには出さない',
      !c1.includes('item.rms.rakuten.co.jp') && !c2.includes('item.rms.rakuten.co.jp'));
    const c4 = cardOf(90004), c5 = cardOf(90005), c6 = cardOf(90006), c7 = cardOf(90007);
    check('ボード(出品): 結果不明のカードは「やり直す」を出さず、RMS で確認の案内 + 管理者だけの再実行ボタン',
      c4.includes('確認できませんでした') && c4.includes('lst-90004') && !c4.includes('やり直す')
      && /class="kb-rk-btn"[^>]*data-force="1"/.test(c4),
      c4.slice(0, 260));
    check('ボード(出品): 実行中のカードは「出品しています…」でボタン無し',
      c5.includes('kb-rk-busy') && !c5.includes('kb-rk-btn'), c5.slice(0, 200));
    check('ボード(出品): 実行中のまま 15 分以上経ったカードは「途中で止まりました」= 結果不明と同じ扱い (やり直す無し・管理者の再実行のみ)',
      c6.includes('途中で止まりました') && c6.includes('RMS') && !c6.includes('やり直す')
      && /class="kb-rk-btn"[^>]*data-force="1"/.test(c6), c6.slice(0, 300));
    check('ボード(出品): 登録済みだがモール状況が未更新のカードは「出品済み」+ 警告 (ボタンは出さない)',
      c7.includes('kb-rk-done') && c7.includes('未更新') && !c7.includes('kb-rk-btn'), c7.slice(0, 260));
  }
  check('ボード: 確認中のカードに理由ラベルが出る',
    bh.includes('kb-checking') && bh.includes('🔍 確認中: パッケージ裏面の確認待ち'),
    bh.includes('kb-checking') ? 'ラベル文言が出ていない' : 'バッジ自体が無い');
  check('ボード: 補足は title 属性に入る (カードを狭くしない)',
    bh.includes('title="裏面の成分表示を確認"'));
  check('ボード: 「🔍 確認中」チップが件数付きで出る',
    /🔍 確認中 \d+/.test(bh), bh.includes('🔍 確認中') ? '件数が付いていない' : 'チップが無い');
  const bhOnly = renderedHtml.get('board.ejs (確認中で絞り込み)') || '';
  check('ボード: 確認中で絞ると そのチップだけが on になる',
    /<a class="chip on"\s+href="[^"]*filter=checking/.test(bhOnly)
    && !/<a class="chip on" href="\/apps\/product-hub\/board">/.test(bhOnly),
    bhOnly.match(/<a class="chip on"[\s\S]{0,80}/g)?.join(' | ') || 'on のチップが無い');
  const dh = renderedHtml.get('detail.ejs (確認中)') || '';
  check('詳細: 確認中の帯と解除ボタンが出る',
    dh.includes('🔍 確認中') && dh.includes('パッケージ裏面の確認待ち') && dh.includes('id="checking-clear-btn"')
    && dh.includes('裏面の成分表示を確認'), dh.includes('checking-clear-btn') ? '文言が出ていない' : 'ボタンが無い');
  check('詳細: 確認中は何日目かを出す (待ちっぱなしを見つけるため)', dh.includes('6日目'));
  const dh0 = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  // 2026-08-31 中原さん: 折りたたみ (details/summary) では気づけない → 理由ボタンを
  // 最初から並べて 1 タップで立つ形にした。折りたたみに戻さないための検査
  check('詳細: 確認中でないときは理由ボタンが**開いた状態で**並ぶ (折りたたみを使わない)',
    dh0.includes('🔍 確認中にする') && dh0.includes('class="btn chk-pick"')
    && !/<summary[^>]*>[^<]*確認中/.test(dh0) && !dh0.includes('id="checking-clear-btn"'),
    /<summary[^>]*>[^<]*確認中/.test(dh0) ? '折りたたみに戻っている' : 'ボタンが無い');
  check('詳細: 理由ボタンは全理由ぶん出て、押す理由がボタン自身に入っている (1タップで確定)', (() => {
    const picks = [...dh0.matchAll(/data-reason="([a-z_]+)"/g)].map((m) => m[1]);
    return picks.length === dbmod.CHECKING_REASONS.length
      && dbmod.CHECKING_REASONS.every((r) => picks.includes(r.code));
  })(), [...dh0.matchAll(/data-reason="([a-z_]+)"/g)].map((m) => m[1]).join(',') || '(1つも無い)');
  // 後始末: ボード fixture 用に立てた確認中を戻す (後続のテストに持ち越さない)
  dbmod.clearDraftChecking(db, wfDraftId, { actor: 'smoke' });
}

// ─── 画像制作の対象商品 / 撮影依頼の定型文 / Amazon URL を開く (2026-08-31 中原さん) ───
{
  const dOk = renderedHtml.get('detail.ejs (取扱先限定商品・画像制作あり)') || '';
  const dLow = renderedHtml.get('detail.ejs (仕入商品・画像制作あり)') || '';
  check('画像制作: 取扱先限定商品 (自社商品でない) でも管理項目が出る',
    dOk.includes('id="ip-request"') && dOk.includes('id="save-ip-btn"'));
  check('画像制作: 仕入商品 (重要度：低) でも管理項目と簡易LPボタンが出る (2026-09-13 全商品に出す)',
    dLow.includes('id="ip-request"') && dLow.includes('id="save-ip-btn"') && dLow.includes('data-kind="simpleLp"')
    && !dLow.includes('ONにすると、画像制作の管理項目が使えます'));
  check('撮影依頼: 定型文を作るボタンがある (デザイナー向けの文言は使わない)',
    dOk.includes('id="ip-request-template"') && dOk.includes('カメラマンへの撮影依頼')
    && !dOk.includes('外注への画像作成依頼'),
    dOk.includes('ip-request-template') ? '旧ラベルが残っている' : 'ボタンが無い');
  const dh0 = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  check('Amazon URL: 開くボタンがある (空のときは隠れる)',
    dh0.includes('id="f-amazon-open"')
    && /id="f-amazon-open"[\s\S]{0,140}display:none/.test(dh0),
    dh0.includes('f-amazon-open') ? '初期状態が隠れていない' : 'ボタンが無い');
}

// ─── 利益の配送方法ピッカー (2026-09-04) ──────────────────────────────
{
  const dh = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  const at = dh.indexOf('id="profit-ship-data"');
  const inside = at >= 0 ? dh.slice(at, dh.indexOf('</script>', at)) : '';
  // 配送方法名は NE 由来のデータ。</script> で script 要素を抜けられないこと
  // (JS 上で < に評価される '\u003c' だと置換が無意味になる — Codex R2 P1)
  check('配送費の試算: 配送方法名の </script> で script 要素を抜けられない',
    at > 0 && !inside.includes('<script>alert') && inside.includes('\\u003c'),
    inside.slice(-200));
  check('配送費の試算: 選べる配送方法が画面に埋め込まれる',
    inside.includes('ネコポス') && inside.includes('237'));
}

// ─── セット展開判断の記録 (2026-09-04 要件定義 §4.2) ──────────────────────
// 派生を作ったときだけ⑤が閉じる作りでは「まだ検討していない」と「作らないと決めた」が
// 区別できなかった。判断そのものを残し、⑤はこの記録があるときだけ閉じられる
{
  const newDraft = (code) => Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES (?, ?, 'draft', 'smoke')`
  ).run(code, `判断テスト ${code}`).lastInsertRowid);
  const decId = newDraft('SETDEC-1');
  const other = newDraft('SETDEC-OTHER');
  const setDec = (input) => { try { return { ok: true, r: sd.recordSetDecision(db, decId, input, 'smoke') }; } catch (e) { return { ok: false, status: e.status, msg: e.message }; } };

  check('セット判断: 「作らない」は理由を選ばないと記録できない',
    setDec({ decision: 'none' }).status === 400
    && setDec({ decision: 'none', reason_code: 'nope' }).status === 400, '');
  check('セット判断: 「その他」を選んだら一言も要る',
    setDec({ decision: 'none', reason_code: 'other' }).status === 400
    && setDec({ decision: 'none', reason_code: 'other', reason_text: '別ルートで検討' }).ok === true, '');
  check('セット判断: 選択肢にある理由なら記録できる',
    setDec({ decision: 'none', reason_code: 'shipping_loss' }).ok === true);
  check('セット判断: 最新の1件が「いまの判断」(append-only なので履歴は残る)',
    sd.latestSetDecision(db, decId).decision === 'none'
    && sd.latestSetDecision(db, decId).reason_code === 'shipping_loss'
    && db.prepare('SELECT COUNT(*) c FROM draft_set_decisions WHERE draft_id = ?').get(decId).c === 2,
    JSON.stringify(sd.latestSetDecision(db, decId)));
  check('セット判断: 表示名は理由まで含む (カードと詳細で同じ言葉)',
    sd.describeSetDecision(sd.latestSetDecision(db, decId)) === '作らない (送料負け (単価が低い))',
    sd.describeSetDecision(sd.latestSetDecision(db, decId)));
  check('セット判断: 「既存あり」は自分から作ったセットしか紐づけられない',
    setDec({ decision: 'existing' }).status === 400
    && setDec({ decision: 'existing', linked_set_draft_id: other }).status === 400, '');
  check('セット判断: 「保留」はメモだけで記録でき、⑤を閉じない',
    setDec({ decision: 'hold', reason_text: '売れ行きを見てから' }).r.closing === false
    && sd.SET_DECISIONS_CLOSING.includes('none') && !sd.SET_DECISIONS_CLOSING.includes('hold'));
  check('セット判断: 判断の指定そのものが不正なら記録しない',
    setDec({ decision: 'maybe' }).status === 400 && setDec({}).status === 400);
  db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(decId, other);
}
{
  // 派生を作ると「セットを作成」が自動で記録され、⑤が閉じる (記録 → 完了の順)
  const pid = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, price, created_by) VALUES ('setdec-create', '判断テスト 作成', 'approved', 1980, 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(pid, { db });
  // ⑤を閉じる操作なので版数が要る (楽観ロックを迂回しない)
  const pv = wfp.progressOf(pid, { db }).main.find((x) => x.step_code === 'set_review').version;
  const r = sd.createSetDraft(pid, { mode: 'copy', members: [{ ne_code: 'setdec-create', qty: 2 }], parent_step_version: pv }, 'smoke', { isAdmin: true, actorStaffId: null });
  const last = sd.latestSetDecision(db, pid);
  check('セット判断: 派生を作ると「セットを作成」が記録され、派生先に紐づく',
    last?.decision === 'create' && last.linked_set_draft_id === r.draftId, JSON.stringify(last));
  const setRow = db.prepare('SELECT parent_snapshot_at, ne_registration_state, provisional_code FROM product_drafts WHERE id = ?').get(r.draftId);
  check('セット作成: 派生時点の親を覚える (親が後で変わったら知らせるため)',
    !!setRow.parent_snapshot_at, JSON.stringify(setRow));
  check('セット作成: NE 登録はまず「未要求」から始まる',
    setRow.ne_registration_state === 'not_requested' && setRow.provisional_code === 1, JSON.stringify(setRow));
  db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(pid, r.draftId);
}

// ─── セット判断の取り消し (2026-09-07 中原さん:「間違って選択したときに戻せるように」) ───
// ボードのカードから 1 クリックで判断できるようにした以上、押し間違いは必ず起きる。
// 戻せないと現場は怖くて押さない = カードから決められるようにした意味が無くなる
{
  const newDraft = (code) => Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES (?, ?, 'draft', 'smoke')`
  ).run(code, `取消テスト ${code}`).lastInsertRowid);
  const undo = (id) => { try { return { ok: true, r: sd.undoSetDecision(db, id, 'smoke') }; } catch (e) { return { ok: false, status: e.status, msg: e.message }; } };

  // ① 何も決めていないものは取り消せない
  const d0 = newDraft('SETUNDO-0');
  check('判断の取り消し: まだ何も決めていなければ取り消せない',
    undo(d0).status === 400, JSON.stringify(undo(d0)));

  // ② 「作らない」1 件 → 取り消すと未判断に戻る (⑤も開け直す = closing:false)
  const d1 = newDraft('SETUNDO-1');
  sd.recordSetDecision(db, d1, { decision: 'none', reason_code: 'low_demand' }, 'smoke');
  const u1 = undo(d1);
  check('判断の取り消し: 直前の判断を消して未判断に戻る',
    u1.ok && u1.r.undone.startsWith('作らない') && sd.latestSetDecision(db, d1) === null
    && u1.r.closing === false, JSON.stringify(u1));
  check('判断の取り消し: 何を取り消したかは履歴に残る (記録が消えるわけではない)',
    db.prepare(`SELECT COUNT(*) c FROM draft_events WHERE draft_id = ? AND event = 'set_decision_undone'`).get(d1).c === 1);
  // 🚨 append-only の表から 1 行消す以上、消した行を**そのまま復元できる**ところまで残す。
  //    人が読む画面に出る detail なので「文 + 区切り + JSON」。区切りの後ろは素で JSON.parse できる
  check('判断の取り消し: 消した行は JSON で復元できる (理由コードや紐づけ先まで)',
    (() => {
      const ev = db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'set_decision_undone'`).get(d1);
      const parsed = sd.undoRecordOf(ev.detail);
      return parsed.removed.decision === 'none' && parsed.removed.reason_code === 'low_demand'
        && parsed.removed.decided_by === 'smoke' && !!parsed.removed.decided_at
        && parsed.removed.draft_id === d1
        && parsed.previous === null && parsed.withdrawn === null;
    })(),
    db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'set_decision_undone'`).get(d1)?.detail);

  // 🚨 理由メモは人の自由入力なので、区切り文字列そのものを書ける (Codex R3)。
  //    本文に紛れ込むと JSON の手前で切れて復元できなくなる — 本文側から区切りを落としている
  const dMark = newDraft('SETUNDO-MARK');
  sd.recordSetDecision(db, dMark, { decision: 'hold', reason_text: `やめる${sd.UNDO_RECORD_MARKER}{"removed":"にせもの"}` }, 'smoke');
  const uMark = undo(dMark);
  check('判断の取り消し: 区切り文字列を含むメモでも、消した行を復元できる',
    uMark.ok && (() => {
      const ev = db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'set_decision_undone'`).get(dMark);
      const parsed = sd.undoRecordOf(ev.detail);
      return parsed.removed.decision === 'hold' && parsed.removed.reason_text.includes('にせもの');
    })(),
    db.prepare(`SELECT detail FROM draft_events WHERE draft_id = ? AND event = 'set_decision_undone'`).get(dMark)?.detail);

  // ③ 判断が 2 件あるときは**ひとつ前**に戻る (必ず未判断に戻すと辻褄が合わなくなる)
  const d2 = newDraft('SETUNDO-2');
  sd.recordSetDecision(db, d2, { decision: 'hold', reason_text: '様子見' }, 'smoke');
  sd.recordSetDecision(db, d2, { decision: 'none', reason_code: 'single_enough' }, 'smoke');
  const u2 = undo(d2);
  check('判断の取り消し: ひとつ前の判断に戻る (履歴があれば未判断にはしない)',
    u2.ok && sd.latestSetDecision(db, d2)?.decision === 'hold' && u2.r.closing === false,
    JSON.stringify(u2));
  check('判断の取り消し: 戻った先が「作る・既存あり・作らない」なら⑤は閉じたまま (closing で伝える)',
    (() => {
      sd.recordSetDecision(db, d2, { decision: 'none', reason_code: 'single_enough' }, 'smoke');
      sd.recordSetDecision(db, d2, { decision: 'hold' }, 'smoke');
      const u = undo(d2);
      return u.ok && u.r.closing === true && sd.latestSetDecision(db, d2)?.decision === 'none';
    })());

  // ④ 「セットを作成」の取り消し = 手つかずのセットも一緒に取り下げる
  const p1 = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, price, created_by) VALUES ('setundo-c', '取消テスト 作成', 'approved', 1980, 'smoke')`
  ).run().lastInsertRowid);
  const pv1 = wfp.progressOf(p1, { db }).main.find((x) => x.step_code === 'set_review').version;
  const created = sd.createSetDraft(p1, { mode: 'copy', parent_step_version: pv1 }, 'smoke', { isAdmin: true, actorStaffId: null });
  const u3 = undo(p1);
  check('判断の取り消し: 「セットを作成」を取り消すと、手つかずのセットも取り下げる (除外)',
    u3.ok && u3.r.withdrawn?.id === created.draftId
    && db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(created.draftId).status === 'excluded',
    JSON.stringify(u3));
  check('判断の取り消し: 取り下げたセットは親のカードの「派生セット」に数えない',
    wfp.boardData(db, {}).columns.concat([{ cards: wfp.boardData(db, {}).doneCards }])
      .flatMap((c) => c.cards).filter((c) => c.id === p1).every((c) => c.setChildrenCount === 0), '');

  // ⑤ 誰かが進めたセットは消さない — 取り消しごと断る (勝手に人の作業を消さない)
  const p2 = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, price, created_by) VALUES ('setundo-t', '取消テスト 進行中', 'approved', 1980, 'smoke')`
  ).run().lastInsertRowid);
  const pv2 = wfp.progressOf(p2, { db }).main.find((x) => x.step_code === 'set_review').version;
  const created2 = sd.createSetDraft(p2, { mode: 'copy', parent_step_version: pv2 }, 'smoke', { isAdmin: true, actorStaffId: null });
  // セットの工程は progressOf の main (track='set' もここに入る) から拾う
  const composeStep = wfp.progressOf(created2.draftId, { db }).main.find((x) => x.step_code === 'set_compose');
  wfp.setStepState(created2.draftId, 'set_compose', { state: 'doing', expected_version: composeStep.version }, 'smoke', { isAdmin: true });
  const u4 = undo(p2);
  check('判断の取り消し: 誰かが進めたセットがあるときは取り消しごと断る',
    u4.ok === false && u4.status === 400 && /作成のあとに操作されている/.test(u4.msg || '')
    && sd.latestSetDecision(db, p2)?.decision === 'create'
    && db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(created2.draftId).status !== 'excluded',
    JSON.stringify(u4));

  // ⑥ 🚨 親に画像がある商品でも、作った直後なら取り消せる (2026-09-07 Codex R1)。
  //    作成時に applyImagePlanToTrack が画像工程を todo → skip にして版数を上げるので、
  //    「工程の版数が動いたか」で見ると**実データでは常に取り消せなくなる**
  const p3 = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, price, created_by) VALUES ('setundo-img', '取消テスト 画像あり', 'approved', 1980, 'smoke')`
  ).run().lastInsertRowid);
  {
    const insImg = db.prepare('INSERT INTO draft_images (draft_id, drive_file_id, sort) VALUES (?, ?, ?)');
    for (let i = 0; i < 4; i++) insImg.run(p3, `undo-img-${i}`, i);
  }
  const pv3 = wfp.progressOf(p3, { db }).main.find((x) => x.step_code === 'set_review').version;
  const created3 = sd.createSetDraft(p3, { mode: 'copy', parent_step_version: pv3 }, 'smoke', { isAdmin: true, actorStaffId: null });
  const imgTouched = db.prepare(`SELECT COUNT(*) c FROM draft_step_progress WHERE draft_id = ? AND version > 0`).get(created3.draftId).c;
  const u5 = undo(p3);
  check('判断の取り消し: 親に画像がある商品でも、作った直後なら取り消せる',
    imgTouched > 0 && u5.ok === true && u5.r.withdrawn?.id === created3.draftId,
    `画像計画で版数が動いた工程 ${imgTouched} 件 / ${JSON.stringify(u5)}`);

  db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
    .run(d0, d1, dMark, d2, p1, created.draftId, p2, created2.draftId, p3, created3.draftId);
}

// ─── ⑤を閉じる画面 (2026-09-04 §5.4) ────────────────────────────────────
// 🚨 判断なしで⑤を閉じられなくした以上、画面から判断を送れないと工程が詰まる (Codex R3 high)
{
  const dh = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
  const at = dh.indexOf('id="setdec-modal"');
  const modal = at >= 0 ? dh.slice(at, at + 3000) : '';
  check('セット判断の画面: 判断を選ぶダイアログが描かれている', at > 0);
  check('セット判断の画面: 作らない・保留を選べる',
    /name="setdec" value="none"/.test(modal) && /name="setdec" value="hold"/.test(modal), modal.slice(0, 200));
  check('セット判断の画面: 「作らない」の理由は選択式 (自由入力ではない)',
    modal.includes('id="setdec-reason"') && modal.includes('送料負け') && modal.includes('需要が見込めない'),
    modal.slice(modal.indexOf('setdec-reason'), modal.indexOf('setdec-reason') + 300));
  check('セット判断の画面: 新規作成はここでは選ばせない (作成の入口へ案内する)',
    !/name="setdec" value="create"/.test(modal) && modal.includes('id="setdec-goto-create"')
    && modal.includes('セットを作る'));
  check('セット判断の画面: ⑤で「完了」を選んだらダイアログを開く (そのままでは閉じられない)',
    /tr\.dataset\.step === 'set_review' && state\.value === 'done'/.test(src) && /openSetDecision\(tr\)/.test(src));
  check('セット判断の画面: 記録は工程の版数を添えて送り、409 なら読み直す',
    /set-decision/.test(src)
    && /expected_version: decRow\.dataset\.version/.test(src)
    && /r\.status === 409[\s\S]{0,80}reloadSafely\(\)/.test(src));
  check('セット判断の画面: 工程の行は今の状態を持つ (完了を選び直しても戻せる)',
    /data-prev-state="<%= s\.state %>"/.test(src));
}

// ─── ⑤のガードは D&D でも効く / カードは boardData の結果で確かめる (2026-09-04) ──
{
  // 🚨 ボードのドラッグは moveBoardCard が直接 setStepState を呼ぶ。ルーター側だけに
  // ガードを置くと、⑤から次の列へドラッグしただけで判断なしに閉じられる (Codex R1 high)
  const pid = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, price, official_url, created_by)
     VALUES ('setdnd-1', 'D&Dテスト', 'approved', 1000, 'https://example.com/dnd', 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(pid, { db });
  const ver = () => wfp.progressOf(pid, { db }).main.find((x) => x.step_code === 'set_review').version;
  // ⑤まで進めてから、次の列へドラッグする (掴んだ時点の現在工程を CAS で渡す)
  for (const code of ['basic_info', 'ai_generate', 'desc_review', 'title_approve']) {
    const v = wfp.progressOf(pid, { db }).main.find((x) => x.step_code === code).version;
    wfp.setStepState(pid, code, { state: 'done', expected_version: v }, 'smoke', { isAdmin: true, actorStaffId: null });
  }
  let moveErr = null;
  try {
    wfp.moveBoardCard(pid, { view: 'main', to: 'listing', expectedCurrent: 'set_review' }, 'smoke', { isAdmin: true, actorStaffId: null });
  } catch (e) { moveErr = e; }
  check('セット判断: 判断がなければ D&D でも⑤を通り越せない',
    moveErr?.status === 400 && /判断を先に記録/.test(moveErr.message || ''), moveErr?.message || '例外が出ていない');
  check('セット判断: 通り越せなかったので⑤は開いたまま',
    wfp.progressOf(pid, { db }).main.find((x) => x.step_code === 'set_review').state !== 'done');
  db.prepare(`INSERT INTO draft_set_decisions (draft_id, decision, reason_code, decided_by) VALUES (?, 'none', 'low_demand', 'smoke')`).run(pid);
  wfp.setStepState(pid, 'set_review', { state: 'done', expected_version: ver() }, 'smoke', { isAdmin: true, actorStaffId: null });
  check('セット判断: 判断を記録すれば閉じられる',
    wfp.progressOf(pid, { db }).main.find((x) => x.step_code === 'set_review').state === 'done');
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(pid);
}
{
  // 🚨 カードの値は **boardData が実際に返すもの**で確かめる。手で組んだフィクスチャだけだと、
  // 置き場所を間違えても (c.image.isSet のように入れ子になっても) テストが通ってしまう (Codex R1 high)
  const parentId = Number(db.prepare(
    `INSERT INTO product_drafts (ne_code, name, status, price, created_by) VALUES ('setcard-parent', 'カード結合テスト 親', 'approved', 1500, 'smoke')`
  ).run().lastInsertRowid);
  wfp.progressOf(parentId, { db });
  const pv = wfp.progressOf(parentId, { db }).main.find((x) => x.step_code === 'set_review').version;
  const made = sd.createSetDraft(parentId, { mode: 'copy', members: [{ ne_code: 'setcard-parent', qty: 2 }], parent_step_version: pv },
    'smoke', { isAdmin: true, actorStaffId: null });
  const cards = wfp.boardData(db, {}).columns.flatMap((c) => c.cards);
  const setCard = cards.find((c) => c.id === made.draftId);
  const parentCard = cards.find((c) => c.id === parentId);
  check('カード(実データ): セットは isSet と親・構成・NEの進みをトップレベルに持つ',
    setCard && setCard.isSet === true && setCard.set?.parentNeCode === 'setcard-parent'
    && setCard.set.members === 'setcard-parent × 2' && setCard.set.provisional === true
    && setCard.set.neState === 'not_requested', JSON.stringify(setCard && { isSet: setCard.isSet, set: setCard.set }));
  check('カード(実データ): 単品は isSet=false で、派生の件数を持つ',
    parentCard && parentCard.isSet === false && parentCard.setChildrenCount === 1,
    JSON.stringify(parentCard && { isSet: parentCard.isSet, n: parentCard.setChildrenCount }));
  check('カード(実データ): 親の判断は「セットを作成」として残る',
    parentCard?.setDecision?.decision === 'create' && parentCard.setDecision.label === 'セットを作成',
    JSON.stringify(parentCard?.setDecision));
  // 本コードに差し替えたら「確定」になる (確定は provisional_code から導出。状態列に持たない)
  db.prepare('UPDATE product_drafts SET provisional_code = 0 WHERE id = ?').run(made.draftId);
  const after = wfp.boardData(db, {}).columns.flatMap((c) => c.cards).find((c) => c.id === made.draftId);
  check('カード(実データ): 本コードが確定したら NE の進みも「確定」になる',
    after?.set?.neState === 'confirmed' && after.set.provisional === false, JSON.stringify(after?.set));
  db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(parentId, made.draftId);
}

// ─── セット商品のカード (2026-09-04 要件定義 §5.2) ────────────────────────
// ボード上でセットが単品と見分けられること。判定はサーバー側で構造化して渡した値だけを使い、
// テンプレで商品コードの文字列から推測しない
{
  const bh = renderedHtml.get('board.ejs (セット商品のカード)') || '';
  const cardOf = (id) => {
    const seg = bh.split('<div class="kb-card ').find((x) => x.includes(`data-draft="${id}"`)) || '';
    return seg.split(/<div class="kb-card[" ]/)[0].split('<script>')[0];
  };
  const c1 = cardOf(95001), c2 = cardOf(95002), c3 = cardOf(95003);
  const parent = cardOf(95004), none = cardOf(95005), hold = cardOf(95006);

  check('セットカード: セットと分かる印と、親・構成が出る',
    c1.includes('data-set="1"') && c1.includes('🎁 セット')
    && c1.includes('親: silicateclay800') && c1.includes('silicateclay800 × 2'), c1.slice(0, 400));
  check('セットカード: 仮コードは (仮) と分かる',
    c1.includes('SET-silicateclay800-01') && c1.includes('(仮)'));
  check('セットカード: NE の進みが出る (反映待ち)',
    c1.includes('NE: 要求済み・反映待ち'), c1.slice(0, 300));
  check('セットカード: NE で止まっているカードは理由まで出る',
    c2.includes('NE: 要対応') && c2.includes('plastic-set') && c2.includes('既に使われています'), c2.slice(0, 400));
  check('セットカード: 親が更新されていたら知らせる (自動追随はしない)',
    c2.includes('親が更新されています') && !c1.includes('親が更新されています'));
  check('セットカード: 本コードが確定したら「確定」と出し (仮) を出さない',
    c3.includes('NE: 本コード確定') && !c3.includes('(仮)'), c3.slice(0, 300));

  check('セットカード: 親のカードには派生の件数を出す (一覧は詳細画面)',
    parent.includes('派生セット 2件') && !parent.includes('data-set="1"'), parent.slice(0, 300));
  // 🚨 判定は「セット商品の印」= その札そのものに限る (2026-09-07)。
  //    ページ全体・カード全体で「🎁 セット」を includes すると、判断の札 (🎁 判断: セットを作成) を拾って
  //    合格・不合格が入れ替わる — 否定形のテストほど、見る範囲を狭くする
  check('セットカード: 親のカードに「セット」の印は付けない',
    !parent.includes('kb-tag set'), parent.slice(0, 300));
  check('⑤の判断: 「作らない」はカードに理由まで出す',
    none.includes('判断: 作らない') && none.includes('送料負け'), none.slice(0, 300));
  check('⑤の判断: 「保留」もカードに出す (未検討と区別する)',
    hold.includes('判断: 保留') && hold.includes('売れ行きを見てから'), hold.slice(0, 300));
  // 2026-09-07: 「セットを作成」も札に出すようにした。判断するとカードは次の列へ動くので、
  // ⑤の列にしか札を出さないと『押し間違いを取り消す』入口がボードから消える
  check('⑤の判断: 「セットを作成」も札に出し、取り消せる',
    parent.includes('判断: セットを作成') && parent.includes('↩ 取り消す'), parent.slice(0, 600));

  // 全体ビューはセット工程を本流の列に投影して置くので、本当の工程名を必ず出す (§4.6)
  check('セットカード: 全体ビューでは本当のセット工程名を出す (投影を隠さない)',
    c1.includes('セット工程: NE登録'), c1.slice(0, 500));
  check('セットカード: 単品には工程名のタグを付けない', !parent.includes('セット工程:'));

  // セット工程ビューは列そのものがセット工程なので、投影の断り書きは要らない
  const bhSet = renderedHtml.get('board.ejs (セット工程ビュー)') || '';
  check('セット工程ビュー: 列がセット工程になる',
    bhSet.includes('構成決定') && bhSet.includes('NE登録') && bhSet.includes('商品情報作成・確認')
    && bhSet.includes('出品準備') && !bhSet.includes('基本情報入力'), bhSet.slice(0, 200));
  check('セット工程ビュー: 列名と工程が同じなので投影のタグは出さない',
    !bhSet.includes('セット工程: '));

  // ─── セットの判断をカードから決める (2026-09-07 中原さん要望) ───
  {
    const bhBtn = renderedHtml.get('board.ejs (セット判断のボタン)') || '';
    // 完了列のカードは class="kb-card" (末尾の空白なし) なので、区切りは正規表現で取る
    const cardB = (id) => {
      const seg = bhBtn.split(/<div class="kb-card[" ]/).find((x) => x.includes(`data-draft="${id}"`)) || '';
      return seg.split('<script>')[0];
    };
    const fresh = cardB(96001), held = cardB(96002), others = cardB(96003), later = cardB(96004);
    check('ボード判断: ⑤の列の未判断カードに 作る/作らない/保留 が並ぶ',
      fresh.includes('🎁 作る') && fresh.includes('🚫 作らない') && fresh.includes('⏸ 保留')
      && fresh.includes('まだ決めていません'), fresh.slice(0, 600));
    check('ボード判断: 未判断のカードには「取り消す」を出さない (取り消すものが無い)',
      !fresh.includes('↩ 取り消す'), fresh.slice(0, 600));
    check('ボード判断: 未割り当ての工程は押した人が引き受ける (claim=1 を持たせる)',
      fresh.includes('data-claim="1"') && fresh.includes('data-version="3"'), fresh.slice(0, 400));
    check('ボード判断: 保留は⑤が開いたままなので、判断ボタンと取り消しが両方出る',
      held.includes('判断: 保留') && held.includes('🚫 作らない') && held.includes('↩ 取り消す'), held.slice(0, 600));
    check('ボード判断: 自分が担当の工程は引き受け直さない (claim を送らない)',
      held.includes('data-claim=""') && held.includes('data-version="5"'), held.slice(0, 400));
    // 🚨 押せない理由は**押す前に**出す (403 で弾かれてから知らせない)。詳細画面 #1223 と同じ物差し
    check('ボード判断: 他人の担当のカードはボタンを出さず、誰の担当かを書く',
      others.includes('高島 和美 さんの担当です')
      && !others.includes('🚫 作らない') && !others.includes('↩ 取り消す'), others.slice(0, 600));
    // 判断が決まるとカードは次の列へ動く。⑤の列にしか置かないと押し間違いを直せなくなる
    check('ボード判断: ⑤を離れたカードでも「取り消す」だけは出る (押し間違いを直せる)',
      later.includes('判断: 作らない') && later.includes('↩ 取り消す')
      && !later.includes('🎁 作る'), later.slice(0, 600));
    // ボタンはカードのリンク (<a class="kb-card-link">) の外 = 押しても詳細画面へ飛ばない
    check('ボード判断: ボタンはカードのリンクの外にある (入れ子リンクにしない)',
      fresh.indexOf('</a>') !== -1 && fresh.indexOf('kb-set-btn') > fresh.indexOf('</a>'), fresh.slice(0, 800));
    // 判断が済むとカードは列を移り、やがて完了列に来る。片方にしか置かないと取り消せなくなる
    const doneCard = cardB(96005);
    check('ボード判断: 完了列のカードでも「取り消す」が出る',
      doneCard.includes('判断: セットを作成') && doneCard.includes('↩ 取り消す')
      && !doneCard.includes('🎁 作る'), doneCard.slice(0, 600));
    check('ボード判断: 理由を聞くダイアログは 1 枚を使い回す (カードごとに入力欄を置かない)',
      (bhBtn.match(/id="kb-setdec"/g) || []).length === 1
      && bhBtn.includes('送料負け (単価が低い)'), '');
  }

  // NE要対応ビュー (§5.5)。列ではなく表 — 止まっているセットを1画面で見比べる
  const bhNe = renderedHtml.get('board.ejs (NE要対応)') || '';
  check('NE要対応: 表に状態・商品・コード・次にやること が出る',
    bhNe.includes('NE: 要対応') && bhNe.includes('プラスチックシール 2種セット')
    && bhNe.includes('SET-plastic-01') && bhNe.includes('理由を直して再要求する'), bhNe.slice(0, 200));
  check('NE要対応: 止まっている理由をそのまま出す',
    bhNe.includes('既に使われています'));
  check('NE要対応: 親と構成が分かる',
    bhNe.includes('plastic-ki × 1 + plastic-ks × 1'));
  check('NE要対応: 依頼から3日以上は目立たせる (工程の滞留警告と同じ物差し)',
    /class="ne-late"/.test(bhNe));
  check('NE要対応: 仮コードは (仮) と分かり、本コード入力済みは「NE取込待ち」と出す',
    bhNe.includes('(仮)') && bhNe.includes('NE取込待ち'));
  check('NE要対応: 未要求には「NEに登録を依頼した」、要対応には「再要求」を出す',
    bhNe.includes('NEに登録を依頼した') && bhNe.includes('再要求'), '');
  check('NE要対応: 依頼済みには「登録できなかった」を出す',
    bhNe.includes('登録できなかった'));
  check('NE要対応: 列 (かんばん) は出さない',
    !/<div class="kb">\s*<div class="kb-col/.test(bhNe));
  check('NE要対応: タブに件数を出す', /kb-tab-n">3</.test(bhNe));

  // セットの詳細画面 (§5.3)
  const dhSet = renderedHtml.get('detail.ejs (セット商品・NE要対応)') || '';
  check('セット詳細: NE の進みをステッパーで出す',
    /class="ne-stepper"/.test(dhSet) && dhSet.includes('未要求') && dhSet.includes('要求済み・反映待ち')
    && dhSet.includes('本コード確定'), dhSet.slice(0, 200));
  check('セット詳細: 要対応なら理由まで出す',
    dhSet.includes('要対応') && dhSet.includes('既に使われています'));
  check('セット詳細: 依頼した日時と人を出す',
    dhSet.includes('2026-09-01 09:30') && dhSet.includes('中原 実紀'), '');
  check('セット詳細: 要対応には「再要求する」を出す',
    /ne-state-btn[^>]*data-state="requested"/.test(dhSet));
  check('セット詳細: 親が更新されていたら知らせ、確認するボタンを出す',
    dhSet.includes('派生したあとに親商品が更新されています') && dhSet.includes('id="parent-ack-btn"'));
  check('セット詳細: 売価の由来を1行で出す (構成の単品売価 × 個数の和)',
    dhSet.includes('WF-SET-P 1,980円 × 2 = 3,960円') && dhSet.includes('売価の目安'),
    dhSet.slice(Math.max(0, dhSet.indexOf('売価の目安')), dhSet.indexOf('売価の目安') + 200));
  check('セット詳細: 配送方法は「NE確定待ち」と理由を出す (コピーしていないため)',
    dhSet.includes('NE確定待ち') && dhSet.includes('個数で箱もサイズも変わる'), '');
  // 画像の引き継ぎ計画 (§4.7)
  const dhImg = renderedHtml.get('detail.ejs (セット商品・画像の計画)') || '';
  check('画像の計画: 枠ごとの表を出す (親の画像・どうする・指示)',
    dhImg.includes('親の画像をどう使うか') && dhImg.includes('白抜き') && dhImg.includes('TOP画像')
    && dhImg.includes('商品画像 1'), dhImg.slice(0, 200));
  check('画像の計画: いまの指定が選ばれた状態で出る',
    /<select class="sip-action"[\s\S]*?<option value="modify" selected>直して使う<\/option>/.test(dhImg), '');
  check('画像の計画: 指示文がそのまま入っている',
    dhImg.includes('value="2個並べた写真に"'), '');
  check('画像の計画: 制作の依頼に載る内容を別に見せる (作る人がここだけ見れば分かる)',
    dhImg.includes('この画像を作ってください')
    && dhImg.includes('<strong>商品画像 1</strong>: 直して使う — 2個並べた写真に'),
    dhImg.slice(Math.max(0, dhImg.indexOf('この画像を作ってください')), dhImg.indexOf('この画像を作ってください') + 300));
  check('画像の計画: 単品の詳細には出さない',
    !(renderedHtml.get('detail.ejs (セットの親・作成済み一覧)') || '').includes('親の画像をどう使うか'));

  // 単品の詳細にはセットの表示を出さない
  // 🚨 キーは**描画テストの名前**であってファイル名ではない。'detail.ejs' では常に undefined になり、
  //    この下の否定形テスト (「出さない」) が空文字列に対して素通りしていた (2026-09-04 発見)
  const dhSingle = renderedHtml.get('detail.ejs (セットの親・作成済み一覧)') || '';
  check('単品の詳細を実際に描けている (この下の「出さない」テストが空文字で素通りしないこと)',
    dhSingle.length > 500, String(dhSingle.length));
  check('単品の詳細には NE の進みを出さない',
    !/class="ne-stepper"/.test(dhSingle) && !dhSingle.includes('NE確定待ち'));
  // セット作成フォーム (§5.6): 構成を**複数行**で指定できる。個数だけの1欄ではない
  check('セット作成フォーム: 構成を複数行で指定できる (行を追加できる)',
    dhSingle.includes('id="set-members"') && dhSingle.includes('id="set-member-add"'), '');
  // 候補は「id があるか」ではなく**中身**を見る (空の datalist でも id は通ってしまう)
  const dlHtml = (dhSingle.match(/<datalist id="set-member-codes">([\s\S]*?)<\/datalist>/) || [])[1] || '';
  check('セット作成フォーム: 商品コードの候補に この商品 と バリエーションの子SKU が入る',
    /<option value="rooms-/.test(dlHtml) && (dlHtml.match(/<option /g) || []).length >= 2,
    dlHtml.replace(/\s+/g, ' ').slice(0, 300));
  check('セット作成フォーム: 個数だけの旧フォーム (set-qty) は残っていない',
    !dhSingle.includes('id="set-qty"'), '');
  check('セット作成フォーム: 配送方法を引き継がないこと・売価の初期値を先に伝える',
    dhSingle.includes('配送方法は引き継ぎません') && dhSingle.includes('単品売価 × 個数'), '');
  // ─── 「作る／作らない」の入口 (2026-09-06 中原さん) ───
  // 以前は「作らない」の入口が工程パネルのプルダウンの中にしか無く、現場から見つけられなかった。
  // 両方を**同じ場所の押せるボタン**として出していることを HTML で確かめる
  check('セット判断の入口: 「作る」「作らない/保留」が両方ボタンとして出ている',
    dhSingle.includes('id="set-create-open"') && dhSingle.includes('id="set-decision-open"'), '');
  check('セット判断の入口: 作成フォームの入口は details の summary ではなくボタン',
    !/<summary[^>]*>セット商品を作る<\/summary>/.test(dhSingle)
    && /id="set-create-box" hidden/.test(dhSingle), '');
  check('セット判断の入口: まだ決めていないことが札で分かる',
    dhSingle.includes('まだ決めていません'), '');
  {
    // 未判断のときは「作る」が青 (下の判断済みループと合わせて「未判断と保留だけ青」を固定する)
    const ca = dhSingle.indexOf('id="set-create-open"');
    check('セット判断の入口: 未判断の「作る」ボタンは青 (いちばん進めたい操作)',
      ca > 0 && dhSingle.slice(Math.max(0, ca - 80), ca).includes('btn-primary'),
      dhSingle.slice(Math.max(0, ca - 80), ca));
    // 取り消すものが無いうちは出さない (押せないボタンを並べない)
    check('セット判断の入口: 未判断のうちは「取り消す」を出さない',
      !dhSingle.includes('id="set-decision-undo"'), '');
  }
  {
    // 担当外の一般ユーザーは押せない (サーバー側 assertStepOperable と同じ物差し)。
    // 押せてしまうと 403 で弾かれてから理由を知ることになる
    const dhOther = renderedHtml.get('detail.ejs (一般ユーザー・担当外)') || '';
    const createBtn = dhOther.slice(dhOther.indexOf('id="set-create-open"'),
      dhOther.indexOf('>', dhOther.indexOf('id="set-create-open"')));
    const decBtn = dhOther.slice(dhOther.indexOf('id="set-decision-open"'),
      dhOther.indexOf('>', dhOther.indexOf('id="set-decision-open"')));
    check('セット判断の入口: 担当外の人にはどちらのボタンも押させない (理由も出す)',
      dhOther.includes('id="set-create-open"') && createBtn.includes('disabled')
      && decBtn.includes('disabled') && dhOther.includes('工程「セット商品作成検討」の担当者'),
      createBtn.slice(0, 120) + ' | ' + decBtn.slice(0, 120));
    // 判断済みなら理由まで札に出し、ボタンは「見直す」に変わる
    const dhNone = renderedHtml.get('detail.ejs (セットの親・判断=作らない)') || '';
    // 🚨 否定形はページ全体で見ない (2026-09-07)。画面下の JS の確認メッセージにも
    //    「まだ決めていません」が出るので、判定は**札そのもの**に限る
    const chipOf = (html) => {
      const at = html.indexOf('🎁 セット商品にする？');
      return at > 0 ? html.slice(at, at + 400) : '';
    };
    check('セット判断の入口: 判断済みなら理由つきで札に出る',
      chipOf(dhNone).includes('作らない (送料負け (単価が低い))') && !chipOf(dhNone).includes('まだ決めていません'),
      chipOf(dhNone));
    // 🚨 「見直す」は判断の種類ごとに確かめる。none だけ見ていると、既存あり・作成済みで
    //    「札=既存のセットあり / ボタン=作らない」の食い違いが残る (Codex R1)
    for (const tag of ['作らない', '既存あり', '保留', '作成済み']) {
      const dh = renderedHtml.get(`detail.ejs (セットの親・判断=${tag})`) || '';
      // 判定は**ボタンの中身**で行う。ページ全体を見ると、同じ言葉を使った
      // 画面下の JS のコメントを拾って素通り/誤検知する
      const at = dh.indexOf('id="set-decision-open"');
      const btn = at > 0 ? dh.slice(at, dh.indexOf('</button>', at)) : '';
      check(`セット判断の入口: 判断済み (${tag}) のボタンは「見直す」`,
        dh.length > 500 && btn.includes('この判断を見直す') && !btn.includes('セットは作らない')
        && !chipOf(dh).includes('まだ決めていません'), btn.slice(0, 220) + ' | ' + chipOf(dh).slice(0, 200));
      // 押し間違いを白紙に戻す入口 (2026-09-07)。「見直す」は上書きなので、未判断には戻せない
      check(`セット判断の入口: 判断済み (${tag}) には「取り消す」も出る`,
        dh.includes('id="set-decision-undo"') && dh.includes('↩ この判断を取り消す'), '');
      // 決めた後に作成を推さない (青を外す)。「保留」はまだ決めていないのと同じなので青のまま
      const ca = dh.indexOf('id="set-create-open"');
      const cls = ca > 0 ? dh.slice(Math.max(0, ca - 80), ca) : ''; // このボタンの開きタグ (class を含む)
      check(`セット判断の入口: ${tag} の「作る」ボタンの色 (保留だけ青のまま)`,
        ca > 0 && cls.includes('btn-primary') === (tag === '保留'), cls);
    }
  }
  // ─── セット欄の開閉を実際に押す (2026-09-06 Codex R1) ───
  // 「id がある・hidden がある」だけでは、配線が消えても開閉の向きが逆になっても素通りする
  {
    const vm = await import('node:vm');
    const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
    const start = src.indexOf('    function setCreateBoxOpen(box, open, onOpen) {');
    const end = src.indexOf('    <%# ここまでがセット欄の開閉の切り出し範囲', start);
    const chunk = start >= 0 && end > start ? src.slice(start, end) : '';
    check('セット欄の開閉: detail.ejs から切り出せる (EJS の埋め込みを含まない)',
      chunk.includes('function wireSetCreateToggle') && !chunk.includes('<%'), String(chunk.length));
    const mk = () => ({
      hidden: true, handlers: {}, scrolled: 0,
      addEventListener(type, fn) { (this.handlers[type] = this.handlers[type] || []).push(fn); },
      click() { (this.handlers.click || []).forEach((fn) => fn()); },
    });
    const els = { 'set-create-box': mk(), 'set-create-open': mk(), 'set-create-cancel': mk() };
    const doc = { getElementById: (id) => els[id] || null };
    const api = new vm.Script(`${chunk}\n({ wire: wireSetCreateToggle, open: setCreateBoxOpen })`,
      { filename: 'set-create-toggle.js' }).runInNewContext({});
    let opened = 0;
    const box = api.wire(doc, () => { opened++; });
    check('セット欄の開閉: 既定は閉じている', box === els['set-create-box'] && box.hidden === true, String(box && box.hidden));
    els['set-create-open'].click();
    check('セット欄の開閉: 「セットを作る」で開く', box.hidden === false && opened === 1, `hidden=${box.hidden} opened=${opened}`);
    els['set-create-open'].click();
    check('セット欄の開閉: もう一度押すと閉じる', box.hidden === true && opened === 1, `hidden=${box.hidden} opened=${opened}`);
    els['set-create-open'].click();
    els['set-create-cancel'].click();
    check('セット欄の開閉: 「閉じる」で閉じる (開くときのコールバックは呼ばない)',
      box.hidden === true && opened === 2, `hidden=${box.hidden} opened=${opened}`);
    // 判断ダイアログの「🎁 セットを作る」= 開くだけ (すでに開いていてもトグルで閉じない)
    api.open(box, true);
    api.open(box, true);
    check('セット判断ダイアログ: 「セットを作る」は常に開く (押すたび閉じない)', box.hidden === false, String(box.hidden));
    check('セット判断ダイアログ: 作成欄を開く配線が入っている',
      /setCreateBoxOpen\(document\.getElementById\('set-create-box'\), true/.test(src), '');
  }
  check('セット工程ビュー: タブは自分が選ばれた状態になる',
    /class="kb-tab on"[^>]*href="[^"]*view=set"/.test(bhSet)
    || /href="[^"]*view=set"[^>]*class="kb-tab on"/.test(bhSet)
    || bhSet.includes('view=set') && /kb-tab on[^<]*">🧩 セット工程/.test(bhSet),
    (bhSet.match(/<a class="kb-tab[^>]*>[^<]*<\/a>/g) || []).join(' | '));
}

// ─── 詳細画面の RMS リンク (2026-09-04) ───────────────────────────────────
{
  const dh = renderedHtml.get('detail.ejs (rakuten published)') || '';
  // 「楽天で公開中」バナーの中。モール別の表にも入れてあるが、そちらは楽天を「完了」に
  // したときだけ出るのでこのフィクスチャでは描かれない
  const hits = (dh.match(/item\.rms\.rakuten\.co\.jp/g) || []).length;
  check('RMSリンク: 詳細画面 (公開済み) の商品ページリンクの隣に RMS も出る',
    hits >= 1 && dh.includes('RMSで編集') && dh.includes('/rms-sku/shops/373343/item/edit/'), 'hits=' + hits);
  // 未登録の商品には出さない (router は常に URL を渡すが、テンプレ側の
  // 「公開中」/「楽天=完了」の条件で描かれない)
  const dh0 = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  check('RMSリンク: 楽天に登録していない商品の詳細画面には出さない',
    !dh0.includes('item.rms.rakuten.co.jp'));
}

// ─── 配送方法プルダウンの復元 (2026-09-03 #1152 / Codex R2 low) ───────────
// DB には楽天IDしか無いので、画面に戻すのは router が逆引きした shippingSelectValue。
// 「保存した複合選択肢が再表示で選ばれている」ことを HTML で確かめる
{
  const dh = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  const sel = dh.slice(dh.indexOf('id="rk-shipping-group"'), dh.indexOf('</select>', dh.indexOf('id="rk-shipping-group"')));
  // 選択肢から外した配送方法 (「現在使用不可」) が保存されていても、その行を足して選択状態を保つ
  check('配送方法: 選択肢に無い保存値でも option を足して選択状態を保つ (Codex R3)',
    /allShippingGroups\[shipDefault\]/.test(fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8')));
  check('配送方法: 保存済みの複合選択肢が再表示で選ばれている',
    /value="1y5"\s+selected/.test(sel) && !/value="1"\s+selected/.test(sel), sel.slice(0, 400));
  check('配送方法: ヤフー別扱いの商品はヤフー欄が開いた状態で描かれる',
    /let yahooOverrideOn = true/.test(dh));
}

// ─── 画面の直し 3 点 (2026-08-31 中原さんの実務フィードバック) ───
{
  const dh = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  // ④ 店舗内カテゴリ: 必須項目なので畳まない。ツリーは 5 枠より上に置く (記載を見逃さないため)
  const accIdx = dh.indexOf('id="rk-cats-acc"');
  const slotIdx = dh.indexOf('id="rk-cat-slots"');
  check('カテゴリ: ツリーは既定で開いている (必須項目を畳まない)',
    accIdx >= 0 && /<details id="rk-cats-acc" open/.test(dh),
    accIdx >= 0 ? dh.slice(accIdx - 40, accIdx + 40) : '(ツリーが無い)');
  check('カテゴリ: ツリーは 1〜5 の枠より上にある',
    accIdx >= 0 && slotIdx >= 0 && accIdx < slotIdx, `acc=${accIdx} slots=${slotIdx}`);
  // ⑤ 画像フォルダのリンクを開くボタン (入力が空なら隠れる = 初期表示は display:none)
  check('画像フォルダ: リンクを開くボタンがある (空のときは隠れる)',
    dh.includes('id="import-folder-open"') && dh.includes('📂 開く')
    && /id="import-folder-open"[\s\S]{0,120}display:none/.test(dh),
    dh.includes('import-folder-open') ? '初期状態が隠れていない' : 'ボタンが無い');
}

// ─── メーカー型番の入口は 1 つ / ジャンル属性の候補ボタン (2026-08-31 中原さん要望) ───
{
  const dl = renderedHtml.get('detail.ejs (メーカー型番が属性側にある旧データ)') || '';
  const attrTable = (dl.match(/<table class="list sku-grid" id="rk-attrs"[\s\S]*?<\/table>/) || [''])[0];
  // 2026-09-02: RMS と同じく、メーカー型番は「商品仕様」テーブルの中の固定行 (rk-article)。
  // 自由入力の属性行 (rk-attr-name) としては出ないこと = 入口が 1 つのまま
  check('メーカー型番: 商品仕様テーブルに固定行として出る (rk-article がテーブル内・自由入力行は出ない)',
    attrTable.length > 0 && attrTable.includes('id="rk-article"') && attrTable.includes('テストブランド')
    && !/class="rk-attr-name"[^>]*value="メーカー型番"/.test(attrTable),
    attrTable.includes('id="rk-article"') ? '自由入力行にメーカー型番が残っている' : 'rk-article がテーブル内に無い');
  check('メーカー型番: 旧データの値は固定行の欄へ引き上げて表示する',
    /id="rk-article" value="toys3pen"/.test(dl) && dl.includes('旧データの値をここへ移しました'),
    (dl.match(/id="rk-article" value="[^"]*"/) || ['(見つからない)'])[0]);
  // 旧値を欄へ引き上げて表示している状態は、そのまま/書き換えて保存できる必要がある
  // (Codex R4 high: フラグが false だと保存が 400 になるのに解消ボタンが出ていない)
  // 埋め込む JSON のエスケープ (Codex R7 high: バックスラッシュ 1 つだと JS 上では '<' そのもので
  // 置換が無意味になり、型番に script 終了タグを入れられると script ブロックを脱出できる)
  {
    // 競合の fixture は型番に script 終了タグを混ぜてある。埋め込み行がそれを生で持たないこと
    const dc = renderedHtml.get('detail.ejs (メーカー型番が競合)') || '';
    const seenLine = dc.split('\n').find((l) => l.includes('const modelSeenValues')) || '';
    check('メーカー型番: 画面へ埋め込む値は < をエスケープする (script ブロックを脱出させない)',
      seenLine.length > 0 && seenLine.includes('u003c')
      && !seenLine.toLowerCase().includes('<' + '/script'),
      seenLine.slice(0, 160) || '(埋め込み行が無い)');
    check('メーカー型番: 競合しているときは「どれを残すか」のボタンを出す',
      dc.includes('id="rk-model-conflict"') && dc.includes('rk-model-pick')
      && dc.includes('型番なしにする'),
      dc.includes('rk-model-conflict') ? '選択ボタンが無い' : '警告が出ていない');
    check('メーカー型番: 競合中は解消フラグを立てない (人が選ぶまで捨てない)',
      dc.includes('let modelConflictResolved = false;'),
      (dc.split('\n').find((l) => l.includes('let modelConflictResolved')) || '(見つからない)').trim());
  }
  check('メーカー型番: 引き上げ表示のときは解消フラグが最初から立つ (書き換えて保存できる)',
    /let modelConflictResolved = true;/.test(dl),
    (dl.match(/let modelConflictResolved = \w+;/) || ['(見つからない)'])[0]);
  const d0f = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  check('メーカー型番: 旧値が無い通常の商品では解消フラグは立てない (黙って捨てない)',
    /let modelConflictResolved = false;/.test(d0f),
    (d0f.match(/let modelConflictResolved = \w+;/) || ['(見つからない)'])[0]);
  // 候補ボタン (②): 入れ物とジャンル辞書の受け渡しがあること
  const d0h = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  check('属性候補: 候補ボタンの置き場所とジャンル辞書の受け渡しがある',
    d0h.includes('id="rk-attr-suggest"') && d0h.includes('id="rk-attrdict-json"')
    && d0h.includes('id="rk-model-attr-json"'));
  check('属性候補: 辞書の全属性 (必須+任意) を渡している (必須だけに絞らない)', (() => {
    const m = d0h.match(/id="rk-attrdict-json">(\[[\s\S]*?\])<\/script>/);
    if (!m) return false;
    let arr = [];
    try { arr = JSON.parse(m[1]); } catch (e) { return false; }
    // fixture のジャンル辞書は必須 1 件のみだが、mandatory フラグ付きで渡っていることを見る
    return arr.length >= 1 && arr.every((x) => typeof x.name === 'string' && 'mandatory' in x);
  })(), (d0h.match(/id="rk-attrdict-json">([\s\S]{0,120})/) || ['', '(無い)'])[1]);
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
    && stepOf(newRow.id, 'imgd_material')?.state === 'todo'
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
  wfp2.setStepState(untouchedId, 'imgd_request', { note: '手で準備中' }, 'smoke', ADMIN);
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
  try { wfp2.setStepState(roomsRow.id, 'imgd_request', { state: 'doing' }, 'smoke', ADMIN); } catch (e) { holdStep = e; }
  check('画像DB: 保留中は画像工程を動かせない (400)', holdStep?.status === 400 && /保留中/.test(holdStep.message)
    && stepOf(roomsRow.id, 'imgd_request')?.state === 'todo', holdStep?.message);
  let holdMove = null;
  try { wfp2.moveBoardCard(roomsRow.id, { view: 'image', kind: 'detail', to: 'compose', expectedCurrent: 'imgd_request' }, 'smoke', ADMIN); } catch (e) { holdMove = e; }
  check('画像DB: 保留中はボード D&D も止まる', !!holdMove && /保留中/.test(holdMove.message), holdMove?.message);
  check('画像DB: 保留中でも本流工程は動く', wfp2.setStepState(roomsRow.id, 'basic_info', { note: '本流メモ' }, 'smoke', ADMIN).changed === true);
  dbmod.setImageWorkflowState(db, roomsRow.id, 'active', { actor: 'smoke' });
  check('画像DB: 解除後は画像工程が動く', wfp2.setStepState(roomsRow.id, 'imgd_request', { state: 'doing' }, 'smoke', ADMIN).changed === true);
  wfp2.setStepState(roomsRow.id, 'imgd_request', { state: 'todo' }, 'smoke', ADMIN);

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

// ─── ボードから楽天に出品 (2026-09-01 中原さん要望): 画像転送 → 登録 → 後処理 を 1 本で ───
// 外部 (Drive / R-Cabinet / RMS) は deps で差し替える。見るのは「順番・止まり方・記録」
{
  const bl = await import('../services/board-listing.js');
  const wfp = wfpEarly;
  // 本流を「出品・展開」まで進めた商品 (ボードでそこへ落とした状態)。工程のゲート (基本情報の材料等) は
  // ここでは見たいものではないので、fixture として直接 done にする
  const mkDraft = (code, { atListing = true, status = 'draft' } = {}) => {
    const id = Number(db.prepare(
      `INSERT INTO product_drafts (ne_code, name, status, created_by) VALUES (?, ?, ?, 'smoke')`,
    ).run(code, `楽天出品テスト ${code}`, status).lastInsertRowid);
    wfp.ensureProgress(db, id);
    if (atListing) {
      db.prepare(`UPDATE draft_step_progress SET state = 'done' WHERE draft_id = ?
                  AND step_code IN (SELECT code FROM ph_steps WHERE track = 'main' AND code <> 'listing')`).run(id);
    }
    return id;
  };
  const rkOf = (id) => db.prepare('SELECT registered_at, last_error FROM draft_rakuten WHERE draft_id = ?').get(id) || {};
  const mallOf = (id) => db.prepare("SELECT state, item_url FROM draft_mall_status WHERE draft_id = ? AND mall = 'rakuten'").get(id) || {};
  const stepOf = (id) => db.prepare("SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'imgd_rakuten'").get(id)?.state;
  const eventsOf = (id) => db.prepare('SELECT event FROM draft_events WHERE draft_id = ? ORDER BY id').all(id).map((r) => r.event);
  const okTransfer = async () => ({ ok: true, uploaded: 2, failed: 0, results: [{ outcome: 'uploaded' }, { outcome: 'uploaded' }, { outcome: 'already' }] });
  // 本物の registerItem は成功時に registered_at / published_at を書き、last_error を消す。偽物も同じ痕跡を残す
  const okRegister = async (id) => {
    db.prepare(`INSERT INTO draft_rakuten (draft_id, registered_at, published_at, last_error) VALUES (?, '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z', NULL)
                ON CONFLICT(draft_id) DO UPDATE SET registered_at = excluded.registered_at, published_at = excluded.published_at, last_error = NULL`).run(id);
    return { ok: true, manageNumber: 'lst-ok', status: 201, shopCategories: { ok: true, count: 1 } };
  };

  {
    // 成功: 転送 → 登録 → 楽天モール=完了 + 画像工程⑧=完了 + 失敗理由なし
    const id = mkDraft('LST-OK');
    let order = [];
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
      transfer: async (...a) => { order.push('transfer'); return okTransfer(...a); },
      register: async (...a) => { order.push('register'); return okRegister(...a); },
    } });
    check('ボード出品: 成功 = 転送 → 登録 の順で呼ぶ', order.join('>') === 'transfer>register', order.join('>'));
    check('ボード出品: 成功の戻り (stage=done・転送の内訳)',
      r.ok === true && r.stage === 'done' && r.transfer.uploaded === 2 && r.transfer.already === 1 && r.register.manageNumber === 'lst-ok',
      JSON.stringify(r).slice(0, 200));
    // 商品ページ URL は draft_mall_status に保存しない設計 (ensureMallStatus: 商品コードから決まる) なので見ない
    check('ボード出品: 成功で 楽天モール=完了', mallOf(id).state === 'done', JSON.stringify(mallOf(id)));
    check('ボード出品: 成功で 画像工程⑧「楽天登録」=完了', stepOf(id) === 'done', String(stepOf(id)));
    check('ボード出品: 成功で last_error は空', rkOf(id).last_error == null, String(rkOf(id).last_error));
    check('ボード出品: 開始・完了のイベントが残る',
      eventsOf(id).includes('rakuten_board_listing_started') && eventsOf(id).includes('rakuten_board_listing_done'), eventsOf(id).join(','));
    // 登録済みは二度出さない (RMS への PUT は取り消せない)
    let dup = null;
    try { await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: { transfer: okTransfer, register: okRegister } }); } catch (e) { dup = e; }
    check('ボード出品: 登録済みの商品は 400 で拒否', dup?.status === 400 && /登録済み/.test(dup.message), dup?.message);
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // 楽天が「対象外」の商品は出さない (2026-09-10 監査: 完了列から差し戻したカードは対象外の印を
    // 持っておらず、確認で OK を押すと取り消せない PUT まで届いていた。サーバー側でも止める)
    const id = mkDraft('LST-SKIP');
    ms.ensureMallStatus(db, id);
    db.prepare("UPDATE draft_mall_status SET state = 'skip' WHERE draft_id = ? AND mall = 'rakuten'").run(id);
    let skipErr = null;
    let touched = false;
    try {
      await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
        transfer: async () => { touched = true; return okTransfer(); },
        register: async () => { touched = true; return okRegister(id); },
      } });
    } catch (e) { skipErr = e; }
    check('ボード出品: 楽天が対象外の商品は 400 で拒否し、転送も登録も呼ばない',
      skipErr?.status === 400 && /対象外/.test(skipErr.message) && !touched, skipErr?.message || 'no error');
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // 転送で 1 枚失敗 → 登録は呼ばない。理由を last_error に残す (カードに出る)
    const id = mkDraft('LST-TRF');
    let registerCalled = false;
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
      transfer: async () => ({ ok: false, uploaded: 1, failed: 1, results: [{ outcome: 'uploaded' }, { outcome: 'failed', source: 'drive', error: 'Drive 403: not shared' }] }),
      register: async () => { registerCalled = true; return okRegister(id); },
    } });
    check('ボード出品: 転送に失敗したら登録を呼ばない', r.ok === false && r.stage === 'transfer' && !registerCalled, JSON.stringify(r).slice(0, 160));
    check('ボード出品: 転送失敗の理由が last_error に残る (Drive の共有を疑う文言)',
      /転送できませんでした/.test(rkOf(id).last_error || '') && /not shared/.test(rkOf(id).last_error || '') && /共有/.test(rkOf(id).last_error || ''),
      String(rkOf(id).last_error));
    check('ボード出品: 転送失敗では楽天モールは動かない', (mallOf(id).state || 'todo') !== 'done', JSON.stringify(mallOf(id)));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
    // R-Cabinet 側で落ちたときに Drive の共有を疑わせない (#1163: 的外れな案内で
    // 「ファイル名が長すぎる」という本当の理由が埋もれ、調べる方向を間違えた)
    const idCab = mkDraft('LST-TRF-CAB');
    await bl.listToRakutenFromBoard(idCab, { actor: 'smoke', deps: {
      transfer: async () => ({ ok: false, uploaded: 0, failed: 1, results: [
        { outcome: 'failed', source: 'cabinet', error: 'file insert 失敗 path=x-white.jpg (HTTP 400 / resultCode 3001)' },
      ] }),
      register: async () => okRegister(idCab),
    } });
    check('ボード出品: R-Cabinet 側の理由が出ているときは Drive の共有を疑わせない',
      /resultCode 3001/.test(rkOf(idCab).last_error || '') && !/共有/.test(rkOf(idCab).last_error || ''),
      String(rkOf(idCab).last_error));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idCab);
  }
  {
    // 画像が 1 枚も無い
    const id = mkDraft('LST-NOIMG');
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
      transfer: async () => ({ ok: false, error: 'no_images' }), register: okRegister,
    } });
    check('ボード出品: 画像が無ければ転送段階で止まり、理由が残る',
      r.ok === false && r.stage === 'transfer' && /商品画像がありません/.test(rkOf(id).last_error || ''), String(rkOf(id).last_error));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // 登録の前提チェックで止まる (registerItem は reasons を last_error に書かない → ここで残す)
    const id = mkDraft('LST-REASON');
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
      transfer: okTransfer,
      register: async () => ({ ok: false, reasons: ['ジャンルIDが未入力か数字ではありません', '売価が未入力か範囲外です (1〜1億円)'] }),
    } });
    check('ボード出品: 前提チェックの理由が戻り値と last_error の両方に残る',
      r.ok === false && r.stage === 'register' && /ジャンルID/.test(r.error) && /出品の前提が揃っていません/.test(rkOf(id).last_error || '') && /売価/.test(rkOf(id).last_error || ''),
      JSON.stringify({ err: r.error, last: rkOf(id).last_error }).slice(0, 220));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // RMS がエラーを返した (registerItem 自身が last_error を書く → 上書きしない)
    const id = mkDraft('LST-RMS');
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
      transfer: okTransfer,
      register: async () => {
        db.prepare("INSERT INTO draft_rakuten (draft_id, last_error) VALUES (?, 'IE1002: 属性が不正') ON CONFLICT(draft_id) DO UPDATE SET last_error = excluded.last_error").run(id);
        return { ok: false, status: 400, error: 'IE1002: 属性が不正' };
      },
    } });
    check('ボード出品: RMS エラーは registerItem の記録をそのまま残す (二重に書かない)',
      r.ok === false && r.stage === 'register' && rkOf(id).last_error === 'IE1002: 属性が不正', String(rkOf(id).last_error));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // 登録が例外 (warehouse 不通など) → 理由を残して失敗で返す (throw しない = 画面が固まらない)
    const id = mkDraft('LST-THROW');
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
      transfer: okTransfer, register: async () => { throw new Error('WAREHOUSE_URL not configured on Render (fail-closed)'); },
    } });
    check('ボード出品: 登録の例外は失敗として返り、理由が残る',
      r.ok === false && r.stage === 'register' && /楽天への登録でエラー/.test(rkOf(id).last_error || '') && /WAREHOUSE_URL/.test(rkOf(id).last_error || ''),
      String(rkOf(id).last_error));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // 実行中の二重起動は 409 (連打・別タブ)。終わればフラグは消える
    const id = mkDraft('LST-INFLIGHT');
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const first = bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: {
      transfer: async () => { await gate; return okTransfer(); }, register: okRegister,
    } });
    await new Promise((resolve) => setTimeout(resolve, 10));
    let second = null;
    try { await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: { transfer: okTransfer, register: okRegister } }); } catch (e) { second = e; }
    check('ボード出品: 実行中に同じ商品をもう一度は 409', second?.status === 409 && bl.isRakutenListingInFlight(id), second?.message);
    release();
    const r1 = await first;
    check('ボード出品: 先の実行は最後まで通り、フラグが消える', r1.ok === true && !bl.isRakutenListingInFlight(id), JSON.stringify(r1).slice(0, 120));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // HTTP: confirm 必須 / 登録済みは 400 / 存在しない商品は 404 (外部には出ない経路だけ叩く)
    const express = (await import('express')).default;
    const routerMod = await import('../router.js');
    const app = express();
    let sessionRole = 'staff';
    app.use((req, res, next) => { req.session = { email: 'smoke@b-faith.biz', displayName: 'smoke', role: sessionRole }; next(); });
    app.use('/ph', routerMod.default);
    const server = app.listen(0);
    const base = `http://127.0.0.1:${server.address().port}/ph`;
    const call = async (p, body) => {
      const res = await fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      return { status: res.status, json: await res.json() };
    };
    const id = mkDraft('LST-HTTP');
    let r = await call(`/api/drafts/${id}/rakuten/list-from-board`, {});
    check('HTTP ボード出品: confirm が無ければ 400 (何もしない)', r.status === 400 && /confirm/.test(r.json.error || ''), JSON.stringify(r.json));
    db.prepare("INSERT INTO draft_rakuten (draft_id, registered_at) VALUES (?, '2026-09-01T00:00:00Z')").run(id);
    r = await call(`/api/drafts/${id}/rakuten/list-from-board`, { confirm: true });
    check('HTTP ボード出品: 登録済みは 400 で理由を返す', r.status === 400 && /登録済み/.test(r.json.error || ''), JSON.stringify(r.json));
    r = await call('/api/drafts/999999999/rakuten/list-from-board', { confirm: true });
    check('HTTP ボード出品: 存在しない商品は 404', r.status === 404, String(r.status));
    // 結果不明の再実行 (force_unknown) は管理者だけ。一般ユーザーは 403 で何も起きない
    const idU = mkDraft('LST-HTTP-FORCE');
    db.prepare("INSERT INTO draft_rakuten (draft_id, listing_outcome, last_error) VALUES (?, 'unknown', 'x')").run(idU);
    r = await call(`/api/drafts/${idU}/rakuten/list-from-board`, { confirm: true, force_unknown: true });
    check('HTTP ボード出品: 結果不明の再実行を一般ユーザーが頼むと 403', r.status === 403, JSON.stringify(r.json));
    r = await call(`/api/drafts/${idU}/rakuten/list-from-board`, { confirm: true });
    check('HTTP ボード出品: 結果不明の商品は force なしだと 400 (RMS で確認、の案内)',
      r.status === 400 && /RMS/.test(r.json.error || '') && /lst-http-force/.test(r.json.error || ''), JSON.stringify(r.json));
    // 詳細画面の「公開で登録」もロックを通る (Codex R1 critical): ボードで実行中は 409
    const idL = mkDraft('LST-HTTP-LOCK');
    const releaseL = bl.acquireRakutenListingLock(idL);
    r = await call(`/api/drafts/${idL}/rakuten/register`, { confirm: true });
    check('HTTP 詳細画面の「公開で登録」: ボードで出品中の商品は 409 (同じロック)', r.status === 409, JSON.stringify(r.json));
    releaseL();
    // 詳細画面の「公開で登録」も 結果不明 / 登録済み を通さない (Codex R2 critical: ここが素通りだと
    // 「結果不明は管理者だけが再実行」を詳細画面から迂回できる)
    r = await call(`/api/drafts/${idU}/rakuten/register`, { confirm: true });
    check('HTTP 詳細画面の「公開で登録」: 結果不明の商品は 400 (RMS で確認、の案内)',
      r.status === 400 && /RMS/.test(r.json.error || ''), JSON.stringify(r.json));
    r = await call(`/api/drafts/${id}/rakuten/register`, { confirm: true });
    check('HTTP 詳細画面の「公開で登録」: 登録済みの商品は 400', r.status === 400 && /登録済み/.test(r.json.error || ''), JSON.stringify(r.json));
    // 途中で止まった (running・実行中でない) も詳細画面から通さない (Codex R3: ロックを取ってから判定すると
    // inFlight が自分自身になって「いま動いている」と誤認し、素通りしていた)
    const idS = mkDraft('LST-HTTP-STUCK');
    db.prepare("INSERT INTO draft_rakuten (draft_id, listing_outcome, listing_attempt_at) VALUES (?, 'running', '2026-08-01T00:00:00Z')").run(idS);
    r = await call(`/api/drafts/${idS}/rakuten/register`, { confirm: true });
    check('HTTP 詳細画面の「公開で登録」: 途中で止まった商品は 400 (ロックを取る前に判定)',
      r.status === 400 && /途中で止まって/.test(r.json.error || '') && !bl.isRakutenListingInFlight(idS), JSON.stringify(r.json));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idS);

    // ─── セット展開判断 (2026-09-04 §4.2) ────────────────────────────────
    // ⑤は「判断を記録してから」でないと閉じられない。派生を作ったときだけ閉じる作りでは
    // 「まだ検討していない」と「作らないと決めた」が区別できなかった
    {
      const idDec = mkDraft('SETDEC-HTTP', { atListing: false });
      sessionRole = 'admin';   // ⑤を閉じるのは担当者か管理者 (smoke のアカウントは担当者に紐づかない)
      const stepVer = () => wfp.progressOf(idDec, { db }).main.find((x) => x.step_code === 'set_review').version;
      let rr = await call(`/api/drafts/${idDec}/steps/set_review`, { state: 'done', expected_version: stepVer() });
      check('HTTP セット判断: 判断を記録せずに⑤は閉じられない',
        rr.status === 400 && /判断を先に記録/.test(rr.json.error || ''), JSON.stringify(rr.json));
      rr = await call(`/api/drafts/${idDec}/set-decision`, { decision: 'none', expected_version: stepVer() });
      check('HTTP セット判断: 「作らない」は理由を選ばないと 400', rr.status === 400, JSON.stringify(rr.json));
      check('HTTP セット判断: 弾かれたときは判断の履歴も残さない (記録と工程更新は同じトランザクション)',
        db.prepare('SELECT COUNT(*) c FROM draft_set_decisions WHERE draft_id = ?').get(idDec).c === 0);
      // 「新規作成」は作成の入口だけが記録する (派生を作らずに⑤を閉じられないように)
      rr = await call(`/api/drafts/${idDec}/set-decision`, { decision: 'create', expected_version: stepVer() });
      check('HTTP セット判断: 「新規作成」はこの口では受けない', rr.status === 400 && /セット商品を作る/.test(rr.json.error || ''), JSON.stringify(rr.json));
      rr = await call(`/api/drafts/${idDec}/set-decision`, { decision: 'hold', reason_text: '売れ行きを見てから', expected_version: stepVer() });
      check('HTTP セット判断: 「保留」は記録できるが⑤は開いたまま (滞留を数え続ける)',
        rr.status === 200 && rr.json.closed === false
        && wfp.progressOf(idDec, { db }).main.find((x) => x.step_code === 'set_review').state !== 'done',
        JSON.stringify(rr.json));
      rr = await call(`/api/drafts/${idDec}/set-decision`, { decision: 'none', reason_code: 'low_demand', expected_version: stepVer() });
      check('HTTP セット判断: 「作らない」を記録すると⑤が閉じる',
        rr.status === 200 && rr.json.closed === true
        && wfp.progressOf(idDec, { db }).main.find((x) => x.step_code === 'set_review').state === 'done',
        JSON.stringify(rr.json));
      check('HTTP セット判断: 判断は履歴として残る (保留 → 作らない の 2 件)',
        db.prepare('SELECT COUNT(*) c FROM draft_set_decisions WHERE draft_id = ?').get(idDec).c === 2);
      // 🚨 判断は状態が変わらなくても版数を消費する。古い画面から続けて送れてはいけない (Codex R2)
      const idCas = mkDraft('SETDEC-CAS', { atListing: false });
      const casVer = wfp.progressOf(idCas, { db }).main.find((x) => x.step_code === 'set_review').version;
      const first = await call(`/api/drafts/${idCas}/set-decision`, { decision: 'hold', reason_text: '1回目', expected_version: casVer });
      const second = await call(`/api/drafts/${idCas}/set-decision`, { decision: 'hold', reason_text: '2回目', expected_version: casVer });
      check('HTTP セット判断: 同じ版数で続けて送ると 2 回目は 409 (状態が変わらなくても版数を消費する)',
        first.status === 200 && second.status === 409, JSON.stringify({ first: first.status, second: second.status, msg: second.json?.error }));
      check('HTTP セット判断: 弾かれた 2 回目は履歴に残らない',
        db.prepare('SELECT COUNT(*) c FROM draft_set_decisions WHERE draft_id = ?').get(idCas).c === 1);
      db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idCas);
      sessionRole = 'staff';
      db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idDec);
    }

    // ─── セット作成を実物のルートで通す (2026-09-04 §5.6 / Codex 2巡目) ──────
    // 画面 → API → DB の往復。ここを通さないと「関数は正しいがルートが受け取れていない」が残る
    {
      sessionRole = 'admin';
      const idSetHttp = mkDraft('SETHTTP-P', { atListing: false });
      db.prepare('UPDATE product_drafts SET price = 1200 WHERE id = ?').run(idSetHttp);
      db.prepare(`INSERT OR REPLACE INTO mirror_products
        (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 標準売価, 原価, 送料, 配送方法, 消費税率, updated_at)
        VALUES (99720, 'sethttp-mix', '混ぜる商品', '1', '取扱中', 'ok', 300, 100, 120, '定形外', 0.1, '2026-09-04T00:00:00Z')`).run();
      const ver = () => wfp.progressOf(idSetHttp, { db }).main.find((x) => x.step_code === 'set_review').version;
      let rs = await call(`/api/drafts/${idSetHttp}/set-drafts`,
        { mode: 'ai', members: [], parent_step_version: ver() });
      check('HTTP セット作成: 構成が空なら 400 (黙って「親×2」にしない)',
        rs.status === 400 && /構成/.test(rs.json.error || ''), JSON.stringify(rs.json));
      rs = await call(`/api/drafts/${idSetHttp}/set-drafts`,
        { mode: 'ai', members: [{ ne_code: 'SETHTTP-P', qty: 2 }, { ne_code: 'sethttp-mix', qty: 1 }], parent_step_version: ver() });
      check('HTTP セット作成: 複数行の構成をそのまま受け取る', rs.status === 200 && !!rs.json.draftId, JSON.stringify(rs.json));
      const madeId = rs.json.draftId;
      check('HTTP セット作成: 構成が順番どおり保存される',
        db.prepare('SELECT member_ne_code, qty FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort')
          .all(madeId).map((x) => `${x.member_ne_code}x${x.qty}`).join(',') === 'SETHTTP-Px2,sethttp-mixx1',
        JSON.stringify(db.prepare('SELECT member_ne_code, qty FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort').all(madeId)));
      check('HTTP セット作成: 売価は構成の和が入る (1,200×2 + 300×1 = 2,700)',
        db.prepare('SELECT price FROM product_drafts WHERE id = ?').get(madeId).price === 2700,
        String(db.prepare('SELECT price FROM product_drafts WHERE id = ?').get(madeId).price));
      check('HTTP セット作成: 配送方法は入らない (NE 確定待ち)',
        (db.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(madeId) || {}).shipping_method_group == null);
      rs = await call(`/api/drafts/${idSetHttp}/set-drafts`,
        { mode: 'ai', members: [{ ne_code: 'SETHTTP-P', qty: 0 }], parent_step_version: ver() });
      check('HTTP セット作成: 個数が不正なら 400', rs.status === 400, JSON.stringify(rs.json));
      db.prepare('DELETE FROM mirror_products WHERE product_id = 99720').run();
      db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(madeId, idSetHttp);
      sessionRole = 'staff';
    }

    // ─── 配送方法の保存の往復 (2026-09-03 #1152 / Codex R1 high) ───────────
    // 画面 → 保存 → 再表示 で状態が落ちないこと。DB には楽天IDだけを入れ、
    // 「ヤフーだけ別配送」は draft_yahoo.shipping_override が持つ
    {
      const rkOf = (d) => db.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(d);
      const yhOf = (d) => db.prepare('SELECT delivery_label, shipping_override FROM draft_yahoo WHERE draft_id = ?').get(d);
      const idShip = mkDraft('SHIP-HTTP');
      // ① 複合選択肢を保存 → 楽天は '1' (定形外)、ヤフーは別扱い + 既定の配送方法
      let rr = await call(`/api/drafts/${idShip}/rakuten`, { shipping_method_group: '1y5', yahoo_shipping_override: 1 });
      check('HTTP 配送方法: 複合(1y5)は 楽天ID + ヤフー別扱い に分けて保存する',
        rr.status === 200 && rkOf(idShip).shipping_method_group === '1'
        && yhOf(idShip).shipping_override === 1 && yhOf(idShip).delivery_label === 'ネコポス',
        JSON.stringify({ rk: rkOf(idShip), yh: yhOf(idShip), res: rr.json }));
      check('HTTP 配送方法: 画面に戻すときは複合キーへ逆引きする',
        shipGroups.shippingSelectValueOf(rkOf(idShip).shipping_method_group, yhOf(idShip)) === '1y5');
      // ② プリセットの変更 (1y5 → 1y8) はヤフーの配送方法にも反映される
      await call(`/api/drafts/${idShip}/rakuten`, { shipping_method_group: '1y8', shipping_method_group_prev: '1y5', yahoo_shipping_override: 1 });
      check('HTTP 配送方法: 複合を選び直すとヤフーの配送方法も追従する (1y5→1y8)',
        yhOf(idShip).delivery_label === '宅急便50サイズ以下' && rkOf(idShip).shipping_method_group === '1',
        JSON.stringify(yhOf(idShip)));
      // ③ ヤフーの配送方法を手で変えたあと、**プルダウンを触らずに**楽天項目を保存しても
      //    手で入れた値は戻らない (画面はまだ 1y8 を指している = 選び直していない)
      await call(`/api/drafts/${idShip}/yahoo`, { delivery_label: '宅急便', shipping_override: 1 });
      await call(`/api/drafts/${idShip}/rakuten`, { shipping_method_group: '1y8', shipping_method_group_prev: '1y8', yahoo_shipping_override: 1 });
      check('HTTP 配送方法: 複合を選び直していなければヤフーの配送方法は書き換えない',
        yhOf(idShip).delivery_label === '宅急便' && yhOf(idShip).shipping_override === 1,
        JSON.stringify(yhOf(idShip)));
      // ④ 再表示すると複合キーへ逆引きできない (定形外表示) が、その状態で保存しても別扱いは残る
      rr = await call(`/api/drafts/${idShip}/rakuten`, { shipping_method_group: '1', shipping_method_group_prev: '1', yahoo_shipping_override: 1 });
      check('HTTP 配送方法: ヤフーの配送方法を変えた商品を楽天保存しても別扱いは残る',
        yhOf(idShip).shipping_override === 1 && yhOf(idShip).delivery_label === '宅急便',
        JSON.stringify(yhOf(idShip)));
      // ⑤ 通常の配送方法へ戻す (画面のゲートも閉じる) = 別扱いの明示的な解除
      await call(`/api/drafts/${idShip}/rakuten`, { shipping_method_group: '5', shipping_method_group_prev: '1', yahoo_shipping_override: 0 });
      check('HTTP 配送方法: 通常の配送方法に戻すと別扱いが解除される (ヤフーの値は残す)',
        rkOf(idShip).shipping_method_group === '5' && yhOf(idShip).shipping_override === 0
        && yhOf(idShip).delivery_label === '宅急便', JSON.stringify(yhOf(idShip)));
      // ⑥ ヤフー項目を先に保存しても別扱いが立つ (保存の順序に依存しない)
      const idShip2 = mkDraft('SHIP-HTTP-2');
      await call(`/api/drafts/${idShip2}/yahoo`, { delivery_label: 'ネコポス', shipping_override: 1 });
      check('HTTP 配送方法: ヤフー項目を先に保存しても別扱いが記録される',
        yhOf(idShip2).shipping_override === 1, JSON.stringify(yhOf(idShip2)));
      // ⑦ 選択肢から外した配送方法 (「現在使用不可」) が保存済みでも、触らず保存すれば残る。
      //    弾くとその商品は出品できなくなり、黙って別の値にすると配送方法が入れ替わる (Codex R2/R3)
      db.prepare('UPDATE draft_rakuten SET shipping_method_group = ? WHERE draft_id = ?').run('2', idShip);
      rr = await call(`/api/drafts/${idShip}/rakuten`, { shipping_method_group: '2', shipping_method_group_prev: '2' });
      check('HTTP 配送方法: 選択肢から外した値も、保存済みなら弾かず残す',
        rr.status === 200 && rkOf(idShip).shipping_method_group === '2',
        JSON.stringify({ status: rr.status, rk: rkOf(idShip), res: rr.json }));
      // ⑧ 選択肢に無い値は 400 (保存しない)
      rr = await call(`/api/drafts/${idShip2}/rakuten`, { shipping_method_group: '1y9' });
      check('HTTP 配送方法: 選択肢に無い値は 400', rr.status === 400 && /配送方法/.test(rr.json.error || ''), JSON.stringify(rr.json));
      db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(idShip, idShip2);
    }

    // ─── 画面が本当に描けるか (2026-09-04 本番 500 の再発防止) ──────────────
    // 🚨 テンプレートの render テストは変数を**自前で**用意するので、router 側の渡し忘れを
    // 素通りさせる。実際 #1176 で detail.ejs に setDecisionReasons を使う枠を足したのに
    // router の res.render に足し忘れ、smoke は ALL PASS のまま本番の詳細画面が
    // ReferenceError で 500 になった (board.ejs には渡っていた)。
    // テンプレートが使う変数を router が全部渡しているかは、実物のルートを叩くまで分からない。
    // ⚠️ 実物のルートなので副作用も本物: `/board` と `/board?view=ne` は reconcileConfirmableSets を
    //    通り、この DB にある**他の**仮コードのセットも (NE mirror に本コードがあれば) 確定させる。
    //    ここで作る SET-PAGE-SINGLE-01 は `SET-` のままなので確定されないが、
    //    「仮のままであること」に依存するテストをこの後ろに足さないこと (Codex low 2026-09-04)。
    {
      const getHtml = async (p) => {
        const res = await fetch(base + p, { headers: { Accept: 'text/html' } });
        return { status: res.status, html: await res.text() };
      };
      const idPage = mkDraft('PAGE-SINGLE', { atListing: false });
      // セット (parent_draft_id がセットの定義。仮コードのまま = NE 登録待ちの見た目も描かせる)
      const idSet = Number(db.prepare(
        `INSERT INTO product_drafts (ne_code, name, status, created_by, parent_draft_id, provisional_code, ne_registration_state)
         VALUES ('SET-PAGE-SINGLE-01', 'ページ描画テスト 2個セット', 'draft', 'smoke', ?, 1, 'requested')`,
      ).run(idPage).lastInsertRowid);
      db.prepare('INSERT INTO draft_set_members (set_draft_id, member_ne_code, qty, sort) VALUES (?, ?, 2, 0)')
        .run(idSet, 'PAGE-SINGLE');
      wfp.ensureProgress(db, idSet);

      let pr = await getHtml(`/detail/${idPage}`);
      check('HTTP 画面: 単品の詳細が 200 で描ける (テンプレートの変数を router が全部渡している)',
        pr.status === 200, `${pr.status} ${pr.html.slice(0, 400)}`);
      check('HTTP 画面: ⑤の「作らない」理由の選択肢が実際に描かれる (setDecisionReasons の渡し忘れ検出)',
        Object.values(sd.SET_DECISION_REASONS).every((label) => pr.html.includes(label)),
        Object.keys(sd.SET_DECISION_REASONS).join(','));
      check('HTTP 画面: 親のカードに派生セットが出る', pr.html.includes('SET-PAGE-SINGLE-01'));
      // その他注意事項 (2026-09-13)。自動保存の JS がこの id を読むので、欄が無いと保存のたびに落ちる
      check('HTTP 画面: 商品情報タブに その他注意事項 の欄がある', pr.html.includes('id="pi-other-notes"'));
      // TOP画像の構成 (2026-09-13)。画像制作の保存 JS がこの 2 つの id を読む (全商品に出る画像制作カードの中)
      check('HTTP 画面: 画像制作カードに TOP画像の構成 と 参考・ラフの URL の欄がある',
        pr.html.includes('id="ip-top-compose"') && pr.html.includes('id="ip-top-ref-url"'));

      // 画像ビューは担当者の絞り込みを効かせない (2026-09-13)。存在しない担当者 ID で絞ると全体ビューは 0 件、
      // 画像ビューは絞られずにカードが出る (全体ビューから切り替えて URL に残っていても効かない)
      {
        const countOf = (html) => Number((html.match(/<span class="muted">(\d+) 件/) || [])[1] ?? NaN);
        const prMain = await getHtml('/board?assignee=99999999');
        const prImg = await getHtml('/board?view=image&assignee=99999999&filter=unassigned');
        check('HTTP ボード: 画像ビューでは URL に担当者・未割り当ての絞り込みが残っていても効かせない',
          prMain.status === 200 && prImg.status === 200 && countOf(prMain.html) === 0 && countOf(prImg.html) > 0
          && !prImg.html.includes('id="assignee-select"'),
          `main=${prMain.status}/${countOf(prMain.html)} image=${prImg.status}/${countOf(prImg.html)}`);
      }

      // 初動判定の定型文のカラバリ (2026-09-13)。lib 単体のテストは router が variation / 選択肢の値 /
      // 項目名を渡し忘れても通るので、実物の画面に埋め込まれた定型文で見る
      {
        const insVar = db.prepare(`INSERT OR REPLACE INTO mirror_products
          (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
          VALUES (?, ?, ?, '1', '取扱中', 'ok', 'PJ-VAR', '2026-09-13T00:00:00Z')`);
        insVar.run(991301, 'PJ-VAR-RED', 'PJ商品 レッド');
        insVar.run(991302, 'PJ-VAR-BLU', 'PJ商品 ブルー');
        const idPj = Number(db.prepare(
          `INSERT INTO product_drafts (ne_code, name, created_by, own_brand) VALUES ('PJ-VAR', '初動判定カラバリ', 'smoke', 1)`,
        ).run().lastInsertRowid);
        wfp.ensureProgress(db, idPj);
        db.prepare('INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)').run(idPj, '説明文');
        db.prepare('INSERT INTO draft_rakuten (draft_id, variant_selector_name) VALUES (?, ?)').run(idPj, 'カラー');
        // 画面の保存と同じく小文字キー (router の sku-selector-values)
        db.prepare('INSERT INTO draft_sku_selector_values (draft_id, sku_code, value) VALUES (?, ?, ?)').run(idPj, 'pj-var-red', 'レッド');
        const prPj = await getHtml(`/detail/${idPj}`);
        const tplJson = prPj.html.match(/<script type="application\/json" id="prompt-templates-json">([\s\S]*?)<\/script>/);
        let tplPj = null;
        try { tplPj = JSON.parse(tplJson?.[1] || 'null'); } catch (_) { /* 下の check で落とす */ }
        check('HTTP 画面: 初動判定の定型文にカラバリが入る (NE の SKU 順・選択肢の値 → NE 商品名)',
          prPj.status === 200 && !!tplPj?.initialJudge?.includes('■カラバリ (カラー・全2種)\n・PJ商品 ブルー\n・レッド'),
          `${prPj.status} ${tplPj?.initialJudge || prPj.html.slice(0, 300)}`);
        check('HTTP 画面: 商品分析の定型文にも同じカラバリが入る',
          !!tplPj?.productAnalysis?.includes('■カラバリ (カラー・全2種)\n・PJ商品 ブルー\n・レッド'),
          tplPj?.productAnalysis || '');
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idPj);
        db.prepare('DELETE FROM mirror_products WHERE product_id IN (991301, 991302)').run();
      }

      // 仕入商品 (重要度：低) でも画像制作カードと簡易LPの定型文が出る (2026-09-13 全商品に出す)。
      // 保存・保留 API が重要度で弾かないことは「裏面情報 + ChatGPT 定型文」のブロックで見る
      // (このブロックのセッションは画像担当の役割を持たないので 403 になる)
      {
        const idLow = Number(db.prepare(
          `INSERT INTO product_drafts (ne_code, name, created_by, own_brand, image_priority)
           VALUES ('PJ-LOW', '仕入れ低の商品', 'smoke', 0, '仕入商品（重要度：低）')`,
        ).run().lastInsertRowid);
        wfp.ensureProgress(db, idLow);
        db.prepare('INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)').run(idLow, '仕入れ低の説明文');
        const prLow = await getHtml(`/detail/${idLow}`);
        let tplLow = null;
        try { tplLow = JSON.parse(prLow.html.match(/<script type="application\/json" id="prompt-templates-json">([\s\S]*?)<\/script>/)?.[1] || 'null'); } catch (_) { /* 下の check で落とす */ }
        check('HTTP 画面: 仕入商品でも画像制作カードと簡易LPの定型文が出る',
          prLow.status === 200 && prLow.html.includes('id="ip-product-info"') && prLow.html.includes('data-kind="simpleLp"')
          && !!tplLow?.simpleLp?.includes('商品情報：\n■商品情報\n仕入れ低の説明文\n\n■カラバリ\nなし (単品)'),
          `${prLow.status} ${tplLow?.simpleLp || prLow.html.slice(0, 300)}`);
        // 画像の重要度 (2026-09-17)。router が draft の重要度を簡易LPに渡し、画面は保存済みの値を data-saved で持つ
        check('HTTP 画面: 簡易LPの定型文に基本情報の画像の重要度が入り、重要度の選択欄が保存済みの値を持つ',
          !!tplLow?.simpleLp?.includes('【入力】\n\n画像の重要度：仕入商品（重要度：低）\n\n商品情報：')
          && tplLow?.simpleLpPriority?.line === '画像の重要度：仕入商品（重要度：低）'
          && /<select id="f-image-priority" data-saved="仕入商品（重要度：低）"/.test(prLow.html),
          `${tplLow?.simpleLp || ''} ${prLow.html.match(/<select id="f-image-priority"[^>]*>/)?.[0] || '(select なし)'}`);
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idLow);
      }

      // 画像タブの商品情報の自動表示 (2026-09-13)。router が自動の説明文を画面と定型文の両方に渡すこと
      {
        const tplOf = (html) => {
          try { return JSON.parse(html.match(/<script type="application\/json" id="prompt-templates-json">([\s\S]*?)<\/script>/)?.[1] || 'null'); } catch (_) { return null; }
        };
        const idAi = Number(db.prepare(
          `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('PJ-AUTO-INFO', '自動商品情報の画面', 'smoke')`,
        ).run().lastInsertRowid);
        wfp.ensureProgress(db, idAi);
        db.prepare("INSERT INTO draft_ai_outputs (draft_id, kind, content) VALUES (?, 'desc_features', '画面で見える説明文')").run(idAi);
        let pa = await getHtml(`/detail/${idAi}`);
        check('HTTP 画面: 商品情報が空なら自動の説明文を data-auto=1 で出し、定型文にも入る',
          pa.status === 200 && /<textarea id="ip-product-info"[^>]*data-auto="1"[^>]*>説明：画面で見える説明文<\/textarea>/.test(pa.html)
          && !!tplOf(pa.html)?.initialJudge?.includes('■商品情報\n説明：画面で見える説明文'),
          `${pa.status} ${(pa.html.match(/<textarea id="ip-product-info"[\s\S]{0,300}/) || [''])[0]}`);
        db.prepare("INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, '人が直した商品情報')").run(idAi);
        pa = await getHtml(`/detail/${idAi}`);
        check('HTTP 画面: 手入力があればそちらを data-auto=0 で出し、「自動に戻す」を出す (定型文も手入力)',
          /<textarea id="ip-product-info"[^>]*data-auto="0"[^>]*>人が直した商品情報<\/textarea>/.test(pa.html)
          && pa.html.includes('id="ip-info-auto-btn"')
          && !!tplOf(pa.html)?.initialJudge?.includes('■商品情報\n人が直した商品情報')
          && !tplOf(pa.html)?.initialJudge?.includes('画面で見える説明文'),
          (pa.html.match(/<textarea id="ip-product-info"[\s\S]{0,300}/) || [''])[0]);
        db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idAi);
      }

      pr = await getHtml(`/detail/${idSet}`);
      check('HTTP 画面: セットの詳細が 200 で描ける', pr.status === 200, `${pr.status} ${pr.html.slice(0, 400)}`);
      {
        // 🚨 200 で返ってきても、画面の JS が構文エラーなら**ボタンが 1 つも効かない**。
        //    セット商品では set_review が無く、埋め込みが空文字になって実際にそうなっていた
        //    (2026-09-07)。実ルートの HTML でも構文を見る
        const vmMod = await import('node:vm');
        const r = checkInlineScriptSyntax(vmMod, pr.html, 'detail(set)');
        check('HTTP 画面: セットの詳細の JS が構文として通る (壊れていると画面の操作が全部死ぬ)',
          r.ok, r.detail);
        // 🚨 上の検査は「script が 1 つも無いページ」を通す (それが正しい)。だからここでは
        //    **事故が起きた当のコードが載っていること**を別に確かめる。載らなくなったら、
        //    緑のまま検査対象だけが消える (Codex R2)
        check('HTTP 画面: セットの詳細に事故が起きた当のコードが載っている (検査対象が消えていないこと)',
          inlineScriptsOf(pr.html).some((s) => s.code.includes('parent_step_version')),
          `${inlineScriptsOf(pr.html).length} 個の script`);
        // この画面が「事故の条件」を持ち続けていることを確かめる。持たなくなったら上の検査は
        // 別物を見ていることになる (fixture が変わって再発を素通りさせないため)
        const setMain = wfp.progressOf(idSet, { db }).main;
        check('HTTP 画面: セット商品の工程には set_review が無い (この検査が意味を持つ条件)',
          setMain.length > 0 && !setMain.some((s) => s.step_code === 'set_review'),
          setMain.map((s) => s.step_code).join(','));
      }

      for (const [label, p] of [
        ['一覧', '/list'],
        ['ボード (全体)', '/board'],
        ['ボード (単品)', '/board?view=single'],
        ['ボード (セット工程)', '/board?view=set'],
        ['ボード (画像)', '/board?view=image'],
        ['ボード (NE要対応)', '/board?view=ne'],
        ['新規作成', '/new'],
        ['担当者・工程の設定', '/staff'],
      ]) {
        // eslint-disable-next-line no-await-in-loop
        const rp = await getHtml(p);
        check(`HTTP 画面: ${label} が 200 で描ける (${p})`, rp.status === 200, `${rp.status} ${rp.html.slice(0, 400)}`);
      }
      db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(idSet, idPage);
    }
    server.close();
    db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?, ?)').run(id, idU, idL);
  }
  {
    // 🚨 PUT の結果が確認できなかった (RMS_OUTCOME_UNKNOWN) は「失敗」ではなく「結果不明」で止める。
    // 実は登録が通っている可能性があるので、やり直し可 (retryable) にしてはいけない (Codex R1 critical)
    const id = mkDraft('LST-UNKNOWN');
    const unknownErr = Object.assign(new Error('楽天への登録結果が確認できませんでした (fetch failed)。RMS で商品管理番号 lst-unknown の有無を確認してから再実行してください'), { code: 'RMS_OUTCOME_UNKNOWN' });
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: { transfer: okTransfer, register: async () => { throw unknownErr; } } });
    const rk = db.prepare('SELECT listing_outcome, last_error FROM draft_rakuten WHERE draft_id = ?').get(id);
    check('ボード出品: 結果不明は outcome=unknown・retryable=false で返る',
      r.ok === false && r.outcome === 'unknown' && r.retryable === false && /確認できませんでした/.test(r.error), JSON.stringify(r).slice(0, 200));
    check('ボード出品: 結果不明は DB に unknown と原文が残る',
      rk.listing_outcome === 'unknown' && /lst-unknown/.test(rk.last_error || ''), JSON.stringify(rk));
    check('ボード出品: 結果不明の商品は force 無しでは実行できない (400)', await (async () => {
      try { await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: { transfer: okTransfer, register: okRegister } }); return false; }
      catch (e) { return e.status === 400 && /RMS/.test(e.message); }
    })());
    // 人が RMS で未登録を確認 → 管理者が force で再実行 → 通る
    const r2 = await bl.listToRakutenFromBoard(id, { actor: 'admin', forceUnknown: true, deps: { transfer: okTransfer, register: okRegister } });
    const rk2 = db.prepare('SELECT listing_outcome, registered_at FROM draft_rakuten WHERE draft_id = ?').get(id);
    check('ボード出品: force で再実行すると通り、outcome は消える', r2.ok === true && rk2.listing_outcome == null && !!rk2.registered_at, JSON.stringify({ r2: r2.ok, rk2 }));
    check('ボード出品: 開始時に前回の last_error を消してから走る (古い理由が残らない)',
      db.prepare('SELECT last_error FROM draft_rakuten WHERE draft_id = ?').get(id).last_error == null);
    check('ボード出品: 結果不明のイベントが残る', eventsOf(id).includes('rakuten_listing_outcome_unknown'), eventsOf(id).join(','));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // サーバー側の状態チェック (confirm は誤操作防止であって認可ではない — Codex R1 high)
    const idStep = mkDraft('LST-NOTYET', { atListing: false });
    let e1 = null;
    try { await bl.listToRakutenFromBoard(idStep, { actor: 'smoke', deps: { transfer: okTransfer, register: okRegister } }); } catch (e) { e1 = e; }
    check('ボード出品: 本流が「出品・展開」まで来ていない商品は 400 (いまの工程名つき)',
      e1?.status === 400 && /出品・展開/.test(e1.message) && /基本情報入力/.test(e1.message), e1?.message);
    const idHold = mkDraft('LST-HOLD', { status: 'on_hold' });
    let e2 = null;
    try { await bl.listToRakutenFromBoard(idHold, { actor: 'smoke', deps: { transfer: okTransfer, register: okRegister } }); } catch (e) { e2 = e; }
    check('ボード出品: 保留中の商品は 400', e2?.status === 400 && /保留/.test(e2.message), e2?.message);
    check('ボード出品: 弾かれた商品には試行の痕跡を残さない (running のまま残らない)',
      !db.prepare('SELECT 1 FROM draft_rakuten WHERE draft_id IN (?, ?) AND listing_outcome IS NOT NULL').get(idStep, idHold));
    db.prepare('DELETE FROM product_drafts WHERE id IN (?, ?)').run(idStep, idHold);
    // 全工程が決着している商品 (楽天は完了か対象外で決着済み) も通さない (Codex R2 high)
    const idDone = mkDraft('LST-ALLDONE');
    db.prepare("UPDATE draft_step_progress SET state = 'done' WHERE draft_id = ? AND step_code = 'listing'").run(idDone);
    let e3 = null;
    try { await bl.listToRakutenFromBoard(idDone, { actor: 'smoke', deps: { transfer: okTransfer, register: okRegister } }); } catch (e) { e3 = e; }
    check('ボード出品: 本流が全部完了している商品は 400 (出し直しはモール別状況を戻してから)',
      e3?.status === 400 && /完了しています/.test(e3.message), e3?.message);
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idDone);
    // 🚨 running のまま実行中でない = 途中で落ちた。PUT 成功直後に落ちた可能性があるので unknown と同じ扱い
    // (Codex R2 critical: 「15 分経ったからやり直せる」にすると二重登録になり得る)
    const idStuck = mkDraft('LST-STUCK');
    db.prepare("INSERT INTO draft_rakuten (draft_id, listing_outcome, listing_attempt_at) VALUES (?, 'running', '2026-08-01T00:00:00Z')").run(idStuck);
    let e4 = null;
    try { await bl.listToRakutenFromBoard(idStuck, { actor: 'smoke', deps: { transfer: okTransfer, register: okRegister } }); } catch (e) { e4 = e; }
    check('ボード出品: 途中で止まった (running・実行中でない) 商品は 400 = 結果不明と同じ扱い',
      e4?.status === 400 && /途中で止まって/.test(e4.message) && /RMS/.test(e4.message), e4?.message);
    const r4 = await bl.listToRakutenFromBoard(idStuck, { actor: 'admin', forceUnknown: true, deps: { transfer: okTransfer, register: okRegister } });
    check('ボード出品: 途中で止まった商品も管理者の force なら再実行できる', r4.ok === true, JSON.stringify(r4).slice(0, 120));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(idStuck);
  }
  {
    // 後処理 (afterRakutenRegistered): 成功の形と、対象外 (skip) の工程⑧を上書きしないこと
    const id = mkDraft('LST-POST');
    wfp.setStepState(id, 'imgd_rakuten', { state: 'skip' }, 'smoke', ADMIN);
    const post = bl.afterRakutenRegistered(db, { id, ne_code: 'LST-POST', name: 'x' }, 'smoke');
    check('後処理: モール=完了・工程⑧はそのまま (対象外を上書きしない)',
      post.mallOk === true && post.stepOk === true && stepOf(id) === 'skip' && mallOf(id).state === 'done', JSON.stringify({ post, step: stepOf(id) }));
    check('後処理: 成功時は失敗イベントを残さない', !eventsOf(id).includes('rakuten_postprocess_failed'));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
  {
    // ロックは解放関数を二度呼んでも安全 / 例外でも解放される
    const id = mkDraft('LST-LOCK');
    const rel = bl.acquireRakutenListingLock(id);
    let dup = null;
    try { bl.acquireRakutenListingLock(id); } catch (e) { dup = e; }
    rel(); rel();
    check('ロック: 二重取得は 409・解放は冪等', dup?.status === 409 && !bl.isRakutenListingInFlight(id));
    const r = await bl.listToRakutenFromBoard(id, { actor: 'smoke', deps: { transfer: async () => { throw new Error('boom'); }, register: okRegister } });
    check('ロック: 転送が例外を投げても失敗で返り、ロックは解放される', r.ok === false && r.stage === 'transfer' && !bl.isRakutenListingInFlight(id), JSON.stringify(r).slice(0, 120));
    db.prepare('DELETE FROM product_drafts WHERE id = ?').run(id);
  }
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

  // 🚨 上のチェックは `<% %>` を `0` に**置き換えて**見るので、埋め込みが空文字になる事故を
  //    一切検出できない。`parent_step_version: <%= undefined %>` が
  //    `parent_step_version: ,` になり、セット商品の画面の JS が丸ごと死んでいた (2026-09-07)。
  //    ここでは**描画済みの HTML** から素の JS を取り出して構文を見る = 埋め込みの結果を見る
  // まずこの検査自身の検出力を見る。ここが緩むと全ページの検査が黙って素通りする (Codex R1)
  {
    const t = (html) => checkInlineScriptSyntax(vm, html, 'selftest').ok;
    check('JS 構文検査: 属性つきの script も見る (defer / nonce で外れない)',
      t('<script defer nonce="x">const a = ;</script>') === false);
    check('JS 構文検査: script を繋がない (片方だけ壊れていても見つける)',
      t('<script>const a =</script>\n<script>1;</script>') === false);
    check('JS 構文検査: src 付きと JSON の塊は対象外・素の script は見る',
      t('<script src="/a.js"></script><script type="application/json">{"a":1}</script><script>const a=1;</script>') === true
      && t('<script src="/a.js"></script><script>const a=;</script>') === false);
    check('JS 構文検査: type="module" は未対応と分かるように落とす',
      t('<script type="module">const a=1;</script>') === false);
    check('JS 構文検査: script が無いページ・JSON だけのページは通す',
      t('<p>x</p>') === true && t('<script type="application/json">{"a":1}</script>') === true);
    // 🚨 data-src / data-type は src / type ではない (ブラウザは普通のインライン JS として動かす)。
    //    語境界だけで見ていると、この 2 つを付けるだけで壊れた JS が検査から消える (Codex R2)
    check('JS 構文検査: data-src / data-type は除外の理由にしない',
      t('<script data-src="/a.js">const a = ;</script>') === false
      && t('<script data-type="application/json">const a = ;</script>') === false);
  }
  for (const [name, html] of renderedHtml) {
    const r = checkInlineScriptSyntax(vm, html, name);
    check(`描画後の JS が構文として通る: ${name}`, r.ok, r.detail);
  }

  // ─── 定型文のコピー画面 (detail.ejs initPromptButtons)。簡易LPの画像の重要度を揃える (2026-09-17) ───
  // 重要度は基本情報タブで選ぶと即保存され、画面は読み直さない。コピー画面を開く・コピーする時点の保存済みの値が入ること
  {
    const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
    const start = src.indexOf('(function initPromptButtons() {');
    const end = src.indexOf('\n  })();', start);
    const iife = start >= 0 && end > start ? src.slice(start, end + '\n  })();'.length) : '';
    check('定型文のコピー画面: detail.ejs から initPromptButtons を切り出せる (EJS タグを含まない)',
      iife.length > 200 && !iife.includes('<%'), String(iife.length));
    const pt = await import('../lib/prompt-templates.js');
    const LOW = '仕入商品（重要度：低）';
    const VERY_LOW = '仕入れ商品（重要度：激低_白抜）';
    const harness = ({ saved = '', hasSelect = true, productInfo = '説明', clipboardFails = false } = {}) => {
      const tpl = pt.buildPromptTemplates({ name: '商品', image_priority: saved || null }, { product_info_text: productInfo });
      const mkEl = (extra = {}) => {
        const ls = {};
        return {
          dataset: {}, hidden: true, textContent: '', value: '',
          addEventListener(t, fn) { (ls[t] = ls[t] || []).push(fn); },
          fire(t) { return Promise.all((ls[t] || []).map((fn) => fn())); },
          focus() {}, select() {},
          ...extra,
        };
      };
      const els = {
        'prompt-templates-json': mkEl({ textContent: JSON.stringify(tpl) }),
        'prompt-modal': mkEl(), 'prompt-text': mkEl(), 'prompt-title': mkEl(), 'prompt-copy-result': mkEl(),
        'prompt-close-btn': mkEl(), 'prompt-copy-btn': mkEl(),
      };
      if (hasSelect) els['f-image-priority'] = mkEl({ dataset: { saved } });
      const btns = ['initialJudge', 'productAnalysis', 'simpleLp'].map((kind) => mkEl({ dataset: { kind } }));
      const copied = [];
      const ctx = {
        document: { getElementById: (id) => els[id] || null, querySelectorAll: (sel) => (sel === '.prompt-btn' ? btns : []) },
        navigator: { clipboard: { writeText: async (t) => { if (clipboardFails) throw new Error('NotAllowedError'); copied.push(t); } } },
        console,
      };
      vm.createContext(ctx);
      new vm.Script(iife, { filename: 'initPromptButtons' }).runInContext(ctx);
      return {
        tpl, copied, sel: els['f-image-priority'], text: els['prompt-text'], result: els['prompt-copy-result'],
        open: (kind) => btns.find((b) => b.dataset.kind === kind).fire('click'),
        copy: () => els['prompt-copy-btn'].fire('click'),
        close: () => els['prompt-close-btn'].fire('click'),
      };
    };
    // 1) 読み込み時の値 (低) のまま開いてコピー
    {
      const h = harness({ saved: LOW });
      await h.open('simpleLp');
      await h.copy();
      check('定型文のコピー画面: 簡易LPを開くと保存済みの重要度 (低) が入り、そのままコピーされる',
        h.text.value === h.tpl.simpleLp && h.copied[0] === h.tpl.simpleLp && h.copied[0].includes(`画像の重要度：${LOW}`)
        && !h.result.textContent.includes('未設定'), `${h.copied[0]} / ${h.result.textContent}`);
    }
    // 2) 読み込んだ後に基本情報タブで激低へ変えた → 開いたときに激低
    {
      const h = harness({ saved: LOW });
      h.sel.dataset.saved = VERY_LOW;
      await h.open('simpleLp');
      check('定型文のコピー画面: 読み込み後に重要度を変えていたら、開いたときに変えた値が入る',
        h.text.value.includes(`【入力】\n\n画像の重要度：${VERY_LOW}\n\n商品情報：`) && !h.text.value.includes(`画像の重要度：${LOW}`),
        h.text.value);
    }
    // 3) 開いたまま重要度を変えた → コピーする本文 (と画面の本文) は変えた値。閉じて開き直しても同じ
    {
      const h = harness({ saved: LOW });
      await h.open('simpleLp');
      h.sel.dataset.saved = VERY_LOW;
      await h.copy();
      const afterCopy = h.text.value;
      await h.close();
      await h.open('simpleLp');
      check('定型文のコピー画面: 開いた後に重要度を変えても、コピーの時点の保存済みの値が入る (画面の本文も揃える)',
        h.copied[0].includes(`画像の重要度：${VERY_LOW}`) && !h.copied[0].includes(`画像の重要度：${LOW}`)
        && afterCopy === h.copied[0] && h.text.value === h.copied[0], h.copied[0]);
    }
    // 4) 本文の手直しは残す。重要度の行は手で書き換えていても基本情報の値にし、手で増やした重要度の行は消す
    {
      const h = harness({ saved: LOW });
      await h.open('simpleLp');
      h.text.value = h.text.value
        .replace('商品画像：', '商品画像：(手で足した補足)')
        .replace(`画像の重要度：${LOW}`, `画像の重要度：手で書いた\n画像の重要度：${VERY_LOW}`);
      await h.copy();
      check('定型文のコピー画面: 本文の手直しは残し、重要度の行は手で書き換えても保存済みの値の 1 行に揃える',
        h.copied[0].includes('商品画像：(手で足した補足)')
        && h.copied[0].includes(`【入力】\n\n画像の重要度：${LOW}\n\n商品情報：`)
        && !h.copied[0].includes('手で書いた') && !h.copied[0].includes(VERY_LOW) && h.text.value === h.copied[0],
        h.copied[0]);
      // 行頭に空白 (全角含む)・区切りを「:」に直した行・見出しに空白を足した行も同じ行として扱う (Codex 名指し R4)
      const h2 = harness({ saved: LOW });
      await h2.open('simpleLp');
      h2.text.value = h2.text.value
        .replace('【入力】', ' 【入力】　')
        .replace(`画像の重要度：${LOW}`, `　画像の重要度：${LOW}\n  画像の重要度: 手で書いた`)
        .replace('\n商品情報：\n', '\n商品情報： \n');
      h2.sel.dataset.saved = VERY_LOW;
      await h2.copy();
      check('定型文のコピー画面: 空白を足した行・「:」に直した重要度の行も揃え、相反する重要度を 2 行残さない',
        h2.copied.length === 1 && h2.copied[0].includes(`\n画像の重要度：${VERY_LOW}\n`)
        && !h2.copied[0].includes(LOW) && !h2.copied[0].includes('手で書いた')
        && h2.copied[0].split('\n').filter((l) => l.includes('画像の重要度')).length === 1,
        JSON.stringify([h2.result.textContent, h2.copied[0]]));
    }
    // 4') 商品情報の中にある同じ文字は書き換えない (Codex R1: 文字列の置換だと当たっていた)
    {
      const info = `画像の重要度：${LOW}\n仕入商品（重要度：低）の説明`;
      const h = harness({ saved: LOW, productInfo: info });
      await h.open('simpleLp');
      h.text.value = h.text.value.replace(`【入力】\n\n画像の重要度：${LOW}`, '【入力】\n\n画像の重要度：手で書いた');
      h.sel.dataset.saved = VERY_LOW;
      await h.copy();
      // 重要度の行を消していても、商品情報の中の行に当てずに「商品情報：」の前へ足す (Codex 名指し R1)
      const h2 = harness({ saved: LOW, productInfo: info });
      await h2.open('simpleLp');
      h2.text.value = h2.text.value.replace(`画像の重要度：${LOW}\n\n商品情報：`, '商品情報：');
      h2.sel.dataset.saved = VERY_LOW;
      await h2.copy();
      check('定型文のコピー画面: 商品情報の中の同じ文字は書き換えず、重要度の行を消していたら「商品情報：」の前に足す',
        h.copied[0].includes(`■商品情報\n${info}`) && h.copied[0].includes(`【入力】\n\n画像の重要度：${VERY_LOW}\n\n商品情報：`)
        && h2.copied[0].includes(`■商品情報\n${info}`) && h2.copied[0].includes(`\n画像の重要度：${VERY_LOW}\n\n商品情報：`),
        `${h.copied[0]}\n---\n${h2.copied[0]}`);
    }
    // 4'') 【入力】の見出しを消していたら、重要度を入れる場所が分からないのでコピーしない
    {
      const h = harness({ saved: LOW });
      await h.open('simpleLp');
      h.text.value = h.text.value.replace('【入力】', '入力');
      await h.copy();
      // 見出しを手で増やした: 最初の「商品情報：」で区切ると元の重要度の行が範囲の外に残る (Codex 名指し R2)
      const h2 = harness({ saved: LOW });
      await h2.open('simpleLp');
      h2.text.value = h2.text.value.replace('【入力】\n', '【入力】\n商品情報：\n(手で足した補足)\n');
      h2.sel.dataset.saved = VERY_LOW;
      await h2.copy();
      // 商品情報そのものに見出しと同じ行が入っているのは定型文と同じ数なので、止めない
      const h3 = harness({ saved: LOW, productInfo: '商品情報：\n【入力】\n本文' });
      await h3.open('simpleLp');
      h3.sel.dataset.saved = VERY_LOW;
      await h3.copy();
      check('定型文のコピー画面: 【入力】か「商品情報：」の見出しを消した・増やしたらコピーせず開き直しを案内 (商品情報の中の同じ行では止めない)',
        h.copied.length === 0 && h.result.textContent.includes('開き直して')
        && h2.copied.length === 0 && h2.result.textContent.includes('開き直して')
        && h3.copied.length === 1 && h3.copied[0].includes(`【入力】\n\n画像の重要度：${VERY_LOW}\n\n商品情報：\n■商品情報\n商品情報：\n【入力】\n本文`),
        JSON.stringify([h.result.textContent, h2.result.textContent, h3.copied[0]]));
    }
    // 5) 未設定: 本文は「未設定」で、開いたとき・コピーしたときに案内を出す。選んだ後のコピーでは案内を消す
    {
      const h = harness({ saved: '' });
      await h.open('simpleLp');
      const openHint = h.result.textContent;
      await h.copy();
      const copyHint = h.result.textContent;
      h.sel.dataset.saved = LOW;
      await h.copy();
      check('定型文のコピー画面: 重要度が未設定なら「未設定」と入れて案内し、選んだ後のコピーは選んだ値で案内なし',
        h.copied[0].includes('画像の重要度：未設定') && openHint.includes('画像の重要度が未設定') && copyHint.includes('画像の重要度が未設定')
        && h.copied[1].includes(`画像の重要度：${LOW}`) && !h.result.textContent.includes('未設定'),
        JSON.stringify({ openHint, copyHint, last: h.result.textContent }));
    }
    // 6) 保存中・保存できたか分からないときはコピーしない (Codex 名指し R1: 保存中にコピーすると古い値が入った)
    {
      const h = harness({ saved: LOW });
      h.sel.dataset.saving = '1';
      await h.open('simpleLp');
      const openMsg = h.result.textContent;
      await h.copy();
      const copyMsg = h.result.textContent;
      const copiedWhileSaving = h.copied.length;
      h.sel.dataset.saving = '';
      h.sel.dataset.saved = VERY_LOW;
      await h.copy();
      const h2 = harness({ saved: LOW });
      await h2.open('simpleLp');
      h2.sel.dataset.unknown = '1';
      await h2.copy();
      check('定型文のコピー画面: 重要度の保存中・保存できたか分からないときはコピーせず案内し、保存が終われば新しい値でコピーする',
        openMsg.includes('保存中') && copyMsg.includes('保存中') && copiedWhileSaving === 0
        && h.copied.length === 1 && h.copied[0].includes(`画像の重要度：${VERY_LOW}`)
        && h2.copied.length === 0 && h2.result.textContent.includes('読み直して'),
        JSON.stringify({ openMsg, copyMsg, copiedWhileSaving, unknown: h2.result.textContent }));
    }
    // 7) 初動判定・商品分析は重要度に関係なくサーバーの本文のまま (保存中でも止めない)。重要度の欄が無い画面でも簡易LPはサーバーの本文
    {
      const h = harness({ saved: LOW });
      h.sel.dataset.saved = VERY_LOW;
      h.sel.dataset.saving = '1';
      await h.open('initialJudge');
      await h.copy();
      const h2 = harness({ saved: LOW, hasSelect: false });
      await h2.open('simpleLp');
      await h2.copy();
      check('定型文のコピー画面: 初動判定は差し替えも案内も止めもしない / 重要度の欄が無ければ簡易LPはサーバーの本文のまま',
        h.copied[0] === h.tpl.initialJudge && !h.result.textContent.includes('重要度')
        && h2.copied[0] === h2.tpl.simpleLp, `${h.result.textContent} / ${h2.copied[0]}`);
    }
    // 8) コピーできなかったとき、iPad でも分かる案内 (長押し)
    {
      const h = harness({ saved: LOW, clipboardFails: true });
      await h.open('simpleLp');
      await h.copy();
      check('定型文のコピー画面: コピーできなければ長押しか Ctrl+C を案内する', h.result.textContent.includes('長押し'), h.result.textContent);
    }
  }

  // ─── 画像の重要度の即保存 (detail.ejs)。簡易LPが使う data-saved / data-saving / data-unknown (2026-09-17) ───
  {
    const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
    const start = src.indexOf("  const imgPrioritySel = document.getElementById('f-image-priority');");
    const end = src.indexOf("  document.getElementById('save-basic-btn')", start);
    const EJS_PRIORITIES = '<%- JSON.stringify(imagePriorities) %>';
    const raw = start >= 0 && end > start ? src.slice(start, end) : '';
    const chunk = raw.replace(EJS_PRIORITIES, JSON.stringify(dbmod.IMAGE_PRIORITIES));
    check('重要度の即保存: detail.ejs から切り出せる (EJS タグは重要度の一覧 1 つだけ)',
      raw.includes(EJS_PRIORITIES) && !chunk.includes('<%') && chunk.includes('saveOwnBrand'), String(raw.length));
    const LOW = '仕入商品（重要度：低）';
    const VERY_LOW = '仕入れ商品（重要度：激低_白抜）';
    const OWN = dbmod.OWN_BRAND_IMAGE_PRIORITY;
    const tick = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise((r) => setTimeout(r, 0)); };
    const mkEl = (extra = {}) => {
      const ls = {};
      return {
        dataset: {}, style: {}, value: '', checked: false, disabled: false, textContent: '',
        addEventListener(t, fn) { (ls[t] = ls[t] || []).push(fn); },
        fire(t) { return Promise.all((ls[t] || []).map((fn) => fn())); },
        ...extra,
      };
    };
    const harness = (saved, respond) => {
      const sel = mkEl({ value: saved, dataset: { saved } });
      const chk = mkEl({ checked: saved === OWN });
      const els = { 'f-image-priority': sel, 'f-own-brand': chk, 'image-priority-note': mkEl() };
      const alerts = [];
      const ctx = {
        document: { getElementById: (id) => els[id] || null },
        post: async (url, body) => respond(url, body),
        BASE: '/ph', result: { textContent: '' },
        alert: (m) => alerts.push(String(m)), setTimeout: () => 0, console,
      };
      vm.createContext(ctx);
      new vm.Script(chunk, { filename: 'imagePriority' }).runInContext(ctx);
      return { sel, chk, alerts };
    };
    {
      const h = harness(LOW, (_u, b) => ({ ok: true, own_brand: 0, image_priority: b.value || null }));
      h.sel.value = VERY_LOW;
      await h.sel.fire('change');
      const afterVeryLow = h.sel.dataset.saved;
      h.sel.value = '';
      await h.sel.fire('change');
      check('重要度の即保存: 保存できたら data-saved を保存した値にする (未設定に戻したら空)',
        afterVeryLow === VERY_LOW && h.sel.dataset.saved === '' && h.alerts.length === 0, JSON.stringify({ afterVeryLow, saved: h.sel.dataset.saved }));
    }
    // 拒否 (ok:false) は保存されていないので戻す。通信エラーは保存できたか分からないので戻さず、コピーを止める印を立てる
    {
      const h = harness(LOW, () => ({ ok: false, error: '保存できません' }));
      h.sel.value = VERY_LOW;
      await h.sel.fire('change');
      let fail = true;
      const h2 = harness(LOW, (_u, b) => {
        if (fail) throw new Error('network');
        return { ok: true, own_brand: 0, image_priority: b.value || null };
      });
      h2.sel.value = VERY_LOW;
      await h2.sel.fire('change');
      const afterNetwork = { value: h2.sel.value, saved: h2.sel.dataset.saved, unknown: h2.sel.dataset.unknown, alert: h2.alerts[0] };
      fail = false;
      await h2.sel.fire('change');
      check('重要度の即保存: 拒否なら data-saved を変えず選択欄を戻す / 通信エラーなら戻さず「分からない」印、次に保存できたら印を消す',
        h.sel.dataset.saved === LOW && h.sel.value === LOW && h.alerts.length === 1 && h.sel.dataset.unknown !== '1'
        && afterNetwork.value === VERY_LOW && afterNetwork.saved === LOW && afterNetwork.unknown === '1'
        && afterNetwork.alert.includes('読み直して')
        && h2.sel.dataset.saved === VERY_LOW && h2.sel.dataset.unknown === '' && h2.sel.disabled === false,
        JSON.stringify({ rejected: [h.sel.value, h.sel.dataset.saved], afterNetwork, end: [h2.sel.dataset.saved, h2.sel.dataset.unknown] }));
    }
    {
      let fail = false;
      const h = harness(LOW, (url, b) => {
        if (fail) throw new Error('network');
        return url.endsWith('/own-brand')
          ? { ok: true, own_brand: b.value ? 1 : 0, image_priority: b.value ? OWN : null }
          : { ok: false };
      });
      h.chk.checked = true;
      await h.chk.fire('change');
      await tick();
      const afterOn = [h.sel.value, h.sel.dataset.saved];
      h.chk.checked = false;
      await h.chk.fire('change');
      await tick();
      const afterOff = [h.sel.value, h.sel.dataset.saved];
      fail = true;
      h.chk.checked = true;
      await h.chk.fire('change');
      await tick();
      check('重要度の即保存: 自社商品チェックで連動した重要度も data-saved に入る (ON = 自社商品 / OFF = 未設定)。通信エラーはチェックを戻さず「分からない」印',
        afterOn[0] === OWN && afterOn[1] === OWN && afterOff[0] === '' && afterOff[1] === ''
        && h.sel.dataset.unknown === '1' && h.sel.dataset.saved === '' && h.chk.checked === true
        && h.alerts.at(-1).includes('読み直して'),
        JSON.stringify({ afterOn, afterOff, unknown: h.sel.dataset.unknown, checked: h.chk.checked, alerts: h.alerts }));
    }
    // どちらかの保存中はもう一方も触らせない (Codex R2: 重なると古い応答が data-saved を上書きする)。失敗しても戻す
    {
      let release = null;
      const h = harness(LOW, (url, b) => new Promise((resolve) => {
        release = (ok = true) => resolve(!ok ? { ok: false, error: 'x' } : url.endsWith('/own-brand')
          ? { ok: true, own_brand: b.value ? 1 : 0, image_priority: b.value ? OWN : null }
          : { ok: true, own_brand: 0, image_priority: b.value || null });
      }));
      const state = () => [h.sel.disabled, h.chk.disabled, h.sel.dataset.saving === '1'].join('/');
      h.chk.checked = true;
      await h.chk.fire('change');
      await tick();
      const duringOwn = state();
      release();
      await tick();
      const afterOwn = state();
      h.sel.value = VERY_LOW;
      const p = h.sel.fire('change');
      await tick();
      const duringPriority = state();
      release();
      await p;
      const afterPriority = state();
      h.sel.value = LOW;
      const p2 = h.sel.fire('change');
      await tick();
      release(false);
      await p2;
      const afterFail = state();
      h.chk.checked = true;
      await h.chk.fire('change');
      await tick();
      release(false);
      await tick();
      check('重要度の即保存: 重要度・自社商品チェックのどちらかを保存中は両方止めて保存中の印を立て、終われば (失敗でも) 戻す',
        duringOwn === 'true/true/true' && afterOwn === 'false/false/false'
        && duringPriority === 'true/true/true' && afterPriority === 'false/false/false'
        && afterFail === 'false/false/false' && state() === 'false/false/false'
        && h.sel.dataset.saved === VERY_LOW && h.sel.value === VERY_LOW,
        JSON.stringify({ duringOwn, afterOwn, duringPriority, afterPriority, afterFail, end: state(), saved: h.sel.dataset.saved }));
    }
    // 基本情報を保存: own_brand を送らない (Codex 名指し R2/R3)。自社商品チェックは即保存済みで、送ると重要度の即保存と
    // 前後して、古いチェックの状態で保存済みの重要度を戻していた (簡易LPの定型文に DB と違う重要度が入る)。
    // 重要度の保存中は何も送らず、基本情報の保存 (→ 読み直し) の間は重要度の欄を触らせない (Codex 名指し R4)
    {
      const bStart = src.indexOf("  document.getElementById('save-basic-btn').addEventListener('click', () => phEnqueueSave(async () => {");
      const bEnd = src.indexOf('  function hasVariationPayload()', bStart);
      const basic = bStart >= 0 && bEnd > bStart ? src.slice(bStart, bEnd) : '';
      check('基本情報を保存: detail.ejs から切り出せる', basic.includes('await post(BASE') && !basic.includes('<%'), String(basic.length));
      const setup = ({ saving = '', rakutenOk = true, respond = () => ({ ok: true }) } = {}) => {
        const btn = mkEl();
        const sel = mkEl({ dataset: { saving } });
        const chk = mkEl({ checked: true });
        const log = []; const posts = []; const alerts = []; const reloads = [];
        let release = null;
        const ctx = {
          document: { getElementById: (id) => (id === 'save-basic-btn' ? btn : id === 'f-own-brand' ? chk : mkEl()) },
          imgPrioritySel: sel, ownBrandChk: chk, BASE: '/ph',
          saveRakutenFields: async () => { log.push('rakuten'); return rakutenOk; },
          post: (url, body) => { log.push('basic'); posts.push(body); return new Promise((r) => { release = () => r(respond()); }); },
          showAndReload: (json) => { if (json.ok) reloads.push(json); },
          hasVariationPayload: () => ({}), alert: (m) => alerts.push(String(m)), console,
          // 未保存ガード (2026-09-21) はこの切り出しの外にあるので、無い状態で動くことも確かめる
          phKeep: null,
          // 保存の列 (2026-09-21) も外。ここでは素通しして、ボタン自身の動きだけを見る
          phEnqueueSave: (fn) => fn(),
          phSending: () => () => {},
        };
        vm.createContext(ctx);
        new vm.Script(basic, { filename: 'saveBasic' }).runInContext(ctx);
        return {
          sel, chk, log, posts, alerts, reloads,
          click: () => btn.fire('click'), release: () => release && release(),
          locked: () => `${sel.disabled}/${chk.disabled}`,
        };
      };
      const tick = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise((r) => setTimeout(r, 0)); };
      // 保存できる: 楽天 → 基本情報 (own_brand なし)。送信中は重要度の欄を止め、読み直すのでそのまま
      const ok = setup();
      const pOk = ok.click();
      await tick();
      const lockedWhileSaving = ok.locked();
      ok.release();
      await pOk;
      // 拒否 (ok:false) なら読み直さないので欄を戻す。楽天の保存で止まった場合も戻す
      const ng = setup({ respond: () => ({ ok: false, error: 'x' }) });
      const pNg = ng.click();
      await tick();
      ng.release();
      await pNg;
      const rk = setup({ rakutenOk: false });
      await rk.click();
      // 重要度の保存中に押した: 楽天も基本情報も送らずに案内する
      const busy = setup({ saving: '1' });
      const pBusy = busy.click();
      await tick();
      busy.release(); // 送ってしまった場合も待ち続けない (NG として落とす)
      await pBusy;
      check('基本情報を保存: own_brand を送らず、送信中は重要度の欄を止める (読み直さないときは戻す)。重要度の保存中は何も送らない',
        ok.log.join(',') === 'rakuten,basic' && !('own_brand' in ok.posts[0]) && 'memo' in ok.posts[0]
        && lockedWhileSaving === 'true/true' && ok.reloads.length === 1 && ok.locked() === 'true/true'
        && ng.reloads.length === 0 && ng.locked() === 'false/false'
        && rk.log.join(',') === 'rakuten' && rk.locked() === 'false/false'
        && busy.log.length === 0 && busy.alerts[0]?.includes('画像の重要度を保存中') && busy.locked() === 'false/false',
        JSON.stringify({ ok: [ok.log, ok.posts, lockedWhileSaving, ok.locked()], ng: ng.locked(), rk: [rk.log, rk.locked()], busy: [busy.log, busy.alerts] }));
    }
  }

  // ─── SKU別JAN の保存ワーカー (detail.ejs initSkuJans) の時系列テスト (2026-09-02 Codex R3/R4 high) ───
  // 画面の IIFE をそのまま切り出し、最小の DOM もどきと手動で応答を返す post で走らせる
  {
    const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
    // 共通ワーカー createSkuSaver + それを使う initSkuJans (2026-09-03 で JAN / 商品仕様 / 理由 に共通化)
    const start = src.indexOf('function createSkuSaver(');
    const jansAt = src.indexOf('(function initSkuJans() {', start);
    const end = src.indexOf('\n  })();', jansAt);
    const iife = start >= 0 && jansAt > start && end > jansAt ? src.slice(start, end + '\n  })();'.length) : '';
    check('sku-jan worker: detail.ejs から createSkuSaver + initSkuJans を切り出せる', iife.length > 200, String(iife.length));
    class FakeEvent { constructor(type) { this.type = type; } }
    const mkInput = (code, value) => {
      const ls = {};
      return {
        value, dataset: { code },
        addEventListener(t, fn) { (ls[t] = ls[t] || []).push(fn); },
        dispatchEvent(ev) { (ls[ev.type] || []).forEach((fn) => fn(ev)); },
        change(v) { this.value = v; this.dispatchEvent(new FakeEvent('change')); },
      };
    };
    const tick = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise((r) => setTimeout(r, 0)); };
    function harness() {
      const a = mkInput('sku-a', ''), b = mkInput('sku-b', '');
      const pending = []; const alerts = []; const posts = [];
      const ctx = {
        document: {
          querySelectorAll: (sel) => (sel === '.sku-jan-input' ? [a, b] : []),
          getElementById: () => null,
        },
        Event: FakeEvent, alert: (m) => alerts.push(String(m)), BASE: '/x',
        post: (url, body) => new Promise((resolve, reject) => { posts.push(body); pending.push({ body, resolve, reject }); }),
        setTimeout, console, phKeep: null,
      };
      vm.createContext(ctx);
      const api = new vm.Script(`const skuSavers = [];\nlet skuSaveGeneration = 0;\nconst skuPendingOps = new Set();\nlet skuOpFailed = false;\n${iife}\n({ flush: () => flushSkuSavers(), track: (p) => trackSkuOp(p) })`, { filename: 'initSkuJans' }).runInContext(ctx);
      return { a, b, pending, alerts, posts, flush: api.flush, track: api.track };
    }
    // 0') 追跡した一括操作が拒否 (ok:false) で終わったら、その flush は 1 回だけ false (Codex R3 medium)
    {
      const h = harness();
      let done = null;
      h.track(new Promise((resolve) => { done = resolve; })).catch(() => {});
      const fl = h.flush();
      await tick();
      done({ ok: false, error: 'rejected' });
      const ok = await fl;
      const ok2 = await h.flush();
      check('sku-jan worker: 拒否された一括操作を待った flush は false、次の flush は true (1 回限り)', ok === false && ok2 === true, JSON.stringify({ ok, ok2 }));
    }
    // 0'') 同時に始まった 2 つの flush (二重クリック) は同じ結果 (Codex R4 high: 片方だけ false になっていた)
    {
      const h = harness();
      let done = null;
      h.track(new Promise((resolve) => { done = resolve; })).catch(() => {});
      const f1 = h.flush();
      const f2 = h.flush();
      await tick();
      done({ ok: false, error: 'rejected' });
      const [r1, r2] = await Promise.all([f1, f2]);
      const r3 = await h.flush();
      check('sku-jan worker: 同時に始まった flush は両方 false、終わった後の新しい flush は true', r1 === false && r2 === false && r3 === true, JSON.stringify({ r1, r2, r3 }));
    }
    // 0) ワーカー外の保存 (一括入力) が実行中なら flush はその完了を待つ (Codex R2 high)
    {
      const h = harness();
      let done = null;
      const op = new Promise((resolve) => { done = resolve; });
      h.track(op);
      let flushed = null;
      const fl = h.flush().then((v) => { flushed = v; return v; });
      await tick();
      check('sku-jan worker: 一括操作が実行中なら flush は戻らない', flushed === null);
      done({ ok: true });
      const ok = await fl;
      check('sku-jan worker: 一括操作が終わってから flush=true', ok === true && flushed === true);
    }
    // 1) A の保存が通信中に flush → その間に B を変更 → A 成功 → B も保存されてから flush が true で戻る
    {
      const h = harness();
      h.a.change('4901234567894');
      await tick();
      const fl = h.flush();
      await tick();
      h.b.change('4912345678904');
      await tick();
      h.pending.shift().resolve({ ok: true });
      await tick();
      check('sku-jan worker: 待機中の変更も保存してから flush が戻る', h.pending.length === 1 && h.pending[0].body.jan_code === '4912345678904', JSON.stringify(h.posts));
      h.pending.shift().resolve({ ok: true });
      const ok = await fl;
      check('sku-jan worker: 全部保存できたら flush=true', ok === true && h.posts.length === 2 && h.alerts.length === 0, JSON.stringify({ ok, posts: h.posts, alerts: h.alerts }));
    }
    // 2) サーバー拒否 → 画面は DB の値へ巻き戻り、flush=false (合流した drain の失敗が伝わる)
    {
      const h = harness();
      h.a.change('4901234567894');
      await tick();
      const fl = h.flush();
      await tick();
      h.pending.shift().resolve({ ok: false, error: 'dup' });
      const ok = await fl;
      check('sku-jan worker: 拒否は巻き戻し + flush=false', ok === false && h.a.value === '' && h.alerts.length === 1 && h.posts.length === 1, JSON.stringify({ ok, a: h.a.value, alerts: h.alerts }));
      // 拒否後は dirty が無いので、次の flush は POST なしで true
      const ok2 = await h.flush();
      check('sku-jan worker: 拒否で巻き戻した後の flush は再送せず true', ok2 === true && h.posts.length === 1);
    }
    // 3) 通信エラー → 巻き戻さず dirty のまま止まる (DB がコミット済みかもしれない)。flush=false。次の flush で冪等に再送
    {
      const h = harness();
      h.a.change('4901234567894');
      await tick();
      const fl = h.flush();
      await tick();
      h.pending.shift().reject(new Error('network'));
      const ok = await fl;
      check('sku-jan worker: 通信エラーは巻き戻さず flush=false', ok === false && h.a.value === '4901234567894' && h.posts.length === 1, JSON.stringify({ ok, a: h.a.value, posts: h.posts }));
      const fl2 = h.flush();
      await tick();
      check('sku-jan worker: 次の flush で同じ値を再送する', h.pending.length === 1 && h.pending[0].body.jan_code === '4901234567894', JSON.stringify(h.posts));
      h.pending.shift().resolve({ ok: true });
      const ok2 = await fl2;
      check('sku-jan worker: 再送が通れば flush=true', ok2 === true && h.posts.length === 2);
    }
    // 4) drain の途中で 1 件目が拒否済み・2 件目が通信中に flush → 2 件目が成功しても false (失敗は drain の結果として残る)
    {
      const h = harness();
      h.a.change('4901234567894');
      await tick();
      h.b.change('4912345678904');
      await tick();
      h.pending.shift().resolve({ ok: false, error: 'dup' }); // A 拒否 → 巻き戻し
      await tick();
      check('sku-jan worker: 2 件目 (B) の保存が進んでいる', h.pending.length === 1 && h.pending[0].body.ne_code === 'sku-b', JSON.stringify(h.posts));
      const fl = h.flush(); // 実行中の drain に合流
      await tick();
      h.pending.shift().resolve({ ok: true });
      const ok = await fl;
      check('sku-jan worker: 合流前に起きた拒否も flush=false に反映される', ok === false && h.a.value === '' && h.b.value === '4912345678904', JSON.stringify({ ok, a: h.a.value, b: h.b.value }));
      const ok2 = await h.flush();
      check('sku-jan worker: その後の flush は (dirty なし・失敗なし) true', ok2 === true && h.posts.length === 2);
    }
    // 5) 通信中に元の値へ戻す + 応答消失 (Codex R5 high): 画面と lastOf は一致するが DB は不明 →
    //    uncertain のまま flush=false。次の flush で現在値 (空 = 解除) を再送し、成功して初めて true
    {
      const h = harness();
      h.a.change('4901234567894');
      await tick();
      h.a.change(''); // 応答待ち中に元の値へ戻す (rerun が立つ)
      await tick();
      h.pending.shift().reject(new Error('network')); // サーバーは A をコミットしたかもしれない
      await tick();
      const fl1 = h.flush();
      await tick();
      check('sku-jan worker: 不明な入力は画面が旧値と同じでも現在値 (空 = 解除) を再送する',
        h.pending.length === 1 && h.pending[0].body.jan_code === '' && h.pending[0].body.ne_code === 'sku-a', JSON.stringify(h.posts));
      h.pending.shift().reject(new Error('network'));
      const ok = await fl1;
      check('sku-jan worker: 再送も失敗なら flush=false のまま (DB 不明)', ok === false && h.posts.length === 2, JSON.stringify({ ok, posts: h.posts }));
      const fl2 = h.flush();
      await tick();
      h.pending.shift().resolve({ ok: true });
      const ok2 = await fl2;
      check('sku-jan worker: 再送が通れば uncertain が消えて flush=true', ok2 === true && h.posts.length === 3, JSON.stringify({ ok2, posts: h.posts }));
      const ok3 = await h.flush();
      check('sku-jan worker: 確定後の flush は再送しない', ok3 === true && h.posts.length === 3);
    }
    // 6) 結果不明のまま再送がサーバーに拒否された (Codex R6 low): 巻き戻す先が無いので巻き戻さず、
    //    flush=false・再読込案内。次の flush でも再送され、成功して初めて確定
    {
      const h = harness();
      h.a.change('4901234567894');
      await tick();
      h.pending.shift().reject(new Error('network'));
      await tick();
      const fl1 = h.flush();
      await tick();
      h.pending.shift().resolve({ ok: false, error: 'dup' });
      const ok = await fl1;
      check('sku-jan worker: 不明中の拒否は巻き戻さず flush=false + 再読込案内',
        ok === false && h.a.value === '4901234567894' && h.alerts.some((m) => m.includes('再読み込み')), JSON.stringify({ ok, a: h.a.value, alerts: h.alerts }));
      const fl2 = h.flush();
      await tick();
      check('sku-jan worker: 不明中の拒否の後も次の flush で再送する', h.pending.length === 1 && h.pending[0].body.jan_code === '4901234567894', JSON.stringify(h.posts));
      h.pending.shift().resolve({ ok: true });
      const ok2 = await fl2;
      check('sku-jan worker: 再送が通れば確定して flush=true', ok2 === true && h.posts.length === 3);
    }
  }

{
  // ジャンルを差し替えたとき、前のジャンルの属性が残ると出品直前に IE1002 で弾かれる。
  // その場で気づけるよう、辞書に無い属性を赤くして一覧を出す (2026-09-04)
  const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
  check('属性のずれ: 警告の置き場所が属性表にある', src.includes('id="rk-attr-unknown"'));
  check('属性のずれ: 判定は属性候補の描画と同じタイミングで走る (呼び忘れが起きない)',
    /function renderAttrSuggest\(\)\s*\{\s*markAttrsNotInGenre\(\);/.test(src));
  check('属性のずれ: 単品の行と SKU 表の行の両方を見る',
    /#rk-attrs tbody tr \.rk-attr-name/.test(src.slice(src.indexOf('function markAttrsNotInGenre')))
    && /#rk-sku-grid tr\.sku-grid-attr/.test(src.slice(src.indexOf('function markAttrsNotInGenre'))));
  check('属性のずれ: 出品前チェックと同じ言葉 (IE1002) で知らせる',
    src.slice(src.indexOf('function markAttrsNotInGenre')).includes('IE1002'));
  {
    const fn = src.slice(src.indexOf('function markAttrsNotInGenre'), src.indexOf('function renderAttrSuggest'));
    // 装飾はクラスで重ねる。style を直接書くと addAttrRow が塗った必須属性の枠色を消す (Codex R3)
    check('属性のずれ: 装飾はクラスの付け外しで行う (必須属性の枠色を消さない)',
      /classList\.toggle\('attr-unknown'/.test(fn) && !/\.style\[/.test(fn) && !/style\.borderColor\s*=/.test(fn), fn.slice(0, 200));
    check('属性のずれ: 辞書が空のときは何も赤くしない (取得前に全部を誤検知しない)',
      /known\.size > 0/.test(fn));
    check('属性のずれ: 専用クラスのスタイルが定義されている',
      /input\.attr-unknown\s*\{/.test(src) && /th\.attr-unknown\s*\{/.test(src));
  }
  // 選択式の属性はセレクトで選ばせる (2026-09-20)。動きは実ブラウザで確かめた。ここは配線の呼び忘れを止める
  {
    const fn = src.slice(src.indexOf('function syncSelectiveInputs'), src.indexOf('function renderAttrSuggest'));
    check('選択式セレクト: 属性候補の描画と同じタイミングで走る (読み込み時・取得後・名前の打ち替え)',
      /function renderAttrSuggest\(\)\s*\{\s*markAttrsNotInGenre\(\);\s*syncSelectiveInputs\(\);/.test(src));
    check('選択式セレクト: 選択肢は画面の辞書 (初期表示と取得後の両方) に渡している',
      (src.match(/options: Array\.isArray\(a\.options\) \? a\.options : null/g) || []).length === 2);
    check('選択式セレクト: セレクトも保存が読むクラス (rk-attr-value) を持つ', /el\.className = 'rk-attr-value'/.test(fn));
    check('🚨 選択式セレクト: 選択肢に無い値は消さずに残して選び直しを促す', /dataset\.unknown/.test(fn) && fn.includes('選び直してください'));
    check('選択式セレクト: 複数の値を入れられる属性・「|」入りの値はセレクトにしない',
      /multiValueLimit > 1\) return null/.test(src) && /value\.includes\('\|'\) \? null/.test(fn));
  }
}

{
  // 画面: 配送費が固定値ではなく、選んだ配送方法で決まること (ソース検査)
  const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
  const sim = src.slice(src.indexOf('function initProfitSim'), src.indexOf('function initSkuPrices'));
  check('配送費の試算: 配送費は書き換えられる変数で持つ (固定の const ではない)',
    /let ship = Number\(box\.dataset\.ship\)/.test(sim) && !/const ship = Number\(box\.dataset\.ship\)/.test(sim));
  check('配送費の試算: 配送方法を変えたら利益を再計算する',
    /sel\.addEventListener\('change', apply\)/.test(sim) && /ship = Number\(op\.dataset\.cost\)/.test(sim)
    && /function apply\(\)[\s\S]{0,400}render\(\)/.test(sim));
  check('配送費の試算: 楽天の配送方法を変えたら候補を組み直す',
    /rkShip\.addEventListener\('change', build\)/.test(sim));
  check('配送費の試算: 売価の入力でも従来どおり再計算する',
    /priceInput\.addEventListener\('input', render\)/.test(sim));
  check('配送費の試算: 試算であって NE や出品内容は変えないと画面に書く',
    /試算だけで、NE や出品内容は変わりません/.test(sim));
}

  // ─── 楽天項目の保存が「前回の選択」を進めること (2026-09-03 #1152 / Codex R3 high) ───
  // この画面は保存後に再読み込みしない経路があるので、送れた配送方法まで shipSelectInitial を
  // 進めないと、2 回目以降の保存で毎回「複合選択肢を選び直した」と誤判定してしまい、
  // 人が直したヤフーの配送方法が既定値へ戻る
  {
    const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
    const start = src.indexOf('  async function postRakutenFields(');
    // 終端は次のセクションの目印で取る (ファイルが CRLF なので改行の形に依存させない)
    const end = src.indexOf('  // 公開 / 非公開の切り替え', start);
    const chunk = start >= 0 && end > start ? src.slice(start, end) : '';
    check('楽天保存: detail.ejs から postRakutenFields を切り出せる',
      chunk.includes('shipSelectInitial = payload.shipping_method_group'), String(chunk.length));
    check('楽天保存: 送信ペイロードに画面の初期選択が入る (サーバの「選び直したか」判定の材料)',
      src.includes('shipping_method_group_prev: shipSelectInitial'));
    const harness = (initial, selectValue, ok) => {
      const posts = [];
      const savedCalls = [];   // 未保存ガードの「ここまで保存できた」の記録
      const h = new Function('collectRakutenFields', 'post', 'BASE', 'initial', 'savedCalls', `
        let shipSelectInitial = initial;
        // 未保存ガード (2026-09-21) はこの切り出しの外にある。送る直前に控え、
        // 保存が通ったときだけ基準を進めることを、このスタブで見る
        const phKeep = { saving: () => 'SNAP', saved: (s) => savedCalls.push(s) };
        ${chunk}
        return { call: postRakutenFields, get: () => shipSelectInitial };
      `)(
        () => ({ shipping_method_group: selectValue, shipping_method_group_prev: initial }),
        async (_url, body) => { posts.push(body); return { ok }; },
        '/ph',
        initial,
        savedCalls,
      );
      return { h, posts, savedCalls };
    };
    const t1 = harness('1y5', '1y8', true);
    await t1.h.call();
    check('楽天保存: 保存できたら「前回の選択」を送った値まで進める',
      t1.h.get() === '1y8' && t1.posts[0].shipping_method_group_prev === '1y5', JSON.stringify(t1.posts));
    check('楽天保存: 保存が通ったら未保存の基準も送った値まで進める (2026-09-21)',
      JSON.stringify(t1.savedCalls) === '["SNAP"]', JSON.stringify(t1.savedCalls));
    const t2 = harness('1y5', '1y8', false);
    await t2.h.call();
    check('楽天保存: 保存できなければ進めない (次の保存でも選び直しとして扱う)', t2.h.get() === '1y5');
    check('楽天保存: 保存できなければ未保存の基準も進めない (打った値を捨てない)',
      t2.savedCalls.length === 0, JSON.stringify(t2.savedCalls));
    const t3 = harness('1y8', '1y8', true);
    await t3.h.call({ drop_legacy_catalog_attr: true });
    check('楽天保存: 追加パラメータを渡す経路も同じ扱い',
      t3.h.get() === '1y8' && t3.posts[0].drop_legacy_catalog_attr === true, JSON.stringify(t3.posts));
  }

  // ─── 単品 JAN の保存 (detail.ejs saveJanIfChanged) の時系列テスト (Codex R5 high) ───
  {
    const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
    const start = src.indexOf('let lastSavedJan =');
    const end = src.indexOf('  // SKU ごとの即保存ワーカー一覧', start);
    const chunk = start >= 0 && end > start ? src.slice(start, end) : '';
    check('jan save: detail.ejs から saveJanIfChanged を切り出せる', chunk.includes('function saveJanIfChanged') && chunk.length > 200, String(chunk.length));
    const tick = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise((r) => setTimeout(r, 0)); };
    function harness(initial) {
      const el = { value: initial };
      const pending = []; const alerts = []; const posts = []; const janSaved = [];
      const ctx = {
        document: { getElementById: (id) => (id === 'f-jan' ? el : null) },
        alert: (m) => alerts.push(String(m)), BASE: '/x',
        post: (url, body) => new Promise((resolve, reject) => { posts.push(body); pending.push({ body, resolve, reject }); }),
        setTimeout, console,
        // 未保存ガード (2026-09-21) は切り出しの外。JAN が保存できたら基準を進めることを見る
        phKeep: { savedValue: (id, v) => janSaved.push(id + '=' + v) },
      };
      vm.createContext(ctx);
      const api = new vm.Script(`${chunk}\n({ save: () => saveJanIfChanged() })`, { filename: 'saveJanIfChanged' }).runInContext(ctx);
      return { el, pending, alerts, posts, save: api.save, janSaved };
    }
    // X → A に変更して保存開始 → 通信中に X へ戻す → 応答消失。次の保存は値が同じでも必ず再送する
    {
      const h = harness('4901234567894');
      h.el.value = '4912345678904';
      const s1 = h.save();
      await tick();
      h.el.value = '4901234567894'; // 元の値へ戻す
      h.pending.shift().reject(new Error('network'));
      const ok1 = await s1;
      check('jan save: 通信エラーは false + 不明状態', ok1 === false && h.alerts.length === 1, JSON.stringify({ ok1, alerts: h.alerts }));
      const s2 = h.save();
      await tick();
      check('jan save: 不明状態なら現在値が lastSavedJan と同じでも再送する', h.pending.length === 1 && h.pending[0].body.jan_code === '4901234567894', JSON.stringify(h.posts));
      h.pending.shift().resolve({ ok: true });
      const ok2 = await s2;
      check('jan save: 再送が通れば true', ok2 === true && h.posts.length === 2);
      const ok3 = await h.save();
      check('jan save: 確定後は値が同じなら再送しない', ok3 === true && h.posts.length === 2);
    }
    // 保存中の打ち直しは最新値まで保存してから戻る (Codex R2 high)
    {
      const h = harness('');
      h.el.value = '4901234567894';
      const s1 = h.save();
      await tick();
      h.el.value = '4912345678904'; // 応答待ち中に打ち直し
      h.pending.shift().resolve({ ok: true });
      await tick();
      check('jan save: 打ち直された値も続けて保存する', h.pending.length === 1 && h.pending[0].body.jan_code === '4912345678904', JSON.stringify(h.posts));
      h.pending.shift().resolve({ ok: true });
      const ok = await s1;
      check('jan save: 最新値まで保存できたら true', ok === true && h.posts.length === 2);
      check('jan save: 保存できた値まで未保存の基準も進める (2026-09-21)',
        h.janSaved.join(',') === 'f-jan=4901234567894,f-jan=4912345678904', h.janSaved.join(','));
    }
    // 結果不明 → 再送が拒否 → それでも未確定のまま (同値を再送し続け、成功時だけ解除) (Codex R6 low)
    {
      const h = harness('4901234567894');
      h.el.value = '4912345678904';
      const s1 = h.save();
      await tick();
      h.pending.shift().reject(new Error('network'));
      const ok1 = await s1;
      const s2 = h.save();
      await tick();
      h.pending.shift().resolve({ ok: false, error: 'bad' });
      const ok2 = await s2;
      check('jan save: 不明中の拒否は false のまま', ok1 === false && ok2 === false && h.posts.length === 2, JSON.stringify({ ok1, ok2, posts: h.posts }));
      const s3 = h.save();
      await tick();
      check('jan save: 拒否の後も未確定なので同値を再送する', h.pending.length === 1 && h.pending[0].body.jan_code === '4912345678904', JSON.stringify(h.posts));
      h.pending.shift().resolve({ ok: true });
      const ok3 = await s3;
      const ok4 = await h.save();
      check('jan save: 成功で確定 → 以後は再送しない', ok3 === true && ok4 === true && h.posts.length === 3);
    }
  }

  // ─── セット作成フォームの構成の行 (detail.ejs addMemberRow / readSetMembers・§5.6) ───
  // 「id が HTML にある」だけでは、行が増えない・空行が混ざる・最後の1行が消える、が全部素通りする
  // (Codex low 2026-09-04)。実際に足して・消して・読み取るところまで動かす
  {
    const src = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
    const start = src.indexOf("const setMembersBox = document.getElementById('set-members');");
    // 切り出し範囲は EJS の埋め込みを含まない (素の JS として vm で動かすため)。
    // 実行 (initSetMembers(...)) だけがテンプレート側に残る
    const end = src.indexOf('    <%# ここまでが smoke の切り出し範囲', start);
    const chunk = start >= 0 && end > start ? src.slice(start, end) : '';
    check('セット構成の行: detail.ejs から addMemberRow / readSetMembers を切り出せる',
      chunk.includes('function addMemberRow') && chunk.includes('function readSetMembers')
      && chunk.includes('function initSetMembers') && !chunk.includes('<%'), String(chunk.length));
    check('セット構成の行: テンプレート側は商品コードを JSON で渡す (HTMLエスケープした値を JS 文字列にしない)',
      /initSetMembers\(<%- JSON\.stringify\(draft\.ne_code/.test(src), '');

    // 最小 DOM。querySelector(All) はクラス名だけを見る (このコードが使う範囲)
    const mkEl = (tag) => {
      const el = {
        tagName: tag, className: '', value: '', type: '', placeholder: '', title: '', textContent: '',
        maxLength: 0, min: '', max: '', style: { cssText: '' }, children: [], parent: null, _click: null,
        setAttribute() {}, appendChild(c) { c.parent = el; el.children.push(c); return c; },
        addEventListener(ev, fn) { if (ev === 'click') el._click = fn; },
        remove() { if (el.parent) el.parent.children = el.parent.children.filter((c) => c !== el); },
        querySelectorAll(sel) { return el.children.filter((c) => c.className === sel.replace('.', '')); },
        querySelector(sel) { return el.querySelectorAll(sel)[0] || null; },
      };
      return el;
    };
    const box = mkEl('div');
    const addBtn = mkEl('button');
    const ctx = {
      document: {
        getElementById: (id) => (id === 'set-members' ? box : (id === 'set-member-add' ? addBtn : null)),
        createElement: mkEl,
      },
      console,
    };
    vm.createContext(ctx);
    const api = new vm.Script(`${chunk}\n({ add: addMemberRow, read: readSetMembers, init: initSetMembers })`,
      { filename: 'setMembers' }).runInContext(ctx);
    const rows = () => box.querySelectorAll('.set-member-row');
    api.init("it's-a-code");
    check('セット構成の行: 開いた時点で「この商品 × 2」の1行が入っている',
      rows().length === 1 && rows()[0].querySelector('.set-member-qty').value === '2',
      JSON.stringify(rows().map((r) => r.querySelector('.set-member-qty').value)));
    check("セット構成の行: `'` を含む商品コードもそのまま入る (HTMLエスケープで別コードにならない)",
      rows()[0].querySelector('.set-member-code').value === "it's-a-code",
      rows()[0].querySelector('.set-member-code').value);
    addBtn._click();
    check('セット構成の行: 「行を追加」で行が増える (既定は 1 個)',
      rows().length === 2 && rows()[1].querySelector('.set-member-qty').value === '1', String(rows().length));
    check('セット構成の行: 空の商品コードの行は送らない (行を足して埋めなかった分)',
      api.read().length === 1 && api.read()[0].ne_code === "it's-a-code", JSON.stringify(api.read()));
    rows()[0].querySelector('.set-member-code').value = ' alpha ';
    rows()[1].querySelector('.set-member-code').value = 'beta';
    rows()[1].querySelector('.set-member-qty').value = '3';
    check('セット構成の行: 商品コードの前後の空白は落として読む',
      JSON.stringify(api.read()) === JSON.stringify([{ ne_code: 'alpha', qty: 2 }, { ne_code: 'beta', qty: 3 }]),
      JSON.stringify(api.read()));
    // 行の子は [商品コード, 個数, 「個」, ✕]。削除ボタンは 4 つ目
    const delOf = (row) => row.children[3];
    delOf(rows()[1])._click();
    check('セット構成の行: ✕ でその行が消える', api.read().length === 1 && api.read()[0].ne_code === 'alpha');
    delOf(rows()[0])._click();
    check('🚨 セット構成の行: 最後の1行は消せない (構成が空のセットを作らせない)',
      rows().length === 1, String(rows().length));
    // 個数が壊れていたら「読める」ままにして、送信側 (createSet) とサーバーの両方で弾かせる
    rows()[0].querySelector('.set-member-qty').value = '0';
    check('セット構成の行: 個数が不正でも読み取りは値を隠さない (押した瞬間に理由を出せる)',
      api.read()[0].qty === 0, JSON.stringify(api.read()));
  }
}

// ─── 未保存の入力は「読み直し」で消えない (2026-09-21 中原さん) ─────────────────
// 売価を打ってから「セット商品にする?」を記録すると、読み直しで売価が空に戻っていた
// (明示保存の欄はボタンを押すまで DB に行かないのに、読み直しが 30 箇所あった)。
// 塞いだのは引き金ではなく**読み直す側** = reloadSafely。ここでは
//   ①関所を通っているか ②挙げた欄の id が実在するか ③本当に値が戻るか を見る。
// 🚨 ③が要る: ①②だけだと「id は合っているが戻らない」が丸ごと素通りする
{
  const vm = await import('node:vm');
  const srcDetail = fs.readFileSync(path.join(views, 'detail.ejs'), 'utf8');
  const full = renderedHtml.get('detail.ejs (full/own_brand)') || '';
  const js = inlineScriptsOf(full).map((b) => b.code).join('\n');

  const reloads = (js.match(/location\.reload\(\)/g) || []).length;
  check('読み直しは関所 1 本だけを通る (直に location.reload() を書かない)',
    reloads === 1 && /function reloadSafely\(\)\s*\{[\s\S]*?location\.reload\(\)/.test(js),
    `直書き ${reloads} 箇所`);
  check('関所は読み直す前に未保存を退避する',
    /function reloadSafely\(\)\s*\{[\s\S]{0,900}?phKeep\.stash\(/.test(js));
  check('関所は退避するだけで保存しない (勝手に DB を書かない)',
    /function writeStash\(/.test(js) && !/function writeStash\([\s\S]{0,900}?(fetch\(|post\()/.test(js));
  // 🚨 読み直しは**列の最後尾**まで待つ。最初の 1 本だけ待つと、待っている間に確定した欄が
  //    通信中のまま読み直され、保存できたばかりの値を「他人の変更」と誤判定して捨てる (Codex R1)
  check('関所は保存の列が空になるまで待つ (待つ間に増えた保存も待つ)',
    /tail !== phSaveTail/.test(js) && /waitTail\(\)/.test(js));
  check('関所は待てなかった欄の「送った値」も添えて退避する (自分の保存と他人の変更を見分ける)',
    /phKeep\.stash\(\{ inflight \}\)/.test(js) && /phInflight\.forEach/.test(js));
  // 保存が通った経路は「保存済み」の基準を進める。忘れると、次の読み直しで
  // 自分が保存した値を他人の変更と誤判定して、打ち直した分を捨てる (Codex R1)
  for (const [route, needle] of [
    ['基本情報', 'phKeep.saved(basicSnap)'],
    ['出品情報 (楽天)', "phKeep.saving(['rakuten'])"],
    ['Yahoo!項目', "phKeep.saving(['yahoo'], ['y-tax'])"],
    ['画像制作情報', "phKeep.saving(['image'])"],
    ['JANコード', "phKeep.savedValue('f-jan', value)"],
  ]) {
    check(`未保存ガード: ${route}の保存が通ったら基準を進める`, js.includes(needle), needle);
  }
  check('戻したことを知らせる置き場と、未保存の印がある',
    full.includes('id="unsaved-zone"') && full.includes('.unsaved-mark') && full.includes('.tab-unsaved'));

  // 退避する欄の id は、打ち間違えても画面は普通に動く (黙って効かなくなる) ので実在を確かめる。
  // 商品によって出ない欄があるため、描いた detail.ejs 全部の和集合で見る
  const ids = [...js.matchAll(/\{ id: '([\w-]+)', label: '/g)].map((m) => m[1]);
  const everywhere = [...renderedHtml.entries()]
    .filter(([n]) => n.startsWith('detail.ejs')).map(([, h]) => h).join('\n');
  const missing = ids.filter((id) => !everywhere.includes(`id="${id}"`));
  check('未保存ガード: 明示保存の欄がひと通り挙がっている (売価を含む)',
    ids.length >= 15 && ids.includes('f-price') && ids.includes('rk-genre') && ids.includes('y-price'),
    `ids=${ids.length}`);
  check('未保存ガード: 挙げた欄の id が画面に実在する', missing.length === 0, missing.join(','));

  // ── 実際に往復させる (素の JS を切り出して、スタブ DOM の上で動かす) ──
  const start = srcDetail.indexOf('  function initUnsavedGuard(KEY) {');
  const end = srcDetail.indexOf('  // ここまでが「未保存ガード」の切り出し範囲', start);
  const chunk = start >= 0 && end > start ? srcDetail.slice(start, end) : '';
  check('未保存ガード: detail.ejs から initUnsavedGuard を切り出せる',
    chunk.length > 1000 && !chunk.includes('<%'), `len=${chunk.length}`);

  if (chunk) {
    // タブの外に置いた欄・ボタンだけを用意する。用意しない id は getElementById が null を返す
    // = ガードが「その商品では出ていない欄」として追わない、という本番と同じ形になる
    const makeEl = (opts = {}) => {
      const el = {
        type: opts.type || 'text', value: opts.value === undefined ? '' : opts.value,
        checked: !!opts.checked, className: '', textContent: '', title: '', disabled: false,
        style: {}, children: [], dataset: opts.dataset || {},
        _classes: new Set(), _handlers: {}, _fired: [],
        classList: {
          add: (c) => el._classes.add(c), remove: (c) => el._classes.delete(c),
          contains: (c) => el._classes.has(c),
          toggle: (c, on) => { if (on) el._classes.add(c); else el._classes.delete(c); },
        },
        closest: (sel) => (sel === '.tab-panel' ? { id: opts.tab || 'tab-basic' } : null),
        addEventListener: (t, fn) => { (el._handlers[t] = el._handlers[t] || []).push(fn); },
        dispatchEvent: (ev) => { el._fired.push(ev && ev.type); (el._handlers[ev.type] || []).forEach((fn) => fn(ev)); return true; },
        insertAdjacentElement: (_pos, node) => { el.children.push(node); return node; },
        appendChild: (node) => { el.children.push(node); return node; },
        replaceChildren: () => { el.children.length = 0; },
        _click: () => (el._handlers.click || []).forEach((fn) => fn({})),
      };
      return el;
    };
    // sessionStorage は「読み直し」をまたいで残る唯一の入れ物 = テストでも 1 つを共有する
    const store = new Map();
    const session = {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => { store.set(k, String(v)); },
      removeItem: (k) => { store.delete(k); },
    };
    // 1 回ぶんの「画面を開く」。dbValues = その時点で DB に入っている値
    function open(dbValues, opts = {}) {
      const at = (id, def, tab) => makeEl({ value: dbValues[id] === undefined ? def : dbValues[id], tab });
      const els = new Map();
      els.set('f-jan', at('f-jan', '', 'tab-category'));
      els.set('y-price', at('y-price', '', 'tab-basic'));
      els.set('f-price', at('f-price', '', 'tab-basic'));
      els.set('f-name', at('f-name', '商品A', 'tab-basic'));
      els.set('f-asin', at('f-asin', '', 'tab-basic'));
      els.set('f-amazon-url', at('f-amazon-url', '', 'tab-basic'));
      // 戻さない欄 (画面のほかの状態と連動するため、印と警告だけ出す)
      els.set('rk-shipping-group', at('rk-shipping-group', 'nekopos', 'tab-basic'));
      els.set('rk-genre', at('rk-genre', '', 'tab-category'));
      els.set('save-basic-btn', makeEl());
      els.set('rk-save-btn-cat', makeEl());
      els.set('unsaved-zone', makeEl());
      // 本番と同じ「ASIN を打つと Amazon URL を作る」連動。復元がこれを走らせないことを見る
      els.get('f-asin').addEventListener('input', () => {
        els.get('f-amazon-url').value = 'https://www.amazon.co.jp/dp/' + els.get('f-asin').value;
      });
      const tabBtns = [makeEl({ dataset: { tab: 'tab-basic' } }), makeEl({ dataset: { tab: 'tab-category' } })];
      const timers = []; const winHandlers = new Map();
      const ctx = {
        document: {
          getElementById: (id) => els.get(id) || null,
          createElement: () => makeEl(),
          querySelectorAll: (sel) => (sel === '#tabbar .tab-btn' ? tabBtns : []),
        },
        sessionStorage: session,
        // 離脱 (pagehide) と、退避を捨てるタイマーは手で動かして確かめる
        window: { addEventListener: (t, fn) => { winHandlers.set(t, fn); } },
        setTimeout: (fn) => { timers.push(fn); return timers.length; },
        Event: class { constructor(type) { this.type = type; } },
        Date, JSON, console,
      };
      vm.createContext(ctx);
      new vm.Script(chunk, { filename: 'initUnsavedGuard' }).runInContext(ctx);
      const api = ctx.initUnsavedGuard('ph-unsaved:9');
      const fire = (id, type) => {
        const el = els.get(id);
        (el._handlers[type] || []).forEach((fn) => fn({ type }));
      };
      return {
        api, els, tabBtns, val: (id) => els.get(id).value,
        // 「実際に出ていく」= pagehide / 「取り消して編集を続ける」= 入力を触る
        leave: () => { const fn = winHandlers.get('pagehide'); if (fn) fn(); },
        type: (id, v) => { els.get(id).value = v; fire(id, 'input'); },
      };
    }

    // ① 売価を打って読み直す = 戻ってくる (今回の症状そのもの)
    const a = open({ 'f-price': '' });
    a.els.get('f-price').value = '1980';
    check('未保存ガード: 打った時点で「未保存」に数える', a.api.dirtyLabels().join(',') === '売価', a.api.dirtyLabels().join(','));
    a.api.stash();
    check('未保存ガード: 読み直す前に退避される', store.size === 1, String(store.size));
    const b = open({ 'f-price': '' });
    check('🚨 売価を打ってからセット判断などで読み直しても消えない', b.val('f-price') === '1980', b.val('f-price'));
    check('未保存ガード: 戻したことを画面で知らせる', b.els.get('unsaved-zone').children.length === 1);
    check('未保存ガード: 戻した欄は「未保存」のまま (保存はしていない)', b.api.dirtyLabels().join(',') === '売価');
    check('未保存ガード: 退避は 1 回で使い切る (次に開いた人に古い値が出ない)', store.size === 0);
    const c = open({ 'f-price': '' });
    check('未保存ガード: 2 回目の読み直しでは何も戻さない', c.val('f-price') === '' && c.els.get('unsaved-zone').children.length === 0);

    // ② すでに保存されていた欄は戻さない (保存 → 読み直しで古い値が復活しない)
    const d = open({ 'f-price': '' });
    d.els.get('f-price').value = '1980';
    d.api.stash();
    const e = open({ 'f-price': '1980' });   // 保存が通った後の画面
    check('未保存ガード: 保存済みの値は「戻した」と言わない', e.els.get('unsaved-zone').children.length === 0);
    check('未保存ガード: 保存済みなら未保存の印も出ない', e.api.dirtyLabels().length === 0);

    // ③ 退避のあいだに別の人が変えていたら戻さない (他人の変更を黙って隠さない)
    const f = open({ 'f-price': '' });
    f.els.get('f-price').value = '1980';
    f.api.stash();
    const g = open({ 'f-price': '2500' });   // 別の人が先に 2500 で保存した
    check('🚨 未保存ガード: 退避中に他の人が変えた欄は戻さない', g.val('f-price') === '2500', g.val('f-price'));
    check('未保存ガード: 戻さなかったことは知らせる', g.els.get('unsaved-zone').children.length === 1);

    // ④ 未保存が無ければ何も残さない / 触ったタブに印が出る
    const h = open({ 'f-price': '1000' });
    check('未保存ガード: 未保存が無ければ退避しない', h.api.stash() === true && store.size === 0);
    h.els.get('rk-genre').value = '100371';
    h.els.get('rk-genre').dispatchEvent(new (class { constructor() { this.type = 'input'; } })());
    check('未保存ガード: 触ったタブに印が出る (カテゴリ・属性タブ)',
      h.tabBtns[1].children[0]._classes.has('on') === true && h.tabBtns[0].children[0]._classes.has('on') === false);
    check('未保存ガード: 印はその欄を保存するボタンの横に出る',
      h.els.get('rk-save-btn-cat').children[0]._classes.has('on') === true
      && h.els.get('save-basic-btn').children[0]._classes.has('on') === false);

    // ⑤ 保存の送信中に打ち直した分を「保存済み」にしない (Codex R1)
    //    送った値 (saving) を控え、応答後の画面の値では基準を進めない
    const i = open({ 'rk-genre': '100' });
    i.els.get('rk-genre').value = '200';
    const snap = i.api.saving(['rakuten']);   // ここで 200 を送った
    i.els.get('rk-genre').value = '300';      // 通信中に人が打ち直した
    i.api.saved(snap);
    check('🚨 未保存ガード: 保存中に打ち直した分は未保存のまま残る',
      i.api.dirtyLabels().join(',') === 'ジャンルID', i.api.dirtyLabels().join(','));
    i.api.stash();
    const j = open({ 'rk-genre': '200' });    // DB は送った 200 になっている
    check('🚨 未保存ガード: 保存中に打ち直した値は読み直しても残る', j.val('rk-genre') === '300', j.val('rk-genre'));

    // ⑥ 自分の保存を「他人の変更」と誤判定しない (Codex R1)
    //    保存 → 読み直しの間に打ち足した分を、競合として捨てない
    const k = open({ 'f-price': '1000' });
    k.els.get('f-price').value = '1980';
    const snapK = k.api.saving(['basic']);
    k.api.saved(snapK);                        // 保存が通った (まだ読み直していない)
    k.els.get('f-price').value = '2500';       // そのあと打ち足した
    k.api.stash();
    const l = open({ 'f-price': '1980' });     // 読み直すと DB は自分が保存した 1980
    check('🚨 未保存ガード: 自分の保存を他人の変更と間違えない', l.val('f-price') === '2500', l.val('f-price'));

    // ⑦ 退避できなければ、離脱の警告を消さない (Codex R1)
    const m = open({ 'f-price': '' });
    m.els.get('f-price').value = '1980';
    const realSet = session.setItem;
    session.setItem = () => { throw new Error('QuotaExceededError'); };
    const kept = m.api.stash();
    session.setItem = realSet;
    check('🚨 未保存ガード: 退避できなければ false を返す (読み直しの前に警告を残す)', kept === false);

    // ⑧ 復元はほかの欄を書き換えるハンドラを走らせない (Codex R1)
    //    ASIN の復元で Amazon URL が書き換わると、競合で守った欄を上書きしてしまう
    store.clear();
    const n = open({ 'f-asin': '', 'f-amazon-url': '' });
    n.els.get('f-asin').value = 'B00TEST123';
    n.api.stash();
    const o = open({ 'f-asin': '', 'f-amazon-url': 'https://www.amazon.co.jp/dp/OTHER' });
    check('🚨 未保存ガード: 復元は連動ハンドラを走らせない (別の欄を上書きしない)',
      o.val('f-asin') === 'B00TEST123' && o.val('f-amazon-url') === 'https://www.amazon.co.jp/dp/OTHER',
      o.val('f-amazon-url'));
    check('未保存ガード: 売価だけは戻したあとに表示を描き直す (自分の欄しか触らないため)',
      /refresh: true/.test(chunk) && (chunk.match(/refresh: true/g) || []).length === 1);

    // ⑨ 戻さない欄 (配送方法など) は書き戻さないが、未保存としては数える
    store.clear();
    const p = open({ 'rk-shipping-group': 'nekopos' });
    p.els.get('rk-shipping-group').value = 'teikeigai';
    check('未保存ガード: 戻さない欄も「未保存」に数える', p.api.dirtyLabels().join(',') === '配送方法');
    check('🚨 未保存ガード: 戻さない欄が残っていれば、退避しても警告は消さない', p.api.stash() === false);
    const q = open({ 'rk-shipping-group': 'nekopos' });
    check('未保存ガード: 戻さない欄は書き戻さない (画面のほかの状態と食い違わせない)',
      q.val('rk-shipping-group') === 'nekopos');

    // ⑩ 古い退避は使わない (読み直しが流れて、だいぶ経ってから値が現れるのを防ぐ)
    store.clear();
    const r = open({ 'f-price': '' });
    r.els.get('f-price').value = '1980';
    r.api.stash();
    const raw = JSON.parse(store.get('ph-unsaved:9'));
    raw.at = Date.now() - 31 * 60 * 1000;
    store.set('ph-unsaved:9', JSON.stringify(raw));
    const s = open({ 'f-price': '' });
    check('未保存ガード: 30 分より古い退避は使わない', s.val('f-price') === '');

    // ⑪ Yahoo!保存は「送った欄」だけを保存済みにする (Codex R2)
    //    まとめて基本情報を保存済みにすると、売価を打ったまま Yahoo!を保存した人の入力が消える
    store.clear();
    const t = open({ 'f-price': '1000', 'y-price': '' });
    t.els.get('f-price').value = '1980';
    t.els.get('y-price').value = '2200';
    t.api.saved(t.api.saving(['yahoo'], ['y-tax']));   // Yahoo!項目を保存した
    check('🚨 未保存ガード: Yahoo!を保存しても、売価の未保存は消えない',
      t.api.dirtyLabels().join(',') === '売価', t.api.dirtyLabels().join(','));
    t.api.stash();
    const u = open({ 'f-price': '1000', 'y-price': '2200' });
    check('🚨 未保存ガード: Yahoo!保存のあとに読み直しても売価が戻る', u.val('f-price') === '1980', u.val('f-price'));

    // ⑫ JAN は戻さない (「IDあり/なし」のラジオと入力枠が DB のままで食い違う)
    store.clear();
    const v = open({ 'f-jan': '' });
    v.els.get('f-jan').value = '4901234567894';
    check('未保存ガード: JAN も「未保存」には数える', v.api.dirtyLabels().join(',') === 'JANコード');
    check('未保存ガード: 戻せない欄なので警告は残す', v.api.stash() === false);
    const w = open({ 'f-jan': '' });
    check('🚨 未保存ガード: JAN は書き戻さない (画面に出ていない JAN を送らせない)', w.val('f-jan') === '');

    // ⑬ 退避は「実際に出ていく瞬間の入力」と結びつける (Codex R2 / R3)
    //    時間では「読み直しの取り消し」と「読み込み待ち」を区別できないので、タイマーには頼らない
    store.clear();
    const x = open({ 'f-price': '1000' });
    x.type('f-price', '1980');
    check('未保存ガード: 読み直しの前に退避できるか確かめている', x.api.stash() === true && store.size === 1);
    x.type('f-price', '1000');   // 取り消して、値を元に戻した
    check('🚨 未保存ガード: 読み直しをやめて編集を続けたら退避を捨てる', store.size === 0);
    const y = open({ 'f-price': '1000' });
    check('未保存ガード: そのあと自分で再読み込みしても、取り消した値は復活しない', y.val('f-price') === '1000');

    // 読み込みが遅れても退避は消えない。出ていく瞬間の値で書き直す
    store.clear();
    const z = open({ 'f-price': '1000' });
    z.type('f-price', '1980');
    z.api.stash();
    z.type('f-price', '2500');   // 読み直しを待っている間に打ち直した (= 取り消し扱いで一度消える)
    check('未保存ガード: 打ち直した時点では退避は消えている', store.size === 0);
    z.api.stash();               // 読み直しをやり直した
    z.leave();                   // ここで実際に出ていく
    check('🚨 未保存ガード: 出ていく瞬間の値が退避される', store.size === 1
      && JSON.parse(store.get('ph-unsaved:9')).values['f-price'] === '2500',
      store.get('ph-unsaved:9'));
    const w2 = open({ 'f-price': '1000' });
    check('🚨 未保存ガード: 読み込みが遅れても値は戻る', w2.val('f-price') === '2500', w2.val('f-price'));

    // ふつうの離脱 (タブを閉じる・別ページへ行く) では退避しない。警告を見て出ていくのは捨てる意思
    store.clear();
    const v2 = open({ 'f-price': '1000' });
    v2.type('f-price', '1980');
    v2.leave();
    check('未保存ガード: 読み直し以外の離脱では退避しない', store.size === 0);
  }
}

console.log(failed === 0 ? '\nSMOKE: ALL PASS' : `\nSMOKE: ${failed} FAILED`);
process.exit(failed === 0 ? 0 : 1);
