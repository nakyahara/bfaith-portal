/**
 * 商品登録ハブ → 楽天→Yahoo!移行 (RYS) の確定値の引き継ぎ (services/yahoo-confirm.mjs) の試験 (2026-10-22)。
 *
 * 実行: DATA_DIR=<使い捨てのフォルダ> node apps/product-hub/scripts/test-yahoo-confirm.mjs
 *   (本番 DB を触らないため DATA_DIR 必須。ハブの DB と RYS の DB をそのフォルダに作る)
 */
import fs from 'fs';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR を指定してください (本番DBを触らないため必須)');
  process.exit(1);
}
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });

(await import('../../../lib/master-legacy-gate.mjs')).__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open' }));
(await import('../../warehouse-mirror/db.js')).initMirrorDB(); // 本番では server.js が起動時に実行する
const { getDB } = await import('../db.js');
const { buildYahooConfirmPreview, confirmYahooFromPortal, ConfirmError } = await import('../services/yahoo-confirm.mjs');
const { getDB: getRysDB } = await import('../../rakuten-yahoo-sync/db.js');
const { loadConfirmedValues } = await import('../../rakuten-yahoo-sync/services/confirmed-values.js');

let failed = 0;
function check(name, cond, detail = '') {
  if (cond) console.log(`  OK  ${name}`);
  else { failed++; console.error(`  NG  ${name} ${detail}`); }
}

const LEGACY = { mode: 'legacy' };
const db = getDB();
const rys = getRysDB();

function makeDraft(code, { price = 1980, title = 'ヤフー用のタイトル', group = '5', tax = '10%', yahoo = {} } = {}) {
  db.prepare('DELETE FROM product_drafts WHERE ne_code = ?').run(code);
  const id = Number(db.prepare('INSERT INTO product_drafts (ne_code, name, price) VALUES (?, ?, ?)').run(code, '商品 ' + code, price).lastInsertRowid);
  if (title !== null) db.prepare(`INSERT INTO draft_ai_outputs (draft_id, kind, content) VALUES (?, 'yahoo_title', ?)`).run(id, title);
  db.prepare('INSERT INTO draft_rakuten (draft_id, shipping_method_group) VALUES (?, ?)').run(id, group);
  db.prepare(`INSERT INTO draft_yahoo (draft_id, tax_rate, yahoo_price, yahoo_price_sagawa, delivery_label, shipping_override, yahoo_category_id, yahoo_path)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(id, tax, yahoo.price ?? null, yahoo.sagawa ?? null, yahoo.delivery ?? null,
    yahoo.override ? 1 : 0, yahoo.category ?? null, yahoo.path ?? null);
  return db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
}

console.log('── 1. ふつうの商品 (楽天と同じ配送・売価) ──');
{
  const d = makeDraft('YCTEST1');
  const p = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  check('確定できる', p.ok, JSON.stringify(p.reasons));
  check('管理番号 = コードの小文字', p.manageNumber === 'yctest1');
  check('売価 = 基本情報の売価', p.values.yahoo_price === 1980);
  check('配送 = 楽天のネコポス → ネコポス', p.values.delivery_label === 'ネコポス');
  check('税率 10% → 10', p.values.tax_rate === '10');
  check('まだ RYS に無い', p.rys === null);
  const r = await confirmYahooFromPortal(db, d, { actor: 't@x', expectedValues: p.values, expectedVersion: null });
  check('確定した (版 1)', r.changed && r.row.version === 1 && r.row.source === 'portal' && r.row.portal_draft_id === d.id);
  const v = loadConfirmedValues(rys, 'yctest1');
  check('RYS の出品の値になった', v.value_source === 'portal' && v.yahoo_title === 'ヤフー用のタイトル');
  const p2 = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  check('確認すると RYS の値が見える', p2.rys?.version === 1 && p2.rys.source === 'portal');
}

console.log('── 2. 確認した後に値が変わったら確定しない ──');
{
  const d = makeDraft('YCTEST2');
  const p = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  db.prepare('UPDATE product_drafts SET price = 2500 WHERE id = ?').run(d.id);
  const d2 = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(d.id);
  let err = null;
  try { await confirmYahooFromPortal(db, d2, { actor: 't', expectedValues: p.values, expectedVersion: null }); } catch (e) { err = e; }
  check('409 values_changed', err instanceof ConfirmError && err.statusCode === 409 && err.code === 'values_changed', String(err));
  check('RYS には書いていない', loadConfirmedValues(rys, 'yctest2') === null);
}

console.log('── 3. 止める場合 ──');
{
  const noTitle = await buildYahooConfirmPreview(db, makeDraft('YCTEST3', { title: null }), { tax: LEGACY });
  check('タイトル空は止める', !noTitle.ok && noTitle.reasons.some((r) => /タイトル/.test(r)));
  const teikei = await buildYahooConfirmPreview(db, makeDraft('YCTEST4', { group: '1' }), { tax: LEGACY });
  check('定形外の単独は推測で当てない', !teikei.ok && teikei.reasons.some((r) => /定形外/.test(r)));
  const noTax = await buildYahooConfirmPreview(db, makeDraft('YCTEST5', { tax: null }), { tax: LEGACY });
  check('税率が空は止める', !noTax.ok && noTax.reasons.some((r) => /税率/.test(r)));
  const blocked = await buildYahooConfirmPreview(db, makeDraft('YCTEST6'), { tax: { mode: 'blocked', reason: '切替の状態を読めない' } });
  check('税率を決められない (切替の状態を読めない) は止める', !blocked.ok && blocked.reasons.includes('切替の状態を読めない'));
  const sku = makeDraft('YCTEST7');
  db.prepare('INSERT INTO draft_sku_prices (draft_id, sku_code, price) VALUES (?, ?, ?)').run(sku.id, 'yctest7-a', 2200);
  const skuP = await buildYahooConfirmPreview(db, sku, { tax: LEGACY });
  check('SKU ごとに売価が違うと止める', !skuP.ok && skuP.reasons.some((r) => /SKU/.test(r)));
  const prov = makeDraft('YCTEST8');
  db.prepare('UPDATE product_drafts SET provisional_code = 1 WHERE id = ?').run(prov.id);
  const provP = await buildYahooConfirmPreview(db, db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(prov.id), { tax: LEGACY });
  check('仮コードは止める', !provP.ok && provP.reasons.some((r) => /仮/.test(r)));
  const cat = await buildYahooConfirmPreview(db, makeDraft('YCTEST9', { yahoo: { category: 123 } }), { tax: LEGACY });
  check('カテゴリ ID だけ (path 空) は止める', !cat.ok && cat.reasons.some((r) => /path/.test(r)));
}

console.log('── 4. ヤフーだけ別配送 (定形外 → ヤフーはネコポス) と佐川売価 ──');
{
  const d = makeDraft('YCTEST10', { group: '1', tax: '8%', yahoo: { override: true, delivery: 'ネコポス', price: 1580 } });
  const p = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  check('佐川売価の選択待ち', !p.ok && p.needsSagawaChoice && p.reasons.length === 0, JSON.stringify(p));
  check('配送 = 定形外（ヤフーのみネコポス）/ 売価 = Yahoo!売価 / 税率 8', p.values.delivery_label === '定形外（ヤフーのみネコポス）' && p.values.yahoo_price === 1580 && p.values.tax_rate === '8');
  const ps = await buildYahooConfirmPreview(db, d, { tax: LEGACY, sagawaSame: true });
  check('「通常売価と同額」を選ぶと確定できる', ps.ok && ps.values.sagawa_mode === 'same');
  let err = null;
  try { await confirmYahooFromPortal(db, d, { actor: 't', expectedValues: ps.values, expectedVersion: null, sagawaSame: false }); } catch (e) { err = e; }
  check('同額を選ばずに確定は止める', err instanceof ConfirmError && err.code === 'not_ready');
  const r = await confirmYahooFromPortal(db, d, { actor: 't', expectedValues: ps.values, expectedVersion: null, sagawaSame: true });
  check('同額で確定', r.row.sagawa_mode === 'same' && r.row.tax_rate === '8');
  const d2 = makeDraft('YCTEST11', { group: '1', yahoo: { override: true, delivery: '宅急便50サイズ以下', price: 1580, sagawa: 1880 } });
  const p2 = await buildYahooConfirmPreview(db, d2, { tax: LEGACY });
  check('佐川売価が入っていれば price', p2.ok && p2.values.sagawa_mode === 'price' && p2.values.yahoo_price_sagawa === 1880
    && p2.values.delivery_label === '定形外（ヤフーのみ宅急便50サイズ）');
}

console.log('── 5. 移行アプリで確定済みの商品をハブが引き継ぐ / ハブの確定後はこのアプリで変えられない ──');
{
  const { confirmValues } = await import('../../rakuten-yahoo-sync/services/confirmed-values.js');
  confirmValues(rys, { manageNumber: 'yctest12', input: { yahoo_title: '移行アプリで', yahoo_price: 900, delivery_label: 'ネコポス', tax_rate: '10' }, source: 'manual', actor: 'm' });
  const d = makeDraft('YCTEST12');
  const p = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  check('いまの RYS の値 (manual) が見える', p.rys?.source === 'manual');
  const r = await confirmYahooFromPortal(db, d, { actor: 't', expectedValues: p.values, expectedVersion: p.rys.version });
  check('ハブが正に切り替わる (版 2)', r.row.source === 'portal' && r.row.version === 2);
  let err = null;
  try { confirmValues(rys, { manageNumber: 'yctest12', input: { yahoo_title: 'x', yahoo_price: 1, delivery_label: 'ネコポス', tax_rate: '10' }, source: 'manual', actor: 'm', expectedVersion: 2 }); } catch (e) { err = e; }
  check('移行アプリからは変えられない', err?.code === 'portal_owned');
}

console.log('── 6. バリエーション: SKU ごとの売価は楽天の出品と同じ決め方 (SKU 別売価 > NE の標準売価 > 代表の売価) ──');
{
  const ins = db.prepare(`INSERT OR REPLACE INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, 標準売価, updated_at)
    VALUES (?, ?, ?, '1', '取扱中', 'ok', ?, ?, '2026-10-22T00:00:00Z')`);
  ins.run(99101, 'ycvar-a', 'A', 'ycvar', 1000);
  ins.run(99102, 'ycvar-b', 'B', 'ycvar', 2000);
  const d = makeDraft('ycvar', { price: 1000 });
  const p = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  check('NE の標準売価が SKU で違えば止める (SKU 別売価を入れていなくても)', !p.ok && p.reasons.some((r) => /SKU ごとに売価が違います/.test(r)), JSON.stringify(p.reasons));
  db.prepare('UPDATE mirror_products SET 標準売価 = 1500 WHERE product_id IN (99101, 99102)').run();
  const p2 = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  check('全 SKU が同じ売価ならその売価 (代表の売価ではなく NE の標準売価)', p2.ok && p2.values.yahoo_price === 1500, JSON.stringify(p2));
  db.prepare('INSERT INTO draft_sku_prices (draft_id, sku_code, price) VALUES (?, ?, ?)').run(d.id, 'ycvar-b', 1800);
  const p3 = await buildYahooConfirmPreview(db, d, { tax: LEGACY });
  check('画面の SKU 別売価が NE より優先 (違えば止める)', !p3.ok && p3.reasons.some((r) => /¥1,500 \/ ¥1,800/.test(r)), JSON.stringify(p3.reasons));
  db.prepare('DELETE FROM mirror_products WHERE product_id IN (99101, 99102)').run();
}

if (failed) { console.error(`FAILED: ${failed}`); process.exit(1); }
console.log('ALL CHECKS PASSED');
