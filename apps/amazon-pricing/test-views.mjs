/**
 * test-views.mjs — 画面の smoke。
 *
 *   A. テンプレート単体: EJS が描画できる / script タグの対応 / 未処理タグ無し
 *   B. ★実物のルートを叩く: 仮の DATA_DIR に mirror DB を作ってフィクスチャを入れ、
 *      express に router を載せて GET/POST を本当に流す (テンプレに渡し忘れた変数は
 *      ここでしか見つからない — feedback_開発プロセス 2026-09-04)
 *
 * 実行: node apps/amazon-pricing/test-views.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import ejs from 'ejs';
import express from 'express';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '../..');
let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const OPEN_TAG = '<' + 'script';
const CLOSE_TAG = '<' + '/script' + '>';

// ── 仮の DATA_DIR (router を import する前に決める。mirror DB はここに作られる) ──
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ap-views-'));
process.env.DATA_DIR = tmp;

const { default: router, VIEW_HELPERS } = await import('./router.js');
const { getDB } = await import('./db.js');
// 本番では server.js が起動時に initMirrorDB() を呼ぶ。ここでも同じ順で呼ぶ (getMirrorDB は自動初期化しない)
const { initMirrorDB } = await import('../warehouse-mirror/db.js');
initMirrorDB();

// ─── A. テンプレート単体 ───
const live = { action: 'lower', proposedPrice: 1900, reasonCode: 'MATCH_BUYBOX', reasonText: 'カート価格に合わせる (2,000 → 1,900 円, -5.0%)', confidence: 0.8,
  flags: ['NO_SALES_30D'], computedFloor: 1750, effectiveFloor: 1750, grossNow: { gross: 400, rate: 0.2 }, costs: { feeRateAssumed: false } };
const row = { seller_sku: 'pr_x', asin: 'B000X', channel: 'FBA', ne_code: 'ne-x', ne_name: 'テスト商品', parts: 1, my_price: 2000, buybox_price: 1900, buybox_is_mine: 0,
  cost_incl_tax: 1100, cost_missing_parts: 0, referral_fee_rate: 0.1, fba_fee: 400, per_item_fee: 0, variable_closing_fee: 0, ship_cost: null, units_30d: 5, sales_30d: 10000,
  snapshot_date_jst: '2026-09-07', mode: 'buybox', floor_price: 1800, ceiling_price: null, offset_jpy: 0, min_margin_rate: null, policy_note: null,
  policy_updated_at: '2026-09-07T01:00:00.000Z', policy_updated_by: 't@example.com', has_policy: true,
  live, computed_floor: 1750, effective_floor: 1800, gross_now: { gross: 400, rate: 0.2 }, buybox_gap: 100, fee_now: 600, stock: 12 };
const evil = 'x' + CLOSE_TAG + OPEN_TAG + '>alert(1)' + CLOSE_TAG + '"><img src=x onerror=alert(1)>';
const evilRow = { ...row, seller_sku: evil, ne_name: evil, policy_note: evil, live: { ...live, reasonText: evil } };
const typeRow = { type_id: 1, name: 'カート・値上げのみ', basis: 'buybox', rival_scope: 'all', direction: 'up_only', offset_kind: 'jpy', offset_value: -10,
  amazon_seller: 'include', prime_as: 'fba', points: 'price_only', solo_raise: 'none', note: null, archived_at: null,
  created_at: '2026-09-10T00:00:00.000Z', created_by: 't', updated_at: '2026-09-10T00:00:00.000Z', updated_by: 't', summary: 'カート価格・値上げのみ・−10円', needs_offers: [], used_by: 1 };
const commonData = { displayName: 'テスト', isAdmin: true, customTypes: [typeRow], typeNames: { 1: typeRow.name }, ...VIEW_HELPERS };
const cases = [
  ['index.ejs', { ...commonData, title: 'x', unavailable: null, rows: [row, evilRow], total: 2, page: 1, per: 100, pages: 1,
    filters: { q: '', mode: '', channel: '', action: '', flag: '', sort: 'units', price_min: '', price_max: '', adv: '' }, stats: { total: 2, with_policy: 1, by_action: { raise: 0, lower: 1, keep: 1, hold: 0 }, flags: { below_floor: 1 } },
    run: { started_at: '2026-09-07T00:00:00.000Z' }, freshness: { snapshot_date_jst: '2026-09-07', snapshot_rows: 2, fees_fetched_at: '2026-09-07T00:00:00.000Z', fees_rows: 2, finance_last_date_jst: '2026-09-01' },
    auto: { skipped: true }, flagLabels: { below_floor: '赤字' } }],
  ['index.ejs', { ...commonData, title: 'x', unavailable: ['mirror_products'], rows: [], total: 0, page: 1, per: 100, pages: 1, filters: {}, stats: { total: 0, with_policy: 0, by_action: { raise: 0, lower: 0, keep: 0, hold: 0 }, flags: {} }, run: null, freshness: null, auto: null, flagLabels: {} }],
  ['listing.ejs', { ...commonData, title: 'x', row: evilRow,
    events: [{ at: '2026-09-07T01:00:00.000Z', field: 'floor_price', old_value: null, new_value: '1800', reason_code: 'initial', reason_text: evil, actor_id: 't', actor_type: 'human' }],
    evaluations: [{ evaluated_at: '2026-09-07T00:00:00.000Z', rule_version: 'rule:ap-v1', action: 'lower', proposed_price: 1900, current_price: 2000, reason_text: evil, review_verdict: 'agree', review_by: 't', review_at: '2026-09-07T02:00:00.000Z', review_comment: evil }],
    history: [{ date_jst: '2026-09-07', my_price: 2000, buybox_price: 1900, buybox_is_mine: 0 }] }],
  ['listing.ejs', { ...commonData, title: 'x', row: null, events: [], evaluations: [], history: [] }],
  ['evaluations.ejs', { ...commonData, title: 'x', run: { run_id: 'apr-1', started_at: '2026-09-07T00:00:00.000Z', snapshot_date_jst: '2026-09-07', trigger: 'page_open', listings_total: 2 }, auto: { skipped: true }, tab: 'change',
    evals: [{ decision_id: 1, seller_sku: evil, asin: 'B000X', current_price: 2000, action: 'lower', proposed_price: 1900, reason_text: evil, flags: ['BELOW_COST_FLOOR', 'NO_SALES_30D'], confidence: 0.8, review_verdict: null, review_comment: evil }],
    shownTotal: 1, limit: 300, counts: { all: 2, change: 1, raise: 0, lower: 1, hold: 0, keep: 1, reviewed: 0 }, reviews: { agree: 0, disagree: 0, unsure: 0 },
    runs: [{ started_at: '2026-09-07T00:00:00.000Z', trigger: 'page_open', actor_id: 't', snapshot_date_jst: '2026-09-07', rule_version: 'rule:ap-v1', status: 'success', listings_total: 2, error: null, summary: { by_action: { raise: 0, lower: 1, keep: 1, hold: 0 } } }] }],
  ['history.ejs', { ...commonData, title: 'x', sku: '', total: 1, groups: [{ at: '2026-09-07T01:00:00.000Z', seller_sku: evil, actor_id: 't', actor_type: 'human', reason_code: 'initial', reason_text: evil, source: 'ui',
    changes: [{ field: 'mode', old_value: null, new_value: 'buybox' }, { field: 'custom_type_id', old_value: null, new_value: '1' }, { field: 'custom_type_id', old_value: '1', new_value: '77' }] }] }],
  // カスタムの型が付いた出品 (方針カードの分岐)
  ['listing.ejs', { ...commonData, title: 'x', row: { ...row, mode: 'custom', custom_type_id: 1, custom_type_name: typeRow.name, custom_type: typeRow, live: { ...live, action: 'keep', reasonCode: 'DIRECTION_UP_ONLY', reasonText: '型が「値上げのみ」' } },
    events: [{ at: '2026-09-10T00:00:00.000Z', field: 'custom_type_id', old_value: null, new_value: '1', reason_code: 'initial', reason_text: '', actor_id: 't', actor_type: 'human' }], evaluations: [], history: [] }],
  ['custom-types.ejs', { ...commonData, title: 'x', types: [typeRow, { ...typeRow, type_id: 2, name: evil, note: evil, basis: 'lowest', archived_at: '2026-09-10T00:00:00.000Z', needs_offers: ['基準=最安値'], used_by: 0 }],
    groups: [{ change_group: 'g', at: '2026-09-10T00:00:00.000Z', actor_id: 't', actor_type: 'human', type_id: 1, reason_text: evil,
      changes: [{ field: 'name', old_value: null, new_value: evil }, { field: 'direction', old_value: 'both', new_value: 'up_only' }, { field: 'archived_at', old_value: null, new_value: '2026-09-10T00:00:00.000Z' }] }] }],
  ['custom-types.ejs', { ...commonData, title: 'x', types: [], groups: [] }],
];
console.log('\n── A. テンプレート単体 ──');
for (const [name, data] of cases) {
  const file = path.join(HERE, 'views', name);
  let html;
  try { html = ejs.render(fs.readFileSync(file, 'utf8'), data, { filename: file }); ok(true, `${name}: 描画できる`); }
  catch (e) { ok(false, `${name}: 描画失敗 — ${e.message}`); continue; }
  const opens = (html.match(new RegExp(OPEN_TAG, 'gi')) || []).length;
  const closes = (html.match(new RegExp(CLOSE_TAG, 'gi')) || []).length;
  ok(opens === closes, `${name}: script の開始/終了タグ数が一致 (${opens}/${closes})`);
  ok(!html.includes('<%'), `${name}: 未処理の EJS タグが無い`);
  // 文字としては残ってよい (エスケープ済み)。「<img ...>」がタグのまま出ていないこと・script が閉じられていないことを見る
  ok(!html.includes('<img src=x onerror=alert(1)>') && !html.includes(CLOSE_TAG + OPEN_TAG + '>alert(1)'), `${name}: 値に細工があってもタグとして出ない`);
  ok(html.includes('この画面から Amazon の価格は変わりません'), `${name}: 「価格は変わらない」の断り書きが出る`);
}
for (const name of ['_top.ejs', '_policy_dialog.ejs', 'index.ejs', 'listing.ejs', 'evaluations.ejs', 'history.ejs', 'custom-types.ejs']) {
  const src = fs.readFileSync(path.join(HERE, 'views', name), 'utf8');
  const inScript = src.split(OPEN_TAG).slice(1).map((s) => s.split(CLOSE_TAG)[0]);
  ok(inScript.every((s) => !s.includes(CLOSE_TAG)), `${name}: inline script の中に終了タグ文字列が無い`);
}

// ─── B. 実物のルート ───
console.log('\n── B. 実物のルートを叩く ──');
const db = getDB();   // 仮 DATA_DIR に本番と同じ DDL で warehouse-mirror.db ができる
const T = '2026-09-07T00:00:00.000Z';
db.prepare(`INSERT INTO mirror_amazon_sku_fees (seller_sku, asin, fulfillment_channel, referral_fee_rate, fba_fee, per_item_fee, variable_closing_fee, fetched_at) VALUES (?,?,?,?,?,?,?,?)`)
  .run('pr_fba1', 'B000FBA1', 'FBA', 0.10, 400, 0, 0, T);
db.prepare(`INSERT INTO mirror_amazon_sku_fees (seller_sku, asin, fulfillment_channel, referral_fee_rate, fba_fee, per_item_fee, variable_closing_fee, fetched_at) VALUES (?,?,?,?,?,?,?,?)`)
  .run('pr_nocost', 'B000NC', 'FBM', 0.15, null, 0, 0, T);
db.prepare(`INSERT INTO mirror_amazon_price_snapshot_daily (date_jst, seller_sku, asin, my_price, buybox_price, buybox_is_mine, source_run_id, source_row_hash, synced_at) VALUES (?,?,?,?,?,?,?,?,?)`)
  .run('2026-09-07', 'pr_fba1', 'B000FBA1', 2000, 1900, 0, 'r', 'h', T);
db.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, synced_at) VALUES (?,?,?,?,?,?)`).run('pr_fba1', 'ne-a', 1, 'master', 'A商品', T);
db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価, 原価状態, 送料, 消費税率, updated_at) VALUES (?,?,?,?,?,?,?,?)`).run('ne-a', 'A商品', '単品', 1000, 'COMPLETE', 200, 0.10, T);
db.prepare(`INSERT INTO mirror_inv_daily_detail (business_date, market, category, source_system, source_item_code, ne_code, qty, cost_status, synced_at) VALUES (?,?,?,?,?,?,?,?,?)`)
  .run('2026-09-07', 'jp', 'fba_warehouse', 'sp', 'pr_fba1', 'ne-a', 12, 'ok', T);

const app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(ROOT, 'views'));
app.use((req, _res, next) => { req.session = { authenticated: true, email: 't@example.com', displayName: 'テスト', role: 'admin', allowedApps: '*' }; next(); });
app.use('/apps/amazon-pricing', router);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const base = `http://127.0.0.1:${server.address().port}`;
const get = async (p) => { const r = await fetch(base + p); return { status: r.status, text: await r.text(), headers: r.headers }; };
const post = async (p, body, headers = {}) => {
  const r = await fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: base, ...headers }, body: JSON.stringify(body) });
  let json = null; try { json = await r.json(); } catch { /* not json */ }
  return { status: r.status, json };
};

try {
  const idx = await get('/apps/amazon-pricing/');
  ok(idx.status === 200, `GET / → ${idx.status}`);
  ok(idx.text.includes('pr_fba1') && idx.text.includes('A商品'), '  出品が表に出る');
  ok(idx.text.includes('原価不明'), '  原価不明の行に札が出る');
  ok(idx.text.includes('data-mode="fba_lowest"') && idx.text.includes('data-mode="off"'), '  プライスター風の価格追従ボタン列が出る');
  ok(idx.text.includes('data-field="floor_price"') && idx.text.includes('data-field="ceiling_price"'), '  ストッパーの直接入力欄が出る');
  ok(/pt-box[^>]*>12</.test(idx.text), '  数量 (FBA 倉庫 12) が出る');
  ok(idx.text.includes('id="bulk-floor-btn"') && idx.text.includes('id="sel-all"'), '  チェックと一括の操作が出る');
  const runs = db.prepare(`SELECT COUNT(*) c FROM ap_evaluation_runs WHERE status='success'`).get().c;
  ok(runs === 1, `  画面を開いたついでに今日の判定が 1 回作られる (実際 ${runs})`);
  const autoRun = db.prepare(`SELECT trigger, actor_id FROM ap_evaluation_runs WHERE status='success'`).get();
  ok(autoRun.trigger === 'page_open' && autoRun.actor_id === 'system:page_open', `  その run の主体は開いた人ではなく system (${autoRun.actor_id})`);
  const idx2 = await get('/apps/amazon-pricing/?q=fba1&channel=FBA&sort=margin&flag=cost_unknown');
  ok(idx2.status === 200 && !idx2.text.includes('>pr_fba1<'), 'GET / 絞り込み (原価不明 + fba1 → 該当なし)');
  ok(db.prepare(`SELECT COUNT(*) c FROM ap_evaluation_runs`).get().c === 1, '  2 回目の表示では run を増やさない');
  const idx3 = await get('/apps/amazon-pricing/?mode=tracking&channel=FBA&flag=stopped_at_floor&price_min=100&price_max=5000&per=150&adv=1');
  ok(idx3.status === 200 && idx3.text.includes('該当する出品がありません') && idx3.text.includes('id="adv-toggle" class="adv-toggle" name="adv" value="1" checked'), 'GET / 詳細検索 (追従する + FBA + 下げ止まり + 価格 100〜5000 → 該当なし、詳細検索は開いたまま)');
  const idx4 = await get('/apps/amazon-pricing/?price_min=1000&price_max=3000&sort=stock');
  ok(idx4.status === 200 && idx4.text.includes('>pr_fba1<'), 'GET / 出品価格 1000〜3000 + 数量順 → pr_fba1 (2000 円) が残る');

  const lst = await get('/apps/amazon-pricing/listings/pr_fba1');
  // 原価 1000×1.10 = 1100 + FBA 400 = 1500 ÷ (1 − 0.10 − 0.10) = 1875
  ok(lst.status === 200 && lst.text.includes('値付けの方針') && lst.text.includes('1,875'), `GET /listings/pr_fba1 → ${lst.status} (計算した下限 1,875 が出る)`);
  const nf = await get('/apps/amazon-pricing/listings/nope');
  ok(nf.status === 404 && nf.text.includes('見つかりません'), `GET /listings/nope → ${nf.status}`);

  const ev = await get('/apps/amazon-pricing/evaluations');
  ok(ev.status === 200 && ev.text.includes('実際には何も送っていません'), `GET /evaluations → ${ev.status}`);
  const evAll = await get('/apps/amazon-pricing/evaluations?tab=all');
  ok(evAll.status === 200 && evAll.text.includes('pr_fba1') && evAll.text.includes('rv-btn'), '  tab=all で判定行と採点ボタンが出る');

  const hist0 = await get('/apps/amazon-pricing/history');
  ok(hist0.status === 200 && hist0.text.includes('まだありません'), `GET /history (空) → ${hist0.status}`);

  // CSRF: Origin 無し → 403 / JSON 以外 → 415
  const noOrigin = await fetch(base + '/apps/amazon-pricing/api/policies/pr_fba1', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
  ok(noOrigin.status === 403, `POST 方針 (Origin 無し) → ${noOrigin.status}`);
  const notJson = await fetch(base + '/apps/amazon-pricing/api/policies/pr_fba1', { method: 'POST', headers: { 'Content-Type': 'text/plain', Origin: base }, body: '{}' });
  ok(notJson.status === 415, `POST 方針 (text/plain) → ${notJson.status}`);

  const bad = await post('/apps/amazon-pricing/api/policies/pr_fba1', { mode: 'buybox', floor_price: 0, reason_code: 'initial' });
  ok(bad.status === 400 && /0 は入れられません/.test(bad.json?.error || ''), `POST 方針 (ストッパー 0) → ${bad.status} ${bad.json?.error}`);
  const saved = await post('/apps/amazon-pricing/api/policies/pr_fba1', { mode: 'buybox', floor_price: '1800', ceiling_price: '', offset_jpy: '-10', min_margin_rate: '10', note: 'メモ', reason_code: 'initial', reason_text: '' });
  ok(saved.status === 200 && saved.json?.ok && saved.json.changed.length === 5, `POST 方針 (初回) → ${saved.status} 変更 ${saved.json?.changed?.length} (既定と違う列だけ: mode / floor / offset / margin / note)`);
  ok(saved.json?.live?.action === 'lower' && saved.json.live.proposed_price === 1890, `  返ってくる live 判定: ${saved.json?.live?.action} → ${saved.json?.live?.proposed_price} (カート 1900 − 10)`);
  // 一覧の直接入力 (プライスター風): ストッパーだけ・モードだけの部分更新が効く
  const inl = await post('/apps/amazon-pricing/api/policies/pr_fba1', { floor_price: '1850', reason_code: 'inline' });
  ok(inl.status === 200 && inl.json?.ok && inl.json.changed.length === 1 && inl.json.changed[0] === 'floor_price' && inl.json.policy.mode === 'buybox', `POST 方針 (一覧から赤字ストッパーだけ) → ${inl.status} 変更 ${inl.json?.changed?.join(',')} / モードは buybox のまま`);
  ok(inl.json?.live?.verdict_label === '値下げ' && inl.json.live.verdict_class === 'lower' && inl.json.live.effective_floor === 1875, `  返ってくる live に画面用の別名 (実効下限 = max(ストッパー 1850, 計算 1875)) (${inl.json?.live?.verdict_label} / 実効下限 ${inl.json?.live?.effective_floor})`);
  const inl2 = await post('/apps/amazon-pricing/api/policies/pr_fba1', { mode: 'off', reason_code: 'bulk', reason_text: '一括' });
  ok(inl2.status === 200 && inl2.json?.ok && inl2.json.changed.length === 1 && inl2.json.live.verdict_class === 'keep', `POST 方針 (一括でモード off) → ${inl2.status} 判定 ${inl2.json?.live?.verdict_class}`);
  const back = await post('/apps/amazon-pricing/api/policies/pr_fba1', { mode: 'buybox', floor_price: '1800', reason_code: 'mistake' });
  ok(back.status === 200 && back.json?.ok, '  元に戻す (モード buybox・ストッパー 1800)');
  const unknownSku = await post('/apps/amazon-pricing/api/policies/nope', { mode: 'off', reason_code: 'stop' });
  ok(unknownSku.status === 400, `POST 方針 (無い SKU) → ${unknownSku.status}`);

  const hist1 = await get('/apps/amazon-pricing/history?sku=pr_fba1');
  ok(hist1.status === 200 && hist1.text.includes('はじめて設定した') && hist1.text.includes('赤字ストッパー'), 'GET /history に記録が出る');

  // ── カスタムの型 ──
  const ct0 = await get('/apps/amazon-pricing/custom-types');
  ok(ct0.status === 200 && ct0.text.includes('まだありません') && ct0.text.includes('id="new-type-btn"'), `GET /custom-types (空) → ${ct0.status}`);
  const ctBad = await post('/apps/amazon-pricing/api/custom-types', { name: '', basis: 'buybox' });
  ok(ctBad.status === 400 && /名前/.test(ctBad.json?.error || ''), `POST 型 (名前なし) → ${ctBad.status} ${ctBad.json?.error}`);
  const ct1 = await post('/apps/amazon-pricing/api/custom-types', { name: 'カート・値上げのみ', direction: 'up_only', offset_kind: 'jpy', offset_value: '-10', reason_text: '初期運用' });
  ok(ct1.status === 200 && ct1.json?.ok && ct1.json.type.type_id === 1 && ct1.json.type.summary === 'カート価格・値上げのみ・−10円', `POST 型 (作る) → ${ct1.status} #${ct1.json?.type?.type_id} ${ct1.json?.type?.summary}`);
  const ct2 = await post('/apps/amazon-pricing/api/custom-types', { name: '最安値', basis: 'lowest' });
  ok(ct2.status === 200 && ct2.json?.type?.needs_offers?.length === 1, '  最安値の型は needs_offers が付く');
  const ctUpdNoReason = await post('/apps/amazon-pricing/api/custom-types/1', { offset_value: '-20' });
  ok(ctUpdNoReason.status === 400 && /理由/.test(ctUpdNoReason.json?.error || ''), `POST 型 (直す・理由なし) → ${ctUpdNoReason.status}`);
  const ctUpd = await post('/apps/amazon-pricing/api/custom-types/1', { offset_value: '-20', reason_text: '負けるので' });
  ok(ctUpd.status === 200 && ctUpd.json?.changed?.length === 1 && ctUpd.json.type.offset_value === -20, `POST 型 (直す) → ${ctUpd.status} 変更 ${ctUpd.json?.changed?.join(',')}`);
  const ctBadId = await post('/apps/amazon-pricing/api/custom-types/abc', { reason_text: 'x' });
  ok(ctBadId.status === 400, `POST 型 (番号が不正) → ${ctBadId.status}`);
  const ctBadId2 = await post('/apps/amazon-pricing/api/custom-types/1abc', { offset_value: '-30', reason_text: 'x' });
  ok(ctBadId2.status === 400, `POST 型 (番号 "1abc") → ${ctBadId2.status} (parseInt で 1 に化けない)`);
  const arcStr = await post('/apps/amazon-pricing/api/custom-types/1/archive', { archived: 'false', reason_text: 'x' });
  ok(arcStr.status === 400 && /true か false/.test(arcStr.json?.error || ''), `POST 型 (archived が文字列) → ${arcStr.status}`);
  const stale = await post('/apps/amazon-pricing/api/custom-types/1', { note: '古い画面から', reason_text: 'x', expected_updated_at: '2020-01-01T00:00:00.000Z' });
  ok(stale.status === 409 && /別の人が/.test(stale.json?.error || ''), `POST 型 (古い画面からの上書き) → ${stale.status}`);
  const fresh = await post('/apps/amazon-pricing/api/custom-types/1', { note: 'いまの画面から', reason_text: 'x', expected_updated_at: ctUpd.json.type.updated_at });
  ok(fresh.status === 200 && fresh.json?.changed?.length === 1, `POST 型 (表示時点の updated_at が一致) → ${fresh.status}`);
  const ctJson = JSON.parse((await get('/apps/amazon-pricing/api/custom-types.json')).text);
  ok(ctJson.ok && ctJson.types.length === 2 && ctJson.types[0].used_by === 0, 'GET /api/custom-types.json (使う型 2)');
  const pcNoType = await post('/apps/amazon-pricing/api/policies/pr_fba1', { mode: 'custom', reason_code: 'margin' });
  ok(pcNoType.status === 400 && /型を選んで/.test(pcNoType.json?.error || ''), `POST 方針 (カスタム・型なし) → ${pcNoType.status}`);
  const pc = await post('/apps/amazon-pricing/api/policies/pr_fba1', { mode: 'custom', custom_type_id: '1', reason_code: 'margin' });
  ok(pc.status === 200 && pc.json?.ok && pc.json.policy.custom_type_id === 1 && pc.json.policy.custom_type_name === 'カート・値上げのみ', `POST 方針 (カスタム #1) → ${pc.status} 型名 ${pc.json?.policy?.custom_type_name}`);
  ok(pc.json?.live?.verdict_class === 'keep' && pc.json.live.reason_code === 'DIRECTION_UP_ONLY', `  live 判定: 値上げのみなので維持 (${pc.json?.live?.reason_code})`);
  const idxC = await get('/apps/amazon-pricing/?mode=custom&custom_type=1');
  ok(idxC.status === 200 && idxC.text.includes('>pr_fba1<') && idxC.text.includes('カート・値上げのみ</div>') && idxC.text.includes('value="custom:1"') && idxC.text.includes('」で絞り込み中') && idxC.text.includes('✕ 解除') && !idxC.text.includes('name="custom_type"'), 'GET / カスタム #1 で絞る → 行と型名・一括の選択肢・「絞り込み中」の札と解除 (hidden では持ち越さない)');
  const idxC2 = await get('/apps/amazon-pricing/?mode=custom&custom_type=2');
  ok(idxC2.status === 200 && idxC2.text.includes('該当する出品がありません'), '  型 #2 で絞ると該当なし');
  const lstC = await get('/apps/amazon-pricing/listings/pr_fba1');
  ok(lstC.status === 200 && lstC.text.includes('カスタムの型') && lstC.text.includes('カート・値上げのみ'), '  出品の画面に型が出る');
  const ctPage = await get('/apps/amazon-pricing/custom-types');
  ok(ctPage.status === 200 && ctPage.text.includes('1 件</a>') && ctPage.text.includes('負けるので') && ctPage.text.includes('data-archived="1"'), 'GET /custom-types に型・使用数・履歴が出る');
  const arcBusy = await post('/apps/amazon-pricing/api/custom-types/1/archive', { archived: true, reason_text: 'やめる' });
  ok(arcBusy.status === 400 && /使っています/.test(arcBusy.json?.error || ''), `POST 型 (使用中を「使わない」) → ${arcBusy.status}`);
  const csvC = await get('/apps/amazon-pricing/api/export.csv?mode=custom');
  ok(csvC.status === 200 && csvC.text.includes(',custom_type,') && csvC.text.includes('カート・値上げのみ'), '  CSV に型の名前の列');
  const pcBack = await post('/apps/amazon-pricing/api/policies/pr_fba1', { mode: 'buybox', floor_price: '1800', reason_code: 'mistake' });
  ok(pcBack.status === 200 && pcBack.json?.policy?.custom_type_id === null, '  方針を buybox に戻すと型が外れる');
  const arc = await post('/apps/amazon-pricing/api/custom-types/2/archive', { archived: true, reason_text: '使わない' });
  ok(arc.status === 200 && arc.json?.type?.archived_at, `POST 型 (「使わない」) → ${arc.status}`);
  ok(JSON.parse((await get('/apps/amazon-pricing/api/custom-types.json?all=1')).text).types.length === 2 && JSON.parse((await get('/apps/amazon-pricing/api/custom-types.json')).text).types.length === 1, '  all=1 で使わない型も出る');
  const histC = await get('/apps/amazon-pricing/history?sku=pr_fba1');
  ok(histC.status === 200 && histC.text.includes('カスタムの型') && histC.text.includes('カート・値上げのみ (#1)'), 'GET /history に型の名前で出る');

  const rerun = await post('/apps/amazon-pricing/api/evaluations/run', {});
  ok(rerun.status === 200 && rerun.json?.ok && rerun.json.summary.by_action.lower === 1, `POST 判定作り直し → ${rerun.status} (値下げ 1)`);
  const decision = db.prepare(`SELECT decision_id FROM ap_evaluations WHERE run_id = ? AND seller_sku = 'pr_fba1'`).get(rerun.json.run.run_id);
  const rv = await post(`/apps/amazon-pricing/api/evaluations/${decision.decision_id}/review`, { verdict: 'agree', comment: 'よい' });
  ok(rv.status === 200 && rv.json?.ok, `POST 採点 → ${rv.status}`);
  const rvBad = await post(`/apps/amazon-pricing/api/evaluations/${decision.decision_id}/review`, { verdict: 'great' });
  ok(rvBad.status === 400, `POST 採点 (不正値) → ${rvBad.status}`);
  const evReviewed = await get('/apps/amazon-pricing/evaluations?tab=reviewed');
  ok(evReviewed.status === 200 && evReviewed.text.includes('よい'), '  採点済みタブにコメントが出る');

  const csv = await get('/apps/amazon-pricing/api/export.csv');
  ok(csv.status === 200 && csv.text.includes('seller_sku,asin') && csv.text.includes('pr_fba1') && csv.text.includes(',stock,') && csv.text.includes(',fee_now,'), `GET /api/export.csv → ${csv.status} (数量・手数料の列あり)`);
  const json = await get('/apps/amazon-pricing/api/listings.json?mode=set');
  const parsed = JSON.parse(json.text);
  ok(json.status === 200 && parsed.count === 1 && parsed.rows[0].live.action === 'lower', `GET /api/listings.json?mode=set → ${json.status} (1 行)`);
  const health = await get('/apps/amazon-pricing/api/health');
  const h = JSON.parse(health.text);
  ok(health.status === 200 && h.writes_to_amazon === false && h.policy_events === 13, `GET /api/health → writes_to_amazon=false, 履歴 ${h.policy_events} (初回 5 + 一覧 1 + 一括 1 + 戻し 2 + カスタム 2 + 戻し 2)`);
} finally {
  server.close();
  try { db.close(); } catch { /* */ }
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows の WAL 残りは無視 */ }
}

console.log(`\n${failed === 0 ? '🎉 ALL PASS' : `❌ ${failed} 件失敗`}`);
process.exit(failed === 0 ? 0 : 1);
