/**
 * test-master-draft-cancel.mjs — 新商品の下書きを消す (0071・lib/master-draft-cancel.mjs・中原さんの頼み 10/11 の場面 a) を PGlite で確かめる
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 切替の後・持ち主は全部 company・今日の照合の回・新規開始の許可・代表の数えの記録)。
 * 書き込みは画面のロール master_edit (本物の関数の権限)。product-hub = 一時の DATA_DIR の SQLite (本物の取り込み・本物の除外)。
 * 固定する契約:
 *   [M] migration の写し: ops.master_write_allowed (0067)・core.guard_master_edit_jan (0053)・ops.transition_sku_registration (0070 = #1683) は前の最新の版から写して「🆕 0071」の行だけ足した
 *       (印の行を外すと前の版と 1 字も違わない)・約束の操作の CHECK = 前の最新の CHECK の全部 (+ 0070 の sku_gone_cancel) + sku_draft_cancel・
 *       security definer の関数は search_path = pg_catalog, pg_temp・public の実行権なし・一時の表を使わない
 *   [C] 確かめ (ops.sku_draft_cancel_check): 消せる下書き = ok と片付けの見込み / 理由の 1 つ 1 つ (下書きでない・一度配った・NE に現れた・まとまりの子・ほかの商品の親・
 *       セットの構成品・ほかのセットの構成の依頼・出品の構成・削除済みの出品の構成・カードを取り込み中)
 *   [F] 消す (lib): cancelled・登録の状態の履歴 (人・理由・根拠 = DB が作った片付け)・保存の記録 done・押し直し = 前の答え・同じ番号で違う理由 = request_id_reused・
 *       理由 / 確かめの窓が要る (400)・MASTER_EDIT_OPEN なし = before_cutover・無いコード = 404・もう一度 = not_cancellable・消したコードはもう使えない (code_taken)
 *   [K] 片付け: 作っただけの NE 登録の CSV を使わない (ほかの商品は作り直せる)・セットの下書きの構成の依頼を取り下げ (構成品の下書きが消せるようになる)・
 *       JAN を外す (同じ JAN で登録し直せる・変更の記録)・まだのカードの知らせ = 済み「作らない」(もう借りられない・見張りの数に入らない)・
 *       取り込んだカード = DB は止めない・product-hub の除外していないカード (結んだ・同じコード・出品済みも) を数える = 画面は押せない (Codex #1688 R1 High 2・3)
 *   [T] (Codex #1688 R1 High 1) 運用のロールが transition_sku_registration を直に呼んで下書きをやめる = 断る / 持ち主は通る / 子の廃止は通る
 *   [R] ロール: master_edit = 消す・確かめの実行だけ (登録の状態・知らせの表は直接書けない)・watcher = 確かめだけ・master_ops は消す関数を持たない・
 *       画面のロールが関数を直に呼んでも DB が同じに確かめる・JAN の守り (0053) は今までどおり (約束なしの JAN の書き込みは断る)
 *   [L] 一覧: やめた商品はふだんの一覧・札の数に出さない・「やめた」で絞る / 商品コードでぴったり探す (詳細検索・上の絞る欄) と出る
 *   [S] 画面 (本物の router): 消せる下書き = 「この下書きを消す」と窓・片付けるもの / まとまりの子 = 子の廃止へ案内 / 業務で使える商品 = 何も出さない /
 *       名簿の人だけ (403)・確かめの窓の印・除外していないカード = 409 card_exists・除外すれば通る・カードを読めない = 503・消した後の画面 = やめた・カードは作らない
 *   [U] 画面の JS (Playwright・Chromium): 押す → 窓 → 理由で押せる → 消す → 読み直し = やめた / 携帯の幅で横にはみ出さない (MASTER_EDIT_UI_SKIP=1 で飛ばす)
 *   🆕 [M4][M5] (0091) ops.sku_draft_cancel_check (0071 の版)・ops.cancel_variation_child (0073 の版) は前の最新の版から写して「🆕 0091」の行だけ足した・関数の外で値を変えない
 *   🆕 [Z] (0091・中原さんの決定 2026-10-18) ロジザード登録の CSV に出した (issued) 下書き = 消さない (確かめ lz_issued・lib 409・DB の関数を直に・画面の理由・API 409) /
 *       0091 の前の DB でも lib が同じ理由で断る / 取り消した (voided) だけ = また消せる・消した後は CSV に出せない / まとまりの子の廃止も同じ (lz_issued)。
 *       出すと消すの競り (同時に片方だけ) は scripts/test-master-draft-cancel-pg.mjs [8]
 * 使い方: node scripts/test-master-draft-cancel.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIG = path.join(ROOT, 'db', 'company', 'migrations');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'deldraft-'));
delete process.env.MASTER_EDIT_OPEN;

const { setupVariationDb, NOW_MS, jan13 } = await import('./fixtures/master-variation-db.mjs');
const R = await import('../lib/master-register.mjs');
const V = await import('../lib/master-variation.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const O = await import('../lib/product-hub-outbox.mjs');
const D = await import('../lib/master-draft-cancel.mjs');
const { listSkus, listCounts } = await import('../apps/master-edit/read.mjs');

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const uuid = () => crypto.randomUUID();
const errOf = async (p) => { try { await (typeof p === 'function' ? p() : p); } catch (e) { return e; } return null; };
async function rejects(p, status, reason) {
  const e = await errOf(p);
  assert.ok(e, `断らなかった (${reason})`);
  assert.equal(e.reason, reason, `${e.reason}: ${e.message}`);
  assert.equal(e.status, status, e.message);
  return e;
}

// ═══════════ [M] migration の写し (文字で) ═══════════
const migFiles = fs.readdirSync(MIG).filter((f) => /^\d{4}_.+\.sql$/.test(f)).sort();
const readMig = (f) => fs.readFileSync(path.join(MIG, f), 'utf8').replace(/\r\n/g, '\n');
const M71 = readMig(migFiles.find((f) => f.startsWith('0071_')));
/** 関数の定義の文字 (create [or replace] function <name>( ... $$;)。見つからない = null */
function fnText(text, name) {
  const re = new RegExp(`create (?:or replace )?function ${name.replace(/\./g, '\\.')}\\(`);
  const i = text.search(re);
  if (i < 0) return null;
  const k = text.indexOf(' as $$', i);
  const j = text.indexOf('$$;', k + 6);
  return text.slice(i, j + 3).replace(/^create or replace function/, 'create function');
}
/** 「🆕 0071」の印の行 (と ここから〜ここまで) を外す */
function strip0071(t) {
  const out = [];
  let skip = false;
  for (const l of t.split('\n')) {
    if (/-- 🆕 0071 ここから/.test(l)) { skip = true; continue; }
    if (/-- 🆕 0071 ここまで/.test(l)) { skip = false; continue; }
    if (skip || /🆕 0071/.test(l)) continue;
    out.push(l);
  }
  return out.join('\n');
}
const lastDefBefore = (name, before = '0071') => migFiles.filter((f) => f < before && fnText(readMig(f), name)).pop();
const checkOps = (text, tbl) => {
  const m = new RegExp(`alter table ops\\.${tbl} add constraint ck_\\w+ check \\(operation in \\(([^;]*?)\\)\\);`).exec(text);
  return m ? [...m[1].matchAll(/'([a-z_]+)'/g)].map((x) => x[1]) : null;
};

console.log('[M] migration の写し');
for (const [name, expectFrom] of [['ops.master_write_allowed', '0067'], ['core.guard_master_edit_jan', '0053'], ['ops.transition_sku_registration', '0070']]) {
  await ta(`[M1] ${name} = ${expectFrom} の版 (前の最新) から写して 🆕 0071 の行だけ足した`, async () => {
    const src = lastDefBefore(name);
    assert.ok(src && src.startsWith(expectFrom), `前の最新の版が ${src} (期待 ${expectFrom})`);
    const before = fnText(readMig(src), name);
    const now = fnText(M71, name);
    assert.ok(now, '0071 に無い');
    assert.notEqual(now, before, '何も足していない');
    assert.equal(strip0071(now), before, '印の行を外すと前の版と違う (写し間違い・印の無い変更)');
  });
}
await ta('[M2] 約束の操作の CHECK = 前の最新の CHECK の全部 (+ 0070 の sku_gone_cancel) + sku_draft_cancel (どちらの表も)', async () => {
  for (const tbl of ['master_write_sessions', 'master_edit_requests']) {
    const src = migFiles.filter((f) => f < '0071' && checkOps(readMig(f), tbl)).pop();
    const before = checkOps(readMig(src), tbl);
    const want = before.includes('sku_gone_cancel') ? [...before, 'sku_draft_cancel'] : [...before, 'sku_gone_cancel', 'sku_draft_cancel'];
    assert.deepEqual(checkOps(M71, tbl), want, `${tbl} (前 = ${src})`);
  }
});
await ta('[M3] 一時の表を使わない・security definer の関数は全部 search_path = pg_catalog, pg_temp・revoke from public', async () => {
  assert.doesNotMatch(M71, /\b(create\s+(temp|temporary)\s+table|on commit drop)\b/i);
  const defs = [...M71.matchAll(/create (?:or replace )?function ([a-z_.]+)\(([^)]*)\)[^$]*?security definer[^$]*?as \$\$/g)];
  assert.ok(defs.length >= 3, `security definer の関数 ${defs.length}`);
  for (const d of defs) {
    assert.match(d[0], /set search_path = pg_catalog, pg_temp as \$\$/, d[1]);
    assert.ok(M71.includes(`revoke all on function ${d[1]}(`), `${d[1]} の public の実行権を外していない`);
  }
});
// 🆕 0091 (名前で探す): ロジザード登録の CSV に出した下書きは消さない
const F99 = migFiles.find((f) => /^\d{4}_deldraft_lz_exported\.sql$/.test(f));
const M99 = F99 ? readMig(F99) : '';
function strip0091(t) {
  const out = [];
  let skip = false;
  for (const l of t.split('\n')) {
    if (/-- 🆕 0091 ここから/.test(l)) { skip = true; continue; }
    if (/-- 🆕 0091 ここまで/.test(l)) { skip = false; continue; }
    if (skip || /🆕 0091/.test(l)) continue;
    out.push(l);
  }
  return out.join('\n');
}
for (const [name, expectFrom] of [['ops.sku_draft_cancel_check', '0071'], ['ops.cancel_variation_child', '0073']]) {
  await ta(`[M4] (0091) ${name} = ${expectFrom} の版 (前の最新) から写して 🆕 0091 の行だけ足した`, async () => {
    assert.ok(F99, '0091 (deldraft_lz_exported) が無い');
    const src = lastDefBefore(name, F99.slice(0, 4));
    assert.ok(src && src.startsWith(expectFrom), `前の最新の版が ${src} (期待 ${expectFrom})`);
    const before = fnText(readMig(src), name);
    const now = fnText(M99, name);
    assert.ok(before && now, '関数が見つからない');
    assert.notEqual(now, before, '何も足していない');
    assert.equal(strip0091(now), before, '印の行を外すと前の版と違う (写し間違い・印の無い変更)');
    assert.match(now, /lz_issued/);
    // 0091 より後の migration はこの関数を書き換えていない (書き換えたら、この写しが今の版でなくなる)
    for (const f of migFiles.filter((x) => x > F99)) assert.equal(fnText(readMig(f), name), null, `${f} にも ${name} がある`);
  });
}
await ta('[M5] (0091) 一時の表を使わない・security definer の関数は search_path = pg_catalog, pg_temp・revoke from public・値を変える文が無い', async () => {
  assert.doesNotMatch(M99, /\b(create\s+(temp|temporary)\s+table|on commit drop)\b/i);
  assert.doesNotMatch(M99.replace(/\$\$[\s\S]*?\$\$/g, ''), /^\s*(insert|update|delete|alter|drop|grant|truncate)\b/im, '関数の外で値・表・権限を変えない');
  const defs = [...M99.matchAll(/create (?:or replace )?function ([a-z_.]+)\(([^)]*)\)[^$]*?security definer[^$]*?as \$\$/g)];
  assert.equal(defs.length, 2);
  for (const d of defs) {
    assert.match(d[0], /set search_path = pg_catalog, pg_temp as \$\$/, d[1]);
    assert.ok(M99.includes(`revoke all on function ${d[1]}(`), `${d[1]} の public の実行権を外していない`);
  }
});

// ═══════════ DB の用意 ═══════════
const T = await setupVariationDb();
const { pg, db, q, one, asEditor, asRole } = T;
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
const SINGLE = (name) => ({ name, standard_price: '1980', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', sales_class: '3', expiry_managed: '0', reorder_months: '1', cost: { jpy: '500' } });
const draft = (code, { card = {}, values = {} } = {}) => asEditor(() => R.registerNewSku(db, { actor: 'naka@test', requestId: uuid(), kind: 'single', code,
  values: { ...SINGLE(`下書き ${code}`), ...values }, card }, { open: true, shippingRates: RATES }));
const setDraft = (code, comps, { card = { create: false } } = {}) => asEditor(() => R.registerNewSku(db, { actor: 'naka@test', requestId: uuid(), kind: 'set', code,
  values: { name: `セットの下書き ${code}`, standard_price: '3980', shipping_code: 'S01', reorder_months: '1', components: comps.map((c) => ({ code: c, qty: 1 })) }, card },
{ open: true, shippingRates: RATES }));
const cancel = (code, reason = 'コードを打ち間違えた', o = {}) => asEditor(() => D.cancelSkuDraft(db, { actor: o.actor ?? 'naka@test', code, reason, requestId: o.requestId ?? uuid(),
  confirm: o.confirm ?? true }, { open: o.open ?? true }));
const sid = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code]))?.id;
const check = async (code) => (await one('select ops.sku_draft_cancel_check($1::bigint) as r', [await sid(code)])).r;
const reasons = (c) => c.problems.map((p) => p.reason).sort();
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))?.state ?? null;
const outboxOf = async (code) => one(`select o.status, o.result, o.leased_until from ops.product_hub_outbox o join core.skus k on k.sku_id = o.sku_id where k.code = $1 and o.kind = 'card_create'`, [code]);
const addJan = async (code, jans, seen = []) => asEditor(async () => {
  await pg.query('begin');
  try { await pg.query('select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb)', [uuid(), 'naka@test', null, T.OWN, await sid(code), JSON.stringify(seen), JSON.stringify(jans)]); await pg.query('commit'); }
  catch (e) { await pg.query('rollback'); throw e; }
});
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
const PHDB = await import('../apps/product-hub/db.js');
const PH = await import('../apps/product-hub/services/cdb-card-intake.js');
const phdb = PHDB.getDB();

for (const c of ['d-ok', 'd-busy', 'd-k1', 'd-k2', 'd-k3', 'd-k4', 'd-k5', 'd-k6', 'd-comp', 'd-list', 'd-listdel', 'd-par', 'd-kid', 'd-iss', 'd-seen', 'd-l1', 'd-s1', 'd-s2', 'd-u1', 'd-u2', 'd-t1', 'd-lz1', 'd-lz2']) await draft(c);
await draft('d-nocard', { card: { create: false } });

console.log('\n[C] 確かめ');
await ta('[C1] 消せる下書き = ok・片付けの見込み (まだのカードの知らせ・CSV なし・JAN なし・構成の依頼なし)', async () => {
  const c = await check('d-ok');
  assert.deepEqual([c.ok, c.reg_state, c.sku_kind, c.problems], [true, 'draft', 'single', []]);
  assert.deepEqual([c.cleanup.reg_csv, c.cleanup.jans, c.cleanup.component_request, c.cleanup.card.status], [[], [], null, 'pending']);
  assert.equal((await check('d-nocard')).cleanup.card, null, 'カードを作らない登録 = 知らせが無い');
  const v = D.draftCancelView(c);
  assert.deepEqual([v.show, v.canGo, v.variation], [true, true, false]);
  assert.ok(v.will.some((w) => /もう作りません/.test(w)), v.will.join(' / '));
});
await ta('[C2] 下書きでない (業務で使える商品) = not_draft・NE に現れた (seen_in_ne)・画面の見せ方は出さない', async () => {
  const c = await check('s001');
  assert.deepEqual(reasons(c), ['not_draft', 'seen_in_ne']);
  assert.equal(D.draftCancelView(c).show, false);
  const none = (await one('select ops.sku_draft_cancel_check(999999999) as r')).r;
  assert.deepEqual([none.ok, none.problems.map((p) => p.reason)], [false, ['no_sku']]);
});
await ta('[C3] まとまりの子 = variation_child (子の廃止の道)・画面は「子の廃止」へ案内', async () => {
  await asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: uuid(), group: { mode: 'new', code: 'dd-grp', name: '消す試験のまとまり' }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-RD', name: 'レッド' }], children: [{ code: 'dd-grp-RD', choices: { 1: '-RD' }, name: '消す試験【レッド】' }],
    values: { standard_price: '1980', cost: { jpy: '500' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'S01' } },
  { open: true, shippingRates: RATES }));
  const c = await check('dd-grp-RD');
  assert.deepEqual(reasons(c), ['variation_child']);
  const v = D.draftCancelView(c);
  assert.deepEqual([v.show, v.canGo, v.variation], [true, false, true]);
  await rejects(cancel('dd-grp-RD'), 409, 'not_cancellable');
});
await ta('[C4] 使われている = セットの構成品・ほかのセットの構成の依頼・出品の構成・削除済みの出品の構成・ほかの商品の親', async () => {
  // セットの構成品 (core.sku_components = 持ち主の直接の書き込み = 試験だけ)
  await setDraft('dd-set1', ['d-k6']);
  await pg.query('insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source, created_by_type) values (1, $1, $2, 1, $3, $4)', [await sid('dd-set1'), await sid('d-comp'), 'manual', 'system']);
  assert.ok(reasons(await check('d-comp')).includes('set_component'));
  assert.ok(reasons(await check('d-k6')).includes('component_request'), 'セットの下書きの構成の依頼に入っている');
  // 出品の構成 (Amazon の対応など) / 削除済みの出品の構成
  for (const [code, status] of [['d-list', 'active'], ['d-listdel', 'deleted']]) {
    const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', $1, $2) returning listing_id`, [`pr-${code}`, status])).listing_id;
    await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'exact', 'system')`, [lid, await sid(code)]);
  }
  assert.deepEqual(reasons(await check('d-list')), ['listing_component']);
  assert.deepEqual(reasons(await check('d-listdel')), ['listing_component_deleted']);
  // ほかの商品の親 (持ち主の直接の書き込み = 試験だけ)
  await pg.query('begin');
  await pg.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
  await pg.query(`update core.products set parent_product_id = (select product_id from core.skus where code = 'd-par') where product_id = (select product_id from core.skus where code = 'd-kid')`);
  await pg.query('commit');
  assert.deepEqual(reasons(await check('d-par')), ['parent_of_others']);
  assert.deepEqual(reasons(await check('d-kid')), ['variation_child']);
  for (const c of ['d-comp', 'd-list', 'd-listdel', 'd-par']) await rejects(cancel(c), 409, 'not_cancellable');
  assert.deepEqual(await Promise.all(['d-comp', 'd-list', 'd-listdel', 'd-par'].map(regOf)), ['draft', 'draft', 'draft', 'draft'], '何も変えない');
});
await ta('[C5] NE 登録の CSV を一度配った = ever_issued (使わないにした後も)', async () => {
  const f = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', codes: ['d-iss'], requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  const iss = await asEditor(() => G.issueRegExport(db, { actor: 'naka@test', exportId: f.export.export_id, requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  assert.equal(iss.export.state, 'issued');
  assert.deepEqual(reasons(await check('d-iss')), ['ever_issued']);
  await asEditor(() => G.supersedeRegExport(db, { actor: 'naka@test', exportId: f.export.export_id, reason: '試験', correction: 'NE では何もしていない', confirm: true }, { open: true }));
  assert.deepEqual(reasons(await check('d-iss')), ['ever_issued'], '使わないにしても「一度配った」は消えない');
  await rejects(cancel('d-iss'), 409, 'not_cancellable');
});
await ta('[C6] カードを取り込み中 (借りの期限の中) = card_busy・借りを返すと消せる', async () => {
  const id = await sid('d-busy');
  const got = await asEditor(async () => (await pg.query("select event_id::text as id from ops.claim_card_events('test-busy', 'auto', null, $1::bigint, 1, 120, 5)", [id])).rows);
  assert.equal(got.length, 1);
  assert.deepEqual(reasons(await check('d-busy')), ['card_busy']);
  await rejects(cancel('d-busy'), 409, 'not_cancellable');
  await asEditor(() => pg.query("select ops.finish_card_event($1::uuid, 'test-busy', 'failed', null, '試験')", [got[0].id]));
  assert.equal((await check('d-busy')).ok, true);
});

console.log('\n[F] 消す');
let FIRST = null;
await ta('[F1] 消す = cancelled・登録の状態の履歴 (人・理由・根拠)・保存の記録 done', async () => {
  const rid = uuid();
  const r = await cancel('d-ok', 'コードを打ち間違えた (正しくは d-ok2)', { requestId: rid });
  FIRST = { rid, r };
  assert.deepEqual([r.ok, r.code, r.from, r.state, r.request_id], [true, 'd-ok', 'draft', 'cancelled', rid]);
  assert.equal(await regOf('d-ok'), 'cancelled');
  const ev = await one(`select e.from_state, e.to_state, e.actor_type, e.actor_id, e.reason, e.evidence, e.request_id from ops.master_registration_events e
      where e.sku_id = $1 order by e.event_id desc limit 1`, [await sid('d-ok')]);
  assert.deepEqual([ev.from_state, ev.to_state, ev.actor_type, ev.actor_id, ev.reason, ev.request_id], ['draft', 'cancelled', 'human', 'naka@test', 'コードを打ち間違えた (正しくは d-ok2)', rid]);
  assert.deepEqual([ev.evidence.rule, ev.evidence.never_issued, ev.evidence.seen_in_ne, ev.evidence.card.outcome], ['draft_cancel_v1', true, false, 'cancelled']);
  const m = await one('select operation, status, actor_id, target_code, sku_id::text as sku_id, result from ops.master_edit_requests where request_id = $1', [rid]);
  assert.deepEqual([m.operation, m.status, m.actor_id, m.target_code, m.sku_id, m.result.state], ['sku_draft_cancel', 'done', 'naka@test', 'd-ok', await sid('d-ok'), 'cancelled']);
  // 行は消さない (SKU・商品・原価・仕入先は残る)
  const k = await one(`select k.code, (select count(*)::int from core.sku_costs c where c.sku_id = k.sku_id) as costs, (select count(*)::int from core.supplier_skus x where x.sku_id = k.sku_id) as sups
      from core.skus k where k.code = 'd-ok'`);
  assert.deepEqual([k.code, k.costs >= 1, k.sups], ['d-ok', true, 1]);
});
await ta('[F2] 押し直し (同じ番号・同じ理由) = 前の答え / 同じ番号で違う理由 = request_id_reused / もう一度 (新しい番号) = not_cancellable', async () => {
  const again = await cancel('d-ok', 'コードを打ち間違えた (正しくは d-ok2)', { requestId: FIRST.rid });
  assert.deepEqual([again.replayed, again.event_id], [true, FIRST.r.event_id]);
  await rejects(cancel('d-ok', '別の理由', { requestId: FIRST.rid }), 409, 'request_id_reused');
  const e = await rejects(cancel('d-ok', 'もう一度'), 409, 'not_cancellable');
  assert.match(e.message, /下書きだけ/);
  assert.equal((await one(`select count(*)::int as n from ops.master_registration_events where sku_id = $1 and to_state = 'cancelled'`, [await sid('d-ok')])).n, 1);
});
await ta('[F3] 断る: 理由が無い・長い・改行 (400 reason)・確かめの窓の印なし (400)・MASTER_EDIT_OPEN なし (409 before_cutover)・無いコード (404)', async () => {
  assert.equal((await rejects(cancel('d-k5', '  '), 400, 'invalid_input')).extra.field, 'reason');
  assert.equal((await rejects(cancel('d-k5', 'あ'.repeat(201)), 400, 'invalid_input')).extra.field, 'reason');
  assert.equal((await rejects(cancel('d-k5', '一行目\n二行目'), 400, 'invalid_input')).extra.field, 'reason');
  assert.equal((await rejects(cancel('d-k5', '理由', { confirm: false }), 400, 'invalid_input')).extra.field, 'confirm');
  await rejects(cancel('d-k5', '理由', { open: false }), 409, 'before_cutover');
  await rejects(cancel('nope-xyz', '理由'), 404, 'not_found');
  assert.equal(await regOf('d-k5'), 'draft');
});
await ta('[F3b] カードを作らない登録の下書き (知らせが無い) も消せる (片付けのカード = なし)', async () => {
  const r = await cancel('d-nocard', '要らなくなった');
  assert.deepEqual([r.state, r.cleanup.card, r.cleanup.reg_csv, r.cleanup.jans, r.cleanup.component_request], ['cancelled', null, [], [], null]);
  const ev = await one(`select evidence from ops.master_registration_events where sku_id = $1 and to_state = 'cancelled'`, [await sid('d-nocard')]);
  assert.deepEqual([ev.evidence.card, ev.evidence.rule], [null, 'draft_cancel_v1']);
});
await ta('[F4] 消したコードはもう使えない (code_taken)・別のコードなら登録し直せる', async () => {
  const e = await errOf(draft('d-ok'));
  assert.ok(e && e.reason === 'code_taken', e && `${e.reason}: ${e.message}`);
  const r = await draft('d-ok2');
  assert.equal(r.state, 'draft');
});

console.log('\n[K] 片付け');
await ta('[K1] 作っただけの NE 登録の CSV = 使わない (ファイルを閉じる)・一緒に入っていた商品は下書きのまま・作り直せる', async () => {
  const f = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', codes: ['d-k1', 'd-k2'], requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  const xid = String(f.export.export_id);
  const c = await check('d-k1');
  assert.deepEqual([c.ok, c.cleanup.reg_csv], [true, [{ export_id: xid, codes: ['d-k2'] }]]);
  assert.ok(D.draftCancelView(c).will.some((w) => w.includes(`#${xid}`) && w.includes('d-k2')));
  const r = await cancel('d-k1', '発売が中止になった');
  assert.deepEqual(r.cleanup.reg_csv, [{ export_id: xid, codes: ['d-k2'] }]);
  const ex = await one('select state, close_reason, closed_by from ops.ne_reg_exports where export_id = $1', [xid]);
  assert.deepEqual([ex.state, ex.close_reason, ex.closed_by], ['closed', 'superseded', 'naka@test']);
  const items = await q('select k.code, i.state, i.superseded_reason from ops.ne_reg_export_items i join core.skus k on k.sku_id = i.sku_id where i.export_id = $1 order by k.code', [xid]);
  assert.deepEqual(items.map((i) => [i.code, i.state]), [['d-k1', 'superseded'], ['d-k2', 'superseded']]);
  assert.match(items[0].superseded_reason, /下書き d-k1 を消した/);
  assert.equal(await regOf('d-k2'), 'draft');
  const f2 = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', codes: ['d-k2'], requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  assert.equal(f2.export.state, 'built', '残った商品は作り直せる');
  await asEditor(() => G.supersedeRegExport(db, { actor: 'naka@test', exportId: f2.export.export_id, reason: '試験', correction: 'NE では何もしていない', confirm: true }, { open: true }));
});
await ta('[K2] セットの下書きを消す = 自分の構成の依頼を取り下げ (cancelled / withdrawn)・構成品の下書きが消せるようになる', async () => {
  await setDraft('dd-set2', ['d-k3']);
  assert.ok(reasons(await check('d-k3')).includes('component_request'));
  const c = await check('dd-set2');
  assert.equal(c.ok, true, JSON.stringify(c.problems));
  const req = c.cleanup.component_request;
  assert.ok(req);
  assert.ok(D.draftCancelView(c).will.some((w) => w.includes(`#${req}`)));
  const r = await cancel('dd-set2', 'セットは作らないことにした');
  assert.equal(r.cleanup.component_request, req);
  const row = await one('select status, close_reason, closed_by from ops.sku_component_requests where component_request_id = $1', [req]);
  assert.deepEqual([row.status, row.close_reason, row.closed_by], ['cancelled', 'withdrawn', 'naka@test']);
  assert.equal((await check('d-k3')).ok, true);
  await cancel('d-k3', '構成品も要らない');
  assert.equal(await regOf('d-k3'), 'cancelled');
  // 消したセットの下書きの構成品にはもうできない (新しいセットの構成品に消した下書き = 断る・0052 の決まり)
  const e = await errOf(setDraft('dd-set3', ['d-k3']));
  assert.ok(e && e.status >= 400, '消した商品を構成品にできてしまった');
});
await ta('[K3] JAN を外す = 同じ JAN で登録し直せる (前は jan_taken)・変更の記録 (人・request_id)', async () => {
  const jan = jan13('490777000001');
  await addJan('d-k4', [jan]);
  const before = await errOf(addJan('d-k5', [jan]));
  assert.match(String(before && before.message), /jan_taken/);
  const c = await check('d-k4');
  assert.deepEqual(c.cleanup.jans, [jan]);
  const rid = uuid();
  const r = await cancel('d-k4', 'JAN の商品を別のコードで作り直す', { requestId: rid });
  assert.deepEqual(r.cleanup.jans, [jan]);
  assert.equal((await one(`select count(*)::int as n from core.external_ids e join core.skus k on k.product_id = e.entity_id
      where k.code = 'd-k4' and e.entity_type = 'product' and e.system = 'jan' and e.valid_to is null`)).n, 0);
  const ce = await one(`select actor_id, request_id::text as rid from events.master_change_events where entity_type = 'external_id' and operation = 'UPDATE' order by recorded_at desc, change_id desc limit 1`);
  assert.deepEqual([ce.actor_id, ce.rid], ['naka@test', rid]);
  await addJan('d-k5', [jan]);   // 外したので通る
});
await ta('[K4] まだ取り込んでいないカードの知らせ = 済み「作らない」・もう借りられない・見張りの数に入らない', async () => {
  const o = await outboxOf('d-ok');
  assert.deepEqual([o.status, o.result.outcome, o.result.status_before, o.leased_until], ['done', 'cancelled', 'pending', null]);
  const got = await asEditor(async () => (await pg.query("select event_id from ops.claim_card_events('t', 'manual', null, $1::bigint, 5, 120, 5)", [await sid('d-ok')])).rows);
  assert.equal(got.length, 0);
  const open = await q("select status, n from ops.v_product_hub_outbox_open");
  const sum = open.reduce((a, r) => a + Number(r.n), 0);
  assert.equal((await one(`select count(*)::int as n from ops.product_hub_outbox o join ops.master_registrations r on r.sku_id = o.sku_id where o.status <> 'done' and r.state = 'cancelled'`)).n, 0);
  assert.ok(sum >= 0);
});
await ta('[K5] 取り込んだカード (知らせ done) = DB は止めない (SQLite は見えない)・product-hub の除外していないカード (結んだ・同じコードの結んでいない・出品済みも) を数える = 画面は押せない・除外すれば押せる', async () => {
  // 本物の取り込み (SQLite) で d-k5 のカードを作る
  const k5 = await sid('d-k5');
  const res = await asEditor(() => O.runCardOutbox(db, (ev) => PH.applyCdbCardEvent(ev, { db: phdb }), { skuId: k5, limit: 1 }));
  assert.equal(res[0].status, 'done', JSON.stringify(res[0]));
  const draftId = res[0].result.draft_id;
  const c = await check('d-k5');
  assert.deepEqual([c.ok, c.cleanup.card.status, c.cleanup.card.outcome, c.cleanup.card.draft_id], [true, 'done', 'created', draftId]);
  const live = () => PH.liveCdbCardsForSku({ skuId: k5, code: 'd-k5' }, { db: phdb });
  assert.deepEqual(live().map((x) => [x.draft_id, x.linked]), [[draftId, true]]);
  const v = D.draftCancelView(c, { cards: live() });
  assert.deepEqual([v.show, v.canGo, v.why.map((w) => w.reason)], [true, false, ['card_exists']]);
  assert.match(v.why[0].text, new RegExp(`#${draftId}.*除外`));
  // 出品済み (listed) でも数える (除外していない)
  phdb.prepare("UPDATE product_drafts SET status = 'listed' WHERE id = ?").run(draftId);
  assert.deepEqual(live().map((x) => x.status), ['listed']);
  // 除外した = 数えない = 押せる
  phdb.prepare("UPDATE product_drafts SET status = 'excluded' WHERE id = ?").run(draftId);
  assert.deepEqual(live(), []);
  assert.equal(D.draftCancelView(c, { cards: live() }).canGo, true);
  // 同じコード (大文字・前後の空白だけ違う) の結んでいないカード (古いやり方で作ったカード) も数える。本番の SQLite は前からの重なりで正規化のコードの一意が無いことがある = 試験の DB でも外して置く
  phdb.exec('DROP INDEX IF EXISTS idx_product_drafts_ne_norm');
  const other = Number(phdb.prepare("INSERT INTO product_drafts (ne_code, name) VALUES (' D-K5 ', '古いカード')").run().lastInsertRowid);
  assert.deepEqual(live().map((x) => [x.draft_id, x.linked]), [[other, false]]);
  phdb.prepare("UPDATE product_drafts SET status = 'excluded' WHERE id = ?").run(other);
  // 読めない = 押せない
  assert.deepEqual(D.draftCancelView(c, { error: 'SQLite が読めない' }).why.map((w) => w.reason), ['card_unreadable']);
  assert.throws(() => PH.liveCdbCardsForSku({ skuId: 'x' }, { db: phdb }), /SKU の番号が不正/);
});

console.log('\n[R] ロール');
const codeOf = async (fn) => { const e = await errOf(fn); return e ? e.code : null; };
await ta('[R1] master_edit = 消す・確かめの実行だけ (登録の状態・知らせ・JAN の表は直接書けない)・watcher = 確かめだけ・master_ops は消す関数を持たない', async () => {
  const id = await sid('d-k6');
  assert.equal(await codeOf(() => asEditor(() => pg.query("update ops.master_registrations set state = 'cancelled' where sku_id = $1", [id]))), '42501');
  assert.equal(await codeOf(() => asEditor(() => pg.query("update ops.product_hub_outbox set status = 'done' where sku_id = $1", [id]))), '42501');
  assert.equal(await codeOf(() => asEditor(() => pg.query("update core.external_ids set valid_to = now() where system = 'jan'"))), '42501');
  assert.equal(await codeOf(() => asRole('watcher', () => pg.query('select ops.sku_draft_cancel_check($1::bigint)', [id]))), null);
  assert.equal(await codeOf(() => asRole('watcher', () => pg.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint)', [uuid(), 'x@test', '理由', id]))), '42501');
  assert.equal(await codeOf(() => asRole('master_ops', () => pg.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint)', [uuid(), 'x@test', '理由', id]))), '42501');
  const pub = (await one(`select bool_or(a.grantee = 0) as pub from pg_proc p cross join lateral aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
      where p.oid in ('ops.cancel_sku_draft(uuid, text, text, bigint)'::regprocedure, 'ops.sku_draft_cancel_check(bigint)'::regprocedure)`)).pub;
  assert.equal(pub, false, 'public の実行権');
});
await ta('[R2] 画面のロールが関数を直に呼んでも DB が同じに確かめる (下書きでない・理由なし・使われている)・同じ取引で 2 回目 = master_write_session_exists', async () => {
  const call = (code, reason) => asEditor(async () => pg.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint) as r', [uuid(), 'naka@test', reason, await sid(code)]));
  assert.match(String((await errOf(call('s001', '理由'))).message), /^not_cancellable: .*下書きだけ/);
  assert.match(String((await errOf(call('d-k6', ''))).message), /^invalid_input/);
  assert.match(String((await errOf(call('d-list', '理由'))).message), /^not_cancellable: .*出品の構成/);
  await asEditor(async () => {
    await pg.query('begin');
    try {
      await pg.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint)', [uuid(), 'naka@test', '一つ目', await sid('d-u2')]);
      const e = await errOf(pg.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint)', [uuid(), 'naka@test', '二つ目', await sid('d-k6')]));
      assert.match(String(e && e.message), /not_cancellable|master_write_session_exists/);
    } finally { await pg.query('rollback'); }
  });
  assert.deepEqual([await regOf('d-u2'), await regOf('d-k6')], ['draft', 'draft'], '巻き戻した');
});
await ta('[R3] JAN の守り (0053) は今までどおり: 約束なしの画面のロールの JAN の書き込み = 断る・JAN の約束で足す / 外すは通る', async () => {
  const jan = jan13('490777000002');
  await addJan('d-k6', [jan]);
  await addJan('d-k6', [], [jan]);
  assert.equal((await one(`select count(*)::int as n from core.external_ids e join core.skus k on k.product_id = e.entity_id where k.code = 'd-k6' and e.system = 'jan' and e.valid_to is null`)).n, 0);
});

await ta('[T1] (Codex #1688 R1 High 1) 運用のロール (master_ops) が ops.transition_sku_registration を直に呼んで下書きをやめる = 42501 draft_cancel_session_required / 表の持ち主は通る / 子の廃止 (約束 variation_child_cancel) は通る', async () => {
  const k6 = await sid('d-k6');
  const e = await errOf(asRole('master_ops', () => pg.query("select ops.transition_sku_registration($1, 'cancelled', 'human', 'ops@test', '運用でやめる')", [k6])));
  assert.equal(e?.code, '42501', e?.message);
  assert.match(String(e.message), /^draft_cancel_session_required/);
  assert.equal(await regOf('d-k6'), 'draft');
  // 画面のロールは実行権が無い (42501)
  assert.equal((await errOf(asEditor(() => pg.query("select ops.transition_sku_registration($1, 'cancelled', 'human', 'x', '理由')", [k6]))))?.code, '42501');
  // 表の持ち主 (migration・手の復旧) は約束を求めない
  await pg.query("select ops.transition_sku_registration($1, 'cancelled', 'human', 'owner@test', '持ち主の手')", [await sid('d-t1')]);
  assert.equal(await regOf('d-t1'), 'cancelled');
  // 子の廃止 (0067) は今までどおり
  await asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: uuid(), group: { mode: 'new', code: 'dd-grp2', name: '子の廃止の試験' }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-BL', name: 'ブルー' }], children: [{ code: 'dd-grp2-BL', choices: { 1: '-BL' }, name: '子の廃止の試験【ブルー】' }],
    values: { standard_price: '1980', cost: { jpy: '500' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'S01' } },
  { open: true, shippingRates: RATES }));
  const r = await asEditor(() => V.cancelVariationChild(db, { actor: 'naka@test', requestId: uuid(), code: 'dd-grp2-BL', reason: '作らない' }, { open: true }));
  assert.equal(r.state, 'cancelled');
});

console.log('\n[L] 一覧');
await ta('[L1] やめた商品はふだんの一覧・札の数に出さない・「やめた」で絞る / 商品コードでぴったり探すと出る', async () => {
  const codesOf = (r) => r.rows.map((x) => x.code);
  const all = await listSkus(db, { q: 'd-' }, { now: new Date(NOW_MS) });
  assert.ok(!codesOf(all).includes('d-ok') && codesOf(all).includes('d-l1'), codesOf(all).join(','));
  const can = await listSkus(db, { reg: 'cancelled' }, { now: new Date(NOW_MS) });
  assert.ok(codesOf(can).includes('d-ok') && codesOf(can).includes('d-k1') && !codesOf(can).includes('d-l1'));
  const exact = await listSkus(db, { codes: 'd-ok\nd-l1' }, { now: new Date(NOW_MS) });
  assert.deepEqual(codesOf(exact).sort(), ['d-l1', 'd-ok']);
  assert.deepEqual(exact.notFound, []);
  assert.deepEqual([all.cancelledHidden, can.cancelledHidden, exact.cancelledHidden], [true, false, false]);
  // 🆕 Codex #1688 R1 Medium: 上の絞る欄にコードをぴったり (大文字・全角でも) = やめた商品も出す / 一部だけ = 出さない
  assert.deepEqual(codesOf(await listSkus(db, { q: 'D-OK' }, { now: new Date(NOW_MS) })), ['d-ok', 'd-ok2'], 'd-ok (やめた・ぴったり) と d-ok2 (下書き・一部)');
  assert.deepEqual(codesOf(await listSkus(db, { q: 'ｄ－ｏｋ' }, { now: new Date(NOW_MS) })), ['d-ok', 'd-ok2']);
  assert.ok(!codesOf(await listSkus(db, { q: 'd-o' }, { now: new Date(NOW_MS) })).includes('d-ok'));
  const c = await listCounts(db, { now: new Date(NOW_MS) });
  const n = (await one(`select count(*)::int as n from core.skus k join ops.master_registrations r on r.sku_id = k.sku_id where r.state = 'cancelled'`)).n;
  const total = (await one('select count(*)::int as n from core.skus where company_id = 1')).n;
  assert.deepEqual([c.n_cancelled, c.n_all], [n, total - n]);
});

// ═══════════ [S] 画面 (本物の router) ═══════════
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/deldraft';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/deldraft';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const RT = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
RT.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
RT.__setShippingRatesProvider(async () => RATES);
const liveReader = (input) => PH.liveCdbCardsForSku(input, { db: phdb });
RT.__setLiveCardsReader(liveReader);
let who = 'naka@test';
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: who, displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', RT.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const HOST = `127.0.0.1:${server.address().port}`;
const B = `http://${HOST}/apps/master-edit`;
const page = async (code) => { const r = await fetch(`${B}/sku/${encodeURIComponent(code)}`); return { status: r.status, html: await r.text() }; };
const post = async (code, body) => {
  const r = await fetch(`${B}/api/sku/${encodeURIComponent(code)}/draft-cancel`, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: `http://${HOST}` }, body: JSON.stringify(body) });
  return { status: r.status, j: await r.json() };
};

console.log('\n[S] 画面');
await ta('[S1] 消せる下書き = 「この下書きを消す」と確かめの窓 (片付けるもの・理由)・まとまりの子 = 子の廃止へ案内・業務で使える商品 = 何も出さない', async () => {
  const p = await page('d-s1');
  assert.equal(p.status, 200);
  assert.match(p.html, /id="deldraft-open"/);
  assert.match(p.html, /id="deldraft-bg"/);
  assert.match(p.html, /id="deldraft-will"[\s\S]*もう作りません/);
  assert.match(p.html, /me-draft-cancel\.js/);
  const k = await page('dd-grp-RD');
  assert.match(k.html, /色違い・サイズ違いのまとまりの子です/);
  assert.doesNotMatch(k.html, /id="deldraft-open"/);
  const a = await page('s001');
  assert.doesNotMatch(a.html, /id="deldraft"/);
  const u = await page('d-list');
  assert.match(u.html, /いまは消せません[\s\S]*出品の構成に使われている/);
  assert.doesNotMatch(u.html, /me-draft-cancel\.js/);
});
await ta('[S2] 名簿の人だけ (403)・確かめの窓の印なし (400)・product-hub に除外していないカード = 409 card_exists (画面も押せない)・除外すれば通る・知らせだけ = 通って「作らない」・カードを読めない = 503', async () => {
  who = 'other@test';
  try {
    const p0 = await page('d-s1');
    assert.match(p0.html, /名簿 \(MASTER_EDITORS\) の人だけ/);
    assert.equal((await post('d-s1', { request_id: uuid(), reason: '理由', confirm: true })).status, 403);
  } finally { who = 'naka@test'; }
  assert.equal((await post('d-s1', { request_id: uuid(), reason: '理由' })).status, 400);
  // カードを取り込む → 画面は押せない・消すは 409 card_exists (何もしない)
  const s1 = await sid('d-s1');
  const res = await asEditor(() => O.runCardOutbox(db, (ev) => PH.applyCdbCardEvent(ev, { db: phdb }), { skuId: s1, limit: 1 }));
  const draftId = res[0].result.draft_id;
  const p1 = await page('d-s1');
  assert.doesNotMatch(p1.html, /id="deldraft-open"/);
  assert.match(p1.html, new RegExp(`product-hub にカードがある[\\s\\S]*#${draftId}`));
  const r0 = await post('d-s1', { request_id: uuid(), reason: '入れ間違い', confirm: true });
  assert.deepEqual([r0.status, r0.j.reason, r0.j.cards.map((c) => c.draft_id)], [409, 'card_exists', [draftId]]);
  assert.match(r0.j.error, /先に product-hub でカードを確かめて「除外」にしてから/);
  assert.equal(await regOf('d-s1'), 'draft');
  // 人が product-hub で除外した → 通る (知らせは done のまま)
  phdb.prepare("UPDATE product_drafts SET status = 'excluded' WHERE id = ?").run(draftId);
  assert.match((await page('d-s1')).html, /id="deldraft-open"/);
  const r = await post('d-s1', { request_id: uuid(), reason: '入れ間違い', confirm: true });
  assert.equal(r.status, 200, JSON.stringify(r.j));
  assert.deepEqual([r.j.ok, r.j.state, r.j.product_hub], [true, 'cancelled', undefined]);
  const o1 = await outboxOf('d-s1');
  assert.deepEqual([o1.status, o1.result.outcome], ['done', 'created']);
  const after = await page('d-s1');
  assert.match(after.html, /id="cancelled-band"/);
  assert.doesNotMatch(after.html, /id="deldraft"/);
  // まだカードになっていない知らせだけ = 通って、知らせは「作らない」で済み
  const q2 = await post('d-s2', { request_id: uuid(), reason: '発売中止', confirm: true });
  assert.equal(q2.status, 200, JSON.stringify(q2.j));
  const o2 = await outboxOf('d-s2');
  assert.deepEqual([o2.status, o2.result.outcome], ['done', 'cancelled']);
  assert.match((await page('d-s2')).html, /id="card-cancelled"/);
  // product-hub のカードを読めない = 503 (消さない)・画面も押せない
  RT.__setLiveCardsReader(() => { throw new Error('SQLite が読めない'); });
  try {
    const q3 = await post('d-u2', { request_id: uuid(), reason: '理由', confirm: true });
    assert.deepEqual([q3.status, q3.j.reason], [503, 'card_unreadable']);
    assert.equal(await regOf('d-u2'), 'draft');
    const pu = await page('d-u2');
    assert.doesNotMatch(pu.html, /id="deldraft-open"/);
    assert.match(pu.html, /product-hub のカードを読めない/);
  } finally { RT.__setLiveCardsReader(liveReader); }
  // 断られた = 409 と文
  const n = await post('d-list', { request_id: uuid(), reason: '理由', confirm: true });
  assert.deepEqual([n.status, n.j.reason], [409, 'not_cancellable']);
  assert.match(n.j.error, /消せません: .*出品の構成/);
});
await ta('[S3] 一覧 = やめた商品を出さない・札「やめた商品」で見る', async () => {
  const r = await fetch(`${B}/?q=d-s`);
  const html = await r.text();
  assert.doesNotMatch(html, /href="[^"]*sku\/d-s1"/);
  assert.match(html, /やめた商品/);
  const r2 = await fetch(`${B}/?reg=cancelled`);
  assert.match(await r2.text(), /sku\/d-s1/);
  const r3 = await fetch(`${B}/?q=D-S1`);
  assert.match(await r3.text(), /sku\/d-s1/, '上の絞る欄にコードをぴったり = 出す');
});

// ═══════════ 🆕 [Z] (0091) ロジザード登録の CSV に出した下書きは消さない (中原さんの決定 2026-10-18) ═══════════
console.log('\n[Z] ロジザード登録の CSV に出した下書き (0091)');
/** 画面のロールで: バーコード = 空欄 (ロジザードが番号を付ける) を入れて、ロジザード登録の CSV を出す (DB の関数を直に・1 品目)。戻り値 = 出したファイルの番号 */
const lzIssue = (code, { expiry = '01', inbound = '0' } = {}) => asEditor(async () => {
  const id = await sid(code);
  const b = (await pg.query("select ops.set_sku_logizard_barcode($1::uuid, $2, $3::bigint, 'lz_assign', null, 'sku_page', null, null, null) as r", [uuid(), 'naka@test', id])).rows[0].r;
  const items = [{ sku_id: id, code, rule_row: String(b.rule_row), kind: 'lz_assign', value: null, expiry_code: expiry, inbound_code: inbound }];
  await pg.query('begin');
  try {
    const r = (await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), null) as r', [uuid(), 'naka@test', JSON.stringify(items)])).rows[0].r;
    await pg.query('commit');
    return r.export_id;
  } catch (e) { await pg.query('rollback'); throw e; }
});
const lzVoid = (exportId) => asEditor(async () => (await pg.query('select ops.lz_new_export_void($1, $2::bigint, $3) as r', ['naka@test', exportId, 'ロジザードから消した'])).rows[0].r);
let LZ1 = null;
await ta('[Z1] 出した (issued) = 確かめが lz_issued (ファイルの番号・ロジザードから消すのが先)・lib = 409 not_cancellable・DB の関数を直に呼んでも断る・画面は押せず理由を出す・API = 409', async () => {
  LZ1 = await lzIssue('d-lz1');
  assert.match(String(LZ1), /^\d+$/);
  const c = await check('d-lz1');
  assert.deepEqual([c.ok, reasons(c)], [false, ['lz_issued']]);
  const t = c.problems[0].text;
  assert.ok(t.includes(`ファイル #${LZ1}`) && t.includes('ロジザードから消すのが先です'), t);
  assert.equal(t, D.lzIssuedText([LZ1]), 'lib の文 = DB の文 (0091 の前の DB でも同じ文)');
  const v = D.draftCancelView(c);
  assert.deepEqual([v.show, v.canGo], [true, false]);
  assert.deepEqual(v.why.map((w) => [w.reason, w.word]), [['lz_issued', 'ロジザード登録の CSV に出した']]);
  const e = await rejects(cancel('d-lz1'), 409, 'not_cancellable');
  assert.match(e.message, new RegExp(`消せません: ロジザード登録の CSV に出しています \\(ファイル #${LZ1}\\)`));
  const direct = await errOf(asEditor(async () => pg.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint) as r', [uuid(), 'naka@test', '理由', await sid('d-lz1')])));
  assert.match(String(direct && direct.message), new RegExp(`^not_cancellable: d-lz1 は消せない \\(ロジザード登録の CSV に出しています \\(ファイル #${LZ1}\\)`));
  assert.equal(await regOf('d-lz1'), 'draft');
  const p = await page('d-lz1');
  assert.match(p.html, /この下書きは、いまは消せません[\s\S]*data-reason="lz_issued"[\s\S]*ロジザード登録の CSV に出した[\s\S]*ロジザードから消すのが先です/);
  assert.ok(p.html.includes(`ファイル #${LZ1}`));
  assert.doesNotMatch(p.html, /id="deldraft-open"/);
  const n = await post('d-lz1', { request_id: uuid(), reason: '理由', confirm: true });
  assert.deepEqual([n.status, n.j.reason], [409, 'not_cancellable']);
  assert.match(n.j.error, /ロジザード登録の CSV に出しています/);
  assert.equal(await regOf('d-lz1'), 'draft');
});
await ta('[Z2] 0091 の前の DB (確かめに lz_issued が無い) でも lib が同じ理由を足す (画面は押せない)・消す前に断る (DB の関数を呼ばない)', async () => {
  const calls = [];
  const old = { query: async (t, p) => {
    calls.push(t);
    const r = await db.query(t, p);
    if (/ops\.sku_draft_cancel_check\(/.test(t) && r.rows[0] && r.rows[0].r) {
      const c0 = typeof r.rows[0].r === 'string' ? JSON.parse(r.rows[0].r) : r.rows[0].r;
      const problems = c0.problems.filter((x) => x.reason !== 'lz_issued');
      return { ...r, rows: [{ r: { ...c0, ok: problems.length === 0, problems } }] };
    }
    return r;
  } };
  const lz1 = await sid('d-lz1');
  const c = await asEditor(() => D.readDraftCancelCheck(old, lz1));
  assert.deepEqual([c.ok, reasons(c)], [false, ['lz_issued']]);
  assert.equal(c.problems[0].text, D.lzIssuedText([LZ1]));
  assert.equal(D.draftCancelView(c).canGo, false);
  calls.length = 0;
  await rejects(asEditor(() => D.cancelSkuDraft(old, { actor: 'naka@test', code: 'd-lz1', reason: '理由', requestId: uuid(), confirm: true }, { open: true })), 409, 'not_cancellable');
  assert.ok(!calls.some((t) => /ops\.cancel_sku_draft\(/.test(t)), 'DB の関数の前に断る');
  // 出していない下書き = lib は何も足さない
  const ok = await asEditor(async () => D.readDraftCancelCheck(old, await sid('d-lz2')));
  assert.deepEqual([ok.ok, ok.problems], [true, []]);
  assert.deepEqual(await asEditor(async () => D.lzIssuedOf(db, await sid('d-lz2'))), []);
});
await ta('[Z3] 取り消した (voided) ファイルだけ = また消せる (画面に「この下書きを消す」)・消した後は CSV にもう出せない', async () => {
  assert.equal((await lzVoid(LZ1)).ok, true);
  const c = await check('d-lz1');
  assert.deepEqual([c.ok, c.problems], [true, []]);
  assert.deepEqual(await asEditor(async () => D.lzIssuedOf(db, await sid('d-lz1'))), []);
  assert.match((await page('d-lz1')).html, /id="deldraft-open"/);
  const r = await cancel('d-lz1', 'ロジザードから消した後に消す');
  assert.equal(r.state, 'cancelled');
  const e = await errOf(lzIssue('d-lz1'));
  assert.match(String(e && e.message), /^cancelled: 登録をやめた商品/, '消した (やめた) 下書きにはバーコードを書かない');
  // 前のバーコードの行のまま CSV を出す関数を直に呼んでも = やめた商品は出さない
  const id = await sid('d-lz1');
  const rr = (await one('select max(rule_row)::text as r from ops.sku_logizard_barcode_rules where sku_id = $1', [id])).r;
  const e2 = await errOf(asEditor(async () => {
    await pg.query('begin');
    try { return await pg.query('select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), null)', [uuid(), 'naka@test', JSON.stringify([{ sku_id: id, code: 'd-lz1', rule_row: rr, kind: 'lz_assign', value: null, expiry_code: '01', inbound_code: '0' }])]); }
    finally { await pg.query('rollback'); }
  }));
  assert.match(String(e2 && e2.message), /^invalid_input: d-lz1 は単品で使える商品でない/);
});
await ta('[Z4] まとまりの子の廃止も同じ: 出した = 409 lz_issued (ファイルの番号)・DB の関数を直に呼んでも断る・取り消すと廃止できる', async () => {
  await asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: uuid(), group: { mode: 'new', code: 'dd-grp3', name: 'ロジザードの試験' }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-GR', name: 'グリーン' }], children: [{ code: 'dd-grp3-GR', choices: { 1: '-GR' }, name: 'ロジザードの試験【グリーン】' }],
    values: { standard_price: '1980', cost: { jpy: '500' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'S01' } },
  { open: true, shippingRates: RATES }));
  const f = await lzIssue('dd-grp3-GR');
  const vc = (o = {}) => asEditor(() => V.cancelVariationChild(db, { actor: 'naka@test', requestId: o.requestId ?? uuid(), code: 'dd-grp3-GR', reason: '作らない' }, { open: true }));
  const e = await rejects(vc(), 409, 'lz_issued');
  assert.match(e.message, new RegExp(`ロジザード登録の CSV に出しています \\(ファイル #${f}\\)。ロジザードから消すのが先です`));
  const direct = await errOf(asEditor(async () => {
    await pg.query('begin');
    try { return await pg.query('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', T.OWN, await sid('dd-grp3-GR')]); }
    finally { await pg.query('rollback'); }
  }));
  assert.match(String(direct && direct.message), /^lz_issued: dd-grp3-GR はロジザード登録の CSV に出しています/);
  assert.equal(await regOf('dd-grp3-GR'), 'draft');
  await lzVoid(f);
  assert.equal((await vc()).state, 'cancelled');
});

// ═══════════ [U] 画面の JS (Playwright) ═══════════
console.log('\n[U] 画面の JS');
if (process.env.MASTER_EDIT_UI_SKIP === '1') console.log('  ⏭️ MASTER_EDIT_UI_SKIP=1');
else {
  let browser = null;
  try { browser = await (await import('playwright')).chromium.launch(); } catch (e) { console.error(`  NG Playwright / Chromium を動かせない: ${e.message} (飛ばす = MASTER_EDIT_UI_SKIP=1)`); process.exitCode = 1; }
  if (browser) {
    const ui = async (name, fn, viewport = { width: 1440, height: 1000 }) => ta(name, async () => {
      const ctx = await browser.newContext({ viewport, locale: 'ja-JP' });
      const p = await ctx.newPage();
      const errors = [];
      p.on('pageerror', (e) => errors.push(e.message));
      p.on('console', (m) => { if (m.type() === 'error' && !/favicon|Failed to load resource/.test(m.text())) errors.push(`console: ${m.text()}`); });
      try { await fn(p); assert.deepEqual(errors, [], '画面の JS の例外'); } finally { await ctx.close(); }
    });
    await ui('[U1] 押す → 窓 (片付けるもの) → 理由で「消す」が押せる → Esc で閉じる → もう一度 → 消す → 読み直し = やめた', async (p) => {
      await p.goto(`${B}/sku/d-u1`);
      await p.click('#deldraft-open');
      await p.waitForSelector('#deldraft-bg.on');
      assert.equal(await p.isDisabled('#deldraft-yes'), true);
      assert.ok((await p.textContent('#deldraft-will')).includes('もう作りません'));
      await p.keyboard.press('Escape');
      await p.waitForSelector('#deldraft-bg:not(.on)', { state: 'attached' });
      await p.click('#deldraft-open');
      await p.fill('#deldraft-reason', 'コードの打ち間違い');
      assert.equal(await p.isDisabled('#deldraft-yes'), false);
      await Promise.all([p.waitForNavigation({ timeout: 15000 }), p.click('#deldraft-yes')]);
      await p.waitForSelector('#cancelled-band');
      assert.equal(await regOf('d-u1'), 'cancelled');
    });
    await ui('[U2] 携帯の幅 (390px) = 消す帯・確かめの窓が画面の幅に収まる (前からの商品の画面と同じだけ)・ボタンは 44px 以上', async (p) => {
      // 商品の画面そのものの横のはみ出し (前からある = この PR の外) を同じ幅の業務で使える商品で測り、下書きの画面がそれより増えないこと
      await p.goto(`${B}/sku/s002`);
      const base = await p.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
      await p.goto(`${B}/sku/d-l1`);
      const w = await p.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
      assert.ok(w <= base, `横にはみ出し ${w}px (前からの商品の画面 ${base}px)`);
      const box = await p.evaluate(() => { const r = document.querySelector('#deldraft').getBoundingClientRect(); return { l: r.left, r: r.right, vw: document.documentElement.clientWidth }; });
      assert.ok(box.l >= 0 && box.r <= box.vw + base, JSON.stringify(box));
      await p.click('#deldraft-open');
      await p.waitForSelector('#deldraft-bg.on');
      const m = await p.evaluate(() => { const r = document.querySelector('#deldraft-bg .modal').getBoundingClientRect(); return { l: r.left, r: r.right, vw: window.innerWidth }; });
      assert.ok(m.l >= 0 && m.r <= m.vw, `窓がはみ出す ${JSON.stringify(m)}`);
      const h = await p.evaluate(() => document.querySelector('#deldraft-open').getBoundingClientRect().height);
      assert.ok(h >= 44, `ボタンの高さ ${h}`);
    }, { width: 390, height: 844 });
    await browser.close();
  }
}

server.close();
console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
