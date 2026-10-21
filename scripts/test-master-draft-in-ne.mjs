/**
 * test-master-draft-in-ne.mjs — 「NE にあるので登録済みにする」(0098・lib/master-draft-in-ne.mjs・2026-10-20 中原さんの承認) を PGlite で確かめる
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 切替の後・持ち主は全部 company・今日の照合の回・新規開始の許可)。書き込みは画面のロール master_edit (本物の関数の権限)。
 * 照合 ② の回は本物の関数 (watch_writer = 判断の候補 → 確かめ待ちの写し → NE のコード → 今朝の NE の値 → 観測 → 封 → 確かめ) で置く (本番の順 = 写しが NE のコードより先)。
 * 固定する契約:
 *   [M] migration の写し: ops.transition_sku_registration (0079)・ops.registration_transition_allowed (0070)・ops.master_write_allowed (0072)・ops.ne_upd_units (0094) は
 *       前の最新の版から写して「🆕 0098」の行だけ足した (印の行を外すと前の版と 1 字も違わない)・確かめ待ちの view = 0079 の版の後ろに枝を 1 つ・
 *       約束の操作の CHECK = 0079 の全部 + draft_ne_confirm・security definer の関数は search_path = pg_catalog, pg_temp・public の実行権なし・一時の表なし
 *   [C] 確かめ (ops.draft_in_ne_check): CSV にしていない下書き (観測の前 = 明日の照合の後に / 観測の後 = ok)・NE に無い・大文字小文字の違うコード・
 *       一部違う (partial) の品目 (CSV の値を NE で直した / 配った後に Company DB の名前を直した = ok・一緒に「使わない」にする品目)・終わっていない品目 (issued)・
 *       色違いの子 (代表が NE と同じ = ok / 違う・NE に代表なし = 断る)・区分が違う・取得が古い・check_hash (同じなら同じ・値が変われば変わる)
 *   [F] 登録済みにする (lib): NE 確認済み・品目 = superseded (resolved_by_human)・履歴の根拠 (DB が作った値)・保存の記録 done・押し直し = 前の答え・
 *       同じ番号で違う表 = request_id_reused・表が変わった = stale・だめ = not_confirmable・MASTER_EDIT_OPEN なし = before_cutover・形の誤り = 400・ロジザードの表に触らない
 *   [N] NE へ送る (ops.ne_upd_units): 下書きの間 = new_product (止める)・登録済みにした後 = send (登録のままの名前も・配った後に直した名前も)・
 *       ふつうの道 (照合で確かめた) の新商品で登録のままの欄は今までどおり出さない
 *   [T] ops.transition_sku_registration を直に呼んでも (運用のロール・持ち主・照合) 下書きは NE 確認済みにならない・ふつうの道 (draft → ne_pending → ne_confirmed) は今までどおり
 *   [R] ロール: master_edit = 確かめ・登録済みにする / watcher = 確かめだけ (読むだけの取引でも) / master_ops = 登録済みにするは 42501 / 部品はだれも・品目の表は直接書けない
 *   [S] 画面 (本物の router): 商品の画面の箱とボタン (名簿 MASTER_DECISION_APPROVERS の人だけ押せる)・押せない理由・「NE 登録の CSV」の行ごとのボタン・API の 403 / 200 と 1 行
 *   [U] 画面の JS (Playwright): 「NE 登録の CSV」の行のボタン → 1 行 / 商品の画面のボタン → 1 行 → 読み直し = 箱が消える / 携帯の幅 (MASTER_EDIT_UI_SKIP=1 で飛ばす)
 * 使い方: node scripts/test-master-draft-in-ne.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIG = path.join(ROOT, 'db', 'company', 'migrations');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
delete process.env.MASTER_EDIT_OPEN;
const watchdog = setTimeout(() => { console.error('  NG  試験が 900 秒で終わらない (止める)'); process.exit(1); }, 900000);
watchdog.unref();

const { setupVariationDb, NOW_MS, BASE_SKUS } = await import('./fixtures/master-variation-db.mjs');
const R = await import('../lib/master-register.mjs');
const V = await import('../lib/master-variation.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const W = await import('../lib/master-write.mjs');
const D = await import('../lib/master-draft-in-ne.mjs');

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const uuid = () => crypto.randomUUID();
const hex = (c) => c.repeat(64);
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
const THIS = migFiles.find((f) => /^\d{4}_draft_in_ne_confirm\.sql$/.test(f));
const THIS_V = THIS.slice(0, 4);
const M98 = readMig(THIS);
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
/** 関数の定義の文字 (create [or replace] function <name>( ... $$;)。見つからない = null */
function fnText(text, name) {
  const i = text.search(new RegExp(`create (?:or replace )?function ${esc(name)}\\(`));
  if (i < 0) return null;
  const k = text.indexOf(' as $$', i);
  const j = text.indexOf('$$;', k + 6);
  return text.slice(i, j + 3).replace(/^create or replace function/, 'create function');
}
/** 「🆕 0098 ここから〜ここまで」のかたまりと「🆕 0098」の行を外す */
function strip0098(t) {
  const out = [];
  let skip = false;
  for (const l of t.split('\n')) {
    if (/-- 🆕 0098 ここから/.test(l)) { skip = true; continue; }
    if (/-- 🆕 0098 ここまで/.test(l)) { skip = false; continue; }
    if (skip || /🆕 0098/.test(l)) continue;
    out.push(l);
  }
  return out.join('\n');
}
const lastDefBefore = (name) => migFiles.filter((f) => f < THIS_V && fnText(readMig(f), name)).pop();
const checkOps = (text, tbl) => {
  const m = new RegExp(`alter table ops\\.${tbl} add constraint ck_\\w+ check \\(operation in \\(([^;]*?)\\)\\);`).exec(text);
  return m ? [...m[1].matchAll(/'([a-z_]+)'/g)].map((x) => x[1]) : null;
};

console.log('[M] migration の写し');
for (const [name, expectFrom] of [['ops.transition_sku_registration', '0079'], ['ops.registration_transition_allowed', '0070'], ['ops.master_write_allowed', '0072'], ['ops.ne_upd_units', '0094']]) {
  await ta(`[M1] ${name} = ${expectFrom} の版 (前の最新) から写して 🆕 0098 の行だけ足した・後の migration は書き換えていない`, async () => {
    const src = lastDefBefore(name);
    assert.ok(src && src.startsWith(expectFrom), `前の最新の版が ${src} (期待 ${expectFrom})`);
    const before = fnText(readMig(src), name);
    const now = fnText(M98, name);
    assert.ok(now, `${THIS} に無い`);
    assert.notEqual(now, before, '何も足していない');
    assert.equal(strip0098(now), before, '印の行を外すと前の版と違う (写し間違い・印の無い変更)');
    for (const f of migFiles.filter((x) => x > THIS)) assert.equal(fnText(readMig(f), name), null, `${f} にも ${name} がある`);
  });
}
await ta('[M2] 確かめ待ちの view = 0079 の版 (前の最新) の後ろに 🆕 0098 の枝を 1 つ足しただけ・約束の操作の CHECK = 0079 の全部 + draft_ne_confirm (どちらの表も)', async () => {
  const viewText = (text) => { const i = text.indexOf('create or replace view ops.v_ne_reg_targets as'); return i < 0 ? null : text.slice(i, text.indexOf(';', i) + 1); };
  const src = migFiles.filter((f) => f < THIS_V && viewText(readMig(f))).pop();
  assert.ok(src.startsWith('0079'), `前の最新の版が ${src}`);
  const before = viewText(readMig(src));
  const i = M98.indexOf('create or replace view ops.v_ne_reg_targets as');
  const now = M98.slice(i, M98.indexOf('-- 🆕 0098 ここまで', i));
  assert.ok(now.startsWith(before.slice(0, -1) + '\n  -- 🆕 0098 ここから'), '0079 の版のまま (最後の ; だけ後ろへ)');
  assert.equal((now.match(/union all/g) || []).length, 2);
  assert.match(now, /'draft_in_ne'::text/);
  for (const tbl of ['master_write_sessions', 'master_edit_requests']) {
    const prev = migFiles.filter((f) => f < THIS_V && checkOps(readMig(f), tbl)).pop();
    assert.ok(prev.startsWith('0079'), `${tbl} の前の最新の CHECK が ${prev}`);
    assert.deepEqual(checkOps(M98, tbl), [...checkOps(readMig(prev), tbl), 'draft_ne_confirm'], tbl);
  }
});
await ta('[M3] 一時の表を使わない・security definer の関数は全部 search_path = pg_catalog, pg_temp・revoke from public・関数の外で値を変えない', async () => {
  assert.doesNotMatch(M98, /\b(create\s+(temp|temporary)\s+table|on commit drop)\b/i);
  const defs = [...M98.matchAll(/create (?:or replace )?function ([a-z_.]+)\(([^)]*)\)[^$]*?security definer[^$]*?as \$\$/g)];
  assert.ok(defs.length >= 4, `security definer の関数 ${defs.length}`);
  for (const d of defs) {
    assert.match(d[0], /set search_path = pg_catalog, pg_temp as \$\$/, d[1]);
    if (!['ops.transition_sku_registration', 'ops.ne_upd_units'].includes(d[1])) assert.ok(M98.includes(`revoke all on function ${d[1]}(`), `${d[1]} の public の実行権を外していない`);
  }
  assert.doesNotMatch(M98.replace(/\$\$[\s\S]*?\$\$/g, ''), /^\s*(insert|update|delete|truncate)\b/im, '関数の外で値を変えない');
  assert.doesNotMatch(M98, /(insert\s+into|update|delete\s+from)\s+ops\.(lz_|master_lz|sku_logizard)/i, 'ロジザードの表に書かない');
});

// ═══════════ DB の用意 ═══════════
const T = await setupVariationDb();
const { pg, db, q, one, asEditor, asRole } = T;
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
const SINGLE = (name) => ({ name, standard_price: '1980', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', sales_class: '3', expiry_managed: '0', reorder_months: '1', cost: { jpy: '500' } });
const draft = (code, name = `下書き ${code}`) => asEditor(() => R.registerNewSku(db, { actor: 'naka@test', requestId: uuid(), kind: 'single', code,
  values: SINGLE(name), card: { create: false } }, { open: true, shippingRates: RATES }));
const sid = async (code) => (await one('select sku_id::text as id from core.skus where code_norm = core.norm_code($1)', [code]))?.id;
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code_norm = core.norm_code($1)', [code]))?.state ?? null;
const itemOf = async (code) => one(`select i.item_id::text, i.export_id::text, i.state, i.superseded_reason, i.superseded_correction, i.state_changed_by
  from ops.ne_reg_export_items i join core.skus k on k.sku_id = i.sku_id where k.code_norm = core.norm_code($1) order by i.item_id desc limit 1`, [code]);
const check = async (code) => (await one('select ops.draft_in_ne_check($1::bigint) as r', [await sid(code)])).r;
const reasons = (c) => c.problems.map((p) => p.reason).sort();
const todayReal = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
/** 画面と同じ保存 (master_edit のロール・saveSku) */
async function save(code, values) {
  const id = await sid(code);
  const cur = await W.readCurrent(db, id, todayReal());
  const ev = (await one('select coalesce(max(event_id), 0)::text as id from events.master_change_events')).id;
  return asEditor(() => W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: cur.code, reason: '試験', seen: { token: W.editTokenOf(cur), event_id: ev }, values }, { open: true }));
}
const confirm = (code, o = {}) => asEditor(async () => D.confirmDraftInNe(db, { actor: o.actor ?? 'naka@test', code, checkHash: o.hash ?? (await check(code)).check_hash, requestId: o.requestId ?? uuid() },
  { open: o.open ?? true }));

// 下書き: dn-a (CSV にしない・NE で先に登録) / dn-b (NE に無い) / Dn-Cs (NE は小文字 dn-cs) / dn-k (NE ではセット) /
//   dn-p (CSV を配った・NE で名前を直した = partial) / dn-e (CSV を配った後に Company DB の名前を直した = partial) / dn-i (配ったが NE にまだ無い) / dn-v (ふつうの道 = 照合で確かめる)
for (const c of ['dn-a', 'dn-b', 'Dn-Cs', 'dn-k', 'dn-p', 'dn-e', 'dn-i', 'dn-v', 'dn-u1', 'dn-u2', 'dn-q']) await draft(c);
// 色違いのまとまり (ポータルで作った) の子: RD = NE の代表もまとまり / BL = NE の代表が違う (ws100) / GR = NE に代表なし
await asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: uuid(), group: { mode: 'new', code: 'dn-grp', name: '登録済みの試験のまとまり' }, axes: [{ axis: 1, name: 'カラー' }],
  options: [{ axis: 1, code: '-RD', name: 'レッド' }, { axis: 1, code: '-BL', name: 'ブルー' }, { axis: 1, code: '-GR', name: 'グリーン' }],
  children: [{ code: 'dn-grp-RD', choices: { 1: '-RD' }, name: '試験【レッド】' }, { code: 'dn-grp-BL', choices: { 1: '-BL' }, name: '試験【ブルー】' }, { code: 'dn-grp-GR', choices: { 1: '-GR' }, name: '試験【グリーン】' }],
  values: { standard_price: '1980', cost: { jpy: '500' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'S01' } },
{ open: true, shippingRates: RATES }));
// NE 登録の CSV (作って配る) = 照合の回を足す前 (作るのは今日の照合の回 RUN1 のとき)
const file = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', codes: ['dn-p', 'dn-e', 'dn-i', 'dn-v', 'dn-q'], requestId: uuid() }, { open: true, nowMs: NOW_MS }));
const FILE_ID = file.export.export_id;
assert.equal((await asEditor(() => G.issueRegExport(db, { actor: 'naka@test', exportId: FILE_ID, requestId: uuid() }, { open: true, nowMs: NOW_MS }))).export.state, 'issued');
// 🆕 0093: 配った後も直せる。CSV の名前 (配った値 = 品目の期待値) を Company DB と NE の両方で直した (本番の bikecover・nanairos) = NE ≠ 期待値 = いつまでも partial
const DN_E_NEW = '配った後に直した名前 dn-e';
await save('dn-e', { name: DN_E_NEW });

// ─── 照合 ② の回 (本物の関数: watch_writer = 判断の候補 → 確かめ待ちの写し → NE のコード → 今朝の NE の値 → 観測 → 封 → 確かめ) ───
const NE = new Set(['dn-a', 'dn-cs', 'dn-k', 'dn-p', 'dn-e', 'dn-i', 'dn-v', 'dn-q', 'dn-u1', 'dn-u2', 'dn-grp-rd', 'dn-grp-bl', 'dn-grp-gr']);   // NE にあるコード (norm)
const NE_SPELL = { 'dn-cs': 'dn-cs', 'dn-grp-rd': 'dn-grp-RD', 'dn-grp-bl': 'dn-grp-BL', 'dn-grp-gr': 'dn-grp-GR' };
let runDay = 0;
let lastStart = Date.now() - 3600e3;
const runIdAt = (at) => `mc_${at.toISOString().slice(0, 10).replace(/-/g, '')}T${String(++runDay).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;
/** 今の Company DB の値と同じ観測 (ops.ne_reg_canonical の期待値と同じ形 = 全部の列が合う) */
async function obsLikeCdb(codeNorm) {
  const k = await one('select sku_id, sku_kind from core.skus where code_norm = $1', [codeNorm]);
  const exp = (await one('select ops.ne_reg_canonical($1, $2::date) as r', [k.sku_id, todayReal()])).r.expected;
  const st = (v) => (v == null ? { st: 'no_value', v: null } : { st: 'ok', v });
  const v = exp.values;
  return { code_norm: codeNorm, present: true, trusted: true, kind: 'single', ne_code: codeNorm,
    cols: { name: st(v.name), supplier: st(v.supplier), cost: st(v.cost), price: st(v.price), tax_rate: st(v.tax_rate), handling: st(v.handling), parent: { st: 'ok', v: v.parent ?? null } } };   // 代表なし = NE の取得では ok の null (本番の観測の形)
}
/** 今の Company DB の値 (D2 = 今朝の NE の値の形) */
async function cdbCols(codeNorm) {
  const r = await one(`select k.name, k.handling, k.tax_rate::float8 as tax_rate, k.standard_price_jpy::float8 as price,
      (select c.cost_jpy::float8 from core.sku_costs c where c.sku_id = k.sku_id and c.valid_to is null) as cost,
      (select coalesce(json_agg(s.code_norm order by s.code_norm), '[]') from core.supplier_skus x join core.suppliers s on s.supplier_id = x.supplier_id where x.sku_id = k.sku_id and x.is_primary) as sup
    from core.skus k where k.code_norm = $1`, [codeNorm]);
  const yen = (v) => (v == null || Number(v) === 0 ? null : Number(v));
  return { name: (r.name || '').trim() || null, handling: r.handling || 'unknown', tax_rate: r.tax_rate, standard_price_jpy: yen(r.price), cost: yen(r.cost),
    primary_supplier: typeof r.sup === 'string' ? JSON.parse(r.sup) : r.sup };
}
/**
 * 照合 ② の回を 1 つ置く。obs = { code_norm: 観測 | (今と同じ観測) => 観測 }・neName = { code_norm: NE の名前 } (観測と今朝の NE の値の両方)・
 * fetchedAt = 取得の時刻 (既定 = 今)・day = 照合の回の日 (RUN1 = 2030-01-10 より後)
 */
async function sealRun({ obs = {}, neName = {}, fetchedAt = null, day = 1 } = {}) {
  const at = new Date(Date.UTC(2030, 0, 10 + day, 5, 0, 0) + runDay * 1000);
  const run = runIdAt(at);
  await asRole('watch_writer', () => pg.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run, observed_at: at.toISOString(), decisions: [] })]));
  const snap = (await asRole('watch_writer', () => pg.query('select ops.snapshot_ne_reg_targets($1) as r', [run]))).rows[0].r;
  // NE のコード (前からの商品 + NE にある下書き)
  const base = BASE_SKUS.map((s) => ({ code_norm: s.code.toLowerCase(), kind: 'product', state: 'ok', ne_code: s.code, spellings: [s.code] }));
  const reps = ['ws100', 'towel-gift'].map((c) => ({ code_norm: c, kind: 'rep', state: 'ok', ne_code: c, spellings: [c] }));
  const mine = [...NE].map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: NE_SPELL[c] || c, spellings: [NE_SPELL[c] || c] }));
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: [...base, ...reps, ...mine] })]);
  // 今朝の NE の値 (NE へ送るの材料)
  const start = Math.max(Date.now(), lastStart + 10);   // 取得の始め = 配った後 (前の回より新しく)
  lastStart = start;
  const codes = [...base.map((x) => x.code_norm), ...NE];
  const entries = [];
  for (const c of codes) {
    const cols = await cdbCols(c);
    if (Object.hasOwn(neName, c)) cols.name = neName[c];
    for (const [col, value] of Object.entries(cols)) entries.push({ code_norm: c, col, ne_kind: 'single', state: 'ok', value });
  }
  const iso = (ms) => new Date(ms).toISOString();
  const n = codes.length;
  const side = (s, k, extra) => ({ started_at: iso(s), finished_at: iso(s + 500), complete_rev: runDay, fetched_rows: k, write_attempts: k, stored_rows: k, dropped_no_code: 0, dropped_missing_fields: 0, raw_rows: k, ...extra });
  await pg.query('select ops.record_ne_values($1::jsonb)', [JSON.stringify({ compare_run_id: run, norm_version: 1,
    fetch: { fingerprint: 'f'.repeat(64), products: side(start, n, { codes: n, set_overlap: 0 }), sets: side(start + 1, 0, { codes: 0 }) }, entries })]);
  // 観測 (写した商品だけ)
  const observations = [];
  for (const t of snap.targets) {
    const b = NE.has(t.code_norm) ? await obsLikeCdb(t.code_norm) : { code_norm: t.code_norm, present: false, trusted: true, kind: null };
    if (NE.has(t.code_norm) && Object.hasOwn(neName, t.code_norm)) b.cols.name = { st: 'ok', v: neName[t.code_norm] };
    const o = obs[t.code_norm];
    observations.push(typeof o === 'function' ? o(b) : o ?? b);
  }
  const fetched = fetchedAt ?? iso(start);   // 観測の取得の時刻 (古い取得の試験 = fetchedAt)
  const w = (await asRole('watch_writer', () => pg.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run,
    fetch: { generation_id: `gen_${run.slice(-6)}`, products_rev: '1', sets_rev: '1', raw_hash: hex('c') }, products_at: fetched, sets_at: fetched, absence_trusted: true, observations })]))).rows[0].r;
  await asRole('watch_writer', () => pg.query('select ops.seal_ne_registration_run($1, $2, $3)', [run, w.observation_hash, hex('e')]));
  const chk = (await asRole('watch_writer', () => pg.query('select ops.record_ne_registration_check($1) as r', [run]))).rows[0].r;
  return { run, targets: snap.targets.map((t) => t.code_norm), chk };
}
const NE_NAMES = { 'dn-a': 'NE で付けた名前 A', 'dn-p': 'NE で直した名前 P', 'dn-e': DN_E_NEW, 'dn-u1': 'NE の名前 U1', 'dn-q': 'NE で直した名前 Q' };
const OBS = { 'dn-i': { code_norm: 'dn-i', present: false, trusted: true, kind: null },
  'dn-k': (b) => ({ ...b, kind: 'set', cols: { name: b.cols.name, price: b.cols.price }, children: [] }),
  'dn-grp-bl': (b) => ({ ...b, cols: { ...b.cols, parent: { st: 'ok', v: 'ws100' } } }),
  'dn-grp-gr': (b) => ({ ...b, cols: { ...b.cols, parent: { st: 'ok', v: null } } }) };
const R2 = await sealRun({ neName: NE_NAMES, obs: OBS, day: 1 });

console.log('\n[C] 確かめ');
await ta('[C0] 照合 ② の確かめ待ち: 配った品目 (前から) は写る・NE にある下書きは「NE のコードの台帳に入った後」の回から写る (本番の順 = 写しが先)', async () => {
  for (const c of ['dn-p', 'dn-e', 'dn-i', 'dn-v']) assert.ok(R2.targets.includes(c), `${c} が写っていない`);
  for (const c of ['dn-a', 'dn-cs', 'dn-grp-rd']) assert.ok(!R2.targets.includes(c), `${c} がもう写っている (台帳の前)`);
  const v = (await q(`select code_norm, state from ops.v_ne_reg_targets where state = 'draft_in_ne' order by 1`)).map((r) => r.code_norm);
  assert.deepEqual(v, ['dn-a', 'dn-cs', 'dn-grp-bl', 'dn-grp-gr', 'dn-grp-rd', 'dn-k', 'dn-u1', 'dn-u2'], '台帳に入った下書き (生きている品目なし) だけ・dn-b (NE に無い) は入らない');
  assert.deepEqual([await regOf('dn-v'), (await itemOf('dn-v')).state], ['ne_confirmed', 'verified'], 'ふつうの道 (照合で確かめた) は今までどおり');
  assert.deepEqual([(await itemOf('dn-p')).state, (await itemOf('dn-e')).state, (await itemOf('dn-i')).state], ['partial', 'partial', 'issued']);
});
await ta('[C1] CSV にしていない下書き: 観測の前 = ne_observed (明日の照合の後に)・画面は箱を出す (押せない)', async () => {
  const c = await check('dn-a');
  assert.deepEqual([c.ok, c.in_ne, c.reg_state, reasons(c)], [false, true, 'draft', ['ne_observed']]);
  assert.match(c.problems[0].text, /明日の朝の照合の後に押せます/);
  const v = D.draftInNeView(c);
  assert.deepEqual([v.show, v.canGo], [true, false]);
});
const R3 = await sealRun({ neName: NE_NAMES, obs: OBS, day: 2 });
await ta('[C2] 次の朝 (観測あり) = ok・NE と違う欄 (名前)・一緒に「使わない」にする品目なし・根拠 (照合の回・NE の書き方・resolved なし)・check_hash は読み直しても同じ', async () => {
  assert.ok(R3.targets.includes('dn-a'));
  const c = await check('dn-a');
  assert.deepEqual([c.ok, c.problems, c.item, c.diffs], [true, [], null, ['name']], JSON.stringify(c.problems));
  assert.equal(c.evidence.rule, 'draft_in_ne_v1');
  assert.equal(c.evidence.compare_run_id, R3.run);
  assert.equal(c.evidence.ne_code, 'dn-a');
  assert.equal(c.evidence.check_hash, c.check_hash);
  assert.equal((await check('dn-a')).check_hash, c.check_hash);
  const v = D.draftInNeView(c);
  assert.deepEqual([v.show, v.canGo, v.diffs, v.resolveItem], [true, true, ['名前'], null]);
});
await ta('[C3] NE に無い = not_in_ne (画面は箱を出さない)・大文字小文字の違うコード (NE dn-cs / 社内 Dn-Cs) = ne_code・区分が違う (NE ではセット) = ne_observed', async () => {
  const b = await check('dn-b');
  assert.deepEqual([b.in_ne, reasons(b)], [false, ['not_in_ne']]);
  assert.equal(D.draftInNeView(b).show, false);
  const cs = await check('Dn-Cs');
  assert.deepEqual(reasons(cs), ['ne_code']);
  assert.match(cs.problems[0].text, /dn-cs.*Dn-Cs.*大文字・小文字/);
  const k = await check('dn-k');
  assert.deepEqual(reasons(k), ['ne_observed']);
  assert.match(k.problems[0].text, /NE ではセット、Company DB では単品/);
  const none = (await one('select ops.draft_in_ne_check(999999999) as r')).r;
  assert.deepEqual([none.ok, reasons(none)], [false, ['no_sku']]);
});
await ta('[C4] 一部違う (partial) の品目: NE で名前を直した (dn-p)・配った後に Company DB の名前を直した (dn-e) = ok・一緒に「使わない」にする品目・終わっていない品目 (issued) = live_file', async () => {
  for (const [code, diffs] of [['dn-p', ['name']], ['dn-e', []]]) {   // dn-e = NE も Company DB も直した名前 (NE と違う所なし)
    const c = await check(code);
    assert.deepEqual([c.ok, c.diffs], [true, diffs], `${code} ${JSON.stringify(c.problems)}`);
    assert.deepEqual([c.item.state, c.item.export_id], ['partial', String(FILE_ID)]);
    assert.equal(c.evidence.item.outcome, 'resolved_by_human');
    assert.equal(D.draftInNeView(c).resolveItem.exportId, String(FILE_ID));
  }
  const i = await check('dn-i');
  assert.ok(reasons(i).includes('live_file'), JSON.stringify(i.problems));
  assert.match(i.problems.find((p) => p.reason === 'live_file').text, new RegExp(`#${FILE_ID} 配った`));
});
await ta('[C5] 色違いの子: 代表が NE と同じ (dn-grp-RD) = ok / NE の代表が違う (ws100) / NE に代表なし = parent (代表をそろえてから)', async () => {
  const rd = await check('dn-grp-RD');
  assert.deepEqual([rd.ok, rd.evidence.parent], [true, { cdb: 'dn-grp', ne: 'dn-grp' }], JSON.stringify(rd.problems));
  const bl = await check('dn-grp-BL');
  assert.deepEqual(reasons(bl), ['parent']);
  assert.match(bl.problems[0].text, /代表が NE \(ws100\) と Company DB \(dn-grp\) で違います/);
  const gr = await check('dn-grp-GR');
  assert.deepEqual(reasons(gr), ['parent']);
  assert.match(gr.problems[0].text, /NE では代表がありませんが、Company DB ではまとまり dn-grp の子です/);
});
await ta('[C6] 取得が 36 時間より古い = ne_run (今朝の照合の後に)・値を直すと check_hash が変わる', async () => {
  await pg.query('begin');
  try {
    await sealRun({ neName: NE_NAMES, obs: OBS, day: 3, fetchedAt: '2026-01-01T00:00:00Z' });
    const c = await check('dn-a');
    assert.ok(reasons(c).includes('ne_run'), JSON.stringify(c.problems));
  } finally { await pg.query('rollback'); }
  const h1 = (await check('dn-a')).check_hash;
  await pg.query('begin');
  try {
    await pg.query(`update core.skus set name = 'ちょっと変えた' where code = 'dn-a'`);
    assert.notEqual((await check('dn-a')).check_hash, h1);
  } finally { await pg.query('rollback'); }
});

console.log('\n[N] NE へ送る (下書きの間)');
const units = async (codes) => (await one(`select ops.ne_upd_units($1::bigint[], 'bulk') as r`, [await Promise.all(codes.map(sid))])).r;
const unitOf = (u, code, col) => u.units.find((x) => x.code_norm === code && x.col === col);
await ta('[N1] 下書きの間: 画面で直した名前 (dn-k) = new_product (止める)・登録のままの名前 (dn-a・dn-p) = 出ない', async () => {
  await save('dn-k', { name: '画面で直した名前 K' });
  const u = await units(['dn-a', 'dn-p', 'dn-k']);
  assert.equal(u.ok, true, JSON.stringify(u.problems));
  assert.deepEqual([unitOf(u, 'dn-k', 'name')?.status, unitOf(u, 'dn-k', 'name')?.why], ['blocked', 'new_product']);
  assert.equal(unitOf(u, 'dn-a', 'name'), undefined);
  assert.equal(unitOf(u, 'dn-p', 'name'), undefined);
});

console.log('\n[F] 登録済みにする');
await ta('[F1] 一部違う品目の商品 (dn-p) = NE 確認済み・品目 = 使わない (resolved_by_human)・履歴の根拠 (DB が作った値)・保存の記録 done・押し直し = 前の答え', async () => {
  const before = await check('dn-p');
  const rid = uuid();
  const r = await confirm('dn-p', { requestId: rid });
  assert.deepEqual([r.ok, r.from, r.state, r.code, r.message], [true, 'draft', 'ne_confirmed', 'dn-p', D.DRAFT_IN_NE_DONE]);
  assert.equal(D.DRAFT_IN_NE_DONE, '登録済みにしました。NE とちがう所は『NEへ送る』に出ます');
  assert.deepEqual([r.resolved_item.item_id, r.resolved_item.outcome], [before.item.item_id, 'resolved_by_human']);
  assert.equal(await regOf('dn-p'), 'ne_confirmed');
  const it = await itemOf('dn-p');
  assert.equal(it.state, 'superseded');
  assert.match(it.superseded_reason, /^resolved_by_human: NE にあるので登録済みにした \(naka@test\)$/);
  assert.match(it.superseded_correction, /NEへ送る/);
  const ev = await one(`select e.from_state, e.to_state, e.actor_type, e.actor_id, e.reason, e.evidence, e.request_id from ops.master_registration_events e where e.sku_id = $1 order by e.event_id desc limit 1`, [await sid('dn-p')]);
  assert.deepEqual([ev.from_state, ev.to_state, ev.actor_type, ev.actor_id, ev.request_id], ['draft', 'ne_confirmed', 'human', 'naka@test', rid]);
  assert.deepEqual([ev.evidence.rule, ev.evidence.check_hash, ev.evidence.item.outcome, ev.evidence.compare_run_id], ['draft_in_ne_v1', before.check_hash, 'resolved_by_human', R3.run]);
  const req = await one('select operation, status, actor_id, sku_id::text from ops.master_edit_requests where request_id = $1', [rid]);
  assert.deepEqual([req.operation, req.status, req.actor_id, req.sku_id], ['draft_ne_confirm', 'done', 'naka@test', await sid('dn-p')]);
  const again = await confirm('dn-p', { requestId: rid, hash: before.check_hash });
  assert.deepEqual([again.replayed, again.state], [true, 'ne_confirmed']);
  assert.equal((await one('select count(*)::int as n from ops.master_registration_events where sku_id = $1 and to_state = $2', [await sid('dn-p'), 'ne_confirmed'])).n, 1, '押し直しで 2 回目を書かない');
  const e = await rejects(confirm('dn-p', { requestId: rid, hash: hex('a') }), 409, 'request_id_reused');
  assert.match(e.message, /何もしていません/);
  const after = await check('dn-p');
  assert.deepEqual([reasons(after), D.draftInNeView(after).show], [['not_draft'], false]);
  assert.equal((await itemOf('dn-i')).state, 'issued', '同じファイルのほかの品目は触らない');
});
await ta('[F2] 配った後に名前を直した商品 (dn-e): 画面を開いた後に値を直した = stale (何もしない) → 読み直して押す = 登録済み', async () => {
  const h = (await check('dn-e')).check_hash;
  await save('dn-e', { name: '配った後に直した名前 dn-e 2' });
  await rejects(confirm('dn-e', { hash: h }), 409, 'stale');
  assert.deepEqual([await regOf('dn-e'), (await itemOf('dn-e')).state], ['draft', 'partial']);
  const r = await confirm('dn-e');
  assert.deepEqual([r.state, r.diffs], ['ne_confirmed', ['name']], '2 回目に直した名前 = NE と違う');
  assert.equal((await itemOf('dn-e')).state, 'superseded');
});
await ta('[F3] CSV にしていない下書き (dn-a) = 登録済み (品目なし)・色違いの子 (代表が同じ) = 登録済み', async () => {
  const r = await confirm('dn-a');
  assert.deepEqual([r.state, r.resolved_item, r.diffs], ['ne_confirmed', null, ['name']]);
  assert.equal((await confirm('dn-grp-RD')).state, 'ne_confirmed');
});
await ta('[F4] だめ = not_confirmable (DB の文のまま)・NE に無い・代表が違う・MASTER_EDIT_OPEN なし = before_cutover・確かめの表が無い = 400・無いコード = 404', async () => {
  const e1 = await rejects(confirm('dn-b'), 409, 'not_confirmable');
  assert.match(e1.message, /NE にこのコードの商品がありません/);
  const e2 = await rejects(confirm('dn-grp-BL'), 409, 'not_confirmable');
  assert.match(e2.message, /代表をそろえてから/);
  await rejects(confirm('dn-u2', { open: false }), 409, 'before_cutover');
  await rejects(confirm('dn-u2', { hash: 'abc' }), 400, 'invalid_input');
  await rejects(confirm('zz-none', { hash: hex('a') }), 404, 'not_found');
  assert.deepEqual(await Promise.all(['dn-b', 'dn-grp-BL', 'dn-u2'].map(regOf)), ['draft', 'draft', 'draft'], '何も変えない');
});
await ta('[F5] ロジザードの表・商品の値は変えない (登録済みにする前と後で同じ)', async () => {
  const snap = async () => JSON.stringify(await q(`select k.code, k.name, k.version::text, (select count(*) from ops.lz_new_export_items l where l.sku_id = k.sku_id)::int as lz
    from core.skus k where k.code = 'dn-u2'`));
  const before = await snap();
  const n0 = (await one('select count(*)::int as n from events.master_change_events')).n;
  // dn-u2 は NE の値が Company DB と同じ (差なし) = diffs []
  const c = await check('dn-u2');
  assert.deepEqual([c.ok, c.diffs], [true, []], JSON.stringify(c.problems));
  await confirm('dn-u2');
  assert.equal(await snap(), before, '商品の行 (名前・版)・ロジザード登録の CSV は同じ');
  assert.equal((await one('select count(*)::int as n from events.master_change_events')).n, n0, '商品の値の変更の記録が増えない');
});

console.log('\n[N] NE へ送る (登録済みにした後)');
await ta('[N2] 登録済みにした後 = send: 配った後に直した名前 (dn-e)・登録のままの名前 (dn-a = 0098 の行)。ふつうの道の新商品 (dn-v) で登録のままの名前は今までどおり出ない', async () => {
  // dn-v (照合で確かめた新商品) は NE で名前が変わった (Company DB は登録のまま) = わざと残した差 (今までどおり出さない)
  await sealRun({ neName: { ...NE_NAMES, 'dn-v': 'NE で変えた名前 V' }, obs: OBS, day: 4 });
  const u = await units(['dn-a', 'dn-p', 'dn-e', 'dn-v', 'dn-u2']);
  assert.equal(u.ok, true, JSON.stringify(u.problems));
  const a = unitOf(u, 'dn-a', 'name');
  assert.deepEqual([a?.status, a?.why, a?.ne_value, a?.cdb_value, a?.cell], ['send', null, 'NE で付けた名前 A', '下書き dn-a', '下書き dn-a'], JSON.stringify(a));
  const p = unitOf(u, 'dn-p', 'name');
  assert.deepEqual([p?.status, p?.ne_value, p?.cdb_value], ['send', 'NE で直した名前 P', '下書き dn-p'], JSON.stringify(p));
  const e = unitOf(u, 'dn-e', 'name');
  assert.deepEqual([e?.status, e?.cdb_value], ['send', '配った後に直した名前 dn-e 2'], JSON.stringify(e));
  assert.equal(unitOf(u, 'dn-v', 'name'), undefined, 'ふつうの道の新商品は今までどおり (登録のままの欄は出さない)');
  assert.deepEqual(u.units.filter((x) => x.code_norm === 'dn-u2'), [], '差が無い商品は出ない');
  assert.deepEqual(u.units.filter((x) => x.code_norm === 'dn-a').map((x) => x.col), ['name'], '名前のほか (原価・売価ほか) は NE と同じ = 出ない');
});
await ta('[N3] 翌朝の照合の確かめ: 終わった商品のファイルは、ほかの品目が終わるまで閉じない (dn-i がまだ配った)', async () => {
  assert.equal((await one('select state from ops.ne_reg_exports where export_id = $1', [FILE_ID])).state, 'issued');
  assert.deepEqual((await q('select state from ops.ne_reg_export_items where export_id = $1 order by item_id', [FILE_ID])).map((r) => r.state).sort(), ['issued', 'partial', 'superseded', 'superseded', 'verified']);
});

console.log('\n[T] 状態を直に進める');
await ta('[T1] ops.transition_sku_registration を直に呼んでも下書きは NE 確認済みにならない (運用のロール・持ち主・照合)・ふつうの道は今までどおり', async () => {
  const id = await sid('dn-grp-GR');
  for (const [role, type, actor] of [['master_ops', 'human', 'naka@test'], ['deploy', 'human', 'naka@test'], ['deploy', 'system', 'ne_compare']]) {
    const e = await errOf(() => asRole(role, () => pg.query(`select ops.transition_sku_registration($1::bigint, 'ne_confirmed', $2, $3, null, '{}'::jsonb, null)`, [id, type, actor])));
    assert.ok(e, `${role} が通った`);
    assert.match(String(e.message), /confirm_session_required|permission denied/, `${role}: ${e.message}`);
  }
  assert.equal(await regOf('dn-grp-GR'), 'draft');
  // 呼び手の根拠は受けない (約束の中でも) = 0098 のかたまりの caller_evidence は、約束を作れるのが関数だけなので届かない (下の [R] で画面のロールが約束を作れないことを見る)
});

console.log('\n[R] ロール');
await ta('[R1] master_edit = 確かめ・登録済みにする / watcher = 確かめだけ (読むだけの取引でも) / master_ops = 登録済みにするは 42501 / 部品はだれも・品目の表は直接書けない', async () => {
  const priv = async (role, sig) => (await one(`select has_function_privilege($1, $2, 'EXECUTE') as ok`, [role, sig])).ok;
  assert.deepEqual([await priv('master_edit', D.DRAFT_IN_NE_CHECK_FN), await priv('master_edit', D.DRAFT_IN_NE_CONFIRM_FN)], [true, true]);
  assert.deepEqual([await priv('watcher', D.DRAFT_IN_NE_CHECK_FN), await priv('watcher', D.DRAFT_IN_NE_CONFIRM_FN)], [true, false]);
  assert.deepEqual([await priv('master_ops', D.DRAFT_IN_NE_CONFIRM_FN), await priv('watch_writer', D.DRAFT_IN_NE_CONFIRM_FN)], [false, false]);
  for (const role of ['master_edit', 'watcher', 'master_ops', 'watch_writer']) assert.equal(await priv(role, 'ops._draft_in_ne_problem(text, text)'), false, role);
  assert.equal((await one(`select count(*)::int as n from pg_proc p, aclexplode(p.proacl) a where p.oid = 'ops.confirm_draft_in_ne(uuid, text, bigint, text)'::regprocedure and a.grantee = 0`)).n, 0, 'public に実行権が無い');
  const ro = await asRole('watcher', async () => {
    await pg.query('begin read only');
    try { return (await pg.query('select ops.draft_in_ne_check($1::bigint) as r', [await sid('dn-grp-GR')])).rows[0].r; } finally { await pg.query('rollback'); }
  });
  assert.deepEqual(reasons(ro), ['parent']);
  const e1 = await errOf(() => asRole('master_ops', () => pg.query('select ops.confirm_draft_in_ne($1::uuid, $2, $3::bigint, $4)', [uuid(), 'naka@test', '1', hex('a')])));
  assert.equal(e1?.code, '42501');
  const e2 = await errOf(() => asEditor(() => pg.query(`update ops.ne_reg_export_items set state = 'superseded' where item_id = 1`)));
  assert.ok(e2, '画面のロールが品目の表を直接書けた');
  const gr = await sid('dn-grp-GR');
  const e3 = await errOf(() => asEditor(() => pg.query(`select ops.transition_sku_registration($1::bigint, 'ne_confirmed', 'human', 'naka@test', null, '{}'::jsonb, null)`, [gr])));
  assert.ok(e3, '画面のロールが状態を直に進めた');
});

// ═══════════ [S] 画面 (本物の router) ═══════════
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/dine';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/dine';
process.env.MASTER_EDITORS = 'naka@test,other@test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const RT = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
RT.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
RT.__setShippingRatesProvider(async () => RATES);
RT.__setLiveCardsReader(async () => ({ cards: [] }));
RT.__setClock(() => NOW_MS);
let who = 'naka@test';
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: who, displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', RT.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const HOST = `127.0.0.1:${server.address().port}`;
const B = `http://${HOST}/apps/master-edit`;
const page = async (p) => { const r = await fetch(`${B}${p}`); return { status: r.status, html: await r.text() }; };
const post = async (code, body) => {
  const r = await fetch(`${B}/api/sku/${encodeURIComponent(code)}/draft-in-ne`, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: `http://${HOST}` }, body: JSON.stringify(body) });
  return { status: r.status, j: await r.json() };
};

console.log('\n[S] 画面');
await ta('[S1] 商品の画面: 押せる下書き = 箱と「NE にあるので登録済みにする」(名簿の人だけ)・NE と違う欄・押せない = 理由・業務で使える商品 / NE に無い下書き = 出さない', async () => {
  who = 'naka@test';
  const p = await page('/sku/dn-u1');
  assert.equal(p.status, 200);
  assert.match(p.html, /id="dine"/);
  assert.match(p.html, /<button[^>]*id="dine-go"[^>]*data-dine-code="dn-u1"[^>]*data-dine-hash="[0-9a-f]{64}"[^>]*>NE にあるので登録済みにする<\/button>/);
  assert.match(p.html, /NE とちがう所 \(名前\) は、登録済みにした後「NEへ送る」に出ます/);
  assert.match(p.html, /me-draft-in-ne\.js/);
  who = 'other@test';
  const o = await page('/sku/dn-u1');
  assert.match(o.html, /id="dine"/);
  assert.doesNotMatch(o.html, /id="dine-go"/);
  assert.match(o.html, /名簿 \(MASTER_DECISION_APPROVERS\) の人だけ/);
  assert.doesNotMatch(o.html, /me-draft-in-ne\.js/);
  who = 'naka@test';
  const bl = await page('/sku/dn-grp-BL');
  assert.match(bl.html, /いまは登録済みにできません/);
  assert.match(bl.html, /<li data-reason="parent">代表が NE \(ws100\)/);
  assert.doesNotMatch(bl.html, /id="dine-go"/);
  for (const c of ['s001', 'dn-b', 'dn-p']) assert.doesNotMatch((await page(`/sku/${c}`)).html, /id="dine"/, c);
});
await ta('[S2] NE 登録の CSV の画面: 「NE にもうある商品」の行ごとのボタン (押せる商品だけ)・押せない = 理由の 1 つ・前の「まだありません」の文は無い', async () => {
  const p = await page('/reg-csv');
  assert.equal(p.status, 200);
  assert.match(p.html, /id="in-ne-list"/);
  assert.match(p.html, /<li data-in-ne="dn-u1">[\s\S]*?data-dine-code="dn-u1"[\s\S]*?NE とちがう所: 名前/);
  assert.match(p.html, /<li data-in-ne="dn-grp-BL">[\s\S]*?登録済みにはまだできません: 代表が NE/);
  assert.match(p.html, /<li data-in-ne="Dn-Cs">[\s\S]*?大文字・小文字/);
  assert.doesNotMatch(p.html, /登録済みにする操作は、まだありません/);
  // 直すもの: 一部違う (partial) の行にもボタン (今の Company DB の値のまま登録済みに)
  assert.match(p.html, /<li class="why-wrn" data-partial="dn-q">[\s\S]*?「NE にあるので登録済みにする」を押すと、今の Company DB の値のまま登録済みになります[\s\S]*?data-dine-code="dn-q"/);
  assert.match(p.html, /me-draft-in-ne\.js/);
});
await ta('[S3] API: 名簿の人でない = 403・確かめの表なし = 400・押す = 200 と 1 行「登録済みにしました。NE とちがう所は『NEへ送る』に出ます」', async () => {
  const h = (await check('dn-u1')).check_hash;
  who = 'other@test';
  const f = await post('dn-u1', { request_id: uuid(), check_hash: h });
  assert.deepEqual([f.status, f.j.reason], [403, 'not_approver']);
  who = 'naka@test';
  const b = await post('dn-u1', { request_id: uuid() });
  assert.deepEqual([b.status, b.j.reason], [400, 'invalid_input']);
  const rid = uuid();
  const ok = await post('dn-u1', { request_id: rid, check_hash: h });
  assert.deepEqual([ok.status, ok.j.ok, ok.j.state, ok.j.message], [200, true, 'ne_confirmed', '登録済みにしました。NE とちがう所は『NEへ送る』に出ます']);
  const again = await post('dn-u1', { request_id: rid, check_hash: h });
  assert.deepEqual([again.status, again.j.replayed], [200, true]);
  assert.doesNotMatch((await page('/sku/dn-u1')).html, /id="dine"/, '登録済み = 箱を出さない');
});

// ═══════════ [U] 画面の JS (Playwright) ═══════════
console.log('\n[U] 画面の JS');
if (process.env.MASTER_EDIT_UI_SKIP === '1') console.log('  ⏭️ MASTER_EDIT_UI_SKIP=1');
else {
  // 押す商品をもう 2 つ (NE にある下書き・次の朝の観測あり)
  for (const c of ['dn-w1', 'dn-w2']) await draft(c);
  NE.add('dn-w1'); NE.add('dn-w2');
  await sealRun({ neName: NE_NAMES, obs: OBS, day: 5 });
  await sealRun({ neName: NE_NAMES, obs: OBS, day: 6 });
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
    await ui('[U1] NE 登録の CSV の画面: 行のボタン → 1 行「登録済みにしました…」・ボタンが消える = 登録済み', async (p) => {
      await p.goto(`${B}/reg-csv`);
      const btn = p.locator('[data-dine-code="dn-w1"]');
      await btn.click();
      const msg = p.locator('li[data-in-ne="dn-w1"] [data-dine-msg]');
      await msg.filter({ hasText: '登録済みにしました' }).waitFor({ timeout: 15000 });
      assert.equal(await msg.textContent(), '登録済みにしました。NE とちがう所は『NEへ送る』に出ます');
      assert.equal(await btn.isHidden(), true);
      assert.equal(await regOf('dn-w1'), 'ne_confirmed');
    });
    await ui('[U2] 商品の画面 (携帯の幅 390px): 箱が幅に収まる・ボタン 44px 以上 → 押す → 1 行 → 読み直し = 箱が消える', async (p) => {
      await p.goto(`${B}/sku/s002`);
      const base = await p.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
      await p.goto(`${B}/sku/dn-w2`);
      const w = await p.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
      assert.ok(w <= base, `横にはみ出し ${w}px (前からの商品の画面 ${base}px)`);
      const box = await p.evaluate(() => { const r = document.querySelector('#dine').getBoundingClientRect(); return { l: r.left, r: r.right, vw: document.documentElement.clientWidth }; });
      assert.ok(box.l >= 0 && box.r <= box.vw + base, JSON.stringify(box));
      const h = await p.evaluate(() => document.querySelector('#dine-go').getBoundingClientRect().height);
      assert.ok(h >= 44, `ボタンの高さ ${h}`);
      await Promise.all([p.waitForNavigation({ timeout: 20000 }), p.click('#dine-go')]);
      await p.waitForSelector('#tab-basic');
      assert.equal(await p.locator('#dine').count(), 0);
      assert.equal(await regOf('dn-w2'), 'ne_confirmed');
    }, { width: 390, height: 844 });
    await browser.close();
  }
}

server.close();
console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
