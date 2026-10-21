#!/usr/bin/env node
/**
 * set-derived-catchup.mjs — セットの導く値 (税率・取扱・原価) を、今の構成 (core.sku_components) で計算し直す (人が 1 回ずつ流す道具・定期実行ではない)
 * (2026-10-21 中原さん承認「セットの中身を NE で変えても、税率・取扱・原価が前の構成のまま残る」の後始末)
 *
 * なぜ道具が要るか: 10/5 からセットの導く値の持ち主は company (構成品から導く)・構成の持ち主は load (夜間ロードが NE の中身を書く)。
 *   この PR から夜間ロードは構成を変えたセットを commit の後に計算し直す (lib/master-write.mjs の recomputeSetsAfterLoad)。でも前から残っているずれ
 *   (本番 10/21: maikoset の原価 1,562 円 ≠ 構成品の合計 2,040 円・NE で直接作られた要確認のセットの原価が空) は、構成がもう変わらないので夜間ロードは拾わない。
 *   → 今の構成で 1 回だけ計算し直す。migration は要らない (画面の保存と同じ書き方 = 持ち主のロールで core.skus・core.sku_costs)。
 *
 * 書くもの (apply・1 つの取引・全部か何もしないか): 計画の各セットについて、画面の保存と同じ lib/master-write.mjs の recomputeSet
 *   (planSetDerived = lib/master-set-rules.js の規則) で
 *   税率・税区分 (core.skus)・取扱 (core.skus.handling)・原価 (今日から set_calc の行・前の行は昨日で閉じる・今日始まりの行は消す・合計できない = 今日から空)。
 * 人の値は残す (planSetDerived のとおり): 例外原価 (cost_source manual / override_zero) は計算で上書きしない・セット自身の取扱 (handling_own) が無いセットは
 *   構成品が止まったときだけ止める (止まっているセットを取扱中に戻さない)・売上分類の上書き (set_sales_class_override) は core に導いた売上分類を持たないので触らない。
 * 直さない (計画から外して表に出す): 構成が空・やめたセット (cancelled)・先の日付の原価がある (set_cost_future)・今日以降を覆う閉じた原価の行がある (期間が重なる)・
 *   閉じていない原価の行が 2 つ以上
 * 変更の記録 (0026 の trigger): 人 (--actor)・理由 (--reason)・request_id・source_system = set_derived_catchup
 * 確かめ: 切替の段階 new_open・導く値の持ち主 (DB の active) が全部 company。apply は dry-run の計画のハッシュ (--expect-hash) と同じときだけ
 *   (人・理由・今日・対象・今の値・書くもの全部。鍵の後に読み直して計算し直す)。書いた答えが計画と違う = 巻き戻す
 * 鍵 (apply): 段階 (共有) → マスタの書き込み (共有・夜間ロードと並ばない) → SKU ごと (セット + 構成品・sku_id の順) → NE の CSV → セットの行 → 読み直す → ハッシュ
 *   (画面の保存・構成の昇格・夜間ロードの後の計算し直しと同じ順)
 * 🚨 NE には何も送らない (NE のセットの欄は名前・売価だけ = 「NEへ送る」にも出ない)。2 回流しても同じ (直した後の dry-run は「直すもの 0」)
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下。DB = env COMPANY_DB_URL (DB の持ち主) か --url):
 *   # 1. 読むだけ (既定・読み取りの取引)。直すセット・前 → 後・計画のハッシュ。直さない理由があれば exit 1 (表は出す)
 *   node -r dotenv/config scripts/company-db/set-derived-catchup.mjs --actor <人のメール> --reason '<理由>' [--code maikoset ...]
 *   # 2. 書く (dry-run の表を中原さんが見て OK の後だけ・同じ日 (JST)・同じ --actor / --reason / --code)
 *   node -r dotenv/config scripts/company-db/set-derived-catchup.mjs --actor <人のメール> --reason '<理由>' [--code ...] --apply --expect-hash <64 桁> --yes
 * 終了コード: 0 = dry-run で直せる (直すもの 0 も) / apply で commit (直すもの 0 = 何も書かない)・1 = 直さない / 巻き戻した / 書けたか分からない・つながらない・2 = 引数が違う
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { normSku } from '../../lib/sku-norm.js';
import { planSetDerived, recomputeSetInTx, costAsOfJoin, jstDate, lockMasterWriteShared, SET_DERIVED_KEYS, MasterWriteError } from '../../lib/master-write.mjs';
import { CUTOVER_SHARED_LOCK_SQL } from '../../lib/master-cutover.mjs';
import { CSV_LOCK_SQL, SKU_LOCK_SQL } from '../../apps/master-decisions/ne-csv-lock.mjs';

export const SOURCE_SYSTEM = 'set_derived_catchup';
export const MAX_CODES = 50;
const PLAN_VERSION = 1;
const KNOWN_COST = new Set(['COMPLETE', 'OVERRIDDEN']);
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const fail = (message) => Object.assign(new Error(message), { code: 'CATCHUP_ARGS' });
export const stable = (v) => (Array.isArray(v) ? `[${v.map(stable).join(',')}]`
  : v && typeof v === 'object' ? `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}` : JSON.stringify(v));
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const byId = (a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0);

/** 引数を読む。誤り = CATCHUP_ARGS */
export function parseCatchupArgs(argv) {
  const o = { codes: [], apply: false, yes: false, expectHash: null, actor: null, reason: null, json: null, url: null };
  const need = (i, name) => { const v = argv[i + 1]; if (v == null || v.startsWith('--')) throw fail(`${name} の後に値が要る`); return v; };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--code') { o.codes.push(need(i, a).trim()); i++; }
    else if (a === '--apply') o.apply = true;
    else if (a === '--yes') o.yes = true;
    else if (a === '--dry-run') o.apply = false;
    else if (a === '--expect-hash') { const v = need(i, a); if (!/^[0-9a-f]{64}$/.test(v)) throw fail('--expect-hash は dry-run が出した 64 桁の 16 進'); o.expectHash = v; i++; }
    else if (a === '--actor') { o.actor = need(i, a); i++; }
    else if (a === '--reason') { o.reason = need(i, a).trim(); i++; }
    else if (a === '--json') { o.json = need(i, a); i++; }
    else if (a === '--url') { o.url = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (o.codes.length > MAX_CODES) throw fail(`--code は ${MAX_CODES} 個まで (付けない = 全部のセット)`);
  for (const c of o.codes) if (!/^[A-Za-z0-9_-]{1,80}$/.test(c)) throw fail(`--code ${c} は英数字と - _ の 1〜80 字`);
  if (new Set(o.codes.map(normSku)).size !== o.codes.length) throw fail('--code が重なっている (大文字小文字をそろえて同じ)');
  if (!o.reason || o.reason.length > 200 || CTRL_RE.test(o.reason)) throw fail('--reason <理由> が要る (1〜200 字・制御文字なし・dry-run も apply も同じ理由)');
  if (!o.actor || !/^[^\s@]+@[^\s@]+$/.test(o.actor) || o.actor.length > 320) throw fail('--actor <人のメール> が要る (dry-run も apply も同じ人)');
  if (o.apply) {
    if (o.expectHash == null) throw fail('--apply は --expect-hash <dry-run が出した計画のハッシュ> が要る');
    if (!o.yes) throw fail('--apply は --yes が要る (dry-run の表を中原さんが見て OK の後だけ)');
  }
  return o;
}

/**
 * 今の Company DB を読んで、計算し直すセット (計画) と直さないもの・理由を決める (読むだけ・セットの全部を 4 つの問い合わせで)。
 * 戻り値 { plan: { items }, context, excluded, kept, counts, problems, warnings, hash }
 */
export async function readCatchupPlan(db, opts, { now = new Date() } = {}) {
  const { codes = [], actor, reason } = opts;
  const problems = [];
  const warnings = [];
  const today = jstDate(now);
  const cut = (await db.query('select phase from ops.master_cutover_state where id = 1')).rows[0] || {};
  const own = (await db.query('select active_hash, active_map from ops.master_ownership_state where id = 1')).rows[0] || {};
  const ownerOf = (k) => (own.active_map || {})[k] ?? 'load';
  const context = { today, phase: cut.phase ?? null, active_hash: own.active_hash ?? null, components_owner: ownerOf('sku_components'),
    derived_owners: Object.fromEntries(SET_DERIVED_KEYS.map((k) => [k, ownerOf(k)])) };
  if (context.phase !== 'new_open') problems.push(`切替の段階が ${context.phase} (new_open でない)`);
  const loadKeys = SET_DERIVED_KEYS.filter((k) => ownerOf(k) !== 'company');
  if (loadKeys.length) problems.push(`セットの導く値の持ち主に load がある (${loadKeys.join('・')}) = 夜間ロードが NE に合わせる (この道具は要らない)`);
  const norms = codes.map(normSku);
  const where = norms.length ? 'and s.code_norm = any($1::text[])' : '';
  const sets = (await db.query(`select s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.tax_rate::text as tax_rate, s.tax_class, s.handling, s.handling_own, r.state as reg
      from core.skus s left join ops.master_registrations r on r.sku_id = s.sku_id
     where s.company_id = 1 and (s.sku_kind = 'set' ${norms.length ? 'or s.code_norm = any($1::text[])' : ''}) ${where} order by s.code_norm`, norms.length ? [norms] : [])).rows;
  for (const n of norms) {
    const s = sets.find((x) => x.code_norm === n);
    if (!s) problems.push(`${codes[norms.indexOf(n)]}: Company DB に無い`);
    else if (s.sku_kind !== 'set') problems.push(`${s.code}: セットでない (${s.sku_kind})`);
  }
  const setRows = sets.filter((s) => s.sku_kind === 'set');
  const ids = setRows.map((s) => s.sku_id);
  const comps = new Map(ids.map((id) => [id, []]));
  const costs = new Map(ids.map((id) => [id, []]));
  const later = new Map();
  if (ids.length) {
    for (const c of (await db.query(`select c.parent_sku_id::text as set_id, c.child_sku_id::text as sku_id, k.code, c.qty, k.tax_rate::text as tax_rate, k.handling,
          x.cost_jpy::text as cost_jpy, x.cost_status
        from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id ${costAsOfJoin('c.child_sku_id', '$2', 'x')}
       where c.parent_sku_id = any($1::bigint[]) order by c.parent_sku_id, c.sort_order, k.code_norm`, [ids, today])).rows) {
      comps.get(c.set_id).push({ sku_id: c.sku_id, code: c.code, qty: Number(c.qty), tax_rate: c.tax_rate, handling: c.handling, cost_jpy: KNOWN_COST.has(c.cost_status) ? c.cost_jpy : null });
    }
    for (const x of (await db.query(`select sku_id::text as sku_id, sku_cost_id::text as id, cost_jpy::text as cost_jpy, cost_source, cost_status, valid_from::text as valid_from
        from core.sku_costs where sku_id = any($1::bigint[]) and valid_to is null order by sku_id, sku_cost_id`, [ids])).rows) costs.get(x.sku_id).push(x);
    for (const x of (await db.query(`select sku_id::text as sku_id, count(*)::int as n from core.sku_costs where sku_id = any($1::bigint[]) and valid_to is not null and valid_to >= $2::date
        group by sku_id`, [ids, today])).rows) later.set(x.sku_id, x.n);
  }
  const items = [];
  const excluded = [];
  const kept = [];   // 人の値で残した (例外原価で、構成品の合計と違う)
  const counts = { sets: setRows.length, same: 0, no_components: 0, cancelled: 0 };
  for (const s of setRows) {
    const list = comps.get(s.sku_id);
    if (!list.length) { counts.no_components++; continue; }
    if (s.reg === 'cancelled') { counts.cancelled++; continue; }
    const open = costs.get(s.sku_id);
    if (open.length > 1) { excluded.push({ code: s.code, reason: 'open_costs', text: `閉じていない原価の行が ${open.length} 行` }); continue; }
    const oc = open[0] ? { id: open[0].id, cost_jpy: Number(open[0].cost_jpy), cost_source: open[0].cost_source, cost_status: open[0].cost_status, valid_from: open[0].valid_from } : null;
    let p;
    try {
      p = planSetDerived({ code: s.code, tax_rate: s.tax_rate, tax_class: s.tax_class, handling: s.handling, handling_own: s.handling_own, open_cost: oc }, list, { tax: true, handling: true, cost: true }, today);
    } catch (e) {
      if (!(e instanceof MasterWriteError)) throw e;
      excluded.push({ code: s.code, reason: e.reason, text: e.message });
      continue;
    }
    if (oc && (oc.cost_source === 'manual' || oc.cost_source === 'override_zero')) {
      const sum = planSetDerived({ code: s.code, tax_rate: s.tax_rate, tax_class: s.tax_class, handling: s.handling, handling_own: s.handling_own, open_cost: null }, list, { cost: true }, today).costPlan;
      if (sum && sum !== 'clear' && sum.jpy !== oc.cost_jpy) kept.push({ code: s.code, exception_jpy: oc.cost_jpy, components_jpy: sum.jpy, cost_source: oc.cost_source });
    }
    if (!p.changes.length) { counts.same++; continue; }
    if (p.costPlan && later.get(s.sku_id)) {
      excluded.push({ code: s.code, reason: 'cost_overlap', text: `今日 (${today}) 以降を覆う閉じた原価の行が ${later.get(s.sku_id)} 行 = 今日からの原価を足すと期間が重なる (画面で原価を確かめてから)` });
      continue;
    }
    items.push({
      sku_id: s.sku_id, code: s.code, reg: s.reg ?? null,
      before: { tax_rate: s.tax_rate, tax_class: s.tax_class, handling: s.handling, handling_own: s.handling_own ?? null, open_cost: oc },
      components: list.map((c) => ({ sku_id: c.sku_id, code: c.code, qty: c.qty, cost_jpy: c.cost_jpy })),
      changes: p.changes, cost_plan: p.costPlan, notes: p.notes,
    });
  }
  if (excluded.length) warnings.push(`直さないセット ${excluded.length} 件 (下の表・人が画面で見る)`);
  const plan = { items };
  const hash = sha(stable({ v: PLAN_VERSION, actor, reason, codes: [...norms].sort(), context, plan, excluded, problems }));
  return { plan, context, excluded, kept, counts, problems, warnings, hash };
}

const yen = (v) => (v == null ? '空' : `${Number(v).toLocaleString('ja-JP')} 円`);
const valOf = (col, x) => (col === 'tax_rate' ? (x && x.rate != null ? `${Math.round(Number(x.rate) * 100)}% (${x.class})` : `未決 (${x?.class ?? '—'})`) : col === 'cost' ? yen(x) : x ?? '空');

/** 人が読む表 */
export function planLines(r) {
  const out = [];
  if (r.context) out.push(`今日 (JST) ${r.context.today}・段階 ${r.context.phase}・構成の持ち主 ${r.context.components_owner}・導く値の持ち主 ${Object.values(r.context.derived_owners).join('/')}`);
  if (r.counts) out.push(`セット ${r.counts.sets} 件: 直す ${r.plan.items.length}・今のままで同じ ${r.counts.same}・構成が空 ${r.counts.no_components}・やめた ${r.counts.cancelled}・直さない ${r.excluded.length}`);
  for (const it of r.plan.items) {
    const b = it.before;
    out.push(`■ ${it.code} (SKU ${it.sku_id}・登録 ${it.reg ?? '行なし'}): 構成 ${it.components.map((c) => `${c.code} × ${c.qty} (${yen(c.cost_jpy)})`).join('・')}`);
    for (const c of it.changes) {
      out.push(`  ${c.col}: ${valOf(c.col, c.from)} → ${valOf(c.col, c.to)}${c.col === 'cost' ? ` (今日 ${r.context.today} から set_calc・前の原価 ${b.open_cost ? `${b.open_cost.cost_source} ${b.open_cost.valid_from}〜` : 'なし'})` : ''}`);
    }
    for (const n of it.notes) out.push(`  メモ: ${n}`);
  }
  for (const k of r.kept || []) out.push(`  (人の値で残す) ${k.code}: 例外原価 ${yen(k.exception_jpy)} (${k.cost_source})・構成品の合計なら ${yen(k.components_jpy)}`);
  for (const x of r.excluded || []) out.push(`⏭️ ${x.code}: ${x.text}`);
  for (const w of r.warnings || []) out.push(`⚠️ ${w}`);
  for (const p of r.problems || []) out.push(`⛔ ${p}`);
  return out;
}

/** commit の答えが分からないとき: 新しい接続で request_id の変更の記録を読む。'written' / 'not_written' / 'unknown' */
export async function outcomeOf(reconnect, requestId, skuIds) {
  if (!reconnect) return { state: 'unknown', why: '読み直す接続が無い' };
  let c = null;
  try {
    c = await reconnect();
    await c.db.query('begin');
    let n;
    try {
      // 元の取引は commit / rollback まで SKU の鍵を持つ = 同じ鍵を取れた = 元の取引は終わった (取れない = lock_timeout = 分からない)
      for (const id of [...skuIds].sort(byId)) await c.db.query(SKU_LOCK_SQL, [id]);
      n = (await c.db.query('select count(*)::int as n from events.master_change_events e where e.request_id = $1', [requestId])).rows[0].n;
    } finally { try { await c.db.query('rollback'); } catch { /* */ } }
    return { state: n > 0 ? 'written' : 'not_written' };
  } catch (e) {
    return { state: 'unknown', why: String((e && e.message) || e).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]').slice(0, 200) };
  } finally { if (c && c.close) { try { await c.close(); } catch { /* */ } } }
}

/** 書く (1 つの取引)。db = DB の持ち主の接続 */
export async function applyCatchup(db, opts, { now = new Date(), reconnect = null, log = console.log } = {}) {
  const requestId = opts.requestId || crypto.randomUUID();
  let r;
  try {
    await db.query('begin');
    // 鍵の順 = 画面の保存・構成の昇格・夜間ロードの後の計算し直し (lib/master-write.mjs) と同じ: 段階 (共有) → マスタの書き込み (共有) → SKU ごと → NE の CSV → 行
    await db.query(CUTOVER_SHARED_LOCK_SQL);
    await lockMasterWriteShared(db);
    const first = await readCatchupPlan(db, opts, { now });
    if (first.problems.length) throw Object.assign(new Error(first.problems.join(' / ')), { reason: 'not_fixable' });
    const ids = [...new Set(first.plan.items.flatMap((it) => [it.sku_id, ...it.components.map((c) => c.sku_id)]))].sort(byId);
    for (const id of ids) await db.query(SKU_LOCK_SQL, [id]);
    await db.query(CSV_LOCK_SQL);
    const setIds = first.plan.items.map((it) => it.sku_id);
    if (setIds.length) await db.query('select sku_id from core.skus where sku_id = any ($1::bigint[]) order by sku_id for update', [setIds]);
    r = await readCatchupPlan(db, opts, { now });   // 鍵の後に読み直す
    if (r.problems.length) throw Object.assign(new Error(r.problems.join(' / ')), { reason: 'not_fixable' });
    if (r.hash !== opts.expectHash) throw Object.assign(new Error(`計画のハッシュが dry-run と違う (今 ${r.hash}・渡された ${opts.expectHash}) = 対象が変わった。dry-run をもう一度見てから`), { reason: 'plan_mismatch' });
    if (!r.plan.items.length) {
      await db.query('rollback');
      log('直すセットは無い (何も書かない)');
      return { ok: true, outcome: 'nothing', plan: r.plan, hash: r.hash, request_id: requestId };
    }
    for (const [k, v] of [['core.actor_type', 'human'], ['core.actor_id', opts.actor], ['core.source_system', SOURCE_SYSTEM], ['core.request_id', requestId], ['core.reason', opts.reason], ['core.run_id', '']]) {
      await db.query('select set_config($1, $2, true)', [k, v]);
    }
    const ctx = { today: r.context.today, actor: opts.actor, actorType: 'human', notes: [] };
    const why = `構成の変更に合わせて計算し直し・後始末: ${opts.reason}`.slice(0, 300);
    for (const it of r.plan.items) {
      const done = await recomputeSetInTx(db, it.sku_id, ctx, why);
      const got = done.map((d) => ({ col: d.col, from: d.from, to: d.to, ...(d.status ? { status: d.status } : {}) }));
      if (stable(got) !== stable(it.changes)) {
        throw Object.assign(new Error(`${it.code}: 書いた答えが計画と違う (計画 ${JSON.stringify(it.changes)}・書いた ${JSON.stringify(got)})`), { reason: 'verify_failed' });
      }
    }
    // 書いた後: 今日を覆う原価の行は 1 行まで・計画のセットはもう直すものが無い
    const after = await readCatchupPlan(db, opts, { now });
    const left = after.plan.items.filter((it) => setIds.includes(it.sku_id));
    if (left.length) throw Object.assign(new Error(`書いた後も直すものが残る (${left.map((x) => x.code).join('・')})`), { reason: 'verify_failed' });
    const over = (await db.query(`select count(*)::int as n from (select sku_id from core.sku_costs where sku_id = any($1::bigint[]) and valid_from <= $2::date and coalesce(valid_to, 'infinity'::date) >= $2::date
        group by sku_id having count(*) > 1) t`, [setIds, r.context.today])).rows[0].n;
    if (over) throw Object.assign(new Error(`今日を覆う原価の行が 2 つ以上のセットが ${over} 件 (期間が重なる)`), { reason: 'verify_failed' });
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    log(`⛔ 巻き戻した (commit の前) = 何も変えていない: ${e.message}`);
    return { ok: false, outcome: 'rolled_back', request_id: requestId, error: { reason: e.reason || e.code || 'error', message: String(e.message || e) } };
  }
  try {
    await db.query('commit');
  } catch (e) {
    const o = await outcomeOf(reconnect, requestId, r.plan.items.map((it) => it.sku_id));
    const words = { written: '書けていた (commit は通っていた)', not_written: '書けていない (何も変わっていない)', unknown: `書けたか分からない (${o.why})` };
    log(`⚠️ commit の答えが分からない (${String(e.message || e).slice(0, 200)}) → 新しい接続で request_id ${requestId} を確かめた: ${words[o.state]}`);
    return { ok: o.state === 'written', outcome: o.state, request_id: requestId, plan: r.plan, hash: r.hash, error: o.state === 'written' ? null : { reason: `commit_${o.state}`, message: String(e.message || e) } };
  }
  log(`✅ commit: セット ${r.plan.items.length} 件の導く値を計算し直した (request_id ${requestId})`);
  return { ok: true, outcome: 'committed', plan: r.plan, hash: r.hash, request_id: requestId };
}

/** 本体 (試験は db を渡す) */
export async function runCatchup(db, opts, { now = new Date(), reconnect = null, log = console.log } = {}) {
  if (!opts.apply) {
    await db.query('begin read only');
    try {
      const r = await readCatchupPlan(db, opts, { now });
      log(`dry-run (読むだけ・何も書かない): セットの導く値を今の構成で計算し直す${opts.codes && opts.codes.length ? ` (${opts.codes.join('・')})` : ' (全部のセット)'}`);
      for (const l of planLines(r)) log(l);
      if (r.problems.length) { log('⛔ 直さない (上の理由)'); return { mode: 'dry-run', ...r, exitCode: 1 }; }
      log(`計画のハッシュ (--apply に --expect-hash で渡す・同じ日・同じ --actor / --reason / --code で): ${r.hash}`);
      return { mode: 'dry-run', ...r, exitCode: 0 };
    } finally { try { await db.query('rollback'); } catch { /* */ } }
  }
  const r = await applyCatchup(db, opts, { now, reconnect, log });
  return { mode: 'apply', ...r, exitCode: r.ok ? 0 : 1 };
}

async function main() {
  let opts;
  try { opts = parseCatchupArgs(process.argv.slice(2)); } catch (e) { console.error(`❌ ${e.message}`); process.exitCode = 2; return; }
  const url = opts.url || process.env.COMPANY_DB_URL;
  if (!url) { console.error('❌ COMPANY_DB_URL (DB の持ち主) か --url が要る'); process.exitCode = 2; return; }
  const clean = (e) => String(e && e.message).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]');
  const connect = async () => {
    const c = await openPgClient(url, { application_name: 'set-derived-catchup' });
    c.on('error', () => {});
    await c.query(`set statement_timeout = '60s'`);
    await c.query(`set lock_timeout = '10s'`);
    return c;
  };
  let client = null;
  try {
    client = await connect();
    const reconnect = async () => { const c = await connect(); await c.query('set default_transaction_read_only = on'); return { db: pgAdapter(c), close: () => c.end() }; };
    const r = await runCatchup(pgAdapter(client), opts, { reconnect });
    if (opts.json) fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, hash: r.hash ?? null, plan: r.plan ?? null, excluded: r.excluded ?? [], kept: r.kept ?? [], counts: r.counts ?? null,
      problems: r.problems ?? [], warnings: r.warnings ?? [], outcome: r.outcome ?? null, request_id: r.request_id ?? null, error: r.error ?? null }, null, 2));
    process.exitCode = r.exitCode;
  } catch (e) {
    console.error(`❌ ${clean(e)}`);
    process.exitCode = 1;
  } finally {
    if (client) { try { await client.end(); } catch { /* */ } }
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main();
