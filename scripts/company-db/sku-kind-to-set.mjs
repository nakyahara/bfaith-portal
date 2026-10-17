#!/usr/bin/env node
/**
 * sku-kind-to-set.mjs — Company DB で単品 (single) の SKU を、NE と同じセット (set) にする (人が 1 回ずつ流す道具・定期実行ではない)
 * (2026-10-17 中原さん「nametag30 / nametag50 は Company DB でもセットにする (NE に合わせる)」。
 *  10/15 にスタッフが NE で nametag30 = nametag10 × 3・nametag50 = nametag10 × 5 のセットを作った = NE はセット・Company DB は単品のまま
 *  → 朝の照合の区分のゲート kind_gate.raw_mismatch = 2 → 新商品の許可が出ない (10/16 の朝から))
 *
 * なぜ道具が要るか: 区分 (skus.sku_kind) の持ち主は company (10/7 に widen 済み) = 夜間ロードは区分を NE に合わせない (engine.mjs の 1.5 = sku_kind_held に残すだけ)・
 *   画面にも区分を変える道は無い。0058 の G18 (core.guard_sku_kind_locked) は区分の UPDATE を全部のロールで拒み、通すのは DB の持ち主が同じ取引で作る
 *   保守の印 (ops.begin_master_maintenance) だけ。G19 (core.check_sku_kind_shape・commit のとき) は印でも外れない = セットは product_id なし・構成はセットの親だけ。
 *   → migration は要らない (今ある印の道を使う)。
 *
 * 書くもの (apply・1 つの取引・全部か何もしないか):
 *   1. core.skus: sku_kind single → set・product_id → null (G19 の最終形。夜間ロードの区分の正規化 engine.mjs の 3. と同じ形)
 *   2. core.sku_components: NE の構成 (miniPC の warehouse.db の NE の完全な取得 = 照合 ② と同じ読み方) を入れる
 *      (source = 'ne'・sort_order = 0 = 夜間ロードが入れる行と同じ形 = 次の夜間ロードは「同じ」で触らない。持ち主 load の今は夜間ロードも同じ行を入れる)
 *   3. セットの導く値 (税率・取扱・原価) を構成品から計算し直す (画面の保存と同じ lib/master-write.mjs の planSetDerived。持ち主が company のときだけ。
 *      原価 = 今日から set_calc の行・前の行は昨日で閉じる (今日始まりの行は消す)・例外原価 (manual / override_zero) は変えない)
 *   変えないもの: 単品だった商品の行 (core.products・JAN・物理属性) は消さない・外すだけ (夜間ロードの区分の正規化と同じ)・
 *     仕入先 (supplier_skus)・名前・標準売価・登録の状態 (ops.master_registrations)・Amazon の対応 (セットも構成品にできる = amazon-map-write.mjs の USABLE_SKU_KINDS)
 *   記録: 変更の記録 (0026 の trigger = events.master_change_events) に 人 (--actor)・理由 (--reason)・request_id・source_system = sku_kind_to_set。
 *     保守の印 (ops.master_maintenance_marks) に理由と取引の番号
 *
 * 確かめ (dry-run と apply の両方。1 つでも当たれば何も書かない):
 *   - 切替の段階 new_open・区分の持ち主 (DB の active) が company
 *   - SKU がある・単品・登録の状態が available / ne_confirmed / distributable / quarantined (下書き・NE 登録待ち・やめた は断る)
 *   - 商品がまとまり (色違い) の子・親でない・まとまりの表に無い / ほかのセットの構成品でない / 構成の依頼・食い違いが無い /
 *     NE 登録の CSV・NE から消す CSV の途中でない / ロジザードのバーコードの決め方・依頼・CSV が無い (単品だけの表) / ほかの商品のケースの中身でない
 *   - NE: warehouse.db の NE の完全な取得 (36 時間以内) で、そのコードがセットの表にある (セット)・取込の整合で保持していない・
 *     構成品がちょうど読める (数量 1〜999 の整数・1〜20 品・自分自身でない・重ならない)
 *   - 構成品は Company DB の単品・やめた / NE から消す途中でない
 *   - 最新の夜間ロード (ops.master_load_commits の最後) の判断の記録 skus.kind_held に [コード, set, single] がある
 *     (= Render の材料 (NE) もセットと見ている = 直した後の夜間ロードも区分のずれを出さない)・構成の持ち主が load なら飛ばした構成品が NE の構成品と同じ
 *   - 計画のハッシュ = 人・理由・コード・今の値・NE の観測・書くもの全部 (apply は dry-run のハッシュ --expect-hash と同じときだけ)
 * 鍵 (apply): 段階 (共有) → マスタの書き込み (共有 = 夜間ロードと並ばない) → SKU ごと (sku_id の順・構成品も) → NE の CSV → 商品の行 → SKU の行 → 読み直す → ハッシュ
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下。NE の取得 = env DATA_DIR / WAREHOUSE_DATA_DIR か --data-dir。DB = env COMPANY_DB_URL (DB の持ち主) か --url):
 *   # 1. 読むだけ (既定・読み取りの取引)。今の姿・書くもの・照合の区分の生の差の見込み・計画のハッシュ。直さない理由があれば exit 1 (表は出す)
 *   node -r dotenv/config scripts/company-db/sku-kind-to-set.mjs --code nametag30 --code nametag50 --actor <人のメール> --reason '<理由>'
 *   # 2. 書く (dry-run の表を中原さんが見て OK の後だけ・同じ日 (JST)・同じ --code / --actor / --reason)
 *   node -r dotenv/config scripts/company-db/sku-kind-to-set.mjs --code nametag30 --code nametag50 --actor <人のメール> --reason '<理由>' --apply --expect-hash <64 桁> --yes
 * 終了コード: 0 = dry-run で直せる / apply で commit・1 = 直さない / 巻き戻した / 書けたか分からない・つながらない・2 = 引数が違う
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { normSku } from '../../lib/sku-norm.js';
import { comparability } from '../../apps/company-db/master-compare/compare-ne.mjs';
import { planSetDerived, costAsOfJoin, jstDate, SET_DERIVED_KEYS, OVERRIDE_SOURCES, MAX_COMPONENTS, MAX_QTY } from '../../lib/master-write.mjs';
import { CUTOVER_SHARED_LOCK_SQL, MASTER_WRITE_SHARED_LOCK_SQL } from '../../lib/master-cutover.mjs';
import { CSV_LOCK_SQL, SKU_LOCK_SQL } from '../../apps/master-decisions/ne-csv-lock.mjs';

/** NE の取得の新しさ (0070 の ops.ne_gone_fresh_hours・once-variation と同じ 36 時間) */
export const FRESH_HOURS = 36;
export const MAX_CODES = 10;
export const SOURCE_SYSTEM = 'sku_kind_to_set';
/** 直してよい登録の状態 (下書き・NE 登録待ち = 登録の途中・やめた = 断る) */
export const OK_REG_STATES = Object.freeze(['available', 'ne_confirmed', 'distributable', 'quarantined']);
/** NE 登録の CSV の品目で「途中」の状態 (lib/master-write.mjs の REG_LIVE_STATES と同じ) */
const REG_LIVE_STATES = ['built', 'issued', 'import_declared', 'partial'];
const PLAN_VERSION = 1;
const KNOWN_COST = new Set(['COMPLETE', 'OVERRIDDEN']);
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const fail = (message) => Object.assign(new Error(message), { code: 'KIND_ARGS' });
export const stable = (v) => (Array.isArray(v) ? `[${v.map(stable).join(',')}]`
  : v && typeof v === 'object' ? `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}` : JSON.stringify(v));
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
/** sync_meta の時刻 ('YYYY-MM-DD HH:MM:SS' = UTC) → ミリ秒 */
const utcTextMs = (t) => Date.parse(`${String(t ?? '').replace(' ', 'T')}Z`);

/** 引数を読む。誤り = KIND_ARGS */
export function parseKindArgs(argv) {
  const o = { codes: [], apply: false, yes: false, expectHash: null, actor: null, reason: null, json: null, url: null, dataDir: null };
  const need = (i, name) => { const v = argv[i + 1]; if (v == null || v.startsWith('--')) throw fail(`${name} の後に値が要る`); return v; };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--code') { o.codes.push(need(i, a).trim()); i++; }
    else if (a === '--apply') o.apply = true;
    else if (a === '--yes') o.yes = true;
    else if (a === '--expect-hash') { const v = need(i, a); if (!/^[0-9a-f]{64}$/.test(v)) throw fail('--expect-hash は dry-run が出した 64 桁の 16 進'); o.expectHash = v; i++; }
    else if (a === '--actor') { o.actor = need(i, a); i++; }
    else if (a === '--reason') { o.reason = need(i, a).trim(); i++; }
    else if (a === '--json') { o.json = need(i, a); i++; }
    else if (a === '--url') { o.url = need(i, a); i++; }
    else if (a === '--data-dir') { o.dataDir = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (!o.codes.length) throw fail('--code <商品コード> が要る (複数は --code を繰り返す)');
  if (o.codes.length > MAX_CODES) throw fail(`--code は ${MAX_CODES} 個まで`);
  for (const c of o.codes) if (!/^[A-Za-z0-9_-]{1,30}$/.test(c)) throw fail(`--code ${c} は英数字と - _ の 1〜30 字`);
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
 * miniPC の warehouse.db の NE の完全な取得を、照合 ② と同じ部品 (readNeSide・neIntegrity・nModelOf・neIntegrityRows) で読む (読むだけ)。
 * 戻り値 = { error } か { products_at, sets_at, integ, nm, collided, badRowNorms, integrityRows, fetchState, P, S, E }
 */
export async function readNeContext(dataDir) {
  const C = await import('../../apps/company-db/master-compare/compare-ne.mjs');
  const ne = C.readNeSide(dataDir);
  if (!ne || ne.error) return { error: `NE の取得を読めない (${ne ? ne.error : 'なし'})` };
  if (!ne.hasSrc) return { error: 'warehouse.db が古い (元の値 *_src の列が無い)' };
  const M = ne.meta || {};
  if (!M.ne_api_products_complete_at || !M.ne_api_setproducts_complete_at) return { error: 'NE の取得の完了の印が無い (取得の途中・失敗)' };
  const integ = C.neIntegrity(M);
  if (!integ) return { error: '取込の整合 (ne_api_*_integrity) を読めない' };
  const { m: nm, collided } = C.nModelOf(ne);
  const { badRowNorms, integrityRows, fetchState } = C.neIntegrityRows(ne, integ.intBlocked);
  // 区分のゲートの母集合 (compare-ne.mjs の kind_gate と同じ: P = 商品の表・S = セットの表の親・E = 例外の表)
  const P = new Set(ne.products.map((r) => normSku(r.code)).filter(Boolean));
  const S = new Set(ne.sets.map((r) => normSku(r.parent)).filter(Boolean));
  const E = new Set((ne.exceptions || []).map((c) => normSku(c)).filter(Boolean));
  return { products_at: M.ne_api_products_complete_at, sets_at: M.ne_api_setproducts_complete_at, integ, nm, collided, badRowNorms, integrityRows, fetchState, P, S, E };
}

/** 区分のゲートの生の差 (compare-ne.mjs の kind_gate.raw_mismatch と同じ数え方の見込み)。kinds = Map(code_norm → { code, sku_kind }) */
export function rawKindMismatch(ne, kinds) {
  const codes = [];
  for (const [norm, c] of kinds) {
    if (ne.badRowNorms.has(norm) || ne.integ.intBlocked.has(norm) || ne.collided.has(norm)) continue;
    const neKind = ne.S.has(norm) ? 'set' : ne.P.has(norm) ? 'single' : ne.E.has(norm) ? 'exception' : null;
    if (neKind && neKind !== c.sku_kind) codes.push(c.code);
  }
  return codes.sort();
}

/** NE のそのコードの観測 (セットの表・構成品・数量)。問題は problems に足す */
function neFactsOf(ne, code, problems) {
  const norm = normSku(code);
  const n = ne.nm.get(norm) || null;
  const f = { code_norm: norm, ne_code: n ? n.code : null, kind: n ? n.kind : null, in_set_table: ne.S.has(norm), name: null, price: null, children: [] };
  if (ne.collided.has(norm)) problems.push(`${code}: NE の取得でコードの書き方が衝突している (正規化で同じ別の書き方)`);
  if (ne.integ.intBlocked.has(norm)) problems.push(`${code}: NE の取得の取込の整合で保持された (${ne.integ.intBlocked.get(norm)})`);
  if (ne.badRowNorms.has(norm)) problems.push(`${code}: NE の取得の行が壊れている (空のコード・子が空のセットの行)`);
  if (!n || n.kind !== 'set' || !f.in_set_table) { problems.push(`${code}: NE の取得でセットでない (${n ? n.kind : 'NE に無い'}) = 直さない`); return f; }
  f.name = n.cols.name && comparability(n.cols.name) === 'comparable' ? n.cols.name.value : null;
  f.price = n.cols.standard_price_jpy && comparability(n.cols.standard_price_jpy) === 'comparable' ? n.cols.standard_price_jpy.value : null;
  for (const [cn, ch] of n.children) {
    const qty = comparability(ch.st) === 'comparable' ? ch.st.value : null;
    f.children.push({ code: ch.code, code_norm: cn, qty });
    if (!Number.isInteger(qty) || qty < 1 || qty > MAX_QTY) problems.push(`${code}: NE の構成品 ${ch.code} の数量が読めない (1〜${MAX_QTY} の整数でない)`);
    if (cn === norm) problems.push(`${code}: NE の構成品が自分自身`);
  }
  f.children.sort((a, b) => (a.code_norm < b.code_norm ? -1 : a.code_norm > b.code_norm ? 1 : 0));
  if (!f.children.length || f.children.length > MAX_COMPONENTS) problems.push(`${code}: NE の構成品が ${f.children.length} 品 (1〜${MAX_COMPONENTS} 品だけ)`);
  return f;
}

/**
 * 今の Company DB と NE の観測を読んで、書くもの (計画) と「直さない」理由を決める (読むだけ)。
 * 戻り値 { plan, context, ne, problems, warnings, estimate, hash }
 */
export async function readKindPlan(db, opts, { ne, now = new Date() } = {}) {
  const { codes, actor, reason } = opts;
  const problems = [];
  const warnings = [];
  const today = jstDate(now);
  // 段階・持ち主 (DB の active)
  const cut = (await db.query('select phase, owner_hash from ops.master_cutover_state where id = 1')).rows[0] || {};
  const own = (await db.query('select active_hash, active_map from ops.master_ownership_state where id = 1')).rows[0] || {};
  const ownMap = own.active_map || {};
  const ownerOf = (k) => ownMap[k] ?? 'load';
  const context = { today, phase: cut.phase ?? null, active_hash: own.active_hash ?? null, sku_kind_owner: ownerOf('skus.sku_kind'),
    components_owner: ownerOf('sku_components'), derived_owners: Object.fromEntries(SET_DERIVED_KEYS.map((k) => [k, ownerOf(k)])) };
  if (context.phase !== 'new_open') problems.push(`切替の段階が ${context.phase} (new_open でない)`);
  if (context.sku_kind_owner !== 'company') problems.push('区分 (skus.sku_kind) の持ち主が company でない = 夜間ロードが NE に合わせる (この道具は要らない)');
  const derivedOwned = SET_DERIVED_KEYS.every((k) => ownerOf(k) === 'company');
  if (!derivedOwned) warnings.push(`セットの導く値の持ち主に load がある (${SET_DERIVED_KEYS.filter((k) => ownerOf(k) !== 'company').join('・')}) = 計算し直さない (夜間ロードが NE に合わせる)`);

  // NE の取得 (照合 ② と同じ読み方)
  const neInfo = { products_at: null, sets_at: null, integrity_untrusted: null, components_untrusted: null };
  const facts = new Map();
  if (!ne || ne.error) problems.push(`NE の取得 (warehouse.db) を読めない: ${ne ? ne.error : 'なし'}`);
  else {
    Object.assign(neInfo, { products_at: ne.products_at, sets_at: ne.sets_at, integrity_untrusted: ne.integrityRows, components_untrusted: !!ne.integ.componentsUntrusted });
    for (const [k, t] of [['商品', ne.products_at], ['セット', ne.sets_at]]) {
      const ms = utcTextMs(t);
      if (!Number.isFinite(ms)) problems.push(`NE の取得の完了の時刻 (${k}) が読めない`);
      else if (now.getTime() - ms > FRESH_HOURS * 3600e3) problems.push(`NE の取得 (${k}) が ${FRESH_HOURS} 時間より古い (${t} UTC) = 新しい取得の後に`);
      else if (ms - now.getTime() > 5 * 60e3) problems.push(`NE の取得 (${k}) の時刻が今より後 (${t} UTC)`);
    }
    if (ne.integ.componentsUntrusted) problems.push('NE の取得でセットの構成の行が落ちた (C1 の形・dropped_missing_key > 0) = 構成を信用できない');
    const sp = ne.fetchState && ne.fetchState.setproducts;
    if (!sp || sp.ok !== true) problems.push(`NE のセットの取得の件数を確かめられない (${sp ? sp.reason : 'なし'})`);
    if (ne.integrityRows > 0) warnings.push(`NE の取得の integrity_untrusted が ${ne.integrityRows} (区分のゲートはこの数でも閉じたまま = 取り直しが要る)`);
    for (const c of codes) facts.set(normSku(c), neFactsOf(ne, c, problems));
  }

  // 最新の夜間ロードの判断の記録 (Render の材料も NE でセットと見ているか)
  const lastLoad = (await db.query(`select c.ingest_run_id, c.committed_at from ops.master_load_commits c order by c.commit_seq desc limit 1`)).rows[0] || null;
  const loadInfo = { ingest_run_id: lastLoad ? lastLoad.ingest_run_id : null, committed_at: lastLoad ? new Date(lastLoad.committed_at).toISOString() : null };
  let held = null; let compDec = null;
  if (!lastLoad) problems.push('夜間ロードの commit の記録 (ops.master_load_commits) が無い');
  else {
    const rows = (await db.query(`select section, payload from ops.load_decisions where ingest_run_id = $1 and section in ('skus', 'set_components')`, [lastLoad.ingest_run_id])).rows;
    const sk = rows.find((r) => r.section === 'skus');
    compDec = rows.find((r) => r.section === 'set_components')?.payload ?? null;
    if (!sk || !Array.isArray(sk.payload?.kind_held)) problems.push(`最新の夜間ロード (${lastLoad.ingest_run_id}) の判断の記録 skus.kind_held が無い`);
    else held = new Map(sk.payload.kind_held.filter((x) => Array.isArray(x)).map((x) => [normSku(x[0]), { ne_kind: x[1], cdb_kind: x[2] }]));
    // 構成の持ち主が load = 夜間ロードの構成の判断の記録が要る (形が違う・持ち主が合わない = 夜間ロードの材料と入れる構成を照らせない = 断る。#R1 Medium)
    if (context.components_owner === 'load') {
      if (!compDec || compDec.owned !== true || !Array.isArray(compDec.skipped)) {
        problems.push(`最新の夜間ロード (${lastLoad.ingest_run_id}) の構成の判断の記録 (set_components) が無い / 形が違う / 持ち主が load でない = 夜間ロードの材料の構成と照らせない`);
        compDec = null;
      }
    } else {
      warnings.push('構成の持ち主が company = 夜間ロードは構成を入れない・直さない (この道具の行がそのまま残る)。夜間ロードの材料の構成とは照らさない');
      compDec = null;
    }
  }

  // SKU ごと
  const items = [];
  for (const code of codes) {
    const norm = normSku(code);
    const s = (await db.query(`select s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.product_id::text as product_id, s.name, s.standard_price_jpy::text as price,
         s.tax_rate::text as tax_rate, s.tax_class, s.handling, s.handling_own, r.state as reg, r.origin,
         p.display_code, p.parent_product_id::text as parent_product_id, p.status as product_status
       from core.skus s left join ops.master_registrations r on r.sku_id = s.sku_id left join core.products p on p.product_id = s.product_id
       where s.company_id = 1 and s.code_norm = $1`, [norm])).rows;
    if (s.length !== 1) { problems.push(`${code}: Company DB の SKU が ${s.length} 件 (ちょうど 1 件だけ直す)`); continue; }
    const k = s[0];
    if (k.sku_kind !== 'single') { problems.push(`${code}: 区分が ${k.sku_kind} (単品でない = もう直してある / 直す相手でない)`); continue; }
    if (!k.product_id) problems.push(`${code}: 単品なのに商品 (product_id) が無い`);
    if (!OK_REG_STATES.includes(k.reg)) problems.push(`${code}: 登録の状態が ${k.reg ?? '行なし'} (${OK_REG_STATES.join(' / ')} だけ直す)`);
    if (k.parent_product_id) problems.push(`${code}: 商品がまとまり (色違い) の子 (親 ${k.parent_product_id}) = この道の外`);
    // 参照の数 (止める物と、知らせるだけの物)
    const ref = (await db.query(`select
        (select count(*)::int from core.products c where c.parent_product_id = $2::bigint) as kids,
        (select count(*)::int from core.sku_variation_choices v where v.sku_id = $1::bigint) as var_choice,
        (select count(*)::int from ops.variation_group_codes g where g.group_product_id = $2::bigint) +
        (select count(*)::int from core.variation_axes a where a.group_product_id = $2::bigint) +
        (select count(*)::int from ops.variation_group_revisions g where g.group_product_id = $2::bigint) +
        (select count(*)::int from ops.variation_parent_adoptions a where a.sku_id = $1::bigint or a.product_id = $2::bigint or a.group_product_id = $2::bigint) as var_group,
        (select count(*)::int from core.sku_components c where c.child_sku_id = $1::bigint) as in_sets,
        (select count(*)::int from core.sku_components c where c.parent_sku_id = $1::bigint) as own_components,
        (select count(*)::int from ops.sku_component_requests q where q.set_sku_id = $1::bigint and q.status = 'open') +
        (select count(*)::int from ops.sku_component_breaches b where b.set_sku_id = $1::bigint and b.status = 'open') as comp_open,
        (select count(*)::int from ops.ne_reg_export_items i where i.sku_id = $1::bigint and i.state = any ($3::text[])) as reg_csv_live,
        (select count(*)::int from ops.ne_del_items i where i.sku_id = $1::bigint and i.state = 'live') as ne_del_live,
        (select count(*)::int from ops.sku_logizard_barcode_rules x where x.sku_id = $1::bigint) +
        (select count(*)::int from ops.sku_logizard_barcode_requests x where x.sku_id = $1::bigint) +
        (select count(*)::int from ops.lz_new_export_items x where x.sku_id = $1::bigint) as logizard,
        (select count(*)::int from core.product_physicals x where x.case_content_sku_id = $1::bigint) as case_content,
        (select count(*)::int from core.supplier_skus x where x.sku_id = $1::bigint) as supplier_skus,
        (select count(*)::int from core.listing_components x where x.sku_id = $1::bigint) as listing_refs,
        (select count(*)::int from core.external_ids e where e.entity_type = 'product' and e.entity_id = $2::bigint and e.system = 'jan' and e.valid_to is null) as jans,
        (select count(*)::int from ops.order_settings_outbox o where o.sku_id = $1::bigint) as order_settings`,
    [k.sku_id, k.product_id, REG_LIVE_STATES])).rows[0];
    const stop = [['kids', 'まとまりの親 (子の商品がある)'], ['var_choice', 'まとまりの子の選択肢がある'], ['var_group', 'まとまりの表 (予約・軸・revision・親の採用) にある'],
      ['in_sets', 'ほかのセットの構成品 (セットはセットの構成品にできない)'], ['own_components', '単品なのに構成の行がある'], ['comp_open', '構成の依頼・食い違いが開いている'],
      ['reg_csv_live', 'NE 登録の CSV の途中'], ['ne_del_live', 'NE から消す CSV の途中'], ['logizard', 'ロジザードのバーコードの決め方・依頼・CSV がある (単品だけの表)'],
      ['case_content', 'ほかの商品のケースの中身 (product_physicals.case_content_sku_id)']];
    for (const [key, words] of stop) if (ref[key]) problems.push(`${code}: ${words} (${ref[key]} 件) = 直さない`);
    // 夜間ロードの判断の記録: NE (Render の材料) = セット・社内 = 単品
    const h = held ? held.get(norm) : null;
    if (held && !(h && h.ne_kind === 'set' && h.cdb_kind === 'single')) {
      problems.push(`${code}: 最新の夜間ロード (${loadInfo.ingest_run_id}) が「NE はセット・社内は単品」と記録していない (${h ? `${h.ne_kind} / ${h.cdb_kind}` : '記録なし'}) = 夜間ロードの材料がまだセットと見ていない`);
    }
    const f = facts.get(norm) || { children: [] };
    // 構成の持ち主が load なら、夜間ロードが飛ばした構成品 (kind_held) = NE の構成品 (2 つの NE の写しが同じ構成)
    let loadChildren = null;
    if (compDec) {
      loadChildren = compDec.skipped.filter((x) => Array.isArray(x) && normSku(x[0]) === norm && x[2] === 'kind_held').map((x) => normSku(x[1])).sort();
      const neKids = f.children.map((c) => c.code_norm);
      if (f.children.length && stable(loadChildren) !== stable(neKids)) {
        problems.push(`${code}: 夜間ロードの材料の構成品 (${loadChildren.join('・') || 'なし'}) と NE の取得の構成品 (${neKids.join('・')}) が違う`);
      }
    }
    // 構成品 (Company DB の単品・やめた / NE から消す途中でない)
    const kids = [];
    for (const c of f.children) {
      const r = (await db.query(`select k.sku_id::text as sku_id, k.code, k.sku_kind, k.tax_rate::text as tax_rate, k.handling, g.state as reg,
           (select count(*)::int from ops.ne_del_items i where i.sku_id = k.sku_id and i.state = 'live') as deleting, x.cost_jpy::text as cost_jpy, x.cost_status
         from core.skus k left join ops.master_registrations g on g.sku_id = k.sku_id ${costAsOfJoin('k.sku_id', '$2', 'x')}
         where k.company_id = 1 and k.code_norm = $1`, [c.code_norm, today])).rows;
      if (r.length !== 1) { problems.push(`${code}: 構成品 ${c.code} が Company DB に ${r.length} 件`); continue; }
      const x = r[0];
      if (x.sku_kind !== 'single') problems.push(`${code}: 構成品 ${x.code} が単品でない (${x.sku_kind})`);
      if (x.reg === 'cancelled' || x.reg == null) problems.push(`${code}: 構成品 ${x.code} の登録の状態が ${x.reg ?? '行なし'}`);
      if (x.deleting) problems.push(`${code}: 構成品 ${x.code} は NE から消す依頼の途中`);
      if (x.sku_id === k.sku_id) problems.push(`${code}: 構成品が自分自身`);
      kids.push({ sku_id: x.sku_id, code: x.code, qty: c.qty, tax_rate: x.tax_rate, handling: x.handling, cost_jpy: KNOWN_COST.has(x.cost_status) ? x.cost_jpy : null });
    }
    // セットの導く値 (画面の保存と同じ計画)。原価の今の行
    const open = (await db.query(`select sku_cost_id::text as id, cost_jpy::text as cost_jpy, cost_source, cost_status, valid_from::text as valid_from
       from core.sku_costs where sku_id = $1 and valid_to is null`, [k.sku_id])).rows;
    if (open.length > 1) problems.push(`${code}: 閉じていない原価の行が ${open.length} 行`);
    // 今日以降を覆う閉じた原価の行 (今の開いている行のほか) = 今日からの行を足すと期間が重なる。
    //   0051 の core.guard_sku_cost_overlap は DB の持ち主の書き込みを source_system が画面・観測のときしか見ない = ここで断る (#R1 High)
    const later = (await db.query(`select count(*)::int as n from core.sku_costs where sku_id = $1 and valid_to is not null and valid_to >= $2::date`, [k.sku_id, today])).rows[0].n;
    if (later) problems.push(`${code}: 今日 (${today}) 以降を覆う閉じた原価の行が ${later} 行ある = 今日からの原価を足すと期間が重なる (画面で原価を確かめてから)`);
    const openCost = open[0] ? { ...open[0], cost_jpy: Number(open[0].cost_jpy) } : null;
    let derived = { changes: [], notes: [], costPlan: null };
    if (derivedOwned && kids.length === f.children.length && kids.length) {
      try {
        derived = planSetDerived({ code: k.code, tax_rate: k.tax_rate, tax_class: k.tax_class, handling: k.handling, handling_own: k.handling_own, open_cost: openCost },
          kids.map((c) => ({ qty: c.qty, tax_rate: c.tax_rate, handling: c.handling, cost_jpy: c.cost_jpy })), { tax: true, handling: true, cost: true }, today);
      } catch (e) { problems.push(`${code}: セットの導く値を計算できない (${e.message})`); }
      if (derived.costPlan === 'clear') warnings.push(`${code}: 構成品の原価が足りない = 今日から原価を空にする (古い単品の原価を残さない・画面の保存と同じ)`);
    }
    if (ref.supplier_skus) warnings.push(`${code}: 仕入先の行 (supplier_skus) ${ref.supplier_skus} 行はそのまま (発注アプリ・Render の SQLite の発注の設定も別)`);
    if (ref.jans) warnings.push(`${code}: 単品だった商品 (product ${k.product_id}) の JAN ${ref.jans} 件はその商品に残る (商品の行は消さない・SKU から外すだけ)`);
    if (f.name != null && f.name !== k.name) warnings.push(`${code}: 名前が NE のセット名と違う (Company DB「${k.name}」・NE「${f.name}」) = 照合 ② の差 (判断の一覧) になる・区分のゲートには関わらない`);
    if (f.price != null && Number(k.price) !== Number(f.price)) warnings.push(`${code}: 標準売価が NE のセット販売価格と違う (Company DB ${k.price}・NE ${f.price}) = 照合 ② の差 (判断の一覧) になる・区分のゲートには関わらない`);
    items.push({
      sku_id: k.sku_id, code: k.code, code_norm: k.code_norm, reg: k.reg, origin: k.origin ?? null,
      before: { sku_kind: 'single', product_id: k.product_id, product_code: k.display_code ?? null, product_status: k.product_status ?? null,
        name: k.name, price: k.price, tax_rate: k.tax_rate, tax_class: k.tax_class, handling: k.handling, handling_own: k.handling_own ?? null, open_cost: openCost },
      refs: ref,
      ne: { ne_code: f.ne_code, name: f.name, price: f.price, children: f.children, load_children: loadChildren },
      components: kids.map((c) => ({ sku_id: c.sku_id, code: c.code, qty: c.qty })),
      derived: { changes: derived.changes, notes: derived.notes, cost_plan: derived.costPlan },
    });
  }

  // 照合の区分の生の差の見込み (今と、直した後)
  let estimate = null;
  if (ne && !ne.error) {
    const kinds = new Map((await db.query('select code_norm, code, sku_kind from core.skus where company_id = 1')).rows.map((r) => [r.code_norm, { code: r.code, sku_kind: r.sku_kind }]));
    const now0 = rawKindMismatch(ne, kinds);
    for (const it of items) kinds.set(it.code_norm, { code: it.code, sku_kind: 'set' });
    estimate = { now: now0, after: rawKindMismatch(ne, kinds), integrity_untrusted: ne.integrityRows };
  }
  const plan = { codes: items.map((x) => x.code), items };
  const hash = sha(stable({ v: PLAN_VERSION, actor, reason, context, ne: neInfo, load: loadInfo, plan, problems }));
  return { plan, context, ne: neInfo, load: loadInfo, problems, warnings, estimate, hash };
}

/** 人が読む表 */
export function planLines(r) {
  const out = [];
  if (r.context) out.push(`今日 (JST) ${r.context.today}・段階 ${r.context.phase}・区分の持ち主 ${r.context.sku_kind_owner}・構成の持ち主 ${r.context.components_owner}`);
  if (r.ne && r.ne.sets_at) out.push(`NE の取得: 商品 ${r.ne.products_at} UTC・セット ${r.ne.sets_at} UTC (integrity_untrusted ${r.ne.integrity_untrusted})`);
  if (r.load && r.load.ingest_run_id) out.push(`最新の夜間ロード: ${r.load.ingest_run_id} (${r.load.committed_at})`);
  for (const it of (r.plan ? r.plan.items : [])) {
    const b = it.before;
    out.push(`■ ${it.code} (SKU ${it.sku_id}・登録 ${it.reg}・${it.origin ?? ''}): 単品 → セット・product_id ${b.product_id} → なし (商品 ${b.product_code ?? ''} の行は消さない)`);
    out.push(`  構成 (NE ${it.ne.ne_code}・source ne): ${it.components.map((c) => `${c.code} × ${c.qty}`).join('・') || 'なし'}`);
    for (const c of it.derived.changes) {
      const v = (x) => (x && typeof x === 'object' ? `${x.rate ?? ''} ${x.class ?? ''}`.trim() : x ?? 'なし');
      out.push(`  導く値: ${c.col} ${v(c.from)} → ${v(c.to)}${c.col === 'cost' ? ` (今日 ${r.context.today} から set_calc・前の原価 ${b.open_cost ? `${b.open_cost.cost_source} ${b.open_cost.valid_from}〜` : 'なし'})` : ''}`);
    }
    if (!it.derived.changes.length) out.push('  導く値: 変わらない');
    for (const n of it.derived.notes) out.push(`  メモ: ${n}`);
    const rf = it.refs;
    out.push(`  そのまま: 仕入先の行 ${rf.supplier_skus}・Amazon の構成の参照 ${rf.listing_refs}・商品の JAN ${rf.jans}・発注の設定の知らせ ${rf.order_settings}`);
  }
  if (r.estimate) {
    out.push(`照合の区分の生の差 (raw_mismatch) の見込み: 今 ${r.estimate.now.length} (${r.estimate.now.join('・') || 'なし'}) → 直した後 ${r.estimate.after.length} (${r.estimate.after.join('・') || 'なし'})`);
  }
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
    // 元の取引は commit / rollback まで SKU の鍵を持つ = 同じ鍵を取れた = 元の取引は終わった (取れない = lock_timeout = 分からない)。
    //   その後に読む (read committed = 鍵の後の文は元の取引の commit を見る。#R1 Medium: 終わる前に読んで「書けていない」と言わない)
    await c.db.query('begin');
    let r;
    try {
      for (const id of [...skuIds].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0))) await c.db.query(SKU_LOCK_SQL, [id]);
      r = (await c.db.query(`select (select count(*)::int from events.master_change_events e where e.request_id = $1) as events,
          (select count(*)::int from core.skus k where k.sku_id = any ($2::bigint[]) and k.sku_kind = 'set') as sets`, [requestId, skuIds])).rows[0];
    } finally { try { await c.db.query('rollback'); } catch { /* */ } }
    if (r.events > 0 && r.sets === skuIds.length) return { state: 'written' };
    if (r.events === 0 && r.sets === 0) return { state: 'not_written' };
    return { state: 'unknown', why: `変更の記録 ${r.events} 件・セットになった SKU ${r.sets} / ${skuIds.length}` };
  } catch (e) {
    return { state: 'unknown', why: String((e && e.message) || e).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]').slice(0, 200) };
  } finally { if (c && c.close) { try { await c.close(); } catch { /* */ } } }
}

/** 書く (1 つの取引)。db = DB の持ち主の接続 */
export async function applyKind(db, opts, { ne, now = new Date(), reconnect = null, log = console.log } = {}) {
  const requestId = opts.requestId || crypto.randomUUID();
  let r;
  try {
    await db.query('begin');
    // 鍵の順 = 画面の保存・構成の昇格 (lib/master-write.mjs) と同じ: 段階 (共有) → マスタの書き込み (共有) → SKU ごと (sku_id の順) → NE の CSV → 行
    await db.query(CUTOVER_SHARED_LOCK_SQL);
    await db.query(MASTER_WRITE_SHARED_LOCK_SQL);
    const first = await readKindPlan(db, opts, { ne, now });
    if (first.problems.length) throw Object.assign(new Error(first.problems.join(' / ')), { reason: 'not_fixable' });
    const ids = [...new Set(first.plan.items.flatMap((it) => [it.sku_id, ...it.components.map((c) => c.sku_id)]))].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0));
    for (const id of ids) await db.query(SKU_LOCK_SQL, [id]);
    await db.query(CSV_LOCK_SQL);
    const pids = first.plan.items.map((it) => it.before.product_id).filter(Boolean);
    if (pids.length) await db.query('select product_id from core.products where product_id = any ($1::bigint[]) order by product_id for update', [pids]);
    await db.query('select sku_id from core.skus where sku_id = any ($1::bigint[]) order by sku_id for update', [ids]);
    r = await readKindPlan(db, opts, { ne, now });   // 鍵の後に読み直す
    if (r.problems.length) throw Object.assign(new Error(r.problems.join(' / ')), { reason: 'not_fixable' });
    if (r.hash !== opts.expectHash) throw Object.assign(new Error(`計画のハッシュが dry-run と違う (今 ${r.hash}・渡された ${opts.expectHash}) = 対象が変わった。dry-run をもう一度見てから`), { reason: 'plan_mismatch' });
    // 変更の記録 (0026 の trigger) の誰が・理由
    for (const [k, v] of [['core.actor_type', 'human'], ['core.actor_id', opts.actor], ['core.source_system', SOURCE_SYSTEM], ['core.request_id', requestId], ['core.reason', opts.reason], ['core.run_id', '']]) {
      await db.query('select set_config($1, $2, true)', [k, v]);
    }
    // 保守の印 (0058 G24 = 区分を変える唯一の道・DB の持ち主だけ・この取引だけ)
    await db.query('select ops.begin_master_maintenance($1)', [`sku-kind-to-set: ${r.plan.codes.join('・')} を NE と同じセットにする (${opts.actor}・${requestId}): ${opts.reason}`.slice(0, 500)]);
    for (const it of r.plan.items) {
      const u = await db.query(`update core.skus set sku_kind = 'set', product_id = null where sku_id = $1 and sku_kind = 'single' and product_id = $2`, [it.sku_id, it.before.product_id]);
      if ((u.rowCount ?? u.affectedRows) !== 1) throw Object.assign(new Error(`${it.code}: 区分を変えられなかった (${u.rowCount})`), { reason: 'verify_failed' });
      for (const c of it.components) {
        await db.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, sort_order, source, created_by_type, created_by_id)
           values (1, $1, $2, $3, 0, 'ne', 'human', $4)`, [it.sku_id, c.sku_id, c.qty, opts.actor]);
      }
      for (const c of it.derived.changes) {
        if (c.col === 'tax_rate') await db.query('update core.skus set tax_rate = $2, tax_class = $3 where sku_id = $1', [it.sku_id, c.to.rate, c.to.class]);
        if (c.col === 'handling') await db.query('update core.skus set handling = $2 where sku_id = $1', [it.sku_id, c.to]);
      }
      // 原価 (画面の保存の replaceCostToday と同じ: 今の行が昨日以前に始まった = 昨日で閉じる / 今日始まった = 消す / 明日以降 = planSetDerived が断る)
      const cp = it.derived.cost_plan;
      if (cp) {
        const open = it.before.open_cost;
        if (open && OVERRIDE_SOURCES.has(open.cost_source)) throw Object.assign(new Error(`${it.code}: 例外原価を計算で上書きしようとした`), { reason: 'verify_failed' });
        if (open) {
          if (open.valid_from === r.context.today) await db.query('delete from core.sku_costs where sku_cost_id = $1 and valid_to is null', [open.id]);
          else await db.query('update core.sku_costs set valid_to = $2::date - 1 where sku_cost_id = $1 and valid_to is null', [open.id, r.context.today]);
        }
        if (cp !== 'clear') {
          await db.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id)
             values (1, $1, $2, $3, $4, $5::date, $6, 'human', $7)`, [it.sku_id, cp.jpy, cp.source, cp.status, r.context.today, `構成品から計算 (単品 → セット: ${opts.reason})`.slice(0, 500), opts.actor]);
        }
      }
    }
    // commit の前に最終形 (G19 の deferred の trigger) をここで確かめる = 誤りは commit の前に出る
    await db.query('set constraints all immediate');
    for (const it of r.plan.items) {
      const v = (await db.query(`select s.sku_kind, s.product_id, (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(c.child_sku_id::text, c.qty) order by c.child_sku_id), '[]'::jsonb)
           from core.sku_components c where c.parent_sku_id = s.sku_id) as comps,
           (select count(*)::int from core.sku_costs x where x.sku_id = s.sku_id and x.valid_to is null) as open_costs,
           (select count(*)::int from core.sku_costs x where x.sku_id = s.sku_id and coalesce(x.valid_to, 'infinity'::date) >= $2::date) as from_today
         from core.skus s where s.sku_id = $1`, [it.sku_id, r.context.today])).rows[0];
      const want = [...it.components].sort((a, b) => (BigInt(a.sku_id) < BigInt(b.sku_id) ? -1 : 1)).map((c) => [c.sku_id, c.qty]);
      // 今日を覆う原価の行 = 原価を付け替えた (set_calc) なら 1 行・空にした (clear) なら 0 行・変えないなら前と同じ (期間を重ねない・#R1 High)
      const cp = it.derived.cost_plan;
      const wantToday = cp === 'clear' ? 0 : cp ? 1 : null;
      if (v.sku_kind !== 'set' || v.product_id != null || stable(v.comps) !== stable(want) || v.open_costs > 1 || (wantToday != null && v.from_today !== wantToday)) {
        throw Object.assign(new Error(`${it.code}: 書いた後の形が計画と違う (${v.sku_kind}・product ${v.product_id}・構成 ${JSON.stringify(v.comps)}・開いた原価の行 ${v.open_costs}・今日を覆う原価の行 ${v.from_today})`), { reason: 'verify_failed' });
      }
    }
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
  log(`✅ commit: ${r.plan.codes.join('・')} をセットにした (request_id ${requestId})`);
  return { ok: true, outcome: 'committed', plan: r.plan, hash: r.hash, request_id: requestId, estimate: r.estimate };
}

/** 本体 (試験は db と ne を渡す) */
export async function runKind(db, opts, { ne, now = new Date(), reconnect = null, log = console.log } = {}) {
  if (!opts.apply) {
    await db.query('begin read only');
    try {
      const r = await readKindPlan(db, opts, { ne, now });
      log(`dry-run (読むだけ・何も書かない): ${opts.codes.join('・')} を単品 → セット`);
      for (const l of planLines(r)) log(l);
      if (r.problems.length) { log('⛔ 直さない (上の理由)'); return { mode: 'dry-run', ...r, exitCode: 1 }; }
      log(`計画のハッシュ (--apply に --expect-hash で渡す・同じ日・同じ --code / --actor / --reason で): ${r.hash}`);
      return { mode: 'dry-run', ...r, exitCode: 0 };
    } finally { try { await db.query('rollback'); } catch { /* */ } }
  }
  const r = await applyKind(db, opts, { ne, now, reconnect, log });
  return { mode: 'apply', ...r, exitCode: r.ok ? 0 : 1 };
}

async function main() {
  let opts;
  try { opts = parseKindArgs(process.argv.slice(2)); } catch (e) { console.error(`❌ ${e.message}`); process.exitCode = 2; return; }
  const url = opts.url || process.env.COMPANY_DB_URL;
  if (!url) { console.error('❌ COMPANY_DB_URL (DB の持ち主) か --url が要る'); process.exitCode = 2; return; }
  const dataDir = (opts.dataDir || process.env.DATA_DIR || process.env.WAREHOUSE_DATA_DIR || '').trim();
  if (!dataDir) { console.error('❌ DATA_DIR / WAREHOUSE_DATA_DIR (warehouse.db の場所) か --data-dir が要る'); process.exitCode = 2; return; }
  const clean = (e) => String(e && e.message).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]');
  let ne;
  try { ne = await readNeContext(dataDir); } catch (e) { console.error(`❌ NE の取得を読めない: ${clean(e)}`); process.exitCode = 1; return; }
  const connect = async () => {
    const c = await openPgClient(url, { application_name: 'sku-kind-to-set' });
    c.on('error', () => {});
    await c.query(`set statement_timeout = '30s'`);
    await c.query(`set lock_timeout = '10s'`);
    return c;
  };
  let client = null;
  try {
    client = await connect();
    const reconnect = async () => { const c = await connect(); await c.query('set default_transaction_read_only = on'); return { db: pgAdapter(c), close: () => c.end() }; };
    const r = await runKind(pgAdapter(client), opts, { ne, reconnect });
    if (opts.json) fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, hash: r.hash ?? null, plan: r.plan ?? null, problems: r.problems ?? [], warnings: r.warnings ?? [],
      estimate: r.estimate ?? null, outcome: r.outcome ?? null, request_id: r.request_id ?? null, error: r.error ?? null }, null, 2));
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
