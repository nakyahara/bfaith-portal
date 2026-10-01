/**
 * fetch.mjs — Company DB の写しを取る (マスタ正本切替 ④a。設計 = AI_reference CompanyDB構想/15 §1〜§4・10 §7。daily-sync の「Company DB の写し」)
 *
 * 何をするか: 持ち主が C (Company DB) の列の値を、watcher ロールで読むだけの 1 つの取引 (REPEATABLE READ READ ONLY) で読み、
 *   確かめてから warehouse.db の世代の表 (cdb_publish_generations / cdb_publish_values) に 1 取引で入れ、今の世代の印 (sync_meta cdb_publish_current) を前へ進める。
 *   すぐ後の m_products 再構築 (rebuild-m-products.js) が今の世代を読み、持ち主が C の列だけ C の値を重ねる (master-publish.js)。
 * 受け入れる条件 (全部そろったときだけ。15 §3・Codex ④ 設計 R0):
 *   時刻が今の世代より新しい / 変更の記録 (0026 の events.master_change_events = 足すだけの表) の最大の番号が下がっていない (下がった = 復元を疑う) /
 *   値の範囲 / 正規化したコードに重なりが無い / 持ち主の設定が 3 か所 (Company DB に記録した epoch (0053。active、切替の日は prepared)・最新の夜間ロードが記録した products と set_components の持ち主) で同じ /
 *   🚨 config/master-ownership.mjs (configured) は世代の持ち主に使わない (Codex #1564 R1 H1。書き換えてデプロイしただけでは何も変わらない。証跡に出すだけ)
 *   値の行数 (同じ持ち主 = 同じ epoch のときだけ)・SKU の数が前の世代の 90% 以上 / 単品の商品名・状態が SKU の名前・取扱区分と同じ (その列の持ち主が C のとき) /
 *   入れた後に読み直したハッシュが同じ
 *   → 1 つでも欠ける = rejected (理由つきの行だけ残す)。印は動かない = 作り直しは前の世代のまま (持ち主が C の列があれば作り直しは止まる)
 * 🚨 今は持ち主が全部 load = 値 0 行の世代 (verified)。毎日しくみが通ることの確かめ (m_products は変わらない)
 * 🚨 Company DB には何も書かない (watcher = select だけ)。証跡に「全部の列を C にしたら何件変わるか」(影運転の材料。10 §8.1)
 * 入れた後の確かめ (--verify-apply。m_products 再構築の後の工程): 今朝の写し・今朝の作り直しの記録・今の世代・読み直した m_products と上書き表が
 *   全部そろったときだけ ok の ping (Codex R0 #4 = 取っただけでは ok にしない)
 *
 * 使い方 (miniPC):
 *   node apps/company-db/publish/fetch.mjs --daily                  daily-sync の回 (取れない・受け入れない = fail の ping)
 *   node apps/company-db/publish/fetch.mjs --verify-apply --daily   daily-sync の回 (m_products 再構築の後。確かめられたときだけ ok の ping)
 *   node apps/company-db/publish/fetch.mjs --dry-run                読んで確かめるだけ (世代も証跡も書かない・ping なし)
 * env: COMPANY_DB_WATCH_URL (watcher = 読むだけ) / DATA_DIR (warehouse.db・証跡) / JOBS_MONITOR_TOKEN・JOBS_MONITOR_URL (ping)
 * 終わり方: 取れた = ✅ exit 0 (ping なし) / 受け入れない・取れない = ❌ exit 1 + fail の ping / 未設定 = ⏭️ exit 0 + fail の ping /
 *   確かめ (--verify-apply) = ✅ exit 0 + ok の ping (台帳 cdb-master-publish) / ❌ exit 1 + fail の ping。
 *   retry には載せない (m_products の作り直しも retry しない = 落ちた朝は前の世代で作り直す・締切で気づく)
 */
import 'dotenv/config';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { normSku } from '../../../lib/sku-norm.js';
import { jstDateStr } from '../../../lib/jst-date.js';
import { MASTER_OWNERSHIP } from '../../../config/master-ownership.mjs';
import { writeEvidence, readEvidence } from '../push/evidence.mjs';
import { selectNightlyLoad, COMPANY_ID } from '../master-compare/compare-load.mjs';
import {
  publishCols, ownershipSorted, ownershipHash, publishContentHash, validPublishValue, currentGenerationNo, makeGenerationId, readCurrentPublish, verifyApplied,
  checkPublishOwnership, completeness, entriesOfRows, PRESENCE_COL,
  costFromCdb, handlingFromCdb, supplierFromCdb, PUBLISH_0027_COLUMNS, PUBLISH_CURRENT_KEY, PUBLISH_KEEP_GENERATIONS, SKU_KINDS, readPublishGeneration,
} from '../../warehouse/master-publish.js';
import { latestBuild, publishOfBuild } from '../../warehouse/master-material.js';
import { readOwnershipState, latestLoadCommit, ALL_LOAD } from '../load/ownership-state.mjs';
import { readGateRow, writePublishGate, validSafeRow, markGateUnknown } from '../../warehouse/publish-gate.js';

export const EVIDENCE_NAME = 'master-publish';
export const JOB_ID = 'cdb-master-publish';   // 台帳 (config/jobs-registry.mjs)
/** 4 = 入れた後の確かめで m_products・上書き表が世代と違う (daily-sync はその後の m_products・上書き表を読む工程を全部止める = apps/warehouse/publish-gate.js。2・3 は使わない = 朝の再試行が別の意味に読む) */
export const EXIT = Object.freeze({ ok: 0, error: 1, applied_broken: 4 });
/** 前の世代に比べてこれより少ない = 読み落としを疑う (行数は持ち主が同じときだけ比べる) */
export const SHRINK_MIN_RATIO = 0.9;

const rowsOf = async (db, sql, params) => (await db.query(sql, params)).rows;
const columnExists = async (db, schema, table, column) => (await rowsOf(db, 'select 1 from information_schema.columns where table_schema = $1 and table_name = $2 and column_name = $3', [schema, table, column])).length > 0;

/**
 * Company DB から写す値を読む (compare-load.mjs の readCdbMaster と同じ作り。売上分類・推奨保有月数・代表の仕入先のコードも読む)。
 * 🚨 呼ぶ側が REPEATABLE READ READ ONLY の取引を張る。最初の文の時刻 = snapshot の時点 (マイクロ秒まで = 世代の新しさを比べる)
 */
export async function readPublishSource(db, { prevWatermark = null } = {}) {
  const cdbReadAt = (await rowsOf(db, `select to_char(clock_timestamp() at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as t`))[0].t;
  const has0027 = await columnExists(db, 'core', 'skus', 'standard_price_jpy');
  const hasVersion = await columnExists(db, 'core', 'skus', 'version');
  const skus = await rowsOf(db, `select s.code, s.code_norm, s.sku_kind, s.name, s.tax_rate::float8 as tax_rate, s.tax_class, s.handling,
      case when s.sku_kind = 'single' then p.sales_class::int end as sales_class,
      case when s.sku_kind = 'single' then p.name end as product_name, case when s.sku_kind = 'single' then p.status end as product_status,
      case when s.sku_kind = 'single' then s.product_id::text end as product_id${has0027 ? `,
      s.standard_price_jpy::float8 as standard_price_jpy, s.shipping_code, s.shipping_method, s.shipping_cost_jpy::float8 as shipping_cost_jpy, s.reorder_months::float8 as reorder_months` : ''}
    from core.skus s left join core.products p on p.product_id = s.product_id where s.company_id = $1 order by s.code_norm`, [COMPANY_ID]);
  const costs = new Map((await rowsOf(db, `select s.code_norm, c.cost_jpy::float8 as cost_jpy, c.cost_source, c.cost_status from core.sku_costs c join core.skus s on s.sku_id = c.sku_id
    where c.valid_to is null and c.company_id = $1`, [COMPANY_ID])).map((r) => [r.code_norm, r]));
  const primary = new Map();   // code_norm → [仕入先コード (Company DB の形)]
  if (has0027) {
    for (const r of await rowsOf(db, `select s.code_norm, sup.code from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id join core.suppliers sup on sup.supplier_id = x.supplier_id
      where x.is_primary and x.company_id = $1 order by sup.code`, [COMPANY_ID])) { if (!primary.has(r.code_norm)) primary.set(r.code_norm, []); primary.get(r.code_norm).push(r.code); }
  }
  // 世代の水位 = 変更の記録 (0026 の events.master_change_events。足すだけ = 消せない表) の最大の番号。下がった = Company DB を古い状態に戻した (復元) を疑う。
  //   行の version の最大値は使わない (正しく行を消しても下がる。Codex R0 #9)
  //   水位の出来事の変わらない列の指紋も持ち、次の回に「前の水位の出来事がまだ同じ中身である」を確かめる (消えた・違う = 復元・歴史の分かれ = 受け入れない。Codex R1)
  const hasEvents = (await rowsOf(db, `select to_regclass('events.master_change_events') is not null as ok`))[0].ok === true;
  const eventCols = `event_id::text as event_id, change_id::text as change_id, entity_type, entity_id::text as entity_id, operation,
    to_char(recorded_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as recorded_at`;
  const top = hasEvents ? (await rowsOf(db, `select ${eventCols} from events.master_change_events order by event_id desc limit 1`))[0] ?? null : null;
  const watermark = top ? top.event_id : null;
  const prevEvent = hasEvents && prevWatermark != null ? (await rowsOf(db, `select ${eventCols} from events.master_change_events where event_id = $1`, [String(prevWatermark)]))[0] ?? null : null;
  // 写しが使う夜間ロード = 最後に commit したロード (0053 の commit の番号。毎晩の cron か --use-prepared の明示のロードかを問わない) と、その記録した持ち主 (0029 の ops.load_materials.ownership)
  const load = await selectPublishLoad(db);
  let loadOwnership = null;
  if (load && await columnExists(db, 'ops', 'load_materials', 'ownership')) {
    loadOwnership = Object.fromEntries((await rowsOf(db, `select entity, ownership, ownership_hash from ops.load_materials where ingest_run_id = $1 and entity in ('products', 'set_components')`,
      [load.ingest_run_id])).map((r) => [r.entity, { ownership: r.ownership, ownership_hash: r.ownership_hash }]));
  }
  // 持ち主の epoch (0053。Codex #1564 R1 H1)。壊れていれば投げる (推測で持ち主を決めない)
  const ownershipState = await readOwnershipState(db);
  return { cdbReadAt, has0027, hasVersion, hasEvents, ownershipState, skus, costs, primary, watermark: watermark == null ? null : Number(watermark),
    watermarkFingerprint: eventFingerprint(top), prevEventFingerprint: prevWatermark == null ? undefined : eventFingerprint(prevEvent), load, loadOwnership };
}

/**
 * 写しが使う夜間ロード = 最後に commit したロード (#1564 Codex R4 High・Medium 2)。
 *   0053 の ops.master_load_commits の番号 (DB が commit の直前に振る = commit の順) が一番大きい回。場所 (host) では選ばない:
 *   切替の日に HTTP (remote-load.mjs load --apply --use-prepared → router の startLoad = host 'render') で流した prepared のロードも、
 *   毎晩の cron (host 'render-nightly') も、後に commit した方が写しの世代になる (照合 ① の selectNightlyLoad は毎晩の cron だけ = 別の目的)。
 *   番号の行がまだ無い (0053 の前・0053 の後に一度も本適用のロードが無い) = 今までどおり毎晩の cron の最新 (commit_seq = null)
 * @returns {{ ingest_run_id, started_at, finished_at, host, commit_seq: number|null, epoch?: string }|null}
 */
export async function selectPublishLoad(db) {
  const last = await latestLoadCommit(db);
  if (last) {
    const r = (await rowsOf(db, 'select started_at::text as started_at, finished_at::text as finished_at, host from ops.ingest_runs where ingest_run_id = $1', [last.ingest_run_id]))[0] || {};
    return { ingest_run_id: last.ingest_run_id, started_at: r.started_at ?? null, finished_at: r.finished_at ?? null, host: last.host ?? r.host ?? null, commit_seq: last.commit_seq, epoch: last.epoch };
  }
  const legacy = await selectNightlyLoad(db);
  return legacy ? { ...legacy, commit_seq: null } : null;
}

/**
 * この写しの世代の持ち主 (epoch)。config (configured) ではなく Company DB の記録 (0053) で決める (Codex #1564 R1 H1):
 *   prepared があり、最新の夜間ロードが prepared の持ち主で動いた (切替の日に明示して頼んだロード) = prepared の世代 (作り直しと確かめが通れば人が activate)
 *   それ以外 = active の世代 (記録が無い = 全部 load)
 */
export function generationEpoch(source) {
  const st = source.ownershipState || { state: 'no_table', active: { map: ALL_LOAD, hash: null }, prepared: null };
  // 夜間ロードが記録した持ち主表から今の式でハッシュを作る (記録したハッシュの式が前の版でも比べられる = 式を 1 つにした日 #1564 Codex R3 Medium)
  const loadMap = source.loadOwnership?.products?.ownership ?? null;
  const loadHash = loadMap && typeof loadMap === 'object' ? ownershipHash(loadMap) : null;
  if (st.prepared && loadHash === st.prepared.hash) return { kind: 'prepared', map: st.prepared.map, hash: st.prepared.hash };
  return { kind: st.state === 'ok' ? 'active' : 'default', map: st.active.map, hash: st.active.hash };
}

/** 変更の記録の 1 行の変わらない列の指紋 (無い = null) */
export const eventFingerprint = (e) => (e ? crypto.createHash('sha256').update(JSON.stringify([e.event_id, e.change_id, e.entity_type, e.entity_id, e.operation, e.recorded_at])).digest('hex') : null);

/** 1 つの SKU の 1 つの列の値 (Company DB の形のまま。逆向きの変換は作り直しの時に master-publish.js で)。undefined = その列の値を持たない */
export function publishValueOf(s, col, source) {
  switch (col) {
    case 'name': return s.name;
    case 'cost': { const c = source.costs.get(s.code_norm); return c ? { jpy: c.cost_jpy, source: c.cost_source, status: c.cost_status } : null; }
    case 'standard_price': return s.standard_price_jpy ?? null;
    case 'tax_rate': return s.tax_rate ?? null;
    case 'tax_class': return s.tax_class ?? null;
    case 'sales_class': return s.sku_kind === 'single' ? (s.sales_class ?? null) : undefined;   // セット・例外は Company DB に置く場所が無い = 構成品から導く
    case 'shipping': return { code: s.shipping_code ?? null, method: s.shipping_method ?? null, cost_jpy: s.shipping_cost_jpy ?? null };
    case 'reorder_months': return s.reorder_months ?? null;
    case 'handling': return s.handling;
    case 'primary_supplier': { const p = source.primary.get(s.code_norm) || []; return p.length === 1 ? p[0] : p.length === 0 ? null : { multiple: p }; }
    default: return undefined;
  }
}

/**
 * 世代を作る (持ち主が C の列だけ。値は JSON の文字列)。problems = 作れない理由 (受け入れない)
 * @returns {{ generation_id, cdb_read_at, version_watermark, load_run_id, ownership, ownership_hash, cols, rows, row_count, sku_count, content_hash, problems }}
 */
export function buildGeneration({ source, ownership, now = new Date() }) {
  const cols = publishCols(ownership);
  const problems = [];
  const need0027 = cols.filter((c) => PUBLISH_0027_COLUMNS.includes(c));
  if (!source.has0027 && need0027.length) problems.push('no_0027');
  if (checkPublishOwnership(ownership).length) problems.push('ownership_not_supported');
  const rows = [];
  if (!problems.length) {
    for (const s of source.skus) {
      // C にある SKU の印 (持ち主が C の列があるときだけ): 「C にある SKU の欄が欠けた」(止める) と「C に無い SKU」(NE の値で作る) を分ける
      if (cols.length) rows.push({ code_norm: s.code_norm, col: PRESENCE_COL, code: s.code, sku_kind: s.sku_kind, value: 'true' });
      for (const col of cols) {
        const v = publishValueOf(s, col, source);
        if (v === undefined) continue;
        rows.push({ code_norm: s.code_norm, col, code: s.code, sku_kind: s.sku_kind, value: JSON.stringify(v) });
      }
    }
  }
  return {
    generation_id: makeGenerationId(now), cdb_read_at: source.cdbReadAt, version_watermark: source.watermark, watermark_fingerprint: source.watermarkFingerprint ?? null,
    load_run_id: source.load?.ingest_run_id ?? null, load_commit_seq: source.load?.commit_seq ?? null, ownership_problems: checkPublishOwnership(ownership),
    ownership: ownershipSorted(ownership), ownership_hash: ownershipHash(ownership), cols, rows, row_count: rows.length, sku_count: source.skus.length,
    content_hash: publishContentHash(rows), problems,
  };
}

/**
 * 受け入れる条件を確かめる (15 §3)。戻り値 = { problems: 理由コードの配列 (空 = 受け入れる), detail }
 * @param {object|null} p.prev  今の世代 (verified) の行
 * @param {Map<string, string>|null} [p.current]  今の m_products のコード → 商品区分 (完全さ = このコード × 要る列の欄が全部あるか。Codex R1 H1)
 */
export function verifyGeneration({ prev, next, source, current = null }) {
  const problems = [...(next.problems || [])];
  const detail = {};
  // 持ち主が全部 load (値 0 行の世代) = 写す値が無い = 値の前提 (単品の商品・変更の記録・夜間ロードの持ち主) が欠けても受け入れる。
  //   止めずに detail.ignored_all_load に残す (毎朝 fail の ping を出して本物の失敗を埋もれさせない。#1564 の見直し L-5)
  const allLoad = !(next.cols || []).length;
  const soft = (p) => { if (allLoad) { (detail.ignored_all_load ||= []).push(p); } else problems.push(p); };
  if (next.ownership_problems && next.ownership_problems.length) detail.ownership_problems = next.ownership_problems;
  // 持ち主の設定 (3 か所: 手元・最新の夜間ロードの products・set_components)。夜間ロードが記録した持ち主と違う = C の列が夜に上書きされうる
  if (!source.load) problems.push('no_nightly_load');
  else if (!source.loadOwnership || !source.loadOwnership.products || !source.loadOwnership.set_components) soft('no_load_ownership');
  else {
    const want = JSON.stringify(next.ownership);
    const got = Object.fromEntries(['products', 'set_components'].map((e) => [e, source.loadOwnership[e]]));
    // ハッシュは記録した持ち主表から今の式で作って比べる (記録したハッシュの式が前の版でも偽の食い違いにしない。#1564 Codex R3 Medium)
    if (Object.values(got).some((lo) => JSON.stringify(ownershipSorted(lo.ownership)) !== want || ownershipHash(lo.ownership) !== next.ownership_hash)) {
      problems.push('ownership_mismatch');
      detail.ownership = { local: next.ownership_hash, products: got.products.ownership_hash, set_components: got.set_components.ownership_hash, load_run_id: source.load.ingest_run_id };
    }
  }
  if (!Number.isSafeInteger(next.version_watermark)) soft('no_version');
  // 単品の商品 (core.products) の名前・状態が SKU と同じか (Codex R0 #6)。m_products の商品名は skus.name・取扱区分は skus.handling から。
  //   商品側と食い違ったまま写すと、商品の名前・状態を読むところと古い表が別の値になる。数は毎回残し、止めるのは持ち主が C の列だけ
  //   handling = discontinued ↔ status = discontinued / active・unknown → active。単品に商品が無い・1 つの商品に単品が 2 つ以上 = いつも受け入れない (DB の制約で起きないはずの形)
  const singles = (source.skus || []).filter((s) => s.sku_kind === 'single');
  const nameDiff = singles.filter((s) => s.product_name !== s.name).length;
  const statusDiff = singles.filter((s) => s.product_status !== (s.handling === 'discontinued' ? 'discontinued' : 'active')).length;
  const noProduct = singles.filter((s) => s.product_id == null).length;
  const perProduct = new Map(); for (const s of singles) if (s.product_id != null) perProduct.set(s.product_id, (perProduct.get(s.product_id) || 0) + 1);
  const shared = [...perProduct.values()].filter((n) => n > 1).length;
  detail.products = { singles: singles.length, name_mismatch: nameDiff, status_mismatch: statusDiff, without_product: noProduct, shared_products: shared };
  if ((next.cols || []).includes('name') && nameDiff) problems.push('product_name_mismatch');
  if ((next.cols || []).includes('handling') && statusDiff) problems.push('product_status_mismatch');
  if (noProduct) soft('single_without_product');
  if (shared) soft('product_shared');
  // 完全さ (Codex R1 H1): 今の m_products のコードのうち C にある SKU × 要る列の欄が全部あるか・同じ C の SKU に当たるコードが 2 つ以上 = 使わない。
  //   C に無い SKU (NE にしか無い) = 止めない (写さない = NE の値のまま)・証跡の not_in_cdb と ⚠️ で知らせる
  if (current && (next.cols || []).length && !(next.problems || []).length) {
    let entries = null;
    try { entries = entriesOfRows(next.rows); } catch { /* 読めない値 = 下の値の範囲で落ちる */ }
    if (entries) {
      const c = completeness(current, entries, next.cols);
      if (c.missing.length) { problems.push('incomplete'); detail.incomplete = { count: c.missing.length, samples: c.missing.slice(0, 5) }; }
      // NE にしか無い SKU = 止めない (写さない = NE の値のまま)。知らせるだけ
      detail.not_in_cdb = { count: c.notInCdb, codes: c.notInCdbCodes };
      // NE と C で種類が違う SKU = その SKU だけ写さない (NE の値のまま)。知らせるだけ (#1564 の見直し M-5)
      detail.kind_mismatch = { count: c.kindMismatch, codes: c.kindMismatchCodes };
      if (c.collided.length) { problems.push('target_norm_collision'); detail.target_norm_collision = { count: c.collided.length, samples: c.collided.slice(0, 5) }; }
    }
  }
  if (prev) {
    if (!(String(next.cdb_read_at) > String(prev.cdb_read_at))) { problems.push('not_newer'); detail.not_newer = { prev: prev.cdb_read_at, next: next.cdb_read_at }; }
    if (Number.isSafeInteger(next.version_watermark) && prev.version_watermark != null && next.version_watermark < prev.version_watermark) {
      problems.push('watermark_backward'); detail.watermark = { prev: prev.version_watermark, next: next.version_watermark };
    }
    // 前の水位の出来事が今も同じ中身か (消えた・違う = Company DB を戻した・歴史が分かれた)。前の世代に指紋が無い (この版の前) = 見ない
    if (prev.watermark_fingerprint && source.prevEventFingerprint !== undefined && source.prevEventFingerprint !== prev.watermark_fingerprint) {
      problems.push('watermark_fork'); detail.watermark_fork = { prev_event: prev.version_watermark, found: source.prevEventFingerprint ? 'different' : 'missing' };
    }
    const shrunkSkus = prev.sku_count > 0 && next.sku_count < prev.sku_count * SHRINK_MIN_RATIO;
    const shrunkRows = prev.ownership_hash === next.ownership_hash && prev.row_count > 0 && next.row_count < prev.row_count * SHRINK_MIN_RATIO;
    if (shrunkSkus || shrunkRows) { problems.push('shrunk'); detail.shrunk = { prev_skus: prev.sku_count, skus: next.sku_count, prev_rows: prev.row_count, rows: next.row_count }; }
  }
  // 値の範囲・コード (写す行だけ。作り直しが使うもの)
  const bad = []; let badN = 0, mismatch = 0, collision = 0;
  const codeOf = new Map(); const seen = new Set();
  for (const r of next.rows) {
    let v; try { v = JSON.parse(r.value); } catch { v = undefined; }
    if (!SKU_KINDS.includes(r.sku_kind) || !validPublishValue(r.col, v)) { badN++; if (bad.length < 5) bad.push([r.code, r.col, String(r.value).slice(0, 80)]); }
    if (normSku(r.code) !== r.code_norm) mismatch++;
    const c = codeOf.get(r.code_norm);
    if (c !== undefined && c !== r.code) collision++;
    codeOf.set(r.code_norm, r.code);
    const k = `${r.code_norm}|${r.col}`;
    if (seen.has(k)) collision++;
    seen.add(k);
  }
  if (badN) { problems.push('value_out_of_range'); detail.value_out_of_range = { count: badN, samples: bad }; }
  if (mismatch) { problems.push('norm_mismatch'); detail.norm_mismatch = mismatch; }
  if (collision) { problems.push('norm_collision'); detail.norm_collision = collision; }
  return { problems: [...new Set(problems)], detail };
}

/** 今の世代 (verified) の行。無ければ null */
export function currentGeneration(sqlite) {
  const no = currentGenerationNo(sqlite);
  if (no == null) return null;
  const g = sqlite.prepare('SELECT * FROM cdb_publish_generations WHERE generation_no = ?').get(no);
  return g && g.state === 'verified' ? g : null;
}

/** 古い世代を消す (新しい順に keep 個残す。今の世代は消さない) */
export function pruneGenerations(sqlite, keep = PUBLISH_KEEP_GENERATIONS) {
  const cur = currentGenerationNo(sqlite);
  const old = sqlite.prepare('SELECT generation_no FROM cdb_publish_generations ORDER BY generation_no DESC LIMIT -1 OFFSET ?').all(keep)
    .map((r) => r.generation_no).filter((n) => n !== cur);
  const delV = sqlite.prepare('DELETE FROM cdb_publish_values WHERE generation_no = ?');
  const delG = sqlite.prepare('DELETE FROM cdb_publish_generations WHERE generation_no = ?');
  for (const n of old) { delV.run(n); delG.run(n); }
  return old.length;
}

/**
 * 世代を warehouse.db に 1 取引で入れる。verified = 値も入れ、読み直してハッシュを確かめ、今の世代の印を前へ進める (前にしか進まない)。
 * rejected = 理由つきの行だけ (値は入れない・印は動かない)。途中で落ちたら全部巻き戻る (印も前のまま)
 * @param {(no: number) => void} [p.beforeCommit]  試験だけ (印を進める前に落とす)
 */
export function stageGeneration(sqlite, gen, { state = 'verified', reason = null, now = new Date(), beforeCommit = null } = {}) {
  return sqlite.transaction(() => {
    const info = sqlite.prepare(`INSERT INTO cdb_publish_generations (generation_id, cdb_read_at, version_watermark, watermark_fingerprint, load_run_id, load_commit_seq, ownership, ownership_hash,
        row_count, sku_count, content_hash, state, reason, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(gen.generation_id, gen.cdb_read_at, gen.version_watermark ?? null, gen.watermark_fingerprint ?? null, gen.load_run_id ?? null, gen.load_commit_seq ?? null, JSON.stringify(gen.ownership), gen.ownership_hash,
        gen.row_count, gen.sku_count, gen.content_hash, state, reason, now.toISOString());
    const no = Number(info.lastInsertRowid);
    let moved = false;
    if (state === 'verified') {
      const ins = sqlite.prepare('INSERT INTO cdb_publish_values (generation_no, code_norm, col, code, sku_kind, value) VALUES (?,?,?,?,?,?)');
      for (const r of gen.rows) ins.run(no, r.code_norm, r.col, r.code, r.sku_kind, r.value);
      // 入れた後に読み直して確かめる (入れた中身 = 作った中身)
      const back = sqlite.prepare('SELECT code_norm, col, code, sku_kind, value FROM cdb_publish_values WHERE generation_no = ?').all(no);
      if (back.length !== gen.row_count || publishContentHash(back) !== gen.content_hash) throw Object.assign(new Error('入れた後に読み直した中身が作った中身と違う'), { code: 'REREAD_MISMATCH' });
      if (beforeCommit) beforeCommit(no);
      const cur = currentGenerationNo(sqlite);
      if (cur == null || no > cur) {
        sqlite.prepare('INSERT INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at')
          .run(PUBLISH_CURRENT_KEY, String(no), now.toISOString());
        moved = true;
      }
    }
    const pruned = pruneGenerations(sqlite);
    return { generation_no: no, moved, pruned };
  })();
}

/**
 * 影運転の材料 (10 §8.1): 全部の列を C にしたら、きのうの m_products (今朝の作り直しの前) の値と違う数 (列ごと)。
 * セットの導く列 (原価・税率・売上分類・取扱区分) も C のセットの値とそのまま比べる = 近似。m_products に無い C の SKU = not_in_ne (足さない)
 */
export function shadowCounts(sqlite, source) {
  const mp = sqlite.prepare('SELECT 商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価ソース, 原価状態, 送料, 送料コード, 配送方法, 消費税率, 税区分, 仕入先コード, 売上分類 FROM m_products').all();
  const reorder = new Map();
  try { for (const r of sqlite.prepare('SELECT sku, 推奨保有月数 FROM m_reorder_setting').all()) if (r.sku) reorder.set(String(r.sku).toLowerCase(), r.推奨保有月数); } catch { /* 表が無い */ }
  const byNorm = new Map(source.skus.map((s) => [s.code_norm, s]));
  const by = { name: 0, cost: 0, standard_price: 0, tax_rate: 0, tax_class: 0, sales_class: 0, shipping: 0, reorder_months: 0, handling: 0, primary_supplier: 0 };
  const num = (a, b) => (a == null && b == null) || (a != null && b != null && Math.abs(Number(a) - Number(b)) < 1e-9);
  const matched = new Set();
  let compared = 0, notInCdb = 0;
  for (const m of mp) {
    const k = normSku(m.商品コード); const s = byNorm.get(k);
    if (!s) { notInCdb++; continue; }
    compared++; matched.add(k);
    if (s.name !== m.商品名) by.name++;
    const c = costFromCdb(publishValueOf(s, 'cost', source));
    if (!num(c.genka, m.原価) || c.genkaSource !== m.原価ソース || c.genkaStatus !== m.原価状態) by.cost++;
    if (source.has0027 && !num(s.standard_price_jpy ?? null, m.標準売価)) by.standard_price++;
    if (!num(s.tax_rate ?? null, m.消費税率)) by.tax_rate++;
    if ((s.tax_class ?? 'UNKNOWN') !== (m.税区分 ?? 'UNKNOWN')) by.tax_class++;
    if (s.sku_kind === 'single' && !num(s.sales_class ?? null, m.売上分類)) by.sales_class++;
    if (source.has0027 && (!num(s.shipping_cost_jpy ?? null, m.送料) || (s.shipping_code ?? null) !== (m.送料コード ?? null) || (s.shipping_method ?? null) !== (m.配送方法 ?? null))) by.shipping++;
    // 空 (null) も値: C が空で古い表に行がある = 全部 C にしたら行を消す = 変わる (Codex #1564 R1 L7)
    if (source.has0027 && !num(s.reorder_months ?? null, reorder.get(String(m.商品コード).toLowerCase()) ?? null)) by.reorder_months++;
    if (handlingFromCdb(s.handling, m.取扱区分) !== (m.取扱区分 ?? null)) by.handling++;
    const p = publishValueOf(s, 'primary_supplier', source);
    if (source.has0027 && (typeof p === 'object' && p !== null ? true : supplierFromCdb(p, m.仕入先コード) !== (m.仕入先コード ?? null))) by.primary_supplier++;
  }
  let notInNe = 0;
  for (const s of source.skus) if (!matched.has(s.code_norm)) notInNe++;
  return { note: '全部の列を C にしたら、きのうの m_products と違う数 (セットの導く列は近似)', compared, not_in_ne: notInNe, not_in_cdb: notInCdb, by_col: by };
}

/**
 * 1 回分 (証跡 → 読む → 作る → 確かめる → 入れる → 証跡)。db = { query } (pg の client でも PGlite でも)
 * @param {object} p
 * @param {{ query: Function }} [p.db]  もう開いた接続 (試験)
 * @param {() => Promise<{ db, close? }>} [p.connect]  接続を開く (本番 = watcher)
 * @param {import('better-sqlite3').Database} p.sqlite  warehouse.db
 * @returns {{ state: 'verified'|'rejected', generation_no, problems, evidence, line }}
 */
export async function runPublish({ db = null, connect = null, sqlite, dataDir, ownership: configured = MASTER_OWNERSHIP, now = new Date(), dryRun = false,
  write = (d, n, p) => writeEvidence(d, n, p, { now }), beforeCommit = null }) {
  const startedAt = now.toISOString();
  const save = (payload) => {
    if (dryRun) return;
    if (!write(dataDir, EVIDENCE_NAME, { as_of: jstDateStr(now), started_at: startedAt, ...payload })) throw new Error('証跡 master-publish を書けない');
  };
  // 始めに「実行中」= 同じ日の前の回の完了を無効にする (途中で落ちても前の回の verified が今朝の結果に見えない)
  save({ state: 'running' });
  try {
    const prev = currentGeneration(sqlite);
    let close = null, source;
    try {
      if (!db) {
        if (!connect) throw new Error('接続が無い (db か connect が要る)');
        const c = await connect(); db = c.db; close = c.close || null;
      }
      await db.query('begin transaction isolation level repeatable read read only');
      try { source = await readPublishSource(db, { prevWatermark: prev ? prev.version_watermark : null }); } finally { try { await db.query('rollback'); } catch { /* */ } }
    } finally { if (close) { try { await close(); } catch { /* */ } } }
    // 世代の持ち主 = Company DB の epoch (config は configured = 証跡に出すだけ)
    const epoch = generationEpoch(source);
    const gen = buildGeneration({ source, ownership: epoch.map, now });
    const current = new Map(sqlite.prepare('SELECT 商品コード, 商品区分 FROM m_products').all().map((r) => [r.商品コード, r.商品区分]));
    const { problems, detail } = verifyGeneration({ prev, next: gen, source, current });
    let shadow;
    try { shadow = shadowCounts(sqlite, source); } catch (e) { shadow = { error: String(e && e.message).slice(0, 160) }; }
    const base = {
      generation_id: gen.generation_id, cdb_read_at: gen.cdb_read_at, version_watermark: gen.version_watermark, load_run_id: gen.load_run_id, load_commit_seq: gen.load_commit_seq,
      company_owned: gen.cols, ownership_hash: gen.ownership_hash, row_count: gen.row_count, sku_count: gen.sku_count, content_hash: gen.content_hash,
      prev_generation_no: prev ? prev.generation_no : null, shadow,
      not_in_cdb: detail.not_in_cdb ?? { count: 0, codes: [] },
      kind_mismatch: detail.kind_mismatch ?? { count: 0, codes: [] },
      // 持ち主の epoch (Codex R1 H1): 手元の設定 / この世代 (用意した) / 今使っている (最後に通った作り直しが使った世代の持ち主)
      epochs: { configured: ownershipHash(configured), generation: { kind: epoch.kind, hash: gen.ownership_hash },
        active: source.ownershipState?.active?.hash ?? null, prepared: source.ownershipState?.prepared?.hash ?? null, state: source.ownershipState?.state ?? null,
        last_build: publishOfBuild(latestBuild(sqlite))?.ownership_hash ?? null },
    };
    const colsText = gen.cols.length ? `持ち主が C の列 ${gen.cols.join('・')}` : '持ち主が C の列なし = m_products は変わらない';
    if (dryRun) {
      return { state: problems.length ? 'rejected' : 'verified', dry_run: true, generation_no: null, problems, detail, evidence: base,
        line: `${problems.length ? '❌' : '✅'} Company DB の写し (試し・書かない): ${problems.length ? `受け入れない (${problems.join('・')})` : '受け入れる'} / 値 ${gen.row_count} 行・SKU ${gen.sku_count}・${colsText}` };
    }
    if (problems.length) {
      const st = stageGeneration(sqlite, gen, { state: 'rejected', reason: problems.join(','), now });
      const evidence = { state: 'rejected', reason: problems[0], problems, detail, generation_no: st.generation_no, ...base };
      save(evidence);
      return { state: 'rejected', generation_no: st.generation_no, problems, evidence,
        line: `❌ Company DB の写し: 受け入れない (${problems.join('・')}) = 今の世代 ${prev ? prev.generation_no : 'なし'} のまま (作り直しは前の世代)` };
    }
    let st;
    try { st = stageGeneration(sqlite, gen, { now, beforeCommit }); }
    catch (e) {
      // 途中で落ちた = 全部巻き戻った (印も前のまま)。理由つきの行だけ残す (残せなくても投げる)
      try { stageGeneration(sqlite, gen, { state: 'rejected', reason: `stage_failed:${(e && e.code) || 'error'}`, now }); } catch { /* */ }
      throw e;
    }
    const evidence = { state: 'complete', verdict: 'verified', generation_no: st.generation_no, moved: st.moved, pruned: st.pruned, ...base };
    save(evidence);
    const sh = shadow && !shadow.error ? `・全部 C にしたら違う列の数 ${Object.values(shadow.by_col).reduce((a, b) => a + b, 0)}・C にしか無い ${shadow.not_in_ne}` : '';
    const nic = base.not_in_cdb, kmc = base.kind_mismatch;
    return { state: 'verified', generation_no: st.generation_no, problems: [], evidence,
      line: `${nic.count || kmc.count ? '⚠️' : '✅'} Company DB の写し: 世代 ${st.generation_no} (値 ${gen.row_count} 行・SKU ${gen.sku_count}・${colsText}${sh})`
        + (nic.count ? ` / Company DB に無い SKU ${nic.count} 件は NE の値のまま (${nic.codes.join(', ')})` : '')
        + (kmc.count ? ` / NE と Company DB で種類が違う SKU ${kmc.count} 件は NE の値のまま (${kmc.codes.join(', ')})` : '') };
  } catch (e) {
    try { save({ state: 'failed', error: String(e && e.message).slice(0, 300) }); } catch { /* 書けなくても元の失敗を投げる */ }
    throw e;
  }
}

/**
 * 入れた後の確かめ (m_products 再構築の後の工程。Codex ④ 設計 R0 #4)。そろったときだけ verified (ok の ping):
 * 🚨 古い表は「最新の作り直しが使った世代」(bp.generation_no) と比べる (今の印の世代ではない。#1564 の見直し H-A)。
 *   作り直しを飛ばした朝 (NE の失敗)・作り直しが止まった朝 (品質チェック・CDB_PUBLISH_*) は、写しだけ新しい世代に進み m_products は前の世代のまま =
 *   遅れ (build_not_this_run・build_generation_not_today・generation_moved) = ❌ exit 1 だけ (翌朝の作り直しで届く = 15 §9 の答え 4)。
 *   exit 4 (broken = 後の工程を止める) は「作り直しが使った世代と m_products・上書き表が違う」(作り直しの後に書き換えられた) ときだけ
 *   今朝の写しが受け入れられた (証跡 master-publish が complete・同じ daily-sync の回) / 今朝の作り直しの記録がある (同じ回) /
 *   作り直しが使った世代 = 今朝の写しの世代 = 今の世代 (持ち主が同じ) / m_products・m_reorder_setting・そろえた上書き表を読み直して世代の値と同じ (verifyApplied) /
 *   読み直したハッシュ = 作り直しが取引の中で記録したハッシュ (作り直しの後に誰かが書いていない)
 * 結果は証跡 master-publish の apply に足す (今朝の写しの結果は残す)
 * @returns {{ state: 'verified'|'failed', problems: string[], evidence, line }}
 */
export async function runVerifyApply({ sqlite, dataDir, ownership: configured = MASTER_OWNERSHIP, now = new Date(), syncRunId = process.env.DAILY_SYNC_RUN_ID || null,
  write = (d, n, p) => writeEvidence(d, n, p, { now }), read = readEvidence, taxRates = null }) {
  const rates = taxRates || (await import('../../warehouse/rebuild-m-products.js')).TAX_RATES;
  const evKey = syncRunId ? EVIDENCE_NAME : `${EVIDENCE_NAME}.manual`;   // writeEvidence は実行 ID の無い回を .manual に書く
  const fetchEv = read(dataDir, jstDateStr(now))[evKey] ?? null;
  const problems = [];
  if (!fetchEv || fetchEv.state !== 'complete' || fetchEv.verdict !== 'verified') problems.push('fetch_not_verified');
  else if (syncRunId && fetchEv.sync_run_id !== syncRunId) problems.push('fetch_not_this_run');
  const build = latestBuild(sqlite);
  const bp = publishOfBuild(build);
  if (!build) problems.push('no_build');
  else if (syncRunId && build.daily_sync_run_id !== syncRunId) problems.push('build_not_this_run');   // NE が失敗した朝 = 作り直しを飛ばした
  if (build && !bp) problems.push('build_without_generation');
  if (bp && fetchEv && fetchEv.generation_no != null && bp.generation_no !== fetchEv.generation_no) problems.push('build_generation_not_today');
  const current = readCurrentPublish(sqlite, { values: false });
  if (current.problem) problems.push(`publication_${current.problem}`);
  else if (bp && current.generation.generation_no !== bp.generation_no) problems.push('generation_moved');   // 作り直しの後に写しが印を進めた (遅れ = 翌朝)
  // 作り直しが使った世代で確かめる (その世代の持ち主 = epoch。config = configured は使わない。Codex #1564 R1 H1)。作り直しの記録が無い = 今の世代 (確かめるものが無い = no_build)
  const publication = bp ? readPublishGeneration(sqlite, bp.generation_no) : readCurrentPublish(sqlite);
  if (bp && publication.problem) problems.push(`build_generation_${publication.problem}`);
  let ownership = ALL_LOAD;
  if (publication.generation) { try { ownership = JSON.parse(publication.generation.ownership); } catch { problems.push('generation_ownership_unreadable'); } }
  const applied = verifyApplied(sqlite, { publication, ownership, taxRates: rates });
  const appliedChecked = !!(bp && !publication.problem);   // 作り直しが使った世代が読めたときだけ「違う」と言える
  if (appliedChecked && !applied.ok) problems.push('applied_mismatch');
  if (appliedChecked && bp.applied_hash !== applied.applied_hash) problems.push('applied_hash_changed');   // 作り直しの後に m_products・上書き表が書き換えられた
  if (bp && publication.generation && bp.ownership_hash !== publication.generation.ownership_hash) problems.push('build_epoch_mismatch');   // 作り直しの記録と世代の持ち主が食い違う
  const state = problems.length ? 'failed' : 'verified';
  // 古い表の値が「作り直しが使った世代」と違う (C の値が入っていない・後から書き換えられた) = 後の工程に流さない (Codex R1 H2・#1564 R1 H4 = publish-gate.js)。
  //   遅れ (作り直しを飛ばした・止まった = 前の世代のまま) は broken にしない (#1564 の見直し H-A)
  const broken = appliedChecked && publishCols(ownership).length > 0 && (problems.includes('applied_mismatch') || problems.includes('applied_hash_changed'));
  const epochKind = fetchEv?.epochs?.generation?.kind ?? null;
  // 門 (warehouse.db の cdb_publish_gate = 後の工程を止めるかどうかの正。#1564 Codex R2 High 2) を証跡より先に書く:
  //   違う = broken (門が読めなくても書く) / 通った = safe (broken を戻せるのはこれだけ。確かめた作り直し・世代・入れた値のハッシュ・持ち主と一緒に =
  //   読み手が今と比べる。持ち主が全部 load でも書く = 作り直しを飛ばした朝 (前の世代のまま) も、確かめた作り直しのままなら流せる。#1564 Codex R4 Medium 1) /
  //   遅れ・確かめられない = 前の値のまま (行が無く持ち主が C = unknown)
  //   門が読めない (表はあるが SELECT が落ちる) = 「行が無い」と同じにしない (#1564 Codex R3 High 2): 通った = safe を書き直す・それ以外 = unknown を書く
  //   通らなかった (遅れ・確かめられない) = 確かめた safe の行が今も合えばそのまま (遅れの朝は流す)・broken はそのまま・それ以外 = unknown を書く
  //   (行が無く全部 load の暗黙の safe で、自動再試行・手の更新を流さない。#1564 Codex R5 Medium)
  let gateBefore = null, gateReadError = null;
  try { gateBefore = readGateRow(sqlite); } catch (e) { gateReadError = String(e && e.message).slice(0, 200); }
  const gateNext = broken ? 'broken' : state === 'verified' ? 'safe'
    : gateReadError ? 'unknown' : gateBefore && gateBefore.state === 'broken' ? null : validSafeRow(sqlite) ? null : 'unknown';
  let gateError = null;
  if (gateNext) {
    try {
      writePublishGate(sqlite, { state: gateNext, reason: gateNext === 'safe' ? 'verified' : problems.join('・') || (gateReadError ? 'gate_unreadable' : null), buildId: build ? build.build_id : null,
        generationNo: bp ? bp.generation_no : null, appliedHash: gateNext === 'safe' ? applied.applied_hash : null, ownershipHash: bp ? bp.ownership_hash : null, checkedAt: now.toISOString(), now });
    } catch (e) { gateError = String(e && e.message).slice(0, 200); }
  }
  if (gateError) problems.push('gate_write_failed');
  const stateOut = state === 'verified' && gateError ? 'failed' : state;
  const apply = { state: stateOut, broken, problems, checked_at: now.toISOString(),
    gate: { before: gateBefore ? gateBefore.state : gateReadError ? 'unreadable' : null, after: gateError ? (gateBefore ? gateBefore.state : gateReadError ? 'unreadable' : null) : (gateNext ?? (gateBefore ? gateBefore.state : null)),
      error: gateError, read_error: gateReadError }, epoch: epochKind, configured: ownershipHash(configured), build_id: build ? build.build_id : null, generation_no: bp ? bp.generation_no : null,
    current_generation_no: current.generation ? current.generation.generation_no : null, lag: !broken && problems.some((p) => LAG_PROBLEMS.includes(p)),
    generation_id: bp ? bp.generation_id : null, applied_hash: applied.applied_hash, build_applied_hash: bp ? bp.applied_hash : null, counts: applied.counts, samples: applied.problems.slice(0, 10),
    // NE にしか無い SKU (Company DB に無い = 写していない = NE の値のまま)。止めない・ping も fail にしない (朝の要約に出す)
    not_in_cdb: { count: applied.counts.not_in_cdb, codes: applied.not_in_cdb_codes },
    // 種類が違う SKU (写さない)・NE にしか無い構成品を持つ C のセット (値が混ざる) = 止めない・知らせる (#1564 の見直し M-5・L-2)
    kind_mismatch: { count: applied.counts.kind_mismatch, codes: applied.kind_mismatch_codes },
    mixed_sets: { count: applied.counts.mixed_sets, codes: applied.mixed_set_codes } };
  // 今朝の写しの証跡に足す (name・date・書いた時刻・実行 ID は書き手が付け直す)
  const { name: _n, date: _d, written_at: _w, sync_run_id: _s, ...kept } = fetchEv || { state: 'missing' };
  const evidence = { ...kept, apply };
  // 証跡が書けなくても投げない (違うと分かった回は exit 4 のまま = 門は先に書いた)
  let evidenceError = null;
  try { if (!write(dataDir, EVIDENCE_NAME, evidence)) evidenceError = '証跡 master-publish を書けない'; } catch (e) { evidenceError = String(e && e.message).slice(0, 200); }
  if (evidenceError) problems.push('evidence_write_failed');
  const stateFinal = stateOut === 'verified' && evidenceError ? 'failed' : stateOut;
  const cols = publishCols(ownership);
  const nic = applied.counts.not_in_cdb;
  const kmc = applied.counts.kind_mismatch, mix = applied.counts.mixed_sets;
  const line = stateFinal === 'verified'
    ? `${nic || kmc || mix ? '⚠️' : '✅'} Company DB の写しの反映: 世代 ${bp.generation_no} を m_products に入れた (${cols.length ? `列 ${cols.join('・')}・SKU ${applied.counts.keys}・比べた値 ${applied.counts.checked}` : '持ち主が C の列なし = 値 0 行'})`
      + (nic ? ` / Company DB に無い SKU ${nic} 件は NE の値のまま (${applied.not_in_cdb_codes.join(', ')})` : '')
      + (kmc ? ` / NE と Company DB で種類が違う SKU ${kmc} 件は NE の値のまま (${applied.kind_mismatch_codes.join(', ')})` : '')
      + (mix ? ` / NE にしか無い構成品を持つセット ${mix} 件は C と NE の値が混ざる (${applied.mixed_set_codes.join(', ')})` : '')
      + (epochKind === 'prepared' ? ' / prepared の持ち主の世代が入った = master-ownership-epoch.mjs activate で active にできる' : '')
    : broken ? `❌ Company DB の写しの反映: 古い表が作り直しの世代 ${bp.generation_no} と違う = 後の工程を止める (${problems.join('・')})`
      : `❌ Company DB の写しの反映: 確かめられない (${problems.join('・')})${apply.lag ? ` = 遅れ (m_products は世代 ${bp ? bp.generation_no : '?'} のまま・今の世代 ${apply.current_generation_no ?? '?'} は翌朝の作り直しで入る。後の工程は止めない)` : ''}`;
  return { state: stateFinal, broken, problems, evidence, line: evidenceError || gateError ? `${line} / 書けない: ${[gateError && `門 ${gateError}`, evidenceError && `証跡 ${evidenceError}`].filter(Boolean).join('・')}` : line };
}

/** 遅れ (作り直しを飛ばした・止まった・写しだけ先へ進んだ) = ❌ exit 1 だけ (後の工程は止めない・翌朝の作り直しで届く) */
export const LAG_PROBLEMS = Object.freeze(['build_not_this_run', 'build_generation_not_today', 'generation_moved', 'fetch_not_verified', 'fetch_not_this_run']);

/** 終わり方 → 監視への報告。ok は入れた後の確かめが通った回だけ (取っただけでは ok にしない)。受け入れない・取れない・未設定・確かめで落ちた = fail */
export function pingFor(state, line, { mode = 'fetch' } = {}) {
  return { status: mode === 'verify' && state === 'verified' ? 'ok' : 'fail', note: String(line || '').replace(/\s+/g, ' ').slice(0, 180) };
}

export function parseArgs(argv) {
  const out = { dataDir: null, daily: false, dryRun: false, verifyApply: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--daily' || a === '7') out.daily = true;   // daily-sync の印 (ping を打つ。'7' = 引数が無いときに daily-sync が足す)
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--verify-apply') out.verifyApply = true;
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.dryRun && out.verifyApply) throw new Error('--dry-run と --verify-apply は一緒に使わない');
  return out;
}

/**
 * 入口 (daily-sync・手の試し)。戻り値 = { code, last }。ping は --daily で --dry-run が無い回だけ
 *   取る回 = 失敗したときだけ fail の ping (取れた = ping なし) / --verify-apply の回 = 確かめられたときだけ ok、ほかは fail
 * @param {object} [deps]  試験で差し替える (env・時刻・接続・warehouse.db・ping・持ち主)
 */
export async function cli(argv, { env = process.env, now = new Date(), run = runPublish, verify = runVerifyApply, connectFor = null, openSqlite = null, ping = null, log = console.log,
  ownership = MASTER_OWNERSHIP, write = undefined } = {}) {
  let code = EXIT.error, last = '', doPing = false, state = null, mode = 'fetch';
  try {
    const a = parseArgs(argv);
    mode = a.verifyApply ? 'verify' : 'fetch';
    doPing = a.daily && !a.dryRun;
    // 一緒に切り替える組・④a が写さない列の確かめ = 世代の持ち主 (epoch) で buildGeneration が行う (ownership_not_supported)。
    //   config (configured) は prepare (master-ownership-epoch.mjs) が確かめる。ここで config を見て止めない (config だけでは何も変わらないため。Codex #1564 R1 H1)
    const dataDir = (a.dataDir || env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const w = write || ((d, n, p) => writeEvidence(d, n, p, { now }));
    const open = async () => (openSqlite ? openSqlite(dataDir) : (async () => {
      if (a.dataDir) process.env.DATA_DIR = dataDir;   // db.js は import の時に DATA_DIR を読む (この後で初めて import する)
      const { initDB } = await import('../../warehouse/db.js');
      return initDB();
    })());
    const url = (env.COMPANY_DB_WATCH_URL || '').trim();
    if (a.verifyApply) {
      const sqlite = await open();
      let r;
      try { r = await verify({ sqlite, dataDir, ownership, now, syncRunId: env.DAILY_SYNC_RUN_ID || null, write: w }); }
      catch (e) {
        // 決める前に落ちた = 門を unknown に (確かめた safe の行が今も合う・broken ならそのまま)。自動再試行・手の更新も止まる (#1564 Codex R5 Medium)
        const m = markGateUnknown(sqlite, { reason: `verify_apply_crashed: ${String(e && e.message).slice(0, 200)}`, now });
        throw Object.assign(e, { message: `${e && e.message}${m.wrote ? ' (門 = unknown)' : m.kept ? ` (門 = ${m.kept} のまま)` : m.error ? ` (門を書けない: ${m.error})` : ''}` });
      }
      last = r.line; state = r.state;
      code = r.broken ? EXIT.applied_broken : r.state === 'verified' ? EXIT.ok : EXIT.error;   // 違うと分かった回は何があっても exit 4
    } else if (!url) {
      // 未設定でも前の回の結果は無効にする (同じ日の古い complete を今朝の結果に見せない)
      if (!a.dryRun) w(dataDir, EVIDENCE_NAME, { as_of: jstDateStr(now), state: 'skipped', reason: 'not_configured' });
      last = '⏭️ Company DB の写し: 取らない (未設定 COMPANY_DB_WATCH_URL) = 作り直しは前の世代'; code = EXIT.ok; state = 'skipped';
    } else {
      const sqlite = await open();
      const connect = connectFor ? connectFor(url) : async () => (await import('../master-compare/run.mjs')).connectWatcher(url);
      const r = await run({ connect, sqlite, dataDir, ownership, now, dryRun: a.dryRun, ...(write ? { write } : {}) });
      last = r.line; state = r.state;
      code = r.state === 'verified' ? EXIT.ok : EXIT.error;
    }
  } catch (e) {
    last = `❌ Company DB の写し${mode === 'verify' ? 'の反映' : ''}: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`;
    code = EXIT.error;
  }
  last = String(last).replace(/\s+/g, ' ');
  log(last);
  // 取れた回 (verified) は打たない = ok は入れた後の確かめの回だけ
  if (doPing && (mode === 'verify' || state !== 'verified')) {
    const send = ping || (async (jobId, p) => (await import('../../../scripts/company-db/lz-daily.mjs')).sendPing(jobId, p, { env }));
    await send(JOB_ID, pingFor(state, last, { mode }));
  }
  return { code, last };
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  const { code } = await cli(process.argv.slice(2));
  // pg・fetch の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
