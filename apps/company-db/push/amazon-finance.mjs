#!/usr/bin/env node
/**
 * amazon-finance.mjs — Amazon の決済の行 (warehouse.db の raw_amazon_settlement_lines) を、注文 (疑似注文) の財務として Company DB (Render Postgres・0043) に送る。F2b-2
 * 設計 = AI_reference『システム設計/CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』§4 / §5 / §7
 *
 * 流れは注文の送り手 (mall-orders.mjs) と同じ共通部 (pipeline.mjs): lock → Render の状態 → 台帳 (種類 'order_finance:amazon') の指紋で差分 → outbox → chunk で POST → 受領記録。
 *   集約 = amazon-finance-transform.mjs (SQLite の build と 1 円まで同じ・試験 scripts/test-company-db-amazon-finance.mjs)。
 *   受け口 = Render の POST /apps/company-db/sync/order-finance (ingest/order-finance.mjs。集合の指紋は受け口が計算し直す)
 *
 * どの注文を送るか (§4.3):
 *   --incremental  台帳の watermark (前回そろって終わった回の ingested_at の最大) の 3 日前から後に入った行を持つ注文・疑似注文 + 送れなかった鍵。
 *                  watermark が無い・変換の版が変わった = 全部 (内容を集約し直して指紋が変わったものだけ送る)
 *   --full         全部の注文・疑似注文を集約し直して指紋が変わったものを送る + Render にだけある鍵に空の集合 (日曜。同じ注文の中の一部の行の削除・訂正を拾う)
 *   --from/--to    計上日がその範囲にある行を持つ注文 (選んだ注文は全期間の全部の行)・その日の疑似注文。バックフィル (人が 1 か月ずつ)。watermark は動かさない
 *   🚨 期間は鍵を選ぶだけ。部分の集合は送らない
 * 送れないもの:
 *   整形できない注文 (円未満・日付が読めない・通貨・500 行超) = まるごと送らない・台帳の「送れない鍵」に残して次の回に必ず読み直す (§4.5)
 *   注文番号も計上日も読めない行 = 台帳の「鍵の分からない不正な行」に残し、その間は **疑似注文を 1 つも送らない** (§4.5b)
 * 容量 (§7 / D-W5): chunk を送る前に「Render の DB の大きさ (+ WAL) + 次の chunk の見込み × 置き換えの倍率 + 余裕」が上限の 80% を超えるなら送らずに止まる。
 *   🚨 上限 (env CDB_DB_LIMIT_BYTES) が無ければ送らない (D-W5 = Render の Postgres のプランを中原さんが決めるまで本番に送らない)
 * 突き合わせ (--reconcile。§4.4): 直近 45 日 + 台帳の「未照合の月」の 日 × SKU を Render の mart.v_finance_daily と SQLite の f_amazon_finance_sku_daily_v1 で (鍵の和集合)、
 *   月の手数料を mart.v_finance_account_fees_monthly と f_amazon_account_fees_monthly_v1 で。差の月 = 日次の財務のやり残し (amazon-finance-pending.json) /
 *   月の手数料のやり残し (amazon-account-fees-pending.json) に登録 = 次の daily-sync の build が作り直す。差が 1 回目 ⚠️・2 回続けば ❌
 *
 * 🆕 2026-10-01 (D7b-1b-3・設計 = AI_reference CompanyDB構想/13 §3.1・D-66):
 *   - 🚨 **送る回 (--incremental / --full) は coordinator (apps/warehouse/amazon-finance-coverage-run.js) の中だけ**。単独は dry-run・調べ (--reconcile など)・
 *     人が 1 か月ずつ流すバックフィル (--from/--to = token の無い chunk = Render は complete を無効にする = coverage に影響しない) だけ
 *   - 決済ごとに採った文書の版の行だけを変換する (filterSelectedRows。版の無い行があれば止める)
 *   - --incremental は watermark に加えて「読み直す注文」(amazon_settlement_dirty_orders) を必ず読む
 *   - coordinator は coverage = { generation, runToken, ... } を渡す: 全部の chunk に coverage_generation / run_token・
 *     走査の同じ読み取りの取引の中で manifest (onScanSnapshot)・送った後に読み直す注文の記録を R 以下だけ消す・complete (finalize)
 *
 * 使い方 (miniPC。F2b-2 = 手で流す。daily-sync には F2b-3 で入れる):
 *   node apps/company-db/push/amazon-finance.mjs --from 2026-08-01 --to 2026-08-31 --dry-run     → 送らずに件数・1 注文の最大の行数・最大の JSON・拾われない金額
 *   node apps/company-db/push/amazon-finance.mjs --from 2026-08-01 --to 2026-08-31               → 1 か月だけ送る (上限の env が要る)
 *   node apps/company-db/push/amazon-finance.mjs --reconcile [--all]                             → 突き合わせ (差があれば exit 1)
 *   node apps/company-db/push/amazon-finance.mjs --incremental --require-backfilled --dry-run | --full --require-backfilled --dry-run   (🆕 送る回は coordinator の中だけ = 単独は dry-run)
 *   node apps/company-db/push/amazon-finance.mjs --mark-backfilled                                → 全期間の突き合わせが一致したら完了印
 *   node apps/company-db/push/amazon-finance.mjs --reset-ledger                                   → 台帳の指紋を空にする
 * env: DATA_DIR / RENDER_MIRROR_URL / MIRROR_SYNC_KEY / CDB_PUSH_CHUNK / CDB_DB_LIMIT_BYTES (必須・送るとき) / CDB_FINANCE_ROW_BYTES / CDB_FINANCE_REPLACE_FACTOR / CDB_WAL_ALLOWANCE_BYTES / CDB_CAPACITY_MARGIN_BYTES
 * 🚨 秘密は表示しない。warehouse.db は読むだけ
 */
import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { openLedger } from './ledger.mjs';
import { runPush, fingerprintOf, isDate, jstDate, splitWindows, getJson, DEFAULT_CHUNK, MAX_CHUNK, MAX_LINES_PER_ROW } from './pipeline.mjs';
import { syncBase } from './ne-shipments.mjs';
import { writeEvidence } from './evidence.mjs';
import { orderKey } from '../ingest/orders.mjs';
import { validateFinanceChunk, FINANCE_ORDER_NO_RE } from '../ingest/order-finance.mjs';
import { pseudoOrderNo, isPseudoOrderNo, PSEUDO_PREFIX } from '../finance/order-finance-checksum.mjs';
import { aggregateOrderFinance, financePayload, isRealDate, RAW_COLUMNS, AMAZON_FINANCE_TRANSFORM_VERSION, FINANCE_MALL, FINANCE_SCOPE } from './amazon-finance-transform.mjs';
import { addPendingMonths, ACCOUNT_FEES_PENDING_FILE, PENDING_FILE } from '../../warehouse/amazon-finance-months.js';
import { filterSelectedRows } from './amazon-finance-transform.mjs';
import { selectDocumentVersions, assertDocumentVersionsReady } from '../../warehouse/amazon-settlement-versions.js';

export const FINANCE_KIND = `order_finance:${FINANCE_MALL}`;
export const FINANCE_FLOOR = '2026-01-01';            // policy (0043) の始まり = 決済の行の始まり
export const WATERMARK_LOOKBACK_DAYS = 3;
export const RECONCILE_DAYS = 45;
export const DAILY_WINDOW_DAYS = 62;                  // Render の /daily の上限 (両端込み)
export const FEE_WINDOW_MONTHS = 24;                  // Render の /account-fees は 800 日まで (24 か月 ≦ 731 日)
// 台帳の meta (種類ごと)
export const META = {
  watermark: 'watermark',                  // 前回そろって終わった回 (incremental / full) の raw の ingested_at の最大
  transformVersion: 'transform_version',   // その回の変換の版 (変われば次の incremental は全部)
  unkeyed: 'unkeyed_invalid',              // JSON [{ economic_date, n, example_id }] = 注文番号も計上日も読めない行 (ある間は疑似注文を送らない)
  unreconciled: 'unreconciled_months',     // JSON ['YYYY-MM'] = 送った集合の計上日の月 (照合がそろうまで消さない)
  diffStreak: 'reconcile_diff_streak',     // 突き合わせの差が続いた回数
  backfill: 'backfill_done',               // '1' = 全期間のバックフィルと突き合わせがそろった
};
export const RENDER_PATHS = {
  post: '/order-finance',
  status: `/order-finance/status?mall=${FINANCE_MALL}&scope=${FINANCE_SCOPE}`,
  receipt: '/order-finance/receipt',
  keys: `/order-finance/keys?mall=${FINANCE_MALL}&scope=${FINANCE_SCOPE}`,
};
export const financeKey = (no) => orderKey(FINANCE_MALL, FINANCE_SCOPE, no);
export const orderNoOfKey = (key) => { const pre = `${FINANCE_MALL}|${FINANCE_SCOPE}|`; if (!key.startsWith(pre)) throw new Error(`鍵の形が違う: ${key}`); return key.slice(pre.length); };
const monthOf = (d) => d.slice(0, 7);
const readJson = (ledger, k, dflt) => { const v = ledger.getMeta(k); if (v == null) return dflt; try { return JSON.parse(v); } catch { return dflt; } };

// ── raw を読む SQL (読み取り取引の中) ──
const COLS = RAW_COLUMNS.join(', ');
export const SQL = {
  maxIngested: `SELECT MAX(ingested_at) AS m FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_ingested`,
  byOrder: `SELECT ${COLS} FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_order WHERE amazon_order_id = ?`,
  byPseudo: `SELECT ${COLS} FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_economic WHERE economic_date = ? AND (amazon_order_id IS NULL OR amazon_order_id = '')`,
  // 注文番号の無い行の計上日 (読めない日付を見つける = §4.5b)。疑似注文の鍵の元
  pseudoDates: `SELECT economic_date AS d, COUNT(*) AS n, MIN(id) AS example_id FROM raw_amazon_settlement_lines WHERE amazon_order_id IS NULL OR amazon_order_id = '' GROUP BY economic_date`,
  allOrders: `SELECT DISTINCT amazon_order_id AS o FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_order WHERE amazon_order_id IS NOT NULL AND amazon_order_id <> ''`,
  ingestedSince: `SELECT DISTINCT amazon_order_id AS o, economic_date AS d FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_ingested WHERE ingested_at >= ?`,
  economicRange: `SELECT DISTINCT amazon_order_id AS o, economic_date AS d FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_economic WHERE economic_date BETWEEN ? AND ?`,
  // 🆕 D7b-1b-3: 文書の版 (決済ごとに採る版を JS で決める)・生の表の版 R・読み直す注文
  versions: `SELECT seq, document_version_id, settlement_id, source_layer, ingested_at FROM amazon_settlement_document_versions`,
  revision: `SELECT revision FROM amazon_settlement_source_revision WHERE id = 1`,
  dirty: `SELECT mall_order_no AS o, revision AS r FROM amazon_settlement_dirty_orders`,
};

/** watermark ('YYYY-MM-DD HH:MM:SS' UTC) の days 日前 (同じ書き方) */
export function sinceOf(watermark, days = WATERMARK_LOOKBACK_DAYS) {
  const t = Date.parse(`${String(watermark).replace(' ', 'T')}Z`);
  if (Number.isNaN(t)) return null;
  return new Date(t - days * 86400000).toISOString().replace('T', ' ').slice(0, 19);
}

/** 台帳の中の「鍵ごとの前回の計上日の月」(未照合の月の古い側を取る。§4.4)。
 *  置き換えで消える行数 (容量の見込み) は台帳に持たない = 毎回 Render の鍵の一覧の行数を正にする (台帳の欠け・送る前の上書きで小さく見込まない。#1534 Codex R4 High) */
function keyMonthsStore(ledger) {
  ledger.db.exec(`create table if not exists finance_key_months (kind text not null, key text not null, months text not null, primary key (kind, key))`);
  const get = ledger.db.prepare(`select months from finance_key_months where kind = ? and key = ?`);
  const put = ledger.db.prepare(`insert into finance_key_months (kind, key, months) values (?, ?, ?) on conflict (kind, key) do update set months = excluded.months`);
  return {
    get: (key) => { const r = get.get(ledger.kind, key); return r ? r.months.split(',').filter(Boolean) : []; },
    putMany: (entries) => ledger.db.transaction(() => { for (const [k, ms] of entries) put.run(ledger.kind, k, ms.join(',')); })(),
  };
}

/**
 * 台帳の中の「読み直す鍵」(送れなかった・止めた・前の回の outbox に残った)。次の回に watermark の窓に関係なく必ず読み直す (§4.5)。
 * 🚨 送信の途中で止まっても失わない: 回の最初 (outbox の鍵)・送る前 (整形できない・止めた疑似注文)・chunk の応答ごと (failed / stale) に足し、
 *    最後まで送れた回だけ作り直す (#1534 Codex R1 High / R2 High 3)
 */
export function retryStore(ledger) {
  ledger.db.exec(`create table if not exists finance_retry (kind text not null, key text not null, error text, primary key (kind, key))`);
  const put = ledger.db.prepare(`insert into finance_retry (kind, key, error) values (?, ?, ?) on conflict (kind, key) do update set error = excluded.error`);
  const clear = ledger.db.prepare(`delete from finance_retry where kind = ?`);
  const list = ledger.db.prepare(`select key, error from finance_retry where kind = ? order by key`);
  const add = (entries) => ledger.db.transaction(() => { for (const e of entries) put.run(ledger.kind, e.key, e.error == null ? null : String(e.error).slice(0, 200)); })();
  return {
    list: () => list.all(ledger.kind),
    add,
    replace: (entries) => ledger.db.transaction(() => { clear.run(ledger.kind); add(entries); })(),
  };
}

/**
 * 送る鍵を決めて 1 注文ずつ yield する generator の材料を作る (pipeline の iterate)。
 * sel = { mode: 'incremental' | 'full' | 'range', from, to, since (incremental の ingested_at の下限。null = 全部), extraKeys: [鍵] }
 */
export function makeIterate(sel, run) {
  return function* iterate(warehouse, stats, ctx) {
    const byOrder = warehouse.prepare(SQL.byOrder).safeIntegers(true);
    const byPseudo = warehouse.prepare(SQL.byPseudo).safeIntegers(true);
    stats.maxIngested = warehouse.prepare(SQL.maxIngested).get().m ?? null;
    // 🆕 D7b-1b-3: 同じ読み取りの取引の中で 生の表の版 R・決済ごとに採る版 (D-66) を決める。版の無い行があれば止める (黙って行を落とさない)
    assertDocumentVersionsReady(warehouse);
    stats.sourceRevision = Number(warehouse.prepare(SQL.revision).get()?.revision ?? 0);
    const versions = warehouse.prepare(SQL.versions).all();
    const selectedVersions = selectDocumentVersions(versions);
    const selected = new Map([...selectedVersions].map(([k, v]) => [k, v.seq]));
    stats.selectedVersions = selectedVersions; stats.versionCount = versions.length;
    const pick = (rows) => filterSelectedRows(rows, selected);
    // ① 注文番号の無い行の計上日 = 疑似注文の鍵。読めない日付は「鍵の分からない不正な行」(§4.5b)
    const valid = new Set();
    stats.unkeyed = [];
    for (const r of warehouse.prepare(SQL.pseudoDates).all()) {
      if (isRealDate(r.d)) valid.add(r.d);
      else stats.unkeyed.push({ economic_date: r.d, n: r.n, example_id: r.example_id });
    }
    // ② 選ぶ
    const orders = new Set(), dates = new Set();
    const addRow = (o, d) => { if (o == null || o === '') { if (valid.has(d)) dates.add(d); } else orders.add(o); };
    if (sel.mode === 'full' || (sel.mode === 'incremental' && sel.since == null)) {
      for (const r of warehouse.prepare(SQL.allOrders).all()) orders.add(r.o);
      for (const d of valid) dates.add(d);
    } else if (sel.mode === 'incremental') {
      for (const r of warehouse.prepare(SQL.ingestedSince).all(sel.since)) addRow(r.o, r.d);
    } else {
      for (const r of warehouse.prepare(SQL.economicRange).all(sel.from, sel.to)) addRow(r.o, r.d);
    }
    const addKey = (k) => {
      let no; try { no = orderNoOfKey(k); } catch { stats.badKeys = (stats.badKeys || 0) + 1; return; }
      // 疑似注文は '-:YYYY-MM-DD' (本物の日付) の完全一致だけ。'-' で始まるほかの番号は本物の注文として読み直す = 形の検査で また送れない鍵に残る
      //   (疑似注文と取り違えて落とすと、次の回に一覧から消えて未投入のまま完了印まで通る。#1534 Codex R5 Medium)
      const d = no.startsWith(PSEUDO_PREFIX) ? no.slice(PSEUDO_PREFIX.length) : null;
      if (d != null && /^\d{4}-\d{2}-\d{2}$/.test(d) && isRealDate(d)) dates.add(d); else orders.add(no);
    };
    // 読み直す鍵 (送れなかった・止めた・前の回に outbox に残った。#1534 Codex R1 High)
    for (const k of sel.extraKeys || []) addKey(k);
    // 🆕 D7b-1b-3: 読み直す注文 (生の表の trigger・採る版が変わった) = どの mode でも読む (incremental は watermark に加えて必ず。R13 M1)
    stats.dirtyOrders = 0;
    if (sel.mode !== 'range') for (const r of warehouse.prepare(SQL.dirty).all()) { addKey(financeKey(r.o)); stats.dirtyOrders++; }
    // 台帳で「追跡するだけ・未確認」(指紋 '') の鍵 = Render の復元で指紋を空にした・台帳を Render から取り戻した・--reset-ledger の後 → 全部読み直す (#1534 Codex R1 High)
    let unconfirmed = 0;
    for (const [k, fp] of ctx.fps) if (fp === '') { addKey(k); unconfirmed++; }
    stats.unconfirmed = unconfirmed;
    // ③ Render にだけある鍵 (--full。空の集合を送る)
    const renderOnly = [];
    if (sel.mode === 'full' && ctx.pre && ctx.pre.renderKeys) {
      for (const [no, lines] of ctx.pre.renderKeys) {
        if (lines === 0) continue;   // もう空
        if (isPseudoOrderNo(no)) { const d = no.slice(PSEUDO_PREFIX.length); if (!dates.has(d)) renderOnly.push(no); }
        else if (!orders.has(no)) renderOnly.push(no);
      }
    }
    stats.selectedOrders = orders.size; stats.selectedPseudo = dates.size; stats.renderOnly = renderOnly.length;
    // 🚨 鍵の分からない不正な行がある間は、疑似注文を 1 つも送らない (空の集合も。§4.5b)
    const pseudoBlocked = stats.unkeyed.length > 0;
    stats.pseudoBlocked = pseudoBlocked ? dates.size + renderOnly.filter(isPseudoOrderNo).length : 0;
    // 止めた疑似注文は全部「読み直す鍵」に残す (今回の窓・期間で選んだ分も。落とすと止めが解けた後に読み直されない。#1534 Codex R2 High)
    stats.blockedPseudoKeys = pseudoBlocked ? [...[...dates].map((d) => financeKey(pseudoOrderNo(d))), ...renderOnly.filter(isPseudoOrderNo).map(financeKey)] : [];
    // 採った版の行が無い注文 (ほかの版にだけある・行が消えた) で、台帳にも Render にも無いもの = 送らない (空の集合の受領記録を作らない)
    const known = (no) => ctx.fps.has(financeKey(no)) || !!(ctx.pre && ctx.pre.renderKeys && ctx.pre.renderKeys.has(no));
    stats.skippedEmpty = 0;
    for (const no of [...orders].sort()) {
      const rows = pick(byOrder.all(no));
      if (!rows.length && !FINANCE_ORDER_NO_RE.test(no)) continue;   // 行が消えた不正な形の番号 = 受け口が受け取らない = Render に無い (空の集合も送れない)
      if (!rows.length && !known(no)) { stats.skippedEmpty++; continue; }
      yield { key: financeKey(no), orderNo: no, rows };
    }
    if (!pseudoBlocked) for (const d of [...dates].sort()) {
      const no = pseudoOrderNo(d), rows = pick(byPseudo.all(d));
      if (!rows.length && !known(no)) { stats.skippedEmpty++; continue; }
      yield { key: financeKey(no), orderNo: no, rows };
    }
    for (const no of renderOnly.sort()) {
      if (pseudoBlocked && isPseudoOrderNo(no)) continue;
      yield { key: financeKey(no), orderNo: no, rows: [], renderOnly: true };
    }
    run.persistBeforeSend(stats);   // 送る前に未照合の月と送れない鍵を台帳に書く (全部の build の後・outbox から送る前 = 送信の途中で落ちても残る)
  };
}

/** 1 注文の集合を作る (pipeline の build)。変わった注文の新旧の計上日の月を未照合の月に集める */
export function makeBuild(run, transformVersion = AMAZON_FINANCE_TRANSFORM_VERSION) {
  return (group, ctx) => {
    try { return buildOne(group, ctx); } catch (e) { run.buildFailed.push({ key: group.key, error: String(e.message).slice(0, 200) }); throw e; }
  };
  function buildOne(group, ctx) {
    // 受け口と同じ注文番号の形 (違えば受け口が chunk ごと 400 にして、同じ chunk の正常な注文まで止まる。#1534 Codex R1 Medium)
    if (!FINANCE_ORDER_NO_RE.test(group.orderNo)) throw new Error(`注文番号の形が受け口の決めに合わない (${JSON.stringify(group.orderNo).slice(0, 80)})`);
    const { lines, stats } = group.rows.length ? aggregateOrderFinance(group.orderNo, group.rows) : { lines: [], stats: null };
    if (lines.length > MAX_LINES_PER_ROW) throw new Error(`明細が ${lines.length} 行 (上限 ${MAX_LINES_PER_ROW})`);
    const payload = financePayload(group.orderNo, lines, transformVersion);
    const bytes = Buffer.byteLength(JSON.stringify(payload));
    const s = run.stats;
    s.lines += lines.length; s.rawRows += stats ? stats.rawRows : 0; s.dedupRows += stats ? stats.dedupRows : 0;
    if (lines.length > s.maxLines) { s.maxLines = lines.length; s.maxLinesKey = group.key; }
    if (bytes > s.maxBytes) { s.maxBytes = bytes; s.maxBytesKey = group.key; }
    if (stats && stats.unclassifiedComponents) s.unclassifiedComponents = (s.unclassifiedComponents || 0) + stats.unclassifiedComponents;   // 分けられない部品の数 (0047)
    if (stats && stats.unmapped.rows) {
      s.unmapped.rows += stats.unmapped.rows;
      for (const [c, n] of Object.entries(stats.unmapped.columns)) s.unmapped.columns[c] = (s.unmapped.columns[c] || 0) + n;
      for (const id of stats.unmapped.exampleIds) if (s.unmapped.exampleIds.length < 5) s.unmapped.exampleIds.push(String(id));
    }
    const fp = fingerprintOf(transformVersion, payload);
    if (ctx.fps.get(group.key) !== fp) run.noteChanged(group.key, lines, ctx.pre && ctx.pre.renderKeys ? ctx.pre.renderKeys.get(group.orderNo) : null);
    // 🆕 D7b-1b-3: 作れた注文 (読み直す注文の記録を消せる候補)・受領記録 (lines > 0 だけ = receipt digest。墓石は入れない)
    run.built.add(group.orderNo);
    if (lines.length > 0) run.receipts.push({ mall_order_no: group.orderNo, set_checksum: payload.header.set_checksum, transform_version: payload.header.transform_version, lines: lines.length });
    return { key: group.key, payload, n_lines: lines.length };
  }
}

/** 容量の見張り (§7)。chunk ごとに「いまの大きさ + 送った分の見込み + 次の chunk の見込み + 余裕」が上限 × ratio を超えるなら throw */
export function capacityGuard({ limitBytes, rowBytes, replaceFactor, walAllowanceBytes, marginBytes, orderBytes, ratio = 0.8, refreshEvery = 20, fetchStatus, weightOf = null }) {
  if (!(limitBytes > 0)) throw new Error('容量の上限 (CDB_DB_LIMIT_BYTES) が無い = D-W5 (Render の Postgres のプラン) を決めるまで送らない');
  if (!(rowBytes > 0) || !(replaceFactor > 0) || !(orderBytes > 0)) throw new Error(`容量の設定が不正: 1 行の大きさ ${rowBytes}・置き換えの倍率 ${replaceFactor}・1 注文の受領の大きさ ${orderBytes} (どれも 0 より大きい = 0 だと見張りが効かない。#1534 Codex R3 Low / R4 Medium)`);
  let known = null, since = 0, n = 0;
  const state = { checks: 0, lastKnown: null, maxProjected: 0 };
  // weightOf(rows) = chunk の行数の見込み (注文ごとに max(前に送った行数, 新しい行数) = 空の集合で大量に消すのも 0 と見なさない。#1534 Codex R3 High)
  const guard = async ({ rows, lines, mustOwn }) => {
    if (known == null || n % refreshEvery === 0) {
      const s = await fetchStatus();
      mustOwn();
      const db = Number(s && s.size && s.size.db_bytes);
      if (!Number.isFinite(db) || db <= 0) throw new Error('Render の状態に DB の大きさ (size.db_bytes) が無い = 容量を確かめられないので送らない');
      const wal = s.size.wal_bytes;
      known = db + (Number.isFinite(wal) && wal != null ? wal : walAllowanceBytes);
      since = 0;
      state.lastKnown = known;
    }
    n++;
    const w = weightOf && rows ? weightOf(rows) : lines;
    const next = w * rowBytes * replaceFactor + (rows ? rows.length : 0) * orderBytes;
    const projected = known + since + next + marginBytes;
    state.checks++; state.maxProjected = Math.max(state.maxProjected, projected);
    if (projected > limitBytes * ratio) {
      throw Object.assign(new Error(`容量: いまの見込み ${mb(known + since)} + 次の chunk ${mb(next)} + 余裕 ${mb(marginBytes)} = ${mb(projected)} が上限 ${mb(limitBytes)} の ${Math.round(ratio * 100)}% (${mb(limitBytes * ratio)}) を超えるので送らずに止めた (D-W5)`), { code: 'CAPACITY' });
    }
    since += next;
    let released = false;
    return { release: () => { if (!released) { released = true; since -= next; } } };   // 期限超過で送らなかった = 予約を戻す (#1534 Codex R3 Medium)
  };
  guard.state = state;
  return guard;
}
const mb = (b) => `${Math.round(b / 1048576).toLocaleString()} MB`;

/** Render の鍵を全部 (注文番号 → 行数) */
export async function fetchRenderKeys(fetchImpl, { base, syncKey, mustOwn = () => {}, getOpts = {} }) {
  const out = new Map(); let after = '';
  for (let i = 0; i < 1000; i++) {
    const j = await getJson(fetchImpl, `${base}${RENDER_PATHS.keys}&after=${encodeURIComponent(after)}&limit=50000`, syncKey, 'Render の鍵', getOpts);
    mustOwn();
    if (!Array.isArray(j.keys) || !Array.isArray(j.lines) || j.keys.length !== j.lines.length) throw new Error('Render の鍵の応答の形が違う');
    j.keys.forEach((k, idx) => out.set(k, Number(j.lines[idx])));
    if (!j.next) return out;
    after = j.next;
  }
  throw new Error('Render の鍵が多すぎる (1,000 ページ超)');
}

/**
 * 送る (本体)。戻り値 = runPush の結果 + { finance: { ... } }
 * mode = 'incremental' | 'full' | 'range'
 */
/**
 * coverage (coordinator だけが渡す・設計 13 §3.1) = {
 *   generation, runToken,                        全部の chunk の coverage_generation / run_token (受け口は updating のその世代・token のときだけ適用)
 *   beforeScan({ fetchImpl, base, syncKey, mustOwn, log }) → 走査の前に Render の coverage がその世代・token の updating か確かめる (throw = 送らない)
 *   onScanSnapshot({ warehouse, stats, ctx, r, receipts }) → 同じ読み取りの取引の中で manifest を計算 (throw = run ごと失敗)
 *   finalize({ r, scanSnapshot, dirty, ledger, owner, mustOwn, log }) → 送り終えた後 (台帳の lock の中) に完成の判定と complete (戻り値の error = run は ok = false)
 * }
 * dirty (coordinator が渡す) = { clear(orderNos, maxRevision) → 消した数 } = 読み直す注文の記録を消す別の短い書き込みの接続 (lease を確かめる)
 */
export async function pushAmazonFinance({ warehouse, ledger, base, syncKey, mode, from = null, to = null, dryRun = false, force = false, chunkSize = DEFAULT_CHUNK,
  fetchImpl = fetch, log = console.log, now = () => new Date(), capacity = null, transformVersion = AMAZON_FINANCE_TRANSFORM_VERSION, coverage = null, dirty = null, ...rest }) {
  if (!['incremental', 'full', 'range'].includes(mode)) throw new Error(`mode が不正: ${mode}`);
  if (coverage && mode === 'range') throw new Error('coverage の回は range にしない (一部の期間だけ = 全体のそろいを言えない)');
  if (mode === 'range' && (!isDate(from) || !isDate(to) || from > to)) throw new Error('--from / --to は YYYY-MM-DD で from <= to');
  const retry = retryStore(ledger);
  const watermark = ledger.getMeta(META.watermark);
  const tvPrev = ledger.getMeta(META.transformVersion);
  const since = mode === 'incremental' && watermark && tvPrev === transformVersion ? sinceOf(watermark) : null;
  // 前の回に outbox に残った鍵 = runPush の中 (carryOverOutbox) で消される前に「読み直す鍵」へ書く (Render の鍵を取る前に落ちても失わない。#1534 Codex R2 High)
  const leftover = ledger.outboxKeys();
  if (!dryRun && leftover.length) retry.add(leftover.map((key) => ({ key, error: 'outbox_leftover' })));
  const sel = { mode, from, to, since, extraKeys: [...new Set([...retry.list().map((f) => f.key), ...leftover].filter((k) => typeof k === 'string'))] };
  const store = keyMonthsStore(ledger);
  const changedMonths = new Set(); const keyMonthsNew = []; const weight = new Map();
  const run = {
    stats: { lines: 0, rawRows: 0, dedupRows: 0, maxLines: 0, maxLinesKey: null, maxBytes: 0, maxBytesKey: null, weightLines: 0, unmapped: { rows: 0, columns: {}, exampleIds: [] } },
    noteChanged: (key, lines, renderLines = null) => {
      const ms = [...new Set(lines.map((l) => monthOf(l.economic_date_jst)))].sort();
      for (const m of [...store.get(key), ...ms]) changedMonths.add(m);
      keyMonthsNew.push([key, ms]);
      // 置き換えで消える行 = Render のいまの行数 (走査の前に取った鍵の一覧)。容量の見込みは max(Render, 新)
      const w = Math.max(lines.length, Number.isFinite(renderLines) ? renderLines : 0);
      weight.set(key, w); run.stats.weightLines += w;
    },
    buildFailed: [],
    built: new Set(),   // 作れた注文番号 (D7b-1b-3)
    receipts: [],       // 作れた注文の受領記録 (lines > 0)
    persistBeforeSend: (st) => {
      if (dryRun) return;
      const cur = new Set(readJson(ledger, META.unreconciled, []));
      for (const m of changedMonths) cur.add(m);
      // 読み直す鍵に足す = 今回の整形できない鍵 ∪ 止めた疑似注文 (送り終えたら下で作り直す。送信の途中で落ちたらこのまま残る)
      retry.add([...run.buildFailed, ...(st.blockedPseudoKeys || []).map((key) => ({ key, error: 'pseudo_blocked' }))]);
      ledger.setMeta({ [META.unreconciled]: JSON.stringify([...cur].sort()), [META.unkeyed]: JSON.stringify(st.unkeyed || []) });
      store.putMany(keyMonthsNew);
    },
  };
  let guard = null;
  if (!dryRun) {
    guard = capacityGuard({ ...capacity, fetchStatus: () => getJson(fetchImpl, `${base}${RENDER_PATHS.status}`, syncKey, 'Render の状態 (容量)', { log, sleep: rest.sleep }),
      weightOf: (rows) => rows.reduce((s, x) => s + (weight.get(financeKey(x.mall_order_no)) ?? (x.lines ? x.lines.length : 0)), 0) });
  }
  const stats = {};
  const r = await runPush({
    kind: FINANCE_KIND, label: 'Amazon 財務', warehouse, ledger, fetchImpl, base, syncKey, paths: RENDER_PATHS,
    countOf: (j) => ({ count: j && j.counts ? Number(j.counts.orders) : NaN, maxBatchSeq: j && j.counts && j.counts.max_batch_seq != null ? Number(j.counts.max_batch_seq) : null }),
    keysOf: (j) => (Array.isArray(j.keys) ? j.keys.map(financeKey) : j.keys),
    iterate: makeIterate(sel, run), inScope: () => true, build: makeBuild(run, transformVersion), transformVersion,
    mode, scopeLabel: mode === 'range' ? `計上日 ${from}〜${to} の行を持つ注文` : mode === 'full' ? '全部 (作り直し)' : since ? `ingested_at ${since} 以後` : '全部 (watermark 無し / 変換の版が変わった)',
    chunkSize, dryRun, force, log, now, stats,
    // Render の鍵の一覧 (注文ごとの行数) はどの mode でも取る = --full は Render にだけある鍵・どれも容量の見込みの「消える行」(#1534 Codex R4 High)
    //   coverage の回は先に Render の coverage がその世代・token の updating か確かめる (送り手は世代を採らない・coverage を変えない。R19 M4)
    beforeScan: async ({ mustOwn }) => {
      if (coverage && coverage.beforeScan) { await coverage.beforeScan({ fetchImpl, base, syncKey, mustOwn, log }); mustOwn(); }
      return { renderKeys: await fetchRenderKeys(fetchImpl, { base, syncKey, mustOwn, getOpts: { log, sleep: rest.sleep } }) };
    },
    chunkExtra: coverage ? { coverage_generation: String(coverage.generation), run_token: coverage.runToken } : null,
    onScanSnapshot: coverage && coverage.onScanSnapshot ? (sctx) => coverage.onScanSnapshot({ ...sctx, receipts: run.receipts }) : null,
    beforeChunk: guard,
    receiptRows: (body) => validateFinanceChunk(body).rows,   // 受け口は行を作り直す (内容の列だけ + 付け足し) = 受領記録の指紋も同じ形で
    // failed / stale の鍵は outbox から消える前に「読み直す鍵」へ (後の chunk で落ちても失わない。#1534 Codex R2 High)
    beforeAck: ({ failedKeys, staleKeys }) => retry.add([...failedKeys.map((key) => ({ key, error: 'failed' })), ...staleKeys.map((key) => ({ key, error: 'stale' }))]),
    // 最後まで送れた回の確定 (読み直す鍵の作り直し・watermark) は lock の中で、持ち主の確認と同じ取引 (lock を外した後に書くと、次の送り手が残した鍵を消す。#1534 Codex R3 High)
    afterSend: async ({ owner, r: rr, mustOwn }) => {
      const failed = [...rr.transformErrors.map((t) => ({ key: t.key, error: String(t.error).slice(0, 200) })),
        ...rr.failed.map((f) => ({ key: f.key, error: String(f.error || 'failed').slice(0, 200) })),
        ...rr.staleKeys.map((k) => ({ key: k, error: 'stale' })),
        ...(stats.blockedPseudoKeys || []).map((k) => ({ key: k, error: 'pseudo_blocked (鍵の分からない不正な行があるので止めた)' }))];
      const unkeyed = stats.unkeyed || [];
      const meta = { [META.unkeyed]: JSON.stringify(unkeyed) };
      // watermark は incremental / full でそろって終わった回だけ進める (範囲のバックフィルは動かさない)
      if (mode !== 'range' && rr.ok && !unkeyed.length && stats.maxIngested) { meta[META.watermark] = stats.maxIngested; meta[META.transformVersion] = transformVersion; }
      ledger.db.transaction(() => { ledger.assertLock(owner); retry.replace(failed); ledger.setMeta(meta); }).immediate();
      const out = { failedKeys: failed.length, dirtyCleared: 0, dirtyClearable: 0 };
      // 🆕 D7b-1b-3: 読み直す注文の記録を消す = 読み取りの版 R 以下だけ (後から入った記録を消さない・R14 M1)。
      //   消してよい注文 = この回で作れた注文のうち、送れなかった・stale・整形できない・止めた疑似注文でないもの
      //   (applied / same になった注文と、中身が台帳の送付確認済みの指紋と同じで送らなかった (unchanged) 注文。
      //    unchanged の 3 つの条件 = 台帳の指紋が送付確認済み・Render の復元の検査を通過 (受領記録が無ければ runPush が指紋を空にする)・今回の読み取りで同じ中身を再計算した = R17 M3)
      if (dirty && mode !== 'range' && stats.sourceRevision != null) {
        const bad = new Set(failed.map((x) => { try { return orderNoOfKey(x.key); } catch { return null; } }).filter(Boolean));
        const clearable = [...run.built].filter((no) => !bad.has(no));
        out.dirtyClearable = clearable.length;
        mustOwn();
        out.dirtyCleared = dirty.clear(clearable, stats.sourceRevision);
      }
      if (coverage && coverage.finalize) {
        mustOwn();
        out.coverage = await coverage.finalize({ r: rr, scanSnapshot: rr.scanSnapshot, stats, ledger, owner, log, failedKeys: failed, unkeyed, dirtyCleared: out.dirtyCleared });
        if (out.coverage && out.coverage.error) out.error = out.coverage.error;
      }
      return out;
    },
    ...rest,
  });
  const finance = { ...run.stats, selectedOrders: stats.selectedOrders ?? 0, selectedPseudo: stats.selectedPseudo ?? 0, renderOnly: stats.renderOnly ?? 0,
    sourceRevision: stats.sourceRevision ?? null, dirtyOrders: stats.dirtyOrders ?? 0, skippedEmpty: stats.skippedEmpty ?? 0, receipts: run.receipts.length,
    unkeyed: stats.unkeyed || [], pseudoBlocked: stats.pseudoBlocked || 0, unconfirmed: stats.unconfirmed ?? 0, maxIngested: stats.maxIngested ?? null, since, capacity: guard ? guard.state : null };
  r.finance = finance;
  if (r.afterSend) { r.finance.failedKeys = r.afterSend.failedKeys; r.finance.dirtyCleared = r.afterSend.dirtyCleared; r.finance.coverage = r.afterSend.coverage ?? null; }
  r.ok = r.ok && !finance.unkeyed.length;
  return r;
}

// ── 突き合わせ (§4.4) ──
export const UNIT_COLUMNS = ['units_ordered', 'units_refunded_customer', 'units_marketplace_guarantee', 'units_a_to_z_refund', 'units_net_sold'];
export const DAILY_AMOUNT_COLUMNS = ['sales_principal_jpy', 'sales_shipping_jpy', 'sales_giftwrap_jpy', 'sales_tax_jpy', 'commission_jpy', 'fba_fulfillment_jpy', 'fba_storage_jpy', 'closing_fee_jpy',
  'shipping_chargeback_jpy', 'giftwrap_chargeback_jpy', 'promotion_jpy', 'promotion_tax_jpy', 'points_jpy', 'warehouse_damage_jpy', 'warehouse_lost_jpy', 'safe_t_jpy',
  'refund_principal_jpy', 'reversal_reimbursement_jpy', 'misc_fee_jpy', 'other_fee_jpy', 'other_amount_jpy'];
export const DAILY_COMPARE_COLUMNS = [...UNIT_COLUMNS, ...DAILY_AMOUNT_COLUMNS, 'profit_before_cogs_jpy'];
/** build の profit_amount から原価を除いた式 (0043 の v_finance_daily と同じ・列から計算 = 丸めの順に左右されない) */
export const profitBeforeCogs = (r) => r.sales_principal_jpy + r.sales_shipping_jpy + r.sales_giftwrap_jpy - r.commission_jpy - r.fba_fulfillment_jpy - r.fba_storage_jpy - r.closing_fee_jpy
  - r.shipping_chargeback_jpy - r.giftwrap_chargeback_jpy - r.promotion_jpy - r.points_jpy - r.refund_principal_jpy + r.warehouse_damage_jpy + r.warehouse_lost_jpy + r.safe_t_jpy + r.reversal_reimbursement_jpy;

/** SQLite の日次の財務 (Easy Ship の割り振りだけの行は除く = Company DB は割り振らない。売上などと混ざった行は残す) */
export function readSqliteDaily(warehouse, from, to) {
  return warehouse.prepare(`SELECT date_jst, seller_sku, ${[...UNIT_COLUMNS, ...DAILY_AMOUNT_COLUMNS].join(', ')} FROM f_amazon_finance_sku_daily_v1
     WHERE date_jst BETWEEN ? AND ? AND COALESCE(source_layer_summary, '') <> 'easy_ship_alloc'`).all(from, to)
    .map((r) => ({ ...r, profit_before_cogs_jpy: profitBeforeCogs(r) }));
}
/** SQLite の月の手数料 */
export function readSqliteFees(warehouse, months) {
  if (!months.length) return [];
  return warehouse.prepare(`SELECT substr(month_start_jst, 1, 7) AS month, fee_type, amount_jpy, row_count FROM f_amazon_account_fees_monthly_v1
     WHERE substr(month_start_jst, 1, 7) IN (${months.map(() => '?').join(', ')})`).all(...months);
}
const cents = (v) => Math.round(Number(v ?? 0) * 100);
/** 日 × SKU の突き合わせ (鍵の和集合)。戻り値 = [{ date_jst, seller_sku, side: 'both' | 'sqlite_only' | 'render_only', columns: [{ c, sqlite, render }] }] */
export function diffFinanceDaily(localRows, remoteRows) {
  const L = new Map(localRows.map((r) => [`${r.date_jst}\u0000${r.seller_sku}`, r]));
  const R = new Map(remoteRows.map((r) => [`${r.date_jst}\u0000${r.seller_sku}`, r]));
  const out = [];
  for (const k of new Set([...L.keys(), ...R.keys()])) {
    const l = L.get(k), r = R.get(k);
    const [date_jst, seller_sku] = k.split('\u0000');
    if (!l || !r) { out.push({ date_jst, seller_sku, side: l ? 'sqlite_only' : 'render_only', columns: [] }); continue; }
    const columns = DAILY_COMPARE_COLUMNS.filter((c) => cents(l[c]) !== cents(r[c])).map((c) => ({ c, sqlite: l[c], render: r[c] }));
    if (columns.length) out.push({ date_jst, seller_sku, side: 'both', columns });
  }
  return out.sort((a, b) => (a.date_jst < b.date_jst ? -1 : a.date_jst > b.date_jst ? 1 : a.seller_sku < b.seller_sku ? -1 : 1));
}
/** 月 × 手数料の種類の突き合わせ (金額と行数) */
export function diffAccountFees(localRows, remoteRows) {
  const key = (r) => `${r.month}\u0000${r.fee_type}`;
  const L = new Map(localRows.map((r) => [key(r), r])), R = new Map(remoteRows.map((r) => [key(r), r]));
  const out = [];
  for (const k of new Set([...L.keys(), ...R.keys()])) {
    const l = L.get(k), r = R.get(k);
    const [month, fee_type] = k.split('\u0000');
    const la = l ? cents(l.amount_jpy) : 0, ra = r ? cents(r.amount_jpy) : 0, ln = l ? Number(l.row_count) : 0, rn = r ? Number(r.row_count) : 0;
    if (la !== ra || ln !== rn) out.push({ month, fee_type, sqlite: l ? { amount_jpy: l.amount_jpy, row_count: ln } : null, render: r ? { amount_jpy: r.amount_jpy, row_count: rn } : null });
  }
  return out.sort((a, b) => (a.month + a.fee_type < b.month + b.fee_type ? -1 : 1));
}
const monthsBetween = (from, to) => { const out = []; let [y, m] = from.split('-').map(Number); const [ty, tm] = to.split('-').map(Number); while (y < ty || (y === ty && m <= tm)) { out.push(`${y}-${String(m).padStart(2, '0')}`); m++; if (m > 12) { m = 1; y++; } } return out; };
const monthEnd = (ym) => { const [y, m] = ym.split('-').map(Number); return new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10); };

/**
 * 突き合わせる。all = 全期間 (FINANCE_FLOOR〜今日)。戻り値 = { ok, level: 'ok' | 'warn' | 'error', daily: [差], fees: [差], uncovered, checkedMonths, dailyDiffMonths, feeDiffMonths, streak }
 * 差の月は日次の財務 / 月の手数料のやり残しに登録。未照合の月は、その月をまるごと比べて差が無ければ消す
 */
export const RECONCILE_BUDGET_MS = 480000;   // 突き合わせ全体の読み取りの締め切り (daily-sync の工程の上限 600 秒より前に、自分で理由を出して止まる)
export async function reconcileAmazonFinance({ warehouse, ledger, dataDir, base, syncKey, fetchImpl = fetch, all = false, today = jstDate(0), log = console.log, registerPending = true, sleep = undefined, budgetMs = RECONCILE_BUDGET_MS }) {
  if (!base) throw new Error('送り先が決まらない (RENDER_MIRROR_URL / RENDER_PORTAL_URL)');
  // 読み取りは 5xx などを読み直す (Render の入れ替わりをまたぐ)。全体の締め切りは 1 つ (1 つの読み取りが長引いても工程の上限を越えない)
  const getOpts = { log, sleep, deadline: Date.now() + budgetMs };
  const unreconciled = readJson(ledger, META.unreconciled, []).filter((m) => /^\d{4}-\d{2}$/.test(m) && m <= today.slice(0, 7));
  const windows = [];   // [from, to, fullMonth | null]
  if (all) { for (const m of monthsBetween(FINANCE_FLOOR.slice(0, 7), today.slice(0, 7))) windows.push([`${m}-01`, m === today.slice(0, 7) ? today : monthEnd(m), m]); }
  else {
    const from = jstDate(-(RECONCILE_DAYS - 1)) < FINANCE_FLOOR ? FINANCE_FLOOR : jstDate(-(RECONCILE_DAYS - 1));
    for (const [a, b] of splitWindows(from, today, DAILY_WINDOW_DAYS)) windows.push([a, b, null]);
    for (const m of unreconciled) windows.push([`${m}-01`, m === today.slice(0, 7) ? today : monthEnd(m), m]);
  }
  const daily = new Map();   // 日 × SKU の差 (窓が重なっても 1 つ)
  const monthsChecked = new Set();
  for (const [a, b] of windows) {
    const j = await getJson(fetchImpl, `${base}/order-finance/daily?mall=${FINANCE_MALL}&scope=${FINANCE_SCOPE}&from=${a}&to=${b}`, syncKey, 'Render の日次の財務', getOpts);
    if (!Array.isArray(j.rows)) throw new Error('Render の日次の財務の応答に rows が無い');
    for (const d of diffFinanceDaily(readSqliteDaily(warehouse, a, b), j.rows)) daily.set(`${d.date_jst}\u0000${d.seller_sku}`, d);
    for (const m of monthsBetween(a.slice(0, 7), b.slice(0, 7))) monthsChecked.add(m);
  }
  const months = [...monthsChecked].sort();
  let fees = [];
  if (months.length) {
    // 受け口は 800 日まで = 期間が 24 か月に収まる組に分けて取る (月は飛び飛びもある。#1534 Codex R1 Medium)
    const remote = [];
    const idx = (m) => Number(m.slice(0, 4)) * 12 + Number(m.slice(5, 7)) - 1;
    const parts = [];
    for (const m of months) { const last = parts[parts.length - 1]; if (last && idx(m) - idx(last[0]) < FEE_WINDOW_MONTHS) last.push(m); else parts.push([m]); }
    for (const part of parts) {
      const j = await getJson(fetchImpl, `${base}/order-finance/account-fees?mall=${FINANCE_MALL}&scope=${FINANCE_SCOPE}&from=${part[0]}-01&to=${monthEnd(part[part.length - 1])}`, syncKey, 'Render の月の手数料', getOpts);
      if (!Array.isArray(j.rows)) throw new Error('Render の月の手数料の応答に rows が無い');
      for (const r of j.rows) { const m = String(r.month_start_jst).slice(0, 7); if (part.includes(m)) remote.push({ month: m, fee_type: r.fee_type, amount_jpy: r.amount_jpy, row_count: r.row_count }); }
    }
    fees = diffAccountFees(readSqliteFees(warehouse, months), remote);
  }
  const unc = await getJson(fetchImpl, `${base}/order-finance/uncovered?mall=${FINANCE_MALL}&scope=${FINANCE_SCOPE}`, syncKey, 'Render の採用されない行', getOpts);
  const uncovered = Array.isArray(unc.rows) ? unc.rows.reduce((s, r) => s + Number(r.n || 0), 0) : NaN;
  const dailyDiff = [...daily.values()];
  const dailyDiffMonths = [...new Set(dailyDiff.map((d) => monthOf(d.date_jst)))].sort();
  const feeDiffMonths = [...new Set(fees.map((f) => f.month))].sort();
  const anyDiff = dailyDiff.length > 0 || fees.length > 0 || !(uncovered === 0);
  // 差の月をやり残しに (次の build が同じ世代の raw から作り直す)。未照合の月は、まるごと比べて差の無い月だけ消す
  if (registerPending && dataDir) {
    if (dailyDiffMonths.length) addPendingMonths(dataDir, dailyDiffMonths, { file: PENDING_FILE });
    if (feeDiffMonths.length) addPendingMonths(dataDir, feeDiffMonths, { file: ACCOUNT_FEES_PENDING_FILE });
  }
  const fullyChecked = new Set(windows.filter((w) => w[2]).map((w) => w[2]));
  const diffMonths = new Set([...dailyDiffMonths, ...feeDiffMonths]);
  const left = readJson(ledger, META.unreconciled, []).filter((m) => !(fullyChecked.has(m) && !diffMonths.has(m)));
  const streak = anyDiff ? (Number(ledger.getMeta(META.diffStreak)) || 0) + 1 : 0;
  ledger.setMeta({ [META.unreconciled]: JSON.stringify([...new Set([...left, ...diffMonths])].sort()), [META.diffStreak]: String(streak) });   // 戻り値の unreconciledLeft と同じ
  const level = !anyDiff ? 'ok' : streak >= 2 ? 'error' : 'warn';
  for (const d of dailyDiff.slice(0, 20)) log(`  差 ${d.date_jst} ${d.seller_sku}: ${d.side === 'both' ? d.columns.map((c) => `${c.c} ${c.sqlite} / ${c.render}`).join(', ') : d.side === 'sqlite_only' ? 'SQLite にだけある' : 'Render にだけある'} (SQLite / Render)`);
  for (const f of fees.slice(0, 20)) log(`  月の手数料の差 ${f.month} ${f.fee_type}: SQLite ${f.sqlite ? `${f.sqlite.amount_jpy} 円・${f.sqlite.row_count} 行` : '無し'} / Render ${f.render ? `${f.render.amount_jpy} 円・${f.render.row_count} 行` : '無し'}`);
  const saved = [...new Set([...left, ...diffMonths])].sort();
  return { ok: !anyDiff, level, daily: dailyDiff, fees, uncovered, checkedMonths: months, dailyDiffMonths, feeDiffMonths, streak, unreconciledLeft: saved.length };
}

export function summarizeReconcile(rr, { all = false } = {}) {
  const head = rr.level === 'ok' ? '✅' : rr.level === 'warn' ? '⚠️' : '❌';
  const scope = all ? `全期間 ${rr.checkedMonths[0] ?? '-'}〜${rr.checkedMonths[rr.checkedMonths.length - 1] ?? '-'}` : `直近 ${RECONCILE_DAYS} 日 + 未照合の月`;
  if (rr.ok) return `${head} Company DB Amazon 財務 突き合わせ (${scope}): 日 × SKU・月の手数料とも SQLite と一致 (採用されない行 0)`;
  return `${head} Company DB Amazon 財務 突き合わせ (${scope}): 日 × SKU の差 ${rr.daily.length} (月 ${rr.dailyDiffMonths.join(', ') || '-'}) / 月の手数料の差 ${rr.fees.length} (月 ${rr.feeDiffMonths.join(', ') || '-'}) / 採用されない行 ${rr.uncovered}`
    + ` → 差の月を build のやり残しに登録した (${rr.streak} 回続けて差${rr.streak >= 2 ? ' = ❌' : ' = 1 回目は ⚠️'})`;
}

export function summarizeFinance(r) {
  const f = r.finance || {};
  const lines = `注文 ${f.selectedOrders ?? 0} + 疑似注文 ${f.selectedPseudo ?? 0}${f.renderOnly ? ` + Render にだけある ${f.renderOnly}` : ''} を集約 (決済の行 ${f.rawRows ?? 0} → 重複除去 ${f.dedupRows ?? 0} → 財務の行 ${f.lines ?? 0})・1 注文の最大 ${f.maxLines ?? 0} 行 (${f.maxLinesKey ?? '-'})・最大の JSON ${Math.round((f.maxBytes || 0) / 1024)} KB`;
  const warn = [
    f.unkeyed && f.unkeyed.length ? `❌ 注文番号も計上日も読めない行 ${f.unkeyed.reduce((s, u) => s + Number(u.n), 0)} 行 (例 id ${f.unkeyed[0].example_id}) = 疑似注文 ${f.pseudoBlocked} 件を送らなかった` : '',
    f.unmapped && f.unmapped.rows ? `⚠️ どの列にも入らない金額を持つ決済の行 ${f.unmapped.rows} (${Object.entries(f.unmapped.columns).map(([c, n]) => `${c} ${n}`).join(', ')}・例 id ${f.unmapped.exampleIds.join(', ')})` : '',
    // 分けられない部品 (0047 = D7b-3 の正式な利益を止める。種類を分けるまでは続く = 頭の ✅ / ⚠️ は変えず、数だけ見せる)
    f.unclassifiedComponents ? `分けられない決済の部品 ${f.unclassifiedComponents} (集約した注文の中・D7b-3 でその行の正式な利益は null)` : '',
  ].filter(Boolean).join(' / ');
  if (r.lockedBy) return `⏸️ Company DB Amazon 財務 push: 別の送り手が走っているので見送り (${r.lockedBy.owner} pid ${r.lockedBy.pid})`;
  if (r.dryRun) return `${r.transformErrors.length || (f.unkeyed && f.unkeyed.length) ? '❌' : f.unmapped && f.unmapped.rows ? '⚠️' : '✅'} dry-run: ${lines} / 変わった ${r.changed} / 整形できない ${r.transformErrors.length}${warn ? ` / ${warn}` : ''}`;
  // 最後の行の頭 = daily-sync の判定 (isWarnSummary は頭の ⚠️ だけを見る)。Render の復元・台帳の取り戻しも ⚠️ (✅ で始めると全部 OK に数えられる。#1536 Codex R1 Medium)
  const head = !r.ok ? '❌' : ((f.unmapped && f.unmapped.rows) || r.ledgerReset || r.ledgerRebuilt) ? '⚠️' : '✅';
  return `${head} Company DB Amazon 財務 push: 変わった ${r.changed} 注文を送った (applied ${r.applied} / same ${r.same} / stale ${r.stale} / failed ${r.failed.length} / 整形できない ${r.transformErrors.length}) 世代 ${r.batchSeq ?? '-'} chunk ${r.chunks} / ${lines}`
    + (warn ? ` / ${warn}` : '') + (r.ledgerReset ? ` / ⚠️Render が復元されていたので台帳の指紋を空にして送り直した (${r.ledgerReset})` : '')
    + (r.ledgerRebuilt ? ` / ⚠️台帳が空だったので Render から ${r.ledgerRebuilt} 注文を取り戻した` : '');
}

export function parseArgs(argv) {
  const out = { incremental: false, full: false, from: null, to: null, dryRun: false, force: false, reconcile: false, all: false, markBackfilled: false, requireBackfilled: false, resetLedger: false, dataDir: null, chunk: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const val = () => { const v = argv[++i]; if (v === undefined || String(v).startsWith('--')) throw new Error(`${a} に値が無い`); return v; };
    if (a === '--incremental') out.incremental = true;
    else if (a === '--full') out.full = true;
    else if (a === '--from') out.from = val();
    else if (a === '--to') out.to = val();
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--force') out.force = true;
    else if (a === '--reconcile') out.reconcile = true;
    else if (a === '--all') out.all = true;
    else if (a === '--mark-backfilled') out.markBackfilled = true;
    else if (a === '--require-backfilled') out.requireBackfilled = true;
    else if (a === '--reset-ledger') out.resetLedger = true;
    else if (a === '--data-dir') out.dataDir = val();
    else if (a === '--chunk') out.chunk = val();
    else throw new Error(`知らない引数: ${a}`);
  }
  const ops = [out.incremental, out.full, !!(out.from || out.to), out.reconcile, out.markBackfilled, out.resetLedger].filter(Boolean).length;
  if (ops !== 1) throw new Error('--incremental / --full / --from と --to / --reconcile / --mark-backfilled / --reset-ledger のどれか 1 つを指定する');
  if ((out.from && !out.to) || (!out.from && out.to)) throw new Error('--from と --to は組で');
  if (out.from && (!isDate(out.from) || !isDate(out.to) || out.from > out.to)) throw new Error('--from / --to は YYYY-MM-DD で from <= to');
  if (out.all && !out.reconcile) throw new Error('--all は --reconcile と一緒に');
  return out;
}

/** 容量の見張りの設定 (env)。1 行の大きさ・置き換えの倍率は PGlite で測った値を既定にする (F2b-2 の測定で決める) */
export function capacityFromEnv(env = process.env) {
  const n = (k, d) => (env[k] != null && env[k] !== '' ? Number(env[k]) : d);
  const c = { limitBytes: n('CDB_DB_LIMIT_BYTES', 0), rowBytes: n('CDB_FINANCE_ROW_BYTES', 1000), replaceFactor: n('CDB_FINANCE_REPLACE_FACTOR', 2), orderBytes: n('CDB_FINANCE_ORDER_BYTES', 300),
    walAllowanceBytes: n('CDB_WAL_ALLOWANCE_BYTES', 1024 ** 3), marginBytes: n('CDB_CAPACITY_MARGIN_BYTES', 512 * 1024 ** 2) };
  for (const [k, v] of Object.entries(c)) if (!Number.isFinite(v) || v < 0) throw new Error(`容量の設定が不正: ${k} = ${v}`);
  if (!(c.rowBytes > 0) || !(c.replaceFactor > 0) || !(c.orderBytes > 0)) throw new Error('容量の設定が不正: CDB_FINANCE_ROW_BYTES / CDB_FINANCE_REPLACE_FACTOR / CDB_FINANCE_ORDER_BYTES は 0 より大きく');
  return c;
}

async function main() {
  const a = parseArgs(process.argv.slice(2));
  const base = syncBase(), syncKey = process.env.MIRROR_SYNC_KEY || '';
  const dataDir = (process.env.DATA_DIR || a.dataDir || '').trim();
  if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
  const chunkSize = a.chunk != null ? Number(a.chunk) : (process.env.CDB_PUSH_CHUNK ? Number(process.env.CDB_PUSH_CHUNK) : DEFAULT_CHUNK);
  if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_CHUNK) throw new Error(`chunk が不正: ${a.chunk ?? process.env.CDB_PUSH_CHUNK} (1〜${MAX_CHUNK})`);
  const warehouse = new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true, timeout: Number(process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS) || 60000 });
  const ledger = openLedger(dataDir, { kind: FINANCE_KIND });
  const backfilled = ledger.getMeta(META.backfill) === '1';
  const mode = a.incremental ? 'incremental' : a.full ? 'full' : a.from ? 'range' : null;
  try {
    if (a.resetLedger) { const n = ledger.resetFingerprints(); console.log(`台帳 (Amazon 財務) の指紋を空にした: ${n} 件 (鍵は残す)。次の回で全部送り直す ('same' が返るだけ)`); return; }
    if (a.requireBackfilled && !backfilled && (mode === 'incremental' || mode === 'full' || a.reconcile)) {
      console.log(`⏭️ Company DB Amazon 財務: 初回のバックフィル前 (台帳に完了印が無い) なので${a.reconcile ? '突き合わせない' : '送らない'} → db/company/README.md の手順で --from/--to を 1 か月ずつ流し、--mark-backfilled`);
      return;
    }
    if (a.reconcile || a.markBackfilled) {
      if (a.markBackfilled) {
        const failed = retryStore(ledger).list(), unkeyed = readJson(ledger, META.unkeyed, []);
        if (ledger.countConfirmed() === 0) throw new Error('台帳に送付確認済みが 1 件も無い = バックフィルをまだ流していない');
        if (failed.length || unkeyed.length) throw new Error(`送れなかった鍵 ${failed.length} / 鍵の分からない不正な行 ${unkeyed.length} が残っている = 完了印を付けない (直してから流し直す)`);
      }
      // 手で流す全期間 (--all・完了印) は月が増えても締め切りに当たらないよう 1 時間 (daily-sync の直近 45 日は既定 480 秒)
      const rr = await reconcileAmazonFinance({ warehouse, ledger, dataDir, base, syncKey, all: a.all || a.markBackfilled, ...(a.all || a.markBackfilled ? { budgetMs: 3600000 } : {}) });
      console.log(summarizeReconcile(rr, { all: a.all || a.markBackfilled }));
      if (a.markBackfilled) {
        if (!rr.ok) { console.log('❌ 全期間の突き合わせに差がある = 完了印を付けない'); process.exitCode = 1; return; }
        ledger.putMeta(META.backfill, '1'); console.log(`台帳 (Amazon 財務) にバックフィルの完了印を付けた (送付確認済み ${ledger.countConfirmed()} 件)`);
        return;
      }
      process.exitCode = rr.level === 'error' ? 1 : 0;
      return;
    }
    // 🚨 D7b-1b-3: 送る回 (--incremental / --full) は coordinator (amazon-finance-coverage-run.js) の中だけ。単独は dry-run・バックフィル (--from/--to) だけ
    if (!a.dryRun && (mode === 'incremental' || mode === 'full')) {
      throw new Error('--incremental / --full で送るのは coordinator (node apps/warehouse/amazon-finance-coverage-run.js) の回の中だけ (決済の取込・coverage の世代と token と一緒)。単独は --dry-run で調べる');
    }
    if (!a.dryRun && mode === 'range') console.log('⚠️ --from/--to の送信は token の無い chunk = Render は Amazon 財務の complete を無効にする (次の coordinator の回で作り直す)');
    const capacity = a.dryRun ? null : capacityFromEnv();
    if (!a.dryRun && !(capacity.limitBytes > 0)) throw new Error('容量の上限 (CDB_DB_LIMIT_BYTES) が無い = D-W5 (Render の Postgres のプラン) を決めるまで送らない。まず --dry-run');
    const startedAt = new Date();
    const r = await pushAmazonFinance({ warehouse, ledger, base, syncKey, mode, from: a.from, to: a.to, dryRun: a.dryRun, force: a.force, chunkSize, capacity });
    console.log(summarizeFinance(r));
    if ((mode === 'incremental' || mode === 'full') && !a.dryRun) {
      writeEvidence(dataDir, 'finance-amazon', { kind: 'order_finance', mall: FINANCE_MALL, scope: FINANCE_SCOPE, mode, ok: !!r.ok, run_id: r.runId ?? null, batch_seq: r.batchSeq ?? null,
        started_at: startedAt.toISOString(), changed: r.changed, applied: r.applied, same: r.same, stale: r.stale, failed: r.failed.length, transform_errors: r.transformErrors.length,
        unkeyed: (r.finance.unkeyed || []).length, unmapped_rows: r.finance.unmapped.rows });
    }
    process.exitCode = r.lockedBy ? 1 : (r.dryRun ? (r.transformErrors.length || r.finance.unkeyed.length ? 1 : 0) : (r.ok ? 0 : 1));
  } finally { ledger.close(); warehouse.close(); }
}

const isMain = !!process.argv[1] && path.resolve(process.argv[1]).toLowerCase() === fileURLToPath(import.meta.url).toLowerCase();
// 🚨 fetch の直後に process.exit() しない (Windows の Node は終了コード 127 になる。#1386)
if (isMain) main().catch((e) => {
  console.error(`❌ Company DB Amazon 財務: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
  process.exitCode = 1;
  setTimeout(() => process.exit(1), 10000).unref();
});
