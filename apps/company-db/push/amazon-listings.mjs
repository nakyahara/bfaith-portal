#!/usr/bin/env node
/**
 * push/amazon-listings.mjs — 前の夜に想定利益の夜間 (miniPC 23:30・apps/expected-profit/fetch-listings.js) が取って残した Amazon の出品レポート
 *   (GET_MERCHANT_LISTINGS_ALL_DATA の TSV 原文 = DATA_DIR/mall-items-history/amazon/YYYY/MM/items_amazon_*.tsv.gz・manifest.jsonl) を読み、
 *   Company DB (Render Postgres) の ops.amazon_listing_report へ 1 要求で送る (2026-10-12 中原さん「Amazon の登録の商品名・まだつないでいない SKU」。
 *   受け口 = POST /apps/company-db/sync/amazon-listings・受け皿 = migration 0076 (仮の番号))。
 *
 *   node apps/company-db/push/amazon-listings.mjs --send        ← daily-sync (毎朝 07:00 の 1 工程)。Render と同じ取得なら送らない
 *   node apps/company-db/push/amazon-listings.mjs --dry-run     ← 読んで数えるだけ (Render は GET で読むだけ = POST しない)
 *   [--history-dir <dir>] (既定 = env DATA_DIR → リポジトリの data/ の mall-items-history) / [--force] (安全弁を越える = 手で流すときだけ)
 *
 * 決め:
 *   - 🚨 **SP-API は呼ばない** (createReport は 1 分に 1 回の制限・同じレポートを 2 回取らない)。材料は想定利益の夜間が残した原文だけ = 新しい定期実行も無い
 *   - 🚨 主店 = 想定利益の夜間 (fetch-listings.js の amazonShopId) と同じ出どころ = expected-profit.db の覚え書きのセラーID → env SP_API_SELLER_ID・marketplace は env SP_API_MARKETPLACE_ID (日本だけ)。
 *     決まらない (無い・読めない・日本でない) = 推測で選ばず ❌ (#1691 Codex R2 M1 = 同じ日本の marketplace の別のセラーの回で主店を置き換えない)
 *   - 送るのは manifest の中で「Amazon・主店と完全に同じ shop_id・取得元 merchant_listings_all_data・complete=true・TSV・64 桁の sha256」の回のうち、
 *     取得の時刻 (snapshot_at) がいちばん新しい回 (manifest の並びに頼らない・#1691 Codex R1 M3)。部分取得・0 件は archive-items.mjs が complete=false / 保存しない。
 *     gz を開いて sha256 が manifest と合わなければ送らない (❌ = 壊れた・書きかけの履歴を Company DB に入れない)
 *   - 世代 = その回の snapshot_at (レポートを取り終えた時刻 ms)。Render の世代と同じ・中身も同じ = 送らない (✅ 変わりなし)。Render の方が新しい = 送らない (⚠️)
 *   - 列は日本語の見出し (日本のマーケットプレイスの今の形) と英語の見出し (SP-API の説明の形) のどちらでも読む (HEADER_ALIASES)。
 *     seller SKU の列が無い = ❌ (別の形のレポート)。ほかの列が無い = その値を null で送り ⚠️ (例: 商品名の列が無い → 画面に商品名が出ない理由が朝の通知で分かる)
 *   - 🚨 1 行の違反で全部を捨てない: SKU が空・同じ SKU が 2 行目以降・形が読めない (列の数が見出しと違う = 説明の中の改行など) 行は外して数える (skipped_count)。
 *     ただし外した行が多い (max(20, 2%)) = レポートの形が変わった疑い = 送らない (❌)
 *   - 🛑 安全弁: Render に前の回があり、行が前の 80% 未満 = 送らない (❌・既存の行を消さない)。初回 (Render に前の回が無い) = 想定利益の夜間の判定 (enum_status) が ok でない回・
 *     manifest の同じ店の 1 つ前の回より 20% 以上少ない回は送らない (#1691 Codex R1 M2)。受け口も同じ取引の中で今の行と比べ直す (SHRINK = 409)。
 *     わざと減ったときだけ --force (手で流す・body の force: true で受け口の安全弁も越える)
 *   - Render に 0076 がまだ無い (status が 409 not_migrated) = ⚠️ で exit 0 (マージから migrate (中原さんの指示の後) までの朝を ❌ にしない)
 *   - 取得が古い (36 時間より前 = 想定利益の夜間が止まっている疑い) = 送るが ⚠️ (想定利益の台帳 expected-profit-nightly の見張りとは別に、朝の通知でも分かる)
 * 最後の 1 行が daily-sync の朝の通知に載る。❌ = exit 1 (retry に載る)・⚠️ / ✅ = exit 0
 * env: DATA_DIR / RENDER_MIRROR_URL (RENDER_PORTAL_URL) / MIRROR_SYNC_KEY
 */
import 'dotenv/config';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';
import { postJson, HTTP_TIMEOUT_MS } from './pipeline.mjs';
import { syncBase } from './ne-shipments.mjs';
import Database from 'better-sqlite3';
import { listingRowOf, listingChecksum, bySku, canonPrice, SHOP_CODE, MAX_ROWS } from '../ingest/amazon-listings.mjs';
import { SETTING_AMAZON_SELLER_ID } from '../../expected-profit/db.js';

export const MAX_BODY_BYTES = 11 * 1024 * 1024;   // 受け口の parser は 12MB
export const GUARD_MIN_ROW_RATIO = 0.8;           // 🛑 行が前の回のこの割合を下回ったら送らない
export const SKIP_LIMIT_MIN = 20;                 // 外した行がこれと 2% の大きい方を超えたら送らない
export const SKIP_LIMIT_RATIO = 0.02;
export const STALE_HOURS = 36;                    // 取得がこれより古い = ⚠️
/**
 * 列の見出し → 送る値。日本のマーケットプレイスの今のレポートは日本語の見出し (expected-profit/fetch-listings.js の amazonRowToSnapshot・
 *   AI_reference『商品別想定利益_要件定義』§15-4 の列)・SP-API の説明は英語の見出し (profit-calculator の在庫一覧の取込と同じ候補)。先に見つかった方を使う
 */
export const HEADER_ALIASES = Object.freeze({
  seller_sku: ['出品者SKU', 'seller-sku', 'Seller SKU', 'seller_sku'],
  item_name: ['商品名', 'item-name', 'Item Name', 'item_name'],
  asin: ['asin1', 'ASIN1', 'ASIN 1'],
  product_id: ['商品ID', 'product-id'],         // ASIN の形のときだけ ASIN に使う (JAN・ISBN の行がある)
  open_date: ['出品日', 'open-date', 'Open Date'],
  fulfillment: ['フルフィルメント・チャンネル', 'フルフィルメントチャネル', 'fulfillment-channel', 'Fulfillment Channel'],
  status: ['ステータス', 'status', 'Status'],
  quantity: ['数量', 'quantity', 'Quantity'],
  price: ['価格', 'price', 'Price'],
});
/** 朝の通知で「無い」と知らせる列 (asin は product_id のどちらかがあればよい) */
const WANTED = ['item_name', 'open_date', 'fulfillment', 'status'];
const LABEL = { item_name: '商品名', open_date: '出品日', fulfillment: 'フルフィルメント・チャンネル', status: 'ステータス', asin: 'ASIN' };
const ASIN_RE = /^[A-Z0-9]{10}$/;

/** フルフィルメント・チャンネル → FBA / FBM (expected-profit/fetch-listings.js の amazonFulfillment と同じ。欠損・知らない値を FBM に倒さない) */
export function fulfillmentOf(raw) {
  const s = String(raw ?? '').trim().toUpperCase();
  if (!s) return null;
  if (s.includes('AMAZON') || s.startsWith('AFN')) return 'FBA';
  if (s === 'DEFAULT' || s.startsWith('MFN') || s.includes('MERCHANT')) return 'FBM';
  return null;
}
/** ステータス → active / inactive / incomplete / unknown (fetch-listings.js の amazonListingStatus と同じ) */
export function statusOf(raw) {
  const s = String(raw ?? '').trim().toLowerCase();
  return s === 'active' || s === 'inactive' || s === 'incomplete' ? s : 'unknown';
}
/** 出品日の表記 ('2023-05-12 10:22:33 JST'・'2023/05/12 10:22:33 JST'・'2019-05-14 03:34:53 PDT') → 表記の日付の部分 'YYYY-MM-DD'。読めなければ null (原文は別に送る) */
export function openDateOf(raw) {
  const m = /^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T]|$)/.exec(String(raw ?? '').trim());
  if (!m) return null;
  const d = `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`;
  const t = Date.parse(`${d}T00:00:00Z`);
  return Number.isNaN(t) || new Date(t).toISOString().slice(0, 10) !== d ? null : d;
}
const cut = (v, max) => { const s = String(v ?? '').trim(); return s === '' ? null : s.length > max ? s.slice(0, max) : s; };

/**
 * TSV の原文 → 送る行。戻り値 = { rows (seller SKU の順), skipped: [{ line, reason }], cols: { 値 → 見出し }, missing: [値], header }
 *   見出しに seller SKU の列が無い = 例外 (別の形のレポート)
 */
export function parseListingReportTsv(text) {
  const lines = String(text ?? '').replace(/^\uFEFF/, '').split(/\r?\n/);
  while (lines.length && lines[lines.length - 1].trim() === '') lines.pop();
  if (!lines.length) throw new Error('レポートが空 (見出しも無い)');
  const header = lines[0].split('\t').map((h) => h.trim());
  const cols = {};
  for (const [k, names] of Object.entries(HEADER_ALIASES)) {
    const i = names.map((n) => header.indexOf(n)).find((x) => x >= 0);
    if (i !== undefined) cols[k] = i;
  }
  if (cols.seller_sku === undefined) throw new Error(`見出しに seller SKU の列 (${HEADER_ALIASES.seller_sku.join(' / ')}) が無い = 別の形のレポート: ${header.slice(0, 8).join(' | ').slice(0, 160)}`);
  const missing = WANTED.filter((k) => cols[k] === undefined);
  if (cols.asin === undefined && cols.product_id === undefined) missing.push('asin');
  const rows = [], skipped = [], seen = new Set();
  for (let li = 1; li < lines.length; li++) {
    const line = lines[li];
    if (line.trim() === '') continue;
    const cells = line.split('\t');
    if (cells.length !== header.length) { skipped.push({ line: li + 1, reason: `列の数が ${cells.length} (見出しは ${header.length})` }); continue; }
    const cell = (k) => (cols[k] === undefined ? null : cells[cols[k]]);
    const sku = String(cell('seller_sku') ?? '').trim();
    if (!sku) { skipped.push({ line: li + 1, reason: 'seller SKU が空' }); continue; }
    if (seen.has(sku)) { skipped.push({ line: li + 1, reason: `同じ seller SKU が 2 行目以降: ${sku.slice(0, 60)}` }); continue; }
    const asin1 = cut(cell('asin'), 20);
    const pid = cut(cell('product_id'), 20);
    const asin = asin1 && ASIN_RE.test(asin1.toUpperCase()) ? asin1.toUpperCase() : pid && ASIN_RE.test(pid.toUpperCase()) && /[A-Z]/i.test(pid) ? pid.toUpperCase() : null;
    const qtyRaw = String(cell('quantity') ?? '').trim();
    const priceRaw = String(cell('price') ?? '').trim().replace(/,/g, '');
    const openRaw = cut(cell('open_date'), 60);
    const chRaw = cut(cell('fulfillment'), 60);
    const stRaw = cut(cell('status'), 60);
    const candidate = {
      seller_sku: sku, item_name: cut(cell('item_name'), 2000), asin, open_date: openDateOf(openRaw), open_date_raw: openRaw,
      fulfillment: fulfillmentOf(chRaw), channel_raw: chRaw, status: statusOf(stRaw), status_raw: stRaw,
      quantity: /^\d{1,9}$/.test(qtyRaw) ? Number(qtyRaw) : null, price: canonPrice(priceRaw) === null ? null : priceRaw,
    };
    let row;
    try { row = listingRowOf(candidate); } catch (e) { skipped.push({ line: li + 1, reason: `${sku.slice(0, 40)}: ${e.message}` }); continue; }
    seen.add(sku);
    rows.push(row);
  }
  rows.sort(bySku);
  return { rows, skipped, cols, missing, header };
}

/** 送る回の取得元 = 想定利益の夜間の出品レポート (fetch-listings.js の archiveListings の source) */
export const REPORT_SOURCE = 'merchant_listings_all_data';
/** 受け口の店 (SHOP_CODE = main@A1VC38T7YXB528) の marketplace = 日本 */
export const JP_MARKETPLACE = 'A1VC38T7YXB528';
/** 店 = 日本のマーケットプレイスの <セラーID>@A1VC38T7YXB528 (fetch-listings.js の amazonShopId。セラーID が分からなかった夜の unknown@… は使わない) */
export const SHOP_ID_RE = /^[A-Z0-9]{5,20}@A1VC38T7YXB528$/;
const SELLER_ID_RE = /^[A-Z0-9]{5,20}$/;

/**
 * 主店の shop_id (#1691 Codex R2 M1 = 同じ日本の marketplace の別のセラーの回で主店を置き換えさせない)。
 *   想定利益の夜間 (fetch-listings.js の amazonShopId) と同じ出どころ・同じ順:
 *   セラーID = expected-profit.db の覚え書き (expected_profit_setting の amazon_seller_id・refresh-fees.js の storedSellerId) → 無ければ env SP_API_SELLER_ID /
 *   marketplace = env SP_API_MARKETPLACE_ID → 無ければ日本。🚨 値をハードコードしない・推測しない
 * 戻り値 = { shopId, from: 'setting' | 'env' } / { error } (セラーID が無い・形が違う・DB が読めない・marketplace が日本でない = 送らない)
 */
export function mainShopId({ dataDir, env = process.env, openDb = (file) => new Database(file, { readonly: true, fileMustExist: true }) } = {}) {
  let setting = null;
  const file = dataDir ? path.join(dataDir, 'expected-profit.db') : null;
  if (file && fs.existsSync(file)) {
    let db = null;
    try {
      db = openDb(file);
      const has = db.prepare(`select 1 as x from sqlite_master where type = 'table' and name = 'expected_profit_setting'`).get();
      if (has) setting = db.prepare('select value from expected_profit_setting where key = ?').get(SETTING_AMAZON_SELLER_ID)?.value ?? null;
    } catch (e) {
      return { error: `想定利益の DB (expected-profit.db) のセラーID の覚え書きを読めない = 主店が決まらないので送らない: ${String(e.message).slice(0, 120)}` };
    } finally { try { if (db) db.close(); } catch { /* */ } }
  }
  const seller = setting || (env.SP_API_SELLER_ID ? String(env.SP_API_SELLER_ID).trim() : '') || null;
  if (!seller) return { error: '主店のセラーID が無い (expected-profit.db の覚え書き amazon_seller_id も env SP_API_SELLER_ID も無い) = 推測で選ばないので送らない' };
  if (!SELLER_ID_RE.test(seller)) return { error: `主店のセラーID の形が違う (${String(seller).slice(0, 30)}) = 送らない` };
  const market = String(env.SP_API_MARKETPLACE_ID || JP_MARKETPLACE).trim();
  if (market !== JP_MARKETPLACE) return { error: `env SP_API_MARKETPLACE_ID が日本 (${JP_MARKETPLACE}) でない (${market.slice(0, 30)}) = 受け口の店 (${SHOP_CODE}) と合わないので送らない` };
  return { shopId: `${seller}@${market}`, from: setting ? 'setting' : 'env' };
}
/** manifest の 1 行が送ってよい回か (店・取得元・形式・完走・64 桁の sha256・読める時刻・履歴のフォルダの中のファイル) */
export function usableRecord(r) {
  return !!r && typeof r === 'object' && r.mall === 'amazon' && r.source === REPORT_SOURCE && typeof r.shop_id === 'string' && SHOP_ID_RE.test(r.shop_id)
    && r.complete === true && r.format === 'tsv' && typeof r.file === 'string' && r.file !== '' && !r.file.includes('..') && !path.isAbsolute(r.file)
    && typeof r.sha256 === 'string' && /^[0-9a-f]{64}$/.test(r.sha256) && typeof r.snapshot_at === 'string' && Number.isSafeInteger(Date.parse(r.snapshot_at)) && Date.parse(r.snapshot_at) > 0;
}
/**
 * manifest.jsonl (archive-items.mjs) から送る回を選ぶ: usableRecord で、店が主店 (shopId) と完全に同じ回 (#1691 Codex R2 M1) の中で、
 *   取得の時刻 (snapshot_at) がいちばん新しい回 (manifest の並びに頼らない・Codex R1 M3)。
 * 戻り値 = { rec, prev } (prev = 同じ主店・同じ取得元の 1 つ前の回 = 初回の安全弁の比べる相手) / 無ければ null。壊れた行は飛ばす
 */
export function latestReportRecord(historyDir, shopId) {
  if (typeof shopId !== 'string' || !SHOP_ID_RE.test(shopId)) throw new Error(`主店の shop_id が無い・形が違う: ${String(shopId).slice(0, 40)}`);
  const mf = path.join(historyDir, 'amazon', 'manifest.jsonl');
  if (!fs.existsSync(mf)) return null;
  const recs = [];
  for (const line of fs.readFileSync(mf, 'utf8').split(/\r?\n/)) {
    if (!line.trim()) continue;
    let r; try { r = JSON.parse(line); } catch { continue; }
    if (usableRecord(r) && r.shop_id === shopId) recs.push(r);
  }
  if (!recs.length) return null;
  const at = (r) => Date.parse(r.snapshot_at);
  const rec = recs.reduce((a, b) => (at(b) >= at(a) ? b : a));
  const older = recs.filter((r) => at(r) < at(rec));
  const prev = older.length ? older.reduce((a, b) => (at(b) >= at(a) ? b : a)) : null;
  return { rec, prev };
}

/** 回の gz を開いて原文と sha256 (manifest の sha256 = 原文の sha256) */
export function readReportText(historyDir, rec) {
  const rel = String(rec.file);
  if (typeof rec.sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(rec.sha256)) throw new Error(`manifest の sha256 が無い・形が違う = 中身を確かめられないので送らない: ${rel}`);
  if (rel.includes('..') || path.isAbsolute(rel)) throw new Error(`manifest の file が履歴のフォルダの外を指す: ${rel.slice(0, 80)}`);
  const file = path.join(historyDir, rel);
  if (!fs.existsSync(file)) throw new Error(`manifest にある履歴のファイルが無い: ${rel}`);
  const buf = zlib.gunzipSync(fs.readFileSync(file));
  const sha = crypto.createHash('sha256').update(buf).digest('hex');
  if (sha !== rec.sha256) throw new Error(`履歴のファイルの sha256 が manifest と違う (壊れた・書き換わった): ${rel}`);
  return buf.toString('utf8');
}

const isHex64 = (v) => typeof v === 'string' && /^[0-9a-f]{64}$/.test(v);
const fmtJst = (ms) => { const d = new Date(ms + 9 * 3600e3); return `${d.getUTCMonth() + 1}/${d.getUTCDate()} ${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`; };

/**
 * 送る (本体)。戻り値 = { ok, status, lastLine, rows, skipped, missing, generation, remote, record }
 *   status = 'applied' | 'same' | 'refreshed' | 'unchanged' | 'remote_newer' | 'not_migrated' | 'dry-run' | null。送信の失敗は例外 (CLI が ❌)
 * 差し替え (試験): fetchImpl / sleep / now
 */
export async function pushAmazonListings({ historyDir, shopId, fetchImpl = fetch, base, syncKey, dryRun = false, force = false, log = console.log, sleep, now = () => Date.now() } = {}) {
  if (!base) throw new Error('Render の宛先が無い (RENDER_MIRROR_URL)');
  if (!syncKey) throw new Error('MIRROR_SYNC_KEY が無い');
  const out = { ok: false, status: null, rows: 0, skipped: 0, missing: [], generation: null, remote: null, record: null, lastLine: '', warn: [] };
  let err = null;
  try {
    if (typeof shopId !== 'string' || !SHOP_ID_RE.test(shopId)) throw new Error(`主店の shop_id が決まらない (${String(shopId ?? 'なし').slice(0, 60)}) = 推測で選ばないので送らない`);
    out.shopId = shopId;
    const picked = latestReportRecord(historyDir, shopId);
    if (!picked) throw new Error(`Amazon の出品レポートの履歴が無い (${path.join(historyDir, 'amazon', 'manifest.jsonl')} に 主店 ${shopId}・${REPORT_SOURCE}・complete・TSV・sha256 つきの回が無い = 想定利益の夜間がまだ一度も保存していない)`);
    const { rec, prev } = picked;
    out.record = { file: rec.file, snapshot_at: rec.snapshot_at, run_id: rec.run_id };
    const generation = Date.parse(rec.snapshot_at);
    if (!Number.isSafeInteger(generation) || generation <= 0) throw new Error(`manifest の snapshot_at が日時として読めない: ${String(rec.snapshot_at).slice(0, 40)}`);
    if (typeof rec.run_id !== 'string' || !/^[A-Za-z0-9_.:-]{1,100}$/.test(rec.run_id)) throw new Error(`manifest の run_id が読めない: ${String(rec.run_id).slice(0, 40)}`);
    out.generation = generation;
    if (now() - generation > STALE_HOURS * 3600e3) out.warn.push(`取得が古い (${fmtJst(generation)}・${STALE_HOURS} 時間より前 = 想定利益の夜間が止まっている?)`);
    const parsed = parseListingReportTsv(readReportText(historyDir, rec));
    out.rows = parsed.rows.length; out.skipped = parsed.skipped.length; out.missing = parsed.missing;
    if (parsed.missing.length) out.warn.push(`見出しに無い列: ${parsed.missing.map((k) => LABEL[k] || k).join('・')} (その値は空で送る)`);
    if (parsed.skipped.length) log(`[company-db amazon-listings] 外した行 ${parsed.skipped.length}: ${parsed.skipped.slice(0, 5).map((s) => `${s.line} 行目 ${s.reason}`).join(' / ')}`);
    const skipLimit = Math.max(SKIP_LIMIT_MIN, Math.floor((parsed.rows.length + parsed.skipped.length) * SKIP_LIMIT_RATIO));
    if (parsed.skipped.length > skipLimit) throw new Error(`外した行が多すぎる (${parsed.skipped.length} > ${skipLimit} = レポートの形が変わった疑い・例 ${parsed.skipped.slice(0, 3).map((s) => `${s.line} 行目 ${s.reason}`).join(' / ').slice(0, 160)})`);
    if (!parsed.rows.length) throw new Error('送る行が 0 (取れなかったを「全部の出品が消えた」と読まない)');
    if (parsed.rows.length > MAX_ROWS) throw new Error(`行が多すぎる (${parsed.rows.length} > 受け口の上限 ${MAX_ROWS})`);
    const checksum = listingChecksum(parsed.rows);
    // ① Render の今の回
    const sres = await fetchImpl(`${base}/amazon-listings/status`, { headers: { 'x-sync-key': syncKey }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (sres.status === 409) {
      const j = await sres.json().catch(() => null);
      if (j && j.error === 'not_migrated') { out.status = 'not_migrated'; out.ok = true; return out; }
      throw new Error(`Render の出品一覧の状態が取れない: HTTP 409 ${JSON.stringify(j).slice(0, 160)}`);
    }
    if (!sres.ok) throw new Error(`Render の出品一覧の状態が取れない: HTTP ${sres.status}${sres.status === 404 ? ' (Render がまだ新しい版になっていない?)' : ''} ${(await sres.text()).replace(/\s+/g, ' ').slice(0, 200)}`);
    const sj = await sres.json();
    if (!sj || typeof sj !== 'object' || !Object.hasOwn(sj, 'load')) throw new Error('Render の出品一覧の状態の応答に load が無い');
    const remote = sj.load;
    if (remote !== null && (!remote || !Number.isSafeInteger(remote.generation) || !isHex64(remote.checksum) || !Number.isInteger(remote.row_count))) throw new Error(`Render の出品一覧の状態の形が分からない: ${JSON.stringify(remote).slice(0, 160)}`);
    out.remote = remote;
    if (remote && remote.generation > generation) { out.status = 'remote_newer'; out.ok = true; return out; }
    if (remote && remote.generation === generation && remote.checksum === checksum && remote.row_count === parsed.rows.length && sj.rows === remote.row_count) { out.status = 'unchanged'; out.ok = true; return out; }
    // 🛑 安全弁 (dry-run でも止めて理由を出す)
    // Render に前の回がある = その行の数と比べる (受け口も同じ取引の中で今の行と比べ直す = SHRINK)。
    // 🆕 初回 (Render に前の回が無い) = manifest で守る (#1691 Codex R1 M2): 想定利益の夜間の判定 (enum_status) が ok でない回・同じ店の 1 つ前の回より 20% 以上少ない回は送らない
    const guard = remote ? (parsed.rows.length < remote.row_count * GUARD_MIN_ROW_RATIO ? `行が前の回 ${remote.row_count} の ${GUARD_MIN_ROW_RATIO * 100}% 未満 (${parsed.rows.length})` : null)
      : rec.enum_status !== 'ok' ? `初回なのに、想定利益の夜間の判定 (enum_status) が ok でない (${String(rec.enum_status ?? 'なし').slice(0, 20)} = 前の夜より出品が大きく減った・読めない行が多い)`
      : prev && Number.isInteger(prev.items) && Number.isInteger(rec.items) && rec.items < prev.items * GUARD_MIN_ROW_RATIO ? `初回なのに、1 つ前の取得 ${prev.items} 行の ${GUARD_MIN_ROW_RATIO * 100}% 未満 (${rec.items})` : null;
    if (guard && (!force || dryRun)) throw new Error(`🛑 安全弁: ${guard} = 送らない (既存の行を消さない)。レポートを確かめ、わざとなら手で --force`);
    if (guard) out.warn.push(`--force で安全弁を越えた: ${guard}`);
    const body = { shop_code: SHOP_CODE, generation, snapshot_at: new Date(generation).toISOString(), report_run_id: rec.run_id, checksum, row_count: parsed.rows.length, skipped_count: parsed.skipped.length, rows: parsed.rows, ...(force ? { force: true } : {}) };
    const bytes = Buffer.byteLength(JSON.stringify(body));
    if (bytes > MAX_BODY_BYTES) throw new Error(`送る中身が大きすぎる (${bytes} バイト > ${MAX_BODY_BYTES})`);
    if (dryRun) { out.status = 'dry-run'; out.ok = true; return out; }
    // ② 送る (5xx・通信の失敗は同じ body で再送 = same になる。4xx は例外)
    const res = await postJson(fetchImpl, { base, syncKey, path: '/amazon-listings', log, ...(sleep ? { sleep } : {}), body });
    if (!res || !['applied', 'same', 'refreshed', 'stale'].includes(res.status)) throw new Error(`受け口の応答が分からない: ${JSON.stringify(res).slice(0, 160)}`);
    if (res.checksum !== checksum || res.generation !== generation) throw new Error(`受け口の応答 (世代 ${res.generation} / 指紋 ${String(res.checksum).slice(0, 16)}…) が送った内容と違う`);
    out.status = res.status;
    if (res.status === 'stale') { out.status = 'remote_newer'; out.ok = true; return out; }
    if (res.status === 'applied' && res.rows !== parsed.rows.length) throw new Error(`受け口が入れた行数 ${res.rows} が、送った行数 ${parsed.rows.length} と違う`);
    out.ok = true;
    return out;
  } catch (e) {
    err = e; out.ok = false; out.error = e.message;
    throw e;
  } finally {
    const when = out.generation ? `取得 ${fmtJst(out.generation)}` : '';
    const tail = `${when} / 行 ${out.rows}${out.skipped ? ` (外した行 ${out.skipped})` : ''}`;
    const warn = out.warn.length ? ` ⚠️ ${out.warn.join(' / ')}` : '';
    const mark = out.warn.length ? '⚠️' : '✅';
    out.lastLine = err ? `❌ Company DB 出品一覧 (Amazon): ${String(err.message).replace(/\s+/g, ' ').slice(0, 300)}`
      : out.status === 'not_migrated' ? '⚠️ Company DB 出品一覧 (Amazon): Render に migration 0076 がまだ無い = 送らない (中原さんの指示で migrate した後に送る)'
      : out.status === 'dry-run' ? `dry-run: 送る予定 ${tail} / Render の今の回 ${out.remote ? fmtJst(out.remote.generation) : 'なし'}${warn} [dry-run = 送っていない]`
      : out.status === 'unchanged' ? `${mark} Company DB 出品一覧 (Amazon): 変わりなし (Render と同じ取得) ${tail}${warn}`
      : out.status === 'remote_newer' ? `⚠️ Company DB 出品一覧 (Amazon): Render の方が新しい取得 (${out.remote ? fmtJst(out.remote.generation) : '?'}) = 送らない ${tail}`
      : `${mark} Company DB 出品一覧 (Amazon): ${out.status === 'applied' ? '入れ替えた' : out.status === 'refreshed' ? '中身は同じ (取得の時刻だけ進めた)' : '送り直し (same)'} ${tail}${warn}`;
    if (err) err.result = out;   // CLI が要約の 1 行を出せるように
  }
}

export function parseArgs(argv) {
  const out = { send: false, dryRun: false, force: false, historyDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--send') out.send = true;
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--force') out.force = true;
    else if (a === '--history-dir') { const v = argv[++i]; if (v === undefined || v.startsWith('--')) throw new Error('--history-dir に値が無い'); out.historyDir = v; }
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.send === out.dryRun) throw new Error('--send か --dry-run のどちらか 1 つを付ける');
  if (out.force && out.dryRun) throw new Error('--force は --send と一緒にだけ使う (dry-run は安全弁で止まって理由を出す)');
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = process.env.DATA_DIR || path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', '..', 'data');
    const historyDir = a.historyDir || path.join(dataDir, 'mall-items-history');
    let r;
    // 主店 = 想定利益の夜間と同じ出どころ (expected-profit.db の覚え書き → env)。決まらない = shopId に理由を入れて ❌ (pushAmazonListings が断る)
    const shop = mainShopId({ dataDir });
    if (shop.error) throw new Error(shop.error);
    try { r = await pushAmazonListings({ historyDir, shopId: shop.shopId, base: syncBase(), syncKey: process.env.MIRROR_SYNC_KEY || '', dryRun: a.dryRun, force: a.force }); }
    catch (e) { r = e.result || { ok: false, lastLine: `❌ Company DB 出品一覧 (Amazon): ${String(e.message).replace(/\s+/g, ' ').slice(0, 300)}` }; }
    console.log(String(r.lastLine).replace(/\s+/g, ' '));   // 最後の 1 行を複数行にしない (daily-sync が朝の通知に載せる)
    code = r.ok ? 0 : 1;
  } catch (e) {
    console.log(`❌ Company DB 出品一覧 (Amazon): ${String(e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
  }
  // 🚨 fetch の直後に process.exit() しない (Windows の Node で終了コード 127。stock-daily.mjs と同じ)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
