/**
 * backfill-ne-order-base — NE 受注ベース (伝票粒度 + 配送方法) の過去分バックフィル
 *
 * 出荷確定日ベースで月ごとに区切って NE API を叩き、raw_ne_order_base を埋める。
 * 埋め終わったら f_shipments_daily を全期間再構築する。
 *
 * API 消費量 (2026-08-05 実測): 直近1年の出荷確定伝票 = 304,239 件。
 *   1 リクエスト最大 10,000 件なので 1年で 35〜40 回程度 (NE 無料枠は月 1,000 回)。
 *
 * 使い方:
 *   node apps/warehouse/backfill-ne-order-base.js --months 12       → 直近12ヶ月 (既定)
 *   node apps/warehouse/backfill-ne-order-base.js --from 2025-01-01 --to 2025-12-31
 *   node apps/warehouse/backfill-ne-order-base.js --months 12 --dry-run  → 対象月と件数だけ表示
 *
 * 冪等: 同じ期間を何度流しても UPSERT なので二重計上しない (変化が無ければ unchanged)。
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { initDB, getDB } from './db.js';
import { fetchOrderBaseRange } from './ne-api.js';
import { rebuildShipmentsDaily } from './rebuild-shipments-daily.js';

function jstDate(offsetDays = 0) {
  const t = Date.now() + 9 * 3600 * 1000 + offsetDays * 86400 * 1000;
  return new Date(t).toISOString().slice(0, 10);
}

function parseArgs(argv) {
  const out = { months: 12, from: null, to: null, dryRun: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--months') out.months = parseInt(argv[++i]) || 12;
    else if (argv[i] === '--from') out.from = argv[++i];
    else if (argv[i] === '--to') out.to = argv[++i];
    else if (argv[i] === '--dry-run') out.dryRun = true;
  }
  return out;
}

/**
 * 日付・期間の検証。不正な日付のまま splitMonths に入れると NaN で終了条件が壊れ、
 * NE API を無限に叩きかねない (Codex R1 low)。実行前に必ず落とす。
 */
export function validateRange(from, to, months) {
  const isDate = (s) => typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s)
    && !Number.isNaN(Date.parse(`${s}T00:00:00Z`))
    && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s; // 2026-02-31 等を弾く
  if (!isDate(from)) throw new Error(`--from が不正です: ${from} (YYYY-MM-DD)`);
  if (!isDate(to)) throw new Error(`--to が不正です: ${to} (YYYY-MM-DD)`);
  if (from > to) throw new Error(`--from が --to より後です: ${from} > ${to}`);
  if (!Number.isFinite(months) || months <= 0 || months > 120) {
    throw new Error(`--months が不正です: ${months} (1〜120)`);
  }
}

/** [from, to] を月単位の区間に割る。返り値は 'YYYY-MM-DD' の組 */
export function splitMonths(from, to) {
  const out = [];
  let [y, m] = from.split('-').map(Number);
  while (true) {
    const first = `${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}-01`;
    const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate(); // m は 1始まり → 翌月0日 = 当月末
    const last = `${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`;
    const s = first < from ? from : first;
    const e = last > to ? to : last;
    if (s > to) break;
    out.push([s, e]);
    if (e >= to) break;
    m++;
    if (m > 12) { m = 1; y++; }
  }
  return out;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const to = opts.to || jstDate(0);
  const from = opts.from || (() => {
    const d = new Date(Date.now() + 9 * 3600 * 1000);
    d.setUTCMonth(d.getUTCMonth() - opts.months);
    return d.toISOString().slice(0, 10);
  })();

  validateRange(from, to, opts.months);

  const months = splitMonths(from, to);
  console.log(`[バックフィル] 出荷確定日 ${from} 〜 ${to} を ${months.length} ヶ月に分割して取得します`);
  console.log(`[バックフィル] 想定 API 回数: 最低 ${months.length} 回 (1区間1万件を超えた月は日付で二分割されて増えます。NE 無料枠は月1,000回)`);
  if (opts.dryRun) {
    for (const [s, e] of months) console.log(`  ${s} 〜 ${e}`);
    console.log('[バックフィル] --dry-run のため API は呼びません');
    return;
  }

  await initDB();
  const db = getDB();

  let totalFetched = 0;
  let totalInserted = 0;
  let totalUpdated = 0;
  for (const [s, e] of months) {
    const r = await fetchOrderBaseRange(`${s} 00:00:00`, `${e} 23:59:59`, { updateMeta: false });
    totalFetched += r.fetched;
    totalInserted += r.inserted;
    totalUpdated += r.updated;
    console.log(`[バックフィル] ${s} 〜 ${e}: ${r.fetched}件 (挿入 ${r.inserted} / 更新 ${r.updated})`);
  }

  console.log(`[バックフィル] 取得完了: ${totalFetched}件処理, ${totalInserted}件挿入, ${totalUpdated}件更新`);

  const rb = rebuildShipmentsDaily(db, { from, to });
  console.log(`[バックフィル] 出荷サマリ再構築: ${rb.from} 〜 ${rb.to} → ${rb.rows}行 (有効伝票 ${rb.slips}件)`);
}

const isDirectRun = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isDirectRun) {
  main().catch(e => {
    console.error('[バックフィル] エラー:', e.message);
    process.exit(1);
  });
}
