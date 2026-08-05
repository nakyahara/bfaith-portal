/**
 * rebuild-shipments-daily — 日次出荷サマリ (f_shipments_daily) の再構築
 *
 * raw_ne_order_base (伝票粒度) から「出荷日 × 店舗 × 配送方法」の伝票件数を集計する。
 * 件数の定義は **伝票 = 発送 1 件** (中原さん 2026-08-05 指示)。商品点数ではない。
 *
 * ・Amazon Easy Ship は NE 側の配送方法名が 'AES' なので、店舗=Amazon × 配送方法=AES で数えられる
 *   (Amazon でも AES 以外の便が数%あるため、モール軸だけでは AES 件数にならない)
 * ・キャンセルは除外せず cancelled_slips として別に持つ (どちらで見るかは画面側で選ぶ)
 * ・期間内を DELETE → INSERT で置換。1トランザクションなので途中経過が見えることはない
 *
 * 使い方:
 *   node apps/warehouse/rebuild-shipments-daily.js              → 直近30日を再構築
 *   node apps/warehouse/rebuild-shipments-daily.js --days 90    → 直近90日
 *   node apps/warehouse/rebuild-shipments-daily.js --all        → 全期間 (バックフィル直後用)
 *   node apps/warehouse/rebuild-shipments-daily.js --from 2025-08-01 --to 2026-08-04
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { initDB, getDB } from './db.js';

function jstDate(offsetDays = 0) {
  const t = Date.now() + 9 * 3600 * 1000 + offsetDays * 86400 * 1000;
  return new Date(t).toISOString().slice(0, 10);
}

function parseArgs(argv) {
  const out = { all: false, days: 30, from: null, to: null };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--all') out.all = true;
    else if (argv[i] === '--days') out.days = parseInt(argv[++i]) || 30;
    else if (argv[i] === '--from') out.from = argv[++i];
    else if (argv[i] === '--to') out.to = argv[++i];
  }
  return out;
}

/**
 * @param {import('better-sqlite3').Database} db
 * @param {{ all?: boolean, days?: number, from?: string|null, to?: string|null }} opts
 * @returns {{ from: string, to: string, rows: number, slips: number }}
 */
export function rebuildShipmentsDaily(db, opts = {}) {
  const all = !!opts.all;
  const from = all ? '0000-01-01' : (opts.from || jstDate(-((opts.days || 30) - 1)));
  const to = all ? '9999-12-31' : (opts.to || jstDate(0));
  const updatedAt = new Date().toISOString();

  // 出荷確定日は 'YYYY-MM-DD HH:MM:SS'。日付部分だけで窓を切る。
  const selectSql = `
    SELECT substr(出荷確定日, 1, 10)                    AS ship_date,
           COALESCE(店舗コード, '')                      AS shop_code,
           COALESCE(配送方法ID, '')                      AS delivery_id,
           MAX(COALESCE(NULLIF(配送方法名, ''), '(未設定)')) AS delivery_name,
           SUM(CASE WHEN キャンセル区分 = 'キャンセル' THEN 0 ELSE 1 END) AS slips,
           SUM(CASE WHEN キャンセル区分 = 'キャンセル' THEN 1 ELSE 0 END) AS cancelled_slips
      FROM raw_ne_order_base
     WHERE 出荷確定日 IS NOT NULL AND 出荷確定日 <> ''
       AND substr(出荷確定日, 1, 10) >= ?
       AND substr(出荷確定日, 1, 10) <= ?
     GROUP BY ship_date, shop_code, delivery_id
  `;

  const tx = db.transaction(() => {
    const rows = db.prepare(selectSql).all(from, to);
    db.prepare('DELETE FROM f_shipments_daily WHERE ship_date >= ? AND ship_date <= ?').run(from, to);
    const ins = db.prepare(`
      INSERT INTO f_shipments_daily
        (ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    let slips = 0;
    for (const r of rows) {
      ins.run(r.ship_date, r.shop_code, r.delivery_id, r.delivery_name, r.slips, r.cancelled_slips, updatedAt);
      slips += r.slips;
    }
    return { rows: rows.length, slips };
  });

  const res = tx();
  return { from, to, ...res };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  await initDB();
  const db = getDB();
  const r = rebuildShipmentsDaily(db, opts);
  console.log(`[出荷サマリ] ${r.from} 〜 ${r.to}: ${r.rows}行 (有効伝票 ${r.slips}件) を再構築`);
}

const isDirectRun = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isDirectRun) {
  main().catch(e => {
    console.error('[出荷サマリ] エラー:', e.message);
    process.exit(1);
  });
}
