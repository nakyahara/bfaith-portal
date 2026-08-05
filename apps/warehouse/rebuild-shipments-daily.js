/**
 * rebuild-shipments-daily — 日次出荷サマリ (f_shipments_daily) の再構築
 *
 * raw_ne_order_base (伝票粒度) から「出荷日 × 店舗 × 配送方法」の伝票件数を集計する。
 * 件数の定義は **伝票 = 発送 1 件** (中原さん 2026-08-05 指示)。商品点数ではない。
 *
 * ・Amazon Easy Ship は NE 側の配送方法名が 'AES' なので、店舗=Amazon × 配送方法=AES で数えられる
 *   (Amazon でも AES 以外の便が数%あるため、モール軸だけでは AES 件数にならない)
 * ・slips = 出荷確定した伝票すべての件数。cancelled_slips は **その内数** で、
 *   出荷確定日が入っているのに現在キャンセル扱いになっている伝票 (実測: 直近1年で
 *   79件 / 30.4万件 = 0.026%)。「伝票1件=発送1件」の定義に slips をそのまま合わせ、
 *   キャンセルを除いた数が要るときは slips - cancelled_slips で引く (Codex R2 high)
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
    else if (argv[i] === '--days') {
      // parseInt(x) || 30 だと --days abc / --days 0 が黙って既定値になる (Codex R2 medium)
      const raw = argv[++i];
      const n = Number(raw);
      if (!Number.isInteger(n) || n <= 0) throw new Error(`--days が不正です: ${raw} (1以上の整数)`);
      out.days = n;
    } else if (argv[i] === '--from') out.from = argv[++i];
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

  // 出荷確定日は 'YYYY-MM-DD HH:MM:SS'。
  // WHERE に substr() を使うと idx_ne_base_send が効かず毎回フルスキャンになるため
  // 文字列の範囲比較で書く (Codex R1 medium)。'YYYY-MM-DD ...' は辞書順 = 時系列順。
  const selectSql = `
    SELECT substr(出荷確定日, 1, 10)                    AS ship_date,
           COALESCE(店舗コード, '')                      AS shop_code,
           COALESCE(配送方法ID, '')                      AS delivery_id,
           COUNT(*)                                      AS slips,
           SUM(CASE WHEN キャンセル区分 = 'キャンセル' THEN 1 ELSE 0 END) AS cancelled_slips
      FROM raw_ne_order_base
     WHERE 出荷確定日 >= ? AND 出荷確定日 < ?
     GROUP BY ship_date, shop_code, delivery_id
  `;

  // 配送方法IDごとの「最新の名称」。同じIDに複数の名称が現れたとき MAX(辞書順) を採ると
  // 最新でも代表でもない名前が出る (Codex R1 medium)。出荷確定日が一番新しい行の名称を正とする。
  // 相関サブクエリだと 30 万伝票 × 再検索で毎朝の --all が重くなるため、window 関数で 1 スキャンにする。
  // 同着 (同じ出荷確定日) のときに名前が揺れないよう 伝票番号 をタイブレーカーに入れる (Codex R2)。
  const nameSql = `
    SELECT delivery_id, delivery_name FROM (
      SELECT COALESCE(配送方法ID, '') AS delivery_id,
             COALESCE(NULLIF(配送方法名, ''), '(未設定)') AS delivery_name,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(配送方法ID, '')
               ORDER BY 出荷確定日 DESC, 伝票番号 DESC
             ) AS rn
        FROM raw_ne_order_base
       WHERE 出荷確定日 <> ''
    ) WHERE rn = 1
  `;

  // '' は '0000-01-01' より小さいので、空文字 (=未出荷) は範囲比較で自然に除外される。
  const lo = `${from} 00:00:00`;
  const hiExclusive = `${to} 99`; // 'YYYY-MM-DD 23:59:59' < 'YYYY-MM-DD 99' (数字 < '9')

  const tx = db.transaction(() => {
    const rows = db.prepare(selectSql).all(lo, hiExclusive);
    const nameMap = new Map(db.prepare(nameSql).all().map(r => [r.delivery_id, r.delivery_name]));
    db.prepare('DELETE FROM f_shipments_daily WHERE ship_date >= ? AND ship_date <= ?').run(from, to);
    const ins = db.prepare(`
      INSERT INTO f_shipments_daily
        (ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    let slips = 0;
    for (const r of rows) {
      ins.run(r.ship_date, r.shop_code, r.delivery_id,
        nameMap.get(r.delivery_id) || '(未設定)', r.slips, r.cancelled_slips, updatedAt);
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
