/**
 * verify-against-production.mjs — 展開ロジックを本番の過去受注で検証する
 *
 * 「選べるセット」の商品OPは モール・セットごとに書式が違い、机上では穴が見つからない。
 * warehouse.db の raw_ne_orders.商品OP に過去の全モール分が入っているので、
 * 実装を変えたら必ずここに通してから本番に出す。
 *
 * 実行 (miniPC / RMS認証と warehouse.db がある環境):
 *   cd C:\Users\bfaith\bfaith-portal && node apps/select-set/tests/verify-against-production.mjs
 *
 * env: WAREHOUSE_DB (既定 ./data/warehouse.db) / RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY
 */
import 'dotenv/config';
import Database from 'better-sqlite3';
import path from 'path';
import { rakutenRequest } from '../../warehouse/rakuten-client.js';
import { buildCatalog, expandOp } from '../expand.js';
import { MANUAL_MAPPINGS, OMAKE_PRIORITY, SET_CODES } from '../seed-data.js';

const DB_FILE = process.env.WAREHOUSE_DB || path.join(process.cwd(), 'data', 'warehouse.db');
const db = new Database(DB_FILE, { readonly: true });

const known = new Set(
  db.prepare('SELECT 商品コード AS c FROM raw_ne_products').all()
    .map((r) => String(r.c || '').trim().toLowerCase()).filter(Boolean)
);
console.log(`NE商品マスタ: ${known.size}件  (DB=${DB_FILE})`);

const stockRows = new Map();
for (const r of db.prepare('SELECT 商品コード AS c, 商品名 AS n, 在庫数 AS z, 引当数 AS h FROM raw_ne_products').all()) {
  stockRows.set(String(r.c || '').toLowerCase(), { name: r.n, available: (Number(r.z) || 0) - (Number(r.h) || 0) });
}
const stockOf = (code) => stockRows.get(String(code).toLowerCase()) || null;

// ---- セットごとに選択肢定義を組み立てる ----
const catalogs = new Map();
for (const setCode of SET_CODES) {
  let rmsOptions = [];
  try {
    const r = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${encodeURIComponent(setCode)}` });
    rmsOptions = (r?.body ?? r?.data ?? r)?.customizationOptions || [];
  } catch (e) {
    console.log(`  ⚠ ${setCode}: RMS取得に失敗 (${String(e.message || e).slice(0, 80)}) → 手動分のみで続行`);
  }
  const cat = buildCatalog({
    setCode,
    rmsOptions,
    manualRows: MANUAL_MAPPINGS[setCode] || [],
    knownProductCodes: known,
  });
  catalogs.set(setCode, cat);
  console.log(`  ${setCode}: RMS枠${rmsOptions.length} / 選択肢${cat.options.size} / SKU${cat.skus.length}`
    + (cat.unresolved.length ? ` / ⚠RMSで解決できず${cat.unresolved.length}` : '')
    + (cat.conflicts.length ? ` / 🚨衝突${cat.conflicts.length}` : ''));
  for (const u of [...new Set(cat.unresolved)]) console.log(`      RMSで商品コードを特定できない選択肢: "${u}"`);
}

// ---- 過去の受注を全件流す ----
const rows = db.prepare(`
  SELECT 店舗コード AS shop, 商品コード AS setCode, 商品OP AS op, 受注数 AS qty, COUNT(*) AS cnt
  FROM raw_ne_orders
  WHERE 商品コード IN (${SET_CODES.map(() => '?').join(',')}) AND 商品OP IS NOT NULL AND 商品OP <> ''
  GROUP BY 店舗コード, 商品コード, 商品OP, 受注数
`).all(...SET_CODES);

const stat = new Map();
const failures = new Map();
let total = 0, ok = 0;
for (const r of rows) {
  const cat = catalogs.get(r.setCode);
  const res = expandOp({
    catalog: cat,
    op: r.op,
    quantity: r.qty,
    omakePriority: OMAKE_PRIORITY,
    stockOf,
  });
  const s = stat.get(r.setCode) || { total: 0, ok: 0 };
  s.total += r.cnt;
  total += r.cnt;
  if (res.ok) { s.ok += r.cnt; ok += r.cnt; }
  else {
    for (const w of res.warnings) {
      const k = `${r.setCode}||${w}`;
      failures.set(k, (failures.get(k) || 0) + r.cnt);
    }
  }
  stat.set(r.setCode, s);
}

console.log('\n===== セット別 =====');
for (const [setCode, s] of stat) {
  console.log(`  ${setCode}: ${s.ok}/${s.total} (${((s.ok / s.total) * 100).toFixed(2)}%)`);
}
console.log(`\n===== 合計: ${ok}/${total} (${((ok / total) * 100).toFixed(2)}%) =====`);

const fails = [...failures.entries()].sort((a, b) => b[1] - a[1]);
console.log(`\n===== 失敗の内訳 (${fails.length}種類) =====`);
for (const [k, n] of fails.slice(0, 40)) {
  const [setCode, msg] = k.split('||');
  console.log(`  ${n}件  [${setCode}]  ${msg}`);
}

// ---- おまけ選定のサンプル ----
console.log('\n===== おまけ選定の確認 =====');
const sample = rows.find((r) => /selectwa10-3/.test(r.setCode) && /プレゼント|おまけ/.test(r.op));
if (sample) {
  const res = expandOp({
    catalog: catalogs.get(sample.setCode), op: sample.op, quantity: sample.qty,
    omakePriority: OMAKE_PRIORITY, stockOf,
  });
  console.log(`  op: ${sample.op.slice(0, 140)}`);
  console.log(`  明細: ${res.lines.map((l) => `${l.code}×${l.quantity}`).join(', ')}`);
  if (res.omake) {
    for (const c of res.omake.candidates) {
      console.log(`   おまけ候補 ${c.chosen ? '▶' : ' '} ${c.code} 利用可能${c.available}  ${c.name}`);
    }
  }
} else {
  console.log('  おまけ付きのサンプルが見つかりませんでした');
}
db.close();
