#!/usr/bin/env node
/**
 * shipping-map-report.mjs — 送料コード (Company DB の発送方法) → product-hub の楽天の配送方法の対応を、本番の値で数える (読むだけ・Company DB構想 14 ⑤-2a・仮レビュー L4)
 *
 * なぜ: 新商品のカードの楽天の配送方法は「送料の表 (mirror_shipping_rates) の小分類区分名称」を ph_shipping_method_map.ne_label で引く
 *   (apps/product-hub/services/cdb-card-intake.js の mapCdbShipping)。ne_label は mirror_products.配送方法 から人が対応を決めた名前 =
 *   送料の表の名前と同じ書き方かは本番のデータで確かめていない。対応の無い送料コードで登録すると、カードは「⚠ 発送方法 要確認」になる
 * 出すもの:
 *   1. 送料コードごと: 方法の名前・対応 (楽天の配送方法グループ / 対応なし / 使えないグループ)・その名前の商品の数 (mirror_products.配送方法)
 *   2. 対応表 (ph_shipping_method_map) にあるのに送料の表に無い名前 (書き方の違いの候補)
 *   3. まとめ: 送料コード N 個のうち対応あり M・対応なし K
 * 🚨 読むだけ (SQLite を readonly で開く)。本番 (Render の DATA_DIR/warehouse-mirror.db) で流すのは中原さんの OK の後
 * 使い方: DATA_DIR=... node apps/product-hub/scripts/shipping-map-report.mjs [--json]
 */
import path from 'node:path';
import Database from 'better-sqlite3';
import { SHIPPING_METHOD_GROUPS, ALL_SHIPPING_METHOD_GROUPS } from '../lib/shipping-groups.js';

/** db = better-sqlite3 (読むだけで開いたもの)。返り値 { rows, orphanLabels, summary } */
export function shippingMapReport(db) {
  const has = (t) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(t);
  if (!has('mirror_shipping_rates')) throw new Error('mirror_shipping_rates が無い (送料の表がまだ写っていない)');
  const map = new Map(has('ph_shipping_method_map') ? db.prepare('SELECT ne_label, rakuten_group FROM ph_shipping_method_map').all().map((r) => [r.ne_label, r.rakuten_group]) : []);
  const used = new Map(has('mirror_products')
    ? db.prepare("SELECT 配送方法 AS label, COUNT(*) AS n FROM mirror_products WHERE 配送方法 IS NOT NULL AND TRIM(配送方法) != '' GROUP BY 配送方法").all().map((r) => [r.label, r.n]) : []);
  const rows = db.prepare('SELECT shipping_code AS code, 小分類区分名称 AS method FROM mirror_shipping_rates ORDER BY shipping_code').all().map((r) => {
    const method = String(r.method ?? '').trim();
    const g = method ? String(map.get(method) ?? '').trim() : '';
    const status = !method ? 'no_method' : !g ? 'unmapped' : SHIPPING_METHOD_GROUPS[g] ? 'mapped' : ALL_SHIPPING_METHOD_GROUPS[g] ? 'unusable_group' : 'unknown_group';
    return { code: String(r.code), method: method || null, status, group: g || null, group_label: g ? (ALL_SHIPPING_METHOD_GROUPS[g] ?? null) : null, products: method ? (used.get(method) ?? 0) : 0 };
  });
  const methods = new Set(rows.map((r) => r.method).filter(Boolean));
  const orphanLabels = [...map.keys()].filter((l) => !methods.has(l)).map((l) => ({ label: l, group: map.get(l), products: used.get(l) ?? 0 }));
  const count = (s) => rows.filter((r) => r.status === s).length;
  return { rows, orphanLabels, summary: { codes: rows.length, mapped: count('mapped'), unmapped: count('unmapped'), unusable_group: count('unusable_group'), unknown_group: count('unknown_group'), no_method: count('no_method') } };
}

const isMain = process.argv[1] && /shipping-map-report\.mjs$/i.test(process.argv[1]);
if (isMain) {
  const dir = process.env.DATA_DIR;
  if (!dir) { console.error('DATA_DIR が要る (warehouse-mirror.db のある場所)'); process.exit(2); }
  const db = new Database(path.join(dir, 'warehouse-mirror.db'), { readonly: true, fileMustExist: true });
  try {
    const r = shippingMapReport(db);
    if (process.argv.includes('--json')) { console.log(JSON.stringify(r, null, 1)); } else {
      const L = { mapped: '対応あり', unmapped: '対応なし (要確認になる)', unusable_group: '使えないグループに対応', unknown_group: '知らないグループに対応', no_method: '名前が空' };
      for (const x of r.rows) console.log(`${x.code}\t${x.method ?? '—'}\t${L[x.status]}${x.group ? ` → ${x.group} ${x.group_label ?? ''}` : ''}\t商品 ${x.products}`);
      if (r.orphanLabels.length) {
        console.log('\n対応表にあるのに送料の表に無い名前 (書き方の違いの候補):');
        for (const o of r.orphanLabels) console.log(`\t${o.label} → ${o.group ?? '—'}\t商品 ${o.products}`);
      }
      const s = r.summary;
      console.log(`\n送料コード ${s.codes}: 対応あり ${s.mapped}・対応なし ${s.unmapped}・使えないグループ ${s.unusable_group}・知らないグループ ${s.unknown_group}・名前が空 ${s.no_method}`);
    }
  } finally { db.close(); }
}
