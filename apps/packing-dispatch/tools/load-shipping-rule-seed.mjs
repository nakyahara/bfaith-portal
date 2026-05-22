/**
 * 商品配送ルール(②)の初期投入: seed/shipping_rule_seed.json を pd_shipping_rule へ投入。
 * 現 Excel「NEW_受注明細行...xlsx」を正規化したシード(商品×モール×数量帯)。冪等(upsertは全置換)。
 *
 * 実行 (Render shell / DATA_DIR 設定済の環境):
 *   node apps/packing-dispatch/tools/load-shipping-rule-seed.mjs
 */
import fs from 'fs';
import zlib from 'zlib';
import path from 'path';
import { fileURLToPath } from 'url';
import { initMirrorDB } from '../../warehouse-mirror/db.js';
import { ensureSchema } from '../db.js';
import { upsertRules } from '../service.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SEED = path.join(__dirname, '..', 'seed', 'shipping_rule_seed.json.gz');

export function loadSeed() {
  initMirrorDB();
  ensureSchema();
  const seed = JSON.parse(zlib.gunzipSync(fs.readFileSync(SEED)).toString('utf8'));
  let ok = 0, ng = 0; const errs = [];
  const t0 = Date.now();
  for (const e of seed) {
    try {
      upsertRules({ product_code: e.product_code, mall_group: e.mall_group, tiers: e.tiers }, 'seed');
      ok++;
    } catch (err) {
      ng++; if (errs.length < 30) errs.push(`${e.product_code}/${e.mall_group}: ${err.message}`);
    }
  }
  return { ok, ng, errs, ms: Date.now() - t0, total: seed.length };
}

// 直接実行された場合のみロード (import 時には実行しない)
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const r = loadSeed();
  console.log(`[seed] 投入 ${r.ok} / 失敗 ${r.ng} / 全 ${r.total} 件 (${r.ms}ms)`);
  for (const e of r.errs) console.log('  ✗', e);
  process.exit(r.ng > 0 ? 1 : 0);
}
