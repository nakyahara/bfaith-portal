/**
 * test-master-legacy-entries.mjs — 古い入口の一覧 (config/master-legacy-entries.mjs) が、コードのルートと CLI を取りこぼしていないかを数える
 * (Company DB構想 14 §9 v2 M2「ルートを数える試験」・§10 契約 v3 H1)
 *
 *   1. apps/ の全部のルートの定義 (router.post / put / patch / delete / all) を探し、その定義から次のルートまでの間で
 *      マスタの表 (MASTER_WRITE_TARGETS.tables) に書く SQL か、マスタを書く関数・欄 (calls) が出てくるルートは、
 *      LEGACY_ENTRIES (閉じる) か LEGACY_NOT_CLOSED (閉じない・理由つき) に載っていなければ落とす
 *      = 新しい書き込みの入口を黙って足せない
 *   2. 一覧のルート・画面が、そのファイルに本当にある (古い行が残っていない)
 *   3. ルート以外でマスタの表に書くファイル (CLI・作り直し) は、CLI の入口か NON_ENTRY_WRITERS (人の入口ではない) に載っている
 *   4. csv-import.js の mode は全部、閉じる (LEGACY_ENTRIES) か止めない (CLI_KEEP_MODES) のどちらかに分けてある
 *   5. 門が実際に掛かっている: 一覧の app ごとに router のファイルが masterLegacyGate('<app>') を使い、CLI のファイルが legacyCliGate を使う
 *
 * 使い方: node scripts/test-master-legacy-entries.mjs (DB もネットも使わない。ファイルを読むだけ)
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { LEGACY_ENTRIES, LEGACY_NOT_CLOSED, CLI_KEEP_MODES, MASTER_WRITE_TARGETS, WHEN_FROZEN, LEGACY_GATES_VERSION } from '../config/master-legacy-entries.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let failed = 0, passed = 0;
const ok = (cond, label, detail = '') => { if (cond) { passed++; console.log(`  ✓ ${label}`); } else { failed++; console.log(`  ✗ ${label}${detail ? `\n      ${detail}` : ''}`); } };

/** ルートでも CLI でもない = 人の入口ではない、マスタの表の書き手 (理由つき) */
const NON_ENTRY_WRITERS = {
  'apps/warehouse/rebuild-m-products.js': '毎朝の m_products の作り直し (上書き表と NE から導く。人の入口ではない。切替後は ④ の写し)',
  'apps/warehouse/record-m-products-history.js': 'm_products の履歴の記録 (作り直しの後)',
  'apps/warehouse/db.js': '表の定義 (CREATE / 移行)',
  'apps/warehouse-mirror/db.js': '表の定義 (CREATE / 移行)',
  'apps/warehouse-mirror/router.js': 'miniPC → Render の写しの受け口 (/api/sync は LEGACY_NOT_CLOSED)',
  'apps/supplier-sales/share-db.js': 'upsertSupplierName の実体 (呼ぶルートは LEGACY_NOT_CLOSED の supplier-sales:POST /api/supplier-name)',
  'apps/purchase-orders/db.js': '発注アプリの表の定義と書き手 (仕入先タブのルートは LEGACY_NOT_CLOSED = 切替の手順で書き込み先を差し替える)',
  'apps/purchase-orders/router.js': '発注アプリ (マスタの表に書くルートは LEGACY_NOT_CLOSED)',
};

// ─── ファイルを集める ───
function walk(dir, out = []) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    if (ent.name === 'node_modules' || ent.name.startsWith('.')) continue;
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, out);
    else if (/\.(m?js|cjs)$/.test(ent.name)) out.push(p);
  }
  return out;
}
const rel = (p) => path.relative(ROOT, p).split(path.sep).join('/');
const isTestFile = (r) => /(^|\/)(test-[^/]*|[^/]*\.test\.m?js|[^/]*smoke[^/]*)$/.test(r) || /\/tests?\//.test(r);
const files = [...walk(path.join(ROOT, 'apps')), ...walk(path.join(ROOT, 'scripts'))].map((p) => ({ abs: p, rel: rel(p) })).filter((f) => !isTestFile(f.rel));
const text = new Map(files.map((f) => [f.rel, fs.readFileSync(f.abs, 'utf8').replace(/\r\n/g, '\n')]));   // CRLF のファイルも同じ形で読む

// ─── 印 (マスタの表に書く SQL・関数) ───
const tables = MASTER_WRITE_TARGETS.tables.join('|');
const SQL_WRITE = new RegExp(`(?:INSERT(?:\\s+OR\\s+\\w+)?\\s+INTO|UPDATE|DELETE\\s+FROM|REPLACE\\s+INTO)\\s+["\`]?(?:${tables})\\b`, 'i');
function writesMaster(fileRel, chunk) {
  if (SQL_WRITE.test(chunk)) return 'SQL';
  for (const c of MASTER_WRITE_TARGETS.calls[fileRel] || []) if (chunk.includes(c)) return c;
  return null;
}

// ─── ルートの定義 ───
const ROUTE_RE = /\b([A-Za-z_$][\w$]*)\.(get|post|put|patch|delete|all)\(\s*(['"`])([^'"`]+)\3/g;
function routesOf(src) {
  const out = [];
  let m;
  ROUTE_RE.lastIndex = 0;
  while ((m = ROUTE_RE.exec(src))) {
    const obj = m[1];
    if (!/router|app|api|^r$/i.test(obj)) continue;   // router / app / serviceApiRouter など
    if (!m[4].startsWith('/')) continue;
    out.push({ method: m[2].toUpperCase(), path: m[4], at: m.index });
  }
  return out;
}

console.log('── 1. マスタの表に書くルートは、全部一覧にある ──');
const listed = new Set([...LEGACY_ENTRIES, ...LEGACY_NOT_CLOSED].filter((e) => e.file && e.method && e.path).map((e) => `${e.file} ${e.method} ${e.path}`));
const flagged = [];
const routeIndex = new Map();   // "file METHOD path" → true (2. で使う)
for (const f of files) {
  const src = text.get(f.rel);
  const routes = routesOf(src);
  routes.forEach((r, i) => {
    routeIndex.set(`${f.rel} ${r.method} ${r.path}`, true);
    if (r.method === 'GET') return;
    const chunk = src.slice(r.at, i + 1 < routes.length ? routes[i + 1].at : src.length);
    const why = writesMaster(f.rel, chunk);
    if (why) flagged.push({ key: `${f.rel} ${r.method} ${r.path}`, why });
  });
}
const missing = flagged.filter((x) => !listed.has(x.key));
if (process.env.SHOW_FLAGGED) for (const x of flagged) console.log('    ·', x.key, x.why);
ok(flagged.length >= 20, `マスタの表に書くルートを見つけた (${flagged.length} 本) = 探し方が効いている`);
ok(missing.length === 0, 'マスタの表に書くルートは全部 LEGACY_ENTRIES か LEGACY_NOT_CLOSED にある', missing.map((x) => `${x.key} (${x.why})`).join('\n      '));

console.log('── 2. 一覧のルート・画面は、そのファイルに本当にある ──');
const stale = [...LEGACY_ENTRIES, ...LEGACY_NOT_CLOSED]
  .filter((e) => e.file && e.method && e.path)
  .filter((e) => !routeIndex.has(`${e.file} ${e.method} ${e.path}`));
ok(stale.length === 0, '一覧のルート・画面がコードにある (古い行が無い)', stale.map((e) => `${e.id} → ${e.file} ${e.method} ${e.path}`).join('\n      '));
ok(LEGACY_ENTRIES.every((e) => WHEN_FROZEN[e.when_frozen]), '閉じる入口は全部 when_frozen (閉じたときの動き) を持つ');
ok(LEGACY_NOT_CLOSED.every((e) => typeof e.reason === 'string' && e.reason.length > 5), '閉じないものは全部理由を持つ');
ok(Number.isInteger(LEGACY_GATES_VERSION) && LEGACY_GATES_VERSION >= 1, `門の版は 1 以上の整数 (${LEGACY_GATES_VERSION})`);

console.log('── 3. ルート以外でマスタの表に書くファイルは、CLI の入口か人の入口ではない書き手 ──');
const cliFiles = new Set(LEGACY_ENTRIES.filter((e) => e.kind === 'cli').map((e) => e.file));
const routeFiles = new Set(LEGACY_ENTRIES.filter((e) => e.kind !== 'cli').map((e) => e.file));
const unlistedWriters = [];
for (const f of files) {
  const src = text.get(f.rel);
  if (!SQL_WRITE.test(src)) continue;
  if (cliFiles.has(f.rel) || routeFiles.has(f.rel) || NON_ENTRY_WRITERS[f.rel]) continue;
  // ルートのファイルで、書く所が全部ルートの中 (1. で見た) なら入口は 1. が数えている
  const routes = routesOf(src);
  if (routes.length && LEGACY_NOT_CLOSED.some((e) => e.file === f.rel)) continue;
  unlistedWriters.push(f.rel);
}
ok(unlistedWriters.length === 0, 'マスタの表に書くファイルは全部分けてある (CLI の入口 / ルート / 人の入口ではない)', unlistedWriters.join('\n      '));

console.log('── 4. csv-import.js の mode は全部分けてある ──');
{
  const src = text.get('apps/warehouse/csv-import.js');
  const block = src.slice(src.indexOf('const handlers = {'), src.indexOf('};', src.indexOf('const handlers = {')));
  const modes = [...block.matchAll(/^\s{4}(\w+):\s*\(\)/gm)].map((m) => m[1]);
  const closed = new Set(LEGACY_ENTRIES.filter((e) => e.kind === 'cli' && e.file === 'apps/warehouse/csv-import.js').map((e) => e.mode));
  const keep = new Set(Object.keys(CLI_KEEP_MODES['apps/warehouse/csv-import.js'] || {}));
  ok(modes.length >= 7, `csv-import.js の mode を読めた (${modes.join(', ')})`);
  const unsorted = modes.filter((m) => !closed.has(m) && !keep.has(m));
  ok(unsorted.length === 0, 'mode は全部「閉じる」か「止めない」', unsorted.join(', '));
  ok([...closed].every((m) => !keep.has(m)), '「閉じる」と「止めない」が重ならない');
  // 閉じる mode は本当にマスタの表に書く関数 / 止めない mode は書かない
  for (const m of modes) {
    const fn = (block.match(new RegExp(`^\\s{4}${m}:\\s*\\(\\)\\s*=>\\s*(\\w+)\\(`, 'm')) || [])[1];
    if (!fn) continue;
    const body = src.slice(src.indexOf(`function ${fn}(`), src.indexOf('\n}\n', src.indexOf(`function ${fn}(`)) + 2);
    const writes = SQL_WRITE.test(body);
    ok(closed.has(m) ? writes : !writes, `csv-import.js ${m} (${fn}) は ${closed.has(m) ? '閉じる = マスタの表に書く' : '止めない = マスタの表に書かない'}`);
  }
}

console.log('── 5. 門が実際に掛かっている ──');
{
  const apps = [...new Set(LEGACY_ENTRIES.filter((e) => e.app).map((e) => e.app))];
  for (const app of apps) {
    const entryFiles = [...new Set(LEGACY_ENTRIES.filter((e) => e.app === app).map((e) => e.file))];
    // SKU マスタの API は warehouse の router に載る (mountSkuMasterApi) = warehouse/router.js の門が見る
    const routerFile = entryFiles.find((f) => /router\.m?js$/.test(f));
    const src = text.get(routerFile) || '';
    ok(src.includes(`masterLegacyGate('${app}')`), `${app}: ${routerFile} が masterLegacyGate('${app}') を使う`);
  }
  ok((text.get('apps/warehouse/router.js') || '').includes('mountSkuMasterApi(router)'), 'SKU マスタの API は warehouse の router (門の後ろ) に載る');
  for (const e of LEGACY_ENTRIES.filter((x) => x.kind === 'cli')) {
    const src = text.get(e.file) || '';
    const direct = src.includes(`legacyCliGate('${e.id}')`);
    const byMode = src.includes('cliEntry(') && src.includes('legacyCliGate(legacyEntry.id)');
    ok(direct || byMode, `${e.id}: ${e.file} が門 (legacyCliGate) を通す`);
  }
}

console.log(`\n${failed ? '❌' : '✅'} ${passed} 件 OK / ${failed} 件 NG`);
if (failed) process.exitCode = 1;
