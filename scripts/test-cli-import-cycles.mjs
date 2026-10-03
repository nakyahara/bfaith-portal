#!/usr/bin/env node
/**
 * 試験: 入口 (isMain) で top-level await を使う .mjs が、自分の import の先から自分へ戻る輪を持たない。
 *   輪があると、入口の await の途中で読む部品 (動的 import) が「評価の終わっていない入口」を待ち、
 *   Node は何もできずに終わる (Warning: Detected unsettled top-level await → exit 13)。
 *   2026-10-03 07:00 の daily-sync で apps/company-db/publish/fetch.mjs がこれで止まった
 *   (fetch.mjs → run.mjs / lz-daily.mjs → compare-old-tables.mjs → fetch.mjs)。
 *   接続や ping を差し替える試験では動的 import の道を通らないので、ファイルの import を読んで輪を探す。
 *
 * 使い方: node scripts/test-cli-import-cycles.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SCAN = ['apps', 'scripts', 'lib', 'tools'];
const SKIP_DIR = new Set(['node_modules', '.git', 'data', 'public']);

let fails = 0, oks = 0;
const ok = (cond, msg) => { if (cond) { oks++; console.log(`  ✅ ${msg}`); } else { fails++; console.log(`  ❌ ${msg}`); } };

function walk(dir, out) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (SKIP_DIR.has(e.name)) continue;
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p, out);
    else if (/\.(mjs|js)$/.test(e.name)) out.push(p);
  }
  return out;
}

// コメントを外す (/* */ と // )。文字列の中の // は URL などで出るので、行の頭の空白の後の // と、コードの後の ' //' だけ外す
function stripComments(src) {
  return src.replace(/\/\*[\s\S]*?\*\//g, '').split(/\r?\n/).map((l) => (/^\s*\/\//.test(l) ? '' : l)).join('\n');
}

const RE_STATIC = /(?:^|[\s;])(?:import|export)\s[^'"`;]*?from\s*['"]([^'"]+)['"]/g;
const RE_BARE = /(?:^|[\s;])import\s*['"]([^'"]+)['"]/g;
const RE_DYNAMIC = /import\(\s*['"]([^'"]+)['"]\s*\)/g;

const cache = new Map();
/** file の import の先。dynamicOnly = 動的 import (import('...')) だけ */
function importsOf(file, dynamicOnly = false) {
  const key = file + (dynamicOnly ? '#dyn' : '');
  if (cache.has(key)) return cache.get(key);
  let src = '';
  try { src = stripComments(fs.readFileSync(file, 'utf8')); } catch { /* */ }
  const resolve = (spec) => {
    if (!spec.startsWith('.')) return null;
    const p = path.resolve(path.dirname(file), spec);
    return fs.existsSync(p) && fs.statSync(p).isFile() ? p : null;
  };
  const out = [];
  if (!dynamicOnly) for (const re of [RE_STATIC, RE_BARE]) for (const m of src.matchAll(re)) { const t = resolve(m[1]); if (t) out.push(t); }
  for (const m of src.matchAll(RE_DYNAMIC)) { const t = resolve(m[1]); if (t) out.push(t); }
  cache.set(key, out);
  return out;
}

/** 入口の isMain の塊の中で await を使っているか (top-level await) */
function mainUsesTopLevelAwait(src) {
  const i = src.search(/^if \(isMain\) \{/m);
  if (i < 0) return false;
  // 塊の終わり = 行頭の } (このリポジトリの入口の書き方)
  const rest = src.slice(i);
  const end = rest.search(/^\}/m);
  const block = end > 0 ? rest.slice(0, end) : rest;
  // 塊の中の関数 (=> や function) の中の await は top-level ではないが、ここでは保守的に塊の中の await を全部数える
  return /\bawait\b/.test(block);
}

/**
 * 入口 file が動的に読む部品から import をたどって file に戻れるか。戻れるなら道を返す。
 *   止まるのは「入口の await の途中で動的に読む部品が入口を待つ」とき。静的な import だけの輪は、
 *   入口の評価 (await) の前に読み終わるので止まらない (照合の run.mjs・lz-daily.mjs は 10/3 の朝も動いた)。
 */
function cycleBack(file) {
  const seen = new Set();
  const stack = importsOf(file, true).map((t) => [t, [file, t]]);
  for (const t of importsOf(file, true)) { if (t === file) return [file, t]; }
  while (stack.length) {
    const [cur, trail] = stack.pop();
    for (const nxt of importsOf(cur)) {
      if (nxt === file) return [...trail, nxt];
      if (seen.has(nxt)) continue;
      seen.add(nxt);
      stack.push([nxt, [...trail, nxt]]);
    }
  }
  return null;
}

console.log('入口 (isMain) で top-level await を使う .mjs が、import の輪で自分を待たない');
const files = SCAN.flatMap((d) => (fs.existsSync(path.join(ROOT, d)) ? walk(path.join(ROOT, d), []) : []));
const rel = (p) => path.relative(ROOT, p).split(path.sep).join('/');
let checked = 0;
for (const f of files) {
  if (!f.endsWith('.mjs')) continue;
  const src = stripComments(fs.readFileSync(f, 'utf8'));
  if (!mainUsesTopLevelAwait(src)) continue;
  checked++;
  const c = cycleBack(f);
  ok(!c, c ? `${rel(f)}: 輪がある ${c.map(rel).join(' → ')}` : `${rel(f)}`);
}
ok(checked > 0, `調べた入口の数 = ${checked} (0 なら探し方が壊れている)`);

// 探し方そのものの確かめ: 2026-10-03 の形 (入口 → 部品 → 入口) を一時のファイルで作って見つける
{
  const tmp = fs.mkdtempSync(path.join(ROOT, '.tmp-cli-cycle-'));
  try {
    fs.writeFileSync(path.join(tmp, 'entry.mjs'), "export const v = 1;\nconst isMain = true;\nif (isMain) {\n  const m = await import('./part.mjs');\n  console.log(m.w);\n}\n");
    fs.writeFileSync(path.join(tmp, 'part.mjs'), "import { v } from './entry.mjs';\nexport const w = v;\n");
    const e = path.join(tmp, 'entry.mjs');
    ok(mainUsesTopLevelAwait(fs.readFileSync(e, 'utf8')) && !!cycleBack(e), '探し方の確かめ: 入口 → 部品 → 入口 の輪を見つける');
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
}

// 本物の入口を起動する: 写しの反映 (--verify-apply --daily) が ping の部品 (lz-daily.mjs) を読んでも止まらない。
//   空の DATA_DIR = 確かめられない = exit 1 で、exit 13 (unsettled top-level await) ではない。
//   本番に ping しない: cwd を一時の場所にして .env を読ませず、送り先・接続の env を外す
console.log('\n本物の入口 apps/company-db/publish/fetch.mjs --verify-apply --daily が止まらずに終わる');
{
  const { spawnSync } = await import('node:child_process');
  const os = await import('node:os');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cli-cycle-entry-'));
  try {
    const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^(JOBS_MONITOR|COMPANY_DB|GCHAT|DATA_DIR|LZ_)/i.test(k)));
    const r = spawnSync(process.execPath, [path.join(ROOT, 'apps/company-db/publish/fetch.mjs'), '--verify-apply', '--daily', '--data-dir', tmp],
      { cwd: tmp, env, encoding: 'utf8', timeout: 120000 });
    const out = `${r.stdout || ''}${r.stderr || ''}`;
    ok(r.status !== 13 && !/unsettled top-level await/.test(out), `exit 13 にならない (exit ${r.status})`);
    ok(r.status === 1 && /Company DB の写しの反映/.test(out), '空の DATA_DIR = 確かめられない = exit 1 で結果の行を出す');
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
}

console.log(`\n${oks} 件 ok${fails ? ` / ${fails} 件 NG` : ''}`);
process.exitCode = fails ? 1 : 0;
