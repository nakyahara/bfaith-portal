#!/usr/bin/env node
/**
 * test-env-single-root.mjs — 設定はリポジトリ直下の .env だけ (2026-09-13)
 *
 * ① 取得ツールのどのファイルも、自分のフォルダの .env や dotenv を直接読まない (読み込み口は lib-env.mjs だけ)
 * ② 本物の lib-env.mjs を、リポジトリと同じ形の一時フォルダで動かし、直下の値が使われる・
 *    scripts/mall-csv-fetcher/.env は読まれない・タスクが渡した環境変数が優先される、を確かめる
 * 実行: node scripts/mall-csv-fetcher/test-env-single-root.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import vm from 'node:vm';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.join(__dirname, '..', '..');

// import の解析に Node 組み込みのモジュール解析 (vm.SourceTextModule) を使う。
// これは --experimental-vm-modules が要るので、無ければ付けて自分を起動し直す
if (!vm.SourceTextModule) {
  const r = spawnSync(process.execPath, ['--experimental-vm-modules', '--no-warnings', fileURLToPath(import.meta.url)], { stdio: 'inherit' });
  process.exit(r.status ?? 1);
}

let passed = 0;
function t(name, fn) { try { fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; } }

t('取得ツールのどのファイルも、自分のフォルダの .env・dotenv を直接読まない (読むのは lib-env.mjs だけ)', () => {
  const bad = [];
  for (const f of fs.readdirSync(__dirname).filter((n) => /\.(mjs|js|cjs)$/.test(n) && !['lib-env.mjs', 'test-env-single-root.mjs'].includes(n))) {
    const s = fs.readFileSync(path.join(__dirname, f), 'utf8');
    if (/from ['"]dotenv['"]|require\(['"]dotenv|['"]dotenv\/config['"]/.test(s)) bad.push(`${f}: dotenv を直接読む`);
    if (/join\(__dirname,\s*['"]\.env['"]\)/.test(s)) bad.push(`${f}: 自分のフォルダの .env を読む`);
    if (/join\(__dirname,\s*['"]\.\.['"],\s*['"]\.\.['"],\s*['"]\.env['"]\)/.test(s)) bad.push(`${f}: 直下の .env を自前で読む (lib-env.mjs を使う)`);
  }
  assert.deepEqual(bad, []);
});

t('ログイン部品・通知部品・一括取得は lib-env.mjs を読み込む', () => {
  for (const f of ['lib-rakuten-login.mjs', 'lib-aupay-login.mjs', 'lib-yahoo-login.mjs', 'lib-qoo10-login.mjs', 'lib-notify.mjs', 'fetch-all.mjs']) {
    assert.match(fs.readFileSync(path.join(__dirname, f), 'utf8'), /^import '\.\/lib-env\.mjs';/m, f);
  }
});

/**
 * file (絶対パス) から **静的 import (import 宣言・export … from) だけ** をたどり、target に行き着くか。
 * Node 組み込みのモジュール解析 (vm.SourceTextModule) を使うので、コメントや文字列の中の import は数えない (Codex #1311 R2)。
 * 動的 import() も数えない: 読み込みより先に process.env を読むコードが走りうるので、設定の読み込み口には使えない
 */
function reachesTarget(file, target, seen = new Set()) {
  if (file === target) return true;
  if (seen.has(file) || !fs.existsSync(file) || !/\.(mjs|js)$/.test(file)) return false; // .cjs は ESM の静的 import を持たない
  seen.add(file);
  let m;
  try { m = new vm.SourceTextModule(fs.readFileSync(file, 'utf8'), { identifier: file }); } catch { return false; } // CommonJS の .js など
  const specs = m.moduleRequests ? m.moduleRequests.map((r) => r.specifier) : m.dependencySpecifiers;
  return specs.filter((s) => s.startsWith('./') || s.startsWith('../'))
    .some((s) => reachesTarget(path.resolve(path.dirname(file), s), target, seen));
}

/** 取得ツールの入口になりうる .mjs (サブフォルダも。出力・プロファイル・試験は除く) */
function entryFiles(dir) {
  const out = [];
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.name.startsWith('.') || ['node_modules', 'logs', 'downloads', 'spike-output'].includes(e.name)) continue;
    const p = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...entryFiles(p));
    else if (e.name.endsWith('.mjs') && !e.name.startsWith('test-') && e.name !== 'lib-env.mjs') out.push(p);
  }
  return out;
}

t('🚨 環境変数を読むファイルは、静的 import をたどると必ず lib-env.mjs に行き着く', () => {
  // 新しい単体スクリプトが lib-env.mjs を読み忘れると、直下の .env が入らないまま動く (Codex #1311 R1/R2 Low)
  const target = path.join(__dirname, 'lib-env.mjs');
  // 設定を読まない部品 (クーポン計算など) は対象外: process.env を読むファイルだけを見る。
  // Windows が入れる環境変数 (USERPROFILE など) だけを読む部品は .env と無関係なので理由付きで外す
  const OS_ENV_ONLY = { 'lib-browser-profile-guard.mjs': 'USERPROFILE / USERNAME / COMPUTERNAME だけ (SYSTEM 実行の判定)' };
  const missing = entryFiles(__dirname)
    .filter((p) => !OS_ENV_ONLY[path.basename(p)] && /process\.env\b/.test(fs.readFileSync(p, 'utf8')) && !reachesTarget(p, target))
    .map((p) => path.relative(__dirname, p));
  assert.deepEqual(missing, [], `lib-env.mjs に行き着かない: ${missing.join(', ')}`);
});

t('解析の見本: コメント内・文字列内・動的 import では行き着かない / 詰めた書き方・export from・../ はたどる', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'env-graph-'));
  try {
    const w = (rel, s) => { const p = path.join(dir, rel); fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(p, s); return p; };
    const target = w('lib-env.mjs', 'export {};\n');
    w('sub/a.mjs', "export * from '../lib-env.mjs';\nexport const a = 1;\n");
    const got = {
      comment: reachesTarget(w('comment.mjs', "/* import './lib-env.mjs'; */\n// import './lib-env.mjs';\nconsole.log(process.env.X);\n"), target),
      string: reachesTarget(w('string.mjs', "const s = \"import './lib-env.mjs'\";\nconsole.log(s, process.env.X);\n"), target),
      dynamic: reachesTarget(w('dynamic.mjs', "await import('./lib-env.mjs');\nconsole.log(process.env.X);\n"), target),
      compact: reachesTarget(w('compact.mjs', "import{a}from'./sub/a.mjs';console.log(a, process.env.X);\n"), target),
    };
    assert.deepEqual(got, { comment: false, string: false, dynamic: false, compact: true });
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

/** 本物の lib-env.mjs をリポジトリと同じ形の一時フォルダに置いて動かす (dotenv はこのリポジトリの node_modules を辿る) */
function runProbe({ withScriptsEnv }) {
  const tmp = fs.mkdtempSync(path.join(REPO, '.tmp-env-test-'));
  try {
    const dir = path.join(tmp, 'scripts', 'mall-csv-fetcher');
    fs.mkdirSync(dir, { recursive: true });
    fs.copyFileSync(path.join(__dirname, 'lib-env.mjs'), path.join(dir, 'lib-env.mjs'));
    fs.writeFileSync(path.join(tmp, '.env'), 'ZZ_ENV_TEST=root\nZZ_ENV_ONLY_ROOT=yes\nZZ_PRESET=root-file\n');
    if (withScriptsEnv) fs.writeFileSync(path.join(dir, '.env'), 'ZZ_ENV_TEST=old\nZZ_ENV_ONLY_SCRIPTS=yes\n');
    fs.writeFileSync(path.join(dir, 'probe.mjs'), "import './lib-env.mjs';\n"
      + 'console.log(JSON.stringify({ t: process.env.ZZ_ENV_TEST ?? null, r: process.env.ZZ_ENV_ONLY_ROOT ?? null, '
      + 's: process.env.ZZ_ENV_ONLY_SCRIPTS ?? null, p: process.env.ZZ_PRESET ?? null }));\n');
    const env = { ...process.env, ZZ_PRESET: 'from-task' };
    for (const k of ['ZZ_ENV_TEST', 'ZZ_ENV_ONLY_ROOT', 'ZZ_ENV_ONLY_SCRIPTS']) delete env[k];
    // cwd はリポジトリの外 = 「どこから実行しても同じファイルを読む」ことも確かめる
    const r = spawnSync(process.execPath, [path.join(dir, 'probe.mjs')], { encoding: 'utf8', cwd: os.tmpdir(), env });
    assert.equal(r.status, 0, r.stderr);
    return { out: JSON.parse(r.stdout.trim().split(/\r?\n/).pop()), stderr: r.stderr };
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

t('🚨 直下の .env の値が使われ、scripts/mall-csv-fetcher/.env は読まない (残っていれば警告だけ)', () => {
  const { out, stderr } = runProbe({ withScriptsEnv: true });
  assert.equal(out.t, 'root', '同じ項目は直下の値');
  assert.equal(out.r, 'yes');
  assert.equal(out.s, null, 'scripts 側にしか無い項目も読まれない');
  assert.match(stderr, /は読みません/);
});

t('タスクが渡した環境変数は .env より優先 (HEADLESS=1 など)', () => {
  assert.equal(runProbe({ withScriptsEnv: false }).out.p, 'from-task');
});

t('scripts/mall-csv-fetcher/.env が無ければ警告は出ない', () => {
  assert.doesNotMatch(runProbe({ withScriptsEnv: false }).stderr, /は読みません/);
});

console.log(`\n${passed} 件 PASS`);
