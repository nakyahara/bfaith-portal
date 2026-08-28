#!/usr/bin/env node
/**
 * phq — product-hub 生成キューの固定機能 CLI (夜間ランナー用、依存パッケージなし)。
 *
 * なぜあるか (Codex R1 critical, 2026-08-28): Claude Code に curl/node/python を許可すると deny は迂回でき、
 * スキルがトークンを読ませていた。そこで **Claude にはこの CLI だけを許可し、トークンはこの中でしか扱わない**。
 *   - HTTP の相手は service-api 固定 (Base URL・メソッド・パスをここで決める)。任意 URL への POST はできない
 *   - fetch は GET のみ・本文なし・出力先は作業ディレクトリ内・private/loopback/link-local は拒否
 *   - トークンは標準出力・エラーに一切出さない (Authorization ヘッダはここで付けるだけ)
 *
 * 使い方 (作業ディレクトリ = C:\tools\ph-nightly で `./phq <cmd>`):
 *   ./phq queue                                      現在の一覧 + queue 内訳
 *   ./phq claim   --run RUN_ID [--limit 1]           claim して材料を返す
 *   ./phq block   ID --run RUN_ID --code CODE --reason-file FILE   人の確認待ちにする
 *   ./phq release ID --run RUN_ID --reason "text"    一時的に手放す (次の claim でまた来る)
 *   ./phq submit  ID --run RUN_ID --file copy-ID.json [--advance] [--note "claude+codex 2026-08-28"]
 *   ./phq fetch   URL --out FILE                     商品ページを取得して保存 (ブラウザUA・リダイレクト追従)
 *   ./phq extract FILE                               保存した HTML から商品情報テキストを抽出 (Amazon 対応・SJIS 自動判定)
 *   ./phq find    FILE needle [needle...]            JAN/ASIN 等がページ内にあるか (同一性確認)
 *   ./phq lint    copy-ID.json                       copy_lint.py を固定パスで実行
 *   ./phq search-amazon JAN --out FILE               JAN で Amazon 検索して候補 ASIN を出す
 *
 * copy-ID.json の形 = copy_lint.py と同じ {code, rakuten_title, yahoo_title, headline, caption, notes}。
 * submit は caption を「【仕様】」で desc_features / desc_spec に分けて 6 kind にする。
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const BASE = process.env.PH_SERVICE_BASE || 'https://bfaith-portal.onrender.com/apps/product-hub/service-api';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36';
const BLOCK_CODES = ['PACK_COUNT_MISMATCH', 'IDENTITY_UNVERIFIED', 'SOURCE_UNREACHABLE', 'FACTS_TOO_THIN', 'OTHER'];
const KINDS = ['rakuten_title', 'yahoo_title', 'desc_catch', 'desc_features', 'desc_spec', 'desc_notes'];

// die は引数検証など「まだ非同期処理を始めていない」場面だけで使う。
// ネットワーク後に process.exit() を即呼びすると libuv がアサート落ちする (Node24・Windows:
// "Assertion failed: !(handle->flags & UV_HANDLE_CLOSING)") → 非同期の後は fail() で exitCode を立てて自然終了させる
function die(msg, code = 2) { process.stderr.write(`phq: ${msg}\n`); process.exit(code); }
function fail(code = 1) { process.exitCode = code; }
function out(obj) { process.stdout.write((typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2)) + '\n'); }

function parseArgs(argv) {
  const pos = []; const opt = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const k = a.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith('--')) opt[k] = true; else { opt[k] = next; i++; }
    } else pos.push(a);
  }
  return { pos, opt };
}

// ── token: ここでしか読まない・どこにも出さない ─────────────────────────────
function token() {
  if (process.env.PH_SERVICE_TOKEN) return process.env.PH_SERVICE_TOKEN.trim();
  const f = path.join(os.homedir(), '.claude', 'secrets', 'ph-service-token.txt');
  try { return fs.readFileSync(f, 'utf8').trim(); } catch { die('service token not found (~/.claude/secrets/ph-service-token.txt or PH_SERVICE_TOKEN)'); }
}

async function api(method, p, body, { retries = 4 } = {}) {
  const tok = token();
  let last;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(BASE + p, {
        method,
        headers: { Authorization: `Bearer ${tok}`, 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(60_000),
      });
      const text = await res.text();
      let json = null;
      try { json = JSON.parse(text); } catch { json = { ok: false, error: `non-JSON response (${text.length}B)` }; }
      if (res.status >= 500 && attempt < retries) { last = { status: res.status, json }; await sleep(8000); continue; }
      return { status: res.status, json };
    } catch (e) {
      last = { status: 0, json: { ok: false, error: String(e.message || e) } };
      if (attempt < retries) await sleep(8000);
    }
  }
  return last;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── 作業ディレクトリの外に書かない ──────────────────────────────────────────
function insideCwd(file) {
  const abs = path.resolve(process.cwd(), file);
  const rel = path.relative(process.cwd(), abs);
  if (rel.startsWith('..') || path.isAbsolute(rel)) die(`refusing to write outside the workspace: ${file}`);
  return abs;
}

// ── fetch: GET のみ・内部ネットワーク拒否 ────────────────────────────────────
function assertPublicHttpUrl(u) {
  let url;
  try { url = new URL(u); } catch { die(`invalid URL: ${u}`); }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') die(`only http(s) is allowed: ${u}`);
  const h = url.hostname.toLowerCase();
  if (h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal')) die(`blocked host: ${h}`);
  if (/^\d+\.\d+\.\d+\.\d+$/.test(h)) {
    const [a, b] = h.split('.').map(Number);
    if (a === 10 || a === 127 || a === 0 || (a === 192 && b === 168) || (a === 172 && b >= 16 && b <= 31) || (a === 169 && b === 254)) die(`blocked private address: ${h}`);
  }
  if (h.includes(':') || h === '[::1]') die(`blocked host: ${h}`);
  return url;
}

async function cmdFetch(pos, opt) {
  const u = pos[0]; if (!u) die('usage: fetch URL --out FILE');
  if (!opt.out) die('--out FILE is required');
  const outFile = insideCwd(String(opt.out));
  let url = assertPublicHttpUrl(u);
  let res;
  for (let hop = 0; hop < 6; hop++) {
    res = await fetch(url, {
      method: 'GET', redirect: 'manual',
      headers: { 'User-Agent': UA, 'Accept-Language': 'ja-JP,ja;q=0.9', Accept: 'text/html,*/*;q=0.8' },
      signal: AbortSignal.timeout(45_000),
    });
    if (res.status >= 300 && res.status < 400 && res.headers.get('location')) {
      url = assertPublicHttpUrl(new URL(res.headers.get('location'), url).toString());
      continue;
    }
    break;
  }
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(outFile, buf);
  out({ ok: res.ok, status: res.status, final_url: url.toString(), bytes: buf.length, file: path.relative(process.cwd(), outFile) });
  if (!res.ok) fail(1);
}

// ── extract: HTML → 商品情報テキスト ──────────────────────────────────────────
function decodeHtml(buf) {
  const head = buf.subarray(0, 4096).toString('latin1');
  const m = /charset=["']?\s*([a-z0-9_-]+)/i.exec(head);
  const cs = (m ? m[1] : 'utf-8').toLowerCase();
  const label = /shift|sjis|windows-31j|cp932|ms932/.test(cs) ? 'shift_jis' : /euc/.test(cs) ? 'euc-jp' : 'utf-8';
  try { return new TextDecoder(label, { fatal: label === 'utf-8' }).decode(buf); }
  catch { return new TextDecoder('shift_jis').decode(buf); }
}
const strip = (s) => s.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ')
  .replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#\d+;/g, '')
  // slice で途中から始まった <style> の中身 (`.cls{...}`) と data-* 属性の残骸を落とす
  .replace(/[.#a-z][\w-]*(?:\[[^\]]*\])*(?:[,\s]+[.#a-z][\w-]*(?:\[[^\]]*\])*)*\s*\{[^}]*\}/g, ' ').replace(/\b[\w-]+="[^"]*"/g, ' ')
  .replace(/\s+/g, ' ').trim();
const section = (h, id, len = 12000) => {
  const i = h.indexOf(`id="${id}"`); if (i < 0) return null;
  return strip(h.slice(i, i + len)).split('ご意見ご要望')[0].split('この商品に関する問題')[0].split('カスタマーレビュー')[0].split('中小企業')[0].slice(0, 1800);
};
function cmdExtract(pos) {
  const f = pos[0]; if (!f) die('usage: extract FILE');
  const h = decodeHtml(fs.readFileSync(f));
  const lines = [];
  const title = /<span id="productTitle"[^>]*>([\s\S]*?)<\/span>/.exec(h) || /<title>([\s\S]*?)<\/title>/i.exec(h);
  lines.push('TITLE: ' + (title ? strip(title[1]) : '-'));
  const by = /id="bylineInfo"[^>]*>([\s\S]*?)<\/a>/.exec(h); if (by) lines.push('BYLINE: ' + strip(by[1]));
  const md = /name="description"\s+content="([^"]*)"/i.exec(h); if (md) lines.push('META_DESCRIPTION: ' + strip(md[1]).slice(0, 800));
  const og = /property="og:description"\s+content="([^"]*)"/i.exec(h); if (og) lines.push('OG_DESCRIPTION: ' + strip(og[1]).slice(0, 800));
  const fb = /<div id="feature-bullets"[\s\S]*?<ul[^>]*>([\s\S]*?)<\/ul>/.exec(h);
  if (fb) { lines.push('FEATURE_BULLETS:'); for (const m of fb[1].matchAll(/<span class="a-list-item[^"]*">([\s\S]*?)<\/span>/g)) { const t = strip(m[1]); if (t) lines.push('  * ' + t); } }
  for (const id of ['productOverview_feature_div', 'prodDetails', 'important-information', 'detailBullets_feature_div']) {
    const s = section(h, id); if (s) lines.push(`${id}: ${s}`);
  }
  if (lines.length <= 2) { // Amazon 以外: 本文の先頭を出す
    lines.push('BODY_HEAD: ' + strip(h).slice(0, 3000));
  }
  out(lines.join('\n'));
}
function cmdFind(pos) {
  const [f, ...needles] = pos; if (!f || needles.length === 0) die('usage: find FILE needle [needle...]');
  const h = decodeHtml(fs.readFileSync(f)).toLowerCase();
  const r = {}; for (const n of needles) r[n] = h.includes(String(n).toLowerCase());
  out(r);
  if (!Object.values(r).every(Boolean)) fail(1);
}
async function cmdSearchAmazon(pos, opt) {
  const jan = pos[0]; if (!/^\d{8,14}$/.test(jan || '')) die('usage: search-amazon JAN --out FILE');
  if (!opt.out) die('--out FILE is required');
  await cmdFetch([`https://www.amazon.co.jp/s?k=${jan}`], { out: opt.out });
  const h = fs.readFileSync(insideCwd(String(opt.out)), 'utf8');
  const asins = [...new Set([...h.matchAll(/data-asin="(B0[A-Z0-9]{8})"/g)].map((m) => m[1]))];
  out({ jan, asins: asins.slice(0, 8) });
}

// ── queue / claim / block / release / submit ─────────────────────────────────
async function cmdQueue() { const r = await api('GET', '/generation-queue'); out(r.json); if (r.status !== 200) process.exit(1); }
async function cmdClaim(pos, opt) {
  if (!opt.run) die('--run RUN_ID is required');
  const limit = Number(opt.limit || 1);
  const r = await api('POST', '/generation-queue/claim', { run_id: String(opt.run), limit });
  out(r.json); if (r.status !== 200) fail(1);
}
async function cmdBlock(pos, opt) {
  const id = pos[0]; if (!/^\d+$/.test(id || '')) die('usage: block ID --run RUN_ID --code CODE --reason-file FILE');
  if (!opt.run || !opt.code || !opt['reason-file']) die('--run, --code, --reason-file are required');
  if (!BLOCK_CODES.includes(String(opt.code))) die(`code must be one of ${BLOCK_CODES.join(' / ')}`);
  const reason = fs.readFileSync(String(opt['reason-file']), 'utf8').trim();
  if (!reason) die('reason file is empty');
  const r = await api('POST', `/drafts/${id}/generation-block`, { run_id: String(opt.run), code: String(opt.code), reason });
  out(r.json); if (r.status !== 200) fail(1);
}
async function cmdRelease(pos, opt) {
  const id = pos[0]; if (!/^\d+$/.test(id || '')) die('usage: release ID --run RUN_ID --reason "text"');
  if (!opt.run) die('--run RUN_ID is required');
  const r = await api('POST', `/drafts/${id}/release`, { run_id: String(opt.run), reason: String(opt.reason || '') });
  out(r.json); if (r.status !== 200) fail(1);
}
async function cmdSubmit(pos, opt) {
  const id = pos[0]; if (!/^\d+$/.test(id || '')) die('usage: submit ID --run RUN_ID --file copy-ID.json [--advance] [--note TEXT]');
  if (!opt.run || !opt.file) die('--run and --file are required');
  const c = JSON.parse(fs.readFileSync(String(opt.file), 'utf8'));
  const cap = String(c.caption || '');
  const i = cap.indexOf('【仕様】');
  const outputs = {
    rakuten_title: c.rakuten_title, yahoo_title: c.yahoo_title, desc_catch: c.headline,
    desc_features: (i >= 0 ? cap.slice(0, i) : cap).trim(),
    desc_spec: i >= 0 ? cap.slice(i + '【仕様】'.length).trim() : '',
    desc_notes: c.notes,
  };
  for (const k of KINDS) if (!outputs[k] || !String(outputs[k]).trim()) { if (opt.advance) die(`outputs.${k} is empty (advance needs all 6)`); delete outputs[k]; }
  const r = await api('POST', `/drafts/${id}/ai-outputs`, {
    run_id: String(opt.run), outputs, advance: opt.advance === true, model_note: String(opt.note || `claude+codex ${new Date().toISOString().slice(0, 10)}`),
  });
  out(r.json); if (r.status !== 200) fail(1);
}

// ── lint: copy_lint.py を固定パスで ──────────────────────────────────────────
function cmdLint(pos) {
  const f = pos[0]; if (!f) die('usage: lint copy-ID.json');
  const script = path.join(HERE, 'copy_lint.py');
  if (!fs.existsSync(script)) die(`copy_lint.py not found next to phq.mjs (${script})`);
  const py = process.platform === 'win32' ? 'python' : 'python3';
  const r = spawnSync(py, ['-X', 'utf8', script, f], { encoding: 'utf8', env: { ...process.env, PYTHONUTF8: '1' } });
  process.stdout.write(r.stdout || ''); process.stderr.write(r.stderr || '');
  process.exit(r.status ?? 1);
}

const { pos, opt } = parseArgs(process.argv.slice(2));
const cmd = pos.shift();
const table = { queue: cmdQueue, claim: cmdClaim, block: cmdBlock, release: cmdRelease, submit: cmdSubmit, fetch: cmdFetch, extract: cmdExtract, find: cmdFind, lint: cmdLint, 'search-amazon': cmdSearchAmazon };
if (!cmd || !table[cmd]) die(`usage: phq <${Object.keys(table).join('|')}> ...`);
Promise.resolve(table[cmd](pos, opt)).catch((e) => die(String(e.message || e), 1));
