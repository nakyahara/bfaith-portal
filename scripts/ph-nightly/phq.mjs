#!/usr/bin/env node
/**
 * phq — product-hub 生成キューの固定機能 CLI (夜間ランナー用、依存パッケージなし)。
 *
 * なぜあるか (Codex R1/R2 critical, 2026-08-28): Claude Code に curl/node/python を許可すると deny は迂回でき、
 * スキルがトークンを読ませていた。そこで **Claude にはこの CLI (と phreview) だけを許可し、トークンはこの中でしか扱わない**。
 *   - HTTP の相手は service-api 固定 (Base URL・メソッド・パスをここで決める)。任意 URL への POST はできない
 *   - fetch は GET のみ・本文なし・DNS の A レコードが全部グローバル IPv4 でなければ拒否し、**その IP に接続を固定**
 *     (検査と接続の間で解決結果が変わる DNS rebinding を潰す。IPv6 は使わない)・8MB 上限
 *   - **ファイル引数は作業ディレクトリ直下の決まった名前だけ** (page-*.html / copy-*.json / reason-*.txt)。
 *     パス区切りを含む名前・symlink は拒否 → トークンや .env をこの CLI 経由で読ませない (R2 critical 1)
 *   - claim は常に 1 件 (lease 30 分に収める)
 *   - トークンは標準出力・エラーに一切出さない
 * 配置: bin/phq.mjs (bfaith に書き込み拒否の ACL) — Claude が動く work/ から ./phq シム経由で呼ぶ。
 *
 * 使い方 (作業ディレクトリ = C:\tools\ph-nightly\work で `./phq <cmd>`):
 *   ./phq queue                                              現在の一覧 + queue 内訳
 *   ./phq claim   --run RUN_ID                               1 件 claim して材料を返す
 *   ./phq block   ID --run RUN_ID --code CODE --reason-file reason-ID.txt
 *   ./phq release ID --run RUN_ID --reason "text"            一時的に手放す (次の claim でまた来る)
 *   ./phq submit  ID --run RUN_ID --file copy-ID.json [--advance] [--note "claude+codex 2026-08-28"]
 *   ./phq fetch   URL --out page-ID.html                     商品ページを取得して保存
 *   ./phq extract page-ID.html                               HTML → 商品情報テキスト (Amazon 対応・SJIS 自動判定)
 *   ./phq find    page-ID.html needle [needle...]            JAN/ASIN 等がページ内にあるか (同一性確認)
 *   ./phq lint    copy-ID.json                               copy_lint.py を固定パスで実行
 *   ./phq search-amazon JAN --out page-s-ID.html             JAN で Amazon 検索して候補 ASIN を出す
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import dns from 'node:dns/promises';
import http from 'node:http';
import https from 'node:https';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const BASE = 'https://bfaith-portal.onrender.com/apps/product-hub/service-api';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36';
const BLOCK_CODES = ['PACK_COUNT_MISMATCH', 'IDENTITY_UNVERIFIED', 'SOURCE_UNREACHABLE', 'FACTS_TOO_THIN', 'OTHER'];
const KINDS = ['rakuten_title', 'yahoo_title', 'desc_catch', 'desc_features', 'desc_spec', 'desc_notes'];
const REASON_MAX = 1000;
const FETCH_MAX_BYTES = 8 * 1024 * 1024;
// 用途別のファイル名 (basename のみ。区切り文字・.. を含む名前は正規表現で弾かれる)
const NAME = {
  page: /^page-[A-Za-z0-9_-]{1,40}\.html$/,
  copy: /^copy-[A-Za-z0-9_-]{1,40}\.json$/,
  reason: /^reason-[A-Za-z0-9_-]{1,40}\.txt$/,
};

// die は引数検証など「まだ非同期処理を始めていない」場面だけで使う。
// ネットワーク後に process.exit() を即呼びすると libuv がアサート落ちする (Node24・Windows) → fail() で exitCode
function die(msg, code = 2) { process.stderr.write(`phq: ${msg}\n`); process.exit(code); }
function fail(code = 1) { process.exitCode = code; }
function out(obj) { process.stdout.write((typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2)) + '\n'); }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

/**
 * 作業ディレクトリ直下・決まった名前のファイルだけを扱う (Codex R2 critical 1 / high 5)。
 *   - basename のみ (path.basename(name) === name)。`../x`, `C:/...`, `sub/x` はここで落ちる
 *   - 用途ごとの正規表現に一致
 *   - 実体ファイルであること (symlink/junction は拒否)。作業ディレクトリ自体も realpath で固定
 */
function workFile(name, kind, { mustExist } = { mustExist: true }) {
  const s = String(name || '');
  if (!s || path.basename(s) !== s || !NAME[kind].test(s)) die(`${kind} file must be a plain name like ${NAME[kind]} (got: ${s})`);
  const cwd = fs.realpathSync(process.cwd());
  const abs = path.join(cwd, s);
  let st = null;
  try { st = fs.lstatSync(abs); } catch { st = null; }
  if (st && !st.isFile()) die(`refusing non-regular file: ${s}`);
  if (mustExist && !st) die(`file not found: ${s}`);
  return abs;
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
      let json;
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

// ── fetch: GET のみ・**検査済み IPv4 に接続を固定** (DNS rebinding 対策)・サイズ上限 ─────
// IPv4 だけを許す判定 (Codex R3 high 3 / R4 medium): IANA Special-Purpose Address Registry の全レンジを CIDR で拒否
const NON_GLOBAL_V4 = [
  '0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12',
  '192.0.0.0/24', '192.0.2.0/24', '192.31.196.0/24', '192.52.193.0/24', '192.88.99.0/24', '192.168.0.0/16',
  '192.175.48.0/24', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '224.0.0.0/4', '240.0.0.0/4',
].map((c) => { const [ip, bits] = c.split('/'); return { base: ipv4ToInt(ip), mask: bits === '0' ? 0 : (~0 << (32 - Number(bits))) >>> 0 }; });
function ipv4ToInt(ip) {
  const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(ip); if (!m) return null;
  const p = m.slice(1).map(Number); if (p.some((x) => x > 255)) return null;
  return ((p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3]) >>> 0;
}
function isGlobalIpv4(ip) {
  const n = ipv4ToInt(ip); if (n === null) return false;
  return !NON_GLOBAL_V4.some((r) => ((n & r.mask) >>> 0) === r.base);
}
/** URL を検証し、接続に使う IPv4 を決める。ホスト名は DNS の A レコード全部がグローバルでなければ拒否 */
async function resolvePublic(u) {
  let url;
  try { url = new URL(u); } catch { die(`invalid URL: ${u}`); }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') die(`only http(s) is allowed: ${u}`);
  const h = url.hostname.toLowerCase();
  if (h.startsWith('[') || h.includes(':')) die(`IPv6 targets are not allowed: ${h}`);
  if (h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal') || h.endsWith('.localhost')) die(`blocked host: ${h}`);
  let addrs;
  if (/^[\d.]+$/.test(h)) addrs = [h];
  else {
    // OS リゾルバ (getaddrinfo)。dns.resolve4 (c-ares 直接) は社内ネットワークで失敗することがある
    try { addrs = (await dns.lookup(h, { all: true, family: 4 })).map((a) => a.address); } catch { die(`DNS lookup (A) failed: ${h}`); }
  }
  if (addrs.length === 0 || !addrs.every(isGlobalIpv4)) die(`blocked address for host ${h}`);
  return { url, ip: addrs[0] };
}
/** 1 回の GET。接続先は resolvePublic が返した IP に固定 (lookup を差し替え)、Host/SNI は元のホスト名のまま */
function getOnce(url, ip) {
  return new Promise((resolve, reject) => {
    const mod = url.protocol === 'https:' ? https : http;
    const req = mod.request(url, {
      method: 'GET',
      headers: { 'User-Agent': UA, 'Accept-Language': 'ja-JP,ja;q=0.9', Accept: 'text/html,*/*;q=0.8', Connection: 'close' },
      // 接続先を検査済み IP に固定。Node 22+ の net は all:true で配列 [{address,family}] を期待する
      lookup: (_host, opts, cb) => (opts && opts.all ? cb(null, [{ address: ip, family: 4 }]) : cb(null, ip, 4)),
      timeout: 45_000,
    }, (res) => {
      const cl = Number(res.headers['content-length'] || 0);
      if (cl > FETCH_MAX_BYTES) { res.destroy(); return reject(new Error(`response too large (${cl}B > ${FETCH_MAX_BYTES}B)`)); }
      const chunks = []; let total = 0;
      res.on('data', (chunk) => {
        total += chunk.length;
        if (total > FETCH_MAX_BYTES) { res.destroy(); return reject(new Error(`response too large (> ${FETCH_MAX_BYTES}B)`)); }
        chunks.push(chunk);
      });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);
    req.end();
  });
}
async function fetchToFile(u, outName) {
  const outFile = workFile(outName, 'page', { mustExist: false });
  let { url, ip } = await resolvePublic(u);
  let res;
  for (let hop = 0; hop < 6; hop++) {
    res = await getOnce(url, ip);
    if (res.status >= 300 && res.status < 400 && res.headers.location) {
      ({ url, ip } = await resolvePublic(new URL(res.headers.location, url).toString()));
      continue;
    }
    break;
  }
  fs.writeFileSync(outFile, res.body);
  const ok = res.status >= 200 && res.status < 300;
  return { ok, status: res.status, final_url: url.toString(), bytes: res.body.length, file: path.basename(outFile) };
}
/** phreview 用: レビューファイルが作業ディレクトリ直下の実体ファイルか (bash の -f は symlink をたどる) */
function cmdCheckReview(pos) {
  const id = pos[0]; if (!/^\d{1,9}$/.test(id || '')) die('usage: checkreview ID');
  const name = `_ph_review_${id}.md`;
  const cwd = fs.realpathSync(process.cwd());
  let st = null; try { st = fs.lstatSync(path.join(cwd, name)); } catch { st = null; }
  if (!st || !st.isFile()) die(`${name} is missing or not a regular file`);
  out({ ok: true, file: name, bytes: st.size });
}
async function cmdFetch(pos, opt) {
  if (!pos[0] || !opt.out) die('usage: fetch URL --out page-ID.html');
  const r = await fetchToFile(pos[0], String(opt.out));
  out(r); if (!r.ok) fail(1);
}

// ── extract / find: HTML → 商品情報テキスト ─────────────────────────────────
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
  // slice で途中から始まった <style> の中身 (`.cls{...}` / `a[x]{...}`) と属性の残骸を落とす
  .replace(/[.#a-z][\w-]*(?:\[[^\]]*\])*(?:[,\s]+[.#a-z][\w-]*(?:\[[^\]]*\])*)*\s*\{[^}]*\}/g, ' ').replace(/\b[\w-]+="[^"]*"/g, ' ')
  .replace(/\s+/g, ' ').trim();
const section = (h, id, len = 12000) => {
  const i = h.indexOf(`id="${id}"`); if (i < 0) return null;
  return strip(h.slice(i, i + len)).split('ご意見ご要望')[0].split('この商品に関する問題')[0].split('カスタマーレビュー')[0].split('中小企業')[0].slice(0, 1800);
};
function cmdExtract(pos) {
  const f = workFile(pos[0], 'page');
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
  if (lines.length <= 2) lines.push('BODY_HEAD: ' + strip(h).slice(0, 3000)); // Amazon 以外
  out(lines.join('\n'));
}
function cmdFind(pos) {
  const [name, ...needles] = pos; if (!name || needles.length === 0) die('usage: find page-ID.html needle [needle...]');
  const h = decodeHtml(fs.readFileSync(workFile(name, 'page'))).toLowerCase();
  const r = {}; for (const n of needles) r[n] = h.includes(String(n).toLowerCase());
  out(r);
  if (!Object.values(r).every(Boolean)) fail(1);
}
async function cmdSearchAmazon(pos, opt) {
  const jan = pos[0]; if (!/^\d{8,14}$/.test(jan || '')) die('usage: search-amazon JAN --out page-s-ID.html');
  if (!opt.out) die('--out page-s-ID.html is required');
  const r = await fetchToFile(`https://www.amazon.co.jp/s?k=${jan}`, String(opt.out));
  const h = fs.readFileSync(workFile(String(opt.out), 'page'), 'utf8');
  const asins = [...new Set([...h.matchAll(/data-asin="(B0[A-Z0-9]{8})"/g)].map((m) => m[1]))];
  out({ jan, status: r.status, asins: asins.slice(0, 8) });
  if (!r.ok) fail(1);
}

// ── queue / claim / block / release / submit ─────────────────────────────────
async function cmdQueue() { const r = await api('GET', '/generation-queue'); out(r.json); if (r.status !== 200) fail(1); }
async function cmdClaim(pos, opt) {
  if (!opt.run) die('--run RUN_ID is required');
  if (opt.limit !== undefined) die('claim is always 1 draft (lease is 30 min); --limit is not accepted');
  const r = await api('POST', '/generation-queue/claim', { run_id: String(opt.run), limit: 1 });
  out(r.json); if (r.status !== 200) fail(1);
}
async function cmdBlock(pos, opt) {
  const id = pos[0]; if (!/^\d+$/.test(id || '')) die('usage: block ID --run RUN_ID --code CODE --reason-file reason-ID.txt');
  if (!opt.run || !opt.code || !opt['reason-file']) die('--run, --code, --reason-file are required');
  if (!BLOCK_CODES.includes(String(opt.code))) die(`code must be one of ${BLOCK_CODES.join(' / ')}`);
  const reason = fs.readFileSync(workFile(String(opt['reason-file']), 'reason'), 'utf8').trim();
  if (!reason) die('reason file is empty');
  if ([...reason].length > REASON_MAX) die(`reason is longer than ${REASON_MAX} characters`);
  const r = await api('POST', `/drafts/${id}/generation-block`, { run_id: String(opt.run), code: String(opt.code), reason });
  out(r.json); if (r.status !== 200) fail(1);
}
async function cmdRelease(pos, opt) {
  const id = pos[0]; if (!/^\d+$/.test(id || '')) die('usage: release ID --run RUN_ID --reason "text"');
  if (!opt.run) die('--run RUN_ID is required');
  const r = await api('POST', `/drafts/${id}/release`, { run_id: String(opt.run), reason: String(opt.reason || '').slice(0, 300) });
  out(r.json); if (r.status !== 200) fail(1);
}
async function cmdSubmit(pos, opt) {
  const id = pos[0]; if (!/^\d+$/.test(id || '')) die('usage: submit ID --run RUN_ID --file copy-ID.json [--advance] [--note TEXT]');
  if (!opt.run || !opt.file) die('--run and --file are required');
  const c = JSON.parse(fs.readFileSync(workFile(String(opt.file), 'copy'), 'utf8'));
  const cap = String(c.caption || '');
  const i = cap.indexOf('【仕様】');
  const outputs = {
    rakuten_title: c.rakuten_title, yahoo_title: c.yahoo_title, desc_catch: c.headline,
    desc_features: (i >= 0 ? cap.slice(0, i) : cap).trim(),
    desc_spec: i >= 0 ? cap.slice(i + '【仕様】'.length).trim() : '',
    desc_notes: c.notes,
  };
  for (const k of KINDS) {
    if (!outputs[k] || !String(outputs[k]).trim()) { if (opt.advance === true) die(`outputs.${k} is empty (advance needs all 6)`); delete outputs[k]; }
  }
  const r = await api('POST', `/drafts/${id}/ai-outputs`, {
    run_id: String(opt.run), outputs, advance: opt.advance === true,
    model_note: String(opt.note || `claude+codex ${new Date().toISOString().slice(0, 10)}`).slice(0, 200),
  });
  out(r.json); if (r.status !== 200) fail(1);
}

// ── lint: copy_lint.py を固定パスで ──────────────────────────────────────────
function cmdLint(pos) {
  const f = workFile(pos[0], 'copy');
  const script = path.join(HERE, 'copy_lint.py');
  if (!fs.existsSync(script)) die(`copy_lint.py not found next to phq.mjs (${script})`);
  const py = process.platform === 'win32' ? 'python' : 'python3';
  const r = spawnSync(py, ['-X', 'utf8', script, f], { encoding: 'utf8', env: { ...process.env, PYTHONUTF8: '1' } });
  process.stdout.write(r.stdout || ''); process.stderr.write(r.stderr || '');
  fail(r.status ?? 1);
}

const { pos, opt } = parseArgs(process.argv.slice(2));
const cmd = pos.shift();
const table = { queue: cmdQueue, claim: cmdClaim, block: cmdBlock, release: cmdRelease, submit: cmdSubmit, fetch: cmdFetch, extract: cmdExtract, find: cmdFind, lint: cmdLint, 'search-amazon': cmdSearchAmazon, checkreview: cmdCheckReview };
if (!cmd || !table[cmd]) die(`usage: phq <${Object.keys(table).join('|')}> ...`);
Promise.resolve(table[cmd](pos, opt)).catch((e) => { process.stderr.write(`phq: ${String(e.message || e)}\n`); fail(1); });
