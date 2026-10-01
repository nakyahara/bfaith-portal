#!/usr/bin/env node
/**
 * phlp — LP 構成の AI 生成 (段階1) の固定機能 CLI。依存パッケージなし。
 *
 * 正本 = AI_reference『商品ハブ_LP構成AI生成_段階1設計_20260930.md』。
 * 作りは `phq.mjs` と同じ (2026-08-28 の Codex R1/R2 critical 対応をそのまま踏襲する)。
 *
 * なぜあるか: Claude Code に curl/node/python を許可すると deny は迂回でき、トークンを読ませられる。
 * そこで **Claude にはこの CLI (と ./phlpreview) だけを許可し、トークンはこの中でしか扱わない**。
 *   - HTTP の相手は service-api 固定 (Base URL・メソッド・パスをここで決める)。任意 URL へは送れない
 *   - **ファイル引数は作業ディレクトリ直下の決まった名前だけ** (out-<ID>.md / lint-<ID>.json / reason-<ID>.txt)。
 *     パス区切りを含む名前・symlink は拒否 → トークンや .env をこの CLI 経由で読ませない
 *   - 画像は `./phlp images <ID>` でしか落とせない。**保存先も img-<ID>-<n>.jpg 固定**。
 *     サーバ側も「その依頼の packet に固定済みの画像」しか配らないので、任意の画像は取れない
 *   - claim は常に 1 件。lease は 40 分
 *   - トークンは標準出力・エラーに一切出さない
 *
 * 使い方 (作業ディレクトリ = C:\tools\ph-nightly\work で `./phlp <cmd>`):
 *   ./phlp queue                                    キューの内訳 (仕事があるか)
 *   ./phlp claim   --run RUN_ID                     1 件 claim (材料 + 仕様書の全文)
 *   ./phlp images  ID                               その依頼の商品画像を img-ID-1.jpg … に落とす
 *   ./phlp reserve ID --model MODEL                 **AI を呼ぶ前に必ず**予約する
 *   ./phlp result  ID --accepted --file out-ID.md [--lint lint-ID.json] [--rounds N]
 *   ./phlp result  ID --rejected --reason-file reason-ID.txt [--lint lint-ID.json] [--rounds N]
 *   ./phlp fail    ID --code CODE --message "text"  予約の**前**だけ (生成できない材料)
 *   ./phlp release ID --reason "text"               予約の**前**だけ (一時障害)
 *   ./phlp clean   ID                               その依頼の一時ファイルを消す (rm は使えない)
 *
 * 🚨 予約の **後** に失敗したら `fail` ではなく `result --rejected` を出す (設計 §4.3b)。
 *    job だけ進んで generation が reserved のまま残る経路を作らない。
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';

const BASE = process.env.PH_LP_BASE || 'https://bfaith-portal.onrender.com/apps/product-hub/service-api';
const PROMPT_VERSION = 'lp-compose-v1';     // サーバ側の lib/lp-compose.js と一致していること
const MAX_IMAGES = 6;
const REASON_MAX = 1000;
const OUT_MAX = 200_000;
const LINT_MAX = 100_000;
const IMAGE_MAX_BYTES = 8 * 1024 * 1024;
const FAIL_CODES = ['SPEC_UNREADABLE', 'MATERIAL_TOO_THIN', 'IMAGES_UNAVAILABLE', 'OTHER'];

// 用途別のファイル名 (basename のみ。区切り文字・.. を含む名前は正規表現で弾かれる)
const NAME = {
  out: /^out-[A-Za-z0-9_-]{1,40}\.md$/,
  lint: /^lint-[A-Za-z0-9_-]{1,40}\.json$/,
  reason: /^reason-[A-Za-z0-9_-]{1,40}\.txt$/,
};
const ID_RE = /^[1-9]\d*$/;

function die(msg, code = 2) { process.stderr.write(`phlp: ${msg}\n`); process.exit(code); }
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
      if (next === undefined || next.startsWith('--')) { opt[k] = true; } else { opt[k] = next; i++; }
    } else pos.push(a);
  }
  return { pos, opt };
}

/** 作業ディレクトリ直下の決まった名前だけ。symlink も拒否 (phq.mjs と同じ) */
function safePath(name, kind, { mustExist = true } = {}) {
  const n = String(name || '');
  if (!NAME[kind] || !NAME[kind].test(n)) die(`ファイル名が不正です (${kind}: ${NAME[kind]})`);
  const p = path.resolve(process.cwd(), n);
  if (path.dirname(p) !== path.resolve(process.cwd())) die('作業ディレクトリ直下のファイルだけです');
  if (mustExist) {
    let st;
    try { st = fs.lstatSync(p); } catch { die(`ファイルがありません: ${n}`); }
    if (!st.isFile()) die(`通常のファイルではありません: ${n}`);
  } else if (fs.existsSync(p)) {
    const st = fs.lstatSync(p);
    if (!st.isFile()) die(`通常のファイルではありません: ${n}`);
  }
  return p;
}

const jobId = (v) => (ID_RE.test(String(v || '')) ? String(v) : die('依頼の ID は正の整数です'));

// ── token: ここでしか読まない・どこにも出さない ─────────────────────────────
function token() {
  if (process.env.PH_SERVICE_TOKEN) return process.env.PH_SERVICE_TOKEN.trim();
  const f = path.join(os.homedir(), '.claude', 'secrets', 'ph-service-token.txt');
  try { return fs.readFileSync(f, 'utf8').trim(); }
  catch { die('service token not found (~/.claude/secrets/ph-service-token.txt or PH_SERVICE_TOKEN)'); }
}

// 再試行の回数。本番は 4 回 (8 秒間隔)。テストは 1 にして待たせない
const RETRIES = (() => { const n = Number.parseInt(process.env.PH_LP_RETRIES || '', 10); return Number.isInteger(n) && n >= 1 && n <= 10 ? n : 4; })();

async function api(method, p, body, { retries = RETRIES, headers = {} } = {}) {
  const tok = token();
  let last;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(BASE + p, {
        method,
        headers: { Authorization: `Bearer ${tok}`, 'Content-Type': 'application/json', ...headers },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(90_000),
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

/** lease はこの CLI の中だけで持ち回る (Claude には見せない。work/ の lease-<ID>.json に置く) */
const leaseFile = (id) => path.resolve(process.cwd(), `lease-${id}.json`);
function saveLease(id, data) { fs.writeFileSync(leaseFile(id), JSON.stringify(data), 'utf8'); }
function loadLease(id) {
  try { return JSON.parse(fs.readFileSync(leaseFile(id), 'utf8')); }
  catch { die(`この依頼の lease がありません (先に ./phlp claim してください): ${id}`); }
}

// ── コマンド ────────────────────────────────────────────────

async function cmdQueue() {
  const r = await api('GET', '/lp-compose/queue');
  out(r.json);
  if (r.status !== 200) fail(1);
}

async function cmdClaim(opt) {
  const run = String(opt.run || '').trim();
  if (!run) die('--run RUN_ID が要ります');
  const r = await api('POST', '/lp-compose/claim', { runner_run_id: run.slice(0, 80) });
  if (r.status !== 200) { out(r.json); return fail(1); }
  const job = r.json.job;
  if (!job) { out({ job: null, exhausted: r.json.exhausted || false, note: '仕事はありません' }); return; }
  // lease と packet_hash は CLI が持つ。Claude には出さない
  saveLease(job.job_id, {
    lease_token: job.lease_token, packet_hash: job.packet_hash, run,
    // 証跡に file_id が要る。packet の並びをそのまま覚えておき、
    // images で n 番目 ↔ file_id を紐づける (Claude に手で写させない)
    image_file_ids: (job.packet.images || []).map((im) => im.file_id),
  });
  // 仕様書の全文はファイルに落とす (プロンプトに貼るのは Claude の仕事)
  fs.writeFileSync(path.resolve(process.cwd(), `spec-${job.job_id}.md`), job.spec.body, 'utf8');
  out({
    job_id: job.job_id,
    draft_id: job.draft_id,
    lease_until: job.lease_until,
    spec: { id: job.spec.id, title: job.spec.title, file: `spec-${job.job_id}.md`, chars: job.spec.body.length },
    packet: {
      name: job.packet.name,
      ne_code: job.packet.ne_code,
      product_info: job.packet.product_info,
      color_variations: job.packet.color_variations,
      images: job.packet.images.length,
    },
    next: `./phlp images ${job.job_id}`,
  });
}

/** その依頼の商品画像を img-<ID>-<n>.jpg に落とす。サーバが packet 固定分しか配らない */
async function cmdImages(id) {
  const lease = loadLease(id);
  const tok = token();
  const saved = [];
  for (let n = 1; n <= MAX_IMAGES; n++) {
    let res;
    try {
      res = await fetch(`${BASE}/lp-compose/jobs/${id}/images/${n}`, {
        headers: { Authorization: `Bearer ${tok}`, 'X-LP-Compose-Lease': lease.lease_token },
        signal: AbortSignal.timeout(90_000),
      });
    } catch (e) { out({ saved, error: String(e.message || e) }); return fail(1); }
    if (res.status === 404) break;             // そこまで。packet の枚数ぶんで終わる
    if (!res.ok) { out({ saved, status: res.status, error: '画像を取得できませんでした' }); return fail(1); }
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > IMAGE_MAX_BYTES) { out({ saved, error: '画像が大きすぎます' }); return fail(1); }
    const name = `img-${id}-${n}.jpg`;
    fs.writeFileSync(path.resolve(process.cwd(), name), buf);
    saved.push({
      file: name, bytes: buf.length,
      file_id: (lease.image_file_ids || [])[n - 1] || null,
      sha256: crypto.createHash('sha256').update(buf).digest('hex'),
    });
  }
  // 証跡は result のときに要る。CLI が覚えておく (Claude に sha256 を手で写させない)
  fs.writeFileSync(path.resolve(process.cwd(), `imgs-${id}.json`), JSON.stringify(saved), 'utf8');
  out({ saved: saved.map((s) => ({ file: s.file, bytes: s.bytes })), count: saved.length });
}

async function cmdReserve(id, opt) {
  const lease = loadLease(id);
  const model = String(opt.model || 'claude-opus-5').trim();
  const r = await api('POST', `/lp-compose/jobs/${id}/reserve`, {
    lease_token: lease.lease_token, model, prompt_version: PROMPT_VERSION,
  });
  if (r.status !== 200) { out(r.json); return fail(1); }
  saveLease(id, { ...lease, generation_id: r.json.generation_id });
  out({ generation_id: r.json.generation_id, note: 'ここから先の失敗は fail ではなく result --rejected' });
}

async function cmdResult(id, opt) {
  const lease = loadLease(id);
  if (!lease.generation_id) die('先に ./phlp reserve してください');
  const accepted = !!opt.accepted;
  const rejected = !!opt.rejected;
  if (accepted === rejected) die('--accepted か --rejected のどちらかが要ります');

  let output = null;
  if (accepted) {
    const p = safePath(opt.file, 'out');
    output = fs.readFileSync(p, 'utf8');
    if (!output.trim()) die('構成の本文が空です');
    if (output.length > OUT_MAX) die(`構成が大きすぎます (${OUT_MAX} 文字まで)`);
  }
  let reason = null;
  if (rejected) {
    if (opt['reason-file']) reason = fs.readFileSync(safePath(opt['reason-file'], 'reason'), 'utf8').slice(0, REASON_MAX);
    else if (typeof opt.reason === 'string') reason = opt.reason.slice(0, REASON_MAX);
    if (!reason || !reason.trim()) die('--reason-file か --reason が要ります');
  }
  let lint = null;
  if (opt.lint) {
    const raw = fs.readFileSync(safePath(opt.lint, 'lint'), 'utf8');
    if (raw.length > LINT_MAX) die(`lint が大きすぎます (${LINT_MAX} 文字まで)`);
    try { lint = JSON.parse(raw); } catch { die('lint が JSON ではありません'); }
  }
  const rounds = opt.rounds === undefined ? null : Number.parseInt(String(opt.rounds), 10);
  if (rounds !== null && (!Number.isInteger(rounds) || rounds < 0 || rounds > 10)) die('--rounds は 0〜10 です');

  // 証跡は images のときに CLI が記録したものをそのまま出す (Claude に書かせない)
  let images = null;
  try { images = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), `imgs-${id}.json`), 'utf8')); } catch { images = null; }
  const receipt = images && images.length
    ? { images: images.filter((im) => im.file_id).map((im) => ({ file_id: im.file_id, sha256: im.sha256, bytes: im.bytes })) }
    : null;

  const r = await api('POST', `/lp-compose/generations/${lease.generation_id}/result`, {
    packet_hash: lease.packet_hash,
    verdict: accepted ? 'accepted' : 'rejected',
    output, lint, review_rounds: rounds, reason, receipt,
  });
  out(r.json);
  if (r.status !== 200) fail(1);
}

async function cmdFail(id, opt) {
  const lease = loadLease(id);
  const code = String(opt.code || 'OTHER').trim();
  if (!FAIL_CODES.includes(code)) die(`--code は ${FAIL_CODES.join(' / ')} です`);
  const r = await api('POST', `/lp-compose/jobs/${id}/fail`, {
    lease_token: lease.lease_token, code, message: String(opt.message || '').slice(0, 500),
  });
  out(r.json);
  if (r.status !== 200) fail(1);
}

async function cmdRelease(id, opt) {
  const lease = loadLease(id);
  const r = await api('POST', `/lp-compose/jobs/${id}/release`, {
    lease_token: lease.lease_token, reason: String(opt.reason || '').slice(0, 300),
  });
  out(r.json);
  if (r.status !== 200) fail(1);
}

/**
 * 検品に渡すファイルが実体かを見る (./phlpreview が先に呼ぶ)。
 * bash の -f は symlink をたどるので、lstat で見るのはこちらの仕事 (phq.mjs と同じ)。
 */
function cmdCheckReview(id) {
  const n = `_lp_review_${id}.md`;
  const p2 = path.resolve(process.cwd(), n);
  if (path.dirname(p2) !== path.resolve(process.cwd())) die('作業ディレクトリ直下のファイルだけです');
  let st;
  try { st = fs.lstatSync(p2); } catch { die(`ファイルがありません: ${n}`); }
  if (!st.isFile()) die(`通常のファイルではありません: ${n}`);
  if (st.size > LINT_MAX * 4) die(`検品に渡すファイルが大きすぎます: ${n}`);
  out({ ok: true, file: n, bytes: st.size });
}

/** その依頼の一時ファイルを消す (`rm` は allowlist に無い。9/1 に rm -f a b c が拒否されてゴミが残った) */
function cmdClean(id) {
  const removed = [];
  const names = [
    `spec-${id}.md`, `imgs-${id}.json`, `lease-${id}.json`,
    `out-${id}.md`, `lint-${id}.json`, `reason-${id}.txt`, `_lp_review_${id}.md`,
    ...Array.from({ length: MAX_IMAGES }, (_, i) => `img-${id}-${i + 1}.jpg`),
  ];
  for (const n of names) {
    const p = path.resolve(process.cwd(), n);
    try {
      const st = fs.lstatSync(p);
      if (st.isFile()) { fs.unlinkSync(p); removed.push(n); }
    } catch { /* 無ければ何もしない */ }
  }
  out({ removed });
}

// ── 入口 ────────────────────────────────────────────────────
const { pos, opt } = parseArgs(process.argv.slice(2));
const cmd = pos[0];
switch (cmd) {
  case 'queue': await cmdQueue(); break;
  case 'claim': await cmdClaim(opt); break;
  case 'images': await cmdImages(jobId(pos[1])); break;
  case 'reserve': await cmdReserve(jobId(pos[1]), opt); break;
  case 'result': await cmdResult(jobId(pos[1]), opt); break;
  case 'fail': await cmdFail(jobId(pos[1]), opt); break;
  case 'release': await cmdRelease(jobId(pos[1]), opt); break;
  case 'checkreview': cmdCheckReview(jobId(pos[1])); break;
  case 'clean': cmdClean(jobId(pos[1])); break;
  default:
    die('使い方: ./phlp queue | claim --run RUN_ID | images ID | reserve ID | result ID --accepted --file out-ID.md | fail ID --code CODE | release ID | clean ID');
}
