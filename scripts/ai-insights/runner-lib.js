// ai-insights PC runner 共通ライブラリ (週次/月次で共有。PR-3 で run-weekly-report.js から切り出し)
// 依存パッケージなし (Node 20+ 組み込みのみ)

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));

export const INPUT_SCHEMA_VERSION = '1.0';
export const RUNNER_ID = `pc-runner@${process.env.COMPUTERNAME || 'unknown'}`;

// ── .env / 設定 ──────────────────────────────────────────────────────

function loadDotEnv() {
  const file = path.join(SCRIPT_DIR, '.env');
  if (!fs.existsSync(file)) return;
  for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (!m || line.trim().startsWith('#')) continue;
    if (process.env[m[1]] === undefined) process.env[m[1]] = m[2];
  }
}

export function config() {
  loadDotEnv();
  const required = ['PORTAL_BASE_URL', 'AI_READ_TOKEN', 'AI_INSIGHT_SERVICE_TOKEN', 'GCHAT_WEBHOOK_URL'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length) throw new Error(`.env に不足: ${missing.join(', ')} (${path.join(SCRIPT_DIR, '.env')})`);
  return {
    base: process.env.PORTAL_BASE_URL.replace(/\/+$/, ''),
    readToken: process.env.AI_READ_TOKEN,
    serviceToken: process.env.AI_INSIGHT_SERVICE_TOKEN,
    webhook: process.env.GCHAT_WEBHOOK_URL,
    claudeCmd: process.env.CLAUDE_CMD || 'claude',
    deadlineHour: Number(process.env.DEADLINE_HOUR || 18),
    claudeTimeoutMs: Number(process.env.CLAUDE_TIMEOUT_MS || 10 * 60 * 1000),
    claudeRetryBaseMs: Number(process.env.CLAUDE_RETRY_BASE_MS || 30000), // smoke で短縮可
  };
}

// ── ログ ─────────────────────────────────────────────────────────────

let logStream = null;
export function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
  try {
    if (!logStream) {
      const dir = path.join(SCRIPT_DIR, 'logs');
      fs.mkdirSync(dir, { recursive: true });
      const ym = new Date().toISOString().slice(0, 7);
      logStream = fs.createWriteStream(path.join(dir, `${ym}.log`), { flags: 'a' });
    }
    logStream.write(`${line}\n`);
  } catch { /* ログ書込失敗で本処理は止めない */ }
}
export function notify(status, extra = '') {
  log(`[NOTIFY:status=${status}]${extra ? ` ${extra}` : ''}`);
}
export function closeLog() {
  logStream?.end();
}

// ── 日付 (JST。standalone のため lib/jst-date.js を最小内製) ──────────

export function nowDate() {
  const injected = process.env.AI_RUNNER_NOW; // テスト用の時刻注入
  return injected ? new Date(injected) : new Date();
}
export function jstParts(d = nowDate()) {
  const t = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  const pad = (n) => String(n).padStart(2, '0');
  return {
    ymd: `${t.getUTCFullYear()}-${pad(t.getUTCMonth() + 1)}-${pad(t.getUTCDate())}`,
    hour: t.getUTCHours(),
    day: t.getUTCDate(),
  };
}
export function addDaysYmd(ymd, delta) {
  const [y, m, d] = ymd.split('-').map(Number);
  const t = new Date(Date.UTC(y, m - 1, d) + delta * 86400000);
  const pad = (n) => String(n).padStart(2, '0');
  return `${t.getUTCFullYear()}-${pad(t.getUTCMonth() + 1)}-${pad(t.getUTCDate())}`;
}
export function lastCompletedWeekStart() {
  const { ymd } = jstParts();
  const [y, m, d] = ymd.split('-').map(Number);
  const dow = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  return addDaysYmd(addDaysYmd(ymd, -((dow + 6) % 7)), -7);
}
export function addMonthsYm(ym, delta) {
  const [y, m] = ym.split('-').map(Number);
  const t = y * 12 + (m - 1) + delta;
  return `${Math.floor(t / 12)}-${String((t % 12) + 1).padStart(2, '0')}`;
}
export function previousMonthYm() {
  return addMonthsYm(jstParts().ymd.slice(0, 7), -1);
}

// ── HTTP ─────────────────────────────────────────────────────────────

export async function http(url, { method = 'GET', headers = {}, body, timeoutMs = 60000, retries = 0 } = {}) {
  for (let attempt = 0; ; attempt++) {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method, signal: ac.signal,
        headers: body ? { 'content-type': 'application/json', ...headers } : headers,
        body: body ? JSON.stringify(body) : undefined,
      });
      const text = await res.text();
      let json = null;
      try { json = JSON.parse(text); } catch { /* 非JSON応答 */ }
      if (!res.ok) {
        const err = new Error(`HTTP ${res.status} ${url}: ${json?.error || text.slice(0, 200)}`);
        err.status = res.status;
        err.body = json;
        throw err;
      }
      return json;
    } catch (e) {
      // 4xx は再試行しない (認証・バリデーション)。5xx/ネットワークのみ retry
      if (attempt >= retries || (e.status && e.status < 500)) throw e;
      const wait = 5000 * (attempt + 1);
      log(`  retry ${attempt + 1}/${retries} in ${wait}ms: ${e.message}`);
      await new Promise((r) => setTimeout(r, wait));
    } finally {
      clearTimeout(timer);
    }
  }
}

// ── claude -p 呼び出し ────────────────────────────────────────────────

/** コマンド文字列を引用符対応でトークン分割 ("C:\Program Files\..." 対応) */
export function splitCommand(cmd) {
  const tokens = [];
  for (const m of String(cmd).matchAll(/"([^"]*)"|(\S+)/g)) tokens.push(m[1] ?? m[2]);
  return tokens;
}

function killTree(child) {
  if (process.platform === 'win32') {
    // cmd.exe 配下の node/claude まで含めてツリーごと終了 (kill() では孫が残る)
    try { spawn('taskkill', ['/pid', String(child.pid), '/T', '/F'], { windowsHide: true }); } catch { /* noop */ }
  } else {
    child.kill('SIGKILL');
  }
}

export function runClaude(cfg, prompt) {
  return new Promise((resolve, reject) => {
    // prompt は stdin で渡す (数十KB になるため引数長制限を回避)
    const args = ['-p', '--output-format', 'json'];
    const cmdParts = splitCommand(cfg.claudeCmd);
    const child = process.platform === 'win32'
      ? spawn('cmd.exe', ['/c', ...cmdParts, ...args], { windowsHide: true })
      : spawn(cmdParts[0], [...cmdParts.slice(1), ...args]);
    let out = '', err = '';
    const timer = setTimeout(() => {
      killTree(child);
      reject(new Error(`claude timeout ${cfg.claudeTimeoutMs}ms`));
    }, cfg.claudeTimeoutMs);
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { err += d; });
    child.on('error', (e) => { clearTimeout(timer); reject(e); });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) return reject(new Error(`claude exit ${code}: ${err.slice(0, 500) || out.slice(0, 500)}`));
      resolve(out);
    });
    child.stdin.write(prompt);
    child.stdin.end();
  });
}

/** claude --output-format json の envelope から本文 JSON を抽出 */
export function parseClaudeOutput(stdout) {
  let resultText = stdout;
  try {
    const envelope = JSON.parse(stdout);
    if (envelope.is_error) throw new Error(`claude reported error: ${String(envelope.result).slice(0, 300)}`);
    if (typeof envelope.result === 'string') resultText = envelope.result;
  } catch (e) {
    if (String(e.message).startsWith('claude reported error')) throw e;
    // envelope でなければ生テキストとして続行
  }
  const start = resultText.indexOf('{');
  const end = resultText.lastIndexOf('}');
  if (start < 0 || end <= start) throw new Error('no JSON object in claude output');
  return JSON.parse(resultText.slice(start, end + 1));
}

export function classifyClaudeError(e) {
  const m = String(e.message || '').toLowerCase();
  if (m.includes('limit') || m.includes('rate') || m.includes('usage')) return 'claude_limit';
  if (m.includes('auth') || m.includes('login') || m.includes('credential')) return 'claude_auth';
  if (m.includes('timeout')) return 'claude_timeout';
  if (m.includes('json')) return 'claude_invalid_json';
  return 'generation_failed';
}

/** 生成 3回リトライ → 失敗時 null (呼び出し側でフォールバック) */
export async function generateWithRetry(cfg, prompt, validate) {
  let lastErrClass = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      log(`claude -p 生成 attempt ${attempt}/3 ...`);
      const stdout = await runClaude(cfg, prompt);
      return { body: validate(parseClaudeOutput(stdout)), errorClass: null };
    } catch (e) {
      lastErrClass = classifyClaudeError(e);
      log(`  生成失敗 (${lastErrClass}): ${String(e.message).slice(0, 300)}`);
      if (lastErrClass === 'claude_auth') break; // 認証切れは再試行しても無駄
      if (attempt < 3) await new Promise((r) => setTimeout(r, cfg.claudeRetryBaseMs * attempt));
    }
  }
  return { body: null, errorClass: lastErrClass };
}

// ── service API ──────────────────────────────────────────────────────

export async function serviceCall(cfg, pathName, body) {
  return http(`${cfg.base}/api/ai-insights/service${pathName}`, {
    method: 'POST', headers: { authorization: `Bearer ${cfg.serviceToken}` }, body, retries: 2,
  });
}

export function startHeartbeat(cfg, jobId) {
  const t = setInterval(() => {
    serviceCall(cfg, `/jobs/${jobId}/heartbeat`).catch((e) => log(`  heartbeat失敗 (続行): ${e.message}`));
  }, 5 * 60 * 1000);
  t.unref?.();
  return () => clearInterval(t);
}

/** 安定ハッシュ (取得時刻を除外して同一データの再取得を同一入力とみなす) */
export function stableInputHash(input) {
  const clone = JSON.parse(JSON.stringify(input));
  if (clone.meta) { delete clone.meta.generated_at; delete clone.meta.data_as_of; }
  return crypto.createHash('sha256').update(JSON.stringify(clone)).digest('hex');
}

// ── 投稿 (webhook 呼び出し以降の失敗は全て「成否不明」扱い。throw しない) ──
// 外側 catch の /failed 申告に到達させると claimed_for_posting → 自動再投稿になり、
// 投稿済みのケースで二重投稿するため (要件 §8.1: 不明時は孤児回収 → 人間照合のみ)

export async function postAndFinalize(cfg, jobId, text, publicId) {
  let messageId = null;
  try {
    const res = await http(cfg.webhook, {
      method: 'POST', body: { text }, timeoutMs: 30000, retries: 0, // 二重投稿防止のため retry しない
    });
    messageId = res?.name || null;
  } catch (e) {
    log(`GChat投稿の成否不明: ${e.message}`);
    notify('failed_posting_uncertain', `public_id=${publicId} → 30分後に⚙️画面の要照合に出ます`);
    return false;
  }
  try {
    const bodyHash = crypto.createHash('sha256').update(text).digest('hex');
    await serviceCall(cfg, `/jobs/${jobId}/posted`, { gchat_message_id: messageId, body_hash: bodyHash });
  } catch (e) {
    // 投稿は成功しているが posted 記録に失敗 → posting のまま残す (孤児回収 → 人間照合)
    log(`投稿成功だが posted 記録に失敗: ${e.message}`);
    notify('failed_posting_uncertain', `public_id=${publicId} 投稿済み・記録失敗 → ⚙️画面で「投稿済み扱い」を選択`);
    return false;
  }
  return true;
}
