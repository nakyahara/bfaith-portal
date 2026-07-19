#!/usr/bin/env node
// ai-insights PC runner: 週次AI経営レポート (PR-2)
//
// 配置: C:\tools\ai-insights\ (OneDrive・全角パス配下に置かない。repo 正本 = scripts/ai-insights/)
// 起動: Task Scheduler 火曜 8:30 + 1時間ごと繰り返し (〜18:00) + StartWhenAvailable。
//       ログインユーザー実行 (Claude サブスク認証がユーザープロファイル紐付きのため SYSTEM 禁止)
//
// フロー (要件 §4/§7/§8):
//   report-input 取得 → (blocked なら締切まで待つ / 締切超過は「生成なし」投稿)
//   → claim (原子的) → claude -p で論点生成 (失敗3回でフォールバック=事実の機械整形)
//   → /report 保存 → /posting → GChat webhook → /posted
//   投稿の成否不明 (webhook 送信後のエラー) は /failed を呼ばず放置
//   → サーバ側の孤児回収が reconciliation_required にして ⚙️ 画面で人間照合
//
// 終端ログマーカー: [NOTIFY:status=ok|ok_repost|fallback|skip|skip_busy|retry_later|
//                   blocked_notice|reconciliation_required|failed|failed_posting_uncertain]

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
  PROMPT_VERSION, buildPrompt, validateOutput, buildFallbackBody,
  buildGChatMessage, buildBlockedNotice, enrichFactsDisplay,
} from './weekly-prompt.js';

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const INPUT_SCHEMA_VERSION = '1.0';
const RUNNER_ID = `pc-runner@${process.env.COMPUTERNAME || 'unknown'}`;

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

function config() {
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
function log(msg) {
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
function notify(status, extra = '') {
  log(`[NOTIFY:status=${status}]${extra ? ` ${extra}` : ''}`);
}

// ── 日付 (JST。runner は standalone のため lib/jst-date.js を最小内製) ──

function nowDate() {
  const injected = process.env.AI_RUNNER_NOW; // テスト用の時刻注入
  return injected ? new Date(injected) : new Date();
}
function jstParts(d = nowDate()) {
  const t = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  const pad = (n) => String(n).padStart(2, '0');
  return {
    ymd: `${t.getUTCFullYear()}-${pad(t.getUTCMonth() + 1)}-${pad(t.getUTCDate())}`,
    hour: t.getUTCHours(),
  };
}
function addDaysYmd(ymd, delta) {
  const [y, m, d] = ymd.split('-').map(Number);
  const t = new Date(Date.UTC(y, m - 1, d) + delta * 86400000);
  const pad = (n) => String(n).padStart(2, '0');
  return `${t.getUTCFullYear()}-${pad(t.getUTCMonth() + 1)}-${pad(t.getUTCDate())}`;
}
function lastCompletedWeekStart() {
  const { ymd } = jstParts();
  const [y, m, d] = ymd.split('-').map(Number);
  const dow = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  return addDaysYmd(addDaysYmd(ymd, -((dow + 6) % 7)), -7);
}

// ── HTTP ─────────────────────────────────────────────────────────────

async function http(url, { method = 'GET', headers = {}, body, timeoutMs = 60000, retries = 0 } = {}) {
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

function runClaude(cfg, prompt) {
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

function classifyClaudeError(e) {
  const m = String(e.message || '').toLowerCase();
  if (m.includes('limit') || m.includes('rate') || m.includes('usage')) return 'claude_limit';
  if (m.includes('auth') || m.includes('login') || m.includes('credential')) return 'claude_auth';
  if (m.includes('timeout')) return 'claude_timeout';
  if (m.includes('json')) return 'claude_invalid_json';
  return 'generation_failed';
}

// ── service API ──────────────────────────────────────────────────────

function serviceHeaders(cfg) {
  return { authorization: `Bearer ${cfg.serviceToken}` };
}
async function serviceCall(cfg, pathName, body) {
  return http(`${cfg.base}/api/ai-insights/service${pathName}`, {
    method: 'POST', headers: serviceHeaders(cfg), body, retries: 2,
  });
}

function startHeartbeat(cfg, jobId) {
  const t = setInterval(() => {
    serviceCall(cfg, `/jobs/${jobId}/heartbeat`).catch((e) => log(`  heartbeat失敗 (続行): ${e.message}`));
  }, 5 * 60 * 1000);
  t.unref?.();
  return () => clearInterval(t);
}

/** 安定ハッシュ (取得時刻を除外して同一データの再取得を同一入力とみなす) */
function stableInputHash(input) {
  const clone = JSON.parse(JSON.stringify(input));
  if (clone.meta) { delete clone.meta.generated_at; delete clone.meta.data_as_of; }
  return crypto.createHash('sha256').update(JSON.stringify(clone)).digest('hex');
}

// ── 投稿 (webhook 送信後のエラーは「成否不明」として /failed を呼ばない) ──

async function postAndFinalize(cfg, jobId, text, publicId) {
  // ⚠️ webhook 呼び出し以降の失敗は全て「成否不明」扱いで throw しない。
  //    外側 catch の /failed 申告に到達させると claimed_for_posting → 自動再投稿になり、
  //    投稿済みのケースで二重投稿するため (要件 §8.1: 不明時は孤児回収 → 人間照合のみ)
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

// ── メイン ───────────────────────────────────────────────────────────

async function main() {
  const cfg = config();
  const args = process.argv.slice(2);
  const periodArg = args.find((a) => a.startsWith('--period-start='))?.split('=')[1];
  const periodStart = periodArg || lastCompletedWeekStart();
  const periodEnd = addDaysYmd(periodStart, 7);
  const periodLabel = `${periodStart}〜${addDaysYmd(periodEnd, -1)}`;
  log(`=== 週次AI経営レポート runner 開始: ${periodLabel} ===`);

  // 1. 入力取得 (read-only。claim より先に blocked 判定するため)
  const input = await http(
    `${cfg.base}/api/ai-insights/report-input?type=weekly&period_start=${periodStart}`,
    { headers: { 'x-read-token': cfg.readToken }, retries: 3 },
  );
  // 金額の表示用文字列 (*_disp、万円/億円) を機械追加。AI は disp をそのまま引用する契約
  enrichFactsDisplay(input);

  // --dry-run: サーバ書き込み・GChat 投稿を一切せず、本文プレビューだけをログに出す
  if (args.includes('--dry-run')) {
    let previewText;
    if (input.constraints?.generation === 'blocked') {
      previewText = buildBlockedNotice(input, 'PREVIEW');
    } else {
      let body = null;
      const prompt = buildPrompt(input);
      try {
        body = validateOutput(parseClaudeOutput(await runClaude(cfg, prompt)), input);
      } catch (e) {
        log(`dry-run: claude生成失敗 → フォールバックでプレビュー (${e.message})`);
        body = buildFallbackBody(input);
      }
      previewText = buildGChatMessage(body, input, 'PREVIEW', { detailUrl: `${cfg.base}/apps/ai-insights` });
    }
    log(`---- GChat本文プレビュー (未投稿) ----\n${previewText}\n---- プレビューここまで ----`);
    notify('dry_run');
    return 0;
  }

  // 2. blocked (必須データ全滅) → 締切前なら次の定期実行に任せて静かに終了
  const { ymd: today, hour } = jstParts();
  const reportDay = addDaysYmd(periodStart, 8); // 火曜
  const beforeDeadline = today < reportDay || (today === reportDay && hour < cfg.deadlineHour);
  if (input.constraints?.generation === 'blocked' && beforeDeadline) {
    notify('retry_later', `必須データ未充足 (締切 ${reportDay} ${cfg.deadlineHour}:00 JST まで再試行)`);
    return 0;
  }

  // 3. claim (原子的獲得)
  const claim = await serviceCall(cfg, '/jobs/claim', {
    report_type: 'weekly', period_start: periodStart, period_end: periodEnd,
    edition: 'final', claimed_by: RUNNER_ID,
  });
  if (claim.result === 'already_posted') { notify('skip', '投稿済み'); return 0; }
  if (claim.result === 'busy') { notify('skip_busy', '別プロセスが実行中'); return 0; }
  if (claim.result === 'reconciliation_required') {
    notify('reconciliation_required', '⚙️ AI経営レポート設定画面で投稿有無を照合してください');
    return 0;
  }
  const jobId = claim.job.job_id;

  // 3a. 生成済みレポートの投稿再開 (repost / 投稿失敗後)
  if (claim.result === 'claimed_for_posting') {
    const report = claim.report;
    const body = JSON.parse(report.body_json);
    const pseudoInput = { meta: { period_label: periodLabel }, coverage: [] };
    const text = report.data_quality_status === 'blocked'
      // 欠損理由は生成時に body.data_notes へ保存済み → 再投稿でも復元できる
      ? buildBlockedNotice(pseudoInput, report.public_id, body.data_notes || null)
      : buildGChatMessage(body, pseudoInput, report.public_id, { detailUrl: `${cfg.base}/apps/ai-insights` });
    const okPost = await postAndFinalize(cfg, jobId, text, report.public_id);
    if (okPost) notify('ok_repost', report.public_id);
    return okPost ? 0 : 1;
  }

  // 3b. 締切超過で blocked → 「生成なし」通知をレポートとして記録・投稿
  const stopHeartbeat = startHeartbeat(cfg, jobId);
  try {
    if (input.constraints?.generation === 'blocked') {
      const body = {
        summary: '必須データ (モール売上) が締切までに揃わず、今週のレポートは生成できませんでした。',
        topics: [], topic_updates: [],
        data_notes: (input.coverage || []).filter((c) => c.status !== 'ok')
          .map((c) => `${c.source_id}=${c.status}`).join(', ').slice(0, 300),
      };
      const saved = await serviceCall(cfg, `/jobs/${jobId}/report`, {
        generation_mode: 'fallback', body_json: body, topics: [],
        input_hash: stableInputHash(input), data_as_of: input.meta?.data_as_of,
        data_quality_status: 'blocked', prompt_version: PROMPT_VERSION,
        input_schema_version: INPUT_SCHEMA_VERSION,
      });
      await serviceCall(cfg, `/jobs/${jobId}/posting`);
      const text = buildBlockedNotice(input, saved.public_id);
      stopHeartbeat();
      const okPost = await postAndFinalize(cfg, jobId, text, saved.public_id);
      if (okPost) notify('blocked_notice', saved.public_id);
      return okPost ? 0 : 1;
    }

    // 4. claude -p 生成 (3回リトライ → フォールバック)
    let body = null;
    let mode = 'claude';
    let lastErrClass = null;
    const prompt = buildPrompt(input);
    for (let attempt = 1; attempt <= 3 && !body; attempt++) {
      try {
        log(`claude -p 生成 attempt ${attempt}/3 ...`);
        const stdout = await runClaude(cfg, prompt);
        body = validateOutput(parseClaudeOutput(stdout), input);
      } catch (e) {
        lastErrClass = classifyClaudeError(e);
        log(`  生成失敗 (${lastErrClass}): ${String(e.message).slice(0, 300)}`);
        if (lastErrClass === 'claude_auth') break; // 認証切れは再試行しても無駄
        if (attempt < 3) await new Promise((r) => setTimeout(r, cfg.claudeRetryBaseMs * attempt));
      }
    }
    if (!body) {
      log(`フォールバック生成 (機械整形) に切替: ${lastErrClass}`);
      body = buildFallbackBody(input);
      mode = 'fallback';
    }

    // 5. 保存 → posting → 投稿 → posted
    const saved = await serviceCall(cfg, `/jobs/${jobId}/report`, {
      generation_mode: mode, body_json: body, topics: body.topics,
      topic_updates: body.topic_updates,
      input_hash: stableInputHash(input), data_as_of: input.meta?.data_as_of,
      data_quality_status: input.constraints?.data_quality_status || null,
      prompt_version: PROMPT_VERSION, input_schema_version: INPUT_SCHEMA_VERSION,
      budget_revision_id: input.budget?.budget_revision_id || null,
    });
    await serviceCall(cfg, `/jobs/${jobId}/posting`);
    const text = buildGChatMessage(body, input, saved.public_id, {
      detailUrl: `${cfg.base}/apps/ai-insights`,
    });
    stopHeartbeat();
    const okPost = await postAndFinalize(cfg, jobId, text, saved.public_id);
    if (okPost) notify(mode === 'claude' ? 'ok' : 'fallback', saved.public_id);
    return okPost ? 0 : 1;
  } catch (e) {
    // posting 遷移前の失敗のみ failed 申告 (webhook 後の不確実性は postAndFinalize 内で処理済み)
    log(`失敗: ${e.stack || e.message}`);
    try {
      await serviceCall(cfg, `/jobs/${jobId}/failed`, {
        error_class: classifyClaudeError(e), error_detail: String(e.message).slice(0, 1000),
      });
    } catch (e2) {
      log(`  failed申告も失敗: ${e2.message}`);
    }
    notify('failed', e.message?.slice(0, 200));
    return 1;
  } finally {
    stopHeartbeat();
  }
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectRun) {
  main()
    .then((code) => { logStream?.end(); process.exitCode = code; })
    .catch((e) => {
      log(`致命的エラー: ${e.stack || e.message}`);
      notify('failed', e.message?.slice(0, 200));
      logStream?.end();
      process.exitCode = 1;
    });
}
