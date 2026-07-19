#!/usr/bin/env node
// ai-insights PC runner: 月次AI経営レポート (PR-3)
//
// 起動: Task Scheduler 毎日 9:00 + StartWhenAvailable (月次状態機械の日次チェック)。
// ほとんどの日は skip で終わる。動くのは:
//   - 毎月10日以降で前月が未宣言 & 暫定版未発行 → 暫定版 (provisional) を生成・投稿
//   - 確定宣言済み & 宣言後にMF同期成功 & 確定版未発行 → 確定版 (final) を生成・投稿
//   - 再オープン→再宣言後 → 訂正版 (correction-N) を生成・投稿
// 締め宣言・リマインダー (15/20日)・確定後変更検知は Render 側 (⚙️画面 + notify-job)。
//
// 終端マーカー: [NOTIFY:status=ok|ok_repost|fallback|skip|skip_busy|skip_before_10th|
//   skip_waiting_declare|retry_later_mf|blocked_wait|dry_run|reconciliation_required|
//   failed|failed_posting_uncertain]

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  INPUT_SCHEMA_VERSION, RUNNER_ID, config, log, notify, closeLog,
  jstParts, previousMonthYm, http,
  generateWithRetry, classifyClaudeError, serviceCall, startHeartbeat,
  stableInputHash, postAndFinalize,
} from './runner-lib.js';
import {
  validateOutput, buildGChatMessage, enrichFactsDisplay,
} from './weekly-prompt.js';
import {
  MONTHLY_PROMPT_VERSION, buildMonthlyPrompt, buildMonthlyFallbackBody, editionLabel,
} from './monthly-prompt.js';

async function main() {
  const cfg = config();
  const args = process.argv.slice(2);
  const month = args.find((a) => a.startsWith('--month='))?.split('=')[1] || previousMonthYm();
  log(`=== 月次AI経営レポート runner 開始: ${month} ===`);

  const input = await http(
    `${cfg.base}/api/ai-insights/report-input?type=monthly&month=${month}`,
    { headers: { 'x-read-token': cfg.readToken }, retries: 3 },
  );
  enrichFactsDisplay(input);
  const closing = input.closing || { status: 'open' };

  // --dry-run: 投稿せずプレビューのみ
  if (args.includes('--dry-run')) {
    const { body: genBody } = await generateWithRetry(
      cfg, buildMonthlyPrompt(input), (raw) => validateOutput(raw, input),
    );
    const body = genBody || buildMonthlyFallbackBody(input);
    if (!genBody) log('dry-run: claude生成失敗 → フォールバックでプレビュー');
    const label = closing.status === 'declared' ? editionLabel('final') : editionLabel('provisional');
    const text = buildGChatMessage(body, input, 'PREVIEW', {
      editionLabel: label, detailUrl: `${cfg.base}/apps/ai-insights`,
    });
    log(`---- GChat本文プレビュー (未投稿) ----\n${text}\n---- プレビューここまで ----`);
    notify('dry_run');
    return 0;
  }

  // ── どの版を出すべきかの判定 (月次状態機械。要件 §6) ──
  const { day } = jstParts();
  let edition = null;
  if (closing.status === 'declared') {
    if (closing.needs_correction) {
      if (!closing.mf_synced_after_declare) {
        notify('retry_later_mf', '再宣言後のMF同期待ち (訂正版は同期成功後に発行)');
        return 0;
      }
      edition = 'correction-next';
    } else if (closing.final_state === 'posted') {
      notify('skip', '確定版発行済み');
      return 0;
    } else if (!closing.mf_synced_after_declare) {
      notify('retry_later_mf', '締め宣言後のMF同期待ち (確定版は宣言後の同期成功データで発行)');
      return 0;
    } else {
      edition = 'final';
    }
  } else {
    // 未宣言 (open / reopened): 10日以降に暫定版を1回だけ
    if (closing.provisional_state === 'posted') {
      notify('skip_waiting_declare', '暫定版発行済み。確定宣言待ち (⚙️画面)');
      return 0;
    }
    if (day < 10) {
      notify('skip_before_10th', `暫定版は10日から (今日=${day}日)`);
      return 0;
    }
    if (input.constraints?.generation === 'blocked') {
      // 月次は毎日走るため blocked 通知は投稿しない (翌日再試行。15/20日リマインダーが人間側の導線)
      notify('blocked_wait', '必須データ未充足。翌日再試行');
      return 0;
    }
    edition = 'provisional';
  }

  // ── claim ──
  const claim = await serviceCall(cfg, '/jobs/claim', {
    report_type: 'monthly', period_start: input.meta.period_start, period_end: input.meta.period_end,
    edition, claimed_by: RUNNER_ID,
  });
  if (claim.result === 'already_posted') { notify('skip', `${claim.job.edition} 投稿済み`); return 0; }
  if (claim.result === 'busy') { notify('skip_busy', '別プロセスが実行中'); return 0; }
  if (claim.result === 'reconciliation_required') {
    notify('reconciliation_required', '⚙️画面で投稿有無を照合してください');
    return 0;
  }
  const jobId = claim.job.job_id;
  const actualEdition = claim.job.edition; // correction-next → correction-N が採番されている

  // 生成済みレポートの投稿再開
  if (claim.result === 'claimed_for_posting') {
    const report = claim.report;
    const body = JSON.parse(report.body_json);
    const pseudoInput = { meta: { period_label: `${month}月次` }, coverage: [] };
    const text = buildGChatMessage(body, pseudoInput, report.public_id, {
      editionLabel: editionLabel(report.edition), detailUrl: `${cfg.base}/apps/ai-insights`,
    });
    const okPost = await postAndFinalize(cfg, jobId, text, report.public_id);
    if (okPost) notify('ok_repost', report.public_id);
    return okPost ? 0 : 1;
  }

  // ── 生成 → 保存 → 投稿 ──
  const stopHeartbeat = startHeartbeat(cfg, jobId);
  try {
    const { body: genBody, errorClass } = await generateWithRetry(
      cfg, buildMonthlyPrompt(input), (raw) => validateOutput(raw, input),
    );
    let body = genBody;
    let mode = 'claude';
    if (!body) {
      log(`フォールバック生成 (機械整形) に切替: ${errorClass}`);
      body = buildMonthlyFallbackBody(input);
      mode = 'fallback';
    }
    const saved = await serviceCall(cfg, `/jobs/${jobId}/report`, {
      generation_mode: mode, body_json: body, topics: body.topics,
      topic_updates: body.topic_updates,
      input_hash: stableInputHash(input), data_as_of: input.meta?.data_as_of,
      data_quality_status: input.constraints?.data_quality_status || null,
      prompt_version: MONTHLY_PROMPT_VERSION, input_schema_version: INPUT_SCHEMA_VERSION,
      budget_revision_id: input.budget?.budget_revision_id || null,
      pl_hash: input.pl_hash || null, // 確定後変更検知の基準 (final/correction 時にサーバが保存)
    });
    await serviceCall(cfg, `/jobs/${jobId}/posting`);
    const text = buildGChatMessage(body, input, saved.public_id, {
      editionLabel: editionLabel(actualEdition), detailUrl: `${cfg.base}/apps/ai-insights`,
    });
    stopHeartbeat();
    const okPost = await postAndFinalize(cfg, jobId, text, saved.public_id);
    if (okPost) notify(mode === 'claude' ? 'ok' : 'fallback', saved.public_id);
    return okPost ? 0 : 1;
  } catch (e) {
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
    .then((code) => { closeLog(); process.exitCode = code; })
    .catch((e) => {
      log(`致命的エラー: ${e.stack || e.message}`);
      notify('failed', e.message?.slice(0, 200));
      closeLog();
      process.exitCode = 1;
    });
}
