#!/usr/bin/env node
// ai-insights PC runner: 週次AI経営レポート (PR-2。共通処理は runner-lib.js)
//
// 配置: C:\tools\ai-insights\ (OneDrive・全角パス配下に置かない。repo 正本 = scripts/ai-insights/)
// 起動: Task Scheduler 火曜 8:30 + 1時間ごと繰り返し (〜18:00) + StartWhenAvailable。
//       ログインユーザー実行 (Claude サブスク認証がユーザープロファイル紐付きのため SYSTEM 禁止)
//
// フロー (要件 §4/§7/§8):
//   report-input 取得 → (blocked なら締切まで待つ / 締切超過は「生成なし」投稿)
//   → claim (原子的) → claude -p で論点生成 (失敗3回でフォールバック=事実の機械整形)
//   → /report 保存 → /posting → GChat webhook → /posted
//   投稿の成否不明 (webhook 後の失敗) は /failed を呼ばず放置
//   → サーバ側の孤児回収が reconciliation_required にして ⚙️ 画面で人間照合
//
// 終端ログマーカー: [NOTIFY:status=ok|ok_repost|fallback|skip|skip_busy|retry_later|dry_run|
//                   blocked_notice|reconciliation_required|failed|failed_posting_uncertain]

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  INPUT_SCHEMA_VERSION, RUNNER_ID, config, log, notify, closeLog,
  jstParts, addDaysYmd, lastCompletedWeekStart, http,
  generateWithRetry, classifyClaudeError, serviceCall, startHeartbeat,
  stableInputHash, postAndFinalize,
} from './runner-lib.js';
import {
  PROMPT_VERSION, buildPrompt, validateOutput, buildFallbackBody,
  buildGChatMessage, buildBlockedNotice, enrichFactsDisplay,
} from './weekly-prompt.js';

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
      const { body: genBody } = await generateWithRetry(
        cfg, buildPrompt(input), (raw) => validateOutput(raw, input),
      );
      const body = genBody || buildFallbackBody(input);
      if (!genBody) log('dry-run: claude生成失敗 → フォールバックでプレビュー');
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
    const { body: genBody, errorClass } = await generateWithRetry(
      cfg, buildPrompt(input), (raw) => validateOutput(raw, input),
    );
    let body = genBody;
    let mode = 'claude';
    if (!body) {
      log(`フォールバック生成 (機械整形) に切替: ${errorClass}`);
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
    .then((code) => { closeLog(); process.exitCode = code; })
    .catch((e) => {
      log(`致命的エラー: ${e.stack || e.message}`);
      notify('failed', e.message?.slice(0, 200));
      closeLog();
      process.exitCode = 1;
    });
}
