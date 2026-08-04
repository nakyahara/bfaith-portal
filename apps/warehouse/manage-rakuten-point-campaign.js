#!/usr/bin/env node
/**
 * manage-rakuten-point-campaign.js — 商品別ポイント変倍 月次自動設定 CLI
 *
 * 何をするか:
 *   Google スプレッドシート (商品コード / 商品名 / ポイント割引率) にある商品へ、
 *   楽天の商品別ポイント変倍「当月1日 12:00 〜 月末 23:59:59」を毎月自動で設定する。
 *   毎日実行して、実際に書くのは 1〜3日 (1日が本番、2〜3日は取りこぼしリカバリ) だけ。
 *   設定済みなら何もしない (冪等)。
 *
 * なぜ API で完結できるか:
 *   Item API 2.0 の PATCH (部分更新) に pointCampaign フィールドがある (2026-08-04 実測)。
 *   画面自動操作は使わない (RMS の自動化検知と無関係)。
 *
 * サブコマンド:
 *   status              シートの各商品の現在の設定を表示 (API GET のみ)
 *   apply               設定計画を表示 (既定は dry-run = 書き込まない)
 *   apply --live        実際に書き込む (1〜3日以外は何もせず終了)
 *   apply --live --force    実行日ゲートを無視して今すぐ書く (初回導入・手動リカバリ用)
 *   apply --live --overwrite  有効中の別設定があっても上書きする
 *
 * env:
 *   RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY  (必須)
 *   GOOGLE_SERVICE_ACCOUNT_KEY  base64 の SA 鍵 (必須。SA にシートの閲覧権限を付けること)
 *   RPOINT_SHEET_ID     対象シートID (--live では必須。status/dry-run のみ既定=ポイント変倍リスト)
 *   RPOINT_SHEET_RANGE  読む範囲 (既定 'A:C'。先頭シートの 商品コード/商品名/ポイント割引率。
 *                       先頭行がこのヘッダーと一致しなければ何も書かずに中止する)
 *   GCHAT_WEBHOOK       通知先 (未設定なら通知しない)
 *
 * exit code: 0=成功 (実行日でない/全て設定済み含む) / 1=失敗・conflict / 2=env・引数エラー
 */
import 'dotenv/config';
import { writeFileSync, readFileSync, unlinkSync, appendFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { google } from 'googleapis';
import { rakutenRequest } from './rakuten-client.js';
import {
  parseSheetRows, computeMonthlyPeriod, isRunDay, planPointCampaigns, campaignEquals,
} from './rakuten-point-campaign-lib.js';

const args = process.argv.slice(2);
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : null;
// タイプミス (--overwite 等) を黙って無視して意図とずれた実行をしない (Codex指摘)
const KNOWN_FLAGS = ['--live', '--force', '--overwrite'];
const unknown = args.slice(mode ? 1 : 0).filter((a) => !KNOWN_FLAGS.includes(a));
if (unknown.length > 0) {
  console.error(`FATAL: 未知の引数: ${unknown.join(' ')} (使えるのは ${KNOWN_FLAGS.join(' / ')})`);
  process.exit(2);
}
const LIVE = args.includes('--live');
const FORCE = args.includes('--force');
const OVERWRITE = args.includes('--overwrite');
// 既定のシートIDは status / dry-run の便宜用。--live は env 明示を必須にして、
// env の設定漏れ・タイプミスで意図しないシートを本番入力にしない (Codex指摘)
const DEFAULT_SHEET_ID = '1e0IV5Xx31PU-M6Z1cHi01jKEmLs_3pe_XjFEOnAOOms';
const SHEET_ID = process.env.RPOINT_SHEET_ID || DEFAULT_SHEET_ID;
const SHEET_RANGE = process.env.RPOINT_SHEET_RANGE || 'A:C';
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

if (!process.env.RAKUTEN_SERVICE_SECRET || !process.env.RAKUTEN_LICENSE_KEY) {
  console.error('FATAL: RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY が未設定');
  process.exit(2);
}
if (LIVE && !process.env.RPOINT_SHEET_ID) {
  console.error('FATAL: --live には RPOINT_SHEET_ID の明示設定が必要 (fail-closed)。miniPC の .env に追記すること');
  process.exit(2);
}

// ─── 単一実行の保証 (書き込みモードのみ) ───
// 古いロックは自動で奪わない (manage-rakuten-store-coupon.js と同じ理由 — 奪取は原子的にできない)。
const LOCK_PATH = join(process.env.DATA_DIR || tmpdir(), 'rakuten-point-campaign.lock');
let lockToken = null;

function acquireLock() {
  const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  try {
    writeFileSync(LOCK_PATH, JSON.stringify({ token, pid: process.pid, at: new Date().toISOString() }), { flag: 'wx' });
    lockToken = token;
    return { ok: true };
  } catch (e) {
    if (e.code !== 'EEXIST') throw e;
    let info = null;
    try { info = JSON.parse(readFileSync(LOCK_PATH, 'utf8')); } catch { /* 壊れていても情報が無いだけ */ }
    return { ok: false, detail: `pid=${info?.pid ?? '不明'} / 開始=${info?.at ?? '不明'} / ${LOCK_PATH}` };
  }
}

function releaseLock() {
  if (!lockToken) return;
  try {
    const info = JSON.parse(readFileSync(LOCK_PATH, 'utf8'));
    if (info.token !== lockToken) return;
  } catch { return; }
  try { unlinkSync(LOCK_PATH); } catch { /* 既に無ければ何もしない */ }
  lockToken = null;
}

/** GChat 通知 (best effort) */
async function sendGChat(text) {
  if (!GCHAT_WEBHOOK) { console.warn('[GChat] GCHAT_WEBHOOK 未設定のため通知スキップ'); return false; }
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(15000),
    });
    return res.ok;
  } catch (e) {
    console.error(`[GChat] 通知失敗: ${e.message}`);
    return false;
  }
}

/** シートを読んで対象リストにする。失敗は throw */
async function loadSheetRows() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) {
    const e = new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定 (base64 の SA 鍵)。SA にシートの閲覧権限も必要');
    e.exitCode = 2;
    throw e;
  }
  const credentials = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
  const sheets = google.sheets({ version: 'v4', auth });
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: SHEET_RANGE });
  const parsed = parseSheetRows(res.data.values || []);
  if (!parsed.ok) {
    throw new Error(`シートの内容が不正なため中止 (1件も書き込みません):\n  ${parsed.errors.join('\n  ')}`);
  }
  return parsed.rows;
}

/**
 * 各商品の現況を GET して Map<manageNumber, {status, item}> にする。
 * 1商品の失敗で全体を止めない — 失敗した商品は status:0 で計画側の error に落とし、他は続行 (Codex指摘)
 */
async function fetchCurrent(rows) {
  const map = new Map();
  for (const row of rows) {
    try {
      const r = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${encodeURIComponent(row.manageNumber)}` });
      map.set(row.manageNumber, { status: r.status, item: r.status === 200 ? r.data : null });
    } catch (e) {
      map.set(row.manageNumber, { status: 0, item: null, error: e.message });
    }
  }
  return map;
}

function fmtPeriod(c) {
  const s = c?.applicablePeriod?.start ?? '?';
  const e = c?.applicablePeriod?.end ?? '?';
  return `${s.slice(0, 16).replace('T', ' ')} 〜 ${e.slice(0, 16).replace('T', ' ')}`;
}

async function cmdStatus() {
  const rows = await loadSheetRows();
  console.log(`=== 商品別ポイント変倍 現況 (シート ${rows.length} 件) ===`);
  const current = await fetchCurrent(rows);
  for (const row of rows) {
    const cur = current.get(row.manageNumber);
    if (cur.status !== 200) { console.log(`  ${row.manageNumber}: ⚠️ HTTP ${cur.status}`); continue; }
    const pc = cur.item.pointCampaign;
    console.log(`  ${row.manageNumber} (${row.name}) シート=${row.pointRate}倍`);
    console.log(`    現在: ${pc ? `${pc.benefits?.pointRate}倍 ${fmtPeriod(pc)}` : '(設定なし)'}`);
  }
  return 0;
}

/** 設定 1件。直前再GET → PATCH → GET 読み戻しで意図どおりかまで確認する。throw しない */
async function applyOne(plan) {
  // 計画時のGETからは時間が経っている (直列レート制御で件数分の間隔が空く)。
  // その間に人がRMS画面から設定を変えたかもしれないので、直前にもう一度見て
  // 計画の前提 (plan.current) と変わっていたら書かない (Codex指摘)
  try {
    const fresh = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${encodeURIComponent(plan.manageNumber)}` });
    if (fresh.status !== 200) {
      return { ...plan, status: 'failed', error: `設定直前の再確認に失敗 (HTTP ${fresh.status}) — 書き込みを見送り` };
    }
    const freshPc = fresh.data?.pointCampaign ?? null;
    if (JSON.stringify(freshPc) !== JSON.stringify(plan.current ?? null)) {
      return { ...plan, status: 'failed', error: `計画後に設定が変更されています (${JSON.stringify(freshPc)}) — 上書きを見送り` };
    }
  } catch (e) {
    return { ...plan, status: 'failed', error: `設定直前の再確認に失敗したため見送り (${e.message})` };
  }
  let patch;
  try {
    // 再送しない (maxAttempts: 1)。client 内部の自動再送だと「直前再GET → 比較」を挟まずに
    // 2回目が飛ぶため、タイムアウト後の手動変更を上書きし得る (Codex指摘)。
    // 取りこぼしは翌日 (2〜3日) のリカバリ実行が同じ 再GET→比較→PATCH 手順で拾う
    patch = await rakutenRequest({
      path: `/es/2.0/items/manage-numbers/${encodeURIComponent(plan.manageNumber)}`,
      method: 'PATCH', body: plan.patchBody, maxAttempts: 1,
    });
  } catch (e) {
    return { ...plan, status: 'failed', error: `PATCH 呼び出しエラー: ${e.message}` };
  }
  if (patch.status < 200 || patch.status >= 300) {
    const detail = typeof patch.data === 'object' && patch.data?.errors
      ? patch.data.errors.map((x) => `${x.code}:${x.message}`).join(', ')
      : JSON.stringify(patch.data).slice(0, 300);
    return { ...plan, status: 'failed', error: `PATCH 失敗 (HTTP ${patch.status}): ${detail}` };
  }
  // 読み戻し確認 (書けたつもりで実は反映されていない、を exit 0 で見逃さない)
  try {
    const after = await rakutenRequest({ path: `/es/2.0/items/manage-numbers/${encodeURIComponent(plan.manageNumber)}` });
    const ok = after.status === 200 && campaignEquals(after.data?.pointCampaign, plan.desired);
    return ok
      ? { ...plan, status: 'set' }
      : { ...plan, status: 'failed', error: `読み戻しが意図と不一致: ${JSON.stringify(after.data?.pointCampaign ?? null)}` };
  } catch (e) {
    return { ...plan, status: 'unknown', error: `設定後の確認に失敗 (設定自体は成功している可能性が高い): ${e.message}` };
  }
}

async function cmdApply() {
  const nowIso = new Date().toISOString();
  if (LIVE && !FORCE && !isRunDay(nowIso)) {
    console.log('実行日 (毎月1〜3日) ではないため何もしません (--force で強制実行)');
    return 0;
  }
  const period = computeMonthlyPeriod(nowIso);
  if (!period.ok) {
    console.error(`FATAL: ${period.error}`);
    await sendGChat(`*楽天ポイント変倍 設定失敗*\n${period.error}`);
    return 1;
  }
  console.log(`=== 商品別ポイント変倍 ${period.ym} (${LIVE ? '★LIVE=実設定' : 'dry-run'}) ===`);
  console.log(`期間: ${period.start} 〜 ${period.end}${period.delayed ? ' ⚠️1日を過ぎたため開始をずらしています' : ''}`);

  const rows = await loadSheetRows();
  const current = await fetchCurrent(rows);
  const plans = planPointCampaigns({ rows, currentByMn: current, period, nowIso, overwriteActive: OVERWRITE });

  console.log('\n--- 計画 ---');
  for (const p of plans) {
    if (p.action === 'set') console.log(`  [set]      ${p.manageNumber} (${p.name}) → ${p.pointRate}倍`);
    else if (p.action === 'skip') console.log(`  [skip]     ${p.manageNumber}: ${p.reason}`);
    else console.log(`  [${p.action}] ${p.manageNumber}: ${p.reason}`);
  }

  const toSet = plans.filter((p) => p.action === 'set');
  const bad = plans.filter((p) => p.action === 'error' || p.action === 'conflict');

  if (!LIVE) {
    console.log(`\ndry-run: 書き込んでいません (${toSet.length}件が設定対象 / ${bad.length}件が要確認)。実行するには --live`);
    return bad.length > 0 ? 1 : 0;
  }

  const results = [];
  let auditOk = true;
  // 計画段階の error / conflict も監査に残す (書かなかった理由が後から追える)
  for (const p of bad) auditOk = writeAuditLine({ period, plan: p, result: null }) && auditOk;
  for (const plan of toSet) {
    const r = await applyOne(plan);
    results.push(r);
    // 1件ごとに逐次追記 — 途中クラッシュでも、それまでの成功PATCHの記録が残る (Codex指摘)
    auditOk = writeAuditLine({ period, plan, result: r }) && auditOk;
    if (r.status === 'set') console.log(`  ✅ ${r.manageNumber} を ${r.pointRate}倍 に設定`);
    else console.error(`  ❌ ${r.manageNumber}: ${r.error}`);
  }

  const failed = results.filter((r) => r.status !== 'set');
  // 「何も起きなかった日」(全て設定済み・失敗なし) は通知しない — リカバリ日 (2〜3日) の空振りで鳴らさない
  if (results.length > 0 || bad.length > 0) {
    await notify({ period, results, bad, skipped: plans.filter((p) => p.action === 'skip').length, auditOk });
  }
  // 監査ログが書けない状態も失敗扱い (設定自体は成功していても、記録なしで ok にしない)
  return (failed.length > 0 || bad.length > 0 || !auditOk) ? 1 : 0;
}

/**
 * 監査ログ (JSONL 逐次追記)。ポイント原資に関わる変更なので
 * 「いつ・どの商品に・何倍を・どの状態から」書いたかを後から確定できるようにする (Codex指摘)。
 * 戻り値 false = 書き込み失敗 (呼び元で失敗扱いにする)。
 */
function writeAuditLine({ period, plan, result }) {
  try {
    const line = JSON.stringify({
      at: new Date().toISOString(),
      ym: period.ym,
      period: { start: period.start, end: period.end },
      sheetId: SHEET_ID,
      mn: plan.manageNumber,
      rate: plan.pointRate,
      action: plan.action,
      before: plan.current ?? null,
      result: result?.status ?? null,
      error: result?.error ?? plan.reason ?? null,
    });
    appendFileSync(join(process.env.DATA_DIR || tmpdir(), 'rakuten-point-campaign-log.jsonl'), line + '\n');
    return true;
  } catch (e) {
    console.error(`[audit] 監査ログの書き込みに失敗: ${e.message}`);
    return false;
  }
}

async function notify({ period, results, bad, skipped, auditOk = true }) {
  const lines = [`*楽天ポイント変倍 (${period.ym})*`, ''];
  const okOnes = results.filter((r) => r.status === 'set');
  if (okOnes.length > 0) {
    lines.push(`✅ ${okOnes.length}件を設定しました (${period.start.slice(5, 16).replace('T', ' ')} 〜 ${period.end.slice(5, 16).replace('T', ' ')})`);
    for (const r of okOnes) lines.push(`　・${r.name || r.manageNumber}: *${r.pointRate}倍*`);
    if (period.delayed) lines.push('　⚠️ 1日を過ぎての設定のため、開始が遅れています');
    lines.push('');
  }
  for (const r of results.filter((x) => x.status !== 'set')) {
    lines.push(`❌ ${r.name || r.manageNumber}: ${r.error}`);
  }
  for (const p of bad) {
    lines.push(`⚠️ ${p.name || p.manageNumber}: ${p.reason}`);
  }
  if (!auditOk) lines.push('⚠️ 監査ログ (rakuten-point-campaign-log.jsonl) の書き込みに失敗しています — 設定自体は上記のとおり');
  if (skipped > 0) lines.push(`_設定済みスキップ ${skipped}件_`);
  lines.push('_シート「ポイント変倍リスト」の商品に毎月1日、当月分を自動設定しています_');
  await sendGChat(lines.join('\n'));
}

// 書き込みし得るモードだけロックを取る
const needsLock = mode === 'apply' && LIVE;
let locked = false;
try {
  if (needsLock) {
    const lock = acquireLock();
    locked = lock.ok;
    if (!locked) {
      console.error(`FATAL: 別の実行が進行中か、異常終了のロックが残っている (${lock.detail})。二重設定を避けるため中止`);
      await sendGChat(['*楽天ポイント変倍 設定を中止*', '',
        '別の実行が進行中か、異常終了のロックが残っています。',
        `　${lock.detail}`, '',
        '_進行中の実行が無ければ、上のロックファイルを削除してください_'].join('\n')).catch(() => {});
      process.exit(1);
    }
  }
  let code = 0;
  if (mode === 'status') code = await cmdStatus();
  else if (mode === 'apply') code = await cmdApply();
  else {
    console.error(`FATAL: unknown mode '${mode ?? '(なし)'}' (status / apply [--live] [--force] [--overwrite])`);
    code = 2;
  }
  process.exitCode = code;
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  // 本番実行の失敗 (シート読み取り・認証エラー等) は fail ping だけでなく内容も通知する (Codex指摘)
  if (mode === 'apply' && LIVE) {
    await sendGChat(`*楽天ポイント変倍 設定失敗*\n${String(e.message).slice(0, 500)}`).catch(() => {});
  }
  process.exitCode = e.exitCode ?? 1;
} finally {
  if (locked) releaseLock();
}
