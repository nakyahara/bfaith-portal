/**
 * 低粗利商品アラート GChat 通知ジョブ (in-server cron)
 *
 * 直近30日の変動費後粗利率 (広告費前) が閾値 (デフォルト10%) を下回る商品を
 * 5モール (Amazon/楽天/Yahoo/au PAY/Qoo10) の finance_sku_daily mirror から抽出し、
 * 経営インサイトスペース (GCHAT_WEBHOOK_INSIGHT) へ毎朝通知する。
 *
 * 判定定義は各モール分析ダッシュボードの margin_pct 定義と完全一致させる (数字の二重真実を作らない):
 *   - Amazon: profit_amount / (principal+shipping+giftwrap) 税抜 — amazon-dashboard getSkuProfit 流用
 *             広告込み利益率 (direct+按分) を併記
 *   - 楽天:   variable_margin_jpy_incl / gross_sales_jpy_incl (税込)
 *   - Yahoo:  variable_margin_partial_jpy_incl / gross_sales_jpy_incl (税込)
 *   - au PAY: variable_margin_partial_jpy_incl / gross_sales_jpy_incl (税込)
 *   - Qoo10:  variable_margin_jpy_incl / net_settlement_api_jpy_incl (ショップ受取、要件§10 正本定義)
 *   - LINEギフト/メルカリ: finance_sku_daily 未整備のため対象外 (メッセージ末尾に明記)
 *
 * 原価未登録など is_cost_complete=0 の商品は判定せず「判定対象外」として件数のみ表示 (誤検知防止)。
 * 足切りなし (2026-07-18 中原さん決定): 期間売上が正なら全商品を判定する。
 *
 * 新規/継続の区別:
 *   前回実行時の該当キー集合を dashboard_settings ('profit_margin_alert_state') に保存し、
 *   今回新たに閾値を割った商品を「新規」として強調表示する。初回実行は全件「継続」扱い。
 *
 * Feature flag: MARGIN_ALERT_ENABLED=true で有効化 (在庫通知の Dark Launch と同パターン)
 *
 * 環境変数:
 *   MARGIN_ALERT_ENABLED       - 'true'/'1' で起動時にスケジュール、未設定/'false'はスキップ
 *   MARGIN_ALERT_CRON          - cron式 (デフォルト '0 1 * * *' = UTC01:00 = JST 10:00)
 *   MARGIN_ALERT_THRESHOLD_PCT - 閾値% (デフォルト 10)
 *   GCHAT_WEBHOOK_INSIGHT      - 経営インサイトスペースの Webhook URL (在庫/売上通知と共用)
 *
 * cron 時刻設計:
 *   miniPC daily-sync (JST 07:00 開始) の mirror 反映完了 ~08:20 の後に発火する必要がある。
 *   JST 10:00 は安全圏 (biz-ops-overview notify-job の 5/21 事故の教訓)。
 */
import cron from 'node-cron';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { sendGChatMessage } from './gchat-client.js';
import { fmtYen } from './notify-job.js';
import { getSkuProfit, jstToday, addDays } from '../amazon-dashboard/queries.js';

const STATE_KEY = 'profit_margin_alert_state';
const WINDOW_DAYS = 30;
const DEFAULT_THRESHOLD_PCT = 10;
const MAX_NEW_LINES = 10;    // 新規はワースト10件まで明細表示
const MAX_CONT_LINES = 5;    // 継続はワースト5件まで
const MAX_TEXT_LEN = 3500;   // GChat ~4000 chars の安全側 (biz-ops notify と同値)

const MALL_LABELS = {
  amazon: 'Amazon',
  rakuten: '楽天',
  yahoo: 'Yahoo',
  aupay: 'au',
  qoo10: 'Qoo10',
};

/** 閾値% を env から取得 (不正値はデフォルトへフォールバック) */
export function resolveThresholdPct() {
  const raw = process.env.MARGIN_ALERT_THRESHOLD_PCT;
  if (raw === undefined || raw === '') return DEFAULT_THRESHOLD_PCT;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0 || n >= 100) {
    console.warn(`[margin-alert] MARGIN_ALERT_THRESHOLD_PCT が不正 (${raw})、デフォルト ${DEFAULT_THRESHOLD_PCT}% を使用`);
    return DEFAULT_THRESHOLD_PCT;
  }
  return n;
}

/** % を小数1桁に丸める (ダッシュボードの margin_pct と同じ丸め) */
function round1(x) {
  return Math.round(x * 10) / 10;
}

/**
 * モール別の 30日集計行を { mall, code, name, sales, marginPct, adInclPct, costComplete } に正規化。
 * テーブル未整備・スキーマ差異で query が落ちてもモール単位で fail-soft (skipped に記録)。
 */
export function collectMarginRows(db, from, to, opts = {}) {
  const rows = [];
  const skipped = [];

  // ─ Amazon: amazon-dashboard の SKU 利益テーブルをそのまま流用 (広告込み margin_pct も取れる) ─
  // Codex R1 High: getSkuProfit は limit 上限 20,000 で切り詰めるため、total を見て全ページ取得する
  // (足切りなし要件。1ページで足りない規模になっても低利益 SKU を無通知で落とさない)
  // Codex R2 Medium/Low:
  //   - 一時配列に集めて全ページ成功時のみ rows へ採用 (途中失敗で部分データが混ざるのを防ぐ)
  //   - sort='seller_sku' (unique キー) でページ間の順序を決定的にする
  //   - 念のため seller_sku で Map dedup (ページ境界の重複防御)
  try {
    const pageSize = opts.amazonPageSize || 10000;
    const amazonRows = new Map(); // seller_sku → row
    let offset = 0;
    let total = Infinity;
    while (offset < total) {
      const res = getSkuProfit(from, to, { limit: pageSize, offset, sort: 'seller_sku', dir: 'asc' });
      total = res.total;
      if (res.rows.length === 0) break;
      for (const r of res.rows) {
        if (!(r.revenue_excl > 0)) continue;
        amazonRows.set(r.seller_sku, {
          mall: 'amazon',
          code: r.seller_sku,
          name: r.product_name || '',
          sales: r.revenue_excl,
          marginPct: round1(r.profit_before_ads / r.revenue_excl * 100),
          adInclPct: r.margin_pct, // 広告 (direct+按分) 込み。null あり
          costComplete: r.cost_status === 'complete',
        });
      }
      offset += res.rows.length;
    }
    rows.push(...amazonRows.values());
  } catch (e) {
    console.error('[margin-alert] Amazon 集計に失敗 (skip):', e.message);
    skipped.push('amazon');
  }

  // ─ 楽天/Yahoo/au PAY/Qoo10: finance_sku_daily を各ダッシュボードと同じ定義で集計 ─
  const mallSql = {
    rakuten: `
      SELECT LOWER(rakuten_code) AS code, MAX(product_name) AS name,
             SUM(gross_sales_jpy_incl) AS denom,
             SUM(variable_margin_jpy_incl) AS margin,
             MIN(is_cost_complete) AS cost_complete
      FROM mirror_rakuten_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY LOWER(rakuten_code)`,
    yahoo: `
      SELECT yahoo_sku_key AS code, MAX(product_name) AS name,
             SUM(gross_sales_jpy_incl) AS denom,
             SUM(variable_margin_partial_jpy_incl) AS margin,
             MIN(is_cost_complete) AS cost_complete
      FROM mirror_yahoo_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY yahoo_sku_key`,
    aupay: `
      SELECT aupay_sku_key AS code, MAX(product_name) AS name,
             SUM(gross_sales_jpy_incl) AS denom,
             SUM(variable_margin_partial_jpy_incl) AS margin,
             MIN(is_cost_complete) AS cost_complete
      FROM mirror_aupay_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY aupay_sku_key`,
    qoo10: `
      SELECT sku_code AS code, MAX(product_name) AS name,
             SUM(net_settlement_api_jpy_incl) AS denom,
             SUM(variable_margin_jpy_incl) AS margin,
             MIN(is_cost_complete) AS cost_complete
      FROM mirror_qoo10_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY sku_code`,
  };
  for (const [mall, sql] of Object.entries(mallSql)) {
    try {
      for (const r of db.prepare(sql).all(from, to)) {
        if (!(r.denom > 0)) continue;
        rows.push({
          mall,
          code: r.code,
          name: r.name || '',
          sales: r.denom,
          marginPct: round1(r.margin / r.denom * 100),
          adInclPct: null,
          costComplete: r.cost_complete === 1,
        });
      }
    } catch (e) {
      console.error(`[margin-alert] ${mall} 集計に失敗 (skip):`, e.message);
      skipped.push(mall);
    }
  }

  return { rows, skipped };
}

/**
 * 判定: 閾値割れの抽出と新規/継続の分離。
 * @returns {{ flagged, newItems, contItems, lossCount, mallCounts, excludedCount }}
 */
export function classifyMarginRows(rows, thresholdPct, prevKeys) {
  const flagged = [];
  let excludedCount = 0;
  for (const r of rows) {
    if (!r.costComplete) {
      excludedCount++;
      continue;
    }
    if (r.marginPct < thresholdPct) flagged.push(r);
  }
  flagged.sort((a, b) => a.marginPct - b.marginPct);

  const prev = new Set(prevKeys || []);
  const hasPrev = prev.size > 0 || (prevKeys !== null && prevKeys !== undefined);
  const keyOf = (r) => `${r.mall}:${r.code}`;
  const newItems = hasPrev ? flagged.filter(r => !prev.has(keyOf(r))) : [];
  const contItems = hasPrev ? flagged.filter(r => prev.has(keyOf(r))) : flagged;

  const mallCounts = {};
  for (const r of flagged) mallCounts[r.mall] = (mallCounts[r.mall] || 0) + 1;
  const lossCount = flagged.filter(r => r.marginPct < 0).length;

  return { flagged, newItems, contItems, lossCount, mallCounts, excludedCount, keys: flagged.map(keyOf) };
}

/**
 * モール由来文字列の無害化 (Codex R1 Low):
 * 改行・制御文字は空白化 (明細行の偽装防止)、GChat マークアップ文字 (*_`~) は除去。
 */
export function sanitizeText(s) {
  return String(s || '')
    .replace(/[\u0000-\u001f\u007f]+/g, ' ') // 改行含む制御文字 → 空白
    .replace(/[*_`~]/g, '')
    .trim();
}

/** 商品名を GChat 1行に収まる長さへ */
function truncateName(name, max = 24) {
  const s = sanitizeText(name);
  if (!s) return '(名称不明)';
  return s.length > max ? s.slice(0, max) + '…' : s;
}

/** 明細1行 */
function formatItemLine(idx, r) {
  const label = MALL_LABELS[r.mall] || r.mall;
  const ad = (r.mall === 'amazon' && r.adInclPct !== null && r.adInclPct !== undefined)
    ? ` (広告込 ${r.adInclPct.toFixed(1)}%)` : '';
  return `${idx}. [${label}] ${truncateName(r.name)}\n` +
    `    粗利率 ${r.marginPct.toFixed(1)}%${ad} | 売上 ${fmtYen(r.sales)} | ${sanitizeText(r.code)}`;
}

/**
 * 通知本文を組み立てる (純関数、テスト対象)。
 */
export function formatMarginAlertMessage({ todayJst, from, to, thresholdPct, result, isFirstRun, skipped }) {
  const jstWeekdays = ['日', '月', '火', '水', '木', '金', '土'];
  const [y, m, day] = todayJst.split('-').map(Number);
  // Date.UTC でカレンダー上の曜日を直接引く (toISOString の UTC ずれ罠を回避)
  const weekday = jstWeekdays[new Date(Date.UTC(y, m - 1, day)).getUTCDay()];
  const dateLabel = `${m}/${day}(${weekday})`;

  const lines = [];
  lines.push(`*粗利アラートサマリ* ${dateLabel} | 直近${WINDOW_DAYS}日 (${from}〜${to})`);
  lines.push(`判定: 変動費後粗利率 (広告費前) < ${thresholdPct}%`);
  lines.push('');

  const { flagged, newItems, contItems, lossCount, mallCounts, excludedCount } = result;

  if (flagged.length === 0) {
    lines.push(`✅ ${thresholdPct}%割れの商品はありません`);
  } else if (isFirstRun) {
    lines.push(`⚠️ ${thresholdPct}%割れ ${flagged.length}件 (初回実行のため全件表示、次回から新規/継続を区別) — ワースト${Math.min(MAX_NEW_LINES, flagged.length)}件`);
    flagged.slice(0, MAX_NEW_LINES).forEach((r, i) => lines.push(formatItemLine(i + 1, r)));
    if (flagged.length > MAX_NEW_LINES) lines.push(`    …ほか ${flagged.length - MAX_NEW_LINES}件`);
  } else {
    if (newItems.length > 0) {
      lines.push(`🚨 新規割れ ${newItems.length}件`);
      newItems.slice(0, MAX_NEW_LINES).forEach((r, i) => lines.push(formatItemLine(i + 1, r)));
      if (newItems.length > MAX_NEW_LINES) lines.push(`    …ほか ${newItems.length - MAX_NEW_LINES}件`);
      lines.push('');
    } else {
      lines.push('🚨 新規割れ なし');
      lines.push('');
    }
    if (contItems.length > 0) {
      lines.push(`⚠️ 継続 ${contItems.length}件 — ワースト${Math.min(MAX_CONT_LINES, contItems.length)}件`);
      contItems.slice(0, MAX_CONT_LINES).forEach((r, i) => lines.push(formatItemLine(i + 1, r)));
      if (contItems.length > MAX_CONT_LINES) lines.push(`    …ほか ${contItems.length - MAX_CONT_LINES}件`);
    }
  }

  lines.push('');
  if (flagged.length > 0) {
    lines.push(`🔻 うち赤字 (粗利率<0%): ${lossCount}件`);
    const mallSummary = Object.entries(mallCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([m, c]) => `${MALL_LABELS[m] || m} ${c}`)
      .join(' / ');
    lines.push(`📊 モール別: ${mallSummary}`);
  }
  if (excludedCount > 0) {
    lines.push(`判定対象外 (原価未登録など): ${excludedCount}件`);
  }
  if (skipped.length > 0) {
    lines.push(`⚠️ 集計スキップ (データ未整備/エラー): ${skipped.map(m => MALL_LABELS[m] || m).join(', ')}`);
  }
  lines.push('※各モール分析ダッシュボードと同一定義 (Amazonのみ税抜/広告込み併記)。LINEギフト/メルカリは未対応');

  let text = lines.join('\n');
  if (text.length > MAX_TEXT_LEN) {
    console.warn(`[margin-alert] text_len=${text.length} > ${MAX_TEXT_LEN}、末尾を切り詰めて送信`);
    text = text.slice(0, MAX_TEXT_LEN - 20) + '\n…(省略)';
  }
  return text;
}

/** 前回実行状態の読み書き (dashboard_settings) */
export function loadAlertState(db) {
  const row = db.prepare('SELECT value_json FROM dashboard_settings WHERE key = ?').get(STATE_KEY);
  if (!row) return null;
  try {
    const parsed = JSON.parse(row.value_json);
    if (!parsed || !Array.isArray(parsed.flagged_keys)) return null;
    return parsed;
  } catch (e) {
    console.error(`[margin-alert] dashboard_settings.${STATE_KEY} の JSON parse に失敗 (初回扱いで続行):`, e.message);
    return null;
  }
}

export function saveAlertState(db, keys, todayJst) {
  const value = JSON.stringify({ last_run_date_jst: todayJst, flagged_keys: keys });
  db.prepare(`
    INSERT INTO dashboard_settings (key, value_json, updated_at, updated_by)
    VALUES (?, ?, datetime('now'), 'margin-alert-job')
    ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
      updated_at = excluded.updated_at, updated_by = excluded.updated_by
  `).run(STATE_KEY, value);
}

/**
 * skipped モールの前回キーを今回の保存キーへ引き継ぐ (Codex R1 Medium #1)。
 * 一時的な集計失敗でそのモールの state が消えると、復旧翌日に既存該当商品が
 * 全部「新規割れ」として誤通知されるため。
 */
export function mergeStateKeys(currentKeys, prevKeys, skipped) {
  if (!prevKeys || skipped.length === 0) return currentKeys;
  const skippedSet = new Set(skipped);
  const carried = prevKeys.filter(k => skippedSet.has(k.split(':')[0]));
  // Set で重複排除 (Codex R2 Medium: 部分取得と引き継ぎが重なっても state が肥大しない)
  return [...new Set([...currentKeys, ...carried])];
}

// 多重起動ガード (Codex R1 Medium #2: 手動実行と cron の重複、遅延 cron の追い付き実行対策)
let _running = false;

/**
 * 通知1回分を実行。在庫/売上通知と独立 (片方失敗で全落ちさせない)。
 * 手動実行: Render Shell から
 *   DATA_DIR=/data node -e "import('./apps/profit-analysis/margin-alert-job.js').then(m => m.runMarginAlertJob())"
 */
export async function runMarginAlertJob() {
  const webhookUrl = process.env.GCHAT_WEBHOOK_INSIGHT;
  if (!webhookUrl) {
    console.warn('[margin-alert] GCHAT_WEBHOOK_INSIGHT が未設定。通知をスキップ。');
    return { ok: false, reason: 'webhook_not_set' };
  }
  if (_running) {
    console.warn('[margin-alert] 前回実行が完了していないためスキップ (多重起動ガード)');
    return { ok: false, reason: 'already_running' };
  }
  _running = true;

  try {
    const db = getMirrorDB();
    const thresholdPct = resolveThresholdPct();
    const todayJst = jstToday();
    const to = addDays(todayJst, -1);              // 昨日まで (当日分は sync 前で常に空のため)
    const from = addDays(todayJst, -WINDOW_DAYS);  // 直近30日ウィンドウ

    const { rows, skipped } = collectMarginRows(db, from, to);
    if (skipped.length === 5) {
      console.error('[margin-alert] 全モールの集計に失敗。通知を中止。');
      return { ok: false, reason: 'all_malls_failed' };
    }

    const state = loadAlertState(db);
    const isFirstRun = state === null;
    const result = classifyMarginRows(rows, thresholdPct, isFirstRun ? null : state.flagged_keys);

    const text = formatMarginAlertMessage({ todayJst, from, to, thresholdPct, result, isFirstRun, skipped });
    console.log(`[margin-alert] 送信開始 flagged=${result.flagged.length} new=${result.newItems.length} excluded=${result.excludedCount} skipped=${skipped.join(',') || 'none'} text_len=${text.length}`);
    const sendResult = await sendGChatMessage(webhookUrl, text);
    console.log(`[margin-alert] 送信成功 status=${sendResult.status}`);

    // 送信成功後に state 更新 (送信失敗時は次回も同じ新規判定を再送できるように)。
    // skipped モールの前回キーは引き継ぐ。保存失敗は「送信済み・state 未更新」として
    // 送信失敗と区別してログ (Codex R1 Medium #3。次回同じ商品が再度「新規」になる旨を明示)
    try {
      saveAlertState(db, mergeStateKeys(result.keys, state?.flagged_keys, skipped), todayJst);
    } catch (e) {
      console.error('[margin-alert] ⚠️ 通知は送信済みだが state 保存に失敗 (次回同じ商品が再度「新規」表示される可能性):', e.message);
      return { ok: true, flagged: result.flagged.length, newCount: result.newItems.length, stateSaveFailed: true };
    }
    return { ok: true, flagged: result.flagged.length, newCount: result.newItems.length };
  } catch (e) {
    console.error('[margin-alert] 通知送信失敗:', e.message);
    return { ok: false, reason: e.message };
  } finally {
    _running = false;
  }
}

/**
 * cron スケジュール起動。MARGIN_ALERT_ENABLED='true'/'1' のときだけ schedule する (Dark Launch)。
 */
export function startMarginAlertJob() {
  const enabled = process.env.MARGIN_ALERT_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[margin-alert] MARGIN_ALERT_ENABLED が未設定/falseのためスケジュールしない (Dark Launch)');
    return null;
  }
  const cronExpr = process.env.MARGIN_ALERT_CRON || '0 1 * * *'; // UTC 01:00 = JST 10:00
  if (!cron.validate(cronExpr)) {
    console.error(`[margin-alert] MARGIN_ALERT_CRON が不正: ${cronExpr}`);
    return null;
  }
  const task = cron.schedule(cronExpr, () => {
    runMarginAlertJob().catch(e => {
      console.error('[margin-alert] cron 実行中に未捕捉例外:', e);
    });
  }, { timezone: 'UTC' });
  console.log(`[margin-alert] cron schedule 起動: '${cronExpr}' (UTC) — JST 10:00 想定 (daily-sync 完了後)`);
  return task;
}
