/**
 * 経営インサイトGChat通知ジョブ
 *
 * 毎朝 INVENTORY_NOTIFY_CRON で指定された時刻に、
 * profit-analysis の判定ロジック (getInventorySummary) を呼び出して、
 * 経営層向けに在庫状況サマリをGChatへ送信する。
 *
 * Feature flag: INVENTORY_NOTIFY_ENABLED=true で有効化 (Dark Launch)
 *
 * 環境変数:
 *   INVENTORY_NOTIFY_ENABLED  - 'true' で起動時にスケジュール、未設定/'false'はスキップ
 *   INVENTORY_NOTIFY_CRON     - cron式 (デフォルト '0 22 * * *' = UTC22:00 = JST 07:00)
 *   GCHAT_WEBHOOK_INSIGHT     - 経営インサイトスペースの Webhook URL
 *
 * Codex指摘 (FBA対象外問題、3巡目反映):
 *   通知メッセージは「総在庫」「判定対象」「対象外」を明確に分離して表示。
 *   撤退検討/警戒の見出しには「判定対象内」を必ず併記、誤読を防ぐ。
 */
import cron from 'node-cron';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { getInventorySummary } from './inventory-decision.js';
import { sendGChatMessage } from './gchat-client.js';
import { getMisShipmentNotifySummary } from '../mis-shipment/summary.js';

// JST曜日表示用
const JST_WEEKDAYS = ['日', '月', '火', '水', '木', '金', '土'];

/**
 * 日本語単位 (億・万) で金額表示。経営層向けの可読性を優先。
 *   1億以上:    「2億1,712万円」 (端数が0なら「2億円」)
 *   1万以上:    「1,823万円」
 *   1万未満:    「7,500円」
 *   負値も対応: 「-75万円」
 */
export function fmtYen(n) {
  const v = Math.round(Number(n) || 0);
  const sign = v < 0 ? '-' : '';
  const abs = Math.abs(v);

  if (abs >= 100_000_000) {
    const oku = Math.floor(abs / 100_000_000);
    const man = Math.floor((abs % 100_000_000) / 10_000);
    if (man === 0) return `${sign}${oku}億円`;
    return `${sign}${oku}億${man.toLocaleString('ja-JP')}万円`;
  }
  if (abs >= 10_000) {
    const man = Math.floor(abs / 10_000);
    return `${sign}${man.toLocaleString('ja-JP')}万円`;
  }
  return `${sign}${abs.toLocaleString('ja-JP')}円`;
}

/** % で「+0.3%」「-1.0%」形式 */
function fmtPct(pct) {
  if (pct == null) return '-';
  const sign = pct >= 0 ? '+' : '';
  return `${sign}${pct.toFixed(1)}%`;
}

/** 比較情報を「+75万円 (+0.3%) ↑」形式で */
function fmtDiff(diff) {
  if (diff?.abs == null) return '比較不可';
  const arrow = diff.abs > 0 ? '↑' : (diff.abs < 0 ? '↓' : '→');
  const yen = fmtYen(diff.abs);
  // fmtYen は負値に '-' を付けるが、正値には符号がない → 正値だけ '+' を頭に補う
  const yenSigned = (diff.abs > 0) ? `+${yen}` : yen;
  return `${yenSigned} (${fmtPct(diff.pct)}) ${arrow}`;
}

/** business_date 'YYYY-MM-DD' から JST曜日表示 */
function fmtDateJa(dateStr) {
  // YYYY-MM-DD は日付のみ、UTCで作るとJSTでも同じ年月日
  const [y, m, d] = dateStr.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  const w = JST_WEEKDAYS[dt.getUTCDay()];
  return `${dateStr} (${w})`;
}

/**
 * getInventorySummary の戻り値を GChat メッセージ文字列に整形する。
 *
 * Codex 3巡目指摘準拠の原則:
 *   - 冒頭に「総在庫」「判定対象」「対象外」を3行で明確分離
 *   - 撤退検討/警戒は「判定対象内」ラベルで囲む
 *   - 補足で「FBA系は別ロジック/別ツール管理」を毎回明示
 *
 * @param {object} summary getInventorySummary の戻り値
 * @returns {string} GChatに送る本文 (mrkdwn)
 */
export function formatNotificationMessage(summary) {
  if (!summary || summary.error) {
    return `*在庫サマリ*\n⚠️ データ取得失敗: ${summary?.error || 'unknown'}`;
  }

  const dateLabel = fmtDateJa(summary.business_date);
  const total = summary.total_inventory;
  const target = summary.judgement_target;
  const outOfScope = summary.out_of_scope;
  const ret = summary.retirement_candidates;
  const warn = summary.warning;
  const ew = summary.early_warning;
  const cls = summary.classification;
  const dq = summary.data_quality;

  const lines = [];
  lines.push(`*在庫サマリ ${dateLabel}*`);
  lines.push('');

  // データ品質警告 (最優先)
  if (dq.has_failure) {
    lines.push(`⚠️ *[比較不可]* データ取得失敗カテゴリ: ${dq.partial_categories.join(', ')}`);
    lines.push('');
  }

  // ── 1. 総在庫 / 判定対象 / 対象外 (Codex指摘の3行分離) ──
  lines.push(`総在庫:    ${fmtYen(total.value)}  前日 ${fmtPct(total.diff_prev_day.pct)} / 前週 ${fmtPct(total.diff_week_ago.pct)} / 月初 ${fmtPct(total.diff_month_start.pct)}`);
  lines.push(`判定対象:  ${fmtYen(target.value)} (${target.ratio_pct.toFixed(0)}%)  ※${target.note}`);
  lines.push(`対象外:    ${fmtYen(outOfScope.value)} (${outOfScope.ratio_pct.toFixed(0)}%)  ※${outOfScope.note}`);
  lines.push('');

  // ── 2. 判定対象内のアラート ──
  const hasAnyAlert = ret.count > 0 || warn.count > 0 || ew.drop_count > 0;
  lines.push('*【判定対象内】*');

  if (ret.count > 0) {
    let line = `🔴 撤退検討候補: ${fmtYen(ret.value)} (${ret.count} SKU)`;
    lines.push(line);
    if (ret.supplier_top3 && ret.supplier_top3.length > 0) {
      const topStr = ret.supplier_top3
        .map(s => `${s.supplier_code} ${fmtYen(s.value)} (${s.count}件)`)
        .join(' / ');
      lines.push(`   仕入先別: ${topStr}`);
    }
  } else {
    lines.push(`✅ 撤退検討候補: 0 SKU`);
  }

  if (warn.count > 0) {
    let line = `🟡 警戒対象: ${fmtYen(warn.value)} (${warn.count} SKU)`;
    if (warn.top1_value > 0 && warn.value > 0) {
      const top1Pct = (warn.top1_value / warn.value * 100).toFixed(0);
      line += ` / 上位1 SKU ${fmtYen(warn.top1_value)} (${top1Pct}%)`;
    }
    lines.push(line);
  } else {
    lines.push(`✅ 警戒対象: 0 SKU`);
  }

  if (ew.drop_count > 0) {
    lines.push(`⚡ 急落検知: ${ew.drop_count} SKU (前30日が前90日平均の1/3以下)`);
  }

  lines.push('');

  // ── 3. 5分類構成 (在庫金額) ──
  lines.push('*5分類構成 (判定対象内):*');
  const totalClsValue = cls.good_stock.value + cls.observe.value + cls.price_down.value + cls.bundle_candidate.value + cls.other.value;
  function clsLine(label, item) {
    const pct = totalClsValue > 0 ? (item.value / totalClsValue * 100).toFixed(0) : 0;
    return `  ${label.padEnd(8, ' ')} ${fmtYen(item.value)} (${pct}%, ${item.count} SKU)`;
  }
  lines.push(clsLine('優良', cls.good_stock));
  lines.push(clsLine('観察', cls.observe));
  lines.push(clsLine('値下げ', cls.price_down));
  lines.push(clsLine('セット', cls.bundle_candidate));
  lines.push(clsLine('その他', cls.other));
  lines.push('');

  // ── 4. リンク・補足 ──
  lines.push('詳細▶ https://bfaith-portal.onrender.com/apps/profit-analysis');
  lines.push(`※FBA系在庫は別ロジック/別ツールで管理。上記の撤退・警戒は判定対象${fmtYen(target.value)}に対する判定です。`);

  // ── 5. 誤出荷セクション (Phase G、損失額抜きで率と件数のみ) ──
  // KPI 定義: 件数ベース (件数 / 出荷ライン数)。業界 ODR 0.10% との近似比較。
  if (summary._misShipment) {
    const m = summary._misShipment;
    lines.push('');
    lines.push('──────────');
    const periodLabel = m.period_label || '今月累計';
    lines.push(`*誤出荷 (${periodLabel}):*`);
    if (m.incident_rate_pct == null) {
      lines.push('  出荷ライン数 0 のため算出不可');
    } else {
      const rateStr = m.incident_rate_pct.toFixed(2) + '%';
      const targetStr = m.industry_target_pct.toFixed(2) + '%';
      const warn = m.over_target ? ' ⚠️ オーバー' : ' ✅';
      lines.push(`  誤出荷件数率: ${rateStr} (${m.incidents}件 / ${m.shipped_line_count.toLocaleString()}ライン)${warn}  目標 ≤ ${targetStr}`);
    }
    if (m.top_sku) {
      const skuMall = m.top_sku.mall ? ` (${m.top_sku.mall})` : '';
      lines.push(`  🏆 Top SKU: ${m.top_sku.sku || '-'}${skuMall} ${m.top_sku.incidents}件`);
    }
    if (m.top_root_cause) {
      lines.push(`  🔍 Top 根因: ${m.top_root_cause.root_cause_stage} ${m.top_root_cause.incidents}件`);
    }
    lines.push('  詳細▶ https://bfaith-portal.onrender.com/apps/mis-shipment/dashboard');
  }

  return lines.join('\n');
}

/**
 * 通知1回分を実行する。例外は飲み込まずログ出力のみ。
 * cron schedule とは独立に手動実行できるよう export する。
 */
export async function runNotificationJob() {
  const webhookUrl = process.env.GCHAT_WEBHOOK_INSIGHT;
  if (!webhookUrl) {
    console.warn('[notify-job] GCHAT_WEBHOOK_INSIGHT が未設定。通知をスキップ。');
    return { ok: false, reason: 'webhook_not_set' };
  }

  try {
    const db = getMirrorDB();
    const summary = getInventorySummary(db, {});

    // 誤出荷サマリを合成 (Phase G、失敗しても在庫通知は止めない)
    try {
      summary._misShipment = getMisShipmentNotifySummary({});
    } catch (e) {
      console.warn('[notify-job] 誤出荷サマリ取得失敗 (在庫通知は継続):', e.message);
    }

    const text = formatNotificationMessage(summary);

    console.log(`[notify-job] 送信開始 business_date=${summary.business_date}`);
    const result = await sendGChatMessage(webhookUrl, text);
    console.log(`[notify-job] 送信成功 status=${result.status}`);
    return { ok: true, business_date: summary.business_date };
  } catch (e) {
    console.error('[notify-job] 通知送信失敗:', e.message);
    return { ok: false, reason: e.message };
  }
}

/**
 * cron スケジュール起動。
 * INVENTORY_NOTIFY_ENABLED='true' のときだけ schedule する。
 */
export function startNotificationJob() {
  const enabled = process.env.INVENTORY_NOTIFY_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[notify-job] INVENTORY_NOTIFY_ENABLED が未設定/falseのためスケジュールしない (Dark Launch)');
    return null;
  }
  const cronExpr = process.env.INVENTORY_NOTIFY_CRON || '0 22 * * *'; // UTC 22:00 = JST 07:00
  if (!cron.validate(cronExpr)) {
    console.error(`[notify-job] INVENTORY_NOTIFY_CRON が不正: ${cronExpr}`);
    return null;
  }
  const task = cron.schedule(cronExpr, () => {
    runNotificationJob().catch(e => {
      console.error('[notify-job] cron 実行中に未捕捉例外:', e);
    });
  }, { timezone: 'UTC' });
  console.log(`[notify-job] cron schedule 起動: '${cronExpr}' (UTC) — JST 07:00 想定`);
  return task;
}
