#!/usr/bin/env node
/**
 * notify-classification-unmapped.js — LINEギフト Phase 1 A-5b
 *   UNMAPPED 商品 (未分類 × 直近7日売上上位20) を Google Chat に通知
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-7
 *
 * 通知先: env GCHAT_WEBHOOK_CLASSIFICATION_OPS (作業系、既存 GCHAT_WEBHOOK_INSIGHT とは別経路)
 *
 * best-effort 設計:
 *   - webhook 未設定 → [NOTIFY:status=disabled] でログ、exit 0
 *   - 0 件 → [NOTIFY:status=skipped_empty] でログ、exit 0
 *   - 送信失敗 → [NOTIFY:status=failed] でログ、exit 0 (daily-sync 全体は止めない)
 *   - 成功 → [NOTIFY:status=sent] でログ、exit 0
 *
 * 終端マーカー: feedback_notify_status_terminal_marker.md 反映
 *
 * 使い方:
 *   node apps/warehouse/notify-classification-unmapped.js              実行
 *   node apps/warehouse/notify-classification-unmapped.js --dry-run    通知文だけ生成、HTTP送信なし
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';

const WEBHOOK = process.env.GCHAT_WEBHOOK_CLASSIFICATION_OPS;
const TOP_N = 20;

function fmtJpy(n) {
  return '¥' + Number(n || 0).toLocaleString('ja-JP');
}

function jstWallclock() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

function jstDateLabel() {
  const j = jstWallclock();
  return `${j.getUTCFullYear()}/${String(j.getUTCMonth() + 1).padStart(2, '0')}/${String(j.getUTCDate()).padStart(2, '0')}`;
}

function loadUnmappedTopN(limit) {
  const db = getDB();
  return db.prepare(`
    WITH unmapped AS (
      SELECT
        x.rakuten_shop_code, x.rakuten_item_code, x.rakuten_item_name, x.rakuten_item_url,
        r.genre_id, r.title AS raw_title
      FROM x_rakuten_item_product_map x
      LEFT JOIN raw_rakuten_items_master r
        ON r.rakuten_shop_code = x.rakuten_shop_code
       AND r.manage_number     = x.rakuten_item_code
       AND r.is_active = 1
       AND r.valid_to IS NULL
      WHERE x.is_active = 1
        AND x.valid_to IS NULL
        AND x.mapping_type = 'UNMAPPED'
    )
    SELECT
      u.rakuten_shop_code, u.rakuten_item_code, u.rakuten_item_name, u.rakuten_item_url,
      u.genre_id,
      g.genre_name,
      COALESCE(SUM(f.gross_sales_jpy_incl), 0) AS sales_7d,
      COALESCE(SUM(f.units_net_sold), 0)       AS units_7d
    FROM unmapped u
    LEFT JOIN m_rakuten_genres g
      ON g.genre_id = u.genre_id
     AND g.is_active = 1
     AND g.valid_to IS NULL
    LEFT JOIN f_linegift_finance_sku_daily_v1 f
      ON f.sku_code = u.rakuten_item_code
     AND f.date_jst >= date('now', '+9 hours', '-7 days')
    GROUP BY u.rakuten_shop_code, u.rakuten_item_code, u.rakuten_item_name, u.rakuten_item_url,
             u.genre_id, g.genre_name
    HAVING sales_7d > 0 OR units_7d > 0
    ORDER BY sales_7d DESC, units_7d DESC
    LIMIT ?
  `).all(limit);
}

function buildMessage(rows, totalUnmapped) {
  const lines = [];
  lines.push(`*LINEギフト 商品分類サマリ ${jstDateLabel()}*`);
  lines.push('');
  lines.push(`UNMAPPED 全体: *${totalUnmapped}* 件`);
  lines.push(`直近7日売上ありの上位: *${rows.length}* 件 (上限 ${TOP_N})`);
  lines.push('');
  if (rows.length === 0) {
    lines.push('_直近7日の売上がある UNMAPPED 商品はありません_');
  } else {
    lines.push('```');
    lines.push('順 | 売上(7d)    | 件数 | 楽天商品コード   | 商品名');
    rows.forEach((r, i) => {
      const rank = String(i + 1).padStart(2);
      const sales = fmtJpy(r.sales_7d).padStart(12);
      const units = String(r.units_7d).padStart(4);
      const code = String(r.rakuten_item_code).slice(0, 18).padEnd(18);
      const name = String(r.rakuten_item_name || r.raw_title || '').slice(0, 30);
      lines.push(`${rank} | ${sales} | ${units} | ${code} | ${name}`);
    });
    lines.push('```');
    lines.push('');
    lines.push('紐付け作業: /apps/sales-analytics-linegift/');
  }
  return lines.join('\n');
}

async function postToGChat(text) {
  const res = await fetch(WEBHOOK, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json; charset=UTF-8' },
    body: JSON.stringify({ text }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');

  if (!WEBHOOK && !dryRun) {
    console.log('[notify-classification-unmapped] webhook 未設定 (GCHAT_WEBHOOK_CLASSIFICATION_OPS)、skip');
    console.log('[NOTIFY:status=disabled]');
    return;
  }

  initDB();
  const db = getDB();

  const totalUnmappedRow = db.prepare(`
    SELECT COUNT(*) AS n FROM x_rakuten_item_product_map
    WHERE is_active=1 AND valid_to IS NULL AND mapping_type='UNMAPPED'
  `).get();
  const totalUnmapped = totalUnmappedRow.n;

  const rows = loadUnmappedTopN(TOP_N);

  if (rows.length === 0 && totalUnmapped === 0) {
    console.log('[notify-classification-unmapped] UNMAPPED 0 件、通知スキップ');
    console.log('[NOTIFY:status=skipped_empty]');
    return;
  }

  const message = buildMessage(rows, totalUnmapped);
  console.log('[notify-classification-unmapped] message preview:');
  console.log(message);

  if (dryRun) {
    console.log('[notify-classification-unmapped] dry-run、HTTP送信なし');
    console.log('[NOTIFY:status=dry_run]');
    return;
  }

  try {
    await postToGChat(message);
    console.log('[notify-classification-unmapped] sent');
    console.log('[NOTIFY:status=sent]');
  } catch (e) {
    console.error('[notify-classification-unmapped] post failed:', e.message);
    console.log('[NOTIFY:status=failed]');
    // best-effort: 失敗しても exit 0 (daily-sync 全体は止めない)
  }
}

main().catch((e) => {
  console.error('[notify-classification-unmapped] FATAL:', e);
  console.log('[NOTIFY:status=fatal]');
  process.exit(0); // best-effort
});
