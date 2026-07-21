/**
 * 入庫情報管理 — 日次 cron (新商品自動追加 + 入荷予定 nefuda.csv 取得)
 *
 * 1) mirror_products (単品・取扱中) のうち f_inbound_info 未登録のコードを INSERT
 * 2) Drive の nefuda.csv (入荷予定) を取得して f_inbound_schedule を full-replace
 * ミラー同期 (miniPC daily-sync 07:00 JST 起点) の完了後に走るよう既定は JST 09:00。
 * 片方の失敗はもう片方を巻き込まない (それぞれ try/catch)。
 *
 * 環境変数:
 *   INBOUND_INFO_SYNC_ENABLED=true  … cron 起動 (既定 off = Dark Launch)
 *   INBOUND_INFO_SYNC_CRON          … 上書き用 cron 式 (JST 基準)。既定 '0 9 * * *' = JST 09:00
 *   INBOUND_NEFUDA_FOLDER_ID / INBOUND_NEFUDA_FILE … nefuda.csv の場所 (nefuda-fetch.js 参照)
 *
 * cron が無効でも UI の「新商品を今すぐ取込」(POST /api/sync-now) と
 * 「入荷予定を更新」(POST /api/schedule/refresh) で同じ処理を実行できる。
 */
import cron from 'node-cron';
import { syncNewProducts } from './db.js';
import { refreshNefudaSchedule } from './nefuda-fetch.js';

export function startInboundInfoCron() {
  if (process.env.INBOUND_INFO_SYNC_ENABLED !== 'true') {
    console.log('[inbound-info] cron disabled (INBOUND_INFO_SYNC_ENABLED != true)');
    return;
  }
  // timezone を明示 (Codex R1 Low: 未指定だとプロセスのローカル TZ 依存になり、
  // 実行環境が変わるとミラー同期完了前に走り得る)。式は JST でそのまま読める形にする。
  const expr = process.env.INBOUND_INFO_SYNC_CRON || '0 9 * * *'; // JST 09:00
  if (!cron.validate(expr)) {
    console.error(`[inbound-info] invalid cron expr: ${expr} — cron not started`);
    return;
  }
  cron.schedule(expr, async () => {
    try {
      const r = syncNewProducts();
      if (!r.ok) {
        console.warn(`[inbound-info] sync skipped: ${r.error}`);
      } else {
        console.log(`[inbound-info] sync done: inserted=${r.inserted} (mirror_products=${r.mirror_products})`);
      }
    } catch (e) {
      console.error('[inbound-info] sync failed:', e.message);
    }
    try {
      const s = await refreshNefudaSchedule('cron');
      console.log(`[inbound-info] nefuda done: rows=${s.schedule_rows} added_to_master=${s.added_to_master}`
        + (s.not_in_master.length ? ` not_in_master=${s.not_in_master.join(',')}` : ''));
    } catch (e) {
      console.error('[inbound-info] nefuda fetch failed:', e.message);
    }
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[inbound-info] cron started (${expr} JST)`);
}
