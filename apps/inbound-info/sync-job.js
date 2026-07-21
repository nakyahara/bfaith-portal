/**
 * 入庫情報管理 — 新商品自動追加 cron
 *
 * mirror_products (単品・取扱中) のうち f_inbound_info 未登録のコードを毎日 INSERT する。
 * ミラー同期 (miniPC daily-sync 07:00 JST 起点) の完了後に走るよう既定は JST 09:00。
 *
 * 環境変数:
 *   INBOUND_INFO_SYNC_ENABLED=true  … cron 起動 (既定 off = Dark Launch)
 *   INBOUND_INFO_SYNC_CRON          … 上書き用 cron 式 (JST 基準)。既定 '0 9 * * *' = JST 09:00
 *
 * cron が無効でも UI の「新商品を今すぐ取込」(POST /api/sync-now) で同じ処理を実行できる。
 */
import cron from 'node-cron';
import { syncNewProducts } from './db.js';

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
  cron.schedule(expr, () => {
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
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[inbound-info] cron started (${expr} JST)`);
}
