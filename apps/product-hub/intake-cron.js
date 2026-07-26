/**
 * product-hub — NE 新商品の自動取込 cron (2026-07-25 中原さん指示)。
 *
 * Notion 側の「新商品カード自動作成」を止めたため、新商品はこのアプリに入る。
 * 毎日、miniPC daily-sync (07:00 JST 起点) による mirror_products 更新の後に走らせる。
 *
 * 環境変数:
 *   PH_INTAKE_CRON_ENABLED=1 … 有効化 (**既定 OFF**。初回はシードだけ行いドラフトを作らない)
 *   PH_INTAKE_CRON           … cron 式 (JST)。既定 '30 9 * * *' (09:30。inbound-info の 09:00 の後)
 *
 * OFF のままでも一覧画面の「商品コードから一括登録」で同じ処理を手動実行できる。
 */
import cron from 'node-cron';
import { syncNewProducts } from './services/new-product-intake.js';

const ON = new Set(['1', 'true', 'on', 'yes']);

export function startProductHubIntakeCron() {
  if (!ON.has(String(process.env.PH_INTAKE_CRON_ENABLED ?? '').trim().toLowerCase())) {
    console.log('[product-hub] intake cron: disabled (PH_INTAKE_CRON_ENABLED 未設定)');
    return;
  }
  const expr = (process.env.PH_INTAKE_CRON || '30 9 * * *').trim();
  if (!cron.validate(expr)) {
    console.error(`[product-hub] intake cron: 不正な cron 式 "${expr}" — 起動しません`);
    return;
  }
  cron.schedule(expr, () => {
    try {
      const r = syncNewProducts({ actor: 'cron:ne-intake' });
      if (!r.ok) console.warn(`[product-hub] intake skipped: ${r.error}`);
      else if (r.mode === 'seed') console.log(`[product-hub] intake seeded: ${r.seeded} codes (初回カットオフ。ドラフトは作っていません)`);
      else console.log(`[product-hub] intake done: created=${r.created} merged=${r.merged}${r.capped ? ' (上限到達)' : ''}`);
    } catch (e) {
      console.error('[product-hub] intake failed:', e.message);
    }
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[product-hub] intake cron: enabled (${expr} JST)`);
}
