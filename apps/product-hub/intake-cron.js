/**
 * product-hub — NE 新商品の自動取込 cron (2026-07-25 中原さん指示)。
 *
 * Notion 側の「新商品カード自動作成」を止めたため、新商品はこのアプリに入る。
 * 毎日、miniPC daily-sync (07:00 JST 起点) による mirror_products 更新の後に走らせる。
 *
 * 環境変数:
 *   PH_INTAKE_CRON_ENABLED=1 … 有効化 (**既定 OFF**。初回はシードだけ行いドラフトを作らない)
 *   PH_INTAKE_CRON           … cron 式 (JST) を1本指定して既定を上書き。
 *                              既定は 09:30 / 10:30 / 12:00 の3回 — daily-sync が retry (08:30/10:00/11:30)
 *                              で遅れて完走した日も当日中に拾う (Codexレビュー: 09:30 1回だと翌日送り)。
 *                              取込は seen/既存判定で冪等 = 多重実行しても二重作成されない
 *
 * OFF のままでも一覧画面の「商品コードから一括登録」で同じ処理を手動実行できる。
 */
import cron from 'node-cron';
import { syncNewProducts } from './services/new-product-intake.js';
import { recordPing } from '../jobs-monitor/store.js';

// dead-man 監視 (jobs-registry: ph-ne-intake)。同一プロセスなので内部関数を直接呼ぶ。
// 監視の記録失敗で取込本体を巻き添えにしない
function ping(status, note) {
  try { recordPing('ph-ne-intake', status, note, Date.now()); }
  catch (e) { console.error('[product-hub] intake ping failed:', e.message); }
}

const ON = new Set(['1', 'true', 'on', 'yes']);

export function startProductHubIntakeCron() {
  if (!ON.has(String(process.env.PH_INTAKE_CRON_ENABLED ?? '').trim().toLowerCase())) {
    console.log('[product-hub] intake cron: disabled (PH_INTAKE_CRON_ENABLED 未設定)');
    return;
  }
  const exprs = process.env.PH_INTAKE_CRON
    ? [process.env.PH_INTAKE_CRON.trim()]
    : ['30 9 * * *', '30 10 * * *', '0 12 * * *'];
  const bad = exprs.filter((e) => !cron.validate(e));
  if (bad.length > 0) {
    console.error(`[product-hub] intake cron: 不正な cron 式 "${bad.join('", "')}" — 起動しません`);
    return;
  }
  const run = () => {
    try {
      const r = syncNewProducts({ actor: 'cron:ne-intake' });
      if (!r.ok) {
        console.warn(`[product-hub] intake skipped: ${r.error}`);
        ping('fail', String(r.error).slice(0, 180));
      } else if (r.mode === 'seed') {
        console.log(`[product-hub] intake seeded: ${r.seeded} codes (初回カットオフ。ドラフトは作っていません)`);
        ping('ok', `seed ${r.seeded}`);
      } else {
        console.log(`[product-hub] intake done: created=${r.created} merged=${r.merged}${r.capped ? ' (上限到達)' : ''}`);
        ping('ok', `created=${r.created} merged=${r.merged}`);
      }
    } catch (e) {
      console.error('[product-hub] intake failed:', e.message);
      ping('fail', String(e.message).slice(0, 180));
    }
  };
  for (const expr of exprs) cron.schedule(expr, run, { timezone: 'Asia/Tokyo' });
  console.log(`[product-hub] intake cron: enabled (${exprs.join(' / ')} JST)`);
}
