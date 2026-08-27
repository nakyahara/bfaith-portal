/**
 * 商品リンク台帳 — 夜間照合 (product-hub 全ドラフト vs 台帳の product_hub 由来) + 起動時バックフィル。
 *
 * 保存フックが正で、これは「フックが落ちた分の自己修復」。新しいスケジュール入口を増やさず
 * Render 常駐 node-cron (product-hub intake と同じ形)。台帳 = jobs-registry 'product-links-reconcile'。
 *
 * env:
 *   PRODUCT_LINKS_RECONCILE_ENABLED=false … 止める (既定 ON)
 *   PRODUCT_LINKS_RECONCILE_CRON            … cron 式 (JST)。既定 09:45 (intake 09:30 の後)
 */
import cron from 'node-cron';
import { getDB } from './db.js';
import { reconcileAll } from './sync.js';
import { recordPing } from '../jobs-monitor/store.js';

const OFF = new Set(['0', 'false', 'off', 'no']);

function ping(status, note) {
  try { recordPing('product-links-reconcile', status, note, Date.now()); }
  catch (e) { console.error('[product-links] ping failed:', e.message); }
}

export function runReconcile(actor) {
  try {
    const r = reconcileAll(getDB(), { actor });
    const note = `drafts=${r.drafts} upserted=${r.upserted} detached=${r.detached} failed=${r.failed}`;
    console.log(`[product-links] reconcile: ${note}`);
    ping(r.ok ? 'ok' : 'fail', note);
    return r;
  } catch (e) {
    console.error('[product-links] reconcile failed:', e.message);
    ping('fail', String(e.message).slice(0, 180));
    return { ok: false, error: e.message };
  }
}

export function startProductLinksCron() {
  // テーブルは cron の有無に関わらず起動時に用意する (product-hub の保存フックが table_missing で空振りしないように)
  try { getDB(); } catch (e) { console.error('[product-links] init failed:', e.message); }
  if (OFF.has(String(process.env.PRODUCT_LINKS_RECONCILE_ENABLED ?? '').trim().toLowerCase())) {
    console.log('[product-links] reconcile cron: disabled');
    return;
  }
  const expr = (process.env.PRODUCT_LINKS_RECONCILE_CRON || '45 9 * * *').trim();
  if (!cron.validate(expr)) {
    console.error(`[product-links] reconcile cron: 不正な cron 式 "${expr}" — 起動しません`);
    return;
  }
  cron.schedule(expr, () => runReconcile('cron:product-links'), { timezone: 'Asia/Tokyo' });
  console.log(`[product-links] reconcile cron: enabled (${expr} JST)`);
  // 初回バックフィル: 台帳が空なら起動直後に一度写す (デプロイ日に「何も出ない」を避ける)。
  // 起動処理を遅らせないよう少し待ってから
  setTimeout(() => {
    try {
      const empty = getDB().prepare('SELECT COUNT(*) AS c FROM ph_product_links').get().c === 0;
      if (empty) { console.log('[product-links] 台帳が空なので初回バックフィルを実行します'); runReconcile('boot:backfill'); }
    } catch (e) { console.error('[product-links] backfill check failed:', e.message); }
  }, 15_000).unref?.();
}
