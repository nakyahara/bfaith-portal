/**
 * 商品別 想定利益 — 夜間の入口 (miniPC で 23:30 に走る)
 *
 * 正本 = §8 / §15-10
 *
 * 流れ:
 *   出品列挙 + 価格取得 → 手数料の再見積もり → 世代を作る → 公開前検証
 *   → Render へ転送 → ポインタ切替 → 読み戻して確認 → ok ping
 *
 * 🚨 成功 ping は「Render の公開ポインタが対象世代になった」ことを確認してから打つ (§8.2)。
 * 🚨 全体終了期限 06:00 (§8.4)。超えたら安全に中断して次回に持ち越す。
 *    daily-sync (07:00・P1) の開始前に必ず終わらせる。
 *
 * 使い方:
 *   node apps/expected-profit/nightly.js
 *   node apps/expected-profit/nightly.js --skip-publish   (転送だけしない)
 *
 * 終了コード (ランナー run-expected-profit-nightly.ps1 との約束):
 *   0 = 成功し、ok も報告済み          → ランナーは何もしない
 *   5 = **成功したが報告できなかった**  → ランナーが代わりに ok ping を打つ
 *   3 = 失敗したが報告済み             → ランナーは重ねて ping しない
 *   1 = 失敗し、報告もできていない      → ランナーが代わりに fail ping を打つ
 *
 * 🚨 5 が要るのは、公開まで成功したのに ping だけ落ちる (401・タイムアウト) ことがあるため。
 *    0 で返すとランナーは「報告済み」と信じ、監視は古い状態のまま残る (Codex 3巡目)。
 */
import 'dotenv/config';
import { initExpectedProfitDB } from './db.js';
import { fetchAmazonListings, fetchRakutenListings } from './fetch-listings.js';
import { refreshFees } from './refresh-fees.js';
import { buildGeneration, validateGeneration } from './build-generation.js';
import { publishToRender, httpDeps } from './publish.js';
import { pruneGenerations } from './publish-api.js';
import {
  openWarehouseReadOnly, loadProducts, loadShippingRates, loadSkuMap, loadMasterFreshness,
} from './load-inputs.js';

const JOB_ID = 'expected-profit-nightly';

/** 全体終了期限 = 実行日の 06:00 JST (§8.4) */
export function deadlineOf(now = new Date()) {
  const jst = new Date(now.getTime() + 9 * 3600 * 1000);
  const d = new Date(Date.UTC(jst.getUTCFullYear(), jst.getUTCMonth(), jst.getUTCDate(), 6 - 9, 0, 0));
  // 23:30 に始まると翌日の 06:00 が期限になる
  if (d <= now) d.setUTCDate(d.getUTCDate() + 1);
  return d;
}

/** 手数料の見積対象を、当夜のスナップショットから作る */
export function feeTargetsFrom(rows, { sellerId, marketplaceId }) {
  return rows
    .filter(r => r.mall === 'amazon' && r.fetch_status === 'ok'
      && (r.fulfillment === 'FBA' || r.fulfillment === 'FBM'))
    .map(r => ({
      seller_id: sellerId,
      marketplace_id: marketplaceId,
      seller_sku: r.mall_item_key,
      asin: r.mall_item_ref,
      in_listing_price: r.price_incl_tax,
      // 🚨 送料も算定基礎に入る (§15-13)。送料込みなら 0
      in_shipping: r.postage_revenue_incl_tax ?? 0,
      in_points: r.points,
      in_fulfillment: r.fulfillment,
      in_currency: 'JPY',
    }));
}

/**
 * 監視への報告 URL を組み立てる。
 *
 * 🚨 受け口は **クエリの `status`** しか見ない (`apps/jobs-monitor/router.js`)。
 *    body に入れて送ると省略扱いになり、**失敗も「成功」として記録される** (Codex R15)。
 *    既存の `scripts/jobs-monitor/ping.ps1` と同じ形に揃える。
 * 🚨 `JOBS_MONITOR_URL` も末尾にパスが付きうるので origin だけを使う。
 */
export function pingUrl(jobId, status, note, env = process.env) {
  const raw = String(env.JOBS_MONITOR_URL || '').trim();
  let u;
  try { u = new URL(raw); } catch { return ''; }
  // 🚨 Bearer トークンを載せるので https だけ。file:// なども自然に弾ける
  if (u.protocol !== 'https:') return '';
  const origin = u.origin;
  const q = new URLSearchParams({ status: String(status || 'ok') });
  if (note) q.set('note', String(note).slice(0, 200));
  return `${origin}/apps/jobs-monitor/ping/${encodeURIComponent(jobId)}?${q}`;
}

/**
 * 監視へ報告する。
 *
 * 🚨 戻り値 = 「この版が報告を試みたか」。ランナー (run-expected-profit-nightly.ps1) は
 *    これを終了コードで受け取り、**自分では fail ping を打たない**。
 *    監視は last-state の上書き (jobs-monitor/store.js recordPing) なので、二重に打つと
 *    「公開できなかった: xxx」という具体的な理由が、ランナーの汎用文言で消える (Codex 2巡目)。
 */
async function ping(status, summary) {
  const token = process.env.JOBS_MONITOR_TOKEN;
  const url = pingUrl(JOB_ID, status, summary);
  // 設定が無いなら報告そのものが無い = ランナーが代わりに打つべき
  if (!url || !token) return false;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(30_000),   // 🚨 ping で固まらせない
    });
    // 🚨 応答も見る。400 で弾かれていても黙って成功したことにしない
    if (!res.ok) {
      console.warn(`[expected-profit] ping が受け付けられなかった: HTTP ${res.status}`);
      return false;
    }
    return true;
  } catch (e) {
    console.warn('[expected-profit] ping 失敗:', e.message);
    return false;
  }
}

export async function runNightly(opts = {}) {
  const now = opts.now || new Date();
  const deadline = opts.deadline || deadlineOf(now);
  const db = opts.db || initExpectedProfitDB();
  const sellerId = process.env.SP_API_SELLER_ID;
  const marketplaceId = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
  const log = opts.log || console.log;
  const result = { steps: [], ok: false, reported: false };
  // 🚨 報告できたかを持ち回る。ランナーはこれを終了コードで受け取り、二重に打たない
  const report = async (status, summary) => { if (await ping(status, summary)) result.reported = true; };

  // 🚨 期限は全工程で見る (§8.4)。手数料だけ見ても、出品取得やリトライが期限後まで走る
  const pastDeadline = () => new Date() >= deadline;
  const abortIfLate = (step) => {
    if (!pastDeadline()) return false;
    result.steps.push({ step, ok: false, error: 'deadline_exceeded' });
    log(`[expected-profit] 期限 (${deadline.toISOString()}) を過ぎたので ${step} を行わない`);
    return true;
  };

  // ── 1. 出品列挙 + 価格取得 (モール単位で fail-soft) ──
  for (const [mall, fn] of [['amazon', fetchAmazonListings], ['rakuten', fetchRakutenListings]]) {
    if (opts.malls && !opts.malls.includes(mall)) continue;
    if (abortIfLate(`fetch:${mall}`)) continue;   // 期限後は新しい取得を始めない
    try {
      const r = await fn(db, { deadline, ...(opts.fetchDeps?.[mall] || {}) });
      result.steps.push({ step: `fetch:${mall}`, ok: true, ...r });
      log(`[expected-profit] ${mall}: ${r.count}件 (${r.status})`);
    } catch (e) {
      // 🚨 1モールの失敗で全体を落とさない (§6.2)
      result.steps.push({ step: `fetch:${mall}`, ok: false, error: e.message });
      log(`[expected-profit] ${mall} 失敗: ${e.message}`);
    }
  }

  // ── 2. Amazon 手数料の再見積もり ──
  if (!opts.skipFees && !abortIfLate('fees')) {
    try {
      const latest = db.prepare(`
        SELECT s.* FROM mall_price_snapshot s
        JOIN (SELECT run_id FROM price_fetch_run WHERE mall='amazon' ORDER BY started_at DESC, rowid DESC LIMIT 1) r
          ON r.run_id = s.run_id
      `).all();
      const targets = feeTargetsFrom(latest, { sellerId, marketplaceId });
      const r = await refreshFees(db, targets, { deadline, now: () => new Date(), ...(opts.feeDeps || {}) });
      result.steps.push({ step: 'fees', ok: true, ...r });
      log(`[expected-profit] 手数料: 再取得${r.refreshed} 再利用${r.reused} 失敗${r.failedTargets} 未処理${r.pendingTargets}`
        + (r.deferred ? ` 翌晩に回した${r.deferred}` : '')
        // 前に失敗して待ち中の対象。ここが増え続けるなら値かカタログ側の問題
        + (r.waitingOnFailure ? ` 失敗待ち${r.waitingOnFailure}` : '')
        + (r.unusable ? ` 使えない見積${r.unusable}` : '')
        // 🚨 取り直した理由を必ず出す。これが無いと「なぜ再利用が効かなかったか」を後から追えない
        + (r.plannedRefetch ? ` 理由=${JSON.stringify(r.refetchReasons)}` : ''));
      // 🚨 キャッシュが効いていないなら必ず出す。SP-API は 0.5 req/s なので、
      //    全件取り直しは 13 分かかり、静かに毎晩やると枠を食い潰す
      if (r.refetchAnomaly) {
        log(`[expected-profit] 🚨 手数料の再利用が効いていない: ${r.cacheMisses}/${r.targets} 件がキャッシュに当たらなかった`
          + ` 理由=${JSON.stringify(r.refetchReasons)}`);
      }
    } catch (e) {
      result.steps.push({ step: 'fees', ok: false, error: e.message });
      log(`[expected-profit] 手数料 失敗: ${e.message}`);
    }
  }

  // ── 3. 世代を作る ──
  let wdb = null;
  let gen;
  if (abortIfLate('build')) {
    await report('fail', '期限を過ぎたので世代を作らなかった');
    return { ...result, error: 'deadline_exceeded' };
  }
  // 🚨 鮮度判定は「ビルド時点の時刻」で行う。夜間処理の開始時刻を使うと、
  //    取得に時間がかかった夜に期限切れを見逃す
  const buildNow = new Date();
  try {
    wdb = opts.warehouseDb || openWarehouseReadOnly();
    const warehouseInputs = {
      products: loadProducts(wdb),
      shippingRates: loadShippingRates(wdb),
      skuMaps: { amazon: loadSkuMap(wdb, 'amazon'), rakuten: loadSkuMap(wdb, 'rakuten') },
      masterFreshness: loadMasterFreshness(wdb),
    };
    gen = buildGeneration(db, {
      warehouseInputs, now: opts.now || buildNow, sellerId, marketplaceId,
      malls: opts.malls, codeVersion: process.env.GIT_SHA || 'dev',
    });
    result.steps.push({ step: 'build', ok: true, ...gen });
    log(`[expected-profit] 世代 ${gen.generationId}: ${gen.rowCount}行 (計算できた ${gen.okCount} / ランキング対象 ${gen.rankEligibleCount})`);
  } catch (e) {
    result.steps.push({ step: 'build', ok: false, error: e.message });
    await report('fail', `世代の作成に失敗: ${e.message}`);
    return { ...result, error: e.message };
  } finally {
    if (wdb && !opts.warehouseDb) { try { wdb.close(); } catch { /* noop */ } }
  }

  // ── 4. 公開前検証 ──
  const v = validateGeneration(db, gen.generationId);
  result.steps.push({ step: 'validate', ok: v.ok, errors: v.errors, warnings: v.warnings });
  if (!v.ok) {
    log(`[expected-profit] 検証で拒否: ${v.errors.join(' / ')}`);
    await report('fail', `世代の検証に失敗: ${v.errors.join(' / ')}`);
    return { ...result, error: 'validation_failed' };
  }

  // ── 5. Render へ転送して公開 ──
  if (opts.skipPublish) {
    log('[expected-profit] --skip-publish のため転送しない');
    return { ...result, ok: true, generationId: gen.generationId, skippedPublish: true };
  }
  if (abortIfLate('publish')) {
    // 世代は作れているので、翌日そのまま転送できる
    await report('fail', '期限を過ぎたので転送しなかった (世代は作成済み)');
    return { ...result, error: 'deadline_exceeded', generationId: gen.generationId };
  }
  try {
    const pub = await publishToRender(db, gen.generationId, { deadline, ...(opts.publishDeps || httpDeps(deadline)) });
    result.steps.push({ step: 'publish', ok: pub.ok, ...pub });
    if (!pub.ok) {
      await report('fail', `公開できなかった: ${pub.error}`);
      return { ...result, error: pub.error };
    }
    log(`[expected-profit] 公開 ${pub.generationId} (seq ${pub.seq})`);
  } catch (e) {
    result.steps.push({ step: 'publish', ok: false, error: e.message });
    await report('fail', `転送に失敗: ${e.message}`);
    return { ...result, error: e.message };
  }

  // ── 6. 掃除 ──
  try { pruneGenerations(db, 7); } catch { /* 掃除の失敗で ok を落とさない */ }

  // ── 7. 成功 ping (ここまで来て初めて ok) ──
  const degraded = gen.mallsDegraded || [];
  const summary = `${gen.rowCount}行 / ランキング対象 ${gen.rankEligibleCount}`
    + (degraded.length ? ` / 劣化: ${degraded.map(d => `${d.mall}(${d.reason})`).join(', ')}` : '');
  await report('ok', summary);
  return { ...result, ok: true, generationId: gen.generationId, degraded };
}

// ────────────────────────────────────────────────────────────
if (process.argv[1] && process.argv[1].endsWith('nightly.js')) {
  runNightly({
    skipPublish: process.argv.includes('--skip-publish'),
    skipFees: process.argv.includes('--skip-fees'),
  })
    .then(r => {
      console.log(JSON.stringify({ ok: r.ok, generationId: r.generationId, error: r.error, reported: r.reported }, null, 2));
      // 🚨 「報告できたか」まで含めて返す。ランナーはこの4値だけを見て、
      //    足りない報告を補い、済んでいる報告は重ねない (ヘッダの表を参照)
      process.exit(r.ok ? (r.reported ? 0 : 5) : (r.reported ? 3 : 1));
    })
    .catch(e => { console.error(e); process.exit(1); });
}
