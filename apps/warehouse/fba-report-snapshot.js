/**
 * fba-report-snapshot.js — SP-API レポート (RESTOCK + PLANNING) を取って fba.db の日次スナップショットに保存する「本体」。
 *
 * 🚨 なぜ切り出したか (2026-09-20):
 *   fba.db は sql.js = 起動時にファイル全体をメモリへ読み、保存のたびに **ファイル全体を書き戻す**。
 *   書き手が 2 プロセスあった: 朝の cron (snapshot-fba-stock.js・毎朝読み直して書く) と 常駐の WarehouseServer (起動時に読んだメモリを持ち続ける)。
 *   → 常駐側が何か保存した瞬間に、古いメモリでファイルごと上書きされ、cron が入れた日が消えていた
 *     (本番で確認: ファイルの最新 9/20・常駐のメモリの最新 9/17。9/18・9/19 はログに「inserted=3992 ✅」と出ているのにファイルに無い。164 日の範囲に 137 日ぶん)。
 *   逆向きも同じ: cron は initDb() の後に 1〜2 分 SP-API を待ってから書くので、その間に常駐側が保存したもの (下書きなど) を cron が消す。
 *
 * 直し方 = **fba.db の書き手を常駐サーバ 1 つにする**:
 *   - この本体を、常駐サーバ (POST /service-api/fba/snapshot-reports) と cron の --direct (常駐サーバを止めてあるときの手動用) の両方が使う
 *   - cron (snapshot-fba-stock.js) は常駐サーバに頼んで終わりを待つ
 *   - それでも外から書かれたときの歯止めは fba-replenishment/db.js の saveToFile() (黙って上書きしない)
 *
 * 保存の順番は今までの cron と同じ: ① RESTOCK 先行 (在庫の区分を確定) → ② PLANNING (販売・価格などを上書き。在庫の区分は保持) → ③ US (env がそろっているときだけ。失敗しても JP は成功のまま)
 */
import { fetchAllReports, normalizePlanningRow, normalizeRestockRow, getMarketplaceContext } from '../fba-replenishment/sp-api-reports.js';
import { saveUsReportRun } from './fba-us-reports-store.js';

export const isBusinessDate = (v) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v) && !Number.isNaN(Date.parse(`${v}T00:00:00Z`)) && new Date(`${v}T00:00:00Z`).toISOString().slice(0, 10) === v;

/** Date を JST (UTC+9) の YYYY-MM-DD に */
export function toJstDate(d) {
  return new Date(d.getTime() + 9 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

/** 取得済みの JP のレポートを保存する (DB の関数は引数で受ける = 試験で差し替えられる) */
export function saveJpReports(db, results, businessDate, { log = console.log, warn = console.warn, fetchedAt = new Date().toISOString() } = {}) {
  const out = { planning: 0, restockDaily: 0, restockLatest: 0, planningLatest: 0, errors: results.errors || [], exportSaved: null, exportError: null };
  let exportRestock = [], exportPlanning = [];
  // ① RESTOCK 先行: daily_snapshots の在庫の区分を確定。先に書いておけば、後続 PLANNING の ON CONFLICT DO UPDATE で区分が保持される
  if (results.restock?.length > 0) {
    const normalized = results.restock.map(normalizeRestockRow).filter((r) => r.amazon_sku);
    exportRestock = normalized;
    const saved = db.saveRestockInventoryToDailySnapshot(normalized, businessDate);
    out.restockDaily = (saved.updated || 0) + (saved.inserted || 0);
    log(`[fba-stock-snapshot] RESTOCK → daily_snapshots: updated=${saved.updated} inserted=${saved.inserted}`);
    try { out.restockLatest = db.saveRestockLatest(normalized).saved || 0; } catch (e) { rethrowIfExternalWrite(e); warn('[fba-stock-snapshot] saveRestockLatest 失敗:', e.message); }
    const fnskuRows = normalized.filter((r) => r.fnsku && r.amazon_sku).map((r) => ({ sku: r.amazon_sku, fnsku: r.fnsku }));
    if (fnskuRows.length > 0) db.updateFnskuBatch(fnskuRows);
  }
  // ② PLANNING で 販売・価格・days_of_supply などを上書き (在庫の区分は保持)
  if (results.planning?.length > 0) {
    const normalized = results.planning.map(normalizePlanningRow);
    exportPlanning = normalized;
    out.planning = db.savePlanningData(normalized, businessDate);
    log(`[fba-stock-snapshot] PLANNING → daily_snapshots: ${out.planning}件 (3カラムは保持)`);
    try { out.planningLatest = db.savePlanningLatest(normalized).saved || 0; } catch (e) { rethrowIfExternalWrite(e); warn('[fba-stock-snapshot] savePlanningLatest 失敗:', e.message); }
    const fnskuRows = results.planning.filter((r) => r['sku']).map((r) => ({ sku: r['sku'], fnsku: r['fnsku'] || null }));
    if (fnskuRows.length > 0) db.syncFnskuBatch(fnskuRows);
  }
  if (out.errors.length) warn('[fba-stock-snapshot] errors:', JSON.stringify(out.errors));
  Object.assign(out, saveExport(db, { snapshotDate: businessDate, market: 'jp', restockRows: exportRestock, planningRows: exportPlanning, capturedAt: fetchedAt }, warn));
  return out;
}

/**
 * Company DB へ送る版を作る (db.saveStockExport)。🚨 daily_snapshots からではなく、**この回に取得した行そのもの** から作る
 * (RESTOCK に無い SKU の 3 区分を 0 と確定しない・値と取得時刻を同じ回のものにする。Codex #1388 R1)。
 * 版を作れなくても、在庫補充の側の保存 (上の daily_snapshots など) は済んでいるので朝のスナップショットは失敗にしない = その日は Company DB へ「一部だけ取れた日」として送られる。
 * ただし fba.db の保存の競合 (FBA_DB_*) は握りつぶさない
 */
function saveExport(db, args, warn) {
  try {
    const r = db.saveStockExport(args);
    return { exportSaved: r.saved, exportError: r.saved ? null : r.reason };
  } catch (e) {
    rethrowIfExternalWrite(e);
    warn(`[fba-stock-snapshot:${args.market}] Company DB へ送る版を作れなかった:`, e.message);
    return { exportSaved: false, exportError: String(e.message).slice(0, 160) };
  }
}

/** fba.db の保存の競合・読み直し (code が FBA_DB_ で始まる = db.js の isFbaDbConflict と同じ判定) は握りつぶさない (保存されていない = この回は失敗。やり直せば通る) */
function rethrowIfExternalWrite(e) { if (e && typeof e.code === 'string' && e.code.startsWith('FBA_DB_')) throw e; }

/**
 * 本体。db = fba-replenishment/db.js の名前空間 (initDb 済み)。
 * inboundCapture (FBA 補充 B1): (phase: 'S0'|'S1', ctx) => Promise<{ note }>。レポートを頼む直前 (S0) と JP を保存した直後 (S1) に
 *   納品プラン・出荷便の状態を記録する (apps/warehouse/inbound-baseline.js)。🚨 投げても日次処理は止めない (注記だけ)
 * @returns {{ businessDate, jp: {planning, restockDaily, restockLatest, planningLatest, errors}, us: null | {planning, restock, inserted, updated, errors} | {error}, ok: boolean, lastLine: string }}
 *   ok = JP のレポートが 1 つも取れなかった回は false (今までは exit 0 で「✅ 完了: planning=0 restock=0」だった = 9/17 の 403 の朝も緑だった)
 */
export async function runFbaReportSnapshot({ db, businessDate, fetchReports = fetchAllReports, usContext = getMarketplaceContext('us'), saveUsRaw = saveUsReportRun, log = console.log, warn = console.warn, inboundCapture = null }) {
  if (!isBusinessDate(businessDate)) throw new Error(`business_date が不正: ${businessDate}`);
  const inboundNotes = [];
  const capture = async (phase, ctx) => {
    if (!inboundCapture) return;
    try {
      const r = await inboundCapture(phase, ctx);
      if (r?.note) { inboundNotes.push(r.note); log(`[fba-stock-snapshot:inbound] ${r.note}`); }
    } catch (e) { inboundNotes.push(`${phase} 失敗: ${String(e.message).slice(0, 80)}`); warn('[fba-stock-snapshot:inbound]', e.message); }
  };
  await capture('S0', { businessDate });
  log('[fba-stock-snapshot] SP-API レポートを取得中...');
  const t0 = Date.now();
  const results = await fetchReports();
  const fetchedAt = new Date().toISOString();   // Company DB へ送る版の取得時刻 (レポートを取り終えた時刻)
  log(`[fba-stock-snapshot] 取得完了 (${((Date.now() - t0) / 1000).toFixed(1)}秒): planning=${results.planning?.length || 0} restock=${results.restock?.length || 0} errors=${(results.errors || []).length}`);
  const jp = saveJpReports(db, results, businessDate, { log, warn, fetchedAt });
  log(`[fba-stock-snapshot:jp] 完了: planning=${jp.planning} restock_daily=${jp.restockDaily} restock_latest=${jp.restockLatest} planning_latest=${jp.planningLatest}`);
  {
    let freshness = null;
    try { freshness = typeof db.getInputFreshness === 'function' ? db.getInputFreshness() : null; } catch { freshness = null; }
    const restockRows = (results.restock || []).map(normalizeRestockRow).filter((r) => r.amazon_sku);
    await capture('S1', { businessDate, fetchedAt, restockRows, freshness, saved: { restock: jp.restockLatest > 0, planning: jp.planningLatest > 0 } });
  }

  let us = null;
  if (usContext && usContext.refresh_token && usContext.client_id && usContext.client_secret) {
    log('[fba-stock-snapshot:us] US (NA リージョン) SP-API 取得開始...');
    const usAttemptedAt = new Date().toISOString();
    let rawRecorded = false;
    // 取れたままの行を別ファイルに残す (米国FBA納品アプリが読む)。失敗しても今までの保存と結果は変えない (警告だけ)
    const recordRaw = (args) => {
      rawRecorded = true;
      try { return saveUsRaw({ businessDate, attemptedAt: usAttemptedAt, ...args }); }
      catch (e) { warn('[fba-stock-snapshot:us] 取れたままのレポートを残せなかった:', e.message); return { error: String(e.message).slice(0, 160) }; }
    };
    let rawSaved = null;
    try {
      const tUs = Date.now();
      const usResults = await fetchReports(usContext);
      const usFetchedAt = new Date().toISOString();
      rawSaved = recordRaw({ fetchedAt: usFetchedAt, results: usResults });
      log(`[fba-stock-snapshot:us] 取得完了 (${((Date.now() - tUs) / 1000).toFixed(1)}秒): planning=${usResults.planning?.length || 0} restock=${usResults.restock?.length || 0} errors=${(usResults.errors || []).length}`);
      const planningRows = (usResults.planning || []).map(normalizePlanningRow);
      const restockRows = (usResults.restock || []).map(normalizeRestockRow).filter((r) => r.amazon_sku);
      const saved = db.saveUsDailySnapshots({ planningRows, restockRows, snapshotDate: businessDate });
      log(`[fba-stock-snapshot:us] daily_snapshots_us: inserted=${saved.inserted} updated=${saved.updated}`);
      if (usResults.errors?.length) warn('[fba-stock-snapshot:us] errors:', JSON.stringify(usResults.errors));
      const usExport = saveExport(db, { snapshotDate: businessDate, market: 'us', restockRows, planningRows, capturedAt: usFetchedAt }, warn);
      us = { planning: planningRows.length, restock: restockRows.length, inserted: saved.inserted, updated: saved.updated, errors: usResults.errors || [], ...usExport, rawSaved };
    } catch (e) {
      if (!rawRecorded) rawSaved = recordRaw({ fetchedAt: null, results: null, error: e.message });   // 取得そのものの失敗も「最後の取得」として残す
      rethrowIfExternalWrite(e);
      warn('[fba-stock-snapshot:us] 失敗 (JP は保存済みなので全体は失敗にしない):', e.message);   // 今までどおり
      us = { error: e.message, rawSaved };
    }
  } else {
    log('[fba-stock-snapshot:us] env (SP_API_*_US) 未設定のためスキップ');
  }

  const ok = jp.restockDaily > 0 || jp.planning > 0;
  // 版を作らなかった理由のうち、知らせるもの: 最初の版を残した (keep_first_version)・行が無い (no_rows) は正常。PLANNING が取れていない (no_planning) は、その日が Company DB で「一部だけ取れた日」になるので知らせる
  const quiet = ['keep_first_version', 'no_rows'];
  const failedExports = [['JP', jp], ['US', us]].filter(([, x]) => x && x.exportSaved === false && !quiet.includes(x.exportError)).map(([m, x]) => `${m}: ${x.exportError}`);
  const exportNote = ok && failedExports.length ? ` / ⚠️ Company DB へ送る版を作れなかった (${failedExports.join(' / ')})` : '';
  const usRawNote = us && us.rawSaved && us.rawSaved.error ? ` (取れたままのレポートを残せなかった: ${us.rawSaved.error.slice(0, 60)})` : '';
  const usNote = (us == null ? 'US 未設定' : us.error ? `US ❌ ${String(us.error).slice(0, 80)}` : `US planning=${us.planning} restock=${us.restock}`) + usRawNote;
  const jpNote = `JP restock=${jp.restockDaily} planning=${jp.planning}${jp.errors.length ? ` (取れなかったレポート: ${jp.errors.map((x) => x.report || '?').join(', ')})` : ''}`;
  const lastLine = ok
    ? `${jp.errors.length || jp.restockDaily === 0 || exportNote ? '⚠️' : '✅'} FBA在庫スナップショット ${businessDate}: ${jpNote} / ${usNote}${jp.restockDaily === 0 ? ' / 🚨 RESTOCK が取れていない = この日の FC 移管中・処理中・出荷待ちは 0 ではなく不明' : ''}${exportNote}`
    : `❌ FBA在庫スナップショット ${businessDate}: JP のレポートが 1 つも取れなかった (${(jp.errors || []).map((x) => `${x.report || '?'}: ${String(x.error).slice(0, 80)}`).join(' / ') || '0 件'}) / ${usNote}`;
  return { businessDate, jp, us, ok, lastLine: inboundNotes.length ? `${lastLine} / 準備中の基準: ${inboundNotes.join(' → ')}` : lastLine, inbound: inboundNotes };
}
