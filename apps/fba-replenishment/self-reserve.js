import { parseCsv, decodeCsvBuffer } from './picking-csv.js';

/**
 * 倉庫在庫の配分 — 自社出荷ぶんを残し、FBA と自社を「同じ日数分」にそろえる (2026-09-24)
 *
 * 背景: 推奨数 = min(FBA に必要な数, 倉庫在庫) で、自社出荷 (楽天/Yahoo/auPay/Qoo10/LINEギフト/
 *   Amazon FBM 等) のぶんを 1 個も残していなかった → 自社出荷の欠品が相次いだ (8/18〜9/17 の FBA 伝票で
 *   自社在庫が 14 日以内に 0 になった 53 件)。多くの商品は Amazon で FBM 併売しているので、
 *   FBA が先に切れても自社倉庫に残っていれば Amazon の売上は FBM で拾える (FBA 欠品日の FBM 販売は 1.8 倍)。
 *   倉庫を空にすると FBA 以外の全チャネルが止まる。
 *
 * 規則 (NE 商品コード = 構成品 c ごと。数量はすべて構成品の個数):
 *   W_c  = 倉庫の出荷可能在庫 − まだ出ていない FBA 伝票ぶん
 *   rS_c = 自社出荷の日販 (NE 受注。FBM を含み、FBA 納品は含まない)
 *   SKU i の不足 (T 日分まで): short_i(T) = max(0, T × rF_i − F_i)   (rF_i = FBA 日販、F_i = 実質 FBA 在庫)
 *   そろう日数 T_c = 「Σ_i q_ic × short_i(T) + T × rS_c ≤ W_c」を満たす最大の T (二分探索)
 *   SKU i が c から受け取れる上限 = floor(short_i(T_c))
 *   🚨 SKU ごとに不足を出してから足す。構成品でまとめて F を足すと、売れないセットの FBA 在庫が
 *      売れる単品の補充を止めてしまう (Codex 2026-09-24 設計レビュー High 1)
 *
 * 配分: 推奨 SKU を緊急度の高い順 (同点は SKU 順) に回し、今の推奨数を「削るだけ」(増やさない)。
 *   実際に出す数だけ倉庫在庫・期限ごとの在庫から引く = 同じ NE 商品を複数 Amazon SKU が取り合って
 *   倉庫在庫を超える問題もここで止まる。🚨 この取り合いの歯止めは「同じ日数」を切っても・自社の
 *   日販が取れなくても常にかける (切り替えるのは自社ぶんの上限だけ。Codex High 3)
 */

/**
 * @param {object[]} items  generateRecommendations の items (緊急度順に並べたあと)。各 item に
 *   `_units: [{ code, qty }]` (norm 済み NE 商品コードと 1 個あたりの構成数) と
 *   `_expiry: [{ code, expiry, total }]` (期限で縛られる構成品の、最初に引き当たる期限の在庫) が要る
 * @param {object} ctx
 * @param {(code: string) => number} ctx.warehouseOf    norm 済みコード → 倉庫の出荷可能在庫 (出荷待ち伝票を引く前)
 * @param {(code: string) => number} ctx.pendingOf      norm 済みコード → まだ出ていない FBA 伝票の数
 * @param {(code: string) => (number|null)} ctx.selfDailyOf  norm 済みコード → 自社日販。null = 分からない (上限をかけない)
 * @param {Set<string>} ctx.excluded   恒久除外 SKU (norm 済み)。在庫の配分を受けない
 * @param {(sku: string) => string} ctx.norm
 * @param {number} ctx.minShipmentDays  最低出荷日数 (設定 min_shipment_cover_days)
 */
export function allocateWarehouse(items, ctx) {
  const { warehouseOf, pendingOf, selfDailyOf, excluded, norm, minShipmentDays } = ctx;
  const isExcluded = (it) => excluded.has(norm(it.amazon_sku));

  // --- 構成品ごとの材料 ---
  const perCode = new Map();
  const codeOf = (code) => {
    let c = perCode.get(code);
    if (!c) {
      const raw = Math.max(0, Number(warehouseOf(code)) || 0);
      const pending = Math.max(0, Number(pendingOf(code)) || 0);
      c = { code, raw, pending, W: Math.max(0, raw - pending), rS: selfDailyOf(code), members: [], T: Infinity };
      perCode.set(code, c);
    }
    return c;
  };
  for (const it of items) {
    if (isExcluded(it)) continue;              // 出さない SKU の分は取り置かない
    for (const u of it._units || []) {
      codeOf(u.code).members.push({
        q: u.qty,
        rF: Math.max(0, Number(it.units_sold_30d) || 0) / 30,
        F: Math.max(0, Number(it.effective_fba_stock) || 0),
      });
    }
  }
  for (const c of perCode.values()) c.T = equalDays(c);

  // --- 緊急度の高い順に配る (同点は SKU 順で結果を安定させる) ---
  //   v3-2: 推奨が少ない日に早めに送る SKU (pull_forward) は、通常の補充を全部配ったあとに残りから (Codex v3 設計レビュー High 5)
  const order = items.map((it, i) => ({ it, i }))
    .sort((a, b) => (a.it.pull_forward ? 1 : 0) - (b.it.pull_forward ? 1 : 0)
      || (b.it.urgency_score || 0) - (a.it.urgency_score || 0)
      || String(a.it.amazon_sku).localeCompare(String(b.it.amazon_sku)) || a.i - b.i);
  const remW = new Map([...perCode.values()].map((c) => [c.code, c.W]));
  // 期限ごとの在庫 (最初に引き当たる期限のロット)。出荷待ちの FBA 伝票も同じ最古ロットから出ていくので先に引く
  //   (🚨 総在庫からだけ引くと、同じ期限で送れる数を多く見積もる。Codex PR レビュー R1 High 3)
  const remExp = new Map();
  for (const it of items) for (const e of it._expiry || []) {
    const k = `${e.code}|${e.expiry}`;
    if (!remExp.has(k)) remExp.set(k, Math.max(0, e.total - (perCode.get(e.code)?.pending || 0)));
  }

  const totals = { skus_self: 0, units_self: 0, skus_shared: 0, units_shared: 0, skus_min_days: 0, units_min_days: 0 };
  for (const { it } of order) {
    const units = it._units || [];
    // 同じ構成品・同じ期限は 1 つだけ (セットに同じ構成品が 2 行あっても、統合した構成数で 1 回だけ引く。Codex R5 Medium 2)
    const pools = [...new Map((it._expiry || []).map((e) => [`${e.code}|${e.expiry}`, e])).values()];
    const before = Math.max(0, Number(it.adjusted_qty) || 0);
    if (before <= 0 || units.length === 0 || isExcluded(it)) continue;

    // ① 自社ぶんの上限 (SKU の個数)
    const rF = Math.max(0, Number(it.units_sold_30d) || 0) / 30;
    const F = Math.max(0, Number(it.effective_fba_stock) || 0);
    let capSelf = Infinity;
    for (const u of units) {
      const T = perCode.get(u.code).T;
      if (Number.isFinite(T)) capSelf = Math.min(capSelf, Math.max(0, Math.floor(T * rF - F + 1e-9)));
    }
    // ② 倉庫在庫・期限ごとの在庫の残り (先に配った SKU のぶんを引いたあと)
    let capShared = Infinity;
    for (const u of units) capShared = Math.min(capShared, Math.floor(remW.get(u.code) / u.qty));
    for (const e of pools) {
      const u = units.find((x) => x.code === e.code);
      capShared = Math.min(capShared, Math.floor(remExp.get(`${e.code}|${e.expiry}`) / (u?.qty || 1)));
    }
    capShared = Math.max(0, capShared);

    let afterSelf = Math.min(before, capSelf);
    // 早めに送る分は、自社出荷の日販が分からない構成品を使うなら出さない (自社ぶんを守れると言えない)
    let pullCut = 0;
    if (it.pull_forward && units.some((u) => perCode.get(u.code).rS === null)) { pullCut = afterSelf; afterSelf = 0; }
    let after = Math.min(afterSelf, capShared);
    // ③ 削った結果が少なすぎたら出さない (既存の最低出荷日数と同じ基準。期限商品は除く)
    let minDaysCut = 0;
    const daily = Number(it.daily_sales) || 0;
    if (after > 0 && after < before && daily > 0 && !it.is_expiry_managed) {
      const threshold = (Number(it.effective_fba_stock) || 0) === 0 ? Math.min(minShipmentDays, 1) : minShipmentDays;
      if (after / daily < threshold) { minDaysCut = after; after = 0; }
    }

    for (const u of units) remW.set(u.code, remW.get(u.code) - after * u.qty);
    for (const e of pools) {
      const u = units.find((x) => x.code === e.code);
      const k = `${e.code}|${e.expiry}`;
      remExp.set(k, remExp.get(k) - after * (u?.qty || 1));
    }

    const selfCut = before - afterSelf - pullCut;
    const sharedCut = afterSelf - Math.min(afterSelf, capShared);
    it.allocation = {
      before,
      after,
      self_cut: selfCut,           // 自社出荷ぶんを残すために減らした (SKU の個数)
      shared_cut: sharedCut,       // 同じ NE 商品を使う他の SKU に先に配ったため減らした
      min_days_cut: minDaysCut,    // 減らした結果が最低出荷日数に満たず 0 にした
      pull_self_unknown_cut: pullCut,   // 早めに送る分を、自社日販が分からないので出さなかった
      cap_self: Number.isFinite(capSelf) ? capSelf : null,
      units: units.map((u) => {
        const c = perCode.get(u.code);
        return {
          code: u.code, qty: u.qty, warehouse: c.raw, pending_fba_slips: c.pending, free: c.W,
          self_daily: c.rS === null ? null : round2(c.rS),
          equal_days: Number.isFinite(c.T) ? Math.round(c.T * 10) / 10 : null,
          left_after: remW.get(u.code),          // この SKU まで配ったあとに倉庫に残る数 (構成品の個数)
        };
      }),
    };
    it.fba_allocatable = Math.min(Number.isFinite(capSelf) ? capSelf : Infinity, capShared);
    if (!Number.isFinite(it.fba_allocatable)) it.fba_allocatable = null;
    it.non_fba_reserve = selfCut;

    if (after < before) {
      it.qty_before_allocation = before;
      it.adjusted_qty = after;
      it.recommended_qty = Math.min(Number(it.recommended_qty) || 0, after);
      it.alerts = it.alerts || [];
      const days = it.allocation.units.map((u) => u.equal_days).filter((d) => d !== null);
      if (selfCut > 0) {
        totals.skus_self++; totals.units_self += selfCut;
        it.alerts.push({ type: 'self_reserve', level: 1,
          message: `自社出荷ぶんを残すため ${before}→${afterSelf} (FBA と自社が約${Math.min(...days)}日分でそろう)` });
      }
      if (sharedCut > 0) {
        totals.skus_shared++; totals.units_shared += sharedCut;
        it.alerts.push({ type: 'shared_stock', level: 1,
          message: `同じ商品を使う他の SKU に先に配ったため ${afterSelf}→${afterSelf - sharedCut}` });
      }
      if (minDaysCut > 0) {
        totals.skus_min_days++; totals.units_min_days += minDaysCut;
        it.skipped_min_days = true;
        it.alerts.push({ type: 'min_days_after_allocation', level: 1,
          message: `減らした結果 ${minDaysCut} 個 (${(minDaysCut / daily).toFixed(1)}日分) は最低出荷日数に満たないため 0` });
      }
    }
  }
  return totals;
}

/**
 * 「Σ q × max(0, T×rF − F) + T × rS ≤ W」を満たす最大の T。
 * 自社日販が分からない / 0 なら上限なし (Infinity)。左辺は T について単調に増えるので二分探索で求まる
 */
export function equalDays(c) {
  if (c.rS === null || !(c.rS > 0)) return Infinity;
  const need = (T) => c.members.reduce((s, m) => s + m.q * Math.max(0, T * m.rF - m.F), 0) + T * c.rS;
  let lo = 0, hi = c.W / c.rS;                  // T × rS ≤ W なので T は W / rS を超えない
  if (need(hi) <= c.W) return hi;
  for (let i = 0; i < 60; i++) {
    const mid = (lo + hi) / 2;
    if (need(mid) <= c.W) lo = mid; else hi = mid;
  }
  return lo;
}

function round2(v) { return Math.round(v * 100) / 100; }

/**
 * 出力した NE 受注 CSV (FBA 伝票) のうち、倉庫 CSV を取り込んだ時点でまだ Amazon に出ていなかったもの。
 * 🚨 NE で起票した FBA 伝票は数日「起票済」のまま = ロジザードに渡っていない → ロジザード CSV の在庫に残っている。
 *    これを引かないと、同じ在庫を翌日また FBA に配ってしまう (9/24 時点で 9/22・9/23 の伝票 8,569 個が起票済のまま。
 *    Codex 2026-09-24 設計レビュー High 2)。
 * 「出た」の決め方 (Codex PR レビュー R1〜R4 で固めた):
 *   - 数える納品 = 状態が出荷済み以降 (LEFT_WAREHOUSE_STATUSES。WORKING = 作っただけ・CANCELLED/DELETED は数えない) で、
 *     出荷済みを初めて確認した時刻 (fba_inbound_shipments.left_seen_at) が倉庫 CSV の取り込みより前のもの。
 *     作成時刻に固定の時間を足す推定はしない = 出荷が遅れても誤らない (R3 High 2 / R4 High 2)
 *   - 納品 1 つは伝票 1 つにだけ結び付ける。候補は伝票の 12 時間前以降にできた納品 (画面の手順どおり「プラン確定 →
 *     NE CSV 出力」だと納品のほうが少し先)。SKU の重なりがいちばん大きい伝票に結び付け、同じ重なりの伝票が 2 つ以上
 *     あれば決めきれない → どれにも結び付けない (R1 High 1 / R2 High 1 / R3 High 1)。
 *     実データ (8/18〜9/17) で、伝票の 12 時間前までにできた別の伝票の納品と SKU が重なるのは最大 5%
 *   - 出荷待ちから外すのは、結び付いた納品で出荷を確認できた数だけ (Amazon SKU の出荷数 × 構成数 を構成品ごとに、
 *     伝票の数を上限に)。一部の SKU だけ出た伝票の残りは出荷待ちのまま (R4 High 1)
 * 同じ伝票 (店舗伝票番号) を 2 回数えない。別の伝票なら中身が同じでも足す (R1 High 2)
 * 倉庫 CSV の取り込みより後に出した伝票は、必ず「まだ」(CSV を取った時点では出ていない)。
 * 迷ったら「まだ」に倒す = 倉庫在庫から引く = FBA に回す数が減る (自社側に倒れる)。
 *
 * @param {object} p
 * @param {{id, filename, created_at, createdMs, file_data, sku_list, sku_detail}[]} p.exports  sku_detail = 出力した時点の SKU ごとの数と構成 (無ければ移行期間として componentsOf で換算)
 * @param {{atMs: number, leftMs: number, qty: Map<string, number>}[]} p.shipments
 *   出荷済み以降の Amazon の納品 (作成時刻・出荷済みを初めて確認した時刻・Amazon SKU → 出荷数)
 * @param {(sku: string) => ([string, number][]|null)} p.componentsOf  Amazon SKU → [構成品コード, 構成数]。null = 分からない
 * @param {number|null} p.warehouseUploadedMs  倉庫 CSV の取り込み時刻 (null = 倉庫在庫が無い)
 * @param {number|null} p.inboundLastSyncMs    Amazon の納品実績を最後に取り込んだ時刻
 * @returns {{ status: 'ok'|'no_warehouse'|'inbound_stale', slips: object[], byCode: Map<string, number> }}
 */
export const SHIPMENT_BEFORE_SLIP_MS = 12 * 3600e3;
export const LEFT_WAREHOUSE_STATUSES = ['SHIPPED', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN', 'RECEIVING', 'CLOSED'];
export function findPendingSlips({ exports, shipments, componentsOf, warehouseUploadedMs, inboundLastSyncMs, nowMs, lookbackDays }) {
  const norm = (v) => String(v ?? '').trim().toLowerCase();
  const out = { status: 'ok', slips: [], byCode: new Map() };
  if (warehouseUploadedMs == null || !Number.isFinite(warehouseUploadedMs)) { out.status = 'no_warehouse'; return out; }
  // 納品実績が 2 日以上取り込まれていない = 出たのに「まだ」に見える (自社側に倒れる)。画面で知らせる
  if (!(inboundLastSyncMs > nowMs - 2 * 86400000)) out.status = 'inbound_stale';

  // ① 見る期間の出力を伝票ごとにまとめる
  const sinceMs = nowMs - lookbackDays * 86400000;
  const slips = new Map();
  for (const ex of exports) {
    if (!(ex.createdMs >= sinceMs) || ex.createdMs > nowMs) continue;
    const byCode = new Map();
    let orderNo = '';
    try {
      const { text } = decodeCsvBuffer(Buffer.from(ex.file_data));
      const rows = parseCsv(text);
      const h = rows[0] || [];
      const iCode = h.indexOf('商品コード'), iQty = h.indexOf('受注数量'), iNo = h.indexOf('店舗伝票番号');
      if (iCode < 0 || iQty < 0) continue;
      for (const r of rows.slice(1)) {
        const code = norm(r[iCode]); const q = Number(r[iQty]);
        if (code && Number.isFinite(q) && q > 0) byCode.set(code, (byCode.get(code) || 0) + q);
        if (!orderNo && iNo >= 0) orderNo = String(r[iNo] || '').trim();
      }
    } catch { continue; }
    if (byCode.size === 0) continue;
    let skus = [];
    try { skus = JSON.parse(ex.sku_list || '[]').map(norm); } catch { skus = []; }
    // 出力した時点の SKU ごとの数と構成 (無い = この仕組みより前の出力)
    let detail = null;
    try {
      const arr = ex.sku_detail ? JSON.parse(ex.sku_detail) : null;
      if (Array.isArray(arr)) {
        detail = new Map();
        for (const d of arr) {
          const k = norm(d?.sku); const q = Number(d?.qty);
          const comps = Array.isArray(d?.comps) ? d.comps.map(([c, n]) => [norm(c), Number(n) || 1]).filter(([c]) => c) : [];
          if (!k || !(q > 0) || comps.length === 0) continue;
          const cur = detail.get(k);
          detail.set(k, mergeDetail(cur, { qty: q, comps }));
        }
      }
    } catch { detail = null; }

    // 同じ伝票 (店舗伝票番号) は 1 つにまとめる。番号が読めなければ出力履歴の行ごとに別の伝票とみなす。
    // 🚨 番号は分単位 (router の export-ne-csv) なので、同じ分に数量を変えて出し直すと同じ番号で中身が違う。
    //    どちらを NE に取り込んだか分からないので、商品ごとに多い方を採る (多めに引く = 自社側に倒す。R2 High 2)
    const key = orderNo || `export#${ex.id}`;
    const cur = slips.get(key);
    if (!cur) {
      slips.set(key, { key, id: ex.id, order_no: orderNo || null, filename: ex.filename, created_at: ex.created_at,
        createdMs: ex.createdMs, byCode, skus: new Set(skus), detail, released: new Map(), releasedSku: new Map() });
    } else {
      for (const [c, q] of byCode) cur.byCode.set(c, Math.max(cur.byCode.get(c) || 0, q));
      for (const k of skus) cur.skus.add(k);
      if (detail) {
        cur.detail = cur.detail || new Map();
        for (const [k, d] of detail) {
          const c = cur.detail.get(k);
          cur.detail.set(k, mergeDetail(c, d));
        }
      }
      if (ex.createdMs > cur.createdMs) { cur.createdMs = ex.createdMs; cur.created_at = ex.created_at; cur.id = ex.id; cur.filename = ex.filename; }
    }
  }

  // ② 倉庫 CSV を取り込む前に出荷済みを確認できた納品を、伝票 1 つにだけ結び付け、出た数だけ出荷待ちから外す
  const list = [...slips.values()];
  for (const s of shipments) {
    if (!(s.leftMs <= warehouseUploadedMs)) continue;
    let best = null, tied = false;
    for (const sl of list) {
      if (sl.createdMs > warehouseUploadedMs || sl.skus.size === 0) continue;
      if (s.atMs < sl.createdMs - SHIPMENT_BEFORE_SLIP_MS) continue;
      let hit = 0;
      for (const k of sl.skus) if (s.qty.has(k)) hit++;
      if (hit === 0) continue;
      const ratio = hit / sl.skus.size;
      if (!best || ratio > best.ratio) { best = { sl, ratio }; tied = false; }
      else if (ratio === best.ratio) tied = true;
    }
    if (!best || tied) continue;
    const sl = best.sl;
    for (const [sku, n] of s.qty) {
      if (!sl.skus.has(sku) || !(n > 0)) continue;
      let comps, count = n;
      if (sl.detail) {
        // 出力した時点の構成で換算し、伝票のその SKU の数を上限にする (R5 High 1)
        const d = sl.detail.get(sku);
        if (!d || d.conflict) continue;               // 出し直しで構成が食い違う SKU は外さない (R6 High 1)
        const used = sl.releasedSku.get(sku) || 0;
        count = Math.max(0, Math.min(n, d.qty - used));
        sl.releasedSku.set(sku, used + count);
        comps = d.comps;
      } else {
        // 移行期間だけ: 構成を残していない (この仕組みより前の) 出力は、いまの構成マスタで換算する。
        //   外さずに残すと、デプロイ後 最大 lookbackDays 日のあいだ直近の伝票が全部出荷待ちに見え、FBA の推奨が止まる
        comps = componentsOf(sku);
      }
      if (!comps || count <= 0) continue;            // 構成が分からない SKU の分は外さない (出荷待ちに残す)
      for (const [code, per] of comps) {
        if (!sl.byCode.has(code)) continue;
        sl.released.set(code, (sl.released.get(code) || 0) + count * per);
      }
    }
  }

  // ③ 伝票の数 − 出たと確認できた数 (0 未満にしない) が出荷待ち
  for (const sl of list) {
    let qty = 0, total = 0;
    const left = new Map();
    for (const [c, q] of sl.byCode) {
      total += q;
      const rest = Math.max(0, q - (sl.released.get(c) || 0));
      if (rest > 0) { left.set(c, rest); qty += rest; }
    }
    if (qty === 0) continue;
    out.slips.push({ id: sl.id, order_no: sl.order_no, filename: sl.filename, created_at: sl.created_at, qty,
      total, codes: left.size });
    for (const [c, q] of left) out.byCode.set(c, (out.byCode.get(c) || 0) + q);
  }
  return out;
}

/**
 * 同じ伝票の同じ SKU の「出力した時点の数と構成」を 1 つにまとめる。
 * 🚨 構成が食い違う (同じ分に構成を直して出し直した等) ときは、数と構成を混ぜると別の出力の構成で換算してしまう
 *    → conflict にして、その SKU は出荷待ちから外さない (Codex PR レビュー R6 High 1)。構成が同じなら数は多い方
 */
function mergeDetail(a, b) {
  if (!a) return b;
  if (!b) return a;
  if (a.conflict || b.conflict) return { ...a, conflict: true };
  const sig = (d) => d.comps.map(([c, n]) => `${c}:${n}`).sort().join(',');
  if (sig(a) !== sig(b)) return { qty: Math.max(a.qty, b.qty), comps: a.comps, conflict: true };
  return { qty: Math.max(a.qty, b.qty), comps: a.comps };
}

/** Amazon の納品を DB から取る下限の日付 (日本時間)。伝票の 12 時間前までさかのぼるぶんも含める (Codex R2 Medium 3) */
export function shipmentSinceJstDate(nowMs, lookbackDays) {
  return new Date(nowMs - lookbackDays * 86400000 - SHIPMENT_BEFORE_SLIP_MS + 9 * 3600e3).toISOString().slice(0, 10);
}
