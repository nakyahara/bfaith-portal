/**
 * 日本優先の在庫配分 (米国FBA在庫補充 PR2・表示だけ)。純粋関数 = DB も通信も持たない。入力は router.js が日本の表から読んで渡す。
 * 設計 = AI_reference システム設計/米国FBA納品アプリ_設計方針_20260924.md §9 (+ §9.5 = Codex PR2 設計レビューの反映。こちらが優先)
 *
 * 構成品 c (NE 商品コード = ロジザード商品ID・trim+小文字) ごと:
 *   日本 FBA の不足 = Σ_i q_ic × max(0, ceil(T_i × rF_i − F_i))
 *     i = 日本 RESTOCK の行のうち c を使う SKU。T_i = 日本の計算と同じ SKU ごとの目標日数 (calcTargetDays)
 *     rF_i = 日本 30日販売 / 30、F_i = 販売可能 + 輸送中 + 受領中 (🚨 準備中は足さない = Codex H2。足すと日本に残す数が減る)
 *     恒久除外 SKU は日本に送らないので不足 0
 *   自社出荷 = ceil(SELF_PROTECT_DAYS × 自社 30日販売 / 30)
 *   米国に回せる数 pool_c = max(0, 倉庫の出荷可能 − 日本の出荷待ち伝票 − 日本 FBA の不足 − 自社出荷)
 * 米国 SKU u (構成品 c を q_uc 個):
 *   rU = 米国 RESTOCK 30日販売 / 30 (PLANNING には落とさない = Codex M3)、FU = 販売可能 + 準備中 + 輸送中 + 受領中
 *   在庫日数 FU / rU が US_REORDER_DAYS 以上なら 0。未満なら 必要数 = ceil(US_TARGET_DAYS × rU − FU)
 *   在庫日数の少ない順に、構成品の pool を減らしながら配る: give = min(必要数, min_c floor(pool_c / q_uc))
 *
 * 分からないものは 0 にしない: 構成品ごと・SKU ごとに status = 'unknown' + 理由。
 * 入力が古い・欠けているときは gates に入れ、数字は「参考」(reference = true) として返す。
 */

export const US_TARGET_DAYS = 90;      // 中原さん 9/24「90 日分くらい」(輸送込み)
export const US_REORDER_DAYS = 45;     // たたき台 (輸送 2〜3 週 + 次の納品まで 1 か月強)
export const SELF_PROTECT_DAYS = 60;   // 自社出荷 (楽天・Yahoo 等) のために倉庫に残す日数。方針・⭐中原さんに確認
export const MAX_INPUT_HOURS = 36;     // 影の下書きの関所と同じ幅
export const PENDING_LOOKBACK_DAYS = 10;   // 日本の出荷待ち伝票を数える範囲 (getPendingFbaSlips の既定)

export const norm = (v) => String(v ?? '').trim().toLowerCase();

/**
 * 構成 (set_components の JSON 文字列 / 配列 / 単品の ne_code) → [{ code, qty }] (コードごとに合算)。
 * 数量は正の整数だけ。空・不正は null (1 個に補わない = Codex H3)
 */
export function parseComponents(setComponents, neCode, isSet = false) {
  let list = setComponents;
  if (typeof list === 'string') {
    if (list.trim() === '') list = null;
    else { try { list = JSON.parse(list); } catch { return null; } }
  }
  let raw;
  if (Array.isArray(list) && list.length > 0) raw = list.map((c) => ({ code: c && c.ne_code, qty: c && c.qty }));
  // 🚨 セット品なのに構成が空 = 何個入りか分からない。単品 1 個に補わない (日本の計算も不正として止める。Codex PR2 R1 Medium 2)
  else if (isSet) return null;
  else if (list == null || (Array.isArray(list) && list.length === 0)) raw = neCode ? [{ code: neCode, qty: 1 }] : [];
  else return null;
  if (raw.length === 0) return null;
  const merged = new Map();
  for (const r of raw) {
    const code = norm(r.code);
    const qty = Number(r.qty);
    if (!code || !Number.isInteger(qty) || qty <= 0) return null;
    merged.set(code, (merged.get(code) || 0) + qty);
  }
  return [...merged].map(([code, qty]) => ({ code, qty }));
}

/**
 * 構成が読めない・食い違うときに「どの構成品に効きうるか」の候補 (代表 ne_code + 読める範囲の構成の ne_code)。
 * 候補に米国の構成品が入っていれば、その構成品を判定不能にする (予約 0 として通さない。Codex PR2 R1 High 1)
 */
export function candidateCodes(m) {
  const out = new Set();
  if (m && m.ne_code) out.add(norm(m.ne_code));
  let list = m && m.set_components;
  if (typeof list === 'string') { try { list = JSON.parse(list); } catch { list = null; } }
  if (Array.isArray(list)) for (const c of list) if (c && c.ne_code) out.add(norm(c.ne_code));
  out.delete('');
  return out;
}

const hoursSince = (ms, now) => (now.getTime() - ms) / 3600e3;
const intOrNull = (v) => (v === null || v === undefined || v === '' || !Number.isFinite(Number(v)) ? null : Number(v));

/**
 * @param {object} a
 * @param {object[]} a.usRows        PR1 の buildUsInventoryView の rows (sku, sold_30d_restock, on_hand, mapping)
 * @param {string|null} a.usRestockFetchedAt  米国 RESTOCK を取った時刻 (ISO)
 * @param {object[]} a.jpRestock     日本 restock_latest の全行
 * @param {(row: object) => number|null} a.jpTargetDaysOf  日本 SKU の目標日数 (日本の calcTargetDays と同じ設定)。分からなければ null
 * @param {object[]} a.jpMappings    getSkuMappings() の全行 (amazon_sku, ne_code, set_components)
 * @param {Set<string>} a.jpExcluded 恒久除外 SKU (norm 済み)
 * @param {object[]} a.warehouse     getWarehouseSummary() の全行 (logizard_code, warehouse_available, earliest_expiry)
 * @param {object} a.selfShip        getSelfShipSalesByCode() の戻り値
 * @param {object} a.pending         getPendingFbaSlips() の戻り値 (status, byCode)
 * @param {object} a.freshness       { jpRestockSourceAt: 'YYYY-MM-DD HH:MM:SS' (UTC), jpRestockSourceMissing, warehouseUploadedAt: localtime }
 * @param {Date} [a.now]
 */
export function computeUsAllocation(a) {
  const now = a.now || new Date();
  const gates = [];      // 参考扱いにする理由 (入力が古い・欠け)
  const notes = [];      // 知っておいてほしいこと (参考にはしない)
  const gate = (code, text) => gates.push({ code, text });

  // ── 関所 (§9.1 + §9.5 M3/M4) ──
  const usMs = a.usRestockFetchedAt ? Date.parse(a.usRestockFetchedAt) : NaN;
  if (!Number.isFinite(usMs) || hoursSince(usMs, now) > MAX_INPUT_HOURS) gate('us_restock_stale', `米国の RESTOCK = ${a.usRestockFetchedAt || 'なし'}`);
  // 米国の最新の取得で RESTOCK が失敗 (表は前の回の分) / 保存に失敗 → 36h 以内でも参考 (Codex PR2 R1 Medium 1)
  const ula = a.usLastAttempt || null;
  const ulaMs = ula ? Date.parse(ula.attempted_at) : NaN;
  if (ula && !ula.restock_ok && (!Number.isFinite(usMs) || !Number.isFinite(ulaMs) || ulaMs > usMs)) {
    gate('us_restock_last_failed', `米国の最新の取得 (${ula.business_date || '?'}) で RESTOCK が失敗: ${ula.error || '不明'}`);
  }
  if ((ula && ula.save_error) || a.usSaveFailure) {
    gate('us_save_failed', `米国のレポートの保存に失敗: ${(ula && ula.save_error) || (a.usSaveFailure && a.usSaveFailure.error) || '不明'}`);
  }
  const f = a.freshness || {};
  const jpMs = f.jpRestockSourceAt ? Date.parse(String(f.jpRestockSourceAt).replace(' ', 'T') + 'Z') : NaN;
  if (!Number.isFinite(jpMs) || Number(f.jpRestockSourceMissing) > 0 || hoursSince(jpMs, now) > MAX_INPUT_HOURS) {
    gate('jp_restock_stale', `日本の RESTOCK を取った時刻 = ${f.jpRestockSourceAt || '不明'} (UTC)${Number(f.jpRestockSourceMissing) > 0 ? ` / 時刻の無い行 ${f.jpRestockSourceMissing}` : ''}。日本の FBA在庫補充で「レポート全取得」を押すと新しくなります`);
  }
  const whMs = f.warehouseUploadedAt ? new Date(String(f.warehouseUploadedAt).replace(' ', 'T')).getTime() : NaN;
  if (!Number.isFinite(whMs) || hoursSince(whMs, now) > MAX_INPUT_HOURS) gate('warehouse_stale', `倉庫在庫 (ロジザード CSV) の取り込み = ${f.warehouseUploadedAt || 'なし'}。日本の FBA在庫補充の Step2 で CSV を上げると新しくなります`);
  const ss = a.selfShip || {};
  const ssDateOk = typeof ss.as_of === 'string' && /^\d{4}-\d{2}-\d{2}/.test(ss.as_of) && Number.isFinite(Date.parse(ss.as_of.slice(0, 10)));
  const selfMap = ss.map instanceof Map ? ss.map : null;
  if (ss.status !== 'ok' || !ssDateOk) gate('self_sales_not_ok', `自社出荷の販売 (商品管理リスト) = ${ss.status || '不明'}${ss.as_of ? ` (${ss.as_of})` : ''}${ss.error ? `: ${ss.error}` : ''}`);
  const pd = a.pending || {};
  const pendingMap = pd.byCode instanceof Map ? pd.byCode : null;
  // 画面に内部の状態名 (inbound_stale 等) をそのまま出さない (中原さん 9/25「なんだこれ」)
  const pendingWhy = {
    inbound_stale: `日本の納品実績 (Amazon の shipment) が 2 日以上古いので、どの伝票がもう倉庫を出たか分かりません (最終取り込み ${pd.inbound_last_synced_at || '不明'})。出荷済みの伝票も「出荷待ち」として引いています`,
    no_warehouse: '倉庫 CSV が無いので、日本の出荷待ち伝票を数えられません',
    error: '日本の出荷待ち伝票を読めませんでした',
  };
  if (pd.status !== 'ok') gate('pending_slips_not_ok', `${pendingWhy[pd.status] || `日本の出荷待ち伝票 = ${pd.status || '不明'}`}${pd.error ? ` (${pd.error})` : ''}`);
  notes.push(`日本の出荷待ち伝票は直近 ${PENDING_LOOKBACK_DAYS} 日に出力したものだけ数えています (それより前の未出荷伝票は入りません)`);
  notes.push('日本 FBA の準備中 (作成済みの納品プラン) は日本の在庫に足していません。準備中の分の伝票が出ていれば、その分は多めに日本に残ります (日本優先の向き)');

  // ── 米国 SKU の構成 ──
  const usItems = (a.usRows || []).map((r) => {
    const m = r.mapping || {};
    const comps = ['master', 'product_code'].includes(m.route)
      ? parseComponents((m.components || []).map((c) => ({ ne_code: c.ne_code, qty: c.qty })), null)
      : null;
    return { sku: r.sku, key: norm(r.sku), row: r, comps, route: m.route || 'unknown' };
  });
  const usCodes = new Set();
  for (const u of usItems) for (const c of u.comps || []) usCodes.add(c.code);

  // ── 構成品ごとの箱 ──
  const codes = new Map();
  for (const c of usCodes) codes.set(c, { code: c, warehouse: 0, earliest_expiry: null, jp_pending: 0, jp_fba_short: 0, jp_self: 0, pool: null, pool_after: null, unknown: [], jp_skus: [], us_skus: [] });
  const unk = (c, why) => { const b = codes.get(c); if (b && !b.unknown.includes(why)) b.unknown.push(why); };

  // 倉庫在庫。CSV に行が無い = そのコードの在庫が無い (ロジザード CSV は在庫のあるロケだけ出す)
  const whSeen = new Map();
  for (const w of a.warehouse || []) {
    const k = norm(w.logizard_code);
    if (!codes.has(k)) continue;
    if (whSeen.has(k)) { unk(k, '倉庫 CSV に同じ商品コードが大文字小文字違いで 2 行'); continue; }
    whSeen.set(k, true);
    const avail = intOrNull(w.warehouse_available);
    if (avail === null || avail < 0) unk(k, `倉庫の出荷可能数が数字でない (${w.warehouse_available})`);
    else codes.get(k).warehouse = avail;
    if (w.earliest_expiry) { codes.get(k).earliest_expiry = w.earliest_expiry; unk(k, `期限管理品 (最も古い期限 ${w.earliest_expiry})。期限ごとの配分は PR3 以降`); }
  }

  // 日本の出荷待ち伝票・自社出荷
  for (const [c, b] of codes) {
    if (pendingMap) b.jp_pending = Math.max(0, Number(pendingMap.get(c)) || 0);
    else unk(c, '日本の出荷待ち伝票を数えられない');
    if (!selfMap) { unk(c, '自社出荷の販売を読めない'); continue; }
    if (!selfMap.has(c)) { unk(c, '自社出荷の販売に行が無い (商品管理リスト)'); continue; }
    b.jp_self = Math.ceil((SELF_PROTECT_DAYS * Number(selfMap.get(c))) / 30);
  }

  // 日本 SKU → 構成 (H1: 正規化後に同じ SKU で構成が食い違えば判定不能)
  //   norm sku → { comps: [{code, qty}] | null, state: 'ok' | 'conflict' | 'invalid', candidates: Set<code> }
  //   conflict / invalid のときは candidates (効きうる構成品) を持つ = 米国の構成品が入っていれば判定不能にする
  const sigOf = (comps) => (comps ? JSON.stringify([...comps].sort((x, y) => x.code.localeCompare(y.code))) : 'invalid');
  const jpComps = new Map();
  for (const m of a.jpMappings || []) {
    const k = norm(m.amazon_sku);
    if (!k) continue;
    const comps = parseComponents(m.set_components, m.ne_code, !!Number(m.is_set));
    const cand = candidateCodes(m);
    if (jpComps.has(k)) {
      const prev = jpComps.get(k);
      for (const c of cand) prev.candidates.add(c);
      if (comps) for (const c of comps) prev.candidates.add(c.code);
      if (prev.state === 'ok' && sigOf(prev.comps) === sigOf(comps)) continue;   // 同じ構成の重複は害が無い
      prev.state = 'conflict'; prev.comps = null;
      continue;
    }
    if (comps) for (const c of comps) cand.add(c.code);
    jpComps.set(k, { comps, state: comps ? 'ok' : 'invalid', candidates: cand });
  }

  // 日本 FBA の不足 (日本 RESTOCK の全行が起点 = H1)
  const unattributed = [];
  const restockSeen = new Map();
  for (const r of a.jpRestock || []) {
    const k = norm(r.amazon_sku);
    if (!k) continue;
    restockSeen.set(k, (restockSeen.get(k) || 0) + 1);
  }
  for (const r of a.jpRestock || []) {
    const k = norm(r.amazon_sku);
    if (!k) continue;
    const sold = intOrNull(r.units_sold_30d);
    const avail = intOrNull(r.fba_available), shipped = intOrNull(r.fba_inbound_shipped), received = intOrNull(r.fba_inbound_received);
    // 「動きが無い」は 販売も在庫も **0 と取れている** ときだけ。取れていない (null) を動きなしにしない (Codex PR2 R2 High)
    const idle = sold === 0 && avail === 0 && shipped === 0 && received === 0;
    const excludedSku = a.jpExcluded instanceof Set && a.jpExcluded.has(k);
    const jm = jpComps.get(k);
    if (!jm || jm.state !== 'ok') {
      // 恒久除外 SKU は日本に送らない = 日本 FBA の不足に入らないので、構成が分からなくても配分を止めない (Codex PR2 R2 Medium)
      if (excludedSku || idle) continue;
      const why = !jm ? '構成が無い' : jm.state === 'conflict' ? '構成が食い違う' : '構成が不正';
      // 効きうる構成品が米国の構成品なら、その構成品は判定不能 (日本の需要を 0 として米国に回さない)
      const hitCodes = jm ? [...jm.candidates].filter((c) => codes.has(c)) : [];
      for (const c of hitCodes) unk(c, `日本 SKU ${r.amazon_sku} の${why} (この構成品を使っている可能性)`);
      unattributed.push({ sku: r.amazon_sku, sold_30d: sold, available: avail, why, blocked: hitCodes.length > 0 });
      continue;
    }
    const comps = jm.comps;
    const hits = comps.filter((c) => codes.has(c.code));
    if (hits.length === 0) continue;
    if (restockSeen.get(k) > 1) { for (const c of hits) unk(c.code, `日本 RESTOCK に ${r.amazon_sku} が大文字小文字違いで 2 行`); continue; }
    const excluded = a.jpExcluded instanceof Set && a.jpExcluded.has(k);
    const entry = { sku: r.amazon_sku, excluded, sold_30d: sold, supply: null, target_days: null, short: null };
    if (!excluded) {
      if (sold === null) { for (const c of hits) unk(c.code, `日本 SKU ${r.amazon_sku} の 30日販売が取れていない`); }
      else if (avail === null || shipped === null || received === null) { for (const c of hits) unk(c.code, `日本 SKU ${r.amazon_sku} の FBA 在庫が取れていない`); }
      else {
        const t = a.jpTargetDaysOf ? a.jpTargetDaysOf(r) : null;
        if (!Number.isFinite(t) || t <= 0) { for (const c of hits) unk(c.code, `日本 SKU ${r.amazon_sku} の目標日数が分からない`); }
        else {
          entry.supply = avail + shipped + received;
          entry.target_days = t;
          entry.short = Math.max(0, Math.ceil((t * sold - 30 * entry.supply) / 30));   // 整数のまま (浮動小数の切り上げずれを避ける)
        }
      }
    } else entry.short = 0;
    for (const c of hits) {
      const b = codes.get(c.code);
      b.jp_skus.push({ ...entry, qty_per: c.qty });
      if (entry.short != null) b.jp_fba_short += entry.short * c.qty;
    }
  }
  // 構成が分からない日本 SKU: 米国の構成品に効きうるもの (その構成品を判定不能にした) と、影響先が分からないもの (計算に入っていない) を分けて出す (Codex PR2 R2 Low)
  const blockedN = unattributed.filter((u) => u.blocked).length;
  const looseN = unattributed.length - blockedN;
  if (blockedN) notes.push(`構成が分からない日本の SKU のうち ${blockedN} 件は米国と同じ構成品を使っている可能性があるので、その構成品を「判定できない」にしました`);
  if (looseN) notes.push(`日本の SKU ${looseN} 件は構成が分からず、どの構成品を使うかも分からないため、日本に残す数に入っていません (販売か在庫あり・または取れていない)。この中に米国と同じ商品を使う SKU があれば、米国に回せる数は多く出ています`);

  for (const [, b] of codes) {
    if (b.unknown.length) continue;
    b.pool = Math.max(0, b.warehouse - b.jp_pending - b.jp_fba_short - b.jp_self);
  }

  // ── 米国の推奨 ──
  const dupRestock = new Set(((a.usDupKeys && a.usDupKeys.restock) || []).map(norm));
  const us = usItems.map((u) => {
    const r = u.row;
    const out = { sku: u.sku, status: 'unknown', reason: null, daily: null, on_hand: r.on_hand ?? null, cover_days: null, need: null, give: null, order: null, consumption: [], limited_by: null };
    // 米国 RESTOCK に同じ SKU が 2 行 = どちらの在庫が正しいか分からない (画面の表は 1 行目を出している。Codex PR2 R1 High 2)
    if (dupRestock.has(u.key)) { out.reason = '米国 RESTOCK に同じ SKU が 2 行ある (どちらの在庫が正しいか分からない)'; return out; }
    if (!u.comps) { out.reason = u.route === 'none' ? '自社の商品コードに結びつかない' : u.route === 'unknown' ? '商品コードへの結びつきを調べられない' : '構成が不正'; return out; }
    for (const c of u.comps) codes.get(c.code).us_skus.push(u.sku);
    const sold = r.sold_30d_restock;
    if (sold == null) { out.reason = '米国 RESTOCK の 30日販売が無い'; return out; }
    if (r.on_hand == null) { out.reason = '米国 FBA の在庫 (販売可能・準備中・輸送中・受領中) のどれかが取れていない'; return out; }
    const bad = u.comps.filter((c) => codes.get(c.code).unknown.length);
    if (bad.length) { out.reason = bad.map((c) => `${c.code}: ${codes.get(c.code).unknown.join(' / ')}`).join(' ・ '); return out; }
    out.daily = sold / 30;
    if (sold === 0) { out.status = 'zero'; out.reason = '米国で売れていない (30日販売 0)'; out.need = 0; out.give = 0; return out; }
    // 🚨 整数のまま計算する (90 × (33 / 30) = 99.00000000000001 → 切り上げで 100 になる)
    out.cover_days = (r.on_hand * 30) / sold;
    if (r.on_hand * 30 >= US_REORDER_DAYS * sold) { out.status = 'zero'; out.reason = `まだ足りている (在庫 ${Math.floor(out.cover_days)} 日分 ≥ ${US_REORDER_DAYS} 日)`; out.need = 0; out.give = 0; return out; }
    out.need = Math.max(0, Math.ceil((US_TARGET_DAYS * sold - 30 * r.on_hand) / 30));
    out.status = 'candidate';
    return out;
  });

  // 在庫日数の少ない順 (丸めない値。同点は SKU 順) に、構成品の pool を減らしながら配る (L1)
  const remain = new Map([...codes].map(([c, b]) => [c, b.pool]));
  const candidates = us.filter((x) => x.status === 'candidate').sort((x, y) => x.cover_days - y.cover_days || x.sku.localeCompare(y.sku));
  candidates.forEach((x, i) => {
    const u = usItems.find((v) => v.sku === x.sku);
    let cap = Infinity, by = null;
    for (const c of u.comps) {
      const can = Math.floor(remain.get(c.code) / c.qty);
      if (can < cap) { cap = can; by = c.code; }
    }
    x.give = Math.max(0, Math.min(x.need, cap));
    x.order = i + 1;
    const byComp = u.comps.find((c) => c.code === by);
    // 足りなくなった構成品について: 1 SKU あたりの構成数・配る前の残り・先に配った米国 SKU の分 (画面が理由を言い切らないため。Codex #1452 R1 Medium 4)
    if (by) x.limit = { code: by, per: byComp.qty, pool: codes.get(by).pool, remain_before: remain.get(by), taken_by_earlier: codes.get(by).pool - remain.get(by) };
    for (const c of u.comps) {
      const before = remain.get(c.code);
      remain.set(c.code, before - x.give * c.qty);
      x.consumption.push({ code: c.code, per: c.qty, qty: x.give * c.qty, remain_before: before, remain_after: remain.get(c.code) });
    }
    if (x.give < x.need) {
      x.limited_by = by;
      const lm = x.limit;
      x.reason = `日本に残す分${lm.taken_by_earlier > 0 ? `と、先に配った米国 SKU の分 (${lm.taken_by_earlier} 個)` : ''}を引くと、${by} の残りが ${lm.remain_before} 個 (1 SKU に ${lm.per} 個) (必要 ${x.need} → ${x.give})`;
    }
    else x.reason = `在庫 ${Math.floor(x.cover_days)} 日分 < ${US_REORDER_DAYS} 日 → ${US_TARGET_DAYS} 日分まで`;
    x.status = x.give > 0 ? 'reco' : 'short';
  });
  for (const [c, b] of codes) b.pool_after = b.pool == null ? null : remain.get(c);

  return {
    reference: gates.length > 0,
    gates,
    notes,
    params: { us_target_days: US_TARGET_DAYS, us_reorder_days: US_REORDER_DAYS, self_protect_days: SELF_PROTECT_DAYS, pending_lookback_days: PENDING_LOOKBACK_DAYS },
    codes: [...codes.values()].sort((x, y) => x.code.localeCompare(y.code)),
    us: us.sort((x, y) => (x.order ?? 1e9) - (y.order ?? 1e9) || x.sku.localeCompare(y.sku)),
    unattributed_jp: unattributed.slice(0, 200),
    unattributed_jp_count: unattributed.length,
  };
}
