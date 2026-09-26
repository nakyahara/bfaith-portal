/**
 * inbound-snapshot.js — 納品プラン・出荷便の「状態のスナップショット」(FBA 補充 B1。2026-09-26)
 *
 * 何のためか:
 *   準備中 (納品プラン) と Amazon のレポート (輸送中・受領中) を、**プラン・出荷便の ID ごと** に突き合わせる材料。
 *   毎朝のレポート取得の直前 (S0) と直後 (S1) に取り、B2 で 9:40 の自動決定が「基準から変わったプラン・便の SKU は
 *   その日は止める」に使う。**B1 では記録と照合だけ** (誰も使わない)。
 *
 * 9/26 に実データで確かめたこと (Codex B 設計レビュー 1・2 の前提):
 *   - レポートの inbound_shipped + inbound_received = v0 出荷便の Σ(送った数 − 受領した数) (280/281 SKU)
 *   - レポートの inbound_working = 配置確定 (便が READY_TO_SHIP) の品目。配置未確定のプランはレポートに出ない
 *   - 出荷するとプランは SHIPPED。getShipment は READY_TO_SHIP の便にも FBA15… (v0 の ID) を返す
 *   - listShipmentItems は使える (便ごとの品目の合計 = プランの品目)
 *   - ACTIVE 471 件 (名前つき 4 件・残りは 2023 年からの空プラン)。一覧だけなら 16 ページ 12 秒
 *
 * 🚨 取り方の決まり (Codex B 設計レビュー 2):
 *   - ACTIVE は年齢で切らずに全ページ。ページ上限に当たったら complete=false
 *   - 追跡中の未解決プラン (品目があって、まだ全便が出荷済みになっていない) は ACTIVE から消えても 1 件ずつ追う (出荷後は v0 の便 ID で追う)
 *   - 新しく作られたプランは SHIPPED/VOIDED も CREATION_TIME で探す (作って出荷まで終わったものを落とさない)
 *   - 状態は許可リスト。知らない状態が 1 つでもあれば complete=false (判定不能)
 *   - 空の ACTIVE プラン・終わったプラン (取り消し・全便が終わった) は、前回と lastUpdatedAt・状態が同じで確かめてから日が浅ければ
 *     取り直さない (プランごとにずらして 1〜7 日で確かめ直す)。取り消し済みのプランは中身を取らない。
 *     品目があるプラン・追跡中・新しいプランは毎回全部取る
 *   - 出荷済みの古い便 (受領中が数か月続く) は v0 の便 ID で追う。v2024 のプランに結べない v0 便は、作ったのが探す範囲より前なら
 *     「v0 だけで追う」(想定どおり)、範囲の中なら「結べない」(異常) として分けて出す
 */

export const PLAN_STATUS = new Set(['ACTIVE', 'SHIPPED', 'VOIDED']);
/** 便の状態 → 区分。ここに無い状態は「判定不能」 */
export const SHIPMENT_CLASS = {
  WORKING: 'unshipped', READY_TO_SHIP: 'unshipped',
  SHIPPED: 'open', IN_TRANSIT: 'open', DELIVERED: 'open', CHECKED_IN: 'open', RECEIVING: 'open',
  CLOSED: 'done',
  CANCELLED: 'void', DELETED: 'void', ABANDONED: 'void',
};
/** v0 で「まだ終わっていない」出荷便として一覧する状態 (出荷前も含めて ID を追う) */
export const V0_LIST_STATUSES = ['WORKING', 'READY_TO_SHIP', 'SHIPPED', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN', 'RECEIVING'];
/** レポートの輸送中・受領中と比べる v0 の状態 (出荷前は含めない。Codex B 設計レビュー 2 High 1) */
export const V0_OPEN_STATUSES = new Set(['SHIPPED', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN', 'RECEIVING']);

const V2024 = '/inbound/fba/2024-03-20';
/** 空プランを確かめ直すまでの間隔 = 1〜verifyEveryDays 日 (ID で決まる。毎日だいたい 1/verifyEveryDays ずつ) */
export function verifyAfterMs(id, verifyEveryDays) {
  let h = 0;
  for (const c of String(id)) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  return (1 + (h % verifyEveryDays)) * 86400e3 - 3600e3;   // 1 時間早め (毎朝ほぼ同じ時刻に回るので、日の境目で 1 日ずれないように)
}
const normSku = (s) => String(s ?? '').trim().toLowerCase();
/** v0 の出荷便名「FBA STA (2026/09/24 07:11)-TYO2」から作った時刻 (JST)。読めなければ null */
export function staTimeMs(name) {
  const m = String(name || '').match(/\((\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2})\)/);
  if (!m) return null;
  const ms = Date.parse(`${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:00+09:00`);
  return Number.isFinite(ms) ? ms : null;
}
const addTo = (m, k, n) => { m[k] = (m[k] || 0) + n; };

/**
 * スナップショットを 1 回取る。
 * @param {object} p
 * @param {(path: string, label: string) => Promise<object>} p.call   SP-API の GET (再試行つき)。応答の payload を返す
 * @param {object} [p.cache]   前回までの記録 { plans: { [id]: { lastUpdatedAt, status, empty, verifiedAt } }, tracked: string[] }
 * @param {number} [p.nowMs]
 * @param {number} [p.discoverDays=14]  SHIPPED/VOIDED を CREATION_TIME で探す日数
 * @param {number} [p.verifyEveryDays=7]
 * @param {number} [p.pageCap=80]
 * @param {string} [p.marketplaceId]
 * @param {(ms: number) => Promise<void>} [p.sleep]
 * @param {number} [p.paceMs=550]  呼び出しの間隔 (v2024 の一覧・品目は 2 回/秒。応答時間に関係なく守れる間隔)
 * @param {number} [p.deadlineMs=240000]  取得全体の締め切り。超えたら残りは取らずに complete=false で返す
 *   (毎朝の日次処理はこのあとにレポート取得が続き、全体で 14 分しか待たない。Codex PR #1463 R1 High 1)
 * @param {number} [p.callTimeoutMs=30000]  1 回の呼び出しを待つ上限 (SDK の中の 429 の再試行も含めて止める)
 * @param {number} [p.budgetMs=240000]  名前の無い古い ACTIVE プランの品目を確かめるのは、確かめ始めてからこの時間まで
 *   (超えた分は complete=false にして翌日以降に回す。品目のあるプラン・追跡中・新しいプランは時間に関係なく必ず取る = 先に取る)
 */
export async function takeInboundSnapshot({
  call, cache = null, nowMs = Date.now(), discoverDays = 14, verifyEveryDays = 7, pageCap = 80,
  marketplaceId = 'A1VC38T7YXB528', sleep = (ms) => new Promise((r) => setTimeout(r, ms)), paceMs = 550, budgetMs = 240000,
  deadlineMs = 240000, callTimeoutMs = 30000, clock = () => Date.now(),
}) {
  const startedAt = new Date(nowMs).toISOString();
  const t0 = clock();
  const errors = [];
  const unknownStates = [];
  let calls = 0;
  const deadlineAt = t0 + deadlineMs;
  let timedOut = false;
  const pastDeadline = () => { if (clock() >= deadlineAt) timedOut = true; return timedOut; };
  // 締め切りを過ぎたら呼ばない。1 回ごとに待つ上限もつける (応答が返らない・SDK が中で再試行し続けるときに止める)
  const get = async (path, label) => {
    if (pastDeadline()) throw new Error(`締め切り (${Math.round(deadlineMs / 1000)} 秒) を過ぎた: ${label}`);
    calls++;
    await sleep(paceMs);
    const lim = Math.max(1, Math.min(callTimeoutMs, deadlineAt - clock()));
    let timer;
    try {
      return await Promise.race([
        call(path, label),
        new Promise((_, rej) => { timer = setTimeout(() => rej(new Error(`応答が ${Math.round(lim / 1000)} 秒ない: ${label}`)), lim); }),
      ]);
    } finally { clearTimeout(timer); }
  };

  // ① v0: 終わっていない出荷便 (出荷前も含む) と明細 (送った数・受領した数)。出荷済みの古い便は v0 の ID で追う
  //    (プランに結ぶ必要があるのは出荷前の便 = ACTIVE のプランで全部見える、と最近出荷したプランだけ)
  const v0 = [];
  try {
    const list = [];
    let token = null, p = 0;
    do {
      const qs = token
        ? new URLSearchParams({ MarketplaceId: marketplaceId, QueryType: 'NEXT_TOKEN', NextToken: token })
        : new URLSearchParams({ MarketplaceId: marketplaceId, QueryType: 'SHIPMENT', ShipmentStatusList: V0_LIST_STATUSES.join(',') });
      const r = await get(`/fba/inbound/v0/shipments?${qs}`, `v0 list p${p + 1}`);
      list.push(...(r?.ShipmentData || []));
      token = r?.NextToken || null;
      p++;
    } while (token && p < pageCap);
    if (token) errors.push(`v0 の一覧がページ上限 ${pageCap} に当たった`);
    for (const s of list) {
      if (!V0_LIST_STATUSES.includes(s.ShipmentStatus)) unknownStates.push(`v0 ${s.ShipmentId}: ${s.ShipmentStatus}`);
      try {
        const items = {};
        const add = (rows) => {
          for (const it of rows) {
            const k = normSku(it.SellerSKU);
            const cur = items[k] || { shipped: 0, received: 0 };
            cur.shipped += Number(it.QuantityShipped) || 0;
            cur.received += Number(it.QuantityReceived) || 0;
            items[k] = cur;
          }
        };
        const r = await get(`/fba/inbound/v0/shipments/${s.ShipmentId}/items?${new URLSearchParams({ MarketplaceId: marketplaceId })}`, `v0 items ${s.ShipmentId}`);
        const first = r?.ItemData || [];
        add(first);
        // 🚨 続きは別の口 (shipmentItems?QueryType=NEXT_TOKEN) でしか取れない。同じ口に NextToken を渡すと 1 ページ目が返る
        //    (inbound-history.js fetchShipmentItems と同じ。Codex PR #1463 R1 High 2)
        const seen = new Set(first.map((i) => i.SellerSKU));
        let token = r?.NextToken || null, page = 1;
        while (token) {
          if (page >= pageCap) throw new Error(`明細がページ上限 ${pageCap} に当たった`);
          const qs = new URLSearchParams({ MarketplaceId: marketplaceId, QueryType: 'NEXT_TOKEN', NextToken: token });
          const nx = await get(`/fba/inbound/v0/shipmentItems?${qs}`, `v0 items ${s.ShipmentId} p${page + 1}`);
          const batch = (nx?.ItemData || []).filter((i) => !i.ShipmentId || i.ShipmentId === s.ShipmentId);
          const fresh = batch.filter((i) => !seen.has(i.SellerSKU));
          if (!fresh.length) break;   // 続きなし (同じページが返り続ける癖の安全弁も兼ねる)
          for (const i of fresh) seen.add(i.SellerSKU);
          add(fresh);
          token = nx?.NextToken || null;
          page++;
        }
        v0.push({ id: s.ShipmentId, name: s.ShipmentName || '', status: s.ShipmentStatus, items });
      } catch (e) { errors.push(`v0 明細 ${s.ShipmentId}: ${e.message}`); }
    }
  } catch (e) { errors.push(`v0 の一覧: ${e.message}`); }

  // ② 一覧: ACTIVE は全部 (年齢で切らない)、SHIPPED/VOIDED は discoverDays 日以内に作ったもの (作って出荷まで終わったものを落とさない)。
  //    🚨 終わっていない v0 便のいちばん古い日までさかのぼるのはやめた (9/26 の試走: 6/10 の受領中の便があり 3 か月分のプランを取りにいった)
  const discoverFromMs = nowMs - discoverDays * 86400e3;
  const listed = new Map();   // id → 一覧の行
  const listStatus = async (status, stopBeforeMs) => {
    let token = null, pages = 0, reachedOld = false;
    do {
      const q = new URLSearchParams({ status, pageSize: '30', sortBy: 'CREATION_TIME', sortOrder: 'DESC' });
      if (token) q.set('paginationToken', token);
      const r = await get(`${V2024}/inboundPlans?${q}`, `list ${status} p${pages + 1}`);
      pages++;
      for (const pl of r?.inboundPlans || []) {
        if (stopBeforeMs !== null && Date.parse(pl.createdAt) < stopBeforeMs) { reachedOld = true; continue; }
        listed.set(pl.inboundPlanId, pl);
      }
      token = r?.pagination?.nextToken || null;
      if (reachedOld) break;
    } while (token && pages < pageCap);
    if (token && !reachedOld) errors.push(`${status} の一覧がページ上限 ${pageCap} に当たった`);
    return pages;
  };
  const pages = {};
  try { pages.ACTIVE = await listStatus('ACTIVE', null); } catch (e) { errors.push(`ACTIVE の一覧: ${e.message}`); }
  for (const st of ['SHIPPED', 'VOIDED']) {
    try { pages[st] = await listStatus(st, discoverFromMs); } catch (e) { errors.push(`${st} の一覧: ${e.message}`); }
  }

  // ③ プランごと。対象 = 一覧 ∪ 追跡中の未解決プラン
  const tracked = new Set((cache?.tracked) || []);
  const kindOf = (id) => {
    const row = listed.get(id) || null;
    const prev = cache?.plans?.[id] || null;
    if (row && row.status === 'VOIDED' && !tracked.has(id)) return 'voided';   // 取り消し済み: 中身は要らない (状態が変わったことが分かれば足りる)
    const same = prev && row && prev.lastUpdatedAt === row.lastUpdatedAt && prev.status === row.status
      && nowMs - Date.parse(prev.verifiedAt) < verifyAfterMs(id, verifyEveryDays) && !tracked.has(id);
    if (same && ((prev.empty && row.status === 'ACTIVE') || prev.resolved)) return 'reuse';   // 空・終わったプランで前回と同じ
    if (tracked.has(id) || !row || row.status !== 'ACTIVE' || !!row.name || (prev && !prev.empty)
      || nowMs - Date.parse(row.createdAt) < 30 * 86400e3) return 'full';
    return 'empty_check';   // 名前の無い古い ACTIVE (ほぼ全部が品目 0)。品目だけ確かめる
  };
  const allIds = [...new Set([...listed.keys(), ...tracked])];
  const order = { full: 0, voided: 1, reuse: 2, empty_check: 3 };
  const ids = allIds.map((id) => [id, kindOf(id)]).sort((a, b) => order[a[1]] - order[b[1]]);
  const plans = [];
  let reused = 0, deferred = 0, emptyT0 = null;
  const deferredIds = [], failedIds = [];

  const fetchItems = async (path, label) => {
    const items = {};
    let tok = null, n = 0;
    do {
      const r = await get(`${path}${tok ? `?paginationToken=${encodeURIComponent(tok)}` : ''}`, label);
      for (const it of r?.items || []) addTo(items, normSku(it.msku), Number(it.quantity) || 0);
      tok = r?.pagination?.nextToken || null;
      if (++n > pageCap) throw new Error(`品目がページ上限 ${pageCap}`);
    } while (tok);
    return items;
  };
  const fetchFull = async (id, row, itemsAlready = null) => {
    const d = await get(`${V2024}/inboundPlans/${id}`, `plan ${id}`);
    const status = d?.status || row?.status || null;
    if (!PLAN_STATUS.has(status)) unknownStates.push(`plan ${id}: ${status}`);
    const base = { id, name: d?.name ?? row?.name ?? '', status, createdAt: d?.createdAt || row?.createdAt || null,
      lastUpdatedAt: d?.lastUpdatedAt || row?.lastUpdatedAt || null, reused: false, verifiedAt: startedAt, contentKnown: true };
    if (status === 'VOIDED') {
      const c = cache?.plans?.[id]?.content;   // 取り消し前に記録した品目を持ち越す (取り消しの差分に使う)
      return { ...base, items: c?.items || {}, shipments: c?.shipments || [], voided: true, contentKnown: !!c };
    }
    const items = itemsAlready || await fetchItems(`${V2024}/inboundPlans/${id}/items`, `items ${id}`);
    const shipments = [];
    for (const s of d?.shipments || []) {
      const g = await get(`${V2024}/inboundPlans/${id}/shipments/${s.shipmentId}`, `shipment ${s.shipmentId}`);
      const st = g?.status || s.status || null;
      if (!SHIPMENT_CLASS[st]) unknownStates.push(`shipment ${s.shipmentId}: ${st}`);
      const sItems = await fetchItems(`${V2024}/inboundPlans/${id}/shipments/${s.shipmentId}/items`, `shipment items ${s.shipmentId}`);
      shipments.push({ id: s.shipmentId, confirmationId: g?.shipmentConfirmationId || null, status: st, items: sItems });
    }
    return { ...base, items, shipments };
  };

  for (const [id, kind] of ids) {
    const row = listed.get(id) || null;
    const prev = cache?.plans?.[id] || null;
    // 取り直さないプランは、前回の中身 (空・終わったプランの品目と便) を持ち越す。中身が分からなければ contentKnown=false
    //   (片方だけ取り直した日も比べられるように / 結べた v0 便を「結べない」にしないように。Codex PR #1463 R1 Medium 3・4)
    const carried = prev?.content || (prev?.empty ? { items: {}, shipments: [] } : null);
    const lite = (extra) => ({ id, name: row?.name || '', status: row?.status || null, createdAt: row?.createdAt || null,
      lastUpdatedAt: row?.lastUpdatedAt || null, items: carried?.items || {}, shipments: carried?.shipments || [],
      contentKnown: !!carried, ...extra });
    try {
      if (kind === 'voided') { plans.push(lite({ reused: false, voided: true, verifiedAt: startedAt })); continue; }
      if (kind === 'reuse') { plans.push(lite({ reused: true, verifiedAt: prev.verifiedAt })); reused++; continue; }
      if (pastDeadline()) { deferred++; deferredIds.push(id); continue; }   // 締め切り = 取っていない (complete=false・差分では unknown)
      if (kind === 'empty_check') {
        emptyT0 ??= clock();
        if (clock() - emptyT0 > budgetMs) { deferred++; deferredIds.push(id); continue; }   // 時間切れ = 翌日以降に回す (complete=false)
        const items = await fetchItems(`${V2024}/inboundPlans/${id}/items`, `items ${id}`);
        if (!Object.values(items).some((q) => q > 0)) { plans.push({ ...lite({ reused: false, verifiedAt: startedAt }), items: {}, shipments: [], contentKnown: true }); continue; }
        plans.push(await fetchFull(id, row, items));   // 品目があった = ちゃんと取る
        continue;
      }
      plans.push(await fetchFull(id, row));
    } catch (e) {
      errors.push(`プラン ${id}: ${e.message}`);
      failedIds.push(id);
    }
  }

  if (timedOut) errors.push(`締め切り (${Math.round(deadlineMs / 1000)} 秒) で打ち切った`);
  if (deferred) errors.push(`時間の上限・締め切りで確かめなかったプラン ${deferred} 件 (翌日以降に回す)`);
  const ms = clock() - t0;
  const finishedAt = new Date(nowMs + ms).toISOString();
  return {
    startedAt, finishedAt, ms, calls, pages, reused, deferred, deferredIds, failedIds,
    discoverFrom: new Date(discoverFromMs).toISOString(),
    complete: errors.length === 0 && unknownStates.length === 0,
    errors, unknownStates, plans, v0,
  };
}

/**
 * SKU ごとの内訳。
 *   unconfirmed = ACTIVE で便がまだ無いプランの品目 (配置未確定。レポートに出ない)
 *   unshipped   = 便が WORKING/READY_TO_SHIP の品目 (レポートの inbound_working と比べる)
 *   v0Open      = v0 の出荷済み〜受領中の便の Σ(送った数 − 受領した数) (レポートの shipped + received と比べる)
 * mixedPlans = ACTIVE なのに出荷済みの便がある / SHIPPED なのに出荷前の便がある プラン (一部だけ出荷)
 */
export function summarizeSnapshot(snap) {
  const bySku = {};
  const at = (k) => (bySku[k] ??= { unconfirmed: 0, unshipped: 0, v0Open: 0 });
  const mixedPlans = [];
  for (const p of snap.plans || []) {
    if (p.status === 'ACTIVE' && p.shipments.length === 0) {
      for (const [k, q] of Object.entries(p.items)) at(k).unconfirmed += q;
    }
    const classes = new Set(p.shipments.map((s) => SHIPMENT_CLASS[s.status] || 'unknown'));
    if ((p.status === 'ACTIVE' && (classes.has('open') || classes.has('done'))) || (p.status === 'SHIPPED' && classes.has('unshipped'))) {
      mixedPlans.push(p.id);
    }
    for (const s of p.shipments) {
      if (SHIPMENT_CLASS[s.status] === 'unshipped') for (const [k, q] of Object.entries(s.items)) at(k).unshipped += q;
    }
  }
  for (const s of snap.v0 || []) {
    if (!V0_OPEN_STATUSES.has(s.status)) continue;
    for (const [k, v] of Object.entries(s.items)) at(k).v0Open += Math.max(0, v.shipped - v.received);
  }
  // v0 の出荷便のうち、v2024 のどの便にも結べないもの (黙って捨てない。Codex B 設計レビュー 2 High 3)
  const linked = new Set((snap.plans || []).flatMap((p) => p.shipments.map((s) => s.confirmationId).filter(Boolean)));
  const fromMs = snap.discoverFrom ? Date.parse(snap.discoverFrom) : null;
  const unlinked = (snap.v0 || []).filter((s) => !linked.has(s.id));
  // 便を作るのはプランを作った数日後 → 探す範囲の境目から 7 日の余裕を持たせる (9/26 の試走: 9/11 のプランの 9/13 の便が「結べない」に出た)
  const isOld = (s) => { const t = staTimeMs(s.name); return fromMs !== null && t !== null && t < fromMs + 7 * 86400e3 && V0_OPEN_STATUSES.has(s.status); };
  const unlinkedV0 = unlinked.filter((s) => !isOld(s)).map((s) => s.id);     // 結べないはずがないもの (出荷前・最近の便・名前が読めない)
  const v0OnlyTracked = unlinked.filter(isOld).map((s) => s.id);             // 古い出荷済みの便 = v0 だけで追う (想定どおり)
  return { bySku, mixedPlans, unlinkedV0, v0OnlyTracked };
}

/**
 * レポート (RESTOCK) との照合。合わない SKU = レポートがまだその状態を反映していない (か、こちらの取りこぼし)。
 * @param {object} summary  summarizeSnapshot の戻り値
 * @param {{ amazon_sku: string, fba_inbound_working?: number|null, fba_inbound_shipped?: number|null, fba_inbound_received?: number|null }[]} reportRows
 */
export function checkAgainstReport(summary, reportRows) {
  const rep = {};
  for (const r of reportRows || []) {
    const k = normSku(r.amazon_sku);
    if (!k) continue;
    const cur = rep[k] || { working: 0, open: 0 };
    cur.working += Number(r.fba_inbound_working) || 0;
    cur.open += (Number(r.fba_inbound_shipped) || 0) + (Number(r.fba_inbound_received) || 0);
    rep[k] = cur;
  }
  const workingMismatch = [], openMismatch = [];
  for (const k of new Set([...Object.keys(rep), ...Object.keys(summary.bySku)])) {
    const a = summary.bySku[k] || { unshipped: 0, v0Open: 0 };
    const b = rep[k] || { working: 0, open: 0 };
    if (a.unshipped !== b.working) workingMismatch.push({ sku: k, api: a.unshipped, report: b.working });
    if (a.v0Open !== b.open) openMismatch.push({ sku: k, api: a.v0Open, report: b.open });
  }
  return { workingMismatch, openMismatch, skusChecked: new Set([...Object.keys(rep), ...Object.keys(summary.bySku)]).size };
}

const itemsKey = (m) => Object.entries(m || {}).filter(([, q]) => q).sort(([a], [b]) => (a < b ? -1 : 1)).map(([k, q]) => `${k}:${q}`).join('|');
const v0ItemsKey = (m) => Object.entries(m || {}).sort(([a], [b]) => (a < b ? -1 : 1)).map(([k, v]) => `${k}:${v.shipped}`).join('|');

/**
 * 2 つのスナップショットの差。プラン・便・v0 出荷便を ID で比べ、変わったものの **前後すべて** の SKU を返す。
 *   received だけが増えた v0 便は `receivedOnly` に分ける (B2 で S1→S2 だけ止めない候補。Codex B 設計レビュー 2 Medium 5)
 *   取り直さなかったプランは前回の中身を持ち越して比べる。中身が分からない (contentKnown=false) プランは、状態・lastUpdatedAt が
 *   変わっていれば unknown (B2 は unknown があれば全体を待つ)
 */
export function diffSnapshots(a, b) {
  const changes = [];
  const skus = new Set();
  const receivedOnly = [];
  const skusOf = (p) => [...Object.keys(p?.items || {}), ...(p?.shipments || []).flatMap((s) => Object.keys(s.items || {}))];
  const known = (p) => p.contentKnown ?? !p.reused;
  const pa = new Map((a.plans || []).map((p) => [p.id, p]));
  const pb = new Map((b.plans || []).map((p) => [p.id, p]));
  // 取れなかった・確かめなかったプランは「消えた」ではなく「分からない」(B2 はこれがあれば全体を待つ)
  const notSeen = new Set([...(a.deferredIds || []), ...(a.failedIds || []), ...(b.deferredIds || []), ...(b.failedIds || [])]);
  const unknown = [];
  for (const id of new Set([...pa.keys(), ...pb.keys(), ...notSeen])) {
    if (notSeen.has(id)) { unknown.push(id); continue; }
    const x = pa.get(id), y = pb.get(id);
    let why = null;
    if (!x) why = 'new_plan';
    else if (!y) why = 'plan_vanished';
    else if (x.status !== y.status) why = `plan_status ${x.status}→${y.status}`;
    else if (!known(x) || !known(y)) {
      if (x.lastUpdatedAt !== y.lastUpdatedAt) { unknown.push(id); continue; }
    }
    else if (itemsKey(x.items) !== itemsKey(y.items)) why = 'plan_items';
    else {
      const sa = new Map(x.shipments.map((s) => [s.id, s])), sb = new Map(y.shipments.map((s) => [s.id, s]));
      if ([...sa.keys()].sort().join() !== [...sb.keys()].sort().join()) why = 'shipment_set';
      else for (const [sid, s] of sa) {
        const t = sb.get(sid);
        if (s.status !== t.status) { why = `shipment ${sid} ${s.status}→${t.status}`; break; }
        if (itemsKey(s.items) !== itemsKey(t.items)) { why = `shipment ${sid} items`; break; }
      }
    }
    if (!why && x && y && x.lastUpdatedAt !== y.lastUpdatedAt) why = 'plan_updated_only';
    if (why) {
      const s = new Set([...skusOf(x), ...skusOf(y)]);
      changes.push({ kind: 'plan', id, why, skus: [...s] });
      if (why !== 'plan_updated_only') for (const k of s) skus.add(k);
    }
  }
  const va = new Map((a.v0 || []).map((s) => [s.id, s]));
  const vb = new Map((b.v0 || []).map((s) => [s.id, s]));
  for (const id of new Set([...va.keys(), ...vb.keys()])) {
    const x = va.get(id), y = vb.get(id);
    const s = new Set([...Object.keys(x?.items || {}), ...Object.keys(y?.items || {})]);
    let why = null;
    if (!x) why = 'new_v0';
    else if (!y) why = 'v0_vanished';
    else if (x.status !== y.status) why = `v0_status ${x.status}→${y.status}`;
    else if (v0ItemsKey(x.items) !== v0ItemsKey(y.items)) why = 'v0_shipped_qty';
    else {
      const dec = Object.keys(x.items).some((k) => (y.items[k]?.received ?? 0) < x.items[k].received);
      const inc = Object.keys(y.items).some((k) => (y.items[k]?.received ?? 0) > (x.items[k]?.received ?? 0));
      if (dec) why = 'v0_received_decreased';
      else if (inc) { receivedOnly.push({ id, skus: [...s] }); continue; }
    }
    if (why) {
      changes.push({ kind: 'v0', id, why, skus: [...s] });
      for (const k of s) skus.add(k);
    }
  }
  return { changes, skus: [...skus].sort(), receivedOnly, unknown };
}

/**
 * 追跡中の未解決プランを次回へ。品目があり、まだ出荷し終わっていないプラン (VOIDED でない・全便が出荷済みでない・
 * 便が 0 件のまま SHIPPED = 解決とみなさない) を残す。取れなかったプランは前回の追跡を引き継ぐ (黙って外さない)
 */
export function nextTracked(snap, prevTracked = []) {
  const seen = new Set((snap.plans || []).map((p) => p.id));
  const out = new Set(prevTracked.filter((id) => !seen.has(id)));
  for (const p of snap.plans || []) {
    const hasItems = Object.values(p.items).some((q) => q > 0) || p.shipments.some((s) => Object.keys(s.items).length);
    if (!hasItems || p.status === 'VOIDED') continue;
    // 全便が出荷済み (輸送中・受領中・終わり・取消) になったら追跡をやめる = 出荷後は v0 の便 ID で追う。
    //   便 0 件の SHIPPED・出荷前の便が残るプラン・知らない状態の便があるプランは追い続ける
    const allShipped = p.shipments.length > 0 && p.shipments.every((s) => ['open', 'done', 'void'].includes(SHIPMENT_CLASS[s.status]));
    if (!allShipped) out.add(p.id);
  }
  return [...out].sort();
}

/** 次回の「空プランを取り直さない」判断の材料 */
export function nextPlanCache(snap, prevPlans = {}) {
  const out = { ...prevPlans };
  for (const p of snap.plans || []) {
    if (p.reused) continue;   // 取り直していない = 前回の記録のまま (確かめた時刻も進めない)
    const prev = prevPlans[p.id] || null;
    const empty = p.contentKnown !== false && !Object.values(p.items).some((q) => q > 0) && p.shipments.length === 0;
    const resolved = p.status === 'VOIDED'
      || (p.status === 'SHIPPED' && p.shipments.length > 0 && p.shipments.every((s) => ['open', 'done', 'void'].includes(SHIPMENT_CLASS[s.status])));
    // 品目のあるプランは中身も残す (取り直さない日に持ち越す・取り消し後も SKU が分かる)。中身が分からない回は前回のものを残す
    const content = p.contentKnown === false ? (prev?.content || null)
      : (empty ? null : { items: p.items, shipments: p.shipments.map((s) => ({ id: s.id, confirmationId: s.confirmationId, status: s.status, items: s.items })) });
    out[p.id] = { lastUpdatedAt: p.lastUpdatedAt, status: p.status, empty, resolved, verifiedAt: p.verifiedAt, content };
  }
  return out;
}
