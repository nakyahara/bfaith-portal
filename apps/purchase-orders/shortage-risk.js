/**
 * P17 欠品リスクリスト (要件v8 F-4、v1 = 簡易目安の参考表示)
 *
 * 対象 = オープン注残 (残数>0) のある商品。仕入先督促の材料にする。
 *
 * 計算仕様 (Codex設計相談 2026-07-16 で確定):
 *  - 加重日販 = 販売7日/7×w7 + 販売30日/30×(1−w7)。w7 は設定 (既定0.5)。クランプはせず、
 *    7日と30日のペース乖離が大きい場合は理由文で説明する (v1は精密予測を狙わない)
 *  - 在庫推移は日次離散シミュレーション。日付は全て JST 業務日付 (YYYY-MM-DD 文字列比較、UTC変換しない)。
 *    【日次の順序仕様】各日 t: (1) その日の予定入荷を加算 → (2) 当日の日販を消費 → (3) 在庫≤0 判定
 *  - 予定入荷の解釈 (明細ごと):
 *      日付 = next_expected_date (分納予定) > promised_date (回答納期) > requested_date (希望納期)
 *      分納予定 (awaiting_delivery) は next_expected_qty 分だけを予定入荷にし、残りは納期不明 (undated) 扱い
 *      希望納期しか無い明細 = 未回答のまま楽観仮定 → requested_date_only フラグで明示
 *      予定日が今日より過去 = 遅延中 → いつ入るか不明としてシミュレーション除外 (overdue フラグ)
 *      日付なし = 納期不明 (undated、シミュレーション除外)
 *  - stockoutDate = 初めて在庫≤0 になる日 (現在庫が既に0以下なら今日)。欠品継続日数・最大不足数・
 *    horizon 内に解消するかも返す
 *  - marginDays = 欠品しない場合の「予定入荷直前の最小在庫 ÷ 日販」(入荷が綱渡りかの余裕日数)
 *
 * リスク分類と督促フラグは2軸に分離 (Codex設計相談):
 *  - risk: unknown (PML行なし=データ異常のみ) / no_demand (日販≤0) / high / attention / ok
 *      high = horizon内に欠品 or marginDays ≤ margin_days (既定3)
 *      attention = highでないが督促フラグ (遅延中 / 未回答N日経過) がある
 *  - flags: overdue / unanswered / undated / requested_date_only — 督促コピーはこちらを使う
 *    (no_demand や unknown の商品でも遅延・未回答があれば督促対象として落とさない)
 */
import { getDB, normProductCode, normSupplierCode } from './db.js';
import { getSetting, jstToday, listBackorders, SHORTAGE_SETTING_RULES } from './ledger.js';
import { loadPmlMerged } from './logic.js';

export const SHORTAGE_DEFAULTS = { w7: 0.5, marginDays: 3, unansweredDays: 7, horizonDays: 90, soonDays: 14 };
const SETTING_KEYS = { w7: 'shortage_w7', marginDays: 'shortage_margin_days', unansweredDays: 'shortage_unanswered_days', horizonDays: 'shortage_horizon_days', soonDays: 'shortage_soon_days' };
// 設定の許容範囲 — 検証の単一ソースは ledger.js の SHORTAGE_SETTING_RULES (setSetting自体も同じ規則で検証する)
export const SHORTAGE_RANGES = Object.fromEntries(Object.entries(SETTING_KEYS).map(([name, key]) => [name, SHORTAGE_SETTING_RULES[key]]));

export function validateShortageSetting(name, value) {
  const r = SHORTAGE_RANGES[name];
  if (!r) throw new Error(`未知の欠品リスク設定です: ${name}`);
  // JSON number のみ受理 (Number(null)=0 / Number('')=0 で「空欄が0として保存」される事故を防ぐ、Codex R2 Medium)
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`${name} は数値で指定してください: ${value}`);
  if (r.int && !Number.isInteger(value)) throw new Error(`${name} は整数で指定してください: ${value}`);
  if (value < r.min || value > r.max) throw new Error(`${name} は ${r.min}〜${r.max} の範囲で指定してください: ${value}`);
  return value;
}

export function shortageSettings() {
  const out = {};
  for (const [name, key] of Object.entries(SETTING_KEYS)) {
    const raw = getSetting(key);
    let v = SHORTAGE_DEFAULTS[name];
    if (raw != null) {
      const n = Number(raw);
      const r = SHORTAGE_RANGES[name];
      // 不正保存値 (範囲外・非整数) は既定にフォールバック (fail-soft。書込側検証 setSetting の防御と二重)
      if (Number.isFinite(n) && n >= r.min && n <= r.max && (!r.int || Number.isInteger(n))) v = n;
    }
    out[name] = v;
  }
  return out;
}
export function shortageSettingKey(name) { return SETTING_KEYS[name]; }

/** JST業務日付の加算 (YYYY-MM-DD + n日)。業務日付なのでタイムゾーン変換は行わない */
function addDays(ymd, n) {
  const t = Date.parse(`${ymd}T00:00:00Z`);
  return new Date(t + n * 86400000).toISOString().slice(0, 10);
}
function diffDays(fromYmd, toYmd) {
  return Math.round((Date.parse(`${toYmd}T00:00:00Z`) - Date.parse(`${fromYmd}T00:00:00Z`)) / 86400000);
}
const fmtMd = ymd => (ymd ? `${Number(ymd.slice(5, 7))}/${Number(ymd.slice(8, 10))}` : '');
const r1 = n => Math.round(n * 10) / 10;

/**
 * 欠品リスクの全計算 (画面と督促コピーの共通サービス)。
 */
export function computeShortageRisk() {
  const settings = shortageSettings();
  const today = jstToday();
  const pml = loadPmlMerged();
  const pmlByKey = new Map(pml.rows.map(r => [normProductCode(r['商品コード']), r]));

  // オープン注残を商品単位に集約 (明細は仕入先表示用に PO 情報を保持)
  const bo = listBackorders();
  const products = new Map(); // product_key → { code, name, lines: [{o, i}] }
  for (const o of bo.orders) {
    if (!o.open) continue;
    for (const i of o.items) {
      if (i.remaining_qty <= 0) continue;
      const key = i.product_key || normProductCode(i.product_code);
      let p = products.get(key);
      if (!p) p = products.set(key, { key, code: i.product_code, name: i.product_name || '', lines: [] }).get(key);
      if (!p.name && i.product_name) p.name = i.product_name;
      p.lines.push({ o, i });
    }
  }

  const items = [];
  for (const p of products.values()) {
    items.push(computeProductRisk(p, pmlByKey.get(p.key), today, settings));
  }

  // 仕入先ごとにグループ化。予測評価 (risk/stockout/margin) は商品単位=全PO横断で共通だが、
  // 督促関連 (遅延・未回答の数量/フラグ/文面) は**その仕入先の明細だけで再集計**する —
  // 他仕入先のPO事情がこの仕入先の督促コピーに混ざるのは誤通知 (Codex R2 High)
  const supplierMap = new Map();
  for (const it of items) {
    for (const supCode of Object.keys(it.bySupplier)) {
      let g = supplierMap.get(supCode);
      if (!g) g = supplierMap.set(supCode, { code: supCode, name: it.bySupplier[supCode].supplierName, items: [], counts: { high: 0, attention: 0, no_demand: 0, unknown: 0, ok: 0, followup: 0 } }).get(supCode);
      const lines = it.bySupplier[supCode].lines;
      const scoped = scopeFacts(lines, today, settings);
      g.items.push({
        ...it, lines, bySupplier: undefined,
        ...scoped, // overdueQty / requestedOverdueQty / undatedQty / unansweredQty / oldestUnansweredDays / flags / needsFollowup をこの仕入先分に置換
        reason: it.predictionReason + procurementText(scoped, settings),
      });
      g.counts[it.risk] = (g.counts[it.risk] || 0) + 1;
      if (scoped.needsFollowup) g.counts.followup++;
    }
  }
  const RISK_ORDER = { high: 0, attention: 1, unknown: 2, no_demand: 3, ok: 4 };
  const suppliers = [...supplierMap.values()];
  for (const g of suppliers) {
    // 督促対象 (needsFollowup) はリスク分類に関わらず前に出す (2軸: 予測評価×督促)
    g.items.sort((a, b) => (RISK_ORDER[a.risk] - RISK_ORDER[b.risk])
      || (Number(b.needsFollowup) - Number(a.needsFollowup))
      || String(a.stockoutDate || '9999').localeCompare(String(b.stockoutDate || '9999'))
      || a.code.localeCompare(b.code));
  }
  suppliers.sort((a, b) => (b.counts.high - a.counts.high) || (b.counts.followup - a.counts.followup)
    || (b.counts.attention - a.counts.attention) || a.name.localeCompare(b.name));

  const summary = { high: 0, attention: 0, no_demand: 0, unknown: 0, ok: 0, followup: 0 };
  for (const it of items) { summary[it.risk]++; if (it.needsFollowup) summary.followup++; }

  // データ基準時刻 (Codex設計相談: 在庫・販売の basis を常時表示。>2日で stale 警告)。
  // as_of_date が不正形式で diffDays が NaN になるケースも stale 扱い (NaN>2 は false、Codex R1 Low)
  const asOfDate = pml.pub ? pml.pub.as_of_date : null;
  const ageDays = asOfDate ? diffDays(String(asOfDate), today) : NaN;
  const stale = !Number.isFinite(ageDays) || ageDays > 2;
  return {
    today, settings, summary, suppliers,
    dataBasis: {
      pmlAsOfDate: asOfDate, stale,
      overlay: pml.overlay ? { source: pml.overlay.source, uploadedAt: pml.overlay.uploaded_at, applied: pml.overlay.applied } : null,
    },
  };
}

/** UTC ISO時刻 → JST業務日付 (issuedAt の日数判定用。UTC素通しsliceはJST 0-9時で1日ずれる) */
function jstYmd(iso) {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  return new Date(t + 9 * 3600000).toISOString().slice(0, 10);
}

/**
 * 明細集合から督促関連の集計を導出する (lines.parts が唯一の解釈ソース)。
 * 商品全体 (全仕入先) にも、仕入先スコープにも同じ関数を使う (Codex R2 High: スコープごとの再集計)
 */
function scopeFacts(lines, today, settings) {
  let overdueQty = 0, requestedOverdueQty = 0, undatedQty = 0, unansweredQty = 0, oldestUnansweredDays = 0;
  let requestedDateOnly = false;
  for (const l of lines) {
    for (const pt of l.parts) {
      if (pt.disp.kind === 'overdue') {
        if (pt.disp.source === 'requested') { requestedOverdueQty += pt.qty; requestedDateOnly = true; }
        else overdueQty += pt.qty;
      } else if (pt.disp.kind === 'undated') {
        undatedQty += pt.qty;
      } else if (pt.disp.source === 'requested') {
        requestedDateOnly = true;
      }
    }
    if (l.unanswered) {
      unansweredQty += l.remaining;
      const issuedYmd = (l.issuedAt && jstYmd(l.issuedAt)) || today;
      oldestUnansweredDays = Math.max(oldestUnansweredDays, diffDays(issuedYmd, today));
    }
  }
  const flags = {
    overdue: overdueQty > 0,
    unanswered: unansweredQty > 0 && oldestUnansweredDays >= settings.unansweredDays,
    undated: undatedQty > 0,
    requested_date_only: requestedDateOnly,
  };
  return {
    overdueQty, requestedOverdueQty, undatedQty, unansweredQty, oldestUnansweredDays, flags,
    needsFollowup: flags.overdue || flags.unanswered || requestedOverdueQty > 0,
  };
}

function computeProductRisk(p, pmlRow, today, settings) {
  const num = v => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
  const arrivals = []; // { date, qty, source }
  const lines = [];    // 表示用明細 (PO横断)。数量の内訳は parts (シミュレーションの解釈と1:1)
  let totalRemaining = 0;

  for (const { o, i } of p.lines) {
    totalRemaining += i.remaining_qty;
    // 予定入荷の分解 (分納予定は next_expected_qty 分だけ日付つき、残りは納期不明)。
    // 表示・督促コピーにも同じ分解 (parts) を出す — 「残100を全量7/20予定」と誤表示しない (Codex R1 High)
    let datedQty = i.remaining_qty, restQty = 0, date = null, source = null;
    if (i.remainder_disposition === 'awaiting_delivery' && i.next_expected_date) {
      date = i.next_expected_date; source = 'next';
      const nq = num(i.next_expected_qty);
      if (nq > 0 && nq < i.remaining_qty) { datedQty = nq; restQty = i.remaining_qty - nq; }
    } else if (i.promised_date) { date = i.promised_date; source = 'promised'; }
    else if (i.requested_date) { date = i.requested_date; source = 'requested'; }

    const parts = [];
    if (!date) {
      parts.push({ qty: datedQty + restQty, disp: { kind: 'undated' } });
    } else if (date < today) {
      // 過去日は source で意味が違う (Codex R1 High): 回答納期/分納予定の超過=遅延中、
      // 希望納期の超過=回答が来ないまま希望日を過ぎた (未回答の一種。仕入先は約束していない)
      parts.push({ qty: datedQty, disp: { kind: 'overdue', date, source } });
      if (restQty > 0) parts.push({ qty: restQty, disp: { kind: 'undated' } });
    } else {
      arrivals.push({ date, qty: datedQty, source });
      parts.push({ qty: datedQty, disp: { kind: 'dated', date, source } });
      if (restQty > 0) parts.push({ qty: restQty, disp: { kind: 'undated' } });
    }
    lines.push({
      orderId: o.id, poNumber: (o.origin === 'migration' ? o.neSlipNumber : null) || o.poNumber || `#${o.id}`,
      supplierCode: o.supplierCode, supplierName: o.supplierName, origin: o.origin,
      issuedAt: o.issuedAt, remaining: i.remaining_qty, vendorCode: i.vendor_code || null,
      requestedDate: i.requested_date || null, promisedDate: i.promised_date || null,
      nextExpectedDate: i.next_expected_date || null, nextExpectedQty: i.next_expected_qty || null,
      unanswered: !i.promised_date, parts,
    });
  }
  // 督促関連の集計は lines.parts から導出 (商品全体スコープ。仕入先スコープは grouping 側で同じ関数を使う)
  const { overdueQty, requestedOverdueQty, undatedQty, unansweredQty, oldestUnansweredDays, flags, needsFollowup } =
    scopeFacts(lines, today, settings);

  // 仕入先ごとの表示明細 (商品が複数仕入先の注残を持つ稀ケースは両方に出す)
  const bySupplier = {};
  for (const l of lines) {
    if (!bySupplier[l.supplierCode]) bySupplier[l.supplierCode] = { supplierName: l.supplierName, lines: [] };
    bySupplier[l.supplierCode].lines.push(l);
  }

  const base = {
    key: p.key, code: p.code, name: p.name, totalRemaining,
    overdueQty, undatedQty, requestedOverdueQty, unansweredQty, oldestUnansweredDays,
    flags, needsFollowup, bySupplier, arrivals: arrivals.slice().sort((a, b) => a.date.localeCompare(b.date)),
  };

  // unknown = PML行なし (データ異常のみ)。リスク軸は unknown のまま、督促は needsFollowup 軸で拾う (2軸分離)
  if (!pmlRow) {
    const predictionReason = 'PMLに商品が見つかりません (商品コード改廃?)';
    return { ...base, risk: 'unknown', pmlMissing: true,
      stock: null, dailySales: null, stockoutDate: null, marginDays: null,
      predictionReason, reason: predictionReason + procurementText(base, settings) };
  }

  const stock = num(pmlRow['総在庫数_引当なし']);
  const s7 = num(pmlRow['販売数7日_合計']);
  const s30 = num(pmlRow['販売数30日_合計']);
  const d7 = s7 / 7, d30 = s30 / 30;
  const dailySales = d7 * settings.w7 + d30 * (1 - settings.w7);

  if (dailySales <= 0) {
    const predictionReason = '販売実績なし (日販0) — 欠品予測の対象外';
    return { ...base, risk: 'no_demand',
      stock, dailySales: 0, stockoutDate: null, marginDays: null,
      predictionReason, reason: predictionReason + procurementText(base, settings) };
  }

  // ── 日次シミュレーション: (1)入荷加算 → (2)日販消費 → (3)判定 ──
  const arrivalByDate = new Map();
  for (const a of base.arrivals) arrivalByDate.set(a.date, (arrivalByDate.get(a.date) || 0) + a.qty);
  // shortageDays は「最初の欠品ウィンドウ」(欠品開始→次の入荷で回復するまで) のみ数える。
  // 入荷後に販売で再び在庫が尽きる分は今回の注残の問題ではなく発注提案 (要発注) の領分
  let level = stock;
  // 現在庫が既に0以下なら欠品予測=今日 (合意仕様。当日入荷で回復してもこの事実は消さない — Codex R1 Medium)
  let stockoutDate = stock <= 0 ? today : null, shortageDays = 0, maxDeficit = 0, recovered = false, preArrivalMin = null;
  let firstWindowClosed = false;
  // horizonDays=90 なら t=0 (今日) 〜 t=89 の90日間を評価 (t<=horizon だと91日になる、Codex R1 Medium)
  for (let t = 0; t < settings.horizonDays; t++) {
    const date = t === 0 ? today : addDays(today, t);
    if (arrivalByDate.has(date)) {
      if (preArrivalMin == null || level < preArrivalMin) preArrivalMin = level; // 入荷直前の最小在庫 (余裕の測定点)
      level += arrivalByDate.get(date);
    }
    level -= dailySales;
    if (level <= 0) {
      if (!stockoutDate) stockoutDate = date;
      if (stockoutDate && !firstWindowClosed) {
        shortageDays++;
        if (-level > maxDeficit) maxDeficit = -level;
      }
    } else if (stockoutDate && !firstWindowClosed) {
      firstWindowClosed = true; // 最初の欠品ウィンドウが入荷で閉じた
    }
  }
  if (stockoutDate) recovered = firstWindowClosed;
  const marginDays = !stockoutDate && preArrivalMin != null ? Math.floor(preArrivalMin / dailySales) : null;
  const stockDays = Math.floor(stock / dailySales);

  let risk, reason;
  if (stockoutDate) {
    // 入荷予定日が1つも無い商品は在庫が horizon 日分未満なら必ず「欠品予測あり」になりノイズ化する。
    // → 🔴 は「予定入荷があるのに間に合わない/足りない」または「欠品予測日が soonDays 以内で切迫」に限定し、
    //   遠い欠品予測+納期未確定は 🟡 (納期確定の督促材料) とする
    const hasDated = base.arrivals.length > 0;
    const soon = diffDays(today, stockoutDate) <= settings.soonDays;
    risk = hasDated || soon ? 'high' : 'attention';
    reason = `${fmtMd(stockoutDate)}に在庫切れ見込み (現在庫${stock.toLocaleString('ja-JP')}≒${stockDays}日分)`;
    if (hasDated) {
      const next = base.arrivals.find(a => a.date > stockoutDate);
      if (next) reason += `。次回入荷 ${next.qty.toLocaleString('ja-JP')}個は ${fmtMd(next.date)} 予定 → ${shortageDays}日間の欠品見込み`;
      else reason += `。予定入荷を織り込んでも${recovered ? `${shortageDays}日間の欠品見込み` : '解消見込みなし (' + settings.horizonDays + '日内)'}`;
      if (maxDeficit > 0) reason += ` (最大不足 約${Math.ceil(maxDeficit).toLocaleString('ja-JP')}個)`;
    } else {
      reason += soon
        ? `。入荷予定日が確定しておらず切迫 (${settings.soonDays}日以内) → 至急納期確定の督促を`
        : '。入荷予定日が未確定 → 納期を確定させれば予測できます (回答督促)';
    }
  } else if (marginDays != null && marginDays <= settings.marginDays) {
    risk = 'high';
    reason = `入荷直前の在庫余裕が約${marginDays}日分 (しきい値${settings.marginDays}日以下) — 入荷遅れで欠品`;
  } else {
    // risk軸は予測評価のみ (遅延・未回答の督促は needsFollowup 軸で拾う — 2軸分離、Codex R1 High)
    risk = 'ok';
    reason = marginDays != null
      ? `欠品見込みなし (入荷前の余裕 約${marginDays}日)`
      : `在庫${stock.toLocaleString('ja-JP')}≒${stockDays}日分 (${settings.horizonDays}日内の欠品なし)`;
  }
  // 7日/30日ペースの乖離が大きい場合は説明を添える (クランプはしない、v1方針)
  if (d7 > 0 && d30 > 0 && (d7 / d30 >= 2 || d30 / d7 >= 2)) {
    reason += `。⚠️直近7日の販売ペース (${r1(d7)}/日) が30日平均 (${r1(d30)}/日) と乖離 — 予測が振れやすい`;
  }
  const predictionReason = reason; // 予測評価の説明 (商品共通)。督促文は仕入先スコープで付け直す

  return { ...base, risk, stock, dailySales: r1(dailySales), stockDays, stockoutDate, shortageDays: stockoutDate ? shortageDays : 0,
    maxDeficit: Math.ceil(maxDeficit), recovered, marginDays,
    predictionReason, reason: predictionReason + procurementText(base, settings) };
}

/** 督促フラグの説明文 (リスク分類と独立に常に付す) */
function procurementText(base, settings) {
  const parts = [];
  if (base.overdueQty > 0) parts.push(`注残${base.overdueQty.toLocaleString('ja-JP')}個が予定日超過 (遅延中)`);
  if (base.requestedOverdueQty > 0) parts.push(`注残${base.requestedOverdueQty.toLocaleString('ja-JP')}個が希望納期を経過も回答未着 → 回答督促`);
  if (base.unansweredQty > 0) {
    parts.push(`注残${base.unansweredQty.toLocaleString('ja-JP')}個が納期未回答` +
      (base.oldestUnansweredDays >= settings.unansweredDays ? ` (発注から${base.oldestUnansweredDays}日) → 回答督促` : ''));
  }
  if (base.undatedQty > 0) parts.push(`${base.undatedQty.toLocaleString('ja-JP')}個は納期不明 (予測の計算外)`);
  if (base.flags.requested_date_only) parts.push('予定は希望納期ベース (回答未取得の楽観仮定)');
  return parts.length ? '。' + parts.join('。') : '';
}
