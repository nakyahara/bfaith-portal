/**
 * ui-format.mjs — マスタ入力画面の「見せ方」だけ (時刻は日本時間・変更の記録を人の言葉に・原価の帯)。DB は読まない・書かない
 *
 * - 時刻: DB の timestamptz の文字 (セッションの TimeZone で '+00' / '+09' が付く)・ISO・Date を受けて、東京の「10/2 (金) 11:05」にそろえる。
 *   日本は夏時間が無いので UTC + 9 時間で足りる (Intl に頼らない = サーバーの ICU に左右されない)
 * - 変更の記録 (events.master_change_events): 列の名前 (external_id・reorder_months …) を出さず「標準売価 1,680 円 → 1,780 円」の形にする。
 *   見て分かる項目が無い行 (version・updated_at だけ) は出さない
 * - 原価の帯: core.sku_costs の行 (valid_from・valid_to は両端を含む) を、今日を中心に 6 か月の帯にする
 */

const WD = '日月火水木金土';
const pad = (n) => String(n).padStart(2, '0');

/** 時刻 → ミリ秒 (読めない = null)。'2026-10-02 01:54:00.12+00' / '…+09:00' / ISO / Date / ミリ秒 */
export function toMs(v) {
  if (v == null || v === '') return null;
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.getTime();
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  const s = String(v).trim();
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?\s*(Z|[+-]\d{2}(?::?\d{2})?)?$/.exec(s);
  if (m) {
    const [, y, mo, d, h, mi, se, frac, tz] = m;
    let ms = Date.UTC(+y, +mo - 1, +d, +h, +mi, +(se || 0), frac ? Math.round(Number(frac) * 1000) : 0);
    if (tz && tz !== 'Z') {
      const sign = tz[0] === '-' ? -1 : 1;
      const digits = tz.slice(1).replace(':', '');
      const off = (Number(digits.slice(0, 2)) * 60 + Number(digits.slice(2, 4) || 0)) * sign;
      ms -= off * 60000;
    } else if (!tz) {
      return null;   // どの時間帯か分からない時刻は推測しない
    }
    return ms;
  }
  const t = Date.parse(s);
  return Number.isNaN(t) ? null : t;
}

/** 東京の年・月・日・時・分・曜日 */
function jst(ms) {
  const d = new Date(ms + 9 * 3600 * 1000);
  return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate(), h: d.getUTCHours(), mi: d.getUTCMinutes(), w: WD[d.getUTCDay()] };
}

/** 時刻 → 「10/2 (金) 11:05」(今年でなければ「2025/12/30 (火) 10:00」)。読めない = 元の文字 (空なら '') */
export function fmtJst(v, { nowMs = Date.now() } = {}) {
  const ms = toMs(v);
  if (ms == null) return v == null ? '' : String(v);
  const a = jst(ms);
  const thisYear = jst(nowMs).y;
  return `${a.y === thisYear ? '' : `${a.y}/`}${a.m}/${a.d} (${a.w}) ${pad(a.h)}:${pad(a.mi)}`;
}

/** 日付 'YYYY-MM-DD' → 「10/2 (金)」(today と年が違えば年も)。読めない = 元の文字 */
export function fmtDay(ymd, today = null) {
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(ymd ?? ''));
  if (!m) return ymd == null ? '' : String(ymd);
  const w = WD[new Date(Date.UTC(+m[1], +m[2] - 1, +m[3])).getUTCDay()];
  const ty = /^(\d{4})/.exec(String(today ?? ''))?.[1];
  return `${ty && ty !== m[1] ? `${m[1]}/` : ''}${+m[2]}/${+m[3]} (${w})`;
}

/** 日付の差 (日)。a・b = 'YYYY-MM-DD' */
export function dayDiff(a, b) {
  const p = (s) => { const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(s)); return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) / 864e5 : null; };
  const x = p(a); const y = p(b);
  return x == null || y == null ? null : x - y;
}

const yen = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? '' : Number(v).toLocaleString('ja-JP'));

// ─── 変更の記録を人の言葉に ───
const HANDLING = { active: '取扱中', discontinued: '中止', unknown: '不明' };
const TAX_CLASS = { STANDARD_10: '標準 10%', REDUCED_8: '軽減 8%', MIXED: '混在', UNKNOWN: '不明' };
const SALES = { 1: '1 自社', 2: '2 取引先限定', 3: '3 仕入', 4: '4 輸出' };
const COST_SRC = { ne: 'NE から', manual: '手で入れた', set_calc: '構成品の合計', override_zero: '0 円の上書き', imported: '取込' };
const SOURCES = {
  portal_master_edit: 'マスタの入力', company_db_load: '夜間の取り込み', ne_observation: 'NE の構成の確かめ', portal: 'ポータル',
  portal_amazon_map: 'Amazon SKU の対応', logizard_diff: 'ロジザードとの差', sql: '手作業 (SQL)',
};
const boolText = (v) => (v === true ? 'あり' : v === false ? 'なし' : '不明');
/** 列ごとの名前と値の見せ方。hide = 人が見る意味が無い列 */
const ATTRS = {
  sku: {
    name: ['名前'], standard_price_jpy: ['標準売価', (v) => `${yen(v)} 円`], tax_rate: ['税率', (v) => `${Math.round(Number(v) * 100)}%`],
    tax_class: ['税区分', (v) => TAX_CLASS[v] || v], handling: ['取扱区分', (v) => HANDLING[v] || v], handling_own: ['セット自身の取扱', (v) => HANDLING[v] || v],
    shipping_code: ['送料コード'], shipping_method: ['配送方法'], shipping_cost_jpy: ['送料', (v) => `${yen(v)} 円`],
    reorder_months: ['推奨保有月数', (v) => `${Number(v)} か月`], set_sales_class_override: ['売上分類の上書き', (v) => SALES[v] || v],
  },
  product: {
    name: ['商品名'], sales_class: ['売上分類', (v) => SALES[v] || v], status: ['取扱区分 (商品)', (v) => HANDLING[v] || v],
    parent_product_id: ['代表 (親)', () => '別の商品'], parent_set_by: ['代表の決め方', (v) => ({ manual: '人が決めた', load: 'NE から' }[v] || v)],
    expiry_managed: ['有効期限の管理', boolText], inbound_date_managed: ['入荷日の管理', boolText],
  },
  supplier_sku: { is_primary: ['代表の仕入先', (v) => (v ? '代表にした' : '代表から外した')], vendor_code: ['先方品番'] },
  sku_component: { qty: ['構成の数量', (v) => `×${v}`], sort_order: ['構成の並び', (v) => `${v} 番目`] },
  sku_cost: { valid_to: ['前の原価の終わりの日', (v) => (v ? fmtDay(v) : 'なし (続く)')], cost_jpy: ['原価', (v) => `${yen(v)} 円`], cost_status: ['原価の状態'] },
  external_id: { valid_to: ['JAN の終わり', (v) => (v ? fmtDay(v) : 'なし (使う)')] },
};
const HIDDEN = new Set(['version', 'updated_at', 'created_at', 'code_norm', 'created_by_type', 'created_by_id', 'row_hash', 'company_id']);
const valOf = (v) => (v && typeof v === 'object' && !Array.isArray(v) && Object.prototype.hasOwnProperty.call(v, 'value') && Object.keys(v).length === 1 ? v.value : v);

/**
 * 1 つの変更の記録 → { what, from, to, text } (見せない行 = null)。
 * from / to は空のことがある (足した・外した)。text = 1 行の言葉 (「標準売価 1,680 円 → 1,780 円」)
 */
export function eventWords(e) {
  if (!e) return null;
  const t = e.entity_type; const op = e.operation;
  const nv = e.new_value && typeof e.new_value === 'object' ? e.new_value : null;
  const ov = e.old_value && typeof e.old_value === 'object' ? e.old_value : null;
  if (op === 'UPDATE') {
    const a = String(e.attribute || '');
    if (HIDDEN.has(a) || /_hash$/.test(a)) return null;
    const def = (ATTRS[t] || {})[a];
    const what = def ? def[0] : `${({ sku: 'SKU', product: '商品', sku_cost: '原価', sku_component: '構成', supplier_sku: '仕入先', external_id: 'JAN', listing: '出品', listing_component: '出品の構成' }[t] || t)} の項目 (${a})`;
    const show = (v) => { const x = valOf(v); if (x === null || x === undefined || x === '') return '(空)'; return def && def[1] ? String(def[1](x)) : typeof x === 'object' ? JSON.stringify(x) : String(x); };
    if (t === 'supplier_sku' && a === 'is_primary') { const to = show(e.new_value); return { what, from: '', to, text: `${what}: ${to}` }; }
    const from = show(e.old_value); const to = show(e.new_value);
    return { what, from, to, text: `${what} ${from} → ${to}` };
  }
  const row = op === 'INSERT' ? nv : ov;
  const verb = op === 'INSERT' ? '足した' : '外した';
  if (t === 'sku_cost' && row) {
    const what = op === 'INSERT' ? '原価' : '原価の行を消した';
    const to = `${yen(row.cost_jpy)} 円${row.valid_from ? ` (${fmtDay(row.valid_from)} から)` : ''}${row.cost_source ? ` · ${COST_SRC[row.cost_source] || row.cost_source}` : ''}`;
    return { what, from: '', to, text: `${what} ${to}` };
  }
  if (t === 'external_id' && row) {
    const what = `JAN を${verb}`;
    return { what, from: '', to: String(row.external_value ?? ''), text: `${what} ${row.external_value ?? ''}` };
  }
  if (t === 'sku_component' && row) {
    const what = `構成品を${verb}`;
    const to = `×${row.qty ?? '?'}`;
    return { what, from: '', to, text: `${what} (${to})` };
  }
  if (t === 'supplier_sku' && row) {
    const what = `仕入先を${verb}`;
    const to = row.vendor_code ? `先方品番 ${row.vendor_code}` : '';
    return { what, from: '', to, text: `${what}${to ? ` (${to})` : ''}` };
  }
  if ((t === 'sku' || t === 'product') && row) {
    const what = op === 'INSERT' ? (t === 'sku' ? 'この商品を登録した' : '商品の行を作った') : (t === 'sku' ? 'この商品を消した' : '商品の行を消した');
    return { what, from: '', to: row.name ? String(row.name) : '', text: `${what}${row.name ? ` (${row.name})` : ''}` };
  }
  const what = `${t} を${verb}`;
  return { what, from: '', to: '', text: what };
}

/** 誰が (人 = ログインのメール / 仕組み = その名前) */
export function actorWords(e) {
  if (!e) return '';
  if (e.actor_type === 'human' && e.actor_id) return String(e.actor_id);
  return SOURCES[e.source_system] || e.actor_id || e.source_system || e.actor_type || '';
}
/** どこから */
export const sourceWords = (s) => SOURCES[s] || s || '';

// ─── 原価の帯 ───
const ymdUtc = (s) => { const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(s ?? '')); return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) / 864e5 : null; };
const ymdOf = (day) => new Date(day * 864e5).toISOString().slice(0, 10);

/**
 * 原価の行 (新しい順でもよい) → 帯の部品。今日の月の 3 か月前の 1 日 〜 2 か月後の末日。
 * kind = past (これまで) / now (今日を含む) / fut (先の日付から始まる行)。原価が無い・全部が窓の外 = segs が空
 */
export function costTimeline(costs, today) {
  const t = ymdUtc(today);
  if (t == null) return null;
  const td = new Date(t * 864e5);
  const from = Date.UTC(td.getUTCFullYear(), td.getUTCMonth() - 3, 1) / 864e5;
  const to = Date.UTC(td.getUTCFullYear(), td.getUTCMonth() + 3, 1) / 864e5 - 1;
  const W = to - from + 1;
  const pct = (d) => Math.max(0, Math.min(100, ((d - from) / W) * 100));
  const rows = (costs || []).filter((c) => c && c.valid_from).map((c) => ({ ...c, a: ymdUtc(c.valid_from), b: c.valid_to ? ymdUtc(c.valid_to) : null }))
    .sort((x, y) => x.a - y.a);
  const segs = [];
  for (const r of rows) {
    const end = r.b == null ? to : r.b;
    if (end < from || r.a > to) continue;
    const kind = end < t ? 'past' : r.a > t ? 'fut' : 'now';
    const left = pct(Math.max(r.a, from));
    const right = pct(Math.min(end, to) + 1);
    if (right - left <= 0) continue;
    const label = kind === 'now' ? `${fmtDay(r.valid_from).replace(/ \(.\)$/, '')}〜 いま` : kind === 'past' ? `〜${fmtDay(ymdOf(end)).replace(/ \(.\)$/, '')}` : `${fmtDay(r.valid_from).replace(/ \(.\)$/, '')}〜`;
    segs.push({ kind, left, width: right - left, value: `${yen(r.cost_jpy)} 円`, label });
  }
  const months = [];
  for (let i = 0; i < 6; i++) {
    const d = Date.UTC(td.getUTCFullYear(), td.getUTCMonth() - 3 + i, 1) / 864e5;
    months.push({ left: pct(d), label: `${new Date(d * 864e5).getUTCMonth() + 1}月` });
  }
  return { segs, months, todayLeft: pct(t + 0.5), todayLabel: `今日 ${fmtDay(today)}`, hasFuture: segs.some((s) => s.kind === 'fut') };
}

/** 画面に渡す道具 (router の pageLocals から) */
export const ui = Object.freeze({ fmtJst, fmtDay, dayDiff, eventWords, actorWords, sourceWords, costTimeline, yen });
