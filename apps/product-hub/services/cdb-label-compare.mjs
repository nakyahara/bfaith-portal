/**
 * cdb-label-compare.mjs — product-hub のページ表記・楽天の型番と、商品マスタ (Company DB) の表示の項目を比べる印 (Company DB構想 27 ④-2・§3.4 = v2・読むだけ)
 *
 * なぜ: 原産国・内容量・メーカー型番・消費期限の目安は 2026-10-18 から Company DB が正本 (マスタの入力で入れる・core.v_product_label_facts)。
 *   product-hub は今までどおり自分の欄 (ページ表記 draft_page_info・楽天の draft_rakuten.article_number) を持って楽天に出す。
 *   ここは「比べて知らせる」+「マスタの値を入れる」の材料を返すだけ (中原さんの決定 Q6 = a: 黙って上書きしない・人がボタンを押す)。
 * 🚨 書き込みは一切しない: Company DB は読むだけの取引 (cdb-jan-compare.mjs と同じ接続先・期限・同時の上限)・product-hub の SQLite も読むだけ。出品は止めない。
 * 比べる単位 (JAN と同じ決め方 = janUnitsForDraft):
 *   単品 = 出品の商品コード (draft.ne_code) の商品の値 ↔ ページ表記の原産国・内容量・楽天の型番
 *   まとまり (子が 2 つ以上) = 子ごとの商品の値を並べる。原産国・内容量は全部の子で同じ (人が入れた値) ときだけ「入れる」。型番は子ごと (参考だけ)
 *   セット (Company DB の区分 = set) = 商品の行を持たない = 原産国だけ、構成品が全部読めて全部に人の原産国があり全部同じときだけ参考に (Q8・内容量と型番は出さない)
 * 印 (state) は項目ごと:
 *   match 一致 / differs 違う (商品マスタの人の値と product-hub の値が違う) / ph_empty product-hub は空 (マスタにはある) /
 *   master_empty 商品マスタに無い (人の値が無い) / not_comparable 比べられない (ml / g / 枚 / 個 に直せない内容量・海外製で国名が空) /
 *   from_ph マスタの値が夜間の取り込み (product-hub 自身などの観測) = 自分と比べない (印なし)
 * 比べ方は lib/master-label.mjs と同じ (countryNorm・netEqual・phCountryOf…) = マスタの入力の画面と同じ決まり
 */
import { janReadUrlFrom, readCdbRowsWithin, janUnitsForDraft, JAN_READ_DEADLINE_MS, JAN_READ_MIN_LEFT_MS } from './cdb-jan-compare.mjs';
import { countryNorm, phCountryOf, phCountryFields, netFromText, netToMaster, netText, numText, sourceWords, phModelOf } from '../../../lib/master-label.mjs';

export const LABEL_STATE_WORDS = Object.freeze({
  match: '一致', differs: '商品マスタと違う', ph_empty: 'product-hub は空', master_empty: '商品マスタに無い', not_comparable: '比べられない', from_ph: '',
});
export const LABEL_STATE_SEVERITY = Object.freeze({ differs: 'warn', ph_empty: 'info', not_comparable: 'muted', master_empty: 'none', match: 'ok', from_ph: 'none' });

/**
 * 読む SQL: 商品コードごとに SKU・区分・商品・いま使う値 (ビュー)・セットなら構成品の原産国 (人の値) を 1 行。
 *   to_regclass でビューが無い DB (0090 の前) を先に見る = ready false の 1 行だけ返す (呼び手は「まだ」と出す)
 */
export const LABEL_READY_SQL = "select to_regclass('core.v_product_label_facts') is not null as ok";
export const LABEL_READ_SQL = `select x.ord::int as ord, k.sku_id::text as sku_id, k.sku_kind, k.product_id::text as product_id,
    v.country_of_origin, v.country_source, v.net_content::text as net_content, v.net_content_uom, v.net_source, v.model_number, v.model_source, v.shelf_life_days, v.shelf_source,
    case when k.sku_kind = 'set' then coalesce((select json_agg(json_build_object('code', c2.code, 'country', v2.country_of_origin, 'source', v2.country_source) order by c.sort_order, c2.code_norm)
       from core.sku_components c join core.skus c2 on c2.sku_id = c.child_sku_id left join core.v_product_label_facts v2 on v2.product_id = c2.product_id
      where c.parent_sku_id = k.sku_id), '[]'::json) end as comps
  from unnest($2::text[]) with ordinality as x(code, ord)
  left join core.skus k on k.company_id = $1 and k.code_norm = core.norm_code(x.code)
  left join core.v_product_label_facts v on v.product_id = k.product_id
 order by x.ord`;

let reader = null;
/** 試験用: 読み方を丸ごと差し替える (本番では呼ばない)。fn(codes) → { ok: true, ready, rows } / { ok: false, reason } */
export function __setCdbLabelReader(fn) { reader = fn || null; }

/** 接続 (pg / PGlite の query) で、読むだけの取引の中で読む。戻り値 { ready, rows } */
export async function queryCdbLabels(client, companyId, codes, { statementTimeoutMs = null } = {}) {
  await client.query('begin transaction read only');
  try {
    if (statementTimeoutMs != null) await client.query(`select set_config('statement_timeout', $1, true)`, [`${Math.max(1, Math.floor(statementTimeoutMs))}ms`]);
    if (!(await client.query(LABEL_READY_SQL)).rows[0].ok) return { ready: false, rows: [] };
    return { ready: true, rows: (await client.query(LABEL_READ_SQL, [companyId, codes])).rows };
  } finally {
    try { await client.query('rollback'); } catch { /* 読むだけ = 戻しても何も変わらない */ }
  }
}

/** 商品コードの並びを読む。戻り値 { ok: true, ready, byCode: Map(小文字のコード → 行) } / { ok: false, reason } */
export async function readCdbLabels(codes, { env = process.env, deadlineMs = JAN_READ_DEADLINE_MS, deadlineAt = null } = {}) {
  const list = [...new Set((codes || []).map((c) => String(c ?? '').trim()).filter(Boolean))];
  if (!list.length) return { ok: true, ready: true, byCode: new Map() };
  const left = deadlineAt != null ? Math.min(deadlineMs, deadlineAt - Date.now()) : deadlineMs;
  if (deadlineAt != null && left < JAN_READ_MIN_LEFT_MS) return { ok: false, reason: '時間切れ (読む前に期限が来た)' };
  let got;
  if (reader) {
    try { got = await reader(list); } catch (e) { got = { ok: false, reason: `Company DB を読めない (${String((e && e.message) || e).slice(0, 120)})` }; }
    if (!got || !got.ok) return { ok: false, reason: (got && got.reason) || 'Company DB を読めない' };
  } else {
    if (!janReadUrlFrom(env).url) return { ok: false, reason: 'Company DB の接続先が無い' };
    let ready = true;
    const r = await readCdbRowsWithin(async (c, companyId, ms) => { const x = await queryCdbLabels(c, companyId, list, { statementTimeoutMs: Math.max(100, ms) }); ready = x.ready; return x.rows; },
      { env, deadlineMs: left, appName: 'product-hub-cdb-label' });
    if (!r.ok) return r;
    got = { ok: true, ready, rows: r.rows };
  }
  const byCode = new Map();
  for (const row of got.rows || []) { const it = list[Number(row.ord) - 1]; if (it) byCode.set(it.toLowerCase(), row); }
  return { ok: true, ready: got.ready !== false, byCode };
}

// ─── 比べる ───
const str = (v) => String(v ?? '').trim();
/** ビューの 1 行 → 商品マスタの値 { country, net, model, shelf } (それぞれ { value / num・uom・text, source, human }) */
export function masterOf(row) {
  const one = (val, src) => (val == null || val === '' ? null : { value: String(val), source: src, human: src === 'human', words: sourceWords(src) });
  if (!row) return { country: null, net: null, model: null, shelf: null };
  return {
    country: one(row.country_of_origin, row.country_source),
    net: row.net_content == null ? null : { num: numText(row.net_content), uom: row.net_content_uom || '', text: netText(row.net_content, row.net_content_uom), source: row.net_source,
      human: row.net_source === 'human', words: sourceWords(row.net_source), master: netToMaster(row.net_content, row.net_content_uom) },
    model: one(row.model_number, row.model_source),
    shelf: row.shelf_life_days == null ? null : { value: Number(row.shelf_life_days), source: row.shelf_source, human: row.shelf_source === 'human', words: sourceWords(row.shelf_source) },
  };
}
const judge = (state, extra = {}) => ({ state, label: LABEL_STATE_WORDS[state], severity: LABEL_STATE_SEVERITY[state], ...extra });
/** 原産国: m = 商品マスタ (人の値か null)・pi = ページ表記 */
export function judgeCountry(m, pi) {
  const p = phCountryOf(pi);
  const phText = [str(pi?.origin_type), str(pi?.origin_country)].filter(Boolean).join(' ');
  const base = { master: m ? m.value : null, masterWords: m ? m.words : '', ph: phText, put: m && m.human ? phCountryFields(m.value) : null };
  if (!m) return judge('master_empty', base);
  if (!m.human) return judge('from_ph', { ...base, put: null });
  if (p.state === 'empty') return judge('ph_empty', base);
  if (p.state === 'no_country') return judge('not_comparable', { ...base, detail: 'product-hub は「海外製」で国名が空' });
  return judge(countryNorm(p.value) === countryNorm(m.value) ? 'match' : 'differs', base);
}
/** 内容量: ページ表記の文字 (50ml など) を夜間ロードと同じ決まりで読む。ml / g / 枚 / 個 に直せない = 比べない */
export function judgeNet(m, text) {
  const t = str(text);
  const base = { master: m ? m.text : null, masterWords: m ? m.words : '', ph: t, put: m && m.human ? `${m.num}${m.uom}` : null };
  if (!m) return judge('master_empty', base);
  if (!m.human) return judge('from_ph', { ...base, put: null });
  if (!t) return judge('ph_empty', base);
  const p = netFromText(t);
  if (!p || !p.master) return judge('not_comparable', { ...base, detail: 'product-hub の内容量を ml / g / 枚 / 個 にちょうど直せない' });
  const mm = netToMaster(m.num, m.uom);
  return judge(mm && mm.uom === p.master.uom && mm.num === p.master.num ? 'match' : 'differs', base);
}
/** メーカー型番 (前後の空白だけ外して比べる) */
export function judgeModel(m, value) {
  const v = str(value);
  const base = { master: m ? m.value : null, masterWords: m ? m.words : '', ph: v, put: m && m.human ? m.value : null };
  if (!m) return judge('master_empty', base);
  if (!m.human) return judge('from_ph', { ...base, put: null });
  if (!v) return judge('ph_empty', base);
  return judge(v === m.value ? 'match' : 'differs', base);
}

/**
 * ドラフト 1 つの印 (詳細画面)。db = product-hub の SQLite (読むだけ)。戻り値:
 *   { ok: true, readable, ready, reason, mode: 'single' | 'variation' | 'set' | null, units: [{ code, kind, master }], items: { country, net, model, shelf } | null,
 *     set: { country, codes, note } | null, kids: [{ code, country, net, model }] | null, summary: { severity, label } }
 */
export async function compareDraftLabels(db, draft, { env = process.env, deadlineAt = null } = {}) {
  let ju;
  try { ju = janUnitsForDraft(db, draft); } catch (e) { return { ok: false, reason: String((e && e.message) || e).slice(0, 160) }; }
  const pi = db.prepare('SELECT origin_type, origin_country, content_volume FROM draft_page_info WHERE draft_id = ?').get(draft.id) || null;
  const codes = ju.units.map((u) => u.code);
  const empty = (readable, reason, ready = true) => ({ ok: true, readable, ready, reason, mode: null, units: [], items: null, set: null, kids: null, summary: { severity: readable ? 'none' : 'muted', label: readable ? '' : '読めない' } });
  if (!codes.length) return empty(true, null);
  const read = await readCdbLabels(codes, { env, deadlineAt });
  if (!read.ok) return empty(false, read.reason);
  if (!read.ready) return { ...empty(true, 'Company DB に表示の項目 (0090) がまだ入っていません', false), summary: { severity: 'none', label: '' } };
  const rowOf = (c) => read.byCode.get(String(c).toLowerCase()) || null;
  const units = codes.map((c) => { const r = rowOf(c); return { code: c, kind: r ? r.sku_kind : null, missing: !r || r.sku_id == null, master: masterOf(r) }; });
  let items = null; let set = null; let kids = null; let mode = ju.mode;
  if (ju.mode === 'single' && units[0].kind === 'set') {
    // セット: 構成品が全部読めて、全部に人の原産国があり、全部同じときだけ参考に (Q8)
    mode = 'set';
    let comps = rowOf(codes[0]).comps;
    if (typeof comps === 'string') { try { comps = JSON.parse(comps); } catch { comps = null; } }
    comps = Array.isArray(comps) ? comps : [];
    const all = comps.length > 0 && comps.every((c) => c.country && c.source === 'human');
    const vals = new Set(comps.map((c) => countryNorm(c.country)).filter(Boolean));
    const same = all && vals.size === 1 ? [...vals][0] : null;
    const pc = phCountryOf(pi);
    set = {
      country: same, codes: comps.map((c) => c.code), put: same ? phCountryFields(same) : null,
      state: !same ? null : pc.state === 'empty' ? 'ph_empty' : pc.state === 'no_country' ? 'not_comparable' : countryNorm(pc.value) === same ? 'match' : 'differs',
      note: !comps.length ? 'Company DB にこのセットの構成がありません' : !all ? '原産国が入っていない構成品があります (参考も出しません)' : !same ? '構成品で原産国が違います (参考も出しません)' : '',
    };
  } else if (ju.mode === 'single') {
    const m = units[0].master;
    // 型番は楽天に出す値と同じ決め方 (SKU ごとの型番の行があればそれ・Codex R1 M1)。SKU ごとの行がある = ページの欄に入れても楽天の値は変わらない = 「入れる」を出さない
    const pm = phModelOf(db, draft.id, codes[0]);
    const model = judgeModel(m.model, pm.value);
    if (pm.from === 'sku') Object.assign(model, { put: null, detail: 'SKU ごとの型番の表の値 (直すときはカテゴリ・属性タブの表で)' });
    items = { country: judgeCountry(m.country, pi), net: judgeNet(m.net, pi?.content_volume), model, shelf: m.shelf };
  } else {
    // まとまり: 子ごとに並べる。原産国・内容量は全部の子で同じ人の値のときだけページの欄と比べて「入れる」を出す。型番は子ごとの参考だけ
    kids = units.map((u) => ({ code: u.code, missing: u.missing, country: u.master.country, net: u.master.net, model: u.master.model, shelf: u.master.shelf }));
    const sameOf = (pick, key) => {
      const vals = kids.map((k) => k[pick]);
      if (!vals.length || vals.some((x) => !x || !x.human)) return null;
      const keys = new Set(vals.map(key));
      return keys.size === 1 ? vals[0] : null;
    };
    const c = sameOf('country', (x) => countryNorm(x.value));
    const n = sameOf('net', (x) => (x.master ? `${x.master.num}|${x.master.uom}` : `raw:${x.text}`));
    items = {
      country: c ? judgeCountry(c, pi) : { ...judge('master_empty', { master: null, ph: [str(pi?.origin_type), str(pi?.origin_country)].filter(Boolean).join(' '), put: null }), detail: '子で違うか、入っていない子があります' },
      net: n ? judgeNet(n, pi?.content_volume) : { ...judge('master_empty', { master: null, ph: str(pi?.content_volume), put: null }), detail: '子で違うか、入っていない子があります' },
      model: null, shelf: null,
    };
  }
  const sev = { warn: 3, info: 2, muted: 1, ok: 0, none: 0 };
  const list = items ? [items.country, items.net, items.model].filter(Boolean) : set && set.state ? [{ state: set.state, severity: LABEL_STATE_SEVERITY[set.state], label: LABEL_STATE_WORDS[set.state] }] : [];
  const worst = list.reduce((a, x) => (sev[x.severity] > sev[a.severity] ? x : a), { severity: 'none', label: '' });
  return { ok: true, readable: true, ready: true, reason: null, mode, units, items, set, kids, summary: { severity: worst.severity, label: worst.severity === 'none' || worst.severity === 'ok' ? '' : worst.label } };
}
