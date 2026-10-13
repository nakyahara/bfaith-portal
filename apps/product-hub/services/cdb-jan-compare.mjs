/**
 * cdb-jan-compare.mjs — product-hub の JAN と Company DB の JAN を比べる印 (Company DB構想 21 §5 B1・読むだけ)
 *
 * なぜ: JAN は 10/5 から Company DB (core.external_ids の system = 'jan'・商品 (product) に付く・1 商品に最大 5 個) が正。
 *   product-hub は別に JAN を持ち (単品 = product_drafts.jan_code / まとまり = draft_sku_jans)、楽天の articleNumber に送っている (二重入力)。
 *   B1 は「比べて知らせる」だけ。B2 で「Company DB の JAN から選ぶ」に変える (中原さんの決定 3 = a)。
 * 🚨 書き込みは一切しない: Company DB は「読むだけの取引」(begin transaction read only) で読む・product-hub の SQLite も読むだけ。
 *   出品は止めない (B1 は業務の動きを変えない)。印は詳細画面・ボードのカード・楽天のプレビューと登録の確認に出す。
 * 比べる単位 (楽天に送る値と同じ・services/rakuten-listing.js の buildItemPayload と同じ決め方):
 *   単品 (まとまりでない) = 出品の商品コード (draft.ne_code) ↔ product_drafts.jan_code
 *   まとまり (NE の代表でまとまる子が 2 つ以上) = 子の SKU ごと ↔ draft_sku_jans (ページ代表の jan_code は楽天に送らないので比べない)
 * 印 (state):
 *   match       一致 (Company DB の有効な JAN のどれかと同じ。Company DB に複数あれば数も出す)
 *   differs     違う (Company DB にはこの商品の JAN があるが、product-hub の値はそのどれでもない)
 *   not_in_cdb  Company DB に無い (この商品に Company DB の JAN が 1 つも無い・セット / 例外は JAN を持てない)
 *   sku_missing Company DB に商品コードが無い (product-hub に JAN があるとき)
 *   ph_empty    product-hub は空・Company DB には JAN がある (知らせだけ)
 *   none        どちらにも無い (印は出さない)
 *   unreadable  Company DB を読めない (接続先が無い・時間切れ・失敗)
 *   どの印でも、product-hub の JAN がほかの商品の有効な JAN なら holder (その商品コード) を添える。
 * env: COMPANY_DB_URL (Render。無ければ COMPANY_DB_WATCH_URL) = cdb-tax-rate.mjs と同じ
 */
import { resolveVariationGroup } from '../lib/variation.js';

/** 印の見出し (画面・プレビューで同じ言葉を使う) */
export const JAN_STATE_LABELS = Object.freeze({
  match: '一致',
  differs: 'Company DB と違う',
  not_in_cdb: 'Company DB に無い',
  sku_missing: 'Company DB に商品コードが無い',
  ph_empty: 'product-hub は空',
  none: 'どちらにも無い',
  unreadable: '読めない',
});
/** 重さ: warn = 直す必要がありそう / muted = 比べられない / info = 知らせ / ok = 一致 / none = 何も出さない */
export const JAN_STATE_SEVERITY = Object.freeze({
  differs: 'warn', not_in_cdb: 'warn', sku_missing: 'warn', unreadable: 'muted', ph_empty: 'info', match: 'ok', none: 'none',
});
const SEVERITY_RANK = Object.freeze({ warn: 4, muted: 3, info: 2, ok: 1, none: 0 });
/** 1 回に読む (商品コード, JAN) の組の上限 (ボードの全部のカードでも今は 100 組ほど) */
export const JAN_READ_MAX_ITEMS = 5000;
/** 画面の要求の中で待つ上限 (楽天のプレビュー・登録の確認)。超えたら「読めない」 */
export const JAN_READ_WAIT_MS = 6000;

/**
 * Company DB の有効な JAN を読む SQL。入れた (商品コード, product-hub の JAN) の組ごとに 1 行 (ord の順)。
 *   jans   = その商品コードの商品 (core.skus.product_id) に付いた有効な JAN (valid_to is null) の全部 [{row, value, norm}]
 *   jan_norm = product-hub の JAN を Company DB と同じ決まり (core.norm_code) で正規化した値 (空なら null)
 *   jan_holder = product-hub の JAN が「ほかの商品」の有効な JAN なら、その商品の商品コード (無ければ null)
 */
export const JAN_READ_SQL = `select x.ord::int as ord, k.sku_id::text as sku_id, k.sku_kind, k.product_id::text as product_id,
    coalesce((select json_agg(json_build_object('row', e.external_id_row::text, 'value', e.external_value, 'norm', e.external_norm) order by e.external_value, e.external_id_row)
       from core.external_ids e
      where e.company_id = $1 and e.entity_type = 'product' and e.entity_id = k.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null), '[]'::json) as jans,
    case when x.jan <> '' then core.norm_code(x.jan) end as jan_norm,
    (select coalesce((select s.code from core.skus s where s.company_id = e.company_id and s.product_id = e.entity_id order by s.code_norm limit 1), e.entity_type || ' ' || e.entity_id::text)
       from core.external_ids e
      where x.jan <> '' and e.company_id = $1 and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null and e.external_norm = core.norm_code(x.jan)
        and not (e.entity_type = 'product' and e.entity_id is not distinct from k.product_id)
      order by e.external_id_row limit 1) as jan_holder
  from unnest($2::text[], $3::text[]) with ordinality as x(code, jan, ord)
  left join core.skus k on k.company_id = $1 and k.code_norm = core.norm_code(x.code)
 order by x.ord`;

const str = (v) => String(v ?? '').trim();
const pairKey = (code, jan) => `${code.toLowerCase()}\u0000${jan}`;

/**
 * 接続 (client.query(sql, params) → { rows } の形・pg / PGlite) で、読むだけの取引の中で JAN を読む。
 * items = [{ code, jan }] (重なりは呼ぶ側で除く)。戻り値 rows (items と同じ順)
 */
export async function queryCdbJans(client, companyId, items) {
  await client.query('begin transaction read only');
  try {
    const r = await client.query(JAN_READ_SQL, [companyId, items.map((i) => i.code), items.map((i) => i.jan)]);
    return r.rows;
  } finally {
    try { await client.query('rollback'); } catch { /* 読むだけ = 戻しても何も変わらない */ }
  }
}

let reader = null;
/** 試験用: 読み方を差し替える (本番では呼ばない)。fn(items) → { ok: true, rows } / { ok: false, reason } */
export function __setCdbJanReader(fn) { reader = fn || null; }

/**
 * (商品コード, JAN) の組をまとめて読む。戻り値 { ok: true, byPair: Map(pairKey → row) } / { ok: false, reason }
 */
export async function readCdbJans(pairs, { env = process.env } = {}) {
  const seen = new Set();
  const items = [];
  for (const p of pairs || []) {
    const code = str(p.code);
    if (!code) continue;
    const jan = str(p.jan);
    const k = pairKey(code, jan);
    if (seen.has(k)) continue;
    seen.add(k);
    items.push({ code, jan });
  }
  if (!items.length) return { ok: true, byPair: new Map() };
  if (items.length > JAN_READ_MAX_ITEMS) return { ok: false, reason: `比べる組が多すぎる (${items.length} 組・上限 ${JAN_READ_MAX_ITEMS})` };
  let rows;
  if (reader) {
    let r;
    try { r = await reader(items); } catch (e) { return { ok: false, reason: `Company DB を読めない (${String((e && e.message) || e).slice(0, 120)})` }; }
    if (!r || !r.ok) return { ok: false, reason: (r && r.reason) || 'Company DB を読めない' };
    rows = r.rows || [];
  } else {
    const url = str(env.COMPANY_DB_URL) || str(env.COMPANY_DB_WATCH_URL);
    if (!url) return { ok: false, reason: 'Company DB の接続先が無い' };
    let client = null;
    try {
      const { openPgClient } = await import('../../../scripts/company-db/migrate.mjs');
      const { COMPANY_ID } = await import('../../../lib/master-write.mjs');
      client = await openPgClient(url, { application_name: 'product-hub-cdb-jan', connectionTimeoutMillis: 5000, statement_timeout: 5000, query_timeout: 6000 });
      rows = await queryCdbJans(client, COMPANY_ID, items);
    } catch (e) {
      return { ok: false, reason: `Company DB を読めない (${String((e && e.message) || e).slice(0, 120)})` };
    } finally {
      if (client) { try { await client.end(); } catch { /* 結果は変えない */ } }
    }
  }
  const byPair = new Map();
  for (const row of rows) {
    const it = items[Number(row.ord) - 1];
    if (it) byPair.set(pairKey(it.code, it.jan), row);
  }
  return { ok: true, byPair };
}

/** 1 つの単位 (商品コード ↔ product-hub の JAN) の印を決める。read = readCdbJans の戻り値 */
export function judgeJanUnit({ code, phJan }, read) {
  const c = str(code);
  const j = str(phJan);
  const base = { code: c, phJan: j, cdbJans: [], cdbCount: 0, holder: null };
  const done = (state, detail) => ({ ...base, state, label: JAN_STATE_LABELS[state], severity: JAN_STATE_SEVERITY[state], detail: detail || '' });
  if (!read || !read.ok) return done('unreadable', (read && read.reason) || 'Company DB を読めない');
  const row = read.byPair.get(pairKey(c, j));
  if (!row) return done('unreadable', 'Company DB の答えにこの商品コードが無い');
  let jans = row.jans;
  if (typeof jans === 'string') { try { jans = JSON.parse(jans); } catch { jans = null; } }
  if (!Array.isArray(jans)) return done('unreadable', 'Company DB の JAN の形が違う');
  base.cdbJans = jans.map((x) => String(x.value));
  base.cdbCount = jans.length;
  base.holder = row.jan_holder == null ? null : String(row.jan_holder);
  const holderNote = base.holder ? `この JAN は Company DB ではほかの商品 (${base.holder}) の JAN` : '';
  const list = base.cdbJans.join('・');
  if (row.sku_id == null) {
    return j ? done('sku_missing', ['Company DB にこの商品コードが無いので JAN を確かめられない', holderNote].filter(Boolean).join('。')) : done('none');
  }
  if (!j) {
    if (base.cdbCount === 0) return done('none');
    return done('ph_empty', base.cdbCount === 1 ? `Company DB の JAN = ${list}` : `Company DB に JAN が ${base.cdbCount} 個 (${list})`);
  }
  if (row.product_id == null) {
    return done('not_in_cdb', [`Company DB では${row.sku_kind === 'set' ? 'セット' : '例外の商品'}は JAN を持たない`, holderNote].filter(Boolean).join('。'));
  }
  const norm = row.jan_norm == null ? null : String(row.jan_norm);
  if (norm && jans.some((x) => String(x.norm) === norm)) {
    return done('match', base.cdbCount > 1 ? `Company DB に JAN が ${base.cdbCount} 個 (${list}) — そのうちの 1 つ` : '');
  }
  if (base.cdbCount === 0) return done('not_in_cdb', ['Company DB のこの商品に JAN が 1 つも無い', holderNote].filter(Boolean).join('。'));
  return done('differs', [`Company DB の JAN = ${list}`, holderNote].filter(Boolean).join('。'));
}

/**
 * ドラフトの比べる単位 (楽天に送る値と同じ決め方)。戻り値 { mode: 'single' | 'variation', units: [{ code, phJan }] }
 * db = product-hub の SQLite (読むだけ)
 */
export function janUnitsForDraft(db, draft) {
  const vari = resolveVariationGroup(db, draft.ne_code, { draftId: draft.id, withMembers: true });
  if (vari.kind === 'variation' && vari.memberCount > 1) {
    const janRows = new Map(db.prepare('SELECT sku_code, jan_code FROM draft_sku_jans WHERE draft_id = ?')
      .all(draft.id).map((r) => [String(r.sku_code), String(r.jan_code)]));
    return {
      mode: 'variation',
      units: vari.members.map((m) => {
        const code = str(m.商品コード);
        return { code, phJan: str(janRows.get(code.toLowerCase())) };
      }).filter((u) => u.code),
    };
  }
  return { mode: 'single', units: [{ code: str(draft.ne_code), phJan: str(draft.jan_code) }].filter((u) => u.code) };
}

/** 単位の印からドラフトのまとめ (一番重い印・数) を作る */
export function summarizeJanUnits(units) {
  const counts = {};
  let worst = null;
  for (const u of units) {
    counts[u.state] = (counts[u.state] || 0) + 1;
    if (!worst || SEVERITY_RANK[u.severity] > SEVERITY_RANK[worst.severity]) worst = u;
  }
  if (!worst) return { state: 'none', severity: 'none', label: '', counts };
  const n = counts[worst.state];
  const label = units.length > 1 && n ? `${JAN_STATE_LABELS[worst.state]} (${n} SKU)` : JAN_STATE_LABELS[worst.state];
  return { state: worst.state, severity: worst.severity, label, counts };
}

/** 注意の文 (楽天のプレビュー・登録の確認に出す)。warn と muted の単位だけ */
export function janWarningsOf(result) {
  if (!result) return [];
  if (!result.ok || result.readable === false) return [`JAN を Company DB と比べられませんでした (${result.reason})。出品は止めません`];
  const out = [];
  for (const u of result.units) {
    if (u.severity !== 'warn' && u.severity !== 'muted') continue;
    out.push(`JAN ${u.label}: ${u.code}${u.phJan ? ` の JAN ${u.phJan}` : ''}${u.detail ? ` — ${u.detail}` : ''}。直すのは商品マスタ (master-edit) の JAN です (この印は知らせだけ・出品は止めません)`);
  }
  return out;
}

/**
 * ドラフト 1 つを比べる。戻り値 { ok: true, mode, units: [...], summary } / { ok: false, reason, mode, units: [] }
 *   Company DB を読めないときも ok: true で、各単位が unreadable になる (読めないのも印)
 */
export async function compareDraftJans(db, draft, { env = process.env } = {}) {
  const { mode, units } = janUnitsForDraft(db, draft);
  if (!units.length) return { ok: true, mode, units: [], summary: summarizeJanUnits([]) };
  const read = await readCdbJans(units.map((u) => ({ code: u.code, jan: u.phJan })), { env });
  const judged = units.map((u) => judgeJanUnit(u, read));
  return { ok: true, mode, readable: read.ok, reason: read.ok ? null : read.reason, units: judged, summary: summarizeJanUnits(judged) };
}

/** 待つ上限つき (画面の要求の中で使う)。時間切れ = 読めない */
export async function compareDraftJansWithin(db, draft, { env = process.env, waitMs = JAN_READ_WAIT_MS } = {}) {
  let timer;
  try {
    return await Promise.race([
      compareDraftJans(db, draft, { env }),
      new Promise((resolve) => { timer = setTimeout(() => resolve({ ok: false, reason: `${Math.round(waitMs / 1000)} 秒で読めなかった`, units: [] }), waitMs); }),
    ]);
  } catch (e) {
    return { ok: false, reason: String((e && e.message) || e).slice(0, 160), units: [] };
  } finally { clearTimeout(timer); }
}

/**
 * ボード: product-hub に JAN があるドラフト全部を 1 回で読んで、直す必要がありそうな印 (warn) のドラフトだけ返す。
 * 戻り値 { ok: true, readable, reason, checked, byDraft: { [draftId]: { state, label, units: [{ code, phJan, label, detail }] } } }
 */
export async function boardJanMarks(db, { env = process.env } = {}) {
  const drafts = db.prepare(`SELECT * FROM product_drafts d
    WHERE TRIM(COALESCE(d.jan_code, '')) <> '' OR EXISTS (SELECT 1 FROM draft_sku_jans j WHERE j.draft_id = d.id)
    ORDER BY d.id`).all();
  const per = [];
  for (const d of drafts) {
    const { units } = janUnitsForDraft(db, d);
    const withJan = units.filter((u) => u.phJan);
    if (withJan.length) per.push({ id: d.id, units: withJan });
  }
  const read = await readCdbJans(per.flatMap((p) => p.units.map((u) => ({ code: u.code, jan: u.phJan }))), { env });
  if (!read.ok) return { ok: true, readable: false, reason: read.reason, checked: per.length, byDraft: {} };
  const byDraft = {};
  for (const p of per) {
    const judged = p.units.map((u) => judgeJanUnit(u, read));
    const warn = judged.filter((u) => u.severity === 'warn');
    if (!warn.length) continue;
    const s = summarizeJanUnits(warn);
    byDraft[p.id] = { state: s.state, label: s.label, units: warn.map((u) => ({ code: u.code, phJan: u.phJan, label: u.label, detail: u.detail })) };
  }
  return { ok: true, readable: true, reason: null, checked: per.length, byDraft };
}
