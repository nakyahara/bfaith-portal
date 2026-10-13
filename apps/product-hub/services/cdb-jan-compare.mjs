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
 * 接続 (janReadUrlFrom): COMPANY_DB_SCREEN_READ_URL (画面用の読むだけのロール・今は Render に無い = 足すならこの名前) → 無ければ COMPANY_DB_URL
 *   (Render の画面が今使っている接続。読むだけの取引 + 取引の中の statement_timeout で読む)。
 *   🚨 見張りの watcher (COMPANY_DB_WATCH_URL・接続 3 本まで) には落ちない (lib/master-legacy-gate.mjs の phaseUrlFrom と同じ決まり)
 * 時間: どの道も JAN_READ_DEADLINE_MS (4.5 秒) まで。超えたら接続を閉じて (= 裏の SELECT も止まる)「読めない」
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
/** 1 回の SELECT で読む (商品コード, JAN) の組の数 (超える分は同じ読むだけの取引の中で分けて読む) */
export const JAN_READ_CHUNK = 1000;
/** 1 回に読む組の上限 (安全弁。ボードの画面のカードは 800 枚まで = 普通は届かない) */
export const JAN_READ_MAX_ITEMS = 20000;
/** 読むのに使ってよい時間 (接続 + SELECT)。超えたら接続を閉じて「読めない」。画面の要求は 5 秒以内に返す */
export const JAN_READ_DEADLINE_MS = 4500;
/** 接続を待つ上限 (残りは SELECT に) */
export const JAN_CONNECT_TIMEOUT_MS = 2500;

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
      where x.jan <> '' and e.company_id = $1 and e.entity_type = 'product' and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null and e.external_norm = core.norm_code(x.jan)
        and e.entity_id is distinct from k.product_id
      order by e.external_id_row limit 1) as jan_holder
  from unnest($2::text[], $3::text[]) with ordinality as x(code, jan, ord)
  left join core.skus k on k.company_id = $1 and k.code_norm = core.norm_code(x.code)
 order by x.ord`;

const str = (v) => String(v ?? '').trim();
const pairKey = (code, jan) => `${code.toLowerCase()}\u0000${jan}`;

/** 読む接続の URL を選ぶ。{ url, source } (url が無ければ null) */
export function janReadUrlFrom(env = process.env) {
  const ro = str(env.COMPANY_DB_SCREEN_READ_URL);
  if (ro) return { url: ro, source: 'COMPANY_DB_SCREEN_READ_URL' };
  const owner = str(env.COMPANY_DB_URL);
  if (owner) return { url: owner, source: 'COMPANY_DB_URL' };
  return { url: null, source: null };
}

/**
 * 接続 (client.query(sql, params) → { rows } の形・pg / PGlite) で、読むだけの取引の中で JAN を読む。
 * items = [{ code, jan }] (重なりは呼ぶ側で除く)。戻り値 rows (items と同じ順・ord は items の 1 から)。
 * statementTimeoutMs = 取引の中だけの statement_timeout (set_config の local = rollback で元に戻る)
 */
export async function queryCdbJans(client, companyId, items, { statementTimeoutMs = null } = {}) {
  await client.query('begin transaction read only');
  try {
    if (statementTimeoutMs != null) await client.query(`select set_config('statement_timeout', $1, true)`, [`${Math.max(1, Math.floor(statementTimeoutMs))}ms`]);
    const out = [];
    for (let off = 0; off < items.length; off += JAN_READ_CHUNK) {
      const part = items.slice(off, off + JAN_READ_CHUNK);
      const r = await client.query(JAN_READ_SQL, [companyId, part.map((i) => i.code), part.map((i) => i.jan)]);
      for (const row of r.rows) out.push({ ...row, ord: Number(row.ord) + off });
    }
    return out;
  } finally {
    try { await client.query('rollback'); } catch { /* 読むだけ = 戻しても何も変わらない */ }
  }
}

let reader = null;
/** 試験用: 読み方を丸ごと差し替える (本番では呼ばない)。fn(items) → { ok: true, rows } / { ok: false, reason } */
export function __setCdbJanReader(fn) { reader = fn || null; }
let connector = null;
/** 試験用: 接続の作り方を差し替える (本番では呼ばない)。fn(url, opts) → client ({ query, end }) */
export function __setCdbJanConnect(fn) { connector = fn || null; }

const errText = (e) => String((e && e.message) || e).slice(0, 120);

/** 期限つきで (接続して) 読む。期限が来たら接続を閉じて (= 走っている SELECT も止まる)「読めない」 */
async function readRowsWithin(items, { env, deadlineMs }) {
  if (reader) {
    let timer;
    try {
      const r = await Promise.race([reader(items), new Promise((resolve) => { timer = setTimeout(() => resolve({ ok: false, reason: `${(deadlineMs / 1000).toFixed(1)} 秒で読めなかった` }), deadlineMs); })]);
      if (!r || !r.ok) return { ok: false, reason: (r && r.reason) || 'Company DB を読めない' };
      return { ok: true, rows: r.rows || [] };
    } catch (e) {
      return { ok: false, reason: `Company DB を読めない (${errText(e)})` };
    } finally { clearTimeout(timer); }
  }
  const { url } = janReadUrlFrom(env);
  if (!url) return { ok: false, reason: 'Company DB の接続先が無い' };
  const started = Date.now();
  let client = null;
  let timedOut = false;
  let timer;
  const closeClient = () => { const c = client; client = null; if (c) Promise.resolve().then(() => c.end()).catch(() => { /* 結果は変えない */ }); };
  const work = (async () => {
    try {
      const connect = connector || (await import('../../../scripts/company-db/migrate.mjs')).openPgClient;
      const { COMPANY_ID } = await import('../../../lib/master-write.mjs');
      const c = await connect(url, { application_name: 'product-hub-cdb-jan', connectionTimeoutMillis: Math.min(JAN_CONNECT_TIMEOUT_MS, deadlineMs), statement_timeout: deadlineMs, query_timeout: deadlineMs });
      if (timedOut) { try { await c.end(); } catch { /* */ } return { ok: false, reason: 'timeout' }; }
      client = c;
      const left = deadlineMs - (Date.now() - started);
      const rows = await queryCdbJans(c, COMPANY_ID, items, { statementTimeoutMs: Math.max(100, left) });
      return { ok: true, rows };
    } catch (e) {
      return { ok: false, reason: `Company DB を読めない (${errText(e)})` };
    } finally { closeClient(); }
  })();
  try {
    return await Promise.race([work, new Promise((resolve) => {
      timer = setTimeout(() => { timedOut = true; closeClient(); resolve({ ok: false, reason: `${(deadlineMs / 1000).toFixed(1)} 秒で読めなかった (接続を閉じた)` }); }, deadlineMs);
    })]);
  } finally { clearTimeout(timer); }
}

/**
 * (商品コード, JAN) の組をまとめて読む。戻り値 { ok: true, byPair: Map(pairKey → row) } / { ok: false, reason }
 */
export async function readCdbJans(pairs, { env = process.env, deadlineMs = JAN_READ_DEADLINE_MS } = {}) {
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
  const got = await readRowsWithin(items, { env, deadlineMs });
  if (!got.ok) return got;
  const rows = got.rows;
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

/** 比べる単位 (janUnitsForDraft の戻り値) を Company DB と比べる。読めないときも ok: true で各単位が unreadable (読めないのも印)。投げない */
export async function compareJanUnits({ mode, units }, { env = process.env, deadlineMs = JAN_READ_DEADLINE_MS } = {}) {
  try {
    if (!units.length) return { ok: true, mode, readable: true, reason: null, units: [], summary: summarizeJanUnits([]) };
    const read = await readCdbJans(units.map((u) => ({ code: u.code, jan: u.phJan })), { env, deadlineMs });
    const judged = units.map((u) => judgeJanUnit(u, read));
    return { ok: true, mode, readable: read.ok, reason: read.ok ? null : read.reason, units: judged, summary: summarizeJanUnits(judged) };
  } catch (e) {
    return { ok: false, mode, reason: String((e && e.message) || e).slice(0, 160), units: [] };
  }
}

/** 今の product-hub の値 (ドラフトを読み直す) で比べる単位を作る。🚨 呼ぶ側は payload を作った直後に await を挟まずに呼ぶ (同じ値で比べる) */
export function janUnitsForDraftId(db, draftId) {
  const d = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  return d ? janUnitsForDraft(db, d) : { mode: null, units: [] };
}

/**
 * ドラフト 1 つを比べる (詳細画面)。戻り値 { ok: true, mode, readable, reason, units: [...], summary } / { ok: false, reason, units: [] }
 * 時間は JAN_READ_DEADLINE_MS まで (超えたら接続を閉じて「読めない」)
 */
export async function compareDraftJans(db, draft, { env = process.env, deadlineMs = JAN_READ_DEADLINE_MS } = {}) {
  let units;
  try { units = janUnitsForDraft(db, draft); } catch (e) { return { ok: false, reason: String((e && e.message) || e).slice(0, 160), units: [] }; }
  return compareJanUnits(units, { env, deadlineMs });
}

/** ボードの画面から受け取るカードの ID の上限 (ボードの表示の上限 800 + 余裕) */
export const BOARD_JAN_MAX_IDS = 1000;
/** ボードの画面から来た ID の並び ("1,2,3") を数の配列に。形が違う・多すぎる = null */
export function parseBoardDraftIds(raw) {
  const t = String(raw ?? '').trim();
  if (!t) return [];
  if (!/^[0-9]{1,12}(,[0-9]{1,12})*$/.test(t)) return null;
  const ids = [...new Set(t.split(',').map(Number).filter((n) => Number.isSafeInteger(n) && n > 0))];
  return ids.length > BOARD_JAN_MAX_IDS ? null : ids;
}

/**
 * ボード: 画面に出ているカード (ids) のうち、product-hub に JAN があるドラフトだけを 1 回で読み、直す必要がありそうな印 (warn) のドラフトだけ返す。
 * 戻り値 { ok: true, readable, reason, checked, byDraft: { [draftId]: { state, label, units: [{ code, phJan, label, detail }] } } }
 */
export async function boardJanMarks(db, { ids = [], env = process.env, deadlineMs = JAN_READ_DEADLINE_MS } = {}) {
  const per = [];
  const pick = db.prepare(`SELECT * FROM product_drafts d WHERE d.id = ?
    AND (TRIM(COALESCE(d.jan_code, '')) <> '' OR EXISTS (SELECT 1 FROM draft_sku_jans j WHERE j.draft_id = d.id))`);
  for (const id of ids) {
    const d = pick.get(id);
    if (!d) continue;
    const { units } = janUnitsForDraft(db, d);
    const withJan = units.filter((u) => u.phJan);
    if (withJan.length) per.push({ id: d.id, units: withJan });
  }
  const read = await readCdbJans(per.flatMap((p) => p.units.map((u) => ({ code: u.code, jan: u.phJan }))), { env, deadlineMs });
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
