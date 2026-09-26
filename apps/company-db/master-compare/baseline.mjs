/**
 * baseline.mjs — 照合 ② の「最後に一致した値」(D2。Company DB構想 10 §6.1.1「D2 最後に一致した値の契約 v2」。Codex D2-R0・R1)
 *
 * 切替の前から、NE と Company DB が「D2 の意味で」一致した値を単位 (SKU × 列・構成は親ごとの集合) ごとに貯める。
 * 切替の後に、差が「CDB 側が変わった (to_ne)」「NE 側で変わった (ne_changed)」「両方 (conflict)」かを見分ける基準にする。
 * 🚨 切替の前は影運転: ② の分類・verdict・W13:ne・D1 の判断は変えない (方向は ne.baseline 節に付けて数えるだけ)
 * 🚨 D2 の意味の一致は ② の comparability / eqv と別 (0 と空は同じ「値なし」・構成は子 × 数量の完全な集合)。両側に同じ正規化をかけ、正準な JSON で比べる
 * 読む = 照合の読み取りの取引 (watcher・savepoint) / 書く = 取引の後に watch_writer で ops.record_ne_baseline (関数だけ)
 */
/** 正規化の版 (規則を変えたら上げる。0033 の関数が受け付ける版も同じ migration で上げる) */
export const BASELINE_NORM_VERSION = 1;
export const BASELINE_COLS = Object.freeze(['exists', 'kind', 'name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier', 'components']);
/** NE の取得と CDB の読みの差の上限 (これを超える回は観測の組が同じ時点と言えない = 全部 held) */
export const BASELINE_MAX_GAP_MS = 4 * 3600 * 1000;
export const DIRECTIONS = Object.freeze(['to_ne', 'ne_changed', 'conflict', 'unknown', 'held']);
const CHUNK = 5000;

/** 正準な JSON (キーを並べる・数は 1e-6 で丸める。配列の順は呼び手が決める) */
export function canonJson(v) {
  const c = (x) => {
    if (Array.isArray(x)) return x.map(c);
    if (x && typeof x === 'object') return Object.fromEntries(Object.keys(x).sort().map((k) => [k, c(x[k])]));
    if (typeof x === 'number') return Number.isFinite(x) ? Math.round(x * 1e6) / 1e6 : null;
    return x === undefined ? null : x;
  };
  return JSON.stringify(c(v));
}
const same = (a, b) => canonJson(a) === canonJson(b);
const yenValue = (x) => (x == null || Number(x) === 0 ? null : Number(x));
const text = (x) => { const t = x == null ? '' : String(x).trim(); return t || null; };

/**
 * NE の値の状態 (compare-ne の numState / textState) → D2 の値。{ ok: false } = 不明・不正 (一致にも方向にも使わない)
 * @param {string} col
 * @param {{ raw: string, validity: string, value: any }} st
 */
export function neValue(col, st) {
  if (!st || st.raw === 'unknown' || st.validity !== 'ok') return { ok: false };
  const empty = st.raw === 'empty' || st.raw === 'null';
  switch (col) {
    case 'name': return { ok: true, value: empty ? null : text(st.value) };
    case 'handling': return { ok: true, value: empty ? 'unknown' : st.value };   // ロードは空を 'unknown' で入れる (engine)
    case 'tax_rate': return { ok: true, value: empty ? null : st.value };        // 0 は numState で invalid
    case 'standard_price_jpy': case 'cost': return { ok: true, value: empty || st.raw === 'zero' ? null : yenValue(st.value) };
    case 'primary_supplier': return { ok: true, value: empty || st.value == null ? [] : [st.value] };
    default: return { ok: false };
  }
}
/** Company DB の値 → D2 の値 (NE と同じ正規化) */
export function cdbValue(col, v) {
  switch (col) {
    case 'name': return text(v);
    case 'handling': return text(v) ?? 'unknown';
    case 'tax_rate': return v == null ? null : Number(v);
    case 'standard_price_jpy': case 'cost': return yenValue(v);
    case 'primary_supplier': return [...(v || [])].filter((x) => x != null).map(String).sort();
    default: return v;
  }
}
/** 構成の集合 (子の code_norm で並べる) */
export const componentsValue = (pairs) => [...pairs].map(([child, qty]) => [String(child), Number(qty)]).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

/**
 * 方向 (契約 v2 の順)。一致 = null。base = { value, norm_version } | null
 */
export function directionOf(n, c, base) {
  if (same(n, c)) return null;
  if (!base || base.norm_version !== BASELINE_NORM_VERSION) return 'unknown';
  const nb = same(n, base.value), cb = same(c, base.value);
  if (!cb && nb) return 'to_ne';
  if (!nb && cb) return 'ne_changed';
  return 'conflict';
}

// ─────────── 世代 ───────────
/** NE の印 ('YYYY-MM-DD HH:MM:SS' = UTC) と ISO の両方を読む */
const neTextMs = (t) => { const s = String(t ?? ''); const ms = Date.parse(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(s) ? `${s.replace(' ', 'T')}Z` : s); return s && Number.isFinite(ms) ? ms : null; };
const isoMs = (t) => { const ms = Date.parse(String(t ?? '')); return Number.isFinite(ms) ? ms : null; };
const revOf = (x) => (x != null && /^\d+$/.test(String(x)) ? BigInt(String(x)) : null);
/**
 * 今回の世代を読める形に。読めない成分があれば null
 * @param {{ products_at, products_rev, sets_at, sets_rev, cdb_read_at }} g
 */
export function parseGeneration(g) {
  if (!g) return null;
  const x = { products_at: neTextMs(g.products_at), sets_at: neTextMs(g.sets_at), products_rev: revOf(g.products_rev), sets_rev: revOf(g.sets_rev), cdb_read_at: isoMs(g.cdb_read_at) };
  return Object.values(x).some((v) => v == null) ? null : x;
}
/** 今回の世代が最高到達点より古い成分の名前 (無ければ []) */
export function regressedComponents(gen, mark) {
  const a = parseGeneration(gen), m = parseGeneration(mark);
  if (!a || !m) return ['unreadable'];
  return ['products_rev', 'sets_rev', 'products_at', 'sets_at', 'cdb_read_at'].filter((k) => a[k] < m[k]);
}
/** 単品・セットのそれぞれの取得と CDB の読みの差が上限以内か */
export function withinGap(gen, maxMs = BASELINE_MAX_GAP_MS) {
  const a = parseGeneration(gen);
  if (!a) return false;
  return Math.abs(a.cdb_read_at - a.products_at) <= maxMs && Math.abs(a.cdb_read_at - a.sets_at) <= maxMs;
}

// ─────────── 単位の評価 (② の集約の前の観測から。契約 v2) ───────────
/**
 * 単位ごとに n・c を D2 の意味で作り、一致なら書く候補・不一致なら方向を付ける。
 * @param {object} p
 * @param {Map} p.nm          compare-ne の NE の形 (norm → { code, kind, cols, children })
 * @param {object} p.cdb      readCdbMaster の結果
 * @param {(norm: string) => string|null} p.holdSku  その SKU の全部を確かめられない理由 (衝突・取込の整合・例外)
 * @param {boolean} p.absenceUntrusted     NE の行が落ちた回 (「NE に無い」を言えない)
 * @param {boolean} p.setRowsDropped       セットの表の行が落ちた回 (単品に見える SKU が本当はセットかもしれない = 種類と値の列を言えない)
 * @param {boolean} p.componentsUntrusted  構成の行が落ちた回
 * @param {{ state: string, rows?: Map, mark?: object|null, reason?: string, cdbReadAt?: string }} p.baseline  readBaseline の結果 + CDB の読みの時刻
 * @param {object} p.generation  { products_at, products_rev, sets_at, sets_rev } (NE の印)
 * @returns {{ section: object, writes: object[], directionOf: Map<string, string> }}  directionOf = `${norm}|${col}` → 方向 (SKU 全部の保持は `${norm}|*`)
 */
export function evaluateBaseline({ nm, cdb, holdSku, absenceUntrusted, setRowsDropped = absenceUntrusted, componentsUntrusted, baseline, generation }) {
  const gen = { ...generation, products_rev: generation.products_rev == null ? null : String(generation.products_rev), sets_rev: generation.sets_rev == null ? null : String(generation.sets_rev),
    cdb_read_at: baseline?.cdbReadAt ?? null };
  const section = { state: baseline ? baseline.state : 'not_applied', held_reason: null, norm_version: BASELINE_NORM_VERSION, generation: gen,
    expected_mark: baseline?.mark?.compare_run_id ?? null, counts: { units: 0, match: 0, held: 0, unknown: 0, to_ne: 0, ne_changed: 0, conflict: 0, to_write: 0, held_skus: 0 }, diffs: [] };
  const dirs = new Map();
  const writes = [];
  if (!baseline || baseline.state === 'not_applied') return { section: { state: 'not_applied' }, writes, directionOf: dirs };
  if (baseline.state === 'unreadable') { section.state = 'unreadable'; section.held_reason = `unreadable: ${baseline.reason ?? ''}`; }
  else if (!parseGeneration(gen)) section.held_reason = 'generation_unreadable';
  else if (!baseline.mark && baseline.rows.size) section.held_reason = 'baseline_without_mark';
  else if (baseline.mark) { const r = regressedComponents(gen, baseline.mark); if (r.length) section.held_reason = `stale_observation: ${r.join(',')}`; }
  if (!section.held_reason && !withinGap(gen)) section.held_reason = 'gap';
  if (section.held_reason && section.state === 'ok') section.state = 'held';
  const runHeld = section.state !== 'ok';
  const rows = baseline.rows || new Map();

  const units = [];
  const put = (norm, col, x) => units.push({ norm, col, ...x });
  const universe = [...new Set([...nm.keys(), ...cdb.skuByNorm.keys()])].sort();
  for (const norm of universe) {
    const n = nm.get(norm) || null, r = cdb.skuByNorm.get(norm) || null;
    const ver = r && r.version != null ? Number(r.version) : null;
    const hs = holdSku(norm);
    if (hs) { put(norm, '*', { held: hs }); continue; }
    if (!n) { if (absenceUntrusted) put(norm, 'exists', { held: 'ne_dropped_rows' }); else put(norm, 'exists', { n: false, c: true, ver: null }); continue; }
    if (!r) { put(norm, 'exists', { n: true, c: false, ver: null }); continue; }
    put(norm, 'exists', { n: true, c: true, ver: null });
    // セットの表の行が落ちた回、単品に見える SKU は本当はセットかもしれない = 種類と値の列を書かない (有無は NE にある証拠なので書く)。
    //   書くと、行が戻った朝に「前から食い違っていた差」を ne_changed と読み違える (レビューの Medium)
    if (setRowsDropped && n.kind === 'single') { put(norm, '*', { held: 'ne_dropped_rows' }); continue; }
    put(norm, 'kind', { n: n.kind, c: r.sku_kind, ver });
    if (n.kind !== r.sku_kind) { put(norm, '*', { held: 'kind_mismatch' }); continue; }
    const cols = n.kind === 'single' ? ['name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier'] : ['name', 'standard_price_jpy'];
    for (const col of cols) {
      if ((col === 'standard_price_jpy' || col === 'primary_supplier') && !cdb.has0027) continue;
      const nv = neValue(col, n.cols[col]);
      if (!nv.ok) { put(norm, col, { held: 'ne_invalid' }); continue; }
      const raw = col === 'cost' ? (cdb.costs.get(norm)?.cost_jpy ?? null) : col === 'primary_supplier' ? (cdb.primary.get(norm) || []) : r[col];
      put(norm, col, { n: nv.value, c: cdbValue(col, raw), ver: col === 'primary_supplier' ? null : ver });
    }
    if (n.kind === 'set') {
      if (componentsUntrusted || setRowsDropped) { put(norm, 'components', { held: 'ne_dropped_rows' }); continue; }
      const ch = [...(n.children || new Map())];
      if (!ch.length || ch.some(([, x]) => !x.st || x.st.validity !== 'ok' || x.st.raw !== 'value')) { put(norm, 'components', { held: 'ne_invalid' }); continue; }
      put(norm, 'components', { n: componentsValue(ch.map(([cn, x]) => [cn, x.st.value])), c: componentsValue([...(cdb.comps.get(norm) || new Map())].map(([cn, x]) => [cn, x.qty])), ver });
    }
  }
  const cnt = section.counts;
  for (const u of units) {
    // SKU 全部の保持 ('*') は単位の数に入れず held_skus に数える
    if (u.col === '*') { cnt.held_skus++; dirs.set(`${u.norm}|*`, 'held'); section.diffs.push({ code_norm: u.norm, col: '*', direction: 'held', held: u.held }); continue; }
    cnt.units++;
    if (u.held) { cnt.held++; dirs.set(`${u.norm}|${u.col}`, 'held'); section.diffs.push({ code_norm: u.norm, col: u.col, direction: 'held', held: u.held }); continue; }
    const base = rows.get(`${u.norm}|${u.col}`) || null;
    const d = directionOf(u.n, u.c, base);
    if (d === null) {
      cnt.match++;
      if (!runHeld && (!base || base.norm_version !== BASELINE_NORM_VERSION || canonJson(base.value) !== canonJson(u.n))) {
        writes.push({ code_norm: u.norm, col: u.col, value: u.n, cdb_version: u.ver, prev_hash: base ? base.value_hash : null, prev_version: base ? base.norm_version : null });
      }
      continue;
    }
    const dir = runHeld ? 'held' : d;
    cnt[dir]++;
    dirs.set(`${u.norm}|${u.col}`, dir);
    section.diffs.push({ code_norm: u.norm, col: u.col, n: u.n, c: u.c, base: base ? base.value : null, direction: dir });
  }
  cnt.to_write = writes.length;
  return { section, writes, directionOf: dirs };
}
/** 書く時に拒まれた回 (mark_moved / stale_run / unit_conflict …) = 古い基準で出した方向を使わない = 全部 held (D2-R1 M3) */
export function holdAllDirections(ne) {
  const s = ne && ne.baseline;
  if (!s || !s.counts) return;
  for (const d of s.diffs || []) if (d.direction !== 'held') { s.counts.held++; s.counts[d.direction]--; d.direction = 'held'; }
  for (const it of ne.items || []) for (const c of it.columns || []) if (c.direction) c.direction = 'held';
}

// ─────────── 読む (照合の読み取りの取引の中・watcher) ───────────
/**
 * 基準と最高到達点を読む。表が無い = not_applied / 読めない = unreadable (savepoint で取引を壊さない = ① と ② は続く)
 * @returns {{ state: 'ok'|'not_applied'|'unreadable', rows?: Map, mark?: object|null, reason?: string }}
 */
export async function readBaseline(db) {
  const has = (await db.query(`select to_regclass('ops.master_ne_baseline') is not null and to_regclass('ops.master_ne_baseline_mark') is not null as ok`)).rows[0].ok;
  if (!has) return { state: 'not_applied' };
  await db.query('savepoint baseline_read');
  try {
    const rows = new Map();
    for (const r of (await db.query(`select code_norm, col, value, value_hash, norm_version from ops.master_ne_baseline where company_id = 1`)).rows) {
      rows.set(`${r.code_norm}|${r.col}`, { value: r.value, value_hash: r.value_hash, norm_version: Number(r.norm_version) });
    }
    const iso = (c) => `to_char(${c} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as ${c}`;
    const m = (await db.query(`select compare_run_id, norm_version, ${iso('ne_products_at')}, ne_products_rev::text as ne_products_rev, ${iso('ne_sets_at')}, ne_sets_rev::text as ne_sets_rev,
        ${iso('cdb_read_at')} from ops.master_ne_baseline_mark where id = 1`)).rows[0] || null;
    await db.query('release savepoint baseline_read');
    const mark = m ? { compare_run_id: m.compare_run_id, norm_version: Number(m.norm_version), products_at: m.ne_products_at, products_rev: m.ne_products_rev,
      sets_at: m.ne_sets_at, sets_rev: m.ne_sets_rev, cdb_read_at: m.cdb_read_at } : null;
    return { state: 'ok', rows, mark };
  } catch (e) {
    try { await db.query('rollback to savepoint baseline_read'); } catch { /* */ }
    return { state: 'unreadable', reason: String(e && e.message).slice(0, 200) };
  }
}

// ─────────── 書く (取引の後・watch_writer) ───────────
/**
 * 1 回の照合の基準を 1 つの取引で書く (5,000 単位ずつ同じ取引で関数を呼ぶ。変更ゼロでも呼ぶ = 札を照らして進める)。
 * 関数が拒んだ (mark_moved / stale_run / unit_conflict・入力の不正) = 例外 = 全部巻き戻る
 * @param {{ query: Function }} writer
 * @returns {Promise<{ inserted: number, updated: number, units: number }>}
 */
export async function writeBaseline(writer, { compareRunId, expectedMark, generation, units }) {
  const chunks = [];
  for (let i = 0; i < units.length; i += CHUNK) chunks.push(units.slice(i, i + CHUNK));
  if (!chunks.length) chunks.push([]);
  const sum = { inserted: 0, updated: 0, units: units.length };
  await writer.query('begin');
  try {
    for (const u of chunks) {
      const p = { compare_run_id: compareRunId, expected_mark: expectedMark ?? null, norm_version: BASELINE_NORM_VERSION, generation, units: u };
      const r = (await writer.query('select ops.record_ne_baseline($1::jsonb) as r', [JSON.stringify(p)])).rows[0].r;
      sum.inserted += Number(r.inserted || 0); sum.updated += Number(r.updated || 0);
    }
    await writer.query('commit');
  } catch (e) {
    try { await writer.query('rollback'); } catch { /* */ }
    throw e;
  }
  return sum;
}

