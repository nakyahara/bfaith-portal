/**
 * ne-csv.mjs — NE に取り込む CSV (③b-1。Company DB構想 10 §6.1.1「③b NE に取り込む CSV の契約 v3」。migration 0040)
 *
 * 判断の画面で「NE を直す」(fix_ne) と承認した差を、NE の一括登録 (商品管理の一括登録) で取り込める CSV にする。
 *   作る → (取り込む直前に) 確かめる → NE に取り込む (人) → 取り込んだと申告 → 翌朝の照合で行ごとに届いたかを見る
 *
 * 対象 (CSV に入れてよい承認) = 次の全部:
 *   - (SKU・列・子) の単位で**最後の判断** (どの指紋でも) が fix_ne の承認 (= その後に承認・却下・取し消しが無い。古い承認を生き返らせない H1)
 *   - まだ完了していない (action_done が無い)
 *   - その指紋が**今日 (JST) の照合の回**に出ている (= NE が承認のときの値のまま)。今日の回が無い日は作れない・確かめられない (H2)
 *   - 列の表にある (種類 × 列)・コードと値が CSV に書ける (書けない = 「NE の画面で直す」)
 *   - その単位に有効な予約が無い (予約は (SKU・列・子) ごとに 1 つ = 部分の一意の索引)
 * 🚨 CSV は固定の列の表 (COLUMNS) からだけ作る。API は (種類・列) と承認の選び方 (指紋) だけを受け取る (列名・値を受け取らない)。在庫の列は表に無い
 * 🚨 書く操作は 1 つの取引で: CSV の鍵 → 候補の行を指紋の順に for update → 条件を照らし直す → 書く (判断の API と同じ順。ne-csv-lock.mjs)
 * 🚨 時刻の判定 (今日・同じ日・次の日) は JST。nowMs を渡す (試験で日をまたぐ)
 */
import crypto from 'node:crypto';
import { DecideError, nameIsCodeOf } from './decide.mjs';
import { CSV_LOCK_SQL, csvApplied } from './ne-csv-lock.mjs';

// ne-csv-v2 (③b-1b): コードを NE の元の書き方で書く (v1 = 小文字の norm)。前の版の実機の確かめは引き継がない
export const CONVERTER_VERSION = 'ne-csv-v2';
export const MAX_ROWS = 1000;
/** 実機で確かめていない (種類・列・文字コード・見出し・変換の版) の組は「試し用」= この行数まで (M4) */
export const TRIAL_ROWS = 5;
/** 在庫の足し引き・在庫連携の解除になる列 = 絶対に出さない (列の表に入れない。試験で表と照らす) */
export const FORBIDDEN_HEADERS = Object.freeze(['zaiko_su', 'yoyaku_zaiko_su', 'nyusyukko_riyu', 'visible_flg']);
export const ENCODINGS = Object.freeze(['utf8']);   // Shift_JIS は実機の試しで要るとなったら足す (表の CHECK は sjis も受ける)
const FP_RE = /^[0-9a-f]{64}$/;
const CODE_RE = /^[a-z0-9_-]{1,30}$/;
/** CSV に書く NE のコード = 元の書き方 (大文字も)。NE のコードは大文字・小文字を区別する (③b-1b) */
const NE_CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
/** 元の書き方の表を読むときの鍵 (照合の書き手 ops.record_ne_codes は同じ鍵を排他で取る) */
const NE_CODES_SHARED_LOCK_SQL = `select pg_advisory_xact_lock_shared(hashtext('ops.ne_codes'))`;
// 改行・制御文字 (C0・DEL・C1) と行・段落の区切り
const CTRL_RE = new RegExp('[\\x00-\\x1f\\x7f-\\x9f' + String.fromCharCode(0x2028, 0x2029) + ']');
const ASTRAL_RE = /[\u{10000}-\u{10FFFF}]/u;
const DAY = 86400000, JST = 9 * 3600000;

// ─────────── 列の表と値の書き方 ───────────
const okCell = (cell) => ({ ok: true, cell });
const bad = (reason) => ({ ok: false, reason });
const isEmptyWord = (s) => s.trim().toLowerCase() === 'empty';
function nameCell(v) {
  if (typeof v !== 'string' || !v || v !== v.trim()) return bad('name_blank');
  if (isEmptyWord(v)) return bad('empty_word');   // 「empty」は NE では「消す」の指示 (M3)
  if (CTRL_RE.test(v)) return bad('name_control');
  if (ASTRAL_RE.test(v)) return bad('name_astral');   // 絵文字など (NE が受けるか分からない = 画面で)
  if ([...v].length > 255) return bad('name_length');
  return okCell(v);
}
const yenCell = (v) => (Number.isInteger(v) && v >= 1 && v <= 999999999 ? okCell(String(v)) : bad('yen_range'));
/** 列の表 (種類:列 → NE の見出しと値の書き方)。③b-1 = 単品の 7 列とセットの名前・売価だけ (構成・セットの税率は NE の画面で) */
export const COLUMNS = Object.freeze({
  'products:name': { kind: 'products', col: 'name', code: 'syohin_code', ne: 'syohin_name', cell: nameCell },
  'products:handling': { kind: 'products', col: 'handling', code: 'syohin_code', ne: 'toriatukai_kbn',
    cell: (v) => (v === 'active' ? okCell('0') : v === 'discontinued' ? okCell('1') : bad('handling_value')) },   // 1 = 取扱中止 (2 メーカー取扱中止 には戻さない)
  // 税率 = NE の「消費税率 (%)」= 10 / 8 の整数 (Company DB の 0.1 / 0.08 を % に)。0.1・0.08 のまま書かない (cellRe で buildCsv が二重に確かめる)
  'products:tax_rate': { kind: 'products', col: 'tax_rate', code: 'syohin_code', ne: 'tax_rate', cellRe: /^(10|8)$/,
    cell: (v) => (v === 0.1 ? okCell('10') : v === 0.08 ? okCell('8') : bad('tax_value')) },
  'products:standard_price_jpy': { kind: 'products', col: 'standard_price_jpy', code: 'syohin_code', ne: 'baika_tnk', cellRe: /^[1-9]\d*$/, cell: yenCell },
  'products:cost': { kind: 'products', col: 'cost', code: 'syohin_code', ne: 'genka_tnk', cellRe: /^[1-9]\d*$/, cell: yenCell },
  // 仕入先 = 4 桁の数字だけ (照合の正規化と NE の表記が同じ形。9999 = NE の「設定なし」も承認の値のまま)
  'products:primary_supplier': { kind: 'products', col: 'primary_supplier', code: 'syohin_code', ne: 'sire_code',
    cell: (v) => (typeof v === 'string' && /^\d{4}$/.test(v) ? okCell(v) : bad('supplier_format')) },
  // 代表 = 親なし (null) だけを empty と書く。親の名札は元の書き方 (judge が決めて渡す)。「empty」という文字・使えない文字 = 画面で
  'products:parent': { kind: 'products', col: 'parent', code: 'syohin_code', ne: 'daihyo_syohin_code',
    cell: (v) => (v === null ? okCell('empty') : typeof v === 'string' && NE_CODE_RE.test(v) && !isEmptyWord(v) ? okCell(v) : bad('parent_code')) },
  'sets:name': { kind: 'sets', col: 'name', code: 'set_syohin_code', ne: 'set_syohin_name', cell: nameCell },
  'sets:standard_price_jpy': { kind: 'sets', col: 'standard_price_jpy', code: 'set_syohin_code', ne: 'set_baika_tnk', cellRe: /^[1-9]\d*$/, cell: yenCell },
});
export const specOf = (kind, col) => (Object.hasOwn(COLUMNS, `${kind}:${col}`) ? COLUMNS[`${kind}:${col}`] : null);
export const headerOf = (spec) => [spec.code, spec.ne];
const kindOfSku = (skuKind) => (skuKind === 'single' ? 'products' : skuKind === 'set' ? 'sets' : null);

/** 1 つのセル (CSV の書き方): カンマ・引用符・前後の空白を含むときだけ引用符で囲む (改行は値の検査で入らない) */
const quote = (s) => (/[",]/.test(s) || s !== s.trim() ? `"${s.replace(/"/g, '""')}"` : s);
/**
 * CSV の byte 列 (UTF-8・BOM なし・CRLF・最後の行にも CRLF)。見出しは列の表から。在庫の列が混ざったら作らない
 * @returns {{ bytes: Buffer, sha256: string, header: string }}
 */
export function buildCsv(spec, rows) {
  const header = headerOf(spec);
  if (header.some((h) => FORBIDDEN_HEADERS.includes(h))) throw new Error(`出してはいけない列: ${header.join(',')}`);
  // 値の形の二重の守り: 税率は % の整数 (10 / 8)・売価/原価は 1 以上の整数。名前 = その行のコード は書かない (judge の後で崩れても CSV にしない)
  for (const r of rows) {
    if (spec.cellRe && !spec.cellRe.test(String(r.cell))) throw new Error(`${spec.kind}:${spec.col} の値の形が違う: ${r.ne_code} = ${r.cell}`);
    if (spec.col === 'name' && nameIsCodeOf(String(r.cell), String(r.ne_code))) throw new Error(`名前が商品コードと同じ: ${r.ne_code}`);
  }
  const lines = [header.join(','), ...rows.map((r) => [quote(r.ne_code), quote(r.cell)].join(','))];
  const bytes = Buffer.from(lines.join('\r\n') + '\r\n', 'utf8');
  return { bytes, sha256: crypto.createHash('sha256').update(bytes).digest('hex'), header: header.join(',') };
}

/**
 * 保存したファイルを今の決まりで確かめ直す (直す前・古い版で作ったファイルを配らない・確かめ・申告で通さない。#1629 Codex R1 High)。
 * 名前 = その行のコード (広い正規化で) / 値の形 (cellRe) / 今の buildCsv で作り直した byte 列が保存した byte 列と同じ。問題が無ければ null
 * @param {{ kind, col }} e  ファイル / rows = 行 (row_id の順) / bytes = 保存した byte 列 (無ければ byte 列は照らさない)
 */
export function unsafeReason(e, rows, bytes = null) {
  const spec = specOf(String(e.kind), String(e.col));
  if (!spec) return 'col_not_csv';
  if (spec.col === 'name' && rows.some((r) => nameIsCodeOf(String(r.cell), r.code_norm) || nameIsCodeOf(String(r.cell), r.ne_code))) return 'name_is_code';
  let csv;
  try { csv = buildCsv(spec, rows); } catch { return 'cell_format'; }
  if (bytes && crypto.createHash('sha256').update(bytes).digest('hex') !== csv.sha256) return 'bytes_differ';
  return null;
}
async function exportRowsOf(db, exportId) {
  return (await db.query(`select ${ROW_COLS} from ops.ne_csv_export_rows where export_id = $1 order by row_id`, [exportId])).rows.map(shapeRow);
}

// ─────────── 時刻 (JST) ───────────
export const jstDate = (ms) => new Date(ms + JST).toISOString().slice(0, 10);
/** その JST の日の次の日の 0 時 (UTC の ms) */
const nextJstDayStart = (ms) => Math.floor((ms + JST) / DAY) * DAY + DAY - JST;
const msOf = (x) => (x == null ? null : Number(x));
const iso = (ms) => new Date(ms).toISOString();

/** 最新の照合の回と、それが今日 (JST) か */
export async function todayRun(db, nowMs) {
  const r = (await db.query(`select compare_run_id, observed_at::text as observed_at, (extract(epoch from observed_at) * 1000)::float8 as ms
    from ops.master_compare_runs order by observed_at desc, compare_run_id desc limit 1`)).rows[0];
  if (!r) return { run: null, observed_at: null, today: false };
  return { run: r.compare_run_id, observed_at: r.observed_at, today: jstDate(Number(r.ms)) === jstDate(nowMs) };
}

// ─────────── 対象の判定 ───────────
/**
 * (SKU・列・子) ごとの最後の判断が fix_ne の承認のもの (候補と完了も)。fingerprints を渡すとその指紋の単位だけ
 */
async function readFixNeUnits(db, fingerprints = null) {
  return (await db.query(`
    with dec as (
      select e.event_id, e.fingerprint, e.kind, e.resolution, e.target, c.code_norm, c.col, coalesce(c.child, '') as child_k
        from ops.master_decision_events e join ops.master_decision_candidates c on c.fingerprint = e.fingerprint
       where e.kind in ('approved', 'rejected', 'revoked')
    ), unit_last as (
      select distinct on (code_norm, col, child_k) code_norm, col, child_k, event_id, fingerprint, kind, resolution, target
        from dec order by code_norm, col, child_k, event_id desc
    )
    select u.event_id, u.fingerprint, u.target, c.subject_key, c.code_norm, c.col, c.child, c.print, c.last_seen_run, c.cls, c.reason_kind,
           exists (select 1 from ops.master_decision_events d where d.kind = 'action_done' and d.approved_event_id = u.event_id) as done
      from unit_last u join ops.master_decision_candidates c on c.fingerprint = u.fingerprint
     where u.kind = 'approved' and u.resolution = 'fix_ne' ${fingerprints ? 'and u.fingerprint = any($1::text[])' : ''}
     order by c.code_norm, c.col, c.child nulls first, u.fingerprint`, fingerprints ? [fingerprints] : [])).rows
    .map((r) => ({ ...r, event_id: Number(r.event_id) }));
}
const unitKey = (u) => `${u.code_norm}|${u.col}|${u.child ?? ''}`;

/**
 * 1 つの承認を判定する。status = csv (CSV にできる) / reserved (ほかのファイルが予約中) / ne_screen (NE の画面で直す) / waiting (今日の回に出ていない) / done
 * @param {object} u  readFixNeUnits の 1 行
 * @param {{ run: string|null, reservations: Map, ownExport?: number|null, neCodes?: { run: string|null, map: Map }|null }} ctx
 *   neCodes = NE の元の書き方 (readNeCodes)。今日の照合の回のものでなければ全部「NE の画面で直す」(小文字に代えて書かない。③b-1b)
 */
export function judge(u, ctx) {
  const base = { fingerprint: u.fingerprint, approved_event_id: u.event_id, subject_key: u.subject_key, code_norm: u.code_norm, col: u.col, child: u.child ?? null,
    sku_kind: u.print?.sku_kind ?? null, value: u.target?.value, n: u.print?.n ?? null, c: u.print?.c ?? null };
  if (u.done) return { ...base, status: 'done' };
  if (!ctx.run || u.last_seen_run !== ctx.run) return { ...base, status: 'waiting', reason: 'not_current' };
  const tg = u.target || {};
  if (tg.subject_key !== u.subject_key || tg.col !== u.col || (tg.child ?? null) !== (u.child ?? null) || !Object.hasOwn(tg, 'value')) return { ...base, status: 'ne_screen', reason: 'target_mismatch' };
  const kind = kindOfSku(base.sku_kind);
  const spec = kind ? specOf(kind, u.col) : null;
  if (!spec) return { ...base, status: 'ne_screen', reason: 'col_not_csv' };
  const key = `${spec.kind}:${spec.col}`;
  if (!CODE_RE.test(u.code_norm) || isEmptyWord(u.code_norm)) return { ...base, status: 'ne_screen', reason: 'code_chars', key };
  // NE の元の書き方 (③b-1b 契約 v3): 今日の照合の回の記録だけ。無い・古い・衝突・使えない = NE の画面で直す
  const nc = ctx.neCodes;
  if (!nc || !nc.run || nc.run !== ctx.run) return { ...base, status: 'ne_screen', reason: 'ne_code_pending', key };
  const own = nc.map.get(`product|${u.code_norm}`);
  const codeWhy = (e) => (!e ? 'ne_code_unknown' : e.state === 'collided' ? 'ne_code_collided' : e.state !== 'ok' ? 'ne_code_invalid' : null);
  if (codeWhy(own)) return { ...base, status: 'ne_screen', reason: codeWhy(own), key };
  let value = tg.value;
  if (u.col === 'parent' && value !== null) {
    // 親の名札: 今回の名札の元の書き方 → 名札が無い (完全な取得でどの代表にも無い) ときだけ親の商品の元の書き方 → それ以外 = 画面で (Codex ③b-1b-R0 H2)
    const rep = nc.map.get(`rep|${value}`);
    const parentOwn = rep ? rep : nc.map.get(`product|${value}`);
    if (!parentOwn || parentOwn.state !== 'ok') return { ...base, status: 'ne_screen', reason: 'parent_code_unknown', key };
    value = parentOwn.ne_code;
  }
  // 名前 = 商品コード は名前ではない (社内の名前がコードのまま = 夜間ロードの代わりの値)。承認されていても CSV に入れない (判断の API・照合と二重の守り。2026-10-05)
  if (u.col === 'name' && (nameIsCodeOf(value, u.code_norm) || nameIsCodeOf(value, own.ne_code))) return { ...base, status: 'ne_screen', reason: 'name_is_code', key };
  const cv = spec.cell(value);
  if (!cv.ok) return { ...base, status: 'ne_screen', reason: cv.reason, key };
  const held = ctx.reservations.get(unitKey(u));
  const out = { ...base, key, spec, ne_code: own.ne_code, cell: cv.cell, target: tg };
  if (held && held.export_id !== ctx.ownExport) return { ...out, status: 'reserved', export_id: held.export_id };
  return { ...out, status: 'csv', held_by: held ? held.export_id : null };
}

// ─────────── 行の届き方 (M1・M2) ───────────
const CANCEL_REASONS = new Set(['superseded', 'void']);
/**
 * 行の状態。判定の順 = 確認済み (action_done) → 取り消し (置き換わった・void) → 申告したファイル: 確認できない / 反映されていない / 確かめが要る → 予約中
 *   申告の次の日 (JST) 以降の最初の照合の回で見る (その回に同じ指紋が出ている = 反映されていない / 出ていない = 確かめが要る)
 */
export async function rowStates(db, rows, exportsById) {
  if (!rows.length) return [];
  const evIds = [...new Set(rows.map((r) => r.approved_event_id).filter((x) => x != null))];
  const done = new Map((await db.query(`select approved_event_id, actor as run, created_at::text as at from ops.master_decision_events
    where kind = 'action_done' and approved_event_id = any($1::bigint[])`, [evIds])).rows.map((r) => [Number(r.approved_event_id), r]));
  const runs = (await db.query(`select compare_run_id, (extract(epoch from observed_at) * 1000)::float8 as ms from ops.master_compare_runs order by observed_at, compare_run_id`)).rows
    .map((r) => ({ run: r.compare_run_id, ms: Number(r.ms) }));
  const firstRunAfter = (declaredMs) => { const from = nextJstDayStart(declaredMs); return runs.find((x) => x.ms >= from) || null; };
  const want = [];
  for (const r of rows) {
    const e = exportsById.get(Number(r.export_id));
    if (e && e.declared_ms != null) { const fr = firstRunAfter(e.declared_ms); if (fr) want.push([r.fingerprint, fr.run]); }
  }
  const seen = new Set();
  if (want.length) {
    for (const o of (await db.query(`select fingerprint, compare_run_id from ops.master_decision_observations
        where (fingerprint, compare_run_id) in (select * from unnest($1::text[], $2::text[]))`, [want.map((w) => w[0]), want.map((w) => w[1])])).rows) seen.add(`${o.fingerprint}|${o.compare_run_id}`);
  }
  return rows.map((r) => {
    const d = r.approved_event_id != null ? done.get(Number(r.approved_event_id)) : null;
    if (d) return { state: 'confirmed', done_at: d.at, done_run: d.run };
    if (!r.reserved && CANCEL_REASONS.has(r.release_reason)) return { state: 'cancelled', reason: r.release_reason };
    const e = exportsById.get(Number(r.export_id));
    if (e && e.declared_ms != null) {
      const fr = firstRunAfter(e.declared_ms);
      if (!fr) return { state: 'unconfirmable' };
      return seen.has(`${r.fingerprint}|${fr.run}`) ? { state: 'not_reflected', run: fr.run } : { state: 'needs_look', run: fr.run };
    }
    return r.reserved ? { state: 'reserved' } : { state: 'cancelled', reason: r.release_reason };
  });
}
const RELEASE_ON = new Set(['confirmed', 'not_reflected', 'needs_look']);

const EXPORT_COLS = `export_id, kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, compare_run_id, created_by, created_at::text as created_at,
  (extract(epoch from created_at) * 1000)::float8 as created_ms, state, checked_at::text as checked_at, (extract(epoch from checked_at) * 1000)::float8 as checked_ms, checked_run, checked_by,
  declared_at::text as declared_at, (extract(epoch from declared_at) * 1000)::float8 as declared_ms, declared_by, void_at::text as void_at, void_reason, void_by`;
const shapeExport = (e) => ({ ...e, export_id: Number(e.export_id), row_count: Number(e.row_count), created_ms: msOf(e.created_ms), checked_ms: msOf(e.checked_ms), declared_ms: msOf(e.declared_ms) });
const ROW_COLS = `row_id, export_id, source, approved_event_id, fingerprint, code_norm, col, child, ne_code, target, cell, prev_row_id, reserved, released_at::text as released_at, release_reason`;
const shapeRow = (r) => ({ ...r, row_id: Number(r.row_id), export_id: Number(r.export_id), approved_event_id: r.approved_event_id == null ? null : Number(r.approved_event_id),
  prev_row_id: r.prev_row_id == null ? null : Number(r.prev_row_id) });

/**
 * 有効な予約 (単位 → { export_id, row_id })。申告したファイルの行で、届き方が決まった (確認済み・反映されていない・確かめが要る) ものは外れたものとして数える。
 * write = true なら実際に外す (CSV の鍵の中で)
 */
async function reservations(db, { write = false } = {}) {
  const rows = (await db.query(`select ${ROW_COLS} from ops.ne_csv_export_rows where reserved order by row_id`)).rows.map(shapeRow);
  const ids = [...new Set(rows.map((r) => r.export_id))];
  const exps = new Map(ids.length ? (await db.query(`select ${EXPORT_COLS} from ops.ne_csv_exports where export_id = any($1::bigint[])`, [ids])).rows.map((e) => [Number(e.export_id), shapeExport(e)]) : []);
  const st = await rowStates(db, rows, exps);
  const map = new Map();
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i], s = st[i], e = exps.get(r.export_id);
    if (e && e.state === 'declared' && RELEASE_ON.has(s.state)) {
      if (write) await db.query(`update ops.ne_csv_export_rows set reserved = false, released_at = now(), release_reason = $2 where row_id = $1 and reserved`, [r.row_id, s.state]);
      continue;
    }
    map.set(unitKey(r), { export_id: r.export_id, row_id: r.row_id });
  }
  return map;
}

/** 実機で確かめた組 (最後の記録が ok のもの) */
async function verifiedSet(db) {
  const rows = (await db.query(`select distinct on (kind, col, encoding, header, converter_version) kind, col, encoding, header, converter_version, result, verified_at::text as verified_at, verified_by
    from ops.ne_csv_verified order by kind, col, encoding, header, converter_version, verified_id desc`)).rows;
  return { rows, ok: new Set(rows.filter((r) => r.result === 'ok').map((r) => `${r.kind}|${r.col}|${r.encoding}|${r.header}|${r.converter_version}`)) };
}
const verifiedKey = (spec, encoding) => `${spec.kind}|${spec.col}|${encoding}|${headerOf(spec).join(',')}|${CONVERTER_VERSION}`;

async function requireApplied(db) {
  if (!(await csvApplied(db))) throw new DecideError('NE 用 CSV の記録の表 (migration 0040) がまだ入っていません', 'not_applied');
}
/** 0041 (NE の元の書き方) が入っているか */
async function neCodesApplied(db) {
  return (await db.query(`select to_regclass('ops.master_ne_code_mark') is not null as ok`)).rows[0].ok;
}
/**
 * NE の元の書き方 (ops.master_ne_codes) と、どの照合の回の中身か (印)。0041 の前・印が無い = run null (全部「NE の画面で直す」)。
 * 🚨 CSV の操作の中では、元のコードの共有の鍵の後に読む (読んでいる間に照合が表を入れ替えない。Codex ③b-1b-R1 H2)
 */
export async function readNeCodes(db) {
  if (!(await neCodesApplied(db))) return { applied: false, run: null, map: new Map() };
  const mark = (await db.query('select compare_run_id from ops.master_ne_code_mark where id = 1')).rows[0];
  const map = new Map((await db.query('select code_norm, kind, state, ne_code from ops.master_ne_codes')).rows.map((r) => [`${r.kind}|${r.code_norm}`, { state: r.state, ne_code: r.ne_code }]));
  return { applied: true, run: mark ? mark.compare_run_id : null, map };
}
async function tx(db, fn) {
  await db.query('begin');
  try {
    await db.query(CSV_LOCK_SQL);
    if (await neCodesApplied(db)) await db.query(NE_CODES_SHARED_LOCK_SQL);   // 鍵の順 = CSV の鍵 → 元のコードの共有の鍵 → 候補の行
    const r = await fn();
    await db.query('commit');
    return r;
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    throw e;
  }
}
/** 候補の行を指紋の順に for update (判断の API・照合の完了の関数と同じ行・同じ順) */
async function lockCandidates(db, fps) {
  if (!fps.length) return;
  await db.query(`select fingerprint from ops.master_decision_candidates where fingerprint = any($1::text[]) order by fingerprint for update`, [[...new Set(fps)].sort()]);
}
const needToday = (tr) => {
  if (!tr.today) throw new DecideError('今日 (JST) の照合の回がまだありません (今朝の照合が止まった・まだ)。今日の照合の後に作る・確かめる', 'no_today_run');
};

// ─────────── 読む ───────────
/** 画面の上: 今日の回・列ごとの件数 (CSV にできる / 予約中 / 試し用か)・NE の画面で直す一覧・最近のファイル・実機の確かめ */
export async function csvSummary(db, { nowMs = Date.now() } = {}) {
  if (!(await csvApplied(db))) return { applied: false };
  const tr = await todayRun(db, nowMs);
  const resv = await reservations(db);
  const units = await readFixNeUnits(db);
  const nc = await readNeCodes(db);
  const judged = units.map((u) => judge(u, { run: tr.run, reservations: resv, neCodes: nc }));
  const ver = await verifiedSet(db);
  const groups = Object.values(COLUMNS).map((spec) => {
    const key = `${spec.kind}:${spec.col}`;
    const mine = judged.filter((j) => j.key === key);
    const verified = ver.ok.has(verifiedKey(spec, 'utf8'));
    return { key, kind: spec.kind, col: spec.col, header: headerOf(spec).join(','), csv: mine.filter((j) => j.status === 'csv').length,
      reserved: mine.filter((j) => j.status === 'reserved').length, verified, limit: verified ? MAX_ROWS : TRIAL_ROWS };
  });
  const pick = (j) => ({ fingerprint: j.fingerprint, approved_event_id: j.approved_event_id, subject_key: j.subject_key, code_norm: j.code_norm, col: j.col, child: j.child, sku_kind: j.sku_kind,
    value: j.value, n: j.n, c: j.c, reason: j.reason ?? null, key: j.key ?? null, export_id: j.export_id ?? null });
  // 一覧 = まだ終わっていないファイル (作った・確かめた・予約が残る) は古くても全部 + 全体の最近の 30 件 (重なった分は 1 つ。古い未処理のファイルが一覧から消えて操作できなくならない。#1495 Codex R2 Medium・R3 Low)
  const exps = (await db.query(`select ${EXPORT_COLS} from ops.ne_csv_exports
     where state in ('made', 'checked') or export_id in (select export_id from ops.ne_csv_export_rows where reserved)
        or export_id in (select export_id from ops.ne_csv_exports order by export_id desc limit 30)
     order by export_id desc`)).rows.map(shapeExport);
  return { applied: true, today: tr.today, run: tr.run, observed_at: tr.observed_at, today_jst: jstDate(nowMs), converter_version: CONVERTER_VERSION, groups,
    ne_codes: { applied: nc.applied, run: nc.run, current: !!nc.run && nc.run === tr.run },
    csv_items: judged.filter((j) => j.status === 'csv' || j.status === 'reserved').map(pick),
    ne_screen: judged.filter((j) => j.status === 'ne_screen').map(pick),
    waiting: judged.filter((j) => j.status === 'waiting').length,
    exports: await withRowCounts(db, exps, nowMs, tr.run), verified: ver.rows };
}
async function withRowCounts(db, exps, nowMs, run) {
  if (!exps.length) return [];
  const released = await releasedExports(db, exps.map((e) => e.export_id));
  const rows = (await db.query(`select ${ROW_COLS} from ops.ne_csv_export_rows where export_id = any($1::bigint[]) order by row_id`, [exps.map((e) => e.export_id)])).rows.map(shapeRow);
  const byId = new Map(exps.map((e) => [e.export_id, e]));
  const st = await rowStates(db, rows, byId);
  const counts = new Map(exps.map((e) => [e.export_id, {}]));
  rows.forEach((r, i) => { const c = counts.get(r.export_id); c[st[i].state] = (c[st[i].state] || 0) + 1; });
  return exps.map((e) => ({ ...publicExport(e, nowMs, { run, released }), row_states: counts.get(e.export_id) }));
}
/** 予約が外れた行を持つファイル */
async function releasedExports(db, ids) {
  if (!ids.length) return new Set();
  return new Set((await db.query(`select distinct export_id from ops.ne_csv_export_rows where export_id = any($1::bigint[]) and not reserved`, [ids])).rows.map((r) => Number(r.export_id)));
}
/**
 * 申告したファイルをもう一度使えるか (配る・申告のし直し) = 申告と同じ日 (JST)・確かめた後に新しい照合の回が無い・予約が外れた行が無い。
 * それ以外は「もう使えない」(取込の試みの記録は残す・取り込み直すなら作り直す。#1495 Codex R1 High = 予約を外した旧いファイルを配り・申告できた)
 */
function declaredUsable(e, { run, released, nowMs }) {
  return e.state === 'declared' && e.declared_ms != null && jstDate(e.declared_ms) === jstDate(nowMs) && !!run && run === e.checked_run && !released.has(e.export_id);
}
/** 確かめたのが今日で、その後に新しい照合の回が無い (= 申告してよい) */
const checkedCurrent = (e, run, nowMs) => e.state === 'checked' && e.checked_ms != null && jstDate(e.checked_ms) === jstDate(nowMs) && !!run && run === e.checked_run;
/** 画面・API に出す形 (byte 列は出さない)。checked_current = 申告してよい / reusable = 申告したファイルをまだ使える (配る・申告のし直し) */
function publicExport(e, nowMs, { run = null, released = new Set() } = {}) {
  const { created_ms, checked_ms, declared_ms, ...rest } = e;
  return { ...rest, file_name: fileNameOf(e), checked_today: e.state === 'checked' && checked_ms != null && jstDate(checked_ms) === jstDate(nowMs),
    checked_current: checkedCurrent(e, run, nowMs), reusable: declaredUsable(e, { run, released, nowMs }) };
}
export function fileNameOf(e) {
  const t = new Date(e.created_ms + JST).toISOString().replace(/[-:]/g, '').replace('T', '_').slice(0, 15);
  return `ne_${e.kind}_${e.col}_${t}_${e.export_id}${e.trial ? '_trial' : ''}.csv`;
}

/** 1 つのファイルの中身 (行と届き方・取込の試み) */
export async function exportDetail(db, exportId, { nowMs = Date.now() } = {}) {
  await requireApplied(db);
  const e = (await db.query(`select ${EXPORT_COLS} from ops.ne_csv_exports where export_id = $1`, [exportId])).rows[0];
  if (!e) return null;
  const ex = shapeExport(e);
  const rows = (await db.query(`select ${ROW_COLS} from ops.ne_csv_export_rows where export_id = $1 order by row_id`, [exportId])).rows.map(shapeRow);
  const st = await rowStates(db, rows, new Map([[ex.export_id, ex]]));
  const attempts = (await db.query(`select attempt_id, declared_by, declared_at::text as declared_at, result, note from ops.ne_csv_attempts where export_id = $1 order by attempt_id`, [exportId])).rows
    .map((a) => ({ ...a, attempt_id: Number(a.attempt_id) }));
  const tr = await todayRun(db, nowMs);
  return { export: publicExport(ex, nowMs, { run: tr.run, released: await releasedExports(db, [ex.export_id]) }), rows: rows.map((r, i) => ({ ...r, ...st[i] })), attempts };
}

/** 配る byte 列。void と、もう使えない申告済みのファイル (state = 'retired' で返す) は配らない */
export async function exportFile(db, exportId, { nowMs = Date.now() } = {}) {
  await requireApplied(db);
  const e = (await db.query(`select ${EXPORT_COLS}, file_bytes from ops.ne_csv_exports where export_id = $1`, [exportId])).rows[0];
  if (!e) return null;
  const ex = shapeExport(e);
  if (ex.state === 'declared' && !declaredUsable(ex, { run: (await todayRun(db, nowMs)).run, released: await releasedExports(db, [ex.export_id]), nowMs })) return { state: 'retired', file_name: fileNameOf(ex), bytes: null };
  const bytes = Buffer.from(e.file_bytes);
  if (crypto.createHash('sha256').update(bytes).digest('hex') !== e.sha256) throw new Error(`ファイル ${exportId} の sha256 が記録と違う`);
  // 今の決まりで確かめ直す (直す前に作った・確かめたファイルも、名前 = コードなどを含めば配らない。確かめを押していなくても)
  const unsafe = unsafeReason(ex, await exportRowsOf(db, ex.export_id), bytes);
  if (unsafe) return { state: 'unsafe', reason: unsafe, file_name: fileNameOf(ex), bytes: null };
  return { state: e.state, file_name: fileNameOf(ex), bytes };
}

// ─────────── 書く ───────────
/**
 * CSV を作る。(種類・列) の対象の承認を、コードの順に上限まで (試し用 = 5 行)。fingerprints を渡すとその承認だけ (全部が対象でなければ作らない)
 * @returns {{ export, left: number }}
 */
export async function createExport(db, { actor, kind, col, fingerprints = null, encoding = 'utf8', nowMs = Date.now() }) {
  if (!actor || typeof actor !== 'string') throw new DecideError('作る人 (メール) が無い');
  const spec = specOf(String(kind), String(col));
  if (!spec) throw new DecideError('CSV にできない種類・列', 'col_not_csv');
  if (!ENCODINGS.includes(encoding)) throw new DecideError('文字コードは utf8 だけ (Shift_JIS はまだ)', 'encoding_not_supported');
  if (fingerprints != null) {
    if (!Array.isArray(fingerprints) || !fingerprints.length || fingerprints.length > MAX_ROWS) throw new DecideError(`fingerprints は 1〜${MAX_ROWS} 件の配列`);
    if (fingerprints.some((f) => !FP_RE.test(String(f)))) throw new DecideError('指紋の形が違う');
    if (new Set(fingerprints).size !== fingerprints.length) throw new DecideError('同じ指紋が 2 回');
  }
  await requireApplied(db);
  return tx(db, async () => {
    needToday(await todayRun(db, nowMs));
    // 1 回目の読み = 錠をかける候補を決める → 候補の行を錠 → 2 回目の読みで判定 (錠の後に照合の完了が書かれていれば落とす)
    const first = (await readFixNeUnits(db, fingerprints)).filter((u) => u.col === spec.col);
    await lockCandidates(db, first.map((u) => u.fingerprint));
    // 照合の回も候補の行を取った後に読み直す (その間に新しい回が入っていれば、古い回で判定しない。#1495 Codex R1 High)
    const tr = await todayRun(db, nowMs);
    needToday(tr);
    const resv = await reservations(db, { write: true });
    const nc = await readNeCodes(db);
    const judged = (await readFixNeUnits(db, fingerprints)).map((u) => judge(u, { run: tr.run, reservations: resv, neCodes: nc }));
    const mine = judged.filter((j) => j.key === `${spec.kind}:${spec.col}`);
    if (fingerprints) {
      const byFp = new Map(judged.map((j) => [j.fingerprint, j]));
      const bad = fingerprints.map((f) => ({ fingerprint: f, j: byFp.get(f) })).filter((x) => !x.j || x.j.status !== 'csv' || x.j.key !== `${spec.kind}:${spec.col}`)
        .map((x) => ({ fingerprint: x.fingerprint, status: !x.j ? 'not_fix_ne' : x.j.status === 'csv' ? 'other_column' : x.j.status, reason: x.j?.reason ?? null, export_id: x.j?.export_id ?? null }));
      if (bad.length) throw Object.assign(new DecideError(`CSV にできない承認がある (${bad.length} 件)`, 'not_eligible'), { details: bad });
    }
    const ok = mine.filter((j) => j.status === 'csv');
    if (!ok.length) throw new DecideError('この列に CSV にできる承認がありません', 'nothing_to_export');
    const verified = (await verifiedSet(db)).ok.has(verifiedKey(spec, encoding));
    const limit = verified ? MAX_ROWS : TRIAL_ROWS;
    if (fingerprints && ok.length > limit) throw new DecideError(`${verified ? '' : '実機で確かめる前は試し用 = '}1 つのファイルに ${limit} 行まで`, verified ? 'too_many' : 'trial_limit');
    const take = ok.slice(0, limit);
    const csv = buildCsv(spec, take);
    const ex = (await db.query(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by, created_at)
      values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) returning ${EXPORT_COLS}`,
    [spec.kind, spec.col, spec.ne, CONVERTER_VERSION, encoding, !verified, take.length, csv.sha256, csv.bytes, tr.run, actor, iso(nowMs)])).rows[0];
    const exportId = Number(ex.export_id);
    for (const j of take) {
      const prev = (await db.query(`select max(row_id) as id from ops.ne_csv_export_rows where approved_event_id = $1`, [j.approved_event_id])).rows[0].id;
      await db.query(`insert into ops.ne_csv_export_rows (export_id, source, approved_event_id, fingerprint, code_norm, col, child, ne_code, target, cell, prev_row_id)
        values ($1, 'fix_ne', $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)`,
      [exportId, j.approved_event_id, j.fingerprint, j.code_norm, j.col, j.child, j.ne_code, JSON.stringify(j.target), j.cell, prev == null ? null : Number(prev)]);
    }
    return { export: publicExport(shapeExport(ex), nowMs), left: ok.length - take.length };
  });
}

/** ファイルを void にして、残っている予約を外す (取引の中で) */
async function voidIn(db, exportId, reason, actor, nowMs) {
  await db.query(`update ops.ne_csv_exports set state = 'void', void_at = $2, void_reason = $3, void_by = $4 where export_id = $1`, [exportId, iso(nowMs), reason, actor]);
  await db.query(`update ops.ne_csv_export_rows set reserved = false, released_at = $2, release_reason = 'void' where export_id = $1 and reserved`, [exportId, iso(nowMs)]);
}
async function lockExport(db, exportId) {
  const e = (await db.query(`select ${EXPORT_COLS} from ops.ne_csv_exports where export_id = $1 for update`, [exportId])).rows[0];
  if (!e) throw new DecideError('ファイルが無い', 'not_found');
  return shapeExport(e);
}
const needId = (id) => { if (!Number.isInteger(id) || id <= 0) throw new DecideError('export_id が違う'); };

/**
 * 取り込む直前に確かめる。全部の行がまだ対象 (承認がその単位の最後の判断・未完了・今日の回に出ている・同じ値) なら checked (今日のうちだけ有効)。
 * 1 行でも外れたら、そのファイルは void (予約を外す = 作り直せる)。結果は passed (HTTP の ok とは別。外れたのは通信の失敗ではない)
 */
export async function checkExport(db, { actor, exportId, nowMs = Date.now() }) {
  if (!actor) throw new DecideError('確かめる人 (メール) が無い');
  needId(exportId);
  await requireApplied(db);
  return tx(db, async () => {
    const e = await lockExport(db, exportId);
    if (e.state === 'void') throw new DecideError('このファイルは使えません (void)。作り直してください', 'void');
    if (e.state === 'declared') throw new DecideError('取り込んだと申告したファイルは確かめ直さない', 'already_declared');
    needToday(await todayRun(db, nowMs));
    const rows = (await db.query(`select ${ROW_COLS} from ops.ne_csv_export_rows where export_id = $1 order by row_id`, [exportId])).rows.map(shapeRow);
    // 今の決まりで確かめ直す (直す前に作ったファイル = 名前 = コードなど)。外れたら void (作り直せる)
    const bytesRow = (await db.query('select file_bytes from ops.ne_csv_exports where export_id = $1', [exportId])).rows[0];
    const unsafe = unsafeReason(e, rows, bytesRow ? Buffer.from(bytesRow.file_bytes) : null);
    if (unsafe) {
      await voidIn(db, exportId, 'check_failed', actor, nowMs);
      return { passed: false, voided: true, failures: rows.map((r) => ({ row_id: r.row_id, code_norm: r.code_norm, fingerprint: r.fingerprint, reason: `unsafe:${unsafe}` })), run: null };
    }
    await lockCandidates(db, rows.map((r) => r.fingerprint).filter(Boolean));
    // 照合の回は候補の行を取った後に読む (その間に入った新しい回で判定する。#1495 Codex R1 High)。この後に入った回は、申告のときに照らす
    const tr = await todayRun(db, nowMs);
    needToday(tr);
    const resv = await reservations(db);
    const nc = await readNeCodes(db);
    const byFp = new Map((await readFixNeUnits(db, rows.map((r) => r.fingerprint))).map((u) => [u.fingerprint, judge(u, { run: tr.run, reservations: resv, ownExport: exportId, neCodes: nc })]));
    const failures = [];
    for (const r of rows) {
      const j = byFp.get(r.fingerprint);
      let reason = null;
      if (!r.reserved) reason = `released:${r.release_reason}`;
      else if (!j) reason = 'not_latest_approval';                          // その単位の最後の判断が、この承認の fix_ne ではなくなった
      else if (j.approved_event_id !== r.approved_event_id) reason = 'not_latest_approval';
      else if (j.status !== 'csv') reason = j.status === 'waiting' || j.status === 'ne_screen' ? `${j.status}:${j.reason}` : j.status;
      else if (j.key !== `${e.kind}:${e.col}` || j.cell !== r.cell || j.ne_code !== r.ne_code) reason = 'value_changed';
      if (reason) failures.push({ row_id: r.row_id, code_norm: r.code_norm, fingerprint: r.fingerprint, reason });
    }
    if (failures.length) {
      await voidIn(db, exportId, 'check_failed', actor, nowMs);
      return { passed: false, voided: true, failures, run: tr.run };
    }
    await db.query(`update ops.ne_csv_exports set state = 'checked', checked_at = $2, checked_run = $3, checked_by = $4 where export_id = $1`, [exportId, iso(nowMs), tr.run, actor]);
    return { passed: true, voided: false, failures: [], run: tr.run };
  });
}

export const RESULTS = Object.freeze(['ok', 'partial', 'rejected_all']);
/**
 * NE に取り込んだと申告する。今日確かめて、その後に新しい照合の回が無い (checked) ファイルだけ。ok / partial = declared (翌朝から行ごとに届いたかを見る) / rejected_all = void (予約を外す)。
 * 申告したファイルにもう一度申告 = 試みを足すだけ (最初の申告の時刻は動かさない)。まだ使えるとき (declaredUsable) だけ
 */
export async function declareExport(db, { actor, exportId, result, note = null, nowMs = Date.now() }) {
  if (!actor) throw new DecideError('申告する人 (メール) が無い');
  needId(exportId);
  if (!RESULTS.includes(result)) throw new DecideError(`結果は ${RESULTS.join(' / ')} のどれか`);
  if (note != null && (typeof note !== 'string' || note.length > 500)) throw new DecideError('メモは 500 字まで');
  await requireApplied(db);
  return tx(db, async () => {
    const e = await lockExport(db, exportId);
    if (e.state === 'void') throw new DecideError('このファイルは使えません (void)。取り込んでしまったなら、つかいかたの「void のファイルを取り込んでしまったら」を見てください', 'void');
    if (e.state === 'made') throw new DecideError('先に「取り込む直前に確かめる」を押してください', 'not_checked');
    if (e.state === 'checked' && jstDate(e.checked_ms) !== jstDate(nowMs)) throw new DecideError('確かめたのが今日ではありません。もう一度確かめてください', 'check_stale');
    const tr = await todayRun(db, nowMs);
    if (e.state === 'checked' && tr.run !== e.checked_run) throw new DecideError('確かめた後に新しい照合がありました。もう一度確かめてください', 'check_stale');
    if (e.state === 'declared' && !declaredUsable(e, { run: tr.run, released: await releasedExports(db, [exportId]), nowMs })) {
      throw new DecideError('この申告済みのファイルはもう使えません (次の日になった・新しい照合があった・行の予約が外れた)。取り込み直すなら作り直してください', 'retired');
    }
    // 今の決まりで確かめ直す (直す前に確かめたファイルを申告で通さない)
    const unsafe = unsafeReason(e, await exportRowsOf(db, exportId));
    if (unsafe) throw new DecideError(`このファイルは今の決まりでは使えません (${unsafe})。取り込まないで、作り直してください`, 'unsafe');
    await db.query(`insert into ops.ne_csv_attempts (export_id, declared_by, declared_at, result, note) values ($1, $2, $3, $4, $5)`, [exportId, actor, iso(nowMs), result, note || null]);
    if (e.state === 'declared') return { state: 'declared', first_declared_at: e.declared_at };
    if (result === 'rejected_all') { await voidIn(db, exportId, 'rejected_all', actor, nowMs); return { state: 'void' }; }
    await db.query(`update ops.ne_csv_exports set state = 'declared', declared_at = $2, declared_by = $3 where export_id = $1`, [exportId, iso(nowMs), actor]);
    return { state: 'declared' };
  });
}

/** 使わないファイルを void にする (申告したファイルは不可) */
export async function voidExport(db, { actor, exportId, nowMs = Date.now() }) {
  if (!actor) throw new DecideError('操作する人 (メール) が無い');
  needId(exportId);
  await requireApplied(db);
  return tx(db, async () => {
    const e = await lockExport(db, exportId);
    if (e.state === 'void') throw new DecideError('もう void です', 'void');
    if (e.state === 'declared') throw new DecideError('取り込んだと申告したファイルは void にしない (行ごとに翌朝の照合で見る)', 'already_declared');
    await voidIn(db, exportId, 'by_user', actor, nowMs);
    return { state: 'void' };
  });
}

/** 実機で確かめた結果を残す (種類・列・文字コード。見出しと変換の版は今のコードの値) */
export async function recordVerified(db, { actor, kind, col, encoding = 'utf8', result, note = null, exportId = null }) {
  if (!actor) throw new DecideError('確かめた人 (メール) が無い');
  const spec = specOf(String(kind), String(col));
  if (!spec) throw new DecideError('CSV にできない種類・列', 'col_not_csv');
  if (!ENCODINGS.includes(encoding)) throw new DecideError('文字コードは utf8 だけ', 'encoding_not_supported');
  if (!['ok', 'ng'].includes(result)) throw new DecideError('結果は ok / ng');
  if (note != null && (typeof note !== 'string' || note.length > 500)) throw new DecideError('メモは 500 字まで');
  if (exportId != null) needId(exportId);
  await requireApplied(db);
  return tx(db, async () => {
    if (exportId != null) {
      const e = (await db.query(`select kind, col, encoding, converter_version from ops.ne_csv_exports where export_id = $1`, [exportId])).rows[0];
      if (!e) throw new DecideError('ファイルが無い', 'not_found');
      if (e.kind !== spec.kind || e.col !== spec.col || e.encoding !== encoding || e.converter_version !== CONVERTER_VERSION) throw new DecideError('ファイルの種類・列・文字コード・変換の版が違う', 'export_mismatch');
    }
    const r = (await db.query(`insert into ops.ne_csv_verified (export_id, kind, col, encoding, header, converter_version, result, note, verified_by)
      values ($1, $2, $3, $4, $5, $6, $7, $8, $9) returning verified_id`, [exportId, spec.kind, spec.col, encoding, headerOf(spec).join(','), CONVERTER_VERSION, result, note || null, actor])).rows[0];
    return { verified_id: Number(r.verified_id) };
  });
}
