/**
 * po-settings-snapshot.mjs — 発注の設定の読むだけの写し (発注アプリの SQLite → Company DB の purchasing・0076・AI_reference CompanyDB構想/21 v4 の C2)
 *
 * 正本は発注アプリ (Render の warehouse-mirror.db の po_*・D-44・中原さんの決定 1 = a)。Company DB は「前の夜の値」の写しを持つだけ (AI・照合が読む)。
 * いつ: 夜間ロード (apps/company-db/nightly.mjs = company-db-nightly-load) の後ろの段 (新しい定期実行は作らない)。本体のロードとは**別の接続・別の取引**
 *   = 写しが失敗しても本体のロードは戻らない (段の失敗 = ジョブの ping が fail)。
 * 約束 (Codex R3 M2・M3・M4):
 *   1. 読む = SQLite の **1 つの読む取引** で 3 つの表 (po_product_attrs・po_order_conditions・po_material_groups) を読む (同じ時点の値)。
 *      表がある・要る列がある・読めた・件数・hash を世代 (purchasing.snapshot_runs) に残す
 *   2. **完全に読めた世代だけ** 入れ替える (差分 = 足す・直す・消す)。不完全 (表が無い・列が無い・読めない・前の世代より大きく減った) = 入れ替えない
 *      (前の世代のまま・rejected の世代を残す・段の失敗)。前と同じ hash = unchanged (行は触らない)
 *   3. 書くのは表の持ち主のロール (夜間ロードの COMPANY_DB_URL) だけ。画面・見張り・AI は読むだけ
 *   4. 写しの日付 = read_at (SQLite を読んだ時刻)。mart.v_product_360 の po_settings_read_at / po_settings_age_hours (決定 5 = b: 古くても使う・時刻を見せる)
 *   5. 🆕 Codex #1708 R1 M1: **PG の鍵を取ってから SQLite を読む** (同時に 2 本 = 読みも順番 = 古い読みが新しい写しを上書きしない)。
 *      それでも同じ時刻 (ミリ秒) で中身の違う読み = あいまい = 断る (ambiguous_read)・前の世代より前の読み = 断る (older_read)
 *   6. 🆕 M2: 減りの受け入れ = env PO_SNAPSHOT_ACCEPT_SHRINK に「表:件数」(例 attrs:10) = **1 回だけ** (使った回を世代の override に残し、同じ値は 2 回目から効かない)
 *   7. 🆕 M3: 正規化した後の鍵の重なり (大文字小文字・前後の空白だけ違う商品) = 不完全とみなして断る (duplicate_key・黙って捨てない)
 * 手で 1 回だけ流す (復元の後など・本番ランナーは流さない) = Render の Shell で `node apps/company-db/load/po-settings-snapshot.mjs --apply` (README「バックアップと復元」)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from '../../../scripts/company-db/migrate.mjs';

/** 前の世代よりこの割合を下回って減った表 = 不完全とみなす (途中の移行・壊れた読み) */
export const SHRINK_RATIO = 0.5;
/** 減り方を見る下限 (前の世代がこれより少ない表は見ない = 始めたばかり・小さい表) */
export const SHRINK_MIN_PREV = 20;
const TABLES = Object.freeze({
  attrs: { table: 'po_product_attrs', key: 'product_key',
    cols: ['product_key', 'product_code', 'order_lot', 'condition_id', 'material_group_id', 'capacity_per_unit', 'case_group', 'case_lot', 'created_via', 'created_at', 'updated_at'] },
  conditions: { table: 'po_order_conditions', key: 'condition_id',
    cols: ['condition_id', 'supplier_code', 'maker_name', 'display_name', 'condition_type', 'condition_value', 'unit', 'created_at', 'updated_at'] },
  materials: { table: 'po_material_groups', key: 'group_id', cols: ['group_id', 'name', 'min_order_qty', 'unit', 'created_at', 'updated_at'] },
});
const sha = (rows) => crypto.createHash('sha256').update(JSON.stringify(rows)).digest('hex');
const runIdOf = (d = new Date()) => `pos_${d.toISOString().replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`;

/**
 * SQLite を 1 つの読む取引で読む。opts.betweenTables = 試験だけ (1 つめの表を読んだ後に呼ぶ = 同じ時点で読めているかを確かめる)。
 * 戻り値 { readAt, rows: { attrs, conditions, materials }, counts, hashes, problems: [文字], sourceMaxUpdatedAt }
 */
export function readPoSettingsSnapshot(file, { betweenTables = null } = {}) {
  const problems = [];
  const rows = { attrs: [], conditions: [], materials: [] };
  let readAt = new Date();
  if (!fs.existsSync(file)) return { readAt, rows, counts: {}, hashes: {}, problems: [`no_file: ${path.basename(file)} が無い`], sourceMaxUpdatedAt: null };
  let db;
  try {
    db = new Database(file, { readonly: true, fileMustExist: true });
    db.pragma('busy_timeout = 5000');
    db.transaction(() => {
      readAt = new Date();
      let first = true;
      for (const [k, t] of Object.entries(TABLES)) {
        const exists = db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(t.table);
        if (!exists) { problems.push(`no_table: ${t.table}`); continue; }
        const have = new Set(db.prepare(`PRAGMA table_info(${t.table})`).all().map((c) => c.name));
        const miss = t.cols.filter((c) => !have.has(c));
        if (miss.length) { problems.push(`no_column: ${t.table}.${miss.join(',')}`); continue; }
        rows[k] = db.prepare(`SELECT ${t.cols.join(', ')} FROM ${t.table} ORDER BY ${t.key}`).all();
        if (first && betweenTables) betweenTables();
        first = false;
      }
    })();
  } catch (e) {
    problems.push(`read_error: ${String((e && e.message) || e).slice(0, 200)}`);
  } finally {
    try { if (db) db.close(); } catch { /* */ }
  }
  const counts = Object.fromEntries(Object.entries(rows).map(([k, v]) => [k, v.length]));
  const hashes = Object.fromEntries(Object.entries(rows).map(([k, v]) => [k, sha(v)]));
  let max = null;
  for (const v of Object.values(rows)) for (const r of v) if (r.updated_at && (!max || String(r.updated_at) > max)) max = String(r.updated_at);
  return { readAt, rows, counts, hashes, problems, sourceMaxUpdatedAt: max ? max.slice(0, 40) : null };
}

const num = (v) => (v == null || v === '' ? null : Number(v));
const txt = (v) => (v == null ? null : String(v));
/** 写しの表の行 (比べる形 = 数は Number・文字は String) */
const SHAPES = {
  attrs: { table: 'purchasing.sku_order_settings', key: 'product_key',
    of: (r, x) => ({ product_key: String(r.product_key).trim().toLowerCase(), product_code: String(r.product_code), sku_id: x.skuOf(r.product_code), order_lot: num(r.order_lot),
      condition_id: txt(r.condition_id), material_group_id: txt(r.material_group_id), capacity_per_unit: num(r.capacity_per_unit), case_group: txt(r.case_group), case_lot: num(r.case_lot),
      primary_supplier_code: x.supplierOf(r.product_code), created_via: txt(r.created_via), source_created_at: txt(r.created_at), source_updated_at: txt(r.updated_at) }),
    types: { product_key: 'text', product_code: 'text', sku_id: 'bigint', order_lot: 'numeric', condition_id: 'text', material_group_id: 'text', capacity_per_unit: 'numeric', case_group: 'text',
      case_lot: 'numeric', primary_supplier_code: 'text', created_via: 'text', source_created_at: 'text', source_updated_at: 'text' } },
  conditions: { table: 'purchasing.order_condition_groups', key: 'condition_id',
    of: (r) => ({ condition_id: String(r.condition_id), supplier_code: txt(r.supplier_code), maker_name: txt(r.maker_name), display_name: txt(r.display_name), condition_type: txt(r.condition_type),
      condition_value: num(r.condition_value), unit: txt(r.unit), source_created_at: txt(r.created_at), source_updated_at: txt(r.updated_at) }),
    types: { condition_id: 'text', supplier_code: 'text', maker_name: 'text', display_name: 'text', condition_type: 'text', condition_value: 'numeric', unit: 'text', source_created_at: 'text', source_updated_at: 'text' } },
  materials: { table: 'purchasing.material_groups', key: 'group_id',
    of: (r) => ({ group_id: String(r.group_id), name: txt(r.name), min_order_qty: num(r.min_order_qty), unit: txt(r.unit), source_created_at: txt(r.created_at), source_updated_at: txt(r.updated_at) }),
    types: { group_id: 'text', name: 'text', min_order_qty: 'numeric', unit: 'text', source_created_at: 'text', source_updated_at: 'text' } },
};
const sameRow = (a, b, types) => Object.keys(types).every((k) => (types[k] === 'numeric' || types[k] === 'bigint' ? num(a[k]) === num(b[k]) : txt(a[k]) === txt(b[k])));

/** 正規化した後の鍵 (attrs = 前後の空白を除いた小文字 = 写しの鍵・グループ = そのまま)。重なり = [鍵] (M3) */
const keyOf = { attrs: (r) => String(r.product_key).trim().toLowerCase(), conditions: (r) => String(r.condition_id), materials: (r) => String(r.group_id) };
function duplicateKeys(rows) {
  const out = {};
  for (const k of Object.keys(keyOf)) {
    const seen = new Set(); const dup = new Set();
    for (const r of rows[k] || []) { const key = keyOf[k](r); if (seen.has(key)) dup.add(key); seen.add(key); }
    if (dup.size) out[k] = [...dup].sort();
  }
  return out;
}
const sameHashes = (a, b) => Object.keys(TABLES).every((k) => (a && a[k]) === (b && b[k]));

/**
 * 写しを入れる (1 つの Postgres の取引・表の持ち主のロール)。
 * source = 読む関数 (() => readPoSettingsSnapshot の答え) = **鍵を取ってから読む** (段の道) / 読んだ答え (試験・前に読んだ値を入れてみる)。
 * opts.shrinkOverride = parseShrinkOverride の答え (減りの受け入れ・1 回だけ)。
 * 戻り値 { ok, status: applied | unchanged | rejected, run_id, counts, changes?, problems, snap, override? }
 */
export async function applyPoSettingsSnapshot(db, source, { host = null, shrinkOverride = null } = {}) {
  const runId = runIdOf();
  await db.query('begin');
  try {
    await db.query(`select pg_advisory_xact_lock(hashtext('purchasing.snapshot'))`);   // 同時に 2 本 = 順番 (読みも鍵の後 = 後の方は前の写しより後の値を読む)
    const snap = typeof source === 'function' ? source() : source;
    const prev = (await db.query(`select run_id, counts, hashes, read_at from purchasing.snapshot_runs where status in ('applied', 'unchanged') order by read_at desc, finished_at desc limit 1`)).rows[0] || null;
    const problems = [...snap.problems];
    // 前の世代より古い読み (遅れて来た読み) = 戻さない (新しい値を古い値で上書きしない)。同じ時刻 (ミリ秒) で中身が違う = どちらが新しいか決められない = 断る
    if (prev && new Date(prev.read_at).getTime() > snap.readAt.getTime()) problems.push(`older_read: 今の写し (${new Date(prev.read_at).toISOString()}) より前に読んだ値`);
    else if (prev && new Date(prev.read_at).getTime() === snap.readAt.getTime() && !sameHashes(prev.hashes, snap.hashes)) {
      problems.push(`ambiguous_read: 今の写しと同じ時刻 (${snap.readAt.toISOString()}) で中身が違う読み (どちらが新しいか決められない)`);
    }
    // 正規化した後の鍵の重なり = 不完全 (黙って 1 行を捨てない・Codex #1708 R1 M3)
    for (const [k, keys] of Object.entries(duplicateKeys(snap.rows))) problems.push(`duplicate_key: ${TABLES[k].table} の鍵が正規化で重なる (${keys.slice(0, 5).join(', ')}${keys.length > 5 ? ` ほか ${keys.length - 5}` : ''})`);
    let override = null;
    if (!problems.length && prev) {
      const shrinks = [];
      for (const k of Object.keys(TABLES)) {
        const was = Number(prev.counts?.[k] ?? 0); const now = Number(snap.counts[k] ?? 0);
        if (was >= SHRINK_MIN_PREV && now < was * SHRINK_RATIO) shrinks.push({ table: k, was, now });
      }
      if (shrinks.length) {
        // 減りの受け入れ (1 回だけ): env の「表:件数」が減った表の全部と件数まで合い、同じ値をまだ使っていないときだけ
        let why = shrinkOverride ? null : '';
        if (shrinkOverride) {
          const used = (await db.query(`select 1 from purchasing.snapshot_runs where override ->> 'token' = $1 limit 1`, [shrinkOverride.token])).rows.length > 0;
          if (used) why = ' (受け入れの env は使用済み = 2 回目は効かない)';
          else if (!shrinks.every((x) => shrinkOverride.tables[x.table] === x.now)) why = ` (受け入れの env ${shrinkOverride.token} と表・件数が違う)`;
        }
        if (why === null) override = { kind: 'accept_shrink', token: shrinkOverride.token, tables: shrinkOverride.tables, accepted: shrinks };
        else for (const x of shrinks) problems.push(`shrink: ${TABLES[x.table].table} が ${x.was} → ${x.now} 行 (前の世代の ${Math.round(SHRINK_RATIO * 100)}% 未満 = 途中の読み・移行の途中かもしれない)${why}`);
      }
    }
    const base = [runId, snap.readAt.toISOString(), snap.sourceMaxUpdatedAt, JSON.stringify(snap.counts), JSON.stringify(snap.hashes), host ? String(host).slice(0, 100) : null];
    if (problems.length) {
      await db.query(`insert into purchasing.snapshot_runs (run_id, source, read_at, source_max_updated_at, counts, hashes, host, status, problems)
        values ($1, 'render-sqlite', $2::timestamptz, $3, $4::jsonb, $5::jsonb, $6, 'rejected', $7::jsonb)`, [...base, JSON.stringify(problems)]);
      await db.query('commit');
      return { ok: false, status: 'rejected', run_id: runId, counts: snap.counts, problems, snap };
    }
    // 結ぶ値 (写した時の Company DB の SKU と代表の仕入先)
    const codes = [...new Set(snap.rows.attrs.map((r) => String(r.product_code)))];
    const link = new Map((await db.query(`select x.code, s.sku_id::text as sku_id,
        (select sp.code from core.supplier_skus ss join core.suppliers sp on sp.supplier_id = ss.supplier_id where ss.sku_id = s.sku_id and ss.is_primary order by sp.code limit 1) as supplier
       from unnest($1::text[]) as x(code) join core.skus s on s.company_id = 1 and s.code_norm = core.norm_code(x.code)`, [codes])).rows.map((r) => [r.code, r]));
    const ctx = { skuOf: (c) => (link.get(String(c)) ? link.get(String(c)).sku_id : null), supplierOf: (c) => (link.get(String(c)) ? link.get(String(c)).supplier : null) };
    const want = Object.fromEntries(Object.entries(SHAPES).map(([k, sh]) => [k, snap.rows[k].map((r) => sh.of(r, ctx))]));
    // 前と同じ (SQLite の hash が同じ・結んだ値も同じ) = unchanged
    const changes = {};
    const plan = {};
    for (const [k, sh] of Object.entries(SHAPES)) {
      const cur = new Map((await db.query(`select * from ${sh.table}`)).rows.map((r) => [r[sh.key], r]));
      const up = []; let add = 0; let mod = 0;
      const seen = new Set();
      for (const r of want[k]) {
        if (seen.has(r[sh.key])) throw new Error(`duplicate_key: ${sh.table} ${r[sh.key]} (先に断っているはず)`);   // 上の duplicateKeys で断っている = ここには来ない
        seen.add(r[sh.key]);
        const c = cur.get(r[sh.key]);
        if (!c) { up.push(r); add++; } else if (!sameRow(c, r, sh.types)) { up.push(r); mod++; }
      }
      const del = [...cur.keys()].filter((key) => !seen.has(key));
      plan[k] = { up, del };
      changes[k] = { added: add, changed: mod, removed: del.length };
    }
    const nChanged = Object.values(changes).reduce((a, c) => a + c.added + c.changed + c.removed, 0);
    const status = nChanged === 0 ? 'unchanged' : 'applied';
    await db.query(`insert into purchasing.snapshot_runs (run_id, source, read_at, source_max_updated_at, counts, hashes, host, status, changes, override)
      values ($1, 'render-sqlite', $2::timestamptz, $3, $4::jsonb, $5::jsonb, $6, $7, $8::jsonb, $9::jsonb)`, [...base, status, JSON.stringify(changes), override ? JSON.stringify(override) : null]);
    for (const [k, sh] of Object.entries(SHAPES)) {
      const { up, del } = plan[k];
      const cols = Object.keys(sh.types);
      if (del.length) await db.query(`delete from ${sh.table} where ${sh.key} = any($1::text[])`, [del]);
      for (let i = 0; i < up.length; i += 500) {
        const chunk = up.slice(i, i + 500);
        await db.query(`insert into ${sh.table} (${cols.join(', ')}, run_id)
          select ${cols.map((c) => `x.${c}`).join(', ')}, $2 from jsonb_to_recordset($1::jsonb) as x(${cols.map((c) => `${c} ${sh.types[c]}`).join(', ')})
          on conflict (${sh.key}) do update set ${cols.filter((c) => c !== sh.key).map((c) => `${c} = excluded.${c}`).join(', ')}, run_id = excluded.run_id`, [JSON.stringify(chunk), runId]);
      }
    }
    // 入れた後の件数 = 読んだ件数 (足りない = 取り消す)
    for (const [k, sh] of Object.entries(SHAPES)) {
      const n = Number((await db.query(`select count(*)::int as n from ${sh.table}`)).rows[0].n);
      if (n !== want[k].length) throw new Error(`count_mismatch: ${sh.table} ${n} 行 (読んだ ${want[k].length})`);
    }
    await db.query('commit');
    return { ok: true, status, run_id: runId, counts: snap.counts, changes, problems: [], snap, override };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    throw e;
  }
}

/**
 * 減った表を受け入れる 1 回だけの印 (本当に発注アプリで半分以上消した = 人が決めた時だけ・Codex #1708 R1 M2)。
 * env PO_SNAPSHOT_ACCEPT_SHRINK = 「表:今の件数」をカンマで (表 = attrs / conditions / materials。例 attrs:1520)。
 * 減った表が全部、ちょうどその件数のときだけ 1 回受け入れる。使った回は世代の override に残る = 同じ値は 2 回目から効かない (env を消し忘れても次は守る)。
 * 読めない値 = null (受け入れない)
 */
export function parseShrinkOverride(env = process.env) {
  const raw = String(env.PO_SNAPSHOT_ACCEPT_SHRINK || '').trim();
  if (!raw || raw.length > 200) return null;
  const tables = {};
  for (const part of raw.split(',')) {
    const m = /^\s*(attrs|conditions|materials)\s*:\s*(\d{1,7})\s*$/.exec(part);
    if (!m || Object.hasOwn(tables, m[1])) return null;
    tables[m[1]] = Number(m[2]);
  }
  return { token: raw, tables };
}

/**
 * 夜間ロードの後ろの段。DATA_DIR の warehouse-mirror.db を読んで、持ち主のロールで入れる。
 * 戻り値 { ok, note, status? }。0076 の前 (purchasing の表が無い) = 飛ばす (ok)
 */
export async function runPoSettingsSnapshotStep({ url, dataDir = process.env.DATA_DIR, connect = openPgClient, log = () => {}, host = 'render-nightly', read = readPoSettingsSnapshot } = {}) {
  if (!url) return { ok: false, note: 'COMPANY_DB_URL が無い' };
  if (!dataDir) return { ok: false, note: 'DATA_DIR が無い' };
  let client;
  try {
    client = await connect(url, { application_name: 'company-db-nightly-po-snapshot' });
    if (client.on) client.on('error', (e) => log(`接続のエラー: ${e.message}`));
    const db = client.query && client.exec ? client : pgAdapter(client);
    await db.query(`set statement_timeout = '120s'`);
    if (!(await db.query(`select to_regclass('purchasing.snapshot_runs') is not null as ok`)).rows[0].ok) return { ok: true, skipped: true, note: '0076 の前 (写しの表が無い)' };
    // 鍵を取ってから読む (Codex #1708 R1 M1)
    const r = await applyPoSettingsSnapshot(db, () => read(path.join(dataDir, 'warehouse-mirror.db')), { host, shrinkOverride: parseShrinkOverride() });
    const snap = r.snap;
    const c = snap.counts;
    const at = snap.readAt.toISOString().slice(0, 16);
    if (!r.ok) return { ok: false, status: r.status, note: `入れ替えない (前の世代のまま): ${r.problems.join(' / ').slice(0, 140)}` };
    const ch = r.changes || {};
    const d = Object.values(ch).reduce((a, x) => ({ add: a.add + x.added, mod: a.mod + x.changed, del: a.del + x.removed }), { add: 0, mod: 0, del: 0 });
    const note = `${r.status === 'unchanged' ? '変化なし' : `+${d.add} ~${d.mod} -${d.del}`} (${c.attrs}/${c.conditions}/${c.materials} 行・${at}Z)${r.override ? '・減りを受け入れた (1 回だけ)' : ''}`;
    log(`発注の設定の写し: ${note}`);
    return { ok: true, status: r.status, note };
  } catch (e) {
    return { ok: false, note: `想定外: ${String((e && e.message) || e).slice(0, 140)}` };
  } finally {
    try { if (client && client.end) await client.end(); } catch { /* */ }
  }
}

// ─── 手で 1 回だけ流す (復元の後など)。本番ランナー (夜間ロード) は流さない = この段だけ ───
//   Render の Shell: node apps/company-db/load/po-settings-snapshot.mjs --apply   (COMPANY_DB_URL = 持ち主のロール・DATA_DIR = 発注アプリの SQLite)
const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  if (!process.argv.includes('--apply')) {
    console.log('使い方: node apps/company-db/load/po-settings-snapshot.mjs --apply  (発注の設定の写しを 1 回だけ入れる・夜間ロードの後ろの段と同じ・Render の Shell で)');
    process.exitCode = 2;
  } else {
    const r = await runPoSettingsSnapshotStep({ url: process.env.COMPANY_DB_URL, dataDir: process.env.DATA_DIR, host: 'manual', log: (m) => console.log(m) });
    console.log(JSON.stringify({ ok: r.ok, status: r.status ?? null, skipped: !!r.skipped, note: r.note }));
    process.exitCode = r.ok ? 0 : 1;
  }
}
