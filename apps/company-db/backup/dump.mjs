/**
 * dump.mjs — Company DB (PostgreSQL) の論理バックアップと復元。Company DB構想 03 §8 / 06 §12 (Codex の条件「Render 外バックアップ + 復元訓練」)
 *
 * なぜ自前か: Render の Web Service に `pg_dump` があるとは限らず、miniPC (Windows) にも入っていない。
 *   Node だけで完結する形にして、どこからでも同じ手順で取れる・戻せるようにする。
 *
 * 形式 (テキスト。gzip して保存する):
 *   -- company-db-dump-v1
 *   -- generated_at: 2026-09-10T01:00:00.000Z
 *   -- migrations: 0001_...,0002_...
 *   -- table: core.products (25 cols) rows=5319
 *   COPY core.products (product_id, company_id, ...) FROM stdin;
 *   5319\t1\t...            ← COPY TEXT 形式 (タブ区切り、\N = null、\\ \t \n \r をエスケープ)
 *   \.
 *   -- end: core.products rows=5319
 *   ...
 *   -- total_rows: 61234
 *
 * 約束:
 *   - 値は Postgres に `::text` で吐かせる (配列・jsonb・timestamptz の表現を Postgres に任せる)。復元は text で渡して
 *     Postgres に列の型へ変換させる。JS 側で型を解釈しないので、桁落ちや時差のずれが起きない
 *   - 生成列 (code_norm / listing_norm / external_norm) は入れない (復元時に自動で入る)
 *   - identity 列は `overriding system value` で元の ID のまま入れ、最後に採番を進める (外部キーの整合が保たれる)
 *   - 自己参照の列 (products.parent_product_id 等) は一旦 null で入れ、全部入ってから UPDATE で埋める
 *   - パーティションの子は取らない (親から取り、親へ戻す。振り分けは Postgres がやる)
 *   - 復元先は「migrations 適用済み」であること (足りなければ止める)。migrations を流すと参照データ (会社・倉庫・解決規則) が
 *     入るので「完全に空」にはならない。だから復元は **入れ替え** = 対象の表を消してから入れる (1 トランザクション)。
 *     append-only の表は trigger を外して消し、終わったら戻す。誤操作を防ぐため CLI では `--yes` を必須にする
 *   - 取ったあと・戻したあとに行数を照合する (合わなければ失敗して巻き戻す)
 */
import zlib from 'node:zlib';
import fs from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

export const DUMP_VERSION = 'company-db-dump-v1';
export const SCHEMAS = ['core', 'raw', 'snapshots', 'events', 'ai', 'docs', 'ops'];   // mart は view なので取らない
const READ_CHUNK = 5000;
const WRITE_CHUNK = 500;

/** COPY TEXT 形式へ (null は \N) */
export function encodeCopyValue(v) {
  if (v === null || v === undefined) return '\\N';
  return String(v).replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t');
}
/** COPY TEXT 形式から (\N は null) */
export function decodeCopyValue(s) {
  if (s === '\\N') return null;
  let out = ''; let esc = false;
  for (const ch of s) {
    if (esc) { out += ch === 'n' ? '\n' : ch === 'r' ? '\r' : ch === 't' ? '\t' : ch === '\\' ? '\\' : ch; esc = false; }
    else if (ch === '\\') esc = true;
    else out += ch;
  }
  return out;
}
export const encodeRow = (values) => values.map(encodeCopyValue).join('\t');
export const decodeRow = (line) => line.split('\t').map(decodeCopyValue);

/** 対象の表を依存順 (親 → 子) に並べて返す。パーティションの子は除く */
export async function listTables(db) {
  const tables = (await db.query(`
    select n.nspname as schema, c.relname as name
    from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where c.relkind in ('r', 'p') and not c.relispartition and n.nspname = any($1::text[])
    order by 1, 2`, [SCHEMAS])).rows.map((r) => `${r.schema}.${r.name}`);
  const set = new Set(tables);
  // 親 → 子 の辺 (自己参照は無視)
  const deps = new Map(tables.map((t) => [t, new Set()]));
  for (const r of (await db.query(`
    select (con.conrelid::regclass)::text as child, (con.confrelid::regclass)::text as parent
    from pg_constraint con where con.contype = 'f' and con.conrelid <> con.confrelid`)).rows) {
    const child = normalizeName(r.child); const parent = normalizeName(r.parent);
    if (set.has(child) && set.has(parent)) deps.get(child).add(parent);
  }
  // トポロジカルソート (残った循環は名前順で足す = FK が deferrable でない循環は現状の DDL には無い)
  const out = []; const done = new Set();
  let guard = tables.length + 5;
  while (out.length < tables.length && guard-- > 0) {
    for (const t of tables) {
      if (done.has(t)) continue;
      if ([...deps.get(t)].every((p) => done.has(p) || p === t)) { out.push(t); done.add(t); }
    }
  }
  for (const t of tables) if (!done.has(t)) out.push(t);
  return out;
}
/** regclass の文字列は schema を省くことがあるので補う */
function normalizeName(s) { return s.includes('.') ? s : `public.${s}`; }

/** 表の列 (生成列を除く) と、identity 列・自己参照の列 */
export async function tableMeta(db, qualified) {
  const [schema, name] = splitName(qualified);
  const cols = (await db.query(`
    select column_name, is_identity from information_schema.columns
    where table_schema = $1 and table_name = $2 and is_generated <> 'ALWAYS' order by ordinal_position`, [schema, name])).rows;
  // 自己参照で「親を指す」列だけ (複合 FK の company_id のような not null の列は含めない。あれば入れた時点で落ちる)
  const self = (await db.query(`
    select a.attname as column_name
    from pg_constraint con
    join pg_attribute a on a.attrelid = con.conrelid and a.attnum = any(con.conkey)
    where con.contype = 'f' and con.conrelid = con.confrelid and con.conrelid = $1::regclass and not a.attnotnull`, [qualified])).rows.map((r) => r.column_name);
  const pk = (await db.query(`
    select a.attname as column_name
    from pg_constraint con join pg_attribute a on a.attrelid = con.conrelid and a.attnum = any(con.conkey)
    where con.contype = 'p' and con.conrelid = $1::regclass order by array_position(con.conkey, a.attnum)`, [qualified])).rows.map((r) => r.column_name);
  return {
    columns: cols.map((c) => c.column_name),
    identity: cols.filter((c) => c.is_identity === 'YES').map((c) => c.column_name),
    selfRefs: [...new Set(self)],
    primaryKey: pk,
  };
}
function splitName(qualified) { const i = qualified.indexOf('.'); return [qualified.slice(0, i), qualified.slice(i + 1)]; }
const quoteIdent = (s) => `"${String(s).replace(/"/g, '""')}"`;
const quoteQualified = (q) => splitName(q).map(quoteIdent).join('.');

/**
 * ダンプを作る。write(line) は 1 行ずつ受け取る (改行は付けない)
 * 戻り値 = { tables: [{table, rows}], totalRows, migrations }
 */
export async function dumpCompanyDb(db, write, { log = () => {} } = {}) {
  const migrations = (await db.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);
  const tables = await listTables(db);
  await write(`-- ${DUMP_VERSION}`);
  await write(`-- generated_at: ${new Date().toISOString()}`);
  await write(`-- migrations: ${migrations.join(',')}`);
  await write(`-- tables: ${tables.length}`);
  const summary = []; let totalRows = 0;
  for (const t of tables) {
    const meta = await tableMeta(db, t);
    const cols = meta.columns;
    const selectList = cols.map((c) => `${quoteIdent(c)}::text as ${quoteIdent(c)}`).join(', ');
    const orderBy = meta.primaryKey.length ? ` order by ${meta.primaryKey.map(quoteIdent).join(', ')}` : '';
    await write(`-- table: ${t} (${cols.length} cols)`);
    await write(`COPY ${t} (${cols.join(', ')}) FROM stdin;`);
    let rows = 0;
    for (;;) {
      const page = (await db.query(`select ${selectList} from ${quoteQualified(t)}${orderBy} limit ${READ_CHUNK} offset ${rows}`)).rows;
      for (const r of page) await write(encodeRow(cols.map((c) => r[c])));
      rows += page.length;
      if (page.length < READ_CHUNK) break;
    }
    await write('\\.');
    await write(`-- end: ${t} rows=${rows}`);
    summary.push({ table: t, rows });
    totalRows += rows;
    if (rows) log(`dump ${t}: ${rows}`);
  }
  await write(`-- total_rows: ${totalRows}`);
  return { tables: summary, totalRows, migrations };
}

/** ダンプ文字列を解析して { header, tables: [{table, columns, rows: [[...]]}] } に */
export function parseDump(text) {
  const lines = text.split(/\r?\n/);
  if (!lines[0] || !lines[0].startsWith(`-- ${DUMP_VERSION}`)) throw Object.assign(new Error('ダンプの版が違う (先頭行が合わない)'), { code: 'DUMP_VERSION' });
  const header = {};
  const tables = [];
  let cur = null;
  for (const line of lines) {
    if (cur) {
      if (line === '\\.') { tables.push(cur); cur = null; continue; }
      cur.rows.push(decodeRow(line));
      continue;
    }
    if (line.startsWith('-- generated_at: ')) header.generatedAt = line.slice(17).trim();
    else if (line.startsWith('-- migrations: ')) header.migrations = line.slice(15).trim().split(',').filter(Boolean);
    else if (line.startsWith('-- total_rows: ')) header.totalRows = Number(line.slice(15).trim());
    else if (line.startsWith('COPY ')) {
      const m = /^COPY ([^ ]+) \(([^)]*)\) FROM stdin;$/.exec(line);
      if (!m) throw Object.assign(new Error(`COPY 行を読めない: ${line.slice(0, 80)}`), { code: 'DUMP_PARSE' });
      cur = { table: m[1], columns: m[2].split(',').map((s) => s.trim()), rows: [] };
    }
  }
  if (cur) throw Object.assign(new Error(`\\. が無いまま終わった (${cur.table})`), { code: 'DUMP_TRUNCATED' });
  return { header, tables };
}

/**
 * 復元する。空の DB (migrations 適用済み) が前提。1 トランザクション
 * 戻り値 = { tables: [{table, rows}], totalRows }
 */
export async function restoreCompanyDb(db, text, { log = () => {}, keepExisting = false } = {}) {
  const { header, tables } = parseDump(text);
  const applied = (await db.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);
  const missing = (header.migrations || []).filter((v) => !applied.includes(v));
  if (missing.length) throw Object.assign(new Error(`復元先に足りないマイグレーションがある: ${missing.join(', ')} (先に migrate.mjs を流す)`), { code: 'RESTORE_MIGRATIONS' });
  await db.exec('begin');
  const disabled = [];
  try {
    if (!keepExisting) {
      // 入れ替え: 子 → 親 の順に消す。append-only の表は trigger を外してから (README「保持期間の整理は同じ tx で disable」)
      const order = tables.map((t) => t.table);
      const before = [];
      for (const t of order) before.push({ table: t, rows: Number((await db.query(`select count(*)::bigint as n from ${quoteQualified(t)}`)).rows[0].n) });
      const had = before.filter((x) => x.rows > 0);
      if (had.length) log(`入れ替え: ${had.length} 表 ${had.reduce((n, x) => n + x.rows, 0)} 行を消してから戻す`);
      for (const t of [...order].reverse()) {
        const hasTrigger = (await db.query(`select 1 from pg_trigger where tgrelid = $1::regclass and tgname = 'trg_append_only_row' and not tgisinternal`, [t])).rows.length > 0;
        if (hasTrigger) { await db.exec(`alter table ${quoteQualified(t)} disable trigger trg_append_only_row`); disabled.push(t); }
        await db.query(`delete from ${quoteQualified(t)}`);
      }
    }
    const summary = []; let totalRows = 0; const selfFix = [];
    for (const t of tables) {
      const meta = await tableMeta(db, t.table);
      const selfSet = new Set(meta.selfRefs);
      const idx = Object.fromEntries(t.columns.map((c, i) => [c, i]));
      const overriding = meta.identity.some((c) => t.columns.includes(c)) ? ' overriding system value' : '';
      const pk = meta.primaryKey.filter((c) => t.columns.includes(c));
      for (let i = 0; i < t.rows.length; i += WRITE_CHUNK) {
        const chunk = t.rows.slice(i, i + WRITE_CHUNK);
        const params = [];
        const values = chunk.map((row) => `(${t.columns.map((c) => {
          const v = selfSet.has(c) ? null : row[idx[c]];
          params.push(v); return `$${params.length}`;
        }).join(', ')})`).join(', ');
        await db.query(`insert into ${quoteQualified(t.table)} (${t.columns.map(quoteIdent).join(', ')})${overriding} values ${values}`, params);
      }
      // 自己参照の列は全部入ってから埋める
      for (const c of meta.selfRefs) {
        if (!t.columns.includes(c) || !pk.length) continue;
        const rows = t.rows.filter((row) => row[idx[c]] !== null);
        for (let i = 0; i < rows.length; i += WRITE_CHUNK) {
          const chunk = rows.slice(i, i + WRITE_CHUNK);
          for (const row of chunk) {
            const where = pk.map((k, j) => `${quoteIdent(k)} = $${j + 2}`).join(' and ');
            await db.query(`update ${quoteQualified(t.table)} set ${quoteIdent(c)} = $1 where ${where}`, [row[idx[c]], ...pk.map((k) => row[idx[k]])]);
          }
        }
        if (rows.length) selfFix.push({ table: t.table, column: c, rows: rows.length });
      }
      summary.push({ table: t.table, rows: t.rows.length });
      totalRows += t.rows.length;
      if (t.rows.length) log(`restore ${t.table}: ${t.rows.length}`);
    }
    // identity の採番を進める (次の insert が既存 ID とぶつからないように)
    for (const t of tables) {
      const meta = await tableMeta(db, t.table);
      for (const c of meta.identity) {
        if (!t.columns.includes(c)) continue;
        await db.query(`select setval(pg_get_serial_sequence($1, $2), coalesce((select max(${quoteIdent(c)}) from ${quoteQualified(t.table)}), 0) + 1, false)`, [t.table, c]);
      }
    }
    // 外した trigger を戻す (commit の前に)
    for (const t of disabled) await db.exec(`alter table ${quoteQualified(t)} enable trigger trg_append_only_row`);
    disabled.length = 0;
    // 行数の照合
    if (!keepExisting) {
      for (const t of tables) {
        const n = Number((await db.query(`select count(*)::bigint as n from ${quoteQualified(t.table)}`)).rows[0].n);
        if (n !== t.rows.length) throw Object.assign(new Error(`${t.table}: 復元後 ${n} 行 ≠ ダンプ ${t.rows.length} 行`), { code: 'RESTORE_ROW_MISMATCH' });
      }
    }
    if (header.totalRows != null && header.totalRows !== totalRows) throw Object.assign(new Error(`合計 ${totalRows} 行 ≠ ダンプの total_rows ${header.totalRows}`), { code: 'RESTORE_TOTAL_MISMATCH' });
    await db.exec('commit');
    return { tables: summary, totalRows, selfFix, generatedAt: header.generatedAt };
  } catch (e) {
    try { for (const t of disabled) await db.exec(`alter table ${quoteQualified(t)} enable trigger trg_append_only_row`); } catch { /* rollback で戻る */ }
    try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗 */ }
    throw e;
  }
}

/** ダンプをテキストのままファイルに書く (gzip しない)。render-backup のように後で gzip する側で使う */
export async function dumpToRawFile(db, file, { log = () => {} } = {}) {
  const out = fs.createWriteStream(file, { encoding: 'utf-8' });
  const write = async (line) => { if (!out.write(line + '\n')) await new Promise((res) => out.once('drain', res)); };
  let result;
  try {
    result = await dumpCompanyDb(db, write, { log });
  } finally {
    await new Promise((res, rej) => out.end((e) => (e ? rej(e) : res())));
  }
  return { ...result, file, bytes: fs.statSync(file).size };
}

/** ダンプを gzip でファイルに書く。戻り値 = { file, bytes, sha256, ...dumpResult } */
export async function dumpToFile(db, file, { log = () => {} } = {}) {
  const chunks = [];
  const result = await dumpCompanyDb(db, (line) => chunks.push(line, '\n'), { log });
  const text = chunks.join('');
  const gz = zlib.gzipSync(Buffer.from(text, 'utf-8'), { level: 6 });
  fs.writeFileSync(file, gz);
  const sha256 = (await import('node:crypto')).createHash('sha256').update(gz).digest('hex');
  return { ...result, file, bytes: gz.length, rawBytes: Buffer.byteLength(text, 'utf-8'), sha256 };
}

/** gzip ファイルから復元する */
export async function restoreFromFile(db, file, opts = {}) {
  const text = zlib.gunzipSync(fs.readFileSync(file)).toString('utf-8');
  return restoreCompanyDb(db, text, opts);
}

/** ダンプの中身を読まずに検証する (壊れていないか・行数が合うか) */
export function verifyDumpText(text) {
  const { header, tables } = parseDump(text);
  const total = tables.reduce((n, t) => n + t.rows.length, 0);
  const ok = header.totalRows == null || header.totalRows === total;
  return { ok, generatedAt: header.generatedAt, migrations: header.migrations || [], tables: tables.map((t) => ({ table: t.table, rows: t.rows.length })), totalRows: total, declaredTotal: header.totalRows ?? null };
}
export async function verifyDumpFile(file) {
  return verifyDumpText(zlib.gunzipSync(fs.readFileSync(file)).toString('utf-8'));
}
void pipeline; void Readable;
