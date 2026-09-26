/**
 * dump.mjs — Company DB (PostgreSQL) の論理バックアップと復元。Company DB構想 03 §8 / 06 §12 (Codex の条件「Render 外バックアップ + 復元訓練」)
 *
 * なぜ自前か: Render の Web Service に `pg_dump` があるとは限らず、miniPC (Windows) にも入っていない。
 *   Node だけで完結する形にして、どこからでも同じ手順で取れる・戻せるようにする。
 *
 * 形式 (テキスト。gzip して保存する):
 *   -- company-db-dump-v2
 *   -- generated_at: 2026-09-10T01:00:00.000Z
 *   -- session: DateStyle=ISO, YMD | IntervalStyle=iso_8601 | TimeZone=UTC | extra_float_digits=3
 *   -- migrations: 0001,0002,...
 *   -- tables: 72
 *   -- sequence: "core"."products_product_id_seq" next=5320
 *   -- table: "core"."products" (25 cols)
 *   COPY "core"."products" ("product_id", "company_id", ...) FROM stdin;
 *   5319\t1\t...            ← COPY TEXT 形式 (タブ区切り、\N = null、\\ \t \n \r をエスケープ)
 *   \.
 *   -- end: "core"."products" rows=5319
 *   ...
 *   -- total_rows: 61234
 *   -- end_of_dump
 *
 * 約束 (Codex レビュー 2026-09-10 を反映):
 *   - 取得は 1 つの `repeatable read read only` トランザクション。途中で誰かが書いても、ある一瞬の姿がまるごと取れる
 *   - 表は **カーソル** で先頭から 1 回だけ読む。`limit … offset …` で読むと、ページごとに先頭から読み直すので行数の 2 乗で遅くなる
 *     (2026-09-22 に Amazon の注文 130 万件が入ってから 30 分で読み終わらず、毎回 `Connection terminated` で打ち切られていた)
 *   - 取得・復元の両方で DateStyle / IntervalStyle / TimeZone / extra_float_digits を固定する (設定差で日付が入れ替わらない)
 *   - 値は Postgres に `::text` で吐かせ、復元は text で渡して型変換も Postgres に任せる (JS で解釈しない)
 *   - 表は OID で扱い、名前は必ず引用する (search_path や大文字・記号を含む名前でも壊れない)
 *   - 生成列は入れない。identity 列は `overriding system value` で元の ID のまま入れる
 *   - **採番の次の値**も記録する (identity も serial も)。復元では `alter sequence ... restart with`(巻き戻せる) で戻す。
 *     `setval` は使わない (rollback されない)。値は **文字列のまま** 運ぶ (JS の Number に入れると 2^53 を超えたところで桁が落ちる)
 *   - 復元の前に、ダンプの採番の集合と復元先の集合を **完全照合** する。足りない・知らないものがあれば何も消さずに止まる
 *     (採番が抜けたまま戻すと、そのあと最初の insert が主キー重複で落ちる)
 *   - 自己参照の列 (`parent_product_id` 等) は一旦 null で入れ、全部入ってから UPDATE で埋める
 *   - パーティションの子は取らない (親から取り、親へ戻す。振り分けは Postgres がやる)
 *   - 復元は **入れ替え** = 対象の表を消してから入れる。その間は対象表のユーザー trigger を全部止め、終わったら元の状態に戻す
 *     (append-only だけでなく、文言の履歴を守る trigger や updated_at を触る trigger も止める)
 *   - trigger の停止・復帰は **パーティションの子孫まで 1 つずつ** `alter table only` で行う。
 *     親にまとめて `enable` をかけると子にも波及し、わざと止めてあった子の trigger まで動き出してしまう
 *   - 復元の前に、復元先を **ダンプとは別に数え上げて** 表と列の顔ぶれを突き合わせる。
 *     片側にしかない表・列があれば拒否 (ダンプに無い表は古いまま残り、ダンプに無い列は null で埋まるため)
 *   - `ops.schema_migrations` は置換しない (復元先の履歴を巻き戻さない)。ダンプの migrations と復元先が **完全一致** でなければ拒否
 *   - 復元の前にダンプを厳密に検証する (末尾の印が最後の非空行であること・表数・表ごとの行数・列数・表と採番の重複)。
 *     1 つでも合わなければ何も消さずに止まる
 *   - 検証も復元も **1 行ずつ** 読む (DumpScanner)。ダンプ全体を 1 つの文字列にしない (Node の文字列は約 512 MB が上限。
 *     2026-09-26 の Company DB は gzip 前で 1.4 GB)。復元はファイルを 2 回読む (1 回目 = 検証だけ / 2 回目 = 流し込み)
 */
import zlib from 'node:zlib';
import fs from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { StringDecoder } from 'node:string_decoder';
import crypto from 'node:crypto';

export const DUMP_VERSION = 'company-db-dump-v2';
export const SCHEMAS = ['core', 'raw', 'snapshots', 'events', 'ai', 'docs', 'ops'];   // mart は view なので取らない
/** 復元しない表 (復元先の履歴を巻き戻さない) */
export const SKIP_RESTORE = ['"ops"."schema_migrations"'];
const READ_CHUNK = 5000;
const WRITE_CHUNK = 500;
/** 取得も復元も同じ設定で (設定差で値が変わらないように) */
const SESSION = [
  ["datestyle", 'ISO, YMD'],
  ['intervalstyle', 'iso_8601'],
  ['timezone', 'UTC'],
  ['extra_float_digits', '3'],
];
const sessionLine = () => SESSION.map(([k, v]) => `${k}=${v}`).join(' | ');
async function applySession(db) { for (const [k, v] of SESSION) await db.query(`set local ${k} = '${v}'`); }

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

export const quoteIdent = (s) => `"${String(s).replace(/"/g, '""')}"`;
export const quoteTable = (schema, name) => `${quoteIdent(schema)}.${quoteIdent(name)}`;
/** `"a"."b"` / `"a"."b" ("c", "d")` の引用を解く */
export function parseQuotedList(s) {
  const out = []; let i = 0;
  while (i < s.length) {
    while (i < s.length && (s[i] === ' ' || s[i] === ',' || s[i] === '.')) i++;
    if (i >= s.length) break;
    if (s[i] !== '"') throw new Error(`引用されていない識別子: ${s.slice(i, i + 20)}`);
    i++; let cur = '';
    for (;;) {
      if (i >= s.length) throw new Error('引用が閉じていない');
      if (s[i] === '"') { if (s[i + 1] === '"') { cur += '"'; i += 2; continue; } i++; break; }
      cur += s[i]; i++;
    }
    out.push(cur);
  }
  return out;
}

/** 対象の表を依存順 (親 → 子) に。OID で突き合わせるので search_path に依存しない。パーティションの子は除く */
export async function listTables(db) {
  const rows = (await db.query(`
    select c.oid::bigint as oid, n.nspname as schema, c.relname as name
    from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where c.relkind in ('r', 'p') and not c.relispartition and n.nspname = any($1::text[])
    order by n.nspname, c.relname`, [SCHEMAS])).rows.map((r) => ({ oid: String(r.oid), schema: r.schema, name: r.name, qualified: quoteTable(r.schema, r.name) }));
  const byOid = new Map(rows.map((r) => [r.oid, r]));
  const deps = new Map(rows.map((r) => [r.oid, new Set()]));
  for (const r of (await db.query(`
    select con.conrelid::bigint as child, con.confrelid::bigint as parent
    from pg_constraint con where con.contype = 'f' and con.conrelid <> con.confrelid`)).rows) {
    const child = String(r.child); const parent = String(r.parent);
    if (byOid.has(child) && byOid.has(parent)) deps.get(child).add(parent);
  }
  const out = []; const done = new Set();
  let guard = rows.length + 5;
  while (out.length < rows.length && guard-- > 0) {
    for (const r of rows) {
      if (done.has(r.oid)) continue;
      if ([...deps.get(r.oid)].every((p) => done.has(p))) { out.push(r); done.add(r.oid); }
    }
  }
  const left = rows.filter((r) => !done.has(r.oid));
  if (left.length) throw Object.assign(new Error(`外部キーが循環していて順序を決められない: ${left.map((r) => r.qualified).join(', ')}`), { code: 'DUMP_FK_CYCLE' });
  return out;
}

/** 表の列 (生成列を除く)・identity 列・自己参照の列 (null 許容のみ)・主キー */
export async function tableMeta(db, table) {
  const cols = (await db.query(`
    select a.attname as column_name, a.attidentity <> '' as is_identity
    from pg_attribute a
    where a.attrelid = $1::oid and a.attnum > 0 and not a.attisdropped and a.attgenerated = ''
    order by a.attnum`, [table.oid])).rows;
  const self = (await db.query(`
    select a.attname as column_name
    from pg_constraint con join pg_attribute a on a.attrelid = con.conrelid and a.attnum = any(con.conkey)
    where con.contype = 'f' and con.conrelid = con.confrelid and con.conrelid = $1::oid and not a.attnotnull`, [table.oid])).rows.map((r) => r.column_name);
  const pk = (await db.query(`
    select a.attname as column_name
    from pg_constraint con join pg_attribute a on a.attrelid = con.conrelid and a.attnum = any(con.conkey)
    where con.contype = 'p' and con.conrelid = $1::oid order by array_position(con.conkey, a.attnum)`, [table.oid])).rows.map((r) => r.column_name);
  return { columns: cols.map((c) => c.column_name), identity: cols.filter((c) => c.is_identity).map((c) => c.column_name), selfRefs: [...new Set(self)], primaryKey: pk };
}

/**
 * 表が所有するシーケンス (identity も serial も) と「次に返す値」。
 * 値は bigint なので **文字列のまま** 扱う (Number にすると 2^53 を超えたところで壊れる)
 */
export async function listSequences(db, tables) {
  const out = [];
  for (const t of tables) {
    const seqs = (await db.query(`
      select ns.nspname as schema, s.relname as name
      from pg_class s
      join pg_depend d on d.objid = s.oid and d.classid = 'pg_class'::regclass and d.deptype in ('a','i')
      join pg_namespace ns on ns.oid = s.relnamespace
      where s.relkind = 'S' and d.refobjid = $1::oid
      order by 1, 2`, [t.oid])).rows;
    for (const q of seqs) {
      const seq = quoteTable(q.schema, q.name);
      const row = (await db.query(`select last_value::text as last_value, is_called from ${seq}`)).rows[0];
      const incRow = (await db.query('select increment_by::text as inc from pg_sequences where schemaname = $1 and sequencename = $2', [q.schema, q.name])).rows[0];
      const inc = BigInt(incRow?.inc ?? '1');
      const next = row.is_called ? BigInt(row.last_value) + inc : BigInt(row.last_value);
      out.push({ sequence: seq, next: next.toString(), table: t.qualified });
    }
  }
  return out;
}

/** 対象表 (と、そのパーティションの子孫) のユーザー trigger の状態。親への ENABLE/DISABLE は子に波及するので、子も含めて覚えておく */
export async function listTriggers(db, oid) {
  return (await db.query(`
    with recursive tree as (
      select $1::oid as oid
      union all
      select i.inhrelid from pg_inherits i join tree on i.inhparent = tree.oid
    )
    select (c.relnamespace::regnamespace)::text as schema_raw, n.nspname as schema, c.relname as table_name, t.tgname, t.tgenabled
    from tree join pg_class c on c.oid = tree.oid
    join pg_namespace n on n.oid = c.relnamespace
    join pg_trigger t on t.tgrelid = c.oid and not t.tgisinternal
    order by 2, 3, 4`, [oid])).rows.map((r) => ({ qualified: quoteTable(r.schema, r.table_name), name: r.tgname, enabled: r.tgenabled }));
}

/**
 * ダンプを作る。write(line) は 1 行ずつ受け取る (改行は付けない)。
 * 取得はまるごと 1 つの読み取り専用トランザクションで行う (途中で書かれても一貫した姿になる)
 */
export async function dumpCompanyDb(db, write, { log = () => {} } = {}) {
  await db.exec('begin isolation level repeatable read read only');
  try {
    await applySession(db);
    const migrations = (await db.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);
    const tables = await listTables(db);
    const sequences = await listSequences(db, tables);
    await write(`-- ${DUMP_VERSION}`);
    await write(`-- generated_at: ${new Date().toISOString()}`);
    await write(`-- session: ${sessionLine()}`);
    await write(`-- migrations: ${migrations.join(',')}`);
    await write(`-- tables: ${tables.length}`);
    for (const s of sequences) await write(`-- sequence: ${s.sequence} next=${s.next}`);
    const summary = []; let totalRows = 0;
    for (const t of tables) {
      const meta = await tableMeta(db, t);
      const cols = meta.columns;
      const selectList = cols.map((c) => `${quoteIdent(c)}::text as ${quoteIdent(c)}`).join(', ');
      const orderBy = meta.primaryKey.length ? ` order by ${meta.primaryKey.map(quoteIdent).join(', ')}` : '';
      await write(`-- table: ${t.qualified} (${cols.length} cols)`);
      await write(`COPY ${t.qualified} (${cols.map(quoteIdent).join(', ')}) FROM stdin;`);
      let rows = 0;
      // カーソルで 1 回だけ走査する (offset は読み飛ばす行も毎回読むので、大きい表で行数の 2 乗になる)
      await db.exec(`declare backup_dump_cursor no scroll cursor for select ${selectList} from ${t.qualified}${orderBy}`);
      for (;;) {
        const page = (await db.query(`fetch forward ${READ_CHUNK} from backup_dump_cursor`)).rows;
        for (const r of page) await write(encodeRow(cols.map((c) => r[c])));
        rows += page.length;
        if (page.length < READ_CHUNK) break;
      }
      await db.exec('close backup_dump_cursor');
      await write('\\.');
      await write(`-- end: ${t.qualified} rows=${rows}`);
      summary.push({ table: t.qualified, rows });
      totalRows += rows;
      if (rows) log(`dump ${t.qualified}: ${rows}`);
    }
    await write(`-- total_rows: ${totalRows}`);
    await write('-- end_of_dump');
    await db.exec('commit');
    return { tables: summary, totalRows, migrations, sequences };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗 */ }
    throw e;
  }
}

/**
 * ダンプを 1 行ずつ厳密に読む判定器。1 つでも辻褄が合わなければ投げる。
 * 🚨 ダンプ全体を 1 つの文字列にしない (Node の文字列は約 512 MB が上限。2026-09-26 の Company DB は gzip 前で 1.4 GB)。
 *    文字列版 (parseDump / verifyDumpText / restoreCompanyDb) もファイル版もこれを通す = 判定の規則は 1 か所だけ。
 * onRow(table, row) を渡すと、行を解いて渡す (渡さなければ列の数だけ数えて、値は解かない = 速い)。
 * push(line) を全行に呼んだあと finish() で { header, tables: [{ table, columns, rows (行数) }] } を返す。
 */
export class DumpScanner {
  constructor({ onRow = null, onTable = null } = {}) {
    this.onRow = onRow; this.onTable = onTable;   // onTable(table) = COPY 行を読んだとき (列の並びを確かめる用)
    this.header = { sequences: [] };
    this.tables = []; this.seen = new Set();
    this.cur = null; this.pendingEnd = null; this.ended = false; this.lineNo = 0;
  }

  push(line) {
    if (this.lineNo++ === 0) {
      if (!line || line.trim() !== `-- ${DUMP_VERSION}`) throw Object.assign(new Error(`ダンプの版が違う (先頭行が ${DUMP_VERSION} でない)`), { code: 'DUMP_VERSION' });
      return;
    }
    if (this.pendingEnd) {   // 直前の行が \. = この行は "-- end:" でなければならない
      const t = this.pendingEnd; this.pendingEnd = null;
      const m = /^-- end: (.+) rows=(\d+)$/.exec(line);
      if (!m) throw Object.assign(new Error(`${t.table}: \\. の次に "-- end:" が無い`), { code: 'DUMP_PARSE' });
      if (m[1] !== t.table) throw Object.assign(new Error(`表の終わりが食い違う: ${t.table} vs ${m[1]}`), { code: 'DUMP_PARSE' });
      if (Number(m[2]) !== t.rows) throw Object.assign(new Error(`${t.table}: 行数が食い違う (書かれている ${m[2]} / 実際 ${t.rows})`), { code: 'DUMP_ROW_MISMATCH' });
      this.tables.push(t);
      return;
    }
    if (this.cur) {
      if (line === '\\.') { this.pendingEnd = this.cur; this.cur = null; return; }
      // 値の中のタブは \t に書き換えてあるので、生のタブは区切りだけ
      const row = this.onRow ? decodeRow(line) : null;
      const n = row ? row.length : line.split('\t').length;
      if (n !== this.cur.columns.length) throw Object.assign(new Error(`${this.cur.table}: 列の数が合わない行がある (${n} ≠ ${this.cur.columns.length})`), { code: 'DUMP_COLUMN_MISMATCH' });
      this.cur.rows++;
      if (row) this.onRow(this.cur, row);
      return;
    }
    if (line === '') return;
    if (this.ended) throw Object.assign(new Error('"-- end_of_dump" のあとに中身がある (継ぎ足された?)'), { code: 'DUMP_TRAILING' });
    const header = this.header;
    if (line === '-- end_of_dump') { this.ended = true; return; }
    if (line.startsWith('-- generated_at: ')) header.generatedAt = line.slice(17).trim();
    else if (line.startsWith('-- session: ')) header.session = line.slice(12).trim();
    else if (line.startsWith('-- migrations: ')) header.migrations = line.slice(15).trim().split(',').filter(Boolean);
    else if (line.startsWith('-- tables: ')) header.tables = Number(line.slice(11).trim());
    else if (line.startsWith('-- sequence: ')) {
      const m = /^-- sequence: (.+) next=(-?\d+)$/.exec(line);
      if (!m) throw Object.assign(new Error(`sequence 行を読めない: ${line.slice(0, 80)}`), { code: 'DUMP_PARSE' });
      // 引用を解いて組み直したものと一致しなければ拒否 (この文字列はあとで SQL に埋めるので、素性を確かめる)
      let parts = null;
      try { parts = parseQuotedList(m[1]); } catch { parts = null; }
      if (!parts || parts.length !== 2 || quoteTable(parts[0], parts[1]) !== m[1]) throw Object.assign(new Error(`sequence 名が "schema"."name" でない: ${m[1].slice(0, 80)}`), { code: 'DUMP_PARSE' });
      if (header.sequences.some((x) => x.sequence === m[1])) throw Object.assign(new Error(`同じシーケンスが 2 回出てくる: ${m[1]}`), { code: 'DUMP_DUPLICATE_SEQUENCE' });
      header.sequences.push({ sequence: m[1], next: m[2] });
    } else if (line.startsWith('-- total_rows: ')) header.totalRows = Number(line.slice(15).trim());
    else if (line.startsWith('COPY ')) {
      const m = /^COPY (".+?"\.".+?") \((.*)\) FROM stdin;$/.exec(line);
      if (!m) throw Object.assign(new Error(`COPY 行を読めない: ${line.slice(0, 80)}`), { code: 'DUMP_PARSE' });
      if (this.seen.has(m[1])) throw Object.assign(new Error(`同じ表が 2 回出てくる: ${m[1]}`), { code: 'DUMP_DUPLICATE_TABLE' });
      this.seen.add(m[1]);
      this.cur = { table: m[1], columns: parseQuotedList(m[2]), rows: 0 };
      if (this.onTable) this.onTable(this.cur);
    }
  }

  finish() {
    if (this.pendingEnd) throw Object.assign(new Error(`${this.pendingEnd.table}: \\. の次に "-- end:" が無い`), { code: 'DUMP_PARSE' });
    if (this.lineNo === 0) throw Object.assign(new Error(`ダンプの版が違う (先頭行が ${DUMP_VERSION} でない)`), { code: 'DUMP_VERSION' });
    if (this.cur) throw Object.assign(new Error(`\\. が無いまま終わった (${this.cur.table})`), { code: 'DUMP_TRUNCATED' });
    if (!this.ended) throw Object.assign(new Error('末尾の "-- end_of_dump" が無い (途中で切れている)'), { code: 'DUMP_TRUNCATED' });
    const { header, tables } = this;
    if (header.totalRows == null) throw Object.assign(new Error('"-- total_rows:" が無い'), { code: 'DUMP_TRUNCATED' });
    if (header.tables == null) throw Object.assign(new Error('"-- tables:" が無い'), { code: 'DUMP_TRUNCATED' });
    if (header.tables !== tables.length) throw Object.assign(new Error(`表の数が食い違う (書かれている ${header.tables} / 実際 ${tables.length})`), { code: 'DUMP_TABLE_COUNT' });
    const total = tables.reduce((n, t) => n + t.rows, 0);
    if (header.totalRows !== total) throw Object.assign(new Error(`合計行数が食い違う (書かれている ${header.totalRows} / 実際 ${total})`), { code: 'DUMP_TOTAL_MISMATCH' });
    return { header, tables };
  }
}

/** 文字列のダンプの行 (小さいダンプ・試験用) */
const textLines = (text) => text.split(/\r?\n/);
/**
 * gzip ファイルのダンプを 1 行ずつ (全体を展開して文字列にしない)。
 * 行の切り方は文字列版 (split(/\r?\n/)) と同じ = LF で切り、LF の直前の CR だけを落とす。最後の要素は改行が無くても (空でも) 返す。
 *   🚨 readline は単独の CR も改行にするので使わない (末尾が "-- end_of_dump\r" のダンプを、ファイル版だけ受け付けてしまう。Codex 2026-09-26)
 * 🚨 元のファイルのストリームも自分で持つ: 読めない (無い・途中の失敗) ときは呼び出し側へ投げ、
 *    途中で抜けたとき (検証の失敗・DB の失敗) もファイルを閉じ終わるまで待つ (.pipe はエラーも後始末も面倒を見ない。Codex 2026-09-26)
 */
async function* fileLines(file) {
  const src = fs.createReadStream(file);
  const gunzip = zlib.createGunzip();
  src.on('error', (e) => gunzip.destroy(e));   // 元のファイルの失敗を gunzip 経由で for await に届ける (拾われない error にしない)
  src.pipe(gunzip);
  const dec = new StringDecoder('utf8');
  let buf = '';
  try {
    for await (const chunk of gunzip) {
      buf += dec.write(chunk);
      const parts = buf.split('\n');
      buf = parts.pop();
      for (const p of parts) yield p.endsWith('\r') ? p.slice(0, -1) : p;
    }
    buf += dec.end();
    yield buf;   // split と同じく、最後の要素は必ず返す (改行で終わっていれば '')
  } finally {
    gunzip.destroy();
    if (!src.closed) await new Promise((res) => { src.once('close', res); src.destroy(); });
  }
}
/** 行の並び (同期でも非同期でも) を判定器に通す。hash を渡すと全行を足し込む (2 回読みの中身の照合用) */
async function scanLines(lines, opts, hash = null) {
  const sc = new DumpScanner(opts);
  for await (const line of lines) { if (hash) hash.update(line + '\n'); sc.push(line); }
  return sc.finish();
}

/** ダンプ文字列を厳密に解析する (小さいダンプ・試験用。行も全部持つ)。1 つでも辻褄が合わなければ投げる */
export function parseDump(text) {
  const rowsOf = new Map();
  const sc = new DumpScanner({ onRow: (t, row) => { let a = rowsOf.get(t.table); if (!a) rowsOf.set(t.table, (a = [])); a.push(row); } });
  for (const line of textLines(text)) sc.push(line);
  const { header, tables } = sc.finish();
  return { header, tables: tables.map((t) => ({ table: t.table, columns: t.columns, rows: rowsOf.get(t.table) || [] })) };
}

/**
 * 復元する (文字列のダンプ。小さいダンプ・試験用)。大きいダンプは restoreFromFile (1 行ずつ読む)
 */
export async function restoreCompanyDb(db, text, opts = {}) {
  return restoreFromLines(db, () => textLines(text), opts);
}

/**
 * 復元の本体。migrations 適用済みの DB が前提。1 トランザクション。
 * 対象の表を消してから入れる (入れ替え)。その間は対象表のユーザー trigger を止め、終わったら元の状態に戻す。
 * openLines() = ダンプの行を最初から返す (2 回呼ぶ):
 *   1 回目 = 全体の検証だけ (DB に触らない。おかしければ、ここで止まる = まだ何も消していない)
 *   2 回目 = 流し込み (行は持たずに WRITE_CHUNK 行ずつ insert)
 * 🚨 1 回目と 2 回目で行の中身が 1 行でも違えば取り消す (改行を LF にそろえた全行の sha256 を commit の前に照合。
 *    LF と CRLF の違いだけは同じ扱い = 戻る値は変わらない)。
 *    行数だけの照合だと、列の並び・値・採番だけが変わった差し替えを見逃し、1 回目の列の並びで 2 回目の値を入れてしまう (Codex 2026-09-26)
 */
export async function restoreFromLines(db, openLines, { log = () => {} } = {}) {
  const firstHash = crypto.createHash('sha256');
  const { header, tables } = await scanLines(openLines(), {}, firstHash);   // ← 何かおかしければ、ここで止まる (まだ何も消していない)
  const firstDigest = firstHash.digest('hex');
  await db.exec('begin');
  try {
    await applySession(db);
    // migrations は完全一致でなければ拒否 (古いダンプで新しいスキーマの履歴を巻き戻さない)
    const applied = (await db.query('select version from ops.schema_migrations order by version')).rows.map((r) => r.version);
    const want = [...(header.migrations || [])].sort();
    const have = [...applied].sort();
    if (want.join(',') !== have.join(',')) {
      throw Object.assign(new Error(`マイグレーションが一致しない\n  ダンプ: ${want.join(',') || '(なし)'}\n  復元先: ${have.join(',') || '(なし)'}\n  → 復元先を同じ版にしてから戻す`), { code: 'RESTORE_MIGRATIONS' });
    }
    const targets = tables.filter((t) => !SKIP_RESTORE.includes(t.table));
    // 🚨 復元先を **ダンプとは別に** 数え上げて、表の顔ぶれがぴったり同じか確かめる。
    //    ダンプ側だけを見ていると、ダンプに無い表は古い中身のまま残り (消えたことに気づけない)、
    //    ダンプに無い列は null で埋まる。どちらも「成功」に見えてしまう (Codex 2026-09-10)
    const live = await listTables(db);
    const liveOf = new Map(live.map((t) => [t.qualified, t]));
    const dumpNames = new Set(tables.map((t) => t.table));
    const onlyLive = live.map((t) => t.qualified).filter((n) => !dumpNames.has(n));
    const onlyDump = [...dumpNames].filter((n) => !liveOf.has(n));
    if (onlyLive.length || onlyDump.length) {
      throw Object.assign(new Error(`表の顔ぶれが合わない\n  ダンプに無い (復元先だけにある): ${onlyLive.join(', ') || '(なし)'}\n  復元先に無い: ${onlyDump.join(', ') || '(なし)'}`), { code: 'RESTORE_TABLE_MISMATCH' });
    }
    // 列も同じように、両側から突き合わせる (生成列はどちらにも入らない)
    const metaOf = new Map();
    for (const t of targets) {
      const table = liveOf.get(t.table);
      const meta = await tableMeta(db, table);
      const colOnlyLive = meta.columns.filter((c) => !t.columns.includes(c));
      const colOnlyDump = t.columns.filter((c) => !meta.columns.includes(c));
      if (colOnlyLive.length || colOnlyDump.length) {
        throw Object.assign(new Error(`${t.table}: 列の顔ぶれが合わない\n  ダンプに無い (復元先だけにある): ${colOnlyLive.join(', ') || '(なし)'}\n  復元先に無い: ${colOnlyDump.join(', ') || '(なし)'}`), { code: 'RESTORE_COLUMN_MISMATCH' });
      }
      const selfRefs = meta.selfRefs.filter((c) => t.columns.includes(c));
      const pk = meta.primaryKey.filter((c) => t.columns.includes(c));
      metaOf.set(t.table, {
        table, meta, selfRefs, pk,
        selfSet: new Set(meta.selfRefs),
        idx: Object.fromEntries(t.columns.map((c, i) => [c, i])),
        overriding: meta.identity.some((c) => t.columns.includes(c)) ? ' overriding system value' : '',
        selfRows: Object.fromEntries(selfRefs.map((c) => [c, []])),   // 自己参照を後で埋めるための (主キー, 値)
      });
    }
    // 採番の照合 (ダンプに足りない・知らないものがあれば、まだ何も消していないここで止まる)。
    // 数え上げは復元先の全表から (ダンプのヘッダも全表分を持っている)
    const wantSeq = new Map((header.sequences || []).map((s) => [s.sequence, s]));
    const haveSeq = new Map();
    for (const s of await listSequences(db, live)) haveSeq.set(s.sequence, s);
    const missingSeq = [...haveSeq.keys()].filter((k) => !wantSeq.has(k));
    const unknownSeq = [...wantSeq.keys()].filter((k) => !haveSeq.has(k));
    if (missingSeq.length || unknownSeq.length) {
      throw Object.assign(new Error(`シーケンスが合わない\n  ダンプに無い: ${missingSeq.join(', ') || '(なし)'}\n  復元先に無い: ${unknownSeq.join(', ') || '(なし)'}`), { code: 'RESTORE_SEQUENCE_MISMATCH' });
    }
    // ユーザー trigger を止める (append-only・履歴の保護・updated_at を触るもの、全部)。
    // 🚨 親への ENABLE/DISABLE はパーティションの子にも波及するので、子孫まで状態を覚えて ONLY で個別に操作する
    const triggerState = [];
    for (const t of targets) {
      const { table } = metaOf.get(t.table);
      const rows = await listTriggers(db, table.oid);
      if (!rows.length) continue;
      triggerState.push(...rows);
    }
    for (const q of [...new Set(triggerState.map((x) => x.qualified))]) await db.exec(`alter table only ${q} disable trigger user`);
    // 消す (子 → 親)
    for (const t of [...targets].reverse()) await db.query(`delete from ${t.table}`);
    // 入れる (親 → 子)。ダンプを最初からもう一度読み、WRITE_CHUNK 行ずつ流す (行は持たない)
    const targetNames = new Set(targets.map((t) => t.table));
    let pending = []; let pendingTable = null;
    const flush = async () => {
      if (!pending.length) return;
      const t = tables.find((x) => x.table === pendingTable);
      const m = metaOf.get(pendingTable);
      const params = [];
      const values = pending.map((row) => `(${t.columns.map((c) => { params.push(m.selfSet.has(c) ? null : row[m.idx[c]]); return `$${params.length}`; }).join(', ')})`).join(', ');
      await db.query(`insert into ${t.table} (${t.columns.map(quoteIdent).join(', ')})${m.overriding} values ${values}`, params);
      pending = [];
    };
    const changed = (why) => Object.assign(new Error(`1 回目と 2 回目でダンプの中身が違う (${why})。読んでいる間に差し替わった?`), { code: 'RESTORE_SOURCE_CHANGED' });
    const firstCols = new Map(tables.map((t) => [t.table, t.columns.join('\u0000')]));
    const sc = new DumpScanner({
      // 列の並びが 1 回目と違えば、その表の行を 1 行も入れる前に止める (idx は 1 回目の並びで作ってある)
      onTable: (cur) => { if (firstCols.get(cur.table) !== cur.columns.join('\u0000')) throw changed(`${cur.table} の列の並び`); },
      onRow: (cur, row) => {
        if (!targetNames.has(cur.table)) return;   // 復元しない表 (ops.schema_migrations)
        pendingTable = cur.table;
        pending.push(row);
        const m = metaOf.get(cur.table);
        for (const c of m.selfRefs) {
          if (row[m.idx[c]] !== null) m.selfRows[c].push({ value: row[m.idx[c]], pk: m.pk.map((k) => row[m.idx[k]]) });
        }
      },
    });
    const secondHash = crypto.createHash('sha256');
    for await (const line of openLines()) {
      secondHash.update(line + '\n');
      sc.push(line);
      if (pending.length >= WRITE_CHUNK || (pending.length && sc.cur?.table !== pendingTable)) await flush();
    }
    await flush();
    sc.finish();
    // 1 回目と 2 回目で中身が変わっていないか (値・採番・migrations の行だけの差し替えもここで捕まえる)
    if (secondHash.digest('hex') !== firstDigest) throw changed('全行の sha256 が一致しない');
    const summary = []; let totalRows = 0;
    for (const t of targets) {
      summary.push({ table: t.table, rows: t.rows });
      totalRows += t.rows;
      if (t.rows) log(`restore ${t.table}: ${t.rows}`);
    }
    // 自己参照を埋める (trigger は止まったまま = updated_at を書き換えない)
    const selfFix = [];
    for (const t of targets) {
      const m = metaOf.get(t.table);
      for (const c of m.selfRefs) {
        const rows = m.selfRows[c];
        if (!rows.length) continue;
        if (!m.pk.length) throw Object.assign(new Error(`${t.table}: 主キーが無いので自己参照 (${c}) を戻せない`), { code: 'RESTORE_NO_PK' });
        const where = m.pk.map((k, j) => `${quoteIdent(k)} = $${j + 2}`).join(' and ');
        for (const r of rows) await db.query(`update ${t.table} set ${quoteIdent(c)} = $1 where ${where}`, [r.value, ...r.pk]);
        selfFix.push({ table: t.table, column: c, rows: rows.length });
      }
    }
    // trigger を元の状態へ (commit 前)。親子それぞれに ONLY で戻す (O = ふつうに有効 / D = 無効 / R = replica / A = always)
    for (const tg of triggerState) {
      const verb = tg.enabled === 'D' ? 'disable trigger' : tg.enabled === 'R' ? 'enable replica trigger' : tg.enabled === 'A' ? 'enable always trigger' : 'enable trigger';
      await db.exec(`alter table only ${tg.qualified} ${verb} ${quoteIdent(tg.name)}`);
    }
    // シーケンスを戻す (alter sequence restart は巻き戻せる。setval は巻き戻せないので使わない)。値は文字列のまま
    for (const s of (header.sequences || [])) {
      if (!/^-?\d+$/.test(String(s.next))) throw Object.assign(new Error(`シーケンスの値が数字でない: ${s.sequence} next=${s.next}`), { code: 'RESTORE_SEQUENCE_VALUE' });
      await db.exec(`alter sequence ${s.sequence} restart with ${s.next}`);
    }
    // 行数の照合
    for (const t of targets) {
      const n = Number((await db.query(`select count(*)::bigint as n from ${t.table}`)).rows[0].n);
      if (n !== t.rows) throw Object.assign(new Error(`${t.table}: 復元後 ${n} 行 ≠ ダンプ ${t.rows} 行`), { code: 'RESTORE_ROW_MISMATCH' });
    }
    await db.exec('commit');
    return { tables: summary, totalRows, selfFix, generatedAt: header.generatedAt, skipped: tables.filter((t) => SKIP_RESTORE.includes(t.table)).map((t) => t.table) };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗 */ }
    throw e;
  }
}

/**
 * ダンプを gzip しながらファイルに書く (テキストのままのファイルは作らない)。書き込みの失敗も拾う。
 * 🚨 テキストのまま一度ディスクに置くと、Render のディスク (5 GB) に Company DB の大きさぶんの一時ファイルができる
 *    (満杯になると sessions.db に書けずログインできなくなる = 2026-07-12 の事故と同じ形)。
 * 戻り値の rawBytes = gzip する前の大きさ (manifest と gzip の検証に使う)
 * signal = 打ち切り (呼び出し側の時間切れ)。DB の接続を切るだけでは、書き出しの drain 待ちや終わりの待ちは解けない
 *   → gzip とファイルの書き出しも壊して、待っているところを全部解く (Codex 2026-09-25)
 */
export async function dumpToGzipFile(db, file, { level = 5, log = () => {}, signal } = {}) {
  const gz = zlib.createGzip({ level });
  const done = pipeline(gz, fs.createWriteStream(file), signal ? { signal } : {});
  let streamError = null;
  done.catch((e) => { streamError = streamError || e; });
  let rawBytes = 0;
  const write = async (line) => {
    if (streamError) throw streamError;
    if (signal?.aborted) throw signal.reason ?? new Error('打ち切られた');
    const buf = Buffer.from(line + '\n', 'utf-8');
    rawBytes += buf.length;
    if (!gz.write(buf)) {
      await new Promise((res, rej) => {
        const cleanup = () => { gz.off('drain', onDrain); gz.off('error', onErr); gz.off('close', onClose); };
        const onDrain = () => { cleanup(); res(); };
        const onErr = (e) => { cleanup(); rej(e); };
        const onClose = () => { cleanup(); rej(streamError || new Error('gzip の書き出しが途中で閉じた')); };
        gz.once('drain', onDrain); gz.once('error', onErr); gz.once('close', onClose);
      });
    }
  };
  let result;
  try {
    result = await dumpCompanyDb(db, write, { log });
  } finally {
    if (!gz.destroyed) gz.end();   // 打ち切りで壊れていれば end しない (pipeline がファイルも閉じる)
    await done.catch(() => {});
  }
  if (streamError) throw streamError;
  return { ...result, file, rawBytes, bytes: fs.statSync(file).size };
}

/**
 * ダンプを gzip でファイルに書く (CLI の dump)。gzip しながら書く = ダンプ全体を文字列にしない (dumpToGzipFile と同じ)。
 * 戻り値に gzip の sha256 を足す。
 * 🚨 同じディレクトリの一時ファイルに書き、最後まで取れてから保存先に置き換える。
 *    保存先へ直接書くと、同じパスへの取り直しが途中で落ちたとき、前の正常なダンプが書きかけで上書きされる (Codex 2026-09-26)
 */
export async function dumpToFile(db, file, { log = () => {} } = {}) {
  const tmp = `${file}.tmp-${process.pid}-${Date.now()}`;
  try {
    const r = await dumpToGzipFile(db, tmp, { level: 6, log });
    const hash = crypto.createHash('sha256');
    await pipeline(fs.createReadStream(tmp), hash);
    fs.renameSync(tmp, file);
    return { ...r, file, sha256: hash.digest('hex') };
  } finally {
    try { fs.unlinkSync(tmp); } catch { /* 置き換え済み = もう無い */ }
  }
}

/** gzip ファイルから復元する。1 行ずつ 2 回読む (1 回目 = 検証だけ / 2 回目 = 流し込み)。全体を文字列にしない */
export async function restoreFromFile(db, file, opts = {}) {
  return restoreFromLines(db, () => fileLines(file), opts);
}

const verifySummary = ({ header, tables }) => ({
  ok: true, generatedAt: header.generatedAt, session: header.session, migrations: header.migrations || [],
  sequences: (header.sequences || []).length,
  tables: tables.map((t) => ({ table: t.table, rows: t.rows })),
  totalRows: tables.reduce((n, t) => n + t.rows, 0), declaredTotal: header.totalRows,
});
/** ダンプが壊れていないかを見る (DB に触らない。文字列版 = 小さいダンプ・試験用) */
export function verifyDumpText(text) {
  const sc = new DumpScanner();
  for (const line of textLines(text)) sc.push(line);
  return verifySummary(sc.finish());
}
/** ダンプが壊れていないかを見る (DB に触らない)。1 行ずつ読む = 全体を文字列にしない */
export async function verifyDumpFile(file) {
  return verifySummary(await scanLines(fileLines(file)));
}
