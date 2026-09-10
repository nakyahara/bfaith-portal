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
 *   - 取得・復元の両方で DateStyle / IntervalStyle / TimeZone / extra_float_digits を固定する (設定差で日付が入れ替わらない)
 *   - 値は Postgres に `::text` で吐かせ、復元は text で渡して型変換も Postgres に任せる (JS で解釈しない)
 *   - 表は OID で扱い、名前は必ず引用する (search_path や大文字・記号を含む名前でも壊れない)
 *   - 生成列は入れない。identity 列は `overriding system value` で元の ID のまま入れる
 *   - **シーケンスの次の値**も記録し、復元では `alter sequence ... restart with`(巻き戻せる) で戻す。`setval` は使わない (rollback されない)
 *   - 自己参照の列 (`parent_product_id` 等) は一旦 null で入れ、全部入ってから UPDATE で埋める
 *   - パーティションの子は取らない (親から取り、親へ戻す。振り分けは Postgres がやる)
 *   - 復元は **入れ替え** = 対象の表を消してから入れる。その間は対象表のユーザー trigger を全部止め、終わったら元の状態に戻す
 *     (append-only だけでなく、文言の履歴を守る trigger や updated_at を触る trigger も止める)
 *   - `ops.schema_migrations` は置換しない (復元先の履歴を巻き戻さない)。ダンプの migrations と復元先が **完全一致** でなければ拒否
 *   - 復元の前にダンプを厳密に検証する (末尾の印・表数・表ごとの行数・列数・表の重複)。1 つでも合わなければ何も消さずに止まる
 */
import zlib from 'node:zlib';
import fs from 'node:fs';

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

/** identity / serial のシーケンスと「次に返す値」 */
export async function listSequences(db, tables) {
  const out = [];
  for (const t of tables) {
    const meta = await tableMeta(db, t);
    for (const c of meta.identity) {
      const seq = (await db.query('select pg_get_serial_sequence($1, $2) as seq', [t.qualified, c])).rows[0].seq;
      if (!seq) continue;
      const row = (await db.query(`select last_value, is_called from ${seq}`)).rows[0];
      const inc = Number((await db.query('select increment_by from pg_sequences where schemaname || \'.\' || sequencename = $1 or quote_ident(schemaname) || \'.\' || quote_ident(sequencename) = $1', [seq])).rows[0]?.increment_by ?? 1);
      const next = row.is_called ? Number(row.last_value) + inc : Number(row.last_value);
      out.push({ sequence: seq, next });
    }
  }
  return out;
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
      for (;;) {
        const page = (await db.query(`select ${selectList} from ${t.qualified}${orderBy} limit ${READ_CHUNK} offset ${rows}`)).rows;
        for (const r of page) await write(encodeRow(cols.map((c) => r[c])));
        rows += page.length;
        if (page.length < READ_CHUNK) break;
      }
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

/** ダンプ文字列を厳密に解析する。1 つでも辻褄が合わなければ投げる */
export function parseDump(text) {
  const lines = text.split(/\r?\n/);
  if (!lines[0] || lines[0].trim() !== `-- ${DUMP_VERSION}`) throw Object.assign(new Error(`ダンプの版が違う (先頭行が ${DUMP_VERSION} でない)`), { code: 'DUMP_VERSION' });
  const header = { sequences: [] };
  const tables = []; const seen = new Set();
  let cur = null; let ended = false;
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (cur) {
      if (line === '\\.') {
        const endLine = lines[i + 1] || '';
        const m = /^-- end: (.+) rows=(\d+)$/.exec(endLine);
        if (!m) throw Object.assign(new Error(`${cur.table}: \\. の次に "-- end:" が無い`), { code: 'DUMP_PARSE' });
        if (m[1] !== cur.table) throw Object.assign(new Error(`表の終わりが食い違う: ${cur.table} vs ${m[1]}`), { code: 'DUMP_PARSE' });
        if (Number(m[2]) !== cur.rows.length) throw Object.assign(new Error(`${cur.table}: 行数が食い違う (書かれている ${m[2]} / 実際 ${cur.rows.length})`), { code: 'DUMP_ROW_MISMATCH' });
        tables.push(cur); cur = null; i++; continue;
      }
      const row = decodeRow(line);
      if (row.length !== cur.columns.length) throw Object.assign(new Error(`${cur.table}: 列の数が合わない行がある (${row.length} ≠ ${cur.columns.length})`), { code: 'DUMP_COLUMN_MISMATCH' });
      cur.rows.push(row);
      continue;
    }
    if (line === '' ) continue;
    if (line === '-- end_of_dump') { ended = true; continue; }
    if (line.startsWith('-- generated_at: ')) header.generatedAt = line.slice(17).trim();
    else if (line.startsWith('-- session: ')) header.session = line.slice(12).trim();
    else if (line.startsWith('-- migrations: ')) header.migrations = line.slice(15).trim().split(',').filter(Boolean);
    else if (line.startsWith('-- tables: ')) header.tables = Number(line.slice(11).trim());
    else if (line.startsWith('-- sequence: ')) {
      const m = /^-- sequence: (.+) next=(\d+)$/.exec(line);
      if (!m) throw Object.assign(new Error(`sequence 行を読めない: ${line.slice(0, 80)}`), { code: 'DUMP_PARSE' });
      header.sequences.push({ sequence: m[1], next: Number(m[2]) });
    } else if (line.startsWith('-- total_rows: ')) header.totalRows = Number(line.slice(15).trim());
    else if (line.startsWith('COPY ')) {
      const m = /^COPY (".+?"\.".+?") \((.*)\) FROM stdin;$/.exec(line);
      if (!m) throw Object.assign(new Error(`COPY 行を読めない: ${line.slice(0, 80)}`), { code: 'DUMP_PARSE' });
      if (seen.has(m[1])) throw Object.assign(new Error(`同じ表が 2 回出てくる: ${m[1]}`), { code: 'DUMP_DUPLICATE_TABLE' });
      seen.add(m[1]);
      cur = { table: m[1], columns: parseQuotedList(m[2]), rows: [] };
    }
  }
  if (cur) throw Object.assign(new Error(`\\. が無いまま終わった (${cur.table})`), { code: 'DUMP_TRUNCATED' });
  if (!ended) throw Object.assign(new Error('末尾の "-- end_of_dump" が無い (途中で切れている)'), { code: 'DUMP_TRUNCATED' });
  if (header.totalRows == null) throw Object.assign(new Error('"-- total_rows:" が無い'), { code: 'DUMP_TRUNCATED' });
  if (header.tables == null) throw Object.assign(new Error('"-- tables:" が無い'), { code: 'DUMP_TRUNCATED' });
  if (header.tables !== tables.length) throw Object.assign(new Error(`表の数が食い違う (書かれている ${header.tables} / 実際 ${tables.length})`), { code: 'DUMP_TABLE_COUNT' });
  const total = tables.reduce((n, t) => n + t.rows.length, 0);
  if (header.totalRows !== total) throw Object.assign(new Error(`合計行数が食い違う (書かれている ${header.totalRows} / 実際 ${total})`), { code: 'DUMP_TOTAL_MISMATCH' });
  return { header, tables };
}

/**
 * 復元する。migrations 適用済みの DB が前提。1 トランザクション。
 * 対象の表を消してから入れる (入れ替え)。その間は対象表のユーザー trigger を止め、終わったら元の状態に戻す
 */
export async function restoreCompanyDb(db, text, { log = () => {} } = {}) {
  const { header, tables } = parseDump(text);   // ← 何かおかしければ、ここで止まる (まだ何も消していない)
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
    // 表の実在と、行の列がその表にあるかを先に確かめる
    const metaOf = new Map();
    for (const t of targets) {
      const [schema, name] = parseQuotedList(t.table);
      const row = (await db.query(`select c.oid::bigint as oid from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 and c.relname = $2 and c.relkind in ('r','p')`, [schema, name])).rows[0];
      if (!row) throw Object.assign(new Error(`復元先に表が無い: ${t.table}`), { code: 'RESTORE_NO_TABLE' });
      const table = { oid: String(row.oid), schema, name, qualified: t.table };
      const meta = await tableMeta(db, table);
      for (const c of t.columns) if (!meta.columns.includes(c)) throw Object.assign(new Error(`${t.table}: 復元先に列が無い (${c})`), { code: 'RESTORE_NO_COLUMN' });
      metaOf.set(t.table, { table, meta });
    }
    // ユーザー trigger を止める (append-only・履歴の保護・updated_at を触るもの、全部)。元の状態は覚えておく
    const triggerState = [];
    for (const t of targets) {
      const { table } = metaOf.get(t.table);
      const rows = (await db.query(`select tgname, tgenabled from pg_trigger where tgrelid = $1::oid and not tgisinternal`, [table.oid])).rows;
      if (!rows.length) continue;
      triggerState.push({ qualified: t.table, triggers: rows.map((r) => ({ name: r.tgname, enabled: r.tgenabled })) });
      await db.exec(`alter table ${t.table} disable trigger user`);
    }
    // 消す (子 → 親)
    for (const t of [...targets].reverse()) await db.query(`delete from ${t.table}`);
    // 入れる (親 → 子)
    const summary = []; let totalRows = 0;
    for (const t of targets) {
      const { meta } = metaOf.get(t.table);
      const selfSet = new Set(meta.selfRefs);
      const idx = Object.fromEntries(t.columns.map((c, i) => [c, i]));
      const overriding = meta.identity.some((c) => t.columns.includes(c)) ? ' overriding system value' : '';
      for (let i = 0; i < t.rows.length; i += WRITE_CHUNK) {
        const chunk = t.rows.slice(i, i + WRITE_CHUNK);
        const params = [];
        const values = chunk.map((row) => `(${t.columns.map((c) => { params.push(selfSet.has(c) ? null : row[idx[c]]); return `$${params.length}`; }).join(', ')})`).join(', ');
        await db.query(`insert into ${t.table} (${t.columns.map(quoteIdent).join(', ')})${overriding} values ${values}`, params);
      }
      summary.push({ table: t.table, rows: t.rows.length });
      totalRows += t.rows.length;
      if (t.rows.length) log(`restore ${t.table}: ${t.rows.length}`);
    }
    // 自己参照を埋める (trigger は止まったまま = updated_at を書き換えない)
    const selfFix = [];
    for (const t of targets) {
      const { meta } = metaOf.get(t.table);
      const idx = Object.fromEntries(t.columns.map((c, i) => [c, i]));
      const pk = meta.primaryKey.filter((c) => t.columns.includes(c));
      for (const c of meta.selfRefs) {
        if (!t.columns.includes(c)) continue;
        const rows = t.rows.filter((row) => row[idx[c]] !== null);
        if (!rows.length) continue;
        if (!pk.length) throw Object.assign(new Error(`${t.table}: 主キーが無いので自己参照 (${c}) を戻せない`), { code: 'RESTORE_NO_PK' });
        for (const row of rows) {
          const where = pk.map((k, j) => `${quoteIdent(k)} = $${j + 2}`).join(' and ');
          await db.query(`update ${t.table} set ${quoteIdent(c)} = $1 where ${where}`, [row[idx[c]], ...pk.map((k) => row[idx[k]])]);
        }
        selfFix.push({ table: t.table, column: c, rows: rows.length });
      }
    }
    // trigger を元の状態へ (commit 前。個別に無効だったものは無効のまま)
    for (const st of triggerState) {
      await db.exec(`alter table ${st.qualified} enable trigger user`);
      for (const tg of st.triggers) {
        if (tg.enabled === 'D') await db.exec(`alter table ${st.qualified} disable trigger ${quoteIdent(tg.name)}`);
        else if (tg.enabled === 'R') await db.exec(`alter table ${st.qualified} enable replica trigger ${quoteIdent(tg.name)}`);
        else if (tg.enabled === 'A') await db.exec(`alter table ${st.qualified} enable always trigger ${quoteIdent(tg.name)}`);
      }
    }
    // シーケンスを戻す (alter sequence restart は巻き戻せる。setval は巻き戻せないので使わない)
    for (const s of (header.sequences || [])) {
      const exists = (await db.query('select to_regclass($1) as r', [s.sequence])).rows[0].r;
      if (!exists) { log(`シーケンスが無い (飛ばす): ${s.sequence}`); continue; }
      await db.exec(`alter sequence ${s.sequence} restart with ${Number(s.next)}`);
    }
    // 行数の照合
    for (const t of targets) {
      const n = Number((await db.query(`select count(*)::bigint as n from ${t.table}`)).rows[0].n);
      if (n !== t.rows.length) throw Object.assign(new Error(`${t.table}: 復元後 ${n} 行 ≠ ダンプ ${t.rows.length} 行`), { code: 'RESTORE_ROW_MISMATCH' });
    }
    await db.exec('commit');
    return { tables: summary, totalRows, selfFix, generatedAt: header.generatedAt, skipped: tables.filter((t) => SKIP_RESTORE.includes(t.table)).map((t) => t.table) };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗 */ }
    throw e;
  }
}

/** ダンプをテキストのままファイルに書く (gzip は呼び出し側)。書き込みの失敗も拾う */
export async function dumpToRawFile(db, file, { log = () => {} } = {}) {
  const out = fs.createWriteStream(file, { encoding: 'utf-8' });
  let streamError = null;
  out.on('error', (e) => { streamError = streamError || e; });
  const write = async (line) => {
    if (streamError) throw streamError;
    if (!out.write(line + '\n')) {
      await new Promise((res, rej) => {
        const onDrain = () => { out.off('error', onErr); res(); };
        const onErr = (e) => { out.off('drain', onDrain); rej(e); };
        out.once('drain', onDrain); out.once('error', onErr);
      });
    }
  };
  let result;
  try {
    result = await dumpCompanyDb(db, write, { log });
  } finally {
    await new Promise((res, rej) => out.end((e) => (e ? rej(e) : res())));
  }
  if (streamError) throw streamError;
  return { ...result, file, bytes: fs.statSync(file).size };
}

/** ダンプを gzip でファイルに書く */
export async function dumpToFile(db, file, { log = () => {} } = {}) {
  const chunks = [];
  const result = await dumpCompanyDb(db, (line) => { chunks.push(line, '\n'); }, { log });
  const text = chunks.join('');
  const gz = zlib.gzipSync(Buffer.from(text, 'utf-8'), { level: 6 });
  fs.writeFileSync(file, gz);
  const sha256 = (await import('node:crypto')).createHash('sha256').update(gz).digest('hex');
  return { ...result, file, bytes: gz.length, rawBytes: Buffer.byteLength(text, 'utf-8'), sha256 };
}

/** gzip ファイルから復元する */
export async function restoreFromFile(db, file, opts = {}) {
  return restoreCompanyDb(db, zlib.gunzipSync(fs.readFileSync(file)).toString('utf-8'), opts);
}

/** ダンプが壊れていないかを見る (DB に触らない) */
export function verifyDumpText(text) {
  const { header, tables } = parseDump(text);
  return {
    ok: true, generatedAt: header.generatedAt, session: header.session, migrations: header.migrations || [],
    sequences: (header.sequences || []).length,
    tables: tables.map((t) => ({ table: t.table, rows: t.rows.length })),
    totalRows: tables.reduce((n, t) => n + t.rows.length, 0), declaredTotal: header.totalRows,
  };
}
export async function verifyDumpFile(file) {
  return verifyDumpText(zlib.gunzipSync(fs.readFileSync(file)).toString('utf-8'));
}
