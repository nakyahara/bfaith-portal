#!/usr/bin/env node
/**
 * migrate.mjs — Company DB (PostgreSQL) のマイグレーション実行器 (Company DB構想 03 §1 P-12)
 *
 * 規約:
 *   - db/company/migrations/NNNN_name.sql を番号順に、未適用のものだけ、1 本ずつトランザクションで流す
 *   - 適用済みは ops.schema_migrations (version, checksum, applied_at, applied_by) に記録
 *   - 🚨 適用済みファイルは書き換えない (checksum が違えば止まる)。直したいときは次の番号で足す
 *   - SQLite の「起動時に CREATE IF NOT EXISTS」方式は持ち込まない。適用は人が (または配布手順が) このコマンドで行う
 *
 * 🆕 D-60 PR 3a-i (設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「migrate の runner の契約 (3a-i)」v3.9):
 *   - **全体の排他** = CLI の入口 (migrateWithLock → withMigrateLock) が、接続の直後に session の advisory lock
 *     (hashtextextended('company_db_migrate', 0)) を try で取り、bootstrap → 未適用の判定 → DDL → 検証 → 記録 を同じ接続のまま行う (ふつうの migration も)。
 *     取れなければ待たずに MIGRATE_LOCKED (exit 1・「別の migrate が動いている」)。
 *     失敗の後の順 = ROLLBACK が終わってから pg_advisory_unlock → client.end()。ROLLBACK も失敗した (接続が死んだ) ら unlock は呼ばずに接続を捨てる
 *     (session が切れて外れる)。concurrent-index の文 (取引の外) の失敗は ROLLBACK が要らない = そのまま unlock。
 *     🚨 この鍵は `company_db_heavy` (取引の lock だけにする共通の鍵) とは別 = 人が流す短い道具の session の lock
 *   - 使い回す applyMigrations() (PGlite の試験も通す) は lock を取らない = Postgres 専用の SQL を無条件では入れない
 *   - **concurrent-index の migration** = 1 行目が `-- migrate:concurrent-index` のファイル。
 *     許す文は `create [unique] index concurrently if not exists …` と `drop index concurrently if exists <schema>.<名前>` だけ
 *     (流す前に、この回に流す全部のファイルの全部の文を検査し、ほかがあれば何も流さずに止まる)。
 *     adapter が supportsConcurrentIndex (pg の Client だけ) なら取引の外で 1 文ずつ流し (migrate の lock の中でだけ)、
 *     indisvalid・indisready・indislive と正規化した catalog の属性 (expect.json) が一致したときだけ記録する。
 *     PGlite など対応しない adapter は、同じ文から concurrently を外した `create index if not exists` をふつうの取引で流し、属性の検証は同じに通す
 *   - **持ち主の mode** (Codex R-D60-v3-10 H2) = 印の表 ops.migrate_owner が無い = legacy (接続の役割のまま・PR 1b の前) / 1 行目が
 *     `-- migrate:owner-transition` の file (PR 1b 自身) だけが印を作れる / 印がある = owner (SET ROLE <印の役割> で流す・PR 1b の後)。下の readOwnerMode
 *     起動時に印と owner-transition の適用を両方向で確かめる (M-new-2)・記録の INSERT の直前に必ず SET LOCAL ROLE <印の役割> (M-new-3)・
 *     owner の状態の file は許す一覧 (ALLOWED_SET_LOCAL_ROLES) の SET LOCAL ROLE だけを許す (設計 13 v3.11 ③)
 *   - **空き容量** (concurrent-index の create の各文の前) = 予想の index の大きさ = reltuples × (列の pg_stats.avg_width の和 + 式の列は 64 + 16) × 1.3。
 *     空き (Render のメトリクスの Disk Capacity − Disk Usage) が 予想 × 3 + 2GB に満たない・メトリクスが読めない = 流さない (fail-closed)
 *
 * 使い方:
 *   COMPANY_DB_URL=postgres://... node scripts/company-db/migrate.mjs            # 未適用を全部
 *   node scripts/company-db/migrate.mjs --url postgres://... --to 0004          # 0004 まで
 *   node scripts/company-db/migrate.mjs --url ... --list                         # 適用状況だけ (lock は取らない・持ち主の pid を出す)
 *   node scripts/company-db/migrate.mjs --url ... --dry-run                      # 何を流すかだけ (try の lock を取る・DDL は流さない・許す文と expect.json を検査)
 *   node scripts/company-db/migrate.mjs --url <使い捨ての DB> --index-expect 0057 # そのファイルの index の属性 (expect.json の中身) を出す
 *   (--dir <フォルダ> で migrations のフォルダを変えられる。試験用)
 *   env (concurrent-index の容量): RENDER_API_KEY・CDB_RENDER_PG_RESOURCE_ID (apps/company-db/profit/render-metrics.mjs)
 *
 * テストは PGlite (WASM の Postgres) で同じ applyMigrations() を通す (scripts/test-company-db-ddl.mjs)。
 * 本物の PG の試験 = scripts/test-company-db-migrate-lock-pg.mjs (lock・concurrent-index・容量)
 * 終了コード: 0 = 成功 / 1 = 失敗 (途中のファイルで止まる。適用済みぶんはそのまま。別の migrate が動いている も 1) / 2 = 引数不正
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const DEFAULT_DIR = path.resolve(__dirname, '../../db/company/migrations');
const FILE_RE = /^(\d{4})_([A-Za-z0-9_-]+)\.sql$/;

/** 全体の排他の鍵の名前 (session の advisory lock・hashtextextended(name, 0))。設計 13 §3.10 の値 = 変えない */
export const MIGRATE_LOCK_NAME = 'company_db_migrate';
/** この runner の版 (applied_by に書く = 古い runner で流したものと見分ける) */
export const MIGRATE_RUNNER_VERSION = 'migrate-v2';
/** concurrent-index のファイルの 1 行目 */
export const CONCURRENT_INDEX_MARKER = '-- migrate:concurrent-index';
/** expect.json の属性は PostgreSQL の版で表し方が変わりうる = 本番と同じ major でだけ流す */
export const EXPECTED_PG_MAJOR = 18;
/**
 * concurrent-index の session の設定 (設計 13 §3.10)。CIC は古いスナップショットを待つ = 長い。
 * 🚨 migrate の lock の見張りは 45 分 (MIGRATE_LOCK_ALERT_MINUTES) = statement_timeout 30 分で先に切れるはず (鳴るのは止まっている印)
 */
export const CONCURRENT_INDEX_SETTINGS = Object.freeze({ lockTimeout: '5min', statementTimeout: '30min', clientConnectionCheckInterval: '1s' });
export const MIGRATE_LOCK_ALERT_MINUTES = 45;
/**
 * 空き容量の関門 (設計 13 §3.10 v3.9):
 *   予想 = reltuples × (index の列の pg_stats.avg_width の和 + 式の列は EXPR_WIDTH + TUPLE_OVERHEAD) × ESTIMATE_FACTOR
 *   流せる = 空き (Disk Capacity − Disk Usage) ≧ 予想 × SAFETY_FACTOR + FIXED_RESERVE
 */
export const DISK_ESTIMATE = Object.freeze({ exprWidth: 64, tupleOverhead: 16, estimateFactor: 1.3, safetyFactor: 3, fixedReserveBytes: 2 * 1024 ** 3 });

/** ファイル内容の checksum (改行コードの違いを吸収。CRLF で checkout されても同じ値) */
export function checksumOf(text) {
  return crypto.createHash('sha256').update(text.replace(/\r\n/g, '\n'), 'utf-8').digest('hex');
}

/** 1 行目が concurrent-index の印か (BOM・行末の空白・CR は無視) */
export function isConcurrentIndexText(text) {
  const first = text.replace(/^\uFEFF/, '').split('\n', 1)[0].replace(/\s+$/, '');
  return first === CONCURRENT_INDEX_MARKER;
}

// ─── 持ち主の mode (Codex R-D60-v3-10 H2・PR 1b の前 / PR 1b 自身 / PR 1b の後) ───
/**
 * PR 1b (持ち主の隔離) の migration の 1 行目。この file だけが持ち主の印 (ops.migrate_owner) を作ってよい。
 *   - legacy (印が無い = PR 1b の前・今の本番) = 接続の役割のまま流す (今までどおり)
 *   - owner-transition の file 自身 = 接続の役割で流し (役割と持ち主の移しをする)、同じ取引の終わりに印ができたことを確かめ、
 *     記録 (ops.schema_migrations) は印の持ち主の役割 (SET LOCAL ROLE) で入れる
 *   - owner (印がある = PR 1b の後) = 全部の file を印の持ち主の役割で流す (ふつうの file = 取引の中の SET LOCAL ROLE・concurrent-index = SET ROLE → RESET ROLE)。
 *     記録表の読み書きも同じ役割。接続の役割がその役割に SET できなければ止まる
 * 印 = 表 ops.migrate_owner があること (PR 1b の owner-transition の file が作る)。**持ち主の役割 = その表の持ち主** (catalog の relowner) で、
 *   ops.schema_migrations の持ち主と同じでなければ止まる。中身 (owner_role・since) は人が読むため (runner は表の SELECT に頼らない = 接続の役割が ops の USAGE を失っても読める)
 * 🚨 advisory lock は session (backend) のもの = SET ROLE に左右されない。取るのも外すのも RESET ROLE の後の session の役割で行う
 */
export const OWNER_TRANSITION_MARKER = '-- migrate:owner-transition';
export const OWNER_MARKER_TABLE = 'ops.migrate_owner';
const ROLE_RE = /^[a-z_][a-z0-9_]{0,62}$/;
export function isOwnerTransitionText(text) {
  const first = text.replace(/^\uFEFF/, '').split('\n', 1)[0].replace(/\s+$/, '');
  return first === OWNER_TRANSITION_MARKER;
}
const ownerErr = (msg) => Object.assign(new Error(`持ち主の mode: ${msg}`), { code: 'OWNER_MODE_INVALID' });
/**
 * 今の持ち主の mode を読む (catalog だけ = 接続の役割で読める)。
 * 戻り = { mode: 'legacy' } | { mode: 'owner', role }。役割の名前が不正・SET できない・記録表の持ち主と違う = OWNER_MODE_INVALID
 */
export async function readOwnerMode(db) {
  // 🚨 catalog だけで読む (pg_class / pg_namespace は誰でも読める)。PR 1b の後は接続の役割が ops の USAGE を持たないかもしれない = to_regclass や表の SELECT に頼らない
  const { rows } = await db.query(`select
      (select pg_get_userbyid(c.relowner)::text from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'ops' and c.relname = 'migrate_owner' and c.relkind in ('r', 'p')) as marker_owner,
      (select pg_get_userbyid(c.relowner)::text from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'ops' and c.relname = 'schema_migrations' and c.relkind in ('r', 'p')) as mig_owner`);
  const role = rows[0].marker_owner;
  if (role == null) return { mode: 'legacy' };
  if (!ROLE_RE.test(role)) throw ownerErr(`印の表 ${OWNER_MARKER_TABLE} の持ち主の名前が不正: ${role}`);
  if (rows[0].mig_owner !== role) throw ownerErr(`ops.schema_migrations の持ち主 (${rows[0].mig_owner}) が印の表 ${OWNER_MARKER_TABLE} の持ち主 ${role} と違う`);
  const { rows: can } = await db.query(`select pg_has_role(session_user, $1, 'SET') as can`, [role]);
  if (!can[0].can) throw ownerErr(`接続の役割 (session_user) が ${role} に SET ROLE できない (grant ${role} to <接続の役割> with set true)`);
  return { mode: 'owner', role };
}
/** ops の表があるか (catalog だけ = schema の USAGE が無くても読める) */
async function opsTableExists(db, name) {
  const { rows } = await db.query(`select exists (select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'ops' and c.relname = $1) as e`, [name]);
  return rows[0].e === true;
}
const modeText = (m) => (m.mode === 'owner' ? `owner (SET ROLE ${m.role})` : 'legacy (接続の役割のまま)');
/** 取引の中で fn (owner なら SET LOCAL ROLE)。読むだけの短い取引に使う */
async function inModeTx(db, mode, fn) {
  await db.exec('begin');
  try {
    if (mode.mode === 'owner') await db.exec(`set local role ${mode.role}`);
    const r = await fn();
    await db.exec('commit');
    return r;
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* */ }
    throw e;
  }
}

/** migrations ディレクトリの一覧 (番号順)。番号の重複・欠番 (0001 から連番でない) は不正 */
export function listMigrationFiles(dir = DEFAULT_DIR) {
  const files = fs.readdirSync(dir).filter((f) => FILE_RE.test(f)).sort();
  const out = [];
  const seen = new Set();
  for (const f of files) {
    const m = FILE_RE.exec(f);
    const version = m[1];
    if (seen.has(version)) throw Object.assign(new Error(`マイグレーション番号が重複: ${version}`), { code: 'BAD_MIGRATIONS' });
    const expected = String(out.length + 1).padStart(4, '0');
    if (version !== expected) throw Object.assign(new Error(`マイグレーション番号に欠番: ${expected} が無く ${version} がある`), { code: 'BAD_MIGRATIONS' });
    seen.add(version);
    const text = fs.readFileSync(path.join(dir, f), 'utf-8');
    out.push({ version, name: m[2], file: f, text, checksum: checksumOf(text), concurrentIndex: isConcurrentIndexText(text), ownerTransition: isOwnerTransitionText(text) });
  }
  return out;
}

// ─── SQL の字句 (concurrent-index の文の検査に使う) ───
const sqlErr = (msg) => Object.assign(new Error(msg), { code: 'SQL_LEX' });
/**
 * SQL を字句に分ける。コメント (-- と入れ子の /* *\/) は捨てる。
 * 字句 = { t: 'word' | 'str' | 'qid' | 'dollar' | 'num' | 'punct', v, start, end }
 *   str = '…' と E'…' (E は \ の escape あり)・qid = "…"・dollar = $tag$…$tag$ (中身は 1 つの字句)
 * 閉じていない文字列・コメント・ドルの引用は SQL_LEX で止まる
 */
export function lexSql(text) {
  const toks = [];
  const n = text.length;
  let i = 0;
  const isIdStart = (c) => /[A-Za-z_\u0080-\uffff]/.test(c);
  const isId = (c) => /[A-Za-z0-9_$\u0080-\uffff]/.test(c);
  const scanString = (q, escape) => {   // q = 開きの ' の位置。戻り = 閉じの次
    let j = q + 1;
    while (j < n) {
      if (escape && text[j] === '\\') { j += 2; continue; }
      if (text[j] === "'") { if (text[j + 1] === "'") { j += 2; continue; } return j + 1; }
      j++;
    }
    throw sqlErr('閉じていない文字列 (\') がある');
  };
  while (i < n) {
    const c = text[i], d = text[i + 1];
    if (/\s/.test(c)) { i++; continue; }
    if (c === '-' && d === '-') { const j = text.indexOf('\n', i); i = j < 0 ? n : j + 1; continue; }
    if (c === '/' && d === '*') {
      let depth = 1, j = i + 2;
      while (j < n && depth) {
        if (text[j] === '/' && text[j + 1] === '*') { depth++; j += 2; } else if (text[j] === '*' && text[j + 1] === '/') { depth--; j += 2; } else j++;
      }
      if (depth) throw sqlErr('閉じていないコメント (/*) がある');
      i = j; continue;
    }
    if ((c === 'E' || c === 'e') && d === "'" && !(i > 0 && isId(text[i - 1]))) {
      const j = scanString(i + 1, true); toks.push({ t: 'str', v: text.slice(i, j), start: i, end: j }); i = j; continue;
    }
    if (c === "'") { const j = scanString(i, false); toks.push({ t: 'str', v: text.slice(i, j), start: i, end: j }); i = j; continue; }
    if (c === '"') {
      let j = i + 1;
      for (;;) {
        const k = text.indexOf('"', j);
        if (k < 0) throw sqlErr('閉じていない引用の名前 (") がある');
        if (text[k + 1] === '"') { j = k + 2; continue; }
        j = k + 1; break;
      }
      toks.push({ t: 'qid', v: text.slice(i, j), start: i, end: j }); i = j; continue;
    }
    if (c === '$') {
      const m = /\$(?:[A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?\$/y;
      m.lastIndex = i;
      const mm = m.exec(text);
      if (mm) {
        const tag = mm[0];
        const k = text.indexOf(tag, i + tag.length);
        if (k < 0) throw sqlErr(`閉じていないドルの引用 (${tag}) がある`);
        toks.push({ t: 'dollar', v: text.slice(i, k + tag.length), start: i, end: k + tag.length }); i = k + tag.length; continue;
      }
      toks.push({ t: 'punct', v: '$', start: i, end: i + 1 }); i++; continue;
    }
    if (isIdStart(c)) {
      let j = i + 1; while (j < n && isId(text[j])) j++;
      toks.push({ t: 'word', v: text.slice(i, j), start: i, end: j }); i = j; continue;
    }
    if (/[0-9]/.test(c)) {
      let j = i + 1; while (j < n && /[0-9A-Za-z_.]/.test(text[j])) j++;
      toks.push({ t: 'num', v: text.slice(i, j), start: i, end: j }); i = j; continue;
    }
    toks.push({ t: 'punct', v: c, start: i, end: i + 1 }); i++;
  }
  return toks;
}

/** 字句を引用の外の ; で文に分ける。戻り = [{ toks, sql, start }] (空の文は捨てる。start = sql の text の中の位置) */
export function splitSqlStatements(text) {
  const toks = lexSql(text);
  const out = [];
  let cur = [];
  const flush = () => { if (cur.length) out.push({ toks: cur, sql: text.slice(cur[0].start, cur[cur.length - 1].end), start: cur[0].start }); cur = []; };
  for (const tk of toks) { if (tk.t === 'punct' && tk.v === ';') flush(); else cur.push(tk); }
  flush();
  return out;
}

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const rejectCi = (file, msg) => Object.assign(new Error(`${file}: concurrent-index の migration に許さない形がある = 何も流さずに止めた: ${msg}`), { code: 'CONCURRENT_INDEX_REJECTED' });

/** ( … ) の中を最上位の , で分けた要素ごとに「列か式か」(容量の見積もり用)。列 = 1 つ目が名前で、次が ( でも . でもない */
function groupElements(toks) {
  const els = [];
  let cur = [], depth = 0;
  for (const tk of toks) {
    if (tk.t === 'punct' && tk.v === '(') depth++;
    if (tk.t === 'punct' && tk.v === ')') depth--;
    if (depth === 0 && tk.t === 'punct' && tk.v === ',') { els.push(cur); cur = []; continue; }
    cur.push(tk);
  }
  if (cur.length) els.push(cur);
  return els.map((e) => {
    const a = e[0], b = e[1];
    const isCol = a && (a.t === 'word' || a.t === 'qid') && !(b && b.t === 'punct' && (b.v === '(' || b.v === '.'));
    if (!isCol) return { column: null, expression: true };
    return { column: a.t === 'qid' ? a.v.slice(1, -1).replace(/""/g, '"') : a.v.toLowerCase(), expression: false };
  });
}

/**
 * concurrent-index の 1 文を読む (許す 2 つの形だけ)。戻り = { kind: 'create' | 'drop', schema, name, table?, unique?, sql, sqlTx, elements? }
 *   create [unique] index concurrently if not exists <名前> on [only] <schema>.<表> [using btree] (…) [include (…)] [where …]
 *   drop index concurrently if exists <schema>.<名前>
 * 名前・schema・表は引用しない名前だけ (Postgres と同じく小文字にする)・63 文字まで。ドルの引用はどこにも置かない
 * sqlTx = 同じ文から concurrently を外したもの (concurrent-index に対応しない adapter がふつうの取引で流す)
 */
export function parseConcurrentIndexStatement(stmt, file = '') {
  const toks = stmt.toks;
  let p = 0;
  const fail = (msg) => { throw rejectCi(file, `${msg} (文: ${stmt.sql.replace(/\s+/g, ' ').slice(0, 160)})`); };
  if (toks.some((tk) => tk.t === 'dollar')) fail('ドルの引用は使わない');
  const isWord = (k) => toks[p] && toks[p].t === 'word' && toks[p].v.toLowerCase() === k;
  const want = (k) => { if (!isWord(k)) fail(`「${k}」が要る位置に「${toks[p] ? toks[p].v : '(文の終わり)'}」`); p++; };
  const ident = (what) => {
    const tk = toks[p];
    if (!tk || tk.t !== 'word' || !IDENT_RE.test(tk.v)) fail(`${what} は引用しない名前 (英数字と _) にする`);
    if (tk.v.length > 63) fail(`${what} が 63 文字を超える (Postgres が切り詰める)`);
    p++; return tk.v.toLowerCase();
  };
  const punct = (ch) => toks[p] && toks[p].t === 'punct' && toks[p].v === ch;
  const group = (what) => {   // ( … ) の釣り合い。中身は空でない。戻り = 中の字句
    if (!punct('(')) fail(`${what} の「(」が要る`);
    let depth = 0; const start = p;
    for (; p < toks.length; p++) {
      if (punct('(')) depth++;
      else if (punct(')')) { depth--; if (depth === 0) { p++; if (p - start <= 2) fail(`${what} が空`); return toks.slice(start + 1, p - 1); } }
    }
    fail(`${what} の「)」が閉じていない`);
  };
  const withoutConcurrently = (tk) => stmt.sql.slice(0, tk.start - stmt.start) + stmt.sql.slice(tk.end - stmt.start);
  const allowedOnly = () => fail('許す文は create [unique] index concurrently if not exists … と drop index concurrently if exists … だけ');
  if (isWord('create')) {
    p++;
    if (!isWord('unique') && !isWord('index')) allowedOnly();
    let unique = false;
    if (isWord('unique')) { unique = true; p++; }
    want('index');
    const conc = toks[p];
    want('concurrently'); want('if'); want('not'); want('exists');
    const name = ident('index の名前');
    want('on');
    if (isWord('only')) p++;
    const schema = ident('schema');
    if (!punct('.')) fail('表は <schema>.<表> で書く');
    p++;
    const table = ident('表');
    if (isWord('using')) { p++; want('btree'); }
    const elements = groupElements(group('key の列'));
    if (isWord('include')) { p++; elements.push(...groupElements(group('include の列'))); }
    if (isWord('where')) { p++; if (p >= toks.length) fail('where の条件が空'); p = toks.length; }
    if (p !== toks.length) fail(`許さない句「${toks[p].v}」(許すのは using btree・include・where だけ)`);
    return { kind: 'create', unique, schema, name, table, elements, sql: stmt.sql, sqlTx: withoutConcurrently(conc) };
  }
  if (isWord('drop')) {
    p++;
    if (!isWord('index')) allowedOnly();
    want('index');
    const conc = toks[p];
    want('concurrently'); want('if'); want('exists');
    const schema = ident('schema');
    if (!punct('.')) fail('drop は <schema>.<名前> で書く');
    p++;
    const name = ident('index の名前');
    if (p !== toks.length) fail(`drop の後ろに「${toks[p].v}」(cascade などは許さない)`);
    return { kind: 'drop', schema, name, sql: stmt.sql, sqlTx: withoutConcurrently(conc) };
  }
  allowedOnly();
}

/** expect.json の置き場 = migration の横の <番号>_<名前>.expect.json */
export const expectPathOf = (dir, f) => path.join(dir, `${f.version}_${f.name}.expect.json`);
export const EXPECT_FORMAT = 'company-db-index-expect/1';

/**
 * concurrent-index のファイルの計画 (流す前の検査を全部)。止めるときは CONCURRENT_INDEX_REJECTED。
 * 戻り = { statements, expect, creates: Map<'schema.name', stmt> (最後の create), drops: Set<'schema.name'> }
 */
export function planConcurrentIndexFile(f, dir = DEFAULT_DIR) {
  let stmts;
  try { stmts = splitSqlStatements(f.text); } catch (e) { throw rejectCi(f.file, e.message); }
  if (!stmts.length) throw rejectCi(f.file, '文が 1 つも無い');
  const statements = stmts.map((s) => parseConcurrentIndexStatement(s, f.file));
  const creates = new Map(), drops = new Set();
  for (const s of statements) {
    const key = `${s.schema}.${s.name}`;
    if (s.kind === 'create') { creates.set(key, s); drops.delete(key); } else { drops.add(key); creates.delete(key); }
  }
  const ep = expectPathOf(dir, f);
  let expect = null;
  if (statements.some((s) => s.kind === 'create')) {
    if (!fs.existsSync(ep)) throw rejectCi(f.file, `期待の属性 ${path.basename(ep)} が無い (使い捨ての PG ${EXPECTED_PG_MAJOR} で流して --index-expect で作る・手で書かない)`);
    try { expect = JSON.parse(fs.readFileSync(ep, 'utf-8')); } catch (e) { throw rejectCi(f.file, `${path.basename(ep)} が JSON として読めない: ${e.message}`); }
    if (!expect || expect.format !== EXPECT_FORMAT || !expect.indexes || typeof expect.indexes !== 'object') throw rejectCi(f.file, `${path.basename(ep)} の形が違う (format = ${EXPECT_FORMAT}・indexes)`);
    if (!Number.isInteger(expect.pg_major)) throw rejectCi(f.file, `${path.basename(ep)} に pg_major が無い`);
    for (const key of creates.keys()) if (!Object.hasOwn(expect.indexes, key)) throw rejectCi(f.file, `${path.basename(ep)} に ${key} の属性が無い`);
    for (const key of Object.keys(expect.indexes)) if (!creates.has(key)) throw rejectCi(f.file, `${path.basename(ep)} の ${key} はこのファイルで作らない`);
    for (const [key, s] of creates) {
      const want = `${s.schema}.${s.table}`;
      if (expect.indexes[key].table !== want) throw rejectCi(f.file, `${path.basename(ep)} の ${key} の表が ${expect.indexes[key].table} (文は ${want})`);
    }
  }
  return { statements, expect, creates, drops };
}

/** 印の無いファイルに concurrently がある = 取引の中で落ちる前に分かりやすく止める (字句に読めないファイルは今までどおり Postgres に任せる) */
function rejectUnmarkedConcurrently(f) {
  let toks;
  try { toks = lexSql(f.text); } catch { return; }
  if (toks.some((tk) => tk.t === 'word' && tk.v.toLowerCase() === 'concurrently')) {
    throw Object.assign(new Error(`${f.file}: concurrently があるのに 1 行目が「${CONCURRENT_INDEX_MARKER}」でない = 何も流さずに止めた (取引の中では CIC は流せない)`), { code: 'CONCURRENT_INDEX_REJECTED', version: f.version });
  }
}

/**
 * owner の状態の file で `SET LOCAL ROLE <名前>` を許す役割の一覧 (設計 13 v3.11 ③・正本はこの定数)。
 * 印の役割 cdb_owner と NOLOGIN の持ち主 5 つ (「以後の更新」の専用の持ち主の関数・設計 19 の F4-2a が file の中で使う)
 */
export const ALLOWED_SET_LOCAL_ROLES = Object.freeze(['cdb_owner', 'profit_definer', 'heavy_guard_definer', 'heavy_read_definer', 'd60_calib_definer', 'finance_revision_definer']);

/**
 * 役割を切り替える文のうち、owner の状態の file で許さないもの (コメント・文字列・ドルの引用の外の文だけを見る)。戻り = 理由の一覧 ([] = 無い)。
 * 拒む (設計 13 v3.11 ③) = RESET ROLE・SET ROLE (LOCAL の無い = session)・SET SESSION ROLE・SET [LOCAL] ROLE NONE・SET / RESET SESSION AUTHORIZATION・
 *   set_config('role', …)・DISCARD・SET LOCAL ROLE <一覧の外の役割>。許す = SET LOCAL ROLE <ALLOWED_SET_LOCAL_ROLES のどれか> だけ。
 * 🚨 字句の検査は補助 = 守りの本体は「記録の INSERT の直前の SET LOCAL ROLE <印の役割>」(動的 SQL で役割を変えられても記録は印の役割)
 */
export function roleSwitchStatements(text, allowed = ALLOWED_SET_LOCAL_ROLES) {
  const toks = lexSql(text);   // 読めない = 例外 (owner の状態では止まる)
  const lw = (tk) => (tk && tk.t === 'word' ? tk.v.toLowerCase() : null);
  const isPunct = (tk, ch) => tk && tk.t === 'punct' && tk.v === ch;
  const nameOf = (tk) => {   // 役割の名前の字句 (引用しない名前・"…"・'…') → 名前 (Postgres と同じく引用しない名前は小文字)
    if (!tk) return null;
    if (tk.t === 'word') return tk.v.toLowerCase();
    if (tk.t === 'qid') return tk.v.slice(1, -1).replace(/""/g, '"');
    if (tk.t === 'str' && tk.v.startsWith("'")) return tk.v.slice(1, -1).replace(/''/g, "'");
    return null;
  };
  const hits = [];
  for (let i = 0; i < toks.length; i++) {
    const w0 = lw(toks[i]), w1 = lw(toks[i + 1]), w2 = lw(toks[i + 2]);
    if (w0 === 'reset' && (w1 === 'role' || (w1 === 'session' && w2 === 'authorization'))) hits.push(`reset ${w1 === 'role' ? 'role' : 'session authorization'}`);
    if (w0 === 'discard' && ['all', 'plans', 'sequences', 'temp', 'temporary'].includes(w1)) hits.push(`discard ${w1}`);
    if (w0 === 'set_config' && isPunct(toks[i + 1], '(') && toks[i + 2] && toks[i + 2].t === 'str' && /^e?'role'$/i.test(toks[i + 2].v)) hits.push("set_config('role', …)");
    if (w0 === 'set') {
      if (w1 === 'session' && w2 === 'authorization') { hits.push('set session authorization'); continue; }
      if (w1 === 'role') { hits.push('set role (session)'); continue; }
      if (w1 === 'session' && w2 === 'role') { hits.push('set session role'); continue; }
      if (w1 === 'local' && w2 === 'role') {
        const name = nameOf(toks[i + 3]);
        const after = toks[i + 4];
        if (name == null) hits.push('set local role (名前が読めない)');
        else if (toks[i + 3].t === 'word' && name === 'none') hits.push('set local role none');
        else if (!allowed.includes(name)) hits.push(`set local role ${name} (許す一覧の外)`);
        else if (after && !isPunct(after, ';')) hits.push(`set local role ${name} の後ろに「${after.v}」`);
      }
      if (w1 === 'local' && w2 === 'session' && lw(toks[i + 3]) === 'authorization') hits.push('set local session authorization');
    }
  }
  return hits;
}
function rejectRoleSwitchInOwnerMode(f) {
  let hits;
  try { hits = roleSwitchStatements(f.text); } catch (e) { throw Object.assign(new Error(`${f.file}: owner の状態の file を字句に読めない (${e.message}) = 役割を切り替える文が無いと言えないので流さない`), { code: 'OWNER_MODE_INVALID', version: f.version }); }
  if (hits.length) throw Object.assign(new Error(`${f.file}: owner の状態の file に許さない役割の切り替えがある (${[...new Set(hits)].join('・')}) = 流さない (許すのは SET LOCAL ROLE ${ALLOWED_SET_LOCAL_ROLES.join(' / ')} だけ)`), { code: 'OWNER_MODE_INVALID', version: f.version });
}

// ─── catalog の属性 (定義の検証) ───
/**
 * index の正規化した属性を読む (無ければ null)。search_path = pg_catalog の取引で読む = 式の中の名前は全部 schema つきで出る (source の空白・大文字に依らない)
 * inTx = 呼び手の取引の中で読む (begin / commit しない・set local は呼び手の取引の終わりまで残る = 後の SQL は schema つきで書く)
 * 戻り = { valid, ready, live, attrs } (attrs が expect.json の 1 つの index の中身) / 名前が index でない物 = { notIndex, relkind }
 */
export async function readIndexAttrs(db, schema, name, { inTx = false } = {}) {
  if (!inTx) await db.exec('begin');
  try {
    await db.exec('set local search_path = pg_catalog, pg_temp');
    const { rows } = await db.query(`
      select c.relkind::text as relkind, i.indisvalid as valid, i.indisready as ready, i.indislive as live,
        case when i.indexrelid is null then null else json_build_object(
          'table', tn.nspname || '.' || t.relname,
          'access_method', am.amname,
          'unique', i.indisunique,
          'nulls_not_distinct', i.indnullsnotdistinct,
          'key_columns', i.indnkeyatts::int,
          'all_columns', i.indnatts::int,
          'columns', (select json_agg(json_build_object(
              'column', case when kk.v = 0 then null else (select a.attname::text from pg_attribute a where a.attrelid = i.indrelid and a.attnum = kk.v) end,
              'definition', pg_get_indexdef(i.indexrelid, s.n, false),
              'key', s.n <= i.indnkeyatts,
              'opclass', (select ons.nspname || '.' || oc.opcname from pg_opclass oc join pg_namespace ons on ons.oid = oc.opcnamespace where oc.oid = cl.v),
              'collation', (select cns.nspname || '.' || co.collname from pg_collation co join pg_namespace cns on cns.oid = co.collnamespace where co.oid = cc.v),
              'option', op.v) order by s.n)
            from generate_series(1, i.indnatts::int) s(n)
            left join lateral (select x.v from unnest(i.indkey::int2[]) with ordinality x(v, o) where x.o = s.n) kk on true
            left join lateral (select x.v from unnest(i.indclass::oid[]) with ordinality x(v, o) where x.o = s.n) cl on true
            left join lateral (select x.v from unnest(i.indcollation::oid[]) with ordinality x(v, o) where x.o = s.n) cc on true
            left join lateral (select x.v::int as v from unnest(i.indoption::int2[]) with ordinality x(v, o) where x.o = s.n) op on true),
          'expressions', pg_get_expr(i.indexprs, i.indrelid, false),
          'predicate', pg_get_expr(i.indpred, i.indrelid, false)
        ) end as attrs
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
      left join pg_index i on i.indexrelid = c.oid
      left join pg_class t on t.oid = i.indrelid
      left join pg_namespace tn on tn.oid = t.relnamespace
      left join pg_am am on am.oid = c.relam
      where n.nspname = $1 and c.relname = $2`, [schema, name]);
    if (!inTx) await db.exec('commit');
    if (!rows.length) return null;
    const r = rows[0];
    if (!['i', 'I'].includes(r.relkind)) return { notIndex: true, relkind: r.relkind };
    return { valid: r.valid, ready: r.ready, live: r.live, attrs: typeof r.attrs === 'string' ? JSON.parse(r.attrs) : r.attrs };
  } catch (e) {
    if (!inTx) { try { await db.exec('rollback'); } catch { /* */ } }
    throw e;
  }
}

/** キーの順に依らない JSON (比べる用) */
export function stableJson(v) {
  if (Array.isArray(v)) return `[${v.map(stableJson).join(',')}]`;
  if (v && typeof v === 'object') return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stableJson(v[k])}`).join(',')}}`;
  return JSON.stringify(v === undefined ? null : v);
}
/** 属性の違い (同じなら []) */
export function attrDiff(actual, expected) {
  const keys = [...new Set([...Object.keys(actual || {}), ...Object.keys(expected || {})])].sort();
  return keys.filter((k) => stableJson(actual?.[k]) !== stableJson(expected?.[k]))
    .map((k) => `${k}: 今 ${stableJson(actual?.[k])} / 期待 ${stableJson(expected?.[k])}`);
}

async function serverMajor(db) {
  const { rows } = await db.query('select current_setting(\'server_version_num\')::int as v');
  return Math.floor(Number(rows[0].v) / 10000);
}

/** 同じ表の index の作りが (別の backend で) 動いていないか。見えない (relid が null = 別の役割) のも「ある」とみなす */
async function buildsInProgress(db, qualifiedTable) {
  const { rows } = await db.query(`
    select p.pid, p.relid::regclass::text as rel, p.phase
      from pg_stat_progress_create_index p
     where p.datid = (select oid from pg_database where datname = current_database())
       and p.pid <> pg_backend_pid()
       and (p.relid is null or $1::text is null or p.relid = to_regclass($1::text))`, [qualifiedTable]);
  return rows;
}

// ─── 空き容量 ───
const mb = (b) => `${Math.round(Number(b) / 1048576).toLocaleString()} MB`;
/**
 * create の 1 文の予想の index の大きさ (bytes)。reltuples が負 (一度も ANALYZE していない)・列の pg_stats が無い = 見積もれない = 例外 (流さない)。
 * 列は key と include の両方 (include も葉に入る = 上に倒す)
 */
export async function estimateIndexBytes(db, st) {
  const fail = (msg) => Object.assign(new Error(`容量: ${st.schema}.${st.name} の大きさを見積もれない (${msg}) = 流さずに止めた`), { code: 'DISK_CHECK_FAILED' });
  const { rows: rel } = await db.query(`select c.reltuples::float8 as reltuples from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = $1 and c.relname = $2 and c.relkind in ('r', 'p', 'm')`, [st.schema, st.table]);
  if (!rel.length) throw fail(`表 ${st.schema}.${st.table} が無い`);
  const reltuples = Number(rel[0].reltuples);
  if (!(reltuples >= 0)) throw fail(`表 ${st.schema}.${st.table} の reltuples が ${reltuples} = 一度も ANALYZE していない (先に analyze ${st.schema}.${st.table})`);
  const cols = [...new Set(st.elements.filter((e) => !e.expression).map((e) => e.column))];
  const { rows: stats } = cols.length ? await db.query('select attname::text as attname, avg_width from pg_stats where schemaname = $1 and tablename = $2 and attname = any($3::text[])', [st.schema, st.table, cols]) : { rows: [] };
  const width = new Map(stats.map((r) => [r.attname, Number(r.avg_width)]));
  let sum = 0;
  for (const e of st.elements) {
    if (e.expression) { sum += DISK_ESTIMATE.exprWidth; continue; }
    if (!width.has(e.column)) throw fail(`列 ${e.column} の pg_stats が無い (先に analyze ${st.schema}.${st.table})`);
    sum += width.get(e.column);
  }
  return Math.ceil(reltuples * (sum + DISK_ESTIMATE.tupleOverhead) * DISK_ESTIMATE.estimateFactor);
}

/**
 * create の各文の前の容量の関門。readDiskMetrics() = { ok: true, capacityBytes, usedBytes } | { ok: false, reason } (例外も「読めない」)。
 * 空き < 予想 × 3 + 2GB・読めない = DISK_CHECK_FAILED (流さない)
 */
export async function checkDiskBeforeCreate(db, st, readDiskMetrics, log = () => {}) {
  const estimate = await estimateIndexBytes(db, st);
  const need = estimate * DISK_ESTIMATE.safetyFactor + DISK_ESTIMATE.fixedReserveBytes;
  const fail = (msg, extra = {}) => Object.assign(new Error(`容量: ${st.schema}.${st.name}: ${msg} = 流さずに止めた`), { code: 'DISK_CHECK_FAILED', ...extra });
  let m;
  try { m = typeof readDiskMetrics === 'function' ? await readDiskMetrics() : { ok: false, reason: 'NO_METRICS_READER' }; } catch { m = { ok: false, reason: 'METRICS_INTERNAL' }; }
  if (!m || m.ok !== true) throw fail(`空き容量が読めない (${(m && m.reason) || '不明'}・Render のメトリクス = RENDER_API_KEY / CDB_RENDER_PG_RESOURCE_ID)`, { reason: (m && m.reason) || 'UNKNOWN', estimateBytes: estimate });
  const cap = Number(m.capacityBytes), used = Number(m.usedBytes);
  if (!Number.isFinite(cap) || !Number.isFinite(used) || cap <= 0 || used < 0 || used > cap) throw fail('空き容量の値が不正', { reason: 'METRICS_INCONSISTENT', estimateBytes: estimate });
  const free = cap - used;
  log(`容量: ${st.schema}.${st.name} の予想 ${mb(estimate)} × ${DISK_ESTIMATE.safetyFactor} + ${mb(DISK_ESTIMATE.fixedReserveBytes)} = ${mb(need)} / 空き ${mb(free)} (容量 ${mb(cap)} − 使用 ${mb(used)})`);
  if (free < need) throw fail(`空き ${mb(free)} が 予想 × ${DISK_ESTIMATE.safetyFactor} + 2GB = ${mb(need)} に満たない`, { reason: 'NOT_ENOUGH', estimateBytes: estimate, needBytes: need, freeBytes: free });
  return { estimateBytes: estimate, needBytes: need, freeBytes: free };
}

/** CLI の容量の読み手 = Render のメトリクス (#1600 の client)。読めなければ { ok: false } (fail-closed) */
export async function renderDiskMetricsReader(env = process.env) {
  const m = await import('../../apps/company-db/profit/render-metrics.mjs');
  return async () => {
    const c = m.readRenderMetricsConfig(env);
    if (!c.ok) return { ok: false, reason: c.reason };
    const r = await m.fetchPostgresMetrics({ apiKey: c.config.apiKey, resourceId: c.config.resourceId });
    if (!r.ok) return { ok: false, reason: r.reason };
    if (!m.checkFreshness(r.snapshot).ok) return { ok: false, reason: 'METRICS_STALE' };
    return { ok: true, capacityBytes: r.snapshot.diskCapacityBytes, usedBytes: r.snapshot.diskUsedBytes };
  };
}

// ─── 全体の排他 (CLI の入口) ───
/** lock を持っている backend (見えれば)。接続からの分 = 見張り (45 分) と同じ物差し */
export async function describeLockHolder(db) {
  const { rows } = await db.query(`
    select l.pid, a.application_name, a.usename::text as usename, a.backend_start,
           floor(extract(epoch from (now() - a.backend_start)) / 60)::int as minutes
      from pg_locks l left join pg_stat_activity a on a.pid = l.pid
     where l.locktype = 'advisory' and l.granted and l.objsubid = 1
       and l.database = (select oid from pg_database where datname = current_database())
       and ((l.classid::bigint << 32) | l.objid::bigint) = hashtextextended($1, 0)`, [MIGRATE_LOCK_NAME]);
  return rows;
}
const holderText = (rows) => rows.map((r) => `pid ${r.pid}${r.application_name ? ` (${r.application_name}${r.usename ? `・${r.usename}` : ''})` : ''}${r.minutes != null ? ` 接続から ${r.minutes} 分` : ''}`).join(', ');

/** この session が migrate の lock を持っているか (concurrent-index は lock の中でだけ流す) */
async function holdsMigrateLock(db) {
  const { rows } = await db.query(`select exists (select 1 from pg_locks l where l.locktype = 'advisory' and l.granted and l.objsubid = 1 and l.pid = pg_backend_pid()
    and l.database = (select oid from pg_database where datname = current_database())
    and ((l.classid::bigint << 32) | l.objid::bigint) = hashtextextended($1, 0)) as held`, [MIGRATE_LOCK_NAME]);
  return rows[0].held === true;
}

/**
 * fn を migrate の session の lock の中で動かす (Postgres の接続だけ)。取れなければ MIGRATE_LOCKED (待たない)。
 * 失敗の後の順 (設計 v3.9) = ROLLBACK が終わってから unlock。ROLLBACK も失敗したら unlock を呼ばない (e.connectionDead = true・呼び手は接続を捨てる)。
 * 取引の外の失敗 (e.noTransaction = concurrent-index の文) は ROLLBACK をせずに unlock
 */
export async function withMigrateLock(db, fn, { log = () => {} } = {}) {
  const got = (await db.query('select pg_try_advisory_lock(hashtextextended($1, 0)) as got', [MIGRATE_LOCK_NAME])).rows[0].got;
  if (!got) {
    let who = '';
    try { const h = await describeLockHolder(db); if (h.length) who = ' / 持っている接続: ' + holderText(h); } catch { /* 見えなくても止まる理由は同じ */ }
    throw Object.assign(new Error(`別の migrate が動いている (lock ${MIGRATE_LOCK_NAME} を取れない) = 何もせずに止めた。終わってからもう一度流す${who}`), { code: 'MIGRATE_LOCKED' });
  }
  log(`lock ${MIGRATE_LOCK_NAME} を取った`);
  let result;
  try {
    result = await fn();
  } catch (e) {
    if (!e || !e.noTransaction) {
      try { await db.exec('rollback'); } catch (re) {
        // 接続が死んでいる = unlock は呼ばない (呼べない)。接続を捨てれば Postgres が外す
        if (e && typeof e === 'object') { e.connectionDead = true; e.rollbackError = re.message; }
        throw e;
      }
    }
    try { await unlock(db); log(`lock ${MIGRATE_LOCK_NAME} を外した`); } catch (ue) { if (e && typeof e === 'object') e.unlockError = ue.message; }
    throw e;
  }
  try { await unlock(db); } catch (ue) {
    // 成功の後に外せない = 黙って成功にしない (接続を閉じれば外れる)
    throw Object.assign(new Error(`migrate は済んだが lock を外せない (接続を閉じれば外れる): ${ue.message}`), { code: 'MIGRATE_UNLOCK_FAILED', result });
  }
  log(`lock ${MIGRATE_LOCK_NAME} を外した`);
  return result;
}
async function unlock(db) {
  // session の役割に戻してから外す (owner mode の SET ROLE が残っていても lock は session のもの = 外れ方は変わらないが、一貫させる・R-D60-v3-10 H2)
  await db.exec('reset role');
  const ok = (await db.query('select pg_advisory_unlock(hashtextextended($1, 0)) as ok', [MIGRATE_LOCK_NAME])).rows[0].ok;
  if (!ok) throw new Error('pg_advisory_unlock が false (持っていなかった)');
}

/** CLI の入口 = lock の中で applyMigrations (流す / dry-run) */
export function migrateWithLock(db, opts = {}) {
  return withMigrateLock(db, () => applyMigrations(db, opts), { log: opts.lockLog || (() => {}) });
}

// ─── concurrent-index のファイルを流す ───
async function setSession(db, s, log) {
  await db.exec(`set lock_timeout = '${s.lockTimeout}'`);
  await db.exec(`set statement_timeout = '${s.statementTimeout}'`);
  try {
    await db.exec(`set client_connection_check_interval = '${s.clientConnectionCheckInterval}'`);
  } catch (e) {
    // 🚨 この設定は Linux (POLLRDHUP) の server だけ。Windows の server (試験の embedded-postgres) は 0 以外を拒む = 使えないと出して続ける (Render は Linux)
    if (!/client_connection_check_interval/.test(e.message)) throw e;
    log(`この server では client_connection_check_interval を使えない (${e.message}) = 切れた接続の検出は TCP に任せる`);
  }
}
async function resetSession(db) {
  for (const k of ['role', 'lock_timeout', 'statement_timeout', 'client_connection_check_interval']) { try { await db.exec(`reset ${k}`); } catch { /* 接続が死んでいる */ } }
}
const SETTING_RE = /^\d+(ms|s|min|h)?$/;

/** invalid が残ったときの回収の手順 (README と同じ) */
export const CONCURRENT_INDEX_RUNBOOK = [
  '① select indexrelid::regclass, indisvalid, indisready from pg_index where not indisvalid; で invalid を見る',
  "② pg_stat_progress_create_index と pg_stat_activity (application_name = 'company-db-migrate') で前の作りが動いていないかを見る (動いていれば終わるのを待つ・止めるなら人が pg_cancel_backend)",
  '③ この runner をもう一度流す (同じ名前の invalid を drop index concurrently してから作り直す)。手で DROP INDEX (CONCURRENTLY なし) はしない (表に強い lock)',
  '詳しくは db/company/README.md「concurrent-index の migration (D-60 PR 3a-i)」',
].join('\n  ');

/** 本物の PG (取引の外) で流す。migrate の lock の中でだけ */
async function applyConcurrentIndexFile(db, f, plan, opts, log, appliedBy, mode) {
  const s = { ...CONCURRENT_INDEX_SETTINGS, ...(opts.concurrentIndexSettings || {}) };
  const noTx = (e) => Object.assign(e, { noTransaction: true });
  for (const [k, v] of Object.entries(s)) if (!SETTING_RE.test(String(v))) throw noTx(Object.assign(new Error(`concurrent-index の設定 ${k} が不正: ${v}`), { code: 'BAD_SETTINGS' }));
  if (!(await holdsMigrateLock(db))) throw noTx(Object.assign(new Error(`${f.file}: concurrent-index の migration は migrate の lock の中でだけ流す (CLI か migrateWithLock から)`), { code: 'MIGRATE_LOCK_REQUIRED', version: f.version }));
  const major = await serverMajor(db);
  const wantMajor = opts.expectedPgMajor ?? EXPECTED_PG_MAJOR;
  if (major !== wantMajor) throw noTx(Object.assign(new Error(`${f.file}: PostgreSQL の major が ${major} (期待 ${wantMajor}) = 式の表し方が版で変わりうるので流さない`), { code: 'PG_MAJOR_MISMATCH', version: f.version }));
  if (plan.expect && plan.expect.pg_major !== major) throw noTx(Object.assign(new Error(`${f.file}: expect.json の pg_major ${plan.expect.pg_major} が今の ${major} と違う (同じ版で作り直す)`), { code: 'PG_MAJOR_MISMATCH', version: f.version }));
  log(`apply ${f.file} (concurrent-index・取引の外で ${plan.statements.length} 文・lock_timeout ${s.lockTimeout}・statement_timeout ${s.statementTimeout}) ...`);
  const fail = (msg, extra = {}) => Object.assign(new Error(`${f.file} で失敗 (記録しない・次に流すと続きから): ${msg}`), { code: 'MIGRATION_FAILED', version: f.version, ...extra });
  await setSession(db, s, log);
  try {
    // owner mode = file の全部 (DDL・記録表・pg_stats の読み) を持ち主の役割で。finally の resetSession で RESET ROLE
    if (mode.mode === 'owner') await db.exec(`set role ${mode.role}`);
    for (const st of plan.statements) {
      const key = `${st.schema}.${st.name}`;
      if (st.kind === 'create') {
        const running = await buildsInProgress(db, `${st.schema}.${st.table}`);
        if (running.length) throw fail(`${st.schema}.${st.table} の index の作りが別の接続で動いている (pid ${running.map((r) => r.pid).join(', ')}) = 終わるか止まるのを待つ\n  ${CONCURRENT_INDEX_RUNBOOK}`, { reason: 'BUILD_IN_PROGRESS' });
        const cur = await readIndexAttrs(db, st.schema, st.name);
        if (cur && cur.notIndex) throw fail(`${key} は index でない (relkind ${cur.relkind}) = 名前が取られている`, { reason: 'NAME_TAKEN' });
        if (cur && cur.valid && cur.ready && cur.live) {
          const diff = attrDiff(cur.attrs, plan.expect.indexes[key]);
          if (diff.length) throw fail(`${key} は同じ名前で定義が違う (人が見る・このファイルは流さない)\n  ${diff.join('\n  ')}`, { reason: 'DEFINITION_MISMATCH', diff });
          log(`${key} は作り済み (valid・属性が期待どおり) = 飛ばす`);
          continue;
        }
        try {
          await checkDiskBeforeCreate(db, st, opts.readDiskMetrics, log);   // 表示だけにしない = 足りない・読めないなら例外
        } catch (e) { e.message = `${f.file}: ${e.message}`; e.version = f.version; throw e; }
        if (cur) {
          log(`${key} は invalid (valid=${cur.valid} ready=${cur.ready} live=${cur.live}) = drop index concurrently してから作り直す`);
          await db.exec(`drop index concurrently if exists ${st.schema}.${st.name}`);
        }
      } else {
        const running = await buildsInProgress(db, null);
        if (running.length) throw fail(`index の作りが別の接続で動いている (pid ${running.map((r) => r.pid).join(', ')}) = 終わるのを待つ`, { reason: 'BUILD_IN_PROGRESS' });
      }
      log(`  ${st.kind} ${key}`);
      try {
        await db.exec(st.sql);
      } catch (e) {
        const hint = /lock timeout/i.test(e.message) ? ` [lock_timeout ${s.lockTimeout}: 長い取引 (夜間のバックアップなど) が終わってからもう一度流す]` : '';
        throw fail(`${key}: ${e.message}${hint}`, { cause: e });
      }
    }
    // 検証 = 作った index が全部 valid・ready・live で属性が期待どおり / 消した index が無い
    for (const [key, st] of plan.creates) {
      const cur = await readIndexAttrs(db, st.schema, st.name);
      if (!cur || cur.notIndex) throw fail(`${key} が無い`, { reason: 'MISSING' });
      if (!(cur.valid && cur.ready && cur.live)) throw fail(`${key} が invalid のまま (valid=${cur.valid} ready=${cur.ready} live=${cur.live})`, { reason: 'INVALID' });
      const diff = attrDiff(cur.attrs, plan.expect.indexes[key]);
      if (diff.length) throw fail(`${key} の属性が期待と違う\n  ${diff.join('\n  ')}`, { reason: 'DEFINITION_MISMATCH', diff });
    }
    for (const key of plan.drops) {
      const [sc, nm] = key.split('.');
      if (await readIndexAttrs(db, sc, nm)) throw fail(`${key} が消えていない`, { reason: 'NOT_DROPPED' });
    }
    await db.query('insert into ops.schema_migrations (version, name, checksum, applied_by) values ($1, $2, $3, $4)', [f.version, f.name, f.checksum, appliedBy]);
  } catch (e) {
    const err = ['MIGRATION_FAILED', 'DISK_CHECK_FAILED'].includes(e.code) ? e : fail(e.message, { cause: e });
    // invalid が残っていれば回収の手順を添える
    try {
      const left = [];
      for (const st of plan.creates.values()) { const cur = await readIndexAttrs(db, st.schema, st.name); if (cur && !cur.notIndex && !cur.valid) left.push(`${st.schema}.${st.name}`); }
      if (left.length) { err.invalidIndexes = left; err.message += `\n  invalid の index が残っている: ${left.join(', ')} = 回収の手順:\n  ${CONCURRENT_INDEX_RUNBOOK}`; }
    } catch { /* 読めなくても元の誤りを出す */ }
    throw noTx(err);
  } finally {
    await resetSession(db);
  }
}

/** concurrent-index に対応しない adapter (PGlite) = 同じ文から concurrently を外し、ふつうの取引で流す。属性の検証は同じ */
async function applyConcurrentIndexFileInTx(db, f, plan, log, appliedBy, lockTimeout, statementTimeout, mode) {
  log(`apply ${f.file} (concurrent-index を取引の中で・concurrently を外す = この adapter は CIC に対応しない) ...`);
  await db.exec('begin');
  try {
    if (mode.mode === 'owner') await db.exec(`set local role ${mode.role}`);
    await db.exec(`set local lock_timeout = '${lockTimeout}'; set local statement_timeout = '${statementTimeout}';`);
    for (const st of plan.statements) {
      if (st.kind === 'create') {
        const cur = await readIndexAttrs(db, st.schema, st.name, { inTx: true });
        if (cur && cur.notIndex) throw new Error(`${st.schema}.${st.name} は index でない (relkind ${cur.relkind}) = 名前が取られている`);
      }
      await db.exec(st.sqlTx);
    }
    for (const [key, st] of plan.creates) {
      const cur = await readIndexAttrs(db, st.schema, st.name, { inTx: true });
      if (!cur || cur.notIndex) throw new Error(`${key} が無い`);
      const diff = attrDiff(cur.attrs, plan.expect.indexes[key]);
      if (diff.length) throw Object.assign(new Error(`${key} の属性が期待と違う (同じ名前で定義が違う index があった?)\n  ${diff.join('\n  ')}`), { reason: 'DEFINITION_MISMATCH', diff });
    }
    for (const key of plan.drops) {
      const [sc, nm] = key.split('.');
      if (await readIndexAttrs(db, sc, nm, { inTx: true })) throw new Error(`${key} が消えていない`);
    }
    if (mode.mode === 'owner') await db.exec(`set local role ${mode.role}`);
    await db.query('insert into ops.schema_migrations (version, name, checksum, applied_by) values ($1, $2, $3, $4)', [f.version, f.name, f.checksum, appliedBy]);
    await db.exec('commit');
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* */ }
    throw Object.assign(new Error(`${f.file} で失敗 (このファイルは巻き戻した。前のファイルまでは適用済み): ${e.message}`), { code: 'MIGRATION_FAILED', version: f.version, reason: e.reason, cause: e });
  }
}

/**
 * db = { query(text, params) → {rows}, exec(text), supportsConcurrentIndex? }  (pg の Client / PGlite の両方をこの形に包む。下の adapters)
 * 🚨 lock は取らない (CLI の入口 = migrateWithLock が取る)。Postgres 専用の SQL は adapter が supportsConcurrentIndex のときの concurrent-index の道だけ
 * 戻り値 { applied: [version...], skipped: [version...], pending: [version...] }
 * opts: dir / to / dryRun / log / appliedBy / lockTimeout / statementTimeout (ふつうの migration)
 *       readDiskMetrics (concurrent-index の容量・{ ok, capacityBytes, usedBytes } を返す関数) / concurrentIndexSettings / expectedPgMajor
 */
export async function applyMigrations(db, opts = {}) {
  const log = opts.log || ((m) => console.log(`[company-db] ${m}`));
  const dir = opts.dir || DEFAULT_DIR;
  const to = opts.to || null;
  const dryRun = !!opts.dryRun;
  const appliedBy = opts.appliedBy || `${process.env.COMPUTERNAME || process.env.HOSTNAME || 'unknown'}/${process.env.USERNAME || process.env.USER || 'unknown'} ${MIGRATE_RUNNER_VERSION}`;
  // 🚨 既存表への ALTER は ACCESS EXCLUSIVE を取る。別の接続が取引を開いたままだと無期限に待ち、後続の読み手まで待機列に入る (PR #1312 Codex R4)
  //    → 各ファイルの取引に lock_timeout / statement_timeout を入れ、待ち切れなければそのファイルだけ巻き戻して失敗にする (少し待ってもう一度流す)
  const lockTimeout = opts.lockTimeout || '10s';
  const statementTimeout = opts.statementTimeout || '10min';

  // 持ち主の mode (Codex R-D60-v3-10 H2) = 記録表の読み書きもこの役割で
  const mode0 = await readOwnerMode(db);
  log(`持ち主の mode = ${modeText(mode0)}`);
  const migExists = await opsTableExists(db, 'schema_migrations');
  if (!migExists && !dryRun) {
    // bootstrap: 記録表だけはここで作る (0001 より前に要る)。あれば DDL を流さない (owner mode の接続の役割は ops に CREATE を持たない)
    await db.exec(`
      create schema if not exists ops;
      create table if not exists ops.schema_migrations (
        version    text primary key,
        name       text not null,
        checksum   text not null,
        applied_at timestamptz not null default now(),
        applied_by text
      );
    `);
  }
  // dry-run は DDL を流さない = 記録表が無ければ「何も適用していない」とみる
  let appliedRows = [];
  if (migExists || !dryRun) {
    try {
      appliedRows = await inModeTx(db, mode0, async () => (await db.query('select version, checksum from ops.schema_migrations order by version')).rows);
    } catch (e) {
      // legacy なのに記録表を読めない = 持ち主を移した後に印 (ops.migrate_owner) が消えた可能性 = legacy に戻して流さない (Codex R-D60-v3-11 M-new-2)
      if (mode0.mode === 'legacy' && e.code === '42501') throw ownerErr(`印 ${OWNER_MARKER_TABLE} が無いのに記録表 ops.schema_migrations を接続の役割で読めない (${e.message}) = 持ち主を移した後に印が消えた可能性 = 流さない`);
      throw e;
    }
  }
  const applied = new Map(appliedRows.map((r) => [r.version, r.checksum]));

  const files = listMigrationFiles(dir);
  // 🚨 持ち主の印と owner-transition の適用を両方向で確かめる (Codex R-D60-v3-11 M-new-2: 印が消えて legacy に戻る fail-open を塞ぐ)
  //   ① owner-transition の migration が適用済み → 印が要る / ② 印がある → owner-transition の migration (file も) が適用済み
  const transFiles = files.filter((f) => f.ownerTransition);
  if (transFiles.length > 1) throw ownerErr(`owner-transition の migration が 2 つある (${transFiles.map((f) => f.file).join(', ')}) = 持ち主の移しは 1 つの file (1 つの取引) だけ`);
  const transFile = transFiles[0] || null;
  const transApplied = !!(transFile && applied.has(transFile.version));
  if (mode0.mode === 'legacy' && transApplied) throw ownerErr(`owner-transition の ${transFile.file} は適用済みなのに印 ${OWNER_MARKER_TABLE} が無い = 印が消えた可能性 = legacy に戻して流さない (印を戻すまで止まる)`);
  if (mode0.mode === 'owner' && !transApplied) throw ownerErr(`印 ${OWNER_MARKER_TABLE} があるのに owner-transition の migration が${transFile ? `適用されていない (${transFile.file})` : ' file に無い'} = 印だけが作られた可能性 = 流さない`);
  // 🚨 DB に記録があるのにファイルが無い = 別ブランチ・別 checkout で流したか、ファイルを消した。黙って成功にしない (Codex R1-M5)
  const onDisk = new Set(files.map((f) => f.version));
  const orphan = [...applied.keys()].filter((v) => !onDisk.has(v));
  if (orphan.length) {
    throw Object.assign(new Error(`DB に適用記録があるのにファイルが無い: ${orphan.join(', ')} (このディレクトリは DB より古い、またはファイルを消した)`), { code: 'ORPHAN_MIGRATIONS', versions: orphan });
  }
  const result = { applied: [], skipped: [], pending: [] };
  // 1 周目 = 判定と流す前の検査を全部 (concurrent-index の文・expect.json・owner-transition)。ここで止まれば何も流さない
  const todo = [];
  for (const f of files) {
    if (applied.has(f.version)) {
      if (applied.get(f.version) !== f.checksum) {
        throw Object.assign(new Error(`${f.file} は適用済みなのに内容が変わっている (checksum 不一致)。適用済みファイルは書き換えず、次の番号で直す`), { code: 'CHECKSUM_MISMATCH', version: f.version });
      }
      result.skipped.push(f.version);
      continue;
    }
    if (to && f.version > to) { result.pending.push(f.version); continue; }
    if (f.concurrentIndex) f.plan = planConcurrentIndexFile(f, dir);
    else rejectUnmarkedConcurrently(f);
    todo.push(f);
  }
  // owner mode で流す file (今が owner・または前に owner-transition がある) は役割を切り替える文を禁止 (設計 13 v3.10)
  let ownerFromHere = mode0.mode === 'owner';
  for (const f of todo) {
    if (f.ownerTransition) { ownerFromHere = true; continue; }
    if (ownerFromHere && !f.concurrentIndex) rejectRoleSwitchInOwnerMode(f);
  }
  const transitions = todo.filter((f) => f.ownerTransition);
  if (transitions.length && mode0.mode === 'owner') throw Object.assign(new Error(`${transitions[0].file} は owner-transition の migration なのに、もう owner mode (${mode0.role}) = 流さない (持ち主の移しは 1 回だけ)`), { code: 'OWNER_MODE_INVALID', version: transitions[0].version });
  if (transitions.length > 1) throw Object.assign(new Error(`owner-transition の migration が 2 つある (${transitions.map((f) => f.file).join(', ')})`), { code: 'OWNER_MODE_INVALID' });
  // 2 周目 = 流す
  for (const f of todo) {
    if (dryRun) {
      log(f.concurrentIndex ? `dry-run: ${f.file} を流す予定 (concurrent-index・${f.plan.statements.length} 文・${db.supportsConcurrentIndex ? '取引の外' : 'concurrently を外して取引の中'})` : `dry-run: ${f.file} を流す予定${f.ownerTransition ? ' (owner-transition = 接続の役割で流し、記録は印の役割で)' : ''}`);
      result.pending.push(f.version);
      continue;
    }
    // file ごとに mode を読み直す (前の file が owner-transition なら、ここから owner mode)
    const mode = await readOwnerMode(db);
    if (f.ownerTransition && mode.mode === 'owner') throw Object.assign(new Error(`${f.file} は owner-transition なのに、もう owner mode = 流さない`), { code: 'OWNER_MODE_INVALID', version: f.version });
    if (f.concurrentIndex) {
      if (db.supportsConcurrentIndex === true) await applyConcurrentIndexFile(db, f, f.plan, opts, log, appliedBy, mode);
      else await applyConcurrentIndexFileInTx(db, f, f.plan, log, appliedBy, lockTimeout, statementTimeout, mode);
      result.applied.push(f.version);
      continue;
    }
    log(`apply ${f.file}${f.ownerTransition ? ' (owner-transition)' : ''} ...`);
    await db.exec('begin');
    try {
      if (mode.mode === 'owner') await db.exec(`set local role ${mode.role}`);   // 取引の終わり (commit / rollback) で戻る = 例外に左右されない
      await db.exec(`set local lock_timeout = '${lockTimeout}'; set local statement_timeout = '${statementTimeout}';`);
      await db.exec(f.text);
      if (mode.mode === 'legacy') {
        // 印を作ってよいのは owner-transition の file だけ / owner-transition の file は印を作り終えていなければならない
        const after = await readOwnerMode(db);
        if (f.ownerTransition) {
          if (after.mode !== 'owner') throw Object.assign(new Error(`owner-transition の file が持ち主の印 (${OWNER_MARKER_TABLE}) を作らなかった`), { code: 'OWNER_MODE_INVALID' });
          await db.exec(`set local role ${after.role}`);   // 記録は移した後の持ち主で (接続の役割は記録表の権限を失っているかもしれない)
          log(`持ち主の mode = ${modeText(after)} に移った`);
        } else if (after.mode === 'owner') {
          throw Object.assign(new Error(`持ち主の印 (${OWNER_MARKER_TABLE}) を作ってよいのは 1 行目が「${OWNER_TRANSITION_MARKER}」の file だけ`), { code: 'OWNER_MODE_INVALID' });
        }
      } else {
        // 記録の INSERT の直前にもう一度 (本文が役割を変えていても、記録は印の役割で = 設計 13 v3.10)
        await db.exec(`set local role ${mode.role}`);
      }
      await db.query('insert into ops.schema_migrations (version, name, checksum, applied_by) values ($1, $2, $3, $4)',
        [f.version, f.name, f.checksum, appliedBy]);
      await db.exec('commit');
    } catch (e) {
      try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
      const hint = /lock timeout|canceling statement due to lock timeout/i.test(e.message) ? ` [lock_timeout ${lockTimeout}: 別の接続が表を使っている。少し待ってもう一度流す]` : '';
      throw Object.assign(new Error(`${f.file} で失敗 (このファイルは巻き戻した。前のファイルまでは適用済み): ${e.message}${hint}`), { code: 'MIGRATION_FAILED', version: f.version, reason: e.code === 'OWNER_MODE_INVALID' ? 'OWNER_MODE_INVALID' : undefined, cause: e });
    }
    result.applied.push(f.version);
  }
  return result;
}

/** 適用状況の一覧 (ファイル × 記録)。読むだけ = lock は取らない */
export async function migrationStatus(db, opts = {}) {
  const dir = opts.dir || DEFAULT_DIR;
  const files = listMigrationFiles(dir);
  let applied = new Map();
  // 記録表が無い = 全部 pending。あれば持ち主の mode の役割で読む (読めない誤りは握りつぶさない = 全部 pending と見せない)
  if (await opsTableExists(db, 'schema_migrations')) {
    const mode = await readOwnerMode(db);
    const rows = await inModeTx(db, mode, async () => (await db.query('select version, checksum, applied_at, applied_by from ops.schema_migrations')).rows);
    applied = new Map(rows.map((r) => [r.version, r]));
  }
  return files.map((f) => {
    const a = applied.get(f.version);
    return {
      version: f.version, name: f.name, concurrentIndex: f.concurrentIndex,
      state: !a ? 'pending' : (a.checksum === f.checksum ? 'applied' : 'CHANGED'),
      applied_at: a?.applied_at || null, applied_by: a?.applied_by || null,
    };
  });
}

/**
 * そのファイルで作る index の属性を今の DB から読んで expect.json の中身を作る (使い捨ての PG で流した後に使う・手で書かない)。
 * 全部 valid でなければ止まる
 */
export async function buildIndexExpect(db, f) {
  if (!f.concurrentIndex) throw Object.assign(new Error(`${f.file} は concurrent-index の migration でない`), { code: 'BAD_OPTIONS' });
  let stmts;
  try { stmts = splitSqlStatements(f.text); } catch (e) { throw rejectCi(f.file, e.message); }
  const creates = new Map();
  for (const st of stmts.map((s) => parseConcurrentIndexStatement(s, f.file))) { const key = `${st.schema}.${st.name}`; if (st.kind === 'create') creates.set(key, st); else creates.delete(key); }
  const indexes = {};
  for (const [key, st] of creates) {
    const cur = await readIndexAttrs(db, st.schema, st.name);
    if (!cur || cur.notIndex) throw new Error(`${key} が無い (先に使い捨ての DB でこのファイルを流す)`);
    if (!(cur.valid && cur.ready && cur.live)) throw new Error(`${key} が invalid`);
    indexes[key] = cur.attrs;
  }
  return { format: EXPECT_FORMAT, pg_major: await serverMajor(db), indexes };
}

// ─── adapters ───
/** node-postgres の Client を { query, exec } に包む。🆕 supportsConcurrentIndex = 取引の外の CIC の道を使う */
export function pgAdapter(client) {
  return {
    query: (text, params) => client.query(text, params),
    exec: (text) => client.query(text),          // 複数文は simple query protocol で流れる (params 無し)
    supportsConcurrentIndex: true,
  };
}
/** PGlite を { query, exec } に包む (テスト用)。🆕 CIC に対応しない = concurrent-index のファイルは concurrently を外して取引の中で流す */
export function pgliteAdapter(pglite) {
  return {
    query: (text, params) => pglite.query(text, params),
    exec: (text) => pglite.exec(text),
    supportsConcurrentIndex: false,
  };
}

/**
 * 接続オプション。
 * 🚨 Render の External URL (dpg-xxx.singapore-postgres.render.com) は TLS 必須。証明書は公的 CA なので検証を有効にする
 *    (rejectUnauthorized: true。切る手段は用意しない。繋がらないときは CA を疑わず接続先を疑う)。
 *    Render 内部 (Internal URL、ホスト名にドットが無い) や localhost は TLS 無し
 */
/** 接続 URL のクエリで許すもの。それ以外 (ssl / sslmode / host / hostaddr / port など) は pg が URL 本体より優先するので全部拒む (Codex R1/R2) */
const ALLOWED_QUERY_KEYS = new Set(['application_name']);
/** TLS 無しでよい接続先 = loopback と Render の内部ホスト名 (dpg-xxxx-a、ドット無し) だけ。それ以外 (IPv6 直指定・短い名前も) は TLS + 検証 (Codex R1) */
const INTERNAL_HOST_RE = /^dpg-[a-z0-9]+(-[a-z0-9]+)?$/;
export function pgClientOptions(url) {
  const u = new URL(url);
  if (!/^postgres(ql)?:$/.test(u.protocol)) throw Object.assign(new Error('接続先は postgres:// で始まる URL'), { code: 'BAD_URL' });
  // 🚨 URL のクエリ (?ssl=no-verify, ?sslmode=..., ?host=別ホスト) は URL 本体や ssl 指定より優先されるので、許可リスト以外は拒む
  for (const k of u.searchParams.keys()) {
    if (!ALLOWED_QUERY_KEYS.has(k)) throw Object.assign(new Error(`接続 URL にクエリ ${k} を付けない (TLS と接続先はコードで決める)`), { code: 'BAD_URL' });
  }
  const host = u.hostname.replace(/^\[|\]$/g, '');
  const internal = host === 'localhost' || host === '127.0.0.1' || host === '::1' || INTERNAL_HOST_RE.test(host);
  return {
    connectionString: url,
    application_name: 'company-db-migrate',
    // 内部は明示的に false (未指定だと PGSSLMODE 等の環境変数を継承する)。外部は検証つき TLS
    ssl: internal ? false : { rejectUnauthorized: true },
  };
}

/**
 * 接続する。`extra` で timeout 等を足せる (connectionTimeoutMillis / query_timeout / statement_timeout など)。
 * 🚨 接続先と TLS は上書きさせない (pgClientOptions の判断をそのまま残す)
 */
export async function openPgClient(url, extra = {}) {
  const { default: pg } = await import('pg');
  const base = pgClientOptions(url);
  const client = new pg.Client({ ...base, ...extra, connectionString: base.connectionString, ssl: base.ssl });
  await client.connect();
  return client;
}

// ─── CLI ───
const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const url = getArg('--url') || process.env.COMPANY_DB_URL;
  if (!url) { console.error('COMPANY_DB_URL (または --url) が要る'); process.exit(2); }
  if (!/^postgres(ql)?:\/\//.test(url)) { console.error('--url は postgres:// で始まる接続文字列'); process.exit(2); }
  const to = getArg('--to');
  if (to && !/^\d{4}$/.test(to)) { console.error('--to は 4 桁の番号'); process.exit(2); }
  const dir = getArg('--dir') ? path.resolve(getArg('--dir')) : DEFAULT_DIR;
  const expectOf = getArg('--index-expect');
  if (args.includes('--index-expect') && !/^\d{4}$/.test(expectOf || '')) { console.error('--index-expect は 4 桁の番号'); process.exit(2); }
  let client = null;
  (async () => {
    client = await openPgClient(url);
    client.on('error', () => {});   // 落ちた接続の誤りは query の失敗として受ける
    const db = pgAdapter(client);
    if (args.includes('--list')) {
      for (const s of await migrationStatus(db, { dir })) console.log(`${s.version} ${s.state.padEnd(8)} ${s.name}${s.concurrentIndex ? ' [concurrent-index]' : ''}${s.applied_at ? `  (${new Date(s.applied_at).toISOString()} by ${s.applied_by})` : ''}`);
      try { console.log(`持ち主の mode = ${modeText(await readOwnerMode(db))}`); } catch (e) { console.log(`持ち主の mode が読めない: ${e.message}`); }
      const h = await describeLockHolder(db).catch(() => []);
      console.log(h.length ? `migrate の lock (${MIGRATE_LOCK_NAME}) を持っている: ${holderText(h)}${h.some((r) => r.minutes >= MIGRATE_LOCK_ALERT_MINUTES) ? ` ⚠️ ${MIGRATE_LOCK_ALERT_MINUTES} 分を超えている` : ''}` : `migrate の lock (${MIGRATE_LOCK_NAME}) を持っている接続は無い`);
      return 0;
    }
    if (expectOf) {
      const f = listMigrationFiles(dir).find((x) => x.version === expectOf);
      if (!f) { console.error(`${expectOf} のファイルが無い`); return 2; }
      console.log(JSON.stringify(await buildIndexExpect(db, f), null, 2));
      return 0;
    }
    const r = await migrateWithLock(db, { dir, to, dryRun: args.includes('--dry-run'), readDiskMetrics: await renderDiskMetricsReader(), lockLog: (m) => console.log(`[company-db] ${m}`) });
    console.log(`[company-db] applied=${r.applied.length} skipped=${r.skipped.length} pending=${r.pending.length}`);
    return 0;
  })().then(async (c) => {
    if (client) await client.end().catch(() => {});
    process.exit(c);
  }).catch(async (e) => {
    console.error(`[company-db] FAILED${e.code ? ` (${e.code})` : ''}: ${e.message}`);
    // 接続が死んでいれば end は待たずに捨てる (session が切れて lock が外れる)
    if (client) { if (e.connectionDead) { try { client.connection.stream.destroy(); } catch { /* */ } } else await client.end().catch(() => {}); }
    process.exit(1);
  });
}
