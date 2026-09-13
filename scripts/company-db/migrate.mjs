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
 * 使い方:
 *   COMPANY_DB_URL=postgres://... node scripts/company-db/migrate.mjs            # 未適用を全部
 *   node scripts/company-db/migrate.mjs --url postgres://... --to 0004          # 0004 まで
 *   node scripts/company-db/migrate.mjs --url ... --list                         # 適用状況だけ
 *   node scripts/company-db/migrate.mjs --url ... --dry-run                      # 何を流すかだけ
 *
 * テストは PGlite (WASM の Postgres) で同じ applyMigrations() を通す (scripts/test-company-db-ddl.mjs)。
 * 終了コード: 0 = 成功 / 1 = 失敗 (途中のファイルで止まる。適用済みぶんはそのまま) / 2 = 引数不正
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const DEFAULT_DIR = path.resolve(__dirname, '../../db/company/migrations');
const FILE_RE = /^(\d{4})_([A-Za-z0-9_-]+)\.sql$/;

/** ファイル内容の checksum (改行コードの違いを吸収。CRLF で checkout されても同じ値) */
export function checksumOf(text) {
  return crypto.createHash('sha256').update(text.replace(/\r\n/g, '\n'), 'utf-8').digest('hex');
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
    out.push({ version, name: m[2], file: f, text, checksum: checksumOf(text) });
  }
  return out;
}

/**
 * db = { query(text, params) → {rows}, exec(text) }  (pg の Client / PGlite の両方をこの形に包む。下の adapters)
 * 戻り値 { applied: [version...], skipped: [version...], pending: [version...] }
 */
export async function applyMigrations(db, opts = {}) {
  const dir = opts.dir || DEFAULT_DIR;
  const log = opts.log || ((m) => console.log(`[company-db] ${m}`));
  const to = opts.to || null;
  const dryRun = !!opts.dryRun;
  const appliedBy = opts.appliedBy || `${process.env.COMPUTERNAME || process.env.HOSTNAME || 'unknown'}/${process.env.USERNAME || process.env.USER || 'unknown'}`;
  // 🚨 既存表への ALTER は ACCESS EXCLUSIVE を取る。別の接続が取引を開いたままだと無期限に待ち、後続の読み手まで待機列に入る (PR #1312 Codex R4)
  //    → 各ファイルの取引に lock_timeout / statement_timeout を入れ、待ち切れなければそのファイルだけ巻き戻して失敗にする (少し待ってもう一度流す)
  const lockTimeout = opts.lockTimeout || '10s';
  const statementTimeout = opts.statementTimeout || '10min';

  // bootstrap: 記録表だけはここで作る (0001 より前に要る)
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
  const { rows: appliedRows } = await db.query('select version, checksum from ops.schema_migrations order by version');
  const applied = new Map(appliedRows.map((r) => [r.version, r.checksum]));

  const files = listMigrationFiles(dir);
  // 🚨 DB に記録があるのにファイルが無い = 別ブランチ・別 checkout で流したか、ファイルを消した。黙って成功にしない (Codex R1-M5)
  const onDisk = new Set(files.map((f) => f.version));
  const orphan = [...applied.keys()].filter((v) => !onDisk.has(v));
  if (orphan.length) {
    throw Object.assign(new Error(`DB に適用記録があるのにファイルが無い: ${orphan.join(', ')} (このディレクトリは DB より古い、またはファイルを消した)`), { code: 'ORPHAN_MIGRATIONS', versions: orphan });
  }
  const result = { applied: [], skipped: [], pending: [] };
  for (const f of files) {
    if (applied.has(f.version)) {
      if (applied.get(f.version) !== f.checksum) {
        throw Object.assign(new Error(`${f.file} は適用済みなのに内容が変わっている (checksum 不一致)。適用済みファイルは書き換えず、次の番号で直す`), { code: 'CHECKSUM_MISMATCH', version: f.version });
      }
      result.skipped.push(f.version);
      continue;
    }
    if (to && f.version > to) { result.pending.push(f.version); continue; }
    if (dryRun) { log(`dry-run: ${f.file} を流す予定`); result.pending.push(f.version); continue; }
    log(`apply ${f.file} ...`);
    await db.exec('begin');
    try {
      await db.exec(`set local lock_timeout = '${lockTimeout}'; set local statement_timeout = '${statementTimeout}';`);
      await db.exec(f.text);
      await db.query('insert into ops.schema_migrations (version, name, checksum, applied_by) values ($1, $2, $3, $4)',
        [f.version, f.name, f.checksum, appliedBy]);
      await db.exec('commit');
    } catch (e) {
      try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
      const hint = /lock timeout|canceling statement due to lock timeout/i.test(e.message) ? ` [lock_timeout ${lockTimeout}: 別の接続が表を使っている。少し待ってもう一度流す]` : '';
      throw Object.assign(new Error(`${f.file} で失敗 (このファイルは巻き戻した。前のファイルまでは適用済み): ${e.message}${hint}`), { code: 'MIGRATION_FAILED', version: f.version, cause: e });
    }
    result.applied.push(f.version);
  }
  return result;
}

/** 適用状況の一覧 (ファイル × 記録) */
export async function migrationStatus(db, opts = {}) {
  const dir = opts.dir || DEFAULT_DIR;
  const files = listMigrationFiles(dir);
  let applied = new Map();
  try {
    const { rows } = await db.query('select version, checksum, applied_at, applied_by from ops.schema_migrations');
    applied = new Map(rows.map((r) => [r.version, r]));
  } catch { /* 記録表がまだ無い */ }
  return files.map((f) => {
    const a = applied.get(f.version);
    return {
      version: f.version, name: f.name,
      state: !a ? 'pending' : (a.checksum === f.checksum ? 'applied' : 'CHANGED'),
      applied_at: a?.applied_at || null, applied_by: a?.applied_by || null,
    };
  });
}

// ─── adapters ───
/** node-postgres の Client を { query, exec } に包む */
export function pgAdapter(client) {
  return {
    query: (text, params) => client.query(text, params),
    exec: (text) => client.query(text),          // 複数文は simple query protocol で流れる (params 無し)
  };
}
/** PGlite を { query, exec } に包む (テスト用) */
export function pgliteAdapter(pglite) {
  return {
    query: (text, params) => pglite.query(text, params),
    exec: (text) => pglite.exec(text),
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
  (async () => {
    const client = await openPgClient(url);
    const db = pgAdapter(client);
    try {
      if (args.includes('--list')) {
        for (const s of await migrationStatus(db)) console.log(`${s.version} ${s.state.padEnd(8)} ${s.name}${s.applied_at ? `  (${new Date(s.applied_at).toISOString()} by ${s.applied_by})` : ''}`);
        return 0;
      }
      const r = await applyMigrations(db, { to, dryRun: args.includes('--dry-run') });
      console.log(`[company-db] applied=${r.applied.length} skipped=${r.skipped.length} pending=${r.pending.length}`);
      return 0;
    } finally {
      await client.end();
    }
  })().then((c) => process.exit(c)).catch((e) => { console.error(`[company-db] FAILED: ${e.message}`); process.exit(1); });
}
