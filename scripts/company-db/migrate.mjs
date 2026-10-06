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
 * 🆕 D-60 PR 3a-i (設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「migrate の runner の契約 (3a-i)」v3.14):
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
 *   - 🆕 Codex R1 (PR #1606) = H1 役割の切り替えの検査は引用の名前も見る / H2 容量の resource と接続先の host を結び付ける / H3 この回に作った index の
 *     予想 × 3 を後の空きから引く + file の合計を先に 1 回で判定 / M1 --list も印の両方向の検査 / M2 属性に tablespace・reloptions /
 *     M3 ふつうの file の最上位の取引の制御を拒む / Low = setSession を try の中に・同じ index の名前の操作は 1 つ
 *   - 🆕 設計 13 v3.13 ①〜⑩・Codex R-D60-v3-13 / v3-14 = ドルの引用の中も役割の検査 (DO・EXECUTE は ⚠️) / ROLE_GRAPH_CHANGED (記録の INSERT → SET CONSTRAINTS
 *     ALL IMMEDIATE の後に比べる) / ROLE_ADMIN_REACHABLE (形 B = 必須・形 A は採らない = owner の状態では必ず拒む) / resource の照合は host + API の databaseName・host の対応は未確認 = CIC を流さない /
 *     予約に前の回の valid も / reltablespace・reloptions の正規化 / session の設定を戻せなければ接続を捨てる。
 *     🚨 owner の状態の migration は sandbox ではない (記録の直前の SET LOCAL ROLE が守るのは記録の行だけ)
 *   - 🆕 Codex R2 (PR #1606) High = Unicode の escape の名前・文字列 (U&"…"・U&'…'・UESCAPE 句) は owner の状態の file では一律に拒む
 *     (U&"role" で役割の検査を迂回できた・decode はしない = 下の unicodeEscapeLabel)。legacy の file は ⚠️ だけ (今までどおり流す)
 *   - 🆕 Codex R3 (PR #1606) High 1 = 役割を変えられる GUC (ROLE_GUCS = role・session_authorization・PG 18 の guc_tables.c の棚卸し) を変える全部の形
 *     (SET [LOCAL | SESSION] <GUC>・RESET <GUC>・set_config・関数の SET 句・ALTER ROLE / DATABASE … SET) を owner の状態の file で拒む /
 *     High 2 = 字句の前提 (LEXER_PREMISE = standard_conforming_strings on・client_encoding UTF8) を owner の状態の file の本文の前に別のクエリで SET LOCAL し
 *     (concurrent-index の session と PGlite の道も)、file の中で LEXER_GUCS を変える文 (同じ全部の形・SET NAMES・UPDATE pg_settings) を拒む。
 *     🚨 字句の検査は補助で sandbox ではない (動的 SQL の中は見ない)・記録の直前の再 SET が守るのは記録の行だけ = 本当の役割の境は形 B (接続の役割が役割の管理に届かない・ROLE_ADMIN_REACHABLE)
 *   - 🆕 Codex R4 (PR #1606) High 1 = 字句の前提の SET LOCAL を **取引の中で流す全部の file (legacy・owner-transition・owner)** の本文の前に (取引の制御の文の検査も
 *     lexSql に頼る)。legacy・owner-transition の file の中の字句の前提の GUC の変更は ⚠️ だけ (lexerPremiseChanges) /
 *     High 2 = lexSql を PostgreSQL 18 の scan.l に合わせる (行コメントは \r でも終わる・空白は [ \t\n\r\f\v] だけ = NBSP・U+3000 は識別子の文字)・1 行目の印も CR だけの改行に合わせる。
 *     🚨 owner-transition の file 自身は ROLE_ADMIN_REACHABLE の対象の外 (流す時は legacy) = R0b-0 / R0b までの間に別の owner の migration を流さない運用が必須 (README)
 *   - 🆕 Codex R5 (PR #1606) M1 = この回に流す全部の CIC の file の create の合計を最初の CIC の file の前に 1 回で判定 (checkDiskForRun・足りなければ 1 本も作らない) /
 *     M2 = runner の検査・記録の SQL は search_path = pg_catalog, pg_temp の取引の中 + 関数・表・型を pg_catalog. で修飾 (前の file が残した search_path と
 *     同じ名前の jsonb_agg・pg_class などに騙されない・本文の前に元の search_path へ戻す = withCatalogPath) /
 *     M3 = owner の状態で、SET で届く owner の役割 (印の役割・ALLOWED_SET_LOCAL_ROLES・接続の役割が SET で届く役割) が全部 NOLOGIN かを確かめる
 *     (OWNER_ROLE_CAN_LOGIN・本文はその役割の password を見えずに変えられる = 資格として使えないことだけを保証) / Low = --dry-run も字句の前提 (reset_val) を確かめる
 *   - 🆕 Codex R6 (PR #1606) Low = PGlite の道 (applyConcurrentIndexFileInTx) も CIC の文は元の session の search_path で流す (本物の PG と同じ・runner の SQL だけ固定)
 *
 * 使い方:
 *   COMPANY_DB_URL=postgres://... node scripts/company-db/migrate.mjs            # 未適用を全部
 *   node scripts/company-db/migrate.mjs --url postgres://... --to 0004          # 0004 まで
 *   node scripts/company-db/migrate.mjs --url ... --list                         # 適用状況だけ (lock は取らない・持ち主の pid を出す)
 *   node scripts/company-db/migrate.mjs --url ... --dry-run                      # 何を流すかだけ (try の lock を取る・DDL は流さない・許す文と expect.json を検査)
 *   node scripts/company-db/migrate.mjs --url <使い捨ての DB> --index-expect 0057 # そのファイルの index の属性 (expect.json の中身) を出す
 *   🆕 concurrent-index の file は 45 分の見張りの接続が無いと流れない (LOCK_WATCH_REQUIRED) = 本番は node scripts/company-db/migrate-watched.mjs から
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
 * 🆕 45 分の見張り (scripts/company-db/migrate-lock-watch.mjs) の接続の application_name。CLI は concurrent-index の各文の前に、
 * この名前の接続が同じ DB にあることを確かめ、無ければ流さない (LOCK_WATCH_REQUIRED・設計 13 §3.10 v3.9「見張り」・PR #1638 Codex R1 High)。
 * 見張りの起動 → GChat に送れたことの確認 → migrate は 1 つのコマンド (scripts/company-db/migrate-watched.mjs) がする
 */
export const MIGRATE_LOCK_WATCH_APPLICATION_NAME = 'company-db-migrate-lock-watch';
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

/**
 * 1 行目 (先頭の BOM は除く) の行末の空白を除いたもの。🆕 Codex R4 High 2 (PR #1606): 行の終わり = PostgreSQL の scan.l の newline [\n\r] と同じ
 * (CR だけの改行も行の終わり = lexSql の行コメントと同じ)。行末の空白も scan.l の non_newline_space [ \t\f\v] だけ (NBSP などは空白でない)
 */
const BOM = String.fromCharCode(0xfeff);
function firstLineOf(text) {
  const t = text.startsWith(BOM) ? text.slice(1) : text;
  return t.split(/[\r\n]/, 1)[0].replace(/[ \t\f\v]+$/, '');
}
/** 1 行目が concurrent-index の印か (BOM・行末の空白は無視・CR だけの改行も行の終わり) */
export function isConcurrentIndexText(text) {
  return firstLineOf(text) === CONCURRENT_INDEX_MARKER;
}

// ─── 持ち主の mode (Codex R-D60-v3-10 H2・PR 1b の前 / PR 1b 自身 / PR 1b の後) ───
/**
 * PR 1b (持ち主の隔離) の migration の 1 行目。この file だけが持ち主の印 (ops.migrate_owner) を作ってよい。
 *   - legacy (印が無い = PR 1b の前・今の本番) = 接続の役割のまま流す (今までどおり)
 *   - owner-transition の file 自身 = 接続の役割で流し (持ち主の移しをする・🆕 役割を作る / membership を変えるのは migration の外 = 役割の図の比較の対象)、同じ取引の終わりに印ができたことを確かめ、
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
  return firstLineOf(text) === OWNER_TRANSITION_MARKER;
}
const ownerErr = (msg) => Object.assign(new Error(`持ち主の mode: ${msg}`), { code: 'OWNER_MODE_INVALID' });

// ─── runner の SQL の名前の解決 (Codex R5 M2・PR #1606) ───
/**
 * 🆕 Codex R5 M2 (PR #1606): runner が流す検査・記録の SQL は、前の file が session に残した search_path (`SET search_path = attacker, pg_catalog`)
 * に左右されない = 解決先を pg_catalog に固定する。PostgreSQL は pg_catalog を search_path に明示で後ろに置くと、前の schema の同じ名前の
 * 関数・aggregate・演算子・型・表が組み込みを隠す (exact match の関数は pg_catalog の多相の関数より先に選ばれる = 後ろに置かなくても効く)。
 * 守りは 2 つ重ねる: ① 取引の中で `SET LOCAL search_path = pg_catalog, pg_temp` (演算子・型・表も含めて全部・pg_temp を最後 = 一時の表に隠されない)
 * ② 関数・表・型は SQL の中でも pg_catalog. で修飾 (① が外れても関数・表は変わらない)。
 * migration の本文は今までどおりの search_path で流す (固定した値は本文の前に元へ戻す = 本文の名前の解決は変えない)
 */
export const CATALOG_SEARCH_PATH = 'pg_catalog, pg_temp';
/**
 * fn を search_path = pg_catalog, pg_temp の中で流す。
 *   inTx = false (取引の外から呼ぶ) = begin → SET LOCAL → fn → commit (失敗は rollback して投げ直す)
 *   inTx = true (呼び手の取引の中) = 今の値を覚えて SET LOCAL → fn → set_config(…, true) で元の値に戻す (本文の前に呼ぶ・本文の名前の解決を変えない)
 */
async function withCatalogPath(db, fn, { inTx = false } = {}) {
  if (inTx) {
    const prev = (await db.query("select pg_catalog.current_setting('search_path') as p")).rows[0].p;
    await db.exec(`set local search_path = ${CATALOG_SEARCH_PATH}`);
    const r = await fn();
    await db.query("select pg_catalog.set_config('search_path', $1, true)", [prev]);
    return r;
  }
  await db.exec('begin');
  try {
    await db.exec(`set local search_path = ${CATALOG_SEARCH_PATH}`);
    const r = await fn();
    await db.exec('commit');
    return r;
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する = 元の誤りを出す */ }
    throw e;
  }
}

/**
 * 今の持ち主の mode を読む (catalog だけ = 接続の役割で読める)。
 * 戻り = { mode: 'legacy' } | { mode: 'owner', role }。役割の名前が不正・SET できない・記録表の持ち主と違う = OWNER_MODE_INVALID
 * 🆕 Codex R5 M2: search_path = pg_catalog の中で読む (前の file が attacker.pg_class を search_path の前に置いても印を隠せない)。
 *   inTx = true = 呼び手の取引の中 (本文の後 = もう pg_catalog に固定してある)
 */
export async function readOwnerMode(db, { inTx = false } = {}) {
  // 🚨 catalog だけで読む (pg_class / pg_namespace は誰でも読める)。PR 1b の後は接続の役割が ops の USAGE を持たないかもしれない = to_regclass や表の SELECT に頼らない
  return withCatalogPath(db, async () => {
    const { rows } = await db.query(`select
        (select pg_catalog.pg_get_userbyid(c.relowner)::pg_catalog.text from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace where n.nspname = 'ops' and c.relname = 'migrate_owner' and c.relkind in ('r', 'p')) as marker_owner,
        (select pg_catalog.pg_get_userbyid(c.relowner)::pg_catalog.text from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace where n.nspname = 'ops' and c.relname = 'schema_migrations' and c.relkind in ('r', 'p')) as mig_owner`);
    const role = rows[0].marker_owner;
    if (role == null) return { mode: 'legacy' };
    if (!ROLE_RE.test(role)) throw ownerErr(`印の表 ${OWNER_MARKER_TABLE} の持ち主の名前が不正: ${role}`);
    if (rows[0].mig_owner !== role) throw ownerErr(`ops.schema_migrations の持ち主 (${rows[0].mig_owner}) が印の表 ${OWNER_MARKER_TABLE} の持ち主 ${role} と違う`);
    const { rows: can } = await db.query(`select pg_catalog.pg_has_role(session_user, $1, 'SET') as can`, [role]);
    if (!can[0].can) throw ownerErr(`接続の役割 (session_user) が ${role} に SET ROLE できない (grant ${role} to <接続の役割> with set true)`);
    return { mode: 'owner', role };
  }, { inTx });
}
/** ops の表があるか (catalog だけ = schema の USAGE が無くても読める)。🆕 Codex R5 M2: search_path = pg_catalog の中で */
async function opsTableExists(db, name) {
  return withCatalogPath(db, async () => {
    const { rows } = await db.query(`select exists (select 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace where n.nspname = 'ops' and c.relname = $1) as e`, [name]);
    return rows[0].e === true;
  });
}
const modeText = (m) => (m.mode === 'owner' ? `owner (SET ROLE ${m.role})` : 'legacy (接続の役割のまま)');
/** 取引の中で fn (owner なら SET LOCAL ROLE)。読むだけの短い取引に使う。🆕 Codex R5 M2: search_path = pg_catalog, pg_temp */
async function inModeTx(db, mode, fn) {
  await db.exec('begin');
  try {
    await db.exec(`set local search_path = ${CATALOG_SEARCH_PATH}`);
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
 * 🆕 Codex R4 High 2 (PR #1606): PostgreSQL 18 の scan.l に合わせる (棚卸しの表 = db/company/README.md「字句の検査と scan.l の違い」)
 *   - 行コメント (--) の終わり = \r と \n の早い方 (scan.l の non_newline [^\n\r])。CR だけの改行の後の文は SQL として流れる (`-- c<CR>COMMIT;`)
 *   - 空白 = scan.l の space [ \t\n\r\f\v] だけ。NBSP・和文の空白 (U+3000) などの JS の \s は Postgres では識別子の文字 (\200-\377) =
 *     `x<NBSP>$a$` は 1 つの識別子でドルの引用にならない (JS の \s で空白とみると、後ろをドルの引用の中と誤って COMMIT を見落とす)
 * 前提 = standard_conforming_strings = on・client_encoding = UTF8 (runner が取引の中で流す全部の file の本文の前に別のクエリで SET LOCAL する = setLexerPremiseLocal)
 */
export function lexSql(text) {
  const toks = [];
  const n = text.length;
  let i = 0;
  const isSpace = (c) => c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v';
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
    if (isSpace(c)) { i++; continue; }
    if (c === '-' && d === '-') { let j = i + 2; while (j < n && text[j] !== '\n' && text[j] !== '\r') j++; i = j; continue; }
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
 * 文を index の名前ごとに分ける。🆕 Codex R1 Low: 同じ名前 (schema.name) の操作が 2 つ以上 (create 2 回・drop と create など) = 拒む
 * (Map で後ろが前を潰すと、if not exists と組んで前の定義を最終形として記録しうる)
 */
function groupIndexStatements(statements, file) {
  const creates = new Map(), drops = new Set();
  for (const s of statements) {
    const key = `${s.schema}.${s.name}`;
    if (creates.has(key) || drops.has(key)) throw rejectCi(file, `同じ index ${key} の操作が 2 つ以上ある (1 つの file では 1 つの名前に 1 つの操作だけ)`);
    if (s.kind === 'create') creates.set(key, s); else drops.add(key);
  }
  return { creates, drops };
}

/**
 * concurrent-index のファイルの計画 (流す前の検査を全部)。止めるときは CONCURRENT_INDEX_REJECTED。
 * 戻り = { statements, expect, creates: Map<'schema.name', stmt>, drops: Set<'schema.name'> } (同じ名前の操作は 1 つだけ)
 */
export function planConcurrentIndexFile(f, dir = DEFAULT_DIR) {
  let stmts;
  try { stmts = splitSqlStatements(f.text); } catch (e) { throw rejectCi(f.file, e.message); }
  if (!stmts.length) throw rejectCi(f.file, '文が 1 つも無い');
  const statements = stmts.map((s) => parseConcurrentIndexStatement(s, f.file));
  const { creates, drops } = groupIndexStatements(statements, f.file);
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
 * 🆕 Codex R2 High (PR #1606): toks[i] から始まる Unicode の escape の名前・文字列 (U&"…"・U&'…') と UESCAPE 句の印 (無ければ null)。
 *   Postgres では U&"r\006Fle" は引用の名前 "role" の別の書き方 = `RESET U&"role"`・`SET LOCAL U&"r\006Fle" = 'none'`・
 *   `pg_catalog.U&"set_confi\0067"('role', 'none', true)` が役割の検査を迂回できた (字句では U・&・"…" の 3 つに分かれる)。
 *   形 = 単独の名前 u (大文字小文字を問わない) → & → 引用の名前か文字列。間の空白・コメントも拒む向きに含める (Postgres では空白を挟むと
 *   U & "x" = 演算子になり escape ではないが、区別しない = 拒む向きに倒す)。UESCAPE は U& の後にしか書けない = 単独でも拒む。
 * 🚨 正しく decode する道は採らない: \XXXX・\+XXXXXX・UESCAPE の任意の escape の文字・サロゲートの組・server の encoding・
 *   standard_conforming_strings との組み合わせを Postgres と 1 文字も違わずに再現しなければならず、1 つでもずれると「無い」と言う側 (fail-open) に倒れる。
 *   一律に拒むのは fail-closed で、migration の file は U& を要らない (0001〜0056 に 1 つも無い・普通の "…" で書ける)
 */
function unicodeEscapeLabel(toks, i) {
  const tk = toks[i];
  if (!tk || tk.t !== 'word') return null;
  const w = tk.v.toLowerCase();
  if (w === 'uescape') return 'UESCAPE 句 (Unicode の escape・中身を読まずに拒む)';
  const amp = toks[i + 1], q = toks[i + 2];
  if (w === 'u' && amp && amp.t === 'punct' && amp.v === '&' && q && (q.t === 'qid' || q.t === 'str')) {
    return `${q.t === 'qid' ? 'U&"…" (Unicode の escape の名前' : "U&'…' (Unicode の escape の文字列"}・中身を読まずに拒む)`;
  }
  return null;
}
/**
 * 文 (ドルの引用の中も深さ 4 まで) の Unicode の escape の印の一覧 ([] = 無い)。legacy の file の ⚠️ に使う (owner の状態は roleSwitchStatements が拒む)。
 * 字句に読めない = 例外 (呼び手が決める)
 */
export function unicodeEscapeLiterals(text, depth = 0) {
  const toks = lexSql(text);
  const hits = [];
  for (let i = 0; i < toks.length; i++) {
    const ue = unicodeEscapeLabel(toks, i);
    if (ue) hits.push(ue);
    const tk = toks[i];
    if (tk.t !== 'dollar' || depth >= 4) continue;
    const tag = /^\$[^$]*\$/.exec(tk.v)[0];
    let sub;
    try { sub = unicodeEscapeLiterals(tk.v.slice(tag.length, tk.v.length - tag.length), depth + 1); } catch { continue; }
    for (const h of sub) hits.push(h.startsWith('(ドルの引用の中) ') ? h : `(ドルの引用の中) ${h}`);
  }
  return hits;
}
function warnUnicodeEscapeInLegacy(f, log = () => {}) {
  let hits;
  try { hits = unicodeEscapeLiterals(f.text); } catch { return; }   // legacy = 読めない file は今までどおり Postgres に任せる
  if (hits.length) log(`⚠️ ${f.file}: Unicode の escape (${[...new Set(hits)].join('・')}) がある。legacy (印が無い) なので今までどおり流すが、owner の状態 (PR 1b の後) では流す前に拒む = 普通の "…"・'…' で書く`);
}
/**
 * 🆕 Codex R4 High 1 (PR #1606): file の中で字句の前提の GUC (LEXER_GUCS・SET NAMES・pg_settings) を変える文の一覧 ([] = 無い・字句に読めない = 例外)。
 * legacy と owner-transition の file では ⚠️ だけ (今までどおり流す)。拒まない理由 =
 *   ① runner は取引の中で流す全部の file の本文の前に、別のクエリで前提を入れ直す (setLexerPremiseLocal)
 *   ② Postgres は simple query の本文の全部を、どの文を流すよりも前に字句に分ける = file の中の変更は、その file の本文と後の file の本文の境目を変えられない
 *      (変えられるのは同じ file の後の DO の本文・動的 SQL = legacy と owner-transition では元から役割の検査をしない・DO の中の COMMIT は Postgres が止める)
 *   ③ ALTER ROLE / DATABASE … SET で既定 (reset_val) を変えれば、次の回は全部の file が LEXER_PREMISE で止まる (fail-closed)
 * owner の状態の file では拒む (rejectRoleSwitchInOwnerMode = DO の本文の役割の検査の前提を守る)
 */
export function lexerPremiseChanges(text) {
  return roleSwitchStatements(text).filter((h) => h.includes('字句の前提の GUC'));
}
function warnLexerGucOutsideOwner(f, log = () => {}) {
  let hits;
  try { hits = lexerPremiseChanges(f.text); } catch { return; }   // 読めない file は今までどおり Postgres に任せる (取引の制御の検査が先に止める)
  if (hits.length) log(`⚠️ ${f.file}: 字句の前提の GUC を変える文 (${[...new Set(hits)].join('・')}) がある。${f.ownerTransition ? 'owner-transition' : 'legacy (印が無い)'} の file なので今までどおり流すが、runner は次の file の本文の前に前提 (${Object.entries(LEXER_PREMISE).map(([k, v]) => `${k} = ${v}`).join('・')}) を入れ直す。owner の状態 (PR 1b の後) では流す前に拒む`);
}

/**
 * owner の状態の file で `SET LOCAL ROLE <名前>` を許す役割の一覧 (設計 13 v3.11 ③・正本はこの定数)。
 * 印の役割 cdb_owner と NOLOGIN の持ち主 5 つ (「以後の更新」の専用の持ち主の関数・設計 19 の F4-2a が file の中で使う)
 */
export const ALLOWED_SET_LOCAL_ROLES = Object.freeze(['cdb_owner', 'profit_definer', 'heavy_guard_definer', 'heavy_read_definer', 'd60_calib_definer', 'finance_revision_definer']);

/**
 * 🆕 Codex R3 High 1 (PR #1606): 役割 (権限の主体 = current_user・session_user) を変えられる GUC の集合 (正本はこの定数)。
 * PostgreSQL 18 の guc_tables.c の棚卸し = assign の hook が SetCurrentRoleId / SetSessionAuthorization を呼ぶのは
 *   role (assign_role) と session_authorization (assign_session_authorization) の 2 つだけ。
 *   is_superuser は PGC_INTERNAL (SET できない・表示だけ)。createrole_self_grant は CREATEROLE の役割が作った役割の membership の既定 =
 *   権限の主体は変えない (CREATEROLE は ROLE_ADMIN_REACHABLE が owner の状態で拒む・membership の変化は ROLE_GRAPH_CHANGED が見る)。
 * owner の状態の file では、この集合の GUC を変える全部の形を拒む = SET [LOCAL | SESSION] <GUC> {TO | =} … / RESET <GUC> /
 *   set_config('<GUC>', …) / 関数の SET 句 (CREATE / ALTER FUNCTION … SET <GUC> …) / ALTER ROLE | DATABASE | SYSTEM … SET | RESET <GUC> /
 *   UPDATE pg_settings (rule で set_config を呼ぶ。role・session_authorization は GUC_NO_SHOW_ALL = pg_settings に出ない = UPDATE しても変わらないが、
 *   LEXER_GUCS は変えられる = 参照ごと拒む)。例外 = SET LOCAL ROLE <ALLOWED_SET_LOCAL_ROLES> (専用の文法) だけ
 */
export const ROLE_GUCS = Object.freeze(['role', 'session_authorization']);
/**
 * 🆕 Codex R3 High 2 (PR #1606): 字句 (lexSql) と Postgres の文字列・引用の境目の前提を変える GUC。
 *   standard_conforming_strings = off なら '…' の中の \ が escape になる ('a\'b' の境目がずれる・'r\ole' が role になる)。
 *   backslash_quote・escape_string_warning は同じ系統 (\' の扱い・警告)。client_encoding (と同じ意味の SET NAMES) は送った bytes の読み方 =
 *   SJIS などでは 2 byte 目の 0x5C (\) を文字に飲み込む = E'…' の境目がずれる。
 * owner の状態の file では、この集合の GUC を変える全部の形を拒み (ROLE_GUCS と同じ形)、runner は本文の前に別のクエリで
 *   LEXER_PREMISE を SET LOCAL する (前の file・動的 SQL が session に残した値に左右されない = lexSql の前提と一致させる)
 */
export const LEXER_GUCS = Object.freeze(['standard_conforming_strings', 'backslash_quote', 'escape_string_warning', 'client_encoding']);
/** owner の状態の file で変えてはいけない GUC の全部 (ROLE_GUCS + LEXER_GUCS) */
export const OWNER_MODE_FORBIDDEN_GUCS = Object.freeze([...ROLE_GUCS, ...LEXER_GUCS]);
/** runner が owner の状態の file の本文の前 (と concurrent-index の session) に入れる字句の前提 (lexSql と同じ読み方) */
export const LEXER_PREMISE = Object.freeze({ standard_conforming_strings: 'on', client_encoding: 'UTF8' });
const gucKind = (g) => (ROLE_GUCS.includes(g) ? '役割の GUC' : '字句の前提の GUC');

/**
 * 役割を切り替える文のうち、owner の状態の file で許さないもの (コメント・文字列の外を見る・🆕 ドルの引用の中も同じに見る = R-D60-v3-13 H1)。戻り = 理由の一覧 ([] = 無い)。
 * 拒む (設計 13 v3.11 ③) = RESET ROLE・SET ROLE (LOCAL の無い = session)・SET SESSION ROLE・SET [LOCAL] ROLE NONE・SET / RESET SESSION AUTHORIZATION・
 *   set_config('role', …)・DISCARD・SET LOCAL ROLE <一覧の外の役割>。許す = SET LOCAL ROLE <ALLOWED_SET_LOCAL_ROLES のどれか> だけ。
 * 🆕 Codex R2 High (PR #1606): Unicode の escape の名前・文字列 (U&"…"・U&'…'・UESCAPE 句) は中身を読まずに拒む (下の unicodeEscapeLabel)。
 * 🆕 Codex R3 High 1・2 (PR #1606): ROLE_GUCS (role・session_authorization) と LEXER_GUCS (standard_conforming_strings ほか) を変える全部の形
 *   (SET [LOCAL | SESSION] <GUC>・RESET <GUC>・set_config・関数の SET 句・ALTER ROLE / DATABASE / SYSTEM … SET | RESET・SET NAMES・pg_settings) も拒む。
 *   引用の名前・大文字小文字・コメント・ドルの引用の中も同じ (下の lw と再帰)。RESET ALL は役割を戻さない (role・session_authorization は
 *   GUC_NO_RESET_ALL) ので許すが、字句の前提の GUC は RESET ALL で reset_val に戻る = runner が reset_val も LEXER_PREMISE と同じかを確かめる (setLexerPremiseLocal)
 * 🚨 守りの分担 (Codex R4 Low で統一): ① 字句の検査 (この関数) は補助で sandbox ではない (動的 SQL・EXECUTE の中は見ない) /
 *   ② 記録の INSERT の直前の SET LOCAL ROLE <印の役割> が守るのは **記録の行だけ** (本文の副作用 = 役割を変えた後の DDL は守らない) /
 *   ③ 本当の役割の境は **形 B** (接続の役割が役割の管理・危険な権限に届かない = ROLE_ADMIN_REACHABLE・役割の図の比較 ROLE_GRAPH_CHANGED)。
 *   役割の管理は migration に書かず、別の資格・別の session で流す (設計 13 v3.13・v3.14)
 */
export function roleSwitchStatements(text, allowed = ALLOWED_SET_LOCAL_ROLES, depth = 0) {
  const toks = lexSql(text);   // 読めない = 例外 (owner の状態では止まる)
  // 🚨 引用の名前 (qid) も小文字にして同じに見る (Codex R1 H1: RESET "role"・SET LOCAL "role" = 'none'・"set_config"('role', …) で検査を迂回できた)。
  //   設定の名前 (GUC) は Postgres が大文字小文字を区別しない = "ROLE" も role。関数・キーワードの引用は Postgres では別物になりうるが、拒む向きに倒す
  const lw = (tk) => {
    if (!tk) return null;
    if (tk.t === 'word') return tk.v.toLowerCase();
    if (tk.t === 'qid') return tk.v.slice(1, -1).replace(/""/g, '"').toLowerCase();
    return null;
  };
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
    const ue = unicodeEscapeLabel(toks, i);   // 🆕 Codex R2 High: U&"…"・U&'…'・UESCAPE は名前を読まずに一律に拒む (ドルの引用の中も下の再帰で同じ)
    if (ue) hits.push(ue);
    const w0 = lw(toks[i]), w1 = lw(toks[i + 1]), w2 = lw(toks[i + 2]);
    // RESET <GUC> (ALTER ROLE / DATABASE / FUNCTION / SYSTEM … RESET <GUC> も同じ字句)。🆕 Codex R3 High 1: RESET session_authorization (1 語の名前) も
    if (w0 === 'reset') {
      if (w1 === 'session' && w2 === 'authorization') hits.push('reset session authorization');
      else if (OWNER_MODE_FORBIDDEN_GUCS.includes(w1)) hits.push(`reset ${w1} (${gucKind(w1)})`);
    }
    if (w0 === 'discard' && ['all', 'plans', 'sequences', 'temp', 'temporary'].includes(w1)) hits.push(`discard ${w1}`);
    if (w0 === 'set_config' && isPunct(toks[i + 1], '(')) {
      // 1 つ目の引数が「ただの文字列 1 つ」で禁止の GUC (OWNER_MODE_FORBIDDEN_GUCS) 以外のときだけ通す (U&'role'・'ro' || 'le'・E'\x72ole'・式・引用の名前は全部拒む向き)
      const a = toks[i + 2];
      const plain = a && a.t === 'str' && a.v.startsWith("'") && isPunct(toks[i + 3], ',');
      const g = plain ? a.v.slice(1, -1).replace(/''/g, "'").trim().toLowerCase() : null;
      if (!plain) hits.push("set_config(<ただの文字列でない名前>, …)");
      else if (OWNER_MODE_FORBIDDEN_GUCS.includes(g)) hits.push(`set_config('${g}', …) (${gucKind(g)})`);
    }
    // 🆕 Codex R3: UPDATE pg_settings は rule で set_config(name, setting, false) を呼ぶ = 名前を読まずに拒む (参照も)。role・session_authorization は
    //   GUC_NO_SHOW_ALL で pg_settings に出ない (UPDATE しても変わらない) が、字句の前提の GUC (standard_conforming_strings・client_encoding ほか) は変えられる
    if (w0 === 'pg_settings') hits.push('pg_settings (UPDATE で set_config と同じ・字句の前提の GUC を変えられる)');
    if (w0 === 'set') {
      // SET [LOCAL | SESSION] <名前> … (ALTER ROLE / DATABASE / FUNCTION / SYSTEM … SET と関数の SET 句も同じ字句)。SET SESSION AUTHORIZATION の SESSION は範囲の語でない
      let j = i + 1, scope = '';
      const a = lw(toks[j]);
      if ((a === 'local' || a === 'session') && !(a === 'session' && lw(toks[j + 1]) === 'authorization')) { scope = a; j++; }
      const v = lw(toks[j]), v2 = lw(toks[j + 1]);
      const sp = scope ? `${scope} ` : '';
      if (v === 'session' && v2 === 'authorization') hits.push(`set ${sp}session authorization`);
      else if (v === 'role') {
        if (scope === 'session') hits.push('set session role');
        else if (scope !== 'local') hits.push('set role (session)');
        else {
          // 許すのは SET LOCAL ROLE <一覧の役割> だけ (role TO x・role = x の generic の形も名前が to / = に見えて拒む向き)
          const name = nameOf(toks[j + 1]);
          const after = toks[j + 2];
          if (name == null) hits.push('set local role (名前が読めない)');
          else if (toks[j + 1].t === 'word' && name === 'none') hits.push('set local role none');
          else if (!allowed.includes(name)) hits.push(`set local role ${name} (許す一覧の外)`);
          else if (after && !isPunct(after, ';')) hits.push(`set local role ${name} の後ろに「${after.v}」`);
        }
      } else if (OWNER_MODE_FORBIDDEN_GUCS.includes(v)) hits.push(`set ${sp}${v} (${gucKind(v)})`);
      else if (v === 'names') hits.push(`set ${sp}names (= client_encoding・字句の前提の GUC)`);
    }
  }
  // 🆕 Codex R-D60-v3-13 H1: ドルの引用の中 (DO の本文・関数の本文) も同じ検査で見る (`do $$ begin perform set_config('role', 'none', true); end $$` を通さない)。
  //   中の中 (入れ子のドルの引用) も見る (深さ 4 まで・それより深い = 拒む)。中を字句に読めない = 拒む (見ていないのに「無い」と言わない)。
  //   🚨 それでも EXECUTE の文字列・format()・関数の呼び出しの先は見ない = この検査は補助で、sandbox ではない (下の ownerModeUncheckedNotes で警告を出す)
  for (const tk of toks) {
    if (tk.t !== 'dollar') continue;
    const tag = /^\$[^$]*\$/.exec(tk.v)[0];
    const inner = tk.v.slice(tag.length, tk.v.length - tag.length);
    if (depth >= 4) { hits.push('ドルの引用の入れ子が深すぎる'); continue; }
    let sub;
    try { sub = roleSwitchStatements(inner, allowed, depth + 1); } catch { hits.push('ドルの引用の中を字句に読めない'); continue; }
    for (const h of sub) hits.push(h.startsWith('(ドルの引用の中) ') ? h : `(ドルの引用の中) ${h}`);
  }
  return hits;
}
/**
 * 🆕 Codex R-D60-v3-13 H1: owner の状態の file で、字句の検査が中まで見られない形 (DO の本文の EXECUTE = 動的 SQL・関数の本文の EXECUTE) の数。
 * 拒まない (0001〜0056 のうち 18 の file が DO・22 の file がドルの引用の中で EXECUTE を使う = 拒むと今の書き方が使えない) = runner は警告を出すだけ。
 * 役割の境 (役割の管理・membership の変更) は migration の中で守れない = 別の資格・別の session で流す (設計 13 v3.13)
 */
export function ownerModeUncheckedNotes(text) {
  let toks;
  try { toks = lexSql(text); } catch { return ['字句に読めない']; }
  const notes = [];
  // 文の頭の do を数える (頭 = 最初の字句か ; の次)
  const doCount = toks.filter((tk, i) => tk.t === 'word' && tk.v.toLowerCase() === 'do' && (i === 0 || (toks[i - 1].t === 'punct' && toks[i - 1].v === ';'))).length;
  if (doCount) notes.push(`DO ${doCount} 個`);
  const execCount = toks.filter((tk) => tk.t === 'dollar' && /\bexecute\b/i.test(tk.v)).length;
  if (execCount) notes.push(`ドルの引用の中の EXECUTE (動的 SQL) ${execCount} 個`);
  return notes;
}
function rejectRoleSwitchInOwnerMode(f, log = () => {}) {
  let hits;
  try { hits = roleSwitchStatements(f.text); } catch (e) { throw Object.assign(new Error(`${f.file}: owner の状態の file を字句に読めない (${e.message}) = 役割を切り替える文が無いと言えないので流さない`), { code: 'OWNER_MODE_INVALID', version: f.version }); }
  if (hits.length) throw Object.assign(new Error(`${f.file}: owner の状態の file に許さない役割の切り替え・字句の前提の変更がある (${[...new Set(hits)].join('・')}) = 流さない (許すのは SET LOCAL ROLE ${ALLOWED_SET_LOCAL_ROLES.join(' / ')} だけ・GUC ${OWNER_MODE_FORBIDDEN_GUCS.join(' / ')} は変えない)`), { code: 'OWNER_MODE_INVALID', version: f.version });
  const notes = ownerModeUncheckedNotes(f.text);
  if (notes.length) log(`⚠️ ${f.file}: owner の状態の file に字句の検査が中まで見られない形がある (${notes.join('・')}) = 役割の切り替えの字句の検査は補助で sandbox ではない (動的 SQL の中は見ない・記録は印の役割で入る)。役割の管理は migration に書かない`);
}

/**
 * ふつうの migration と owner-transition の file の最上位 (コメント・文字列・ドルの引用の外) の取引の制御の文 (Codex R1 M3)。戻り = 見つけた文の頭 ([] = 無い)。
 * runner は 1 file = 1 取引 (owner-transition は R1 + R2 + 印 + 記録が同じ取引) を保証する = file の中の COMMIT / ROLLBACK で取引を切られると、
 * 後の文と記録が autocommit になりうる。拒む = begin・start (transaction)・commit・end・rollback (to savepoint も)・abort・savepoint・release・
 * prepare transaction (commit / rollback prepared も commit / rollback で拒む)。
 * SQL 標準の関数の本文 `begin atomic … end` の中の文と、その終わりの `end` は数えない
 */
const TX_CONTROL_WORDS = new Set(['begin', 'start', 'commit', 'end', 'rollback', 'abort', 'savepoint', 'release']);
export function txControlStatements(text) {
  const stmts = splitSqlStatements(text);   // 読めない = 例外 (呼び手が止める)
  const hits = [];
  let inAtomic = false;
  const lw = (tk) => (tk && tk.t === 'word' ? tk.v.toLowerCase() : null);
  for (const s of stmts) {
    const w0 = lw(s.toks[0]), w1 = lw(s.toks[1]);
    if (inAtomic) { if (w0 === 'end') inAtomic = false; continue; }
    if (s.toks.some((tk, k) => lw(tk) === 'begin' && lw(s.toks[k + 1]) === 'atomic') && w0 !== 'begin') {
      // begin atomic の本文の最初の文が同じ文 (create function … begin atomic <文 1>) = 後ろの文は end まで本文。end だけで閉じる (本文が空 = begin atomic end も同じ文で閉じる)
      const last = s.toks[s.toks.length - 1];
      if (!(lw(last) === 'end' && lw(s.toks[s.toks.length - 2]) === 'atomic')) inAtomic = true;
      continue;
    }
    if (TX_CONTROL_WORDS.has(w0)) hits.push(w0 === 'start' || w0 === 'rollback' || w0 === 'release' ? `${w0}${w1 ? ` ${w1}` : ''}` : w0);
    else if (w0 === 'prepare' && w1 === 'transaction') hits.push('prepare transaction');
  }
  return hits;
}
function rejectTxControl(f) {
  let hits;
  try { hits = txControlStatements(f.text); } catch (e) { throw Object.assign(new Error(`${f.file}: 字句に読めない (${e.message}) = 取引の制御の文が無いと言えないので流さない`), { code: 'TX_CONTROL_REJECTED', version: f.version }); }
  if (hits.length) throw Object.assign(new Error(`${f.file}: 最上位に取引の制御の文がある (${[...new Set(hits)].join('・')}) = 何も流さずに止めた (runner が 1 file = 1 取引で流す・記録も同じ取引。file の中で begin / commit / rollback / savepoint を書かない)`), { code: 'TX_CONTROL_REJECTED', version: f.version });
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
          'predicate', pg_get_expr(i.indpred, i.indrelid, false),
          -- 🆕 Codex R1 M2・設計 13 v3.13 ⑧: 置き場 (pg_class.reltablespace・0 = DB の既定) と storage の設定 (reloptions・null は空の配列・名前=値 を並べ替え)。
          --   文の側で with / tablespace を拒んでいる = 期待はいつも 0 と [] = 既存の側にも無いことを確かめる
          'reltablespace', c.reltablespace::int8,
          'reloptions', coalesce((select json_agg(o order by o) from unnest(c.reloptions) o), '[]'::json)
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
  // 🆕 Codex R5 M2: 関数と型を pg_catalog. で修飾 (演算子は使わない = search_path に左右されない)
  const { rows } = await db.query("select pg_catalog.current_setting('server_version_num')::pg_catalog.int4 as v");
  return Math.floor(Number(rows[0].v) / 10000);
}

/** 同じ表の index の作りが (別の backend で) 動いていないか。見えない (relid が null = 別の役割) のも「ある」とみなす。🆕 Codex R5 M2: search_path = pg_catalog の中で */
async function buildsInProgress(db, qualifiedTable) {
  return withCatalogPath(db, async () => (await db.query(`
    select p.pid, p.relid::pg_catalog.regclass::pg_catalog.text as rel, p.phase
      from pg_catalog.pg_stat_progress_create_index p
     where p.datid = (select oid from pg_catalog.pg_database where datname = pg_catalog.current_database())
       and p.pid <> pg_catalog.pg_backend_pid()
       and (p.relid is null or $1::pg_catalog.text is null or p.relid = pg_catalog.to_regclass($1::pg_catalog.text))`, [qualifiedTable])).rows);
}

// ─── 空き容量 ───
const mb = (b) => `${Math.round(Number(b) / 1048576).toLocaleString()} MB`;
/**
 * create の 1 文の予想の index の大きさ (bytes)。reltuples が負 (一度も ANALYZE していない)・列の pg_stats が無い = 見積もれない = 例外 (流さない)。
 * 列は key と include の両方 (include も葉に入る = 上に倒す)
 */
export async function estimateIndexBytes(db, st) {
  return withCatalogPath(db, () => estimateIndexBytesInPath(db, st));   // 🆕 Codex R5 M2: 前の file の attacker.pg_stats などに騙されない
}
/** estimateIndexBytes の中身 (呼び手が search_path = pg_catalog の取引を開いている) */
async function estimateIndexBytesInPath(db, st) {
  const fail = (msg) => Object.assign(new Error(`容量: ${st.schema}.${st.name} の大きさを見積もれない (${msg}) = 流さずに止めた`), { code: 'DISK_CHECK_FAILED' });
  const { rows: rel } = await db.query(`select c.reltuples::pg_catalog.float8 as reltuples from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = $1 and c.relname = $2 and c.relkind in ('r', 'p', 'm')`, [st.schema, st.table]);
  if (!rel.length) throw fail(`表 ${st.schema}.${st.table} が無い`);
  const reltuples = Number(rel[0].reltuples);
  if (!(reltuples >= 0)) throw fail(`表 ${st.schema}.${st.table} の reltuples が ${reltuples} = 一度も ANALYZE していない (先に analyze ${st.schema}.${st.table})`);
  const cols = [...new Set(st.elements.filter((e) => !e.expression).map((e) => e.column))];
  const { rows: stats } = cols.length ? await db.query('select attname::pg_catalog.text as attname, avg_width from pg_catalog.pg_stats where schemaname = $1 and tablename = $2 and attname = any($3::pg_catalog.text[])', [st.schema, st.table, cols]) : { rows: [] };
  const width = new Map(stats.map((r) => [r.attname, Number(r.avg_width)]));
  let sum = 0;
  for (const e of st.elements) {
    if (e.expression) { sum += DISK_ESTIMATE.exprWidth; continue; }
    if (!width.has(e.column)) throw fail(`列 ${e.column} の pg_stats が無い (先に analyze ${st.schema}.${st.table})`);
    sum += width.get(e.column);
  }
  return Math.ceil(reltuples * (sum + DISK_ESTIMATE.tupleOverhead) * DISK_ESTIMATE.estimateFactor);
}

/** 容量の読み手を呼んで空きを出す (読めない・値が不正 = 例外)。fail = (msg, extra) → Error */
async function readFreeBytes(readDiskMetrics, fail, extra) {
  let m;
  try { m = typeof readDiskMetrics === 'function' ? await readDiskMetrics() : { ok: false, reason: 'NO_METRICS_READER' }; } catch { m = { ok: false, reason: 'METRICS_INTERNAL' }; }
  if (!m || m.ok !== true) throw fail(`空き容量が読めない (${(m && m.reason) || '不明'}${m && m.detail ? `/${m.detail}` : ''}・Render のメトリクス = RENDER_API_KEY / CDB_RENDER_PG_RESOURCE_ID・接続先と同じ resource)`, { reason: (m && m.reason) || 'UNKNOWN', ...(m && m.detail ? { detail: m.detail } : {}), ...extra });
  const cap = Number(m.capacityBytes), used = Number(m.usedBytes);
  if (!Number.isFinite(cap) || !Number.isFinite(used) || cap <= 0 || used < 0 || used > cap) throw fail('空き容量の値が不正', { reason: 'METRICS_INCONSISTENT', ...extra });
  return { cap, used, free: cap - used };
}

/**
 * create の各文の前の容量の関門。readDiskMetrics() = { ok: true, capacityBytes, usedBytes } | { ok: false, reason } (例外も「読めない」)。
 * 必要 = 予想 × 3 + reservedBytes + 2GB。空きが足りない・読めない = DISK_CHECK_FAILED (流さない)
 * 🆕 Codex R1 H3: reservedBytes = この回 (この runner の実行) で先に作った index の「予想 × 3」の和。Render のメトリクスは最大 2 分古い =
 *   続けて作った index の分がまだ使用に入っていない = 後の文の空きから先に引く (メトリクスに入っていても引く = 二重に数える向き = 止まる向き。
 *   止まったらもう一度流せば作り済みは飛ばされ、予約の和も 0 から = 待てば通る)
 */
export async function checkDiskBeforeCreate(db, st, readDiskMetrics, log = () => {}, { reservedBytes = 0 } = {}) {
  const estimate = await estimateIndexBytes(db, st);
  const need = estimate * DISK_ESTIMATE.safetyFactor + reservedBytes + DISK_ESTIMATE.fixedReserveBytes;
  const fail = (msg, extra = {}) => Object.assign(new Error(`容量: ${st.schema}.${st.name}: ${msg} = 流さずに止めた`), { code: 'DISK_CHECK_FAILED', ...extra });
  const { cap, used, free } = await readFreeBytes(readDiskMetrics, fail, { estimateBytes: estimate });
  const resText = reservedBytes ? ` + この回に先に作った分 ${mb(reservedBytes)}` : '';
  log(`容量: ${st.schema}.${st.name} の予想 ${mb(estimate)} × ${DISK_ESTIMATE.safetyFactor}${resText} + ${mb(DISK_ESTIMATE.fixedReserveBytes)} = ${mb(need)} / 空き ${mb(free)} (容量 ${mb(cap)} − 使用 ${mb(used)})`);
  if (free < need) throw fail(`空き ${mb(free)} が 予想 × ${DISK_ESTIMATE.safetyFactor}${resText} + 2GB = ${mb(need)} に満たない`, { reason: 'NOT_ENOUGH', estimateBytes: estimate, needBytes: need, freeBytes: free, reservedBytes });
  return { estimateBytes: estimate, needBytes: need, freeBytes: free, reservedBytes };
}

/**
 * 🆕 Codex R1 H3: file の最初の文の前に、この file でこれから作る index の全部の「予想 × 3」の和 + この回に先に作った分 + 2GB を 1 回で判定する
 * (途中まで作って容量で止まる = 半端な状態を作らない)。各文の前の関門 (checkDiskBeforeCreate) はそのまま残す (流している間の空きの変化を見る)
 */
export async function checkDiskForFile(db, creates, readDiskMetrics, log = () => {}, { reservedBytes = 0, file = '' } = {}) {
  return checkDiskTotal(db, creates, readDiskMetrics, log, { reservedBytes, label: file });
}
/** creates の予想の和 (1 つの取引・search_path = pg_catalog)。呼び手の session の役割で読む (owner の状態の CIC は SET ROLE の後 = pg_stats の列が見える) */
async function sumIndexEstimates(db, creates) {
  return withCatalogPath(db, async () => { let total = 0; for (const st of creates) total += await estimateIndexBytesInPath(db, st); return total; });
}
/** 合計の判定 (file の合計と run の合計で共通)。必要 = 予想の合計 × 3 + reservedBytes + 2GB */
async function checkDiskTotal(db, creates, readDiskMetrics, log, { reservedBytes = 0, label = '' } = {}) {
  if (!creates.length) return { totalEstimateBytes: 0 };
  const total = await sumIndexEstimates(db, creates);
  const need = total * DISK_ESTIMATE.safetyFactor + reservedBytes + DISK_ESTIMATE.fixedReserveBytes;
  const fail = (msg, extra = {}) => Object.assign(new Error(`容量: ${label} の index ${creates.length} 本の合計: ${msg} = 何も作らずに止めた`), { code: 'DISK_CHECK_FAILED', ...extra });
  const { free } = await readFreeBytes(readDiskMetrics, fail, { estimateBytes: total });
  const resText = reservedBytes ? ` + この回に先に作った分 ${mb(reservedBytes)}` : '';
  log(`容量: ${label} の index ${creates.length} 本の予想の合計 ${mb(total)} × ${DISK_ESTIMATE.safetyFactor}${resText} + ${mb(DISK_ESTIMATE.fixedReserveBytes)} = ${mb(need)} / 空き ${mb(free)}`);
  if (free < need) throw fail(`空き ${mb(free)} が 合計 × ${DISK_ESTIMATE.safetyFactor}${resText} + 2GB = ${mb(need)} に満たない`, { reason: 'NOT_ENOUGH', estimateBytes: total, needBytes: need, freeBytes: free, reservedBytes });
  return { totalEstimateBytes: total, needBytes: need, freeBytes: free };
}
/**
 * 🆕 Codex R5 M1 (設計 13 v3.13 ⑤ (a)・v3.14 ④): この回に流す **全部の未適用の concurrent-index の file** の create を最初に集めて 1 回で判定する
 * (最初の CIC の file の最初の文の前・足りなければ 1 本も作らない)。前の回で作り済み・valid・未記録の index も含める (二重に数えても止まる向き)。
 * 式 = 予想の合計 × 3 + 2GB (設計の「Σ 予想 + max(予想) × 2 + 2GB」より大きいか同じ = 止まる向き・file の合計の判定と同じ式)。
 * file の合計の判定 (checkDiskForFile)・各文の前の判定 (checkDiskBeforeCreate) はそのまま残す (流している間の空きの変化を見る)。
 * CIC の file が 1 つだけの回は、その file の合計の判定と同じ = 呼ばない (メトリクスを二重に読まない)
 */
export async function checkDiskForRun(db, files, readDiskMetrics, log = () => {}) {
  const creates = files.flatMap((f) => [...f.plan.creates.values()]);
  return checkDiskTotal(db, creates, readDiskMetrics, log, { label: `この回の concurrent-index の file ${files.length} 本 (${files.map((f) => f.file).join('・')})` });
}

/**
 * 🆕 Codex R-D60-v3-14 M5: Render の Postgres の host と resource の ID の対応は **まだ Render に確かめていない** (設計 13 §5 の英文の質問 14)。
 * confirmed = false の間は、容量の読み手が照合を通さない = concurrent-index (CIC) の migration は流れない (fail-closed)。
 * 形の fixture = scripts/fixtures/render-postgres-hosts.json (試験が confirmed も同じかを確かめる)。回答で形が決まったら fixture と同じ PR で true にする
 */
export const RENDER_PG_HOST_MAPPING = Object.freeze({ confirmed: false, fixture: 'scripts/fixtures/render-postgres-hosts.json' });
/** Render の Postgres の外部の host = <resource の ID>.<地域>-postgres.render.com / 内部の host = <resource の ID> (pool の host は未確認 = 一致にしない) */
const RENDER_EXTERNAL_PG_HOST_RE = /^([a-z0-9-]+)\.[a-z0-9]+-postgres\.render\.com$/;
/**
 * 🆕 Codex R1 H2: 容量を読む resource (CDB_RENDER_PG_RESOURCE_ID) が、接続先 (COMPANY_DB_URL / --url) の DB と同じかを確かめる。
 * Render の Postgres の host は resource の ID そのもの (内部 = dpg-xxxx-a・外部 = dpg-xxxx-a.<地域>-postgres.render.com) = host の最初の名前が ID と同じときだけ OK。
 * 🚨 Render の API で host を読める道は connection-info (DB の password を返す) だけ = 秘密を持ち込まないために使わない (設計 13 v3.13 ④)。
 *   host の決まりが違えば OK にならない (= 流さない向き)。loopback・IP・ほかの host・ID の後ろに余りがある host は全部「合わない」。
 *   DB の名前は別に API の名札 (GET /v1/postgres/{ID} の databaseName) と current_database() で照合する (下の renderDiskMetricsReader)
 * 戻り = { ok: true } | { ok: false, reason: 'RESOURCE_MISMATCH' }
 */
export function renderResourceMatchesUrl(resourceId, url) {
  try {
    if (typeof resourceId !== 'string' || !/^dpg-[a-z0-9][a-z0-9-]{2,78}$/.test(resourceId) || typeof url !== 'string') return { ok: false, reason: 'RESOURCE_MISMATCH' };
    const host = new URL(url).hostname;
    if (host === resourceId) return { ok: true };
    const m = RENDER_EXTERNAL_PG_HOST_RE.exec(host);
    if (m && m[1] === resourceId) return { ok: true };
    return { ok: false, reason: 'RESOURCE_MISMATCH' };
  } catch { return { ok: false, reason: 'RESOURCE_MISMATCH' }; }
}

/**
 * CLI の容量の読み手 = Render のメトリクス (#1600 の client)。読めなければ { ok: false } (fail-closed)。
 * 🆕 Codex R1 H2・設計 13 v3.13 ④ = resource (CDB_RENDER_PG_RESOURCE_ID) と接続先の照合 (照合できない・合わない = { ok: false, reason: 'RESOURCE_MISMATCH', detail }):
 *   ① 接続先 (url = COMPANY_DB_URL / --url) の host の最初の名前 = resource の ID (要求を送る前)
 *   ② API の名札 (GET /v1/postgres/{ID}) の databaseName = 接続先の current_database() (currentDatabase・接続の後に CLI が読む)。1 回通れば同じ読み手の中では覚える
 */
export async function renderDiskMetricsReader(env = process.env, url = null, { currentDatabase = null, assumeHostMappingConfirmedForTest = false } = {}) {
  const m = await import('../../apps/company-db/profit/render-metrics.mjs');
  let identityOk = false;
  const mismatch = (detail) => ({ ok: false, reason: 'RESOURCE_MISMATCH', detail });
  return async () => {
    const c = m.readRenderMetricsConfig(env);
    if (!c.ok) return { ok: false, reason: c.reason };
    const bind = renderResourceMatchesUrl(c.config.resourceId, url);
    if (!bind.ok) return mismatch('HOST');
    // 🆕 Codex R-D60-v3-14 M5: host の対応を Render に確かめるまで (RENDER_PG_HOST_MAPPING.confirmed = false) は照合の方法を固定できない = 流さない (要求も送らない)
    if (!RENDER_PG_HOST_MAPPING.confirmed && assumeHostMappingConfirmedForTest !== true) return mismatch('HOST_MAPPING_UNCONFIRMED');
    if (!identityOk) {
      if (typeof currentDatabase !== 'string' || !currentDatabase) return mismatch('NO_CURRENT_DATABASE');
      const id = await m.fetchPostgresIdentity({ apiKey: c.config.apiKey, resourceId: c.config.resourceId });
      if (!id.ok) return mismatch(id.reason);
      if (id.databaseName !== currentDatabase) return mismatch('DATABASE_NAME');
      identityOk = true;
    }
    const r = await m.fetchPostgresMetrics({ apiKey: c.config.apiKey, resourceId: c.config.resourceId });
    if (!r.ok) return { ok: false, reason: r.reason };
    if (!m.checkFreshness(r.snapshot).ok) return { ok: false, reason: 'METRICS_STALE' };
    return { ok: true, capacityBytes: r.snapshot.diskCapacityBytes, usedBytes: r.snapshot.diskUsedBytes };
  };
}

// ─── 全体の排他 (CLI の入口) ───
/** lock を持っている backend (見えれば)。接続からの分 = 見張り (45 分) と同じ物差し */
export async function describeLockHolder(db) {
  // 🆕 Codex R5 M2: search_path = pg_catalog の取引で (演算子・型も pg_catalog で解く)
  return withCatalogPath(db, async () => (await db.query(`
    select l.pid, a.application_name, a.usename::pg_catalog.text as usename, a.backend_start,
           pg_catalog.floor(extract(epoch from (pg_catalog.now() - a.backend_start)) / 60)::pg_catalog.int4 as minutes
      from pg_catalog.pg_locks l left join pg_catalog.pg_stat_activity a on a.pid = l.pid
     where l.locktype = 'advisory' and l.granted and l.objsubid = 1
       and l.database = (select oid from pg_catalog.pg_database where datname = pg_catalog.current_database())
       and ((l.classid::pg_catalog.int8 << 32) | l.objid::pg_catalog.int8) = pg_catalog.hashtextextended($1, 0)`, [MIGRATE_LOCK_NAME])).rows);
}
const holderText = (rows) => rows.map((r) => `pid ${r.pid}${r.application_name ? ` (${r.application_name}${r.usename ? `・${r.usename}` : ''})` : ''}${r.minutes != null ? ` 接続から ${r.minutes} 分` : ''}`).join(', ');

/** この session が migrate の lock を持っているか (concurrent-index は lock の中でだけ流す) */
async function holdsMigrateLock(db) {
  // 🆕 Codex R5 M2: search_path = pg_catalog の取引で (前の file が残した search_path の同じ名前の関数・演算子に「持っている」と言わせない)
  return withCatalogPath(db, async () => {
    const { rows } = await db.query(`select exists (select 1 from pg_catalog.pg_locks l where l.locktype = 'advisory' and l.granted and l.objsubid = 1 and l.pid = pg_catalog.pg_backend_pid()
      and l.database = (select oid from pg_catalog.pg_database where datname = pg_catalog.current_database())
      and ((l.classid::pg_catalog.int8 << 32) | l.objid::pg_catalog.int8) = pg_catalog.hashtextextended($1, 0)) as held`, [MIGRATE_LOCK_NAME]);
    return rows[0].held === true;
  });
}

/**
 * fn を migrate の session の lock の中で動かす (Postgres の接続だけ)。取れなければ MIGRATE_LOCKED (待たない)。
 * 失敗の後の順 (設計 v3.9) = ROLLBACK が終わってから unlock。ROLLBACK も失敗したら unlock を呼ばない (e.connectionDead = true・呼び手は接続を捨てる)。
 * 取引の外の失敗 (e.noTransaction = concurrent-index の文) は ROLLBACK をせずに unlock
 */
export async function withMigrateLock(db, fn, { log = () => {} } = {}) {
  // 🆕 Codex R5 M2: 関数を pg_catalog. で修飾 (演算子は使わない・session の lock = 取引に入れない)
  const got = (await db.query('select pg_catalog.pg_try_advisory_lock(pg_catalog.hashtextextended($1, 0)) as got', [MIGRATE_LOCK_NAME])).rows[0].got;
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
  // 🆕 Codex R5 M2: 関数を pg_catalog. で修飾 (前の file が残した search_path の同じ名前の関数に外させない・演算子は使わない)
  const ok = (await db.query('select pg_catalog.pg_advisory_unlock(pg_catalog.hashtextextended($1, 0)) as ok', [MIGRATE_LOCK_NAME])).rows[0].ok;
  if (!ok) throw new Error('pg_advisory_unlock が false (持っていなかった)');
}

/**
 * 🆕 45 分の見張りの接続 (application_name = MIGRATE_LOCK_WATCH_APPLICATION_NAME・同じ DB・自分以外) が無ければ止まる (LOCK_WATCH_REQUIRED・取引の外)。
 * application_name と datname はどの役割からも見える (権限の無い役割からほかの役割の session を見ても = 試験 R1)。見張りを人が忘れる・途中で死んだ の守り (偽の名前を付けた接続までは見分けない)
 */
export async function assertLockWatchPresent(db, label = '') {
  const n = await withCatalogPath(db, async () => (await db.query(`select pg_catalog.count(*)::pg_catalog.int4 as n from pg_catalog.pg_stat_activity a
     where a.application_name = $1 and a.datname = pg_catalog.current_database() and a.pid <> pg_catalog.pg_backend_pid()`, [MIGRATE_LOCK_WATCH_APPLICATION_NAME])).rows[0].n);
  if (!n) {
    throw Object.assign(new Error(`${label ? `${label}: ` : ''}migrate の lock の 45 分の見張り (${MIGRATE_LOCK_WATCH_APPLICATION_NAME}) の接続が無い = concurrent-index を流さない (記録しない)。` +
      'scripts/company-db/migrate-watched.mjs から流す (見張りを起動 → GChat に送れたのを確かめてから migrate)'), { code: 'LOCK_WATCH_REQUIRED', noTransaction: true });
  }
  return n;
}
/** CLI が migrateWithLock に渡す opts (🆕 concurrent-index の各文の前に見張りの接続を確かめる = requireLockWatch。試験も同じ関数を通す) */
export function cliRunOptions(o) {
  return { ...o, requireLockWatch: (db, f) => assertLockWatchPresent(db, f.file) };
}

/** CLI の入口 = lock の中で applyMigrations (流す / dry-run) */
export function migrateWithLock(db, opts = {}) {
  return withMigrateLock(db, () => applyMigrations(db, opts), { log: opts.lockLog || (() => {}) });
}

// ─── 字句の前提 (Codex R3 High 2) ───
/**
 * 🆕 Codex R3 High 2 (PR #1606): file の本文の前に、別のクエリで LEXER_PREMISE を SET LOCAL する
 * (simple query は送った文字列の全部を、本文のどの文を流すよりも前に、受け取った時の設定で字句に分ける = 本文と同じクエリに入れても効かない・前のクエリで入れる)。
 * 前の file (legacy の SET standard_conforming_strings = off・動的 SQL) が session に残した値に左右されない。
 * さらに reset_val も同じかを確かめる (本文の RESET ALL は字句の前提の GUC を reset_val に戻す = ALTER ROLE / DATABASE の既定が off なら
 * 本文の DO の本文 (流す時に字句に分ける) から前提がずれる = 流さない)。
 * 🆕 Codex R4 High 1 (PR #1606): owner の状態の file だけでなく **取引の中で流す全部の file (legacy・owner-transition・owner)** に入れる
 *   (取引の制御の文の検査 txControlStatements も lexSql に頼る = legacy の前の file が off を残すと、owner-transition の本文の `'a\'b'; COMMIT; …` の
 *   COMMIT を見落とし、R1 + R2 + 印の同じ取引を途中で切られた)。0001〜0056 に字句の前提の GUC も最上位の '…' の中の \ も無い (ドルの引用の中の \ は
 *   関数の本文 = 今の本番 (on) と同じ前提) = 今の本番の再実行・--list (どちらも本文を流さない) と次の legacy の file の流れ方は変わらない。
 *   client_encoding の reset_val = node-postgres が起動の時に必ず送る UTF8 (pg-protocol の startup)
 */
async function setLexerPremiseLocal(db) {
  await db.exec(`set local standard_conforming_strings = ${LEXER_PREMISE.standard_conforming_strings}`);
  await db.exec(`set local client_encoding = '${LEXER_PREMISE.client_encoding}'`);
  // 🆕 Codex R5 M2: 呼び手が search_path = pg_catalog に固定した中で呼ぶ (型も修飾)
  const { rows } = await db.query(`select name::pg_catalog.text as name, setting::pg_catalog.text as setting, reset_val::pg_catalog.text as reset_val from pg_catalog.pg_settings where name = any($1::pg_catalog.text[])`, [Object.keys(LEXER_PREMISE)]);
  const bad = [];
  for (const [k, want] of Object.entries(LEXER_PREMISE)) {
    const r = rows.find((x) => x.name === k);
    if (!r) { bad.push(`${k} が読めない`); continue; }
    if (String(r.setting).toLowerCase() !== want.toLowerCase()) bad.push(`${k} = ${r.setting}`);
    if (String(r.reset_val).toLowerCase() !== want.toLowerCase()) bad.push(`${k} の reset_val = ${r.reset_val} (RESET ALL で前提がずれる・ALTER ROLE / DATABASE の既定を見る)`);
  }
  if (bad.length) throw Object.assign(new Error(`LEXER_PREMISE: 字句の前提 (${Object.entries(LEXER_PREMISE).map(([k, v]) => `${k} = ${v}`).join('・')}) にできない: ${bad.join('・')} = 流さない`), { code: 'LEXER_PREMISE_INVALID', reason: 'LEXER_PREMISE' });
}

// ─── concurrent-index のファイルを流す ───
async function setSession(db, s, log) {
  // 🆕 Codex R3 High 2: 字句の前提を先に (concurrent-index の文も lexSql で読んだ境目のまま流す)。finally の resetSession で戻す
  await db.exec(`set standard_conforming_strings = ${LEXER_PREMISE.standard_conforming_strings}`);
  await db.exec(`set client_encoding = '${LEXER_PREMISE.client_encoding}'`);
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
/** session の設定を全部戻す (1 つが落ちても残りを試す)。戻り = 戻せなかった設定の名前 ([] = 全部戻した)。🆕 設計 13 v3.13 ⑩: 戻せなければ呼び手は接続を捨てる */
async function resetSession(db) {
  const bad = [];
  for (const k of ['role', 'lock_timeout', 'statement_timeout', 'client_connection_check_interval', 'standard_conforming_strings', 'client_encoding']) { try { await db.exec(`reset ${k}`); } catch { bad.push(k); } }
  return bad;
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
async function applyConcurrentIndexFile(db, f, plan, opts, log, appliedBy, mode, diskRun = { reservedBytes: 0 }) {
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
  let failedErr = null;
  try {
    // 🆕 Codex R1 Low: session の設定も try の中 = 途中の SET だけ通って次で落ちても finally の resetSession で戻す (接続を使い回す呼び手のため)
    await setSession(db, s, log);
    // owner mode = file の全部 (DDL・記録表・pg_stats の読み) を持ち主の役割で。finally の resetSession で RESET ROLE
    if (mode.mode === 'owner') await db.exec(`set role ${mode.role}`);
    // 🆕 Codex R5 M1: この回の最初の CIC の file の前に、この回に流す全部の CIC の file の create の合計を 1 回で判定 (足りなければ 1 本も作らない)
    if (!diskRun.runChecked) {
      diskRun.runChecked = true;
      const runFiles = diskRun.runFiles || [];
      if (runFiles.length >= 2) {
        try {
          await checkDiskForRun(db, runFiles, opts.readDiskMetrics, log);
        } catch (e) { if (e.code === 'DISK_CHECK_FAILED') { e.message = `${f.file} の前: ${e.message}`; e.version = f.version; } throw e; }
      }
    }
    // 🆕 Codex R1 H3: この file の create の全部の合計を先に 1 回で判定 (+ この回の前の file で作った分)。
    //   🆕 Codex R-D60-v3-14 M4: 作り済みの valid (前の回が作って記録の前に落ちた = メトリクスにまだ出ていないかもしれない) も合計に入れる (二重に数えても止まる向き)
    try {
      await checkDiskForFile(db, [...plan.creates.values()], opts.readDiskMetrics, log, { reservedBytes: diskRun.reservedBytes, file: f.file });
    } catch (e) { if (e.code === 'DISK_CHECK_FAILED') { e.message = `${f.file}: ${e.message}`; e.version = f.version; } throw e; }
    // 🆕 設計 13 v3.14 ④ (b): 各文の前の予約に、この file の作り済みの valid (今回は飛ばす) の予想 × 3 を最初から足しておく
    for (const st of plan.creates.values()) {
      const cur = await readIndexAttrs(db, st.schema, st.name);
      if (cur && !cur.notIndex && cur.valid && cur.ready && cur.live) diskRun.reservedBytes += (await estimateIndexBytes(db, st)) * DISK_ESTIMATE.safetyFactor;
    }
    for (const st of plan.statements) {
      const key = `${st.schema}.${st.name}`;
      // 🆕 45 分の見張りの接続が無ければ、この文から先を流さない (CLI だけが渡す。試験の直の呼び出し・PGlite は渡さない = 今までどおり)
      if (typeof opts.requireLockWatch === 'function') await opts.requireLockWatch(db, f);
      if (st.kind === 'create') {
        const running = await buildsInProgress(db, `${st.schema}.${st.table}`);
        if (running.length) throw fail(`${st.schema}.${st.table} の index の作りが別の接続で動いている (pid ${running.map((r) => r.pid).join(', ')}) = 終わるか止まるのを待つ\n  ${CONCURRENT_INDEX_RUNBOOK}`, { reason: 'BUILD_IN_PROGRESS' });
        const cur = await readIndexAttrs(db, st.schema, st.name);
        if (cur && cur.notIndex) throw fail(`${key} は index でない (relkind ${cur.relkind}) = 名前が取られている`, { reason: 'NAME_TAKEN' });
        if (cur && cur.valid && cur.ready && cur.live) {
          const diff = attrDiff(cur.attrs, plan.expect.indexes[key]);
          if (diff.length) throw fail(`${key} は同じ名前で定義が違う (人が見る・このファイルは流さない)\n  ${diff.join('\n  ')}`, { reason: 'DEFINITION_MISMATCH', diff });
          log(`${key} は作り済み (valid・属性が期待どおり) = 飛ばす`);
          continue;   // 予想 × 3 は上で最初から予約に入れた (R-D60-v3-14 M4)
        }
        try {
          // 表示だけにしない = 足りない・読めないなら例外。この回に先に作った index の分を引く (H3)
          st.diskCheck = await checkDiskBeforeCreate(db, st, opts.readDiskMetrics, log, { reservedBytes: diskRun.reservedBytes });
        } catch (e) { if (e.code === 'DISK_CHECK_FAILED') { e.message = `${f.file}: ${e.message}`; e.version = f.version; } throw e; }
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
      // この回に作った分 = 後の文・後の file の空きから引く (H3)
      if (st.kind === 'create' && st.diskCheck) diskRun.reservedBytes += st.diskCheck.estimateBytes * DISK_ESTIMATE.safetyFactor;
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
    // 🆕 Codex R5 M2: 記録も search_path = pg_catalog の取引で (ふつうの file の記録と同じ・前の file が残した search_path に依らない)
    await withCatalogPath(db, () => db.query('insert into ops.schema_migrations (version, name, checksum, applied_by) values ($1, $2, $3, $4)', [f.version, f.name, f.checksum, appliedBy]));
  } catch (e) {
    const err = ['MIGRATION_FAILED', 'DISK_CHECK_FAILED', 'LOCK_WATCH_REQUIRED'].includes(e.code) ? e : fail(e.message, { cause: e });
    // invalid が残っていれば回収の手順を添える
    try {
      const left = [];
      for (const st of plan.creates.values()) { const cur = await readIndexAttrs(db, st.schema, st.name); if (cur && !cur.notIndex && !cur.valid) left.push(`${st.schema}.${st.name}`); }
      if (left.length) { err.invalidIndexes = left; err.message += `\n  invalid の index が残っている: ${left.join(', ')} = 回収の手順:\n  ${CONCURRENT_INDEX_RUNBOOK}`; }
    } catch { /* 読めなくても元の誤りを出す */ }
    failedErr = noTx(err);
    throw failedErr;
  } finally {
    // 🆕 設計 13 v3.13 ⑩: 入れた設定を全部戻す。戻せない = 接続を使い回さない (connectionDead = 呼び手 (CLI) が接続を捨てる)
    const bad = await resetSession(db);
    if (bad.length) {
      if (failedErr) { failedErr.connectionDead = true; failedErr.resetFailed = bad; failedErr.message += `\n  session の設定 (${bad.join('・')}) も戻せない = この接続は捨てる`; }
      else throw noTx(Object.assign(new Error(`${f.file}: 記録は済んだが session の設定 (${bad.join('・')}) を戻せない = この接続は使い回さない (捨てる)`), { code: 'SESSION_RESET_FAILED', version: f.version, connectionDead: true, resetFailed: bad }));
    }
  }
}

/** concurrent-index に対応しない adapter (PGlite) = 同じ文から concurrently を外し、ふつうの取引で流す。属性の検証は同じ */
async function applyConcurrentIndexFileInTx(db, f, plan, log, appliedBy, lockTimeout, statementTimeout, mode) {
  log(`apply ${f.file} (concurrent-index を取引の中で・concurrently を外す = この adapter は CIC に対応しない) ...`);
  await db.exec('begin');
  try {
    if (mode.mode === 'owner') await db.exec(`set local role ${mode.role}`);
    await db.exec(`set local lock_timeout = '${lockTimeout}'; set local statement_timeout = '${statementTimeout}';`);
    // 🆕 Codex R5 M2: runner の SQL は pg_catalog で解く (drop は schema つき)。
    // 🆕 Codex R6 Low: CIC の文は本物の PG と同じく **元の session の search_path** で流す (前の file が残した search_path と同じ名前の関数を使う
    //   index の式が、PGlite だけ通って本物の PG で属性が違って止まる・または逆、を作らない)。固定する前の値を覚え、各文の直前に set_config(…, true) で戻し、
    //   文の後にまた固定する (readIndexAttrs の inTx は固定を取引の終わりまで残す = 文の前に毎回戻す)
    const sessionPath = (await db.query("select pg_catalog.current_setting('search_path') as p")).rows[0].p;
    await db.exec(`set local search_path = ${CATALOG_SEARCH_PATH}`);
    await setLexerPremiseLocal(db);   // 🆕 Codex R3 High 2: PGlite の道も同じ前提 (concurrent-index の file は新しい形 = legacy の互換は関係ない)
    for (const st of plan.statements) {
      if (st.kind === 'create') {
        const cur = await readIndexAttrs(db, st.schema, st.name, { inTx: true });
        if (cur && cur.notIndex) throw new Error(`${st.schema}.${st.name} は index でない (relkind ${cur.relkind}) = 名前が取られている`);
      }
      await db.query("select pg_catalog.set_config('search_path', $1, true)", [sessionPath]);
      await db.exec(st.sqlTx);
      await db.exec(`set local search_path = ${CATALOG_SEARCH_PATH}`);
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
 * 🆕 設計 13 v3.13 ② / v3.14 ② `ROLE_ADMIN_REACHABLE` (Codex R-D60-v3-14 M2 = 禁止の集合を 1 つに固定・正本はこの定数と下の属性):
 * 接続の役割 (session_user) が MEMBER で届く全部の役割 (自身を含む・INHERIT / SET の options に依らない) のうち、
 * superuser・CREATEROLE・CREATEDB・REPLICATION・BYPASSRLS・ADMIN の membership の行を持つもの / 危険な定義済みの役割 (ROLE_ADMIN_FORBIDDEN_PREDEFINED)。
 * owner の状態の migration の本文は、動的 SQL で session_user が届く役割の権限を使える = 届くなら「migration から他の役割を管理できない」は成り立たない。
 * 🚨 **owner の状態 (印がある) では必須 = 届けば流さない** (設計 13 v3.14 = 形 A は採らない・警告だけで流す道を owner の状態に残さない)。
 *   legacy (PR 1b の前・印が無い) は対象の外 (今の本番 = 接続の役割は役割の管理に届く)。
 *   例外 = adapter が roleAdminReachExempt (PGlite = 試験だけの superuser の 1 つの session・本番の道 (pg の Client) は持たない) なら log を出して飛ばす
 */
export const ROLE_ADMIN_FORBIDDEN_PREDEFINED = Object.freeze(['pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program', 'pg_create_subscription', 'pg_checkpoint', 'pg_signal_backend']);
export async function roleAdminReachable(db) {
  // 🆕 Codex R5 M2: search_path = pg_catalog の取引で (前の file が残した search_path の同じ名前の関数・演算子に「届かない」と言わせない)
  const rows = await withCatalogPath(db, async () => (await db.query(`select r.rolname::pg_catalog.text as role, r.rolsuper as su, r.rolcreaterole as cr, r.rolcreatedb as cdb, r.rolreplication as rep, r.rolbypassrls as bypass,
      exists (select 1 from pg_catalog.pg_auth_members m where m.member = r.oid and m.admin_option) as adm,
      r.rolname = any($1::pg_catalog.text[]) as predefined
    from pg_catalog.pg_roles r
   where pg_catalog.pg_has_role(session_user, r.oid, 'MEMBER')
   order by 1`, [ROLE_ADMIN_FORBIDDEN_PREDEFINED])).rows);
  return rows.filter((r) => r.su || r.cr || r.cdb || r.rep || r.bypass || r.adm || r.predefined)
    .map((r) => `${r.role} (${[r.su && 'superuser', r.cr && 'CREATEROLE', r.cdb && 'CREATEDB', r.rep && 'REPLICATION', r.bypass && 'BYPASSRLS', r.adm && 'ADMIN の membership', r.predefined && '危険な定義済みの役割'].filter(Boolean).join('・')})`);
}
/**
 * 🆕 Codex R5 M3 (PR #1606) `OWNER_ROLE_CAN_LOGIN`: 形 B の保証の範囲 = owner の状態の本文は runner の `SET LOCAL ROLE <印の役割>` (と動的 SQL の
 * set_config('role', …)) で接続の役割 (session_user) が SET で届く役割になり、その役割として **自分の password を変えられる** (PostgreSQL は普通の役割にも
 * 自分の password の変更を許す・password は役割の図の比較に見えない = pg_authid は superuser だけ)。
 * → SET で届く owner の役割が NOLOGIN なら、変えられた password は資格として使えない。ここではそれを fail-closed で確かめる:
 *   対象 = 印の役割 + 許す SET LOCAL ROLE の一覧 (ALLOWED_SET_LOCAL_ROLES・在れば) + session_user が SET で届く全部の役割 (session_user 自身は除く =
 *   deployer 自身の password は形 B でも残る穴・設計 13 v3.14 ③)。1 つでも LOGIN (rolcanlogin) なら止まる。
 * 戻り = LOGIN の役割の名前の一覧 ([] = 全部 NOLOGIN)
 */
export async function ownerRolesCanLogin(db, markRole) {
  const rows = await withCatalogPath(db, async () => (await db.query(`select r.rolname::pg_catalog.text as role
    from pg_catalog.pg_roles r
   where r.rolcanlogin
     and (r.rolname = $1::pg_catalog.text
          or r.rolname = any($2::pg_catalog.text[])
          or (r.rolname <> session_user and pg_catalog.pg_has_role(session_user, r.oid, 'SET')))
   order by 1`, [markRole, ALLOWED_SET_LOCAL_ROLES])).rows);
  return rows.map((r) => r.role);
}
/** owner の状態のときに呼ぶ。届けば OWNER_MODE_INVALID (reason ROLE_ADMIN_REACHABLE)・🆕 SET で届く owner の役割が LOGIN なら OWNER_MODE_INVALID (reason OWNER_ROLE_CAN_LOGIN) */
async function checkRoleAdminReach(db, log = () => {}, markRole = null) {
  if (db.roleAdminReachExempt === true) { log('到達の検査 (ROLE_ADMIN_REACHABLE・OWNER_ROLE_CAN_LOGIN) を飛ばす = この adapter は試験だけ (PGlite の superuser の 1 つの session)'); return; }
  const hits = await roleAdminReachable(db);
  if (hits.length) throw Object.assign(ownerErr(`ROLE_ADMIN_REACHABLE: 接続の役割 (session_user) が役割の管理・危険な権限に届く: ${hits.join(', ')} = owner の状態の migration の本文は動的 SQL でこの権限を使える (sandbox ではない) = 流さない (形 B = 役割の管理は別の LOGIN・設計 13 v3.14 ②)`), { reason: 'ROLE_ADMIN_REACHABLE' });
  const login = await ownerRolesCanLogin(db, markRole);
  if (login.length) throw Object.assign(ownerErr(`OWNER_ROLE_CAN_LOGIN: SET で届く owner の役割 (印の役割・許す SET LOCAL ROLE の一覧・接続の役割が SET で届く役割) に LOGIN がある: ${login.join(', ')} = owner の状態の本文はその役割として自分の password を見えずに変えられる (役割の図の比較に出ない) = 資格として使える役割には SET させない (ALTER ROLE … NOLOGIN にしてから流す・Codex R5 M3)`), { reason: 'OWNER_ROLE_CAN_LOGIN', loginRoles: login });
}

/**
 * 🆕 設計 13 v3.13 ③ `ROLE_GRAPH_CHANGED` = 役割の図の hash (Codex R-D60-v3-14 L1 = jsonb_build_object と順の決まった jsonb_agg・SHA-256 = 区切りの文字の結合はしない)。
 * 中身 = pg_auth_members の全部の行 (roleid・member・grantor・admin / inherit / set の options) / pg_roles の属性 (connlimit・validuntil・config を含む) / pg_db_role_setting。
 * owner の状態のふつうの file の取引で、本文の前と「記録の INSERT → SET CONSTRAINTS ALL IMMEDIATE」の後を比べる (R-D60-v3-14 M1)。
 * 🚨 password の変更は見えない (pg_authid は superuser だけ) = これだけでは役割の境を守れない
 */
export async function roleGraphHash(db) {
  // 🆕 Codex R5 M2: 関数・aggregate・型を全部 pg_catalog. で修飾 (jsonb_agg・jsonb_build_object・to_jsonb・jsonb・int8・text)。演算子は使わない
  //   (order by は型の既定の btree の演算子のクラス = search_path に依らない)。呼び手は search_path = pg_catalog, pg_temp の中で呼ぶ (二重の守り)
  const { rows } = await db.query(`select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object(
      'members', (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('roleid', m.roleid::pg_catalog.int8, 'member', m.member::pg_catalog.int8, 'grantor', m.grantor::pg_catalog.int8,
          'admin', m.admin_option, 'inherit', m.inherit_option, 'set', m.set_option) order by m.roleid, m.member, m.grantor), '[]'::pg_catalog.jsonb) from pg_catalog.pg_auth_members m),
      'roles', (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('oid', r.oid::pg_catalog.int8, 'name', r.rolname::pg_catalog.text, 'super', r.rolsuper, 'inherit', r.rolinherit, 'createrole', r.rolcreaterole,
          'createdb', r.rolcreatedb, 'login', r.rolcanlogin, 'replication', r.rolreplication, 'bypassrls', r.rolbypassrls, 'connlimit', r.rolconnlimit,
          'validuntil', r.rolvaliduntil, 'config', pg_catalog.to_jsonb(r.rolconfig)) order by r.oid), '[]'::pg_catalog.jsonb) from pg_catalog.pg_roles r),
      'settings', (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('database', s.setdatabase::pg_catalog.int8, 'role', s.setrole::pg_catalog.int8, 'config', pg_catalog.to_jsonb(s.setconfig)) order by s.setdatabase, s.setrole), '[]'::pg_catalog.jsonb)
          from pg_catalog.pg_db_role_setting s)
    )::pg_catalog.text, 'UTF8')), 'hex') as h`);
  return rows[0].h;
}

/** 記録表を持ち主の mode の役割で読む。legacy なのに読めない (42501) = 持ち主を移した後に印が消えた可能性 = OWNER_MODE_INVALID (Codex R-D60-v3-11 M-new-2) */
async function readAppliedRows(db, mode, sql) {
  try {
    return await inModeTx(db, mode, async () => (await db.query(sql)).rows);
  } catch (e) {
    if (mode.mode === 'legacy' && e.code === '42501') throw ownerErr(`印 ${OWNER_MARKER_TABLE} が無いのに記録表 ops.schema_migrations を接続の役割で読めない (${e.message}) = 持ち主を移した後に印が消えた可能性 = 流さない`);
    throw e;
  }
}

/**
 * 🚨 持ち主の印と owner-transition の適用を両方向で確かめる (Codex R-D60-v3-11 M-new-2: 印が消えて legacy に戻る fail-open を塞ぐ)。
 *   ① owner-transition の migration が適用済み → 印が要る / ② 印がある → owner-transition の migration (file も) が適用済み
 * 流す道 (applyMigrations・--dry-run) と 🆕 --list (migrationStatus・Codex R1 M1) の両方が通る。applied = Map<version, …>
 */
function assertOwnerTransitionConsistent(mode, files, applied) {
  const transFiles = files.filter((f) => f.ownerTransition);
  if (transFiles.length > 1) throw ownerErr(`owner-transition の migration が 2 つある (${transFiles.map((f) => f.file).join(', ')}) = 持ち主の移しは 1 つの file (1 つの取引) だけ`);
  const transFile = transFiles[0] || null;
  const transApplied = !!(transFile && applied.has(transFile.version));
  if (mode.mode === 'legacy' && transApplied) throw ownerErr(`owner-transition の ${transFile.file} は適用済みなのに印 ${OWNER_MARKER_TABLE} が無い = 印が消えた可能性 = legacy に戻して流さない (印を戻すまで止まる)`);
  if (mode.mode === 'owner' && !transApplied) throw ownerErr(`印 ${OWNER_MARKER_TABLE} があるのに owner-transition の migration が${transFile ? `適用されていない (${transFile.file})` : ' file に無い'} = 印だけが作られた可能性 = 流さない`);
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
    // 🆕 Codex R5 M2: 型と既定の関数 (text・timestamptz・now()) を pg_catalog で解く (role / database の既定の search_path に依らない)
    await withCatalogPath(db, () => db.exec(`
      create schema if not exists ops;
      create table if not exists ops.schema_migrations (
        version    text primary key,
        name       text not null,
        checksum   text not null,
        applied_at timestamptz not null default now(),
        applied_by text
      );
    `));
  }
  // dry-run は DDL を流さない = 記録表が無ければ「何も適用していない」とみる
  let appliedRows = [];
  if (migExists || !dryRun) appliedRows = await readAppliedRows(db, mode0, 'select version, checksum from ops.schema_migrations order by version');
  const applied = new Map(appliedRows.map((r) => [r.version, r.checksum]));

  const files = listMigrationFiles(dir);
  assertOwnerTransitionConsistent(mode0, files, applied);
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
    else { rejectUnmarkedConcurrently(f); rejectTxControl(f); }   // 🆕 Codex R1 M3: ふつうの file と owner-transition の file の最上位の取引の制御を拒む
    todo.push(f);
  }
  // owner mode で流す file (今が owner・または前に owner-transition がある) は役割を切り替える文を禁止 (設計 13 v3.10)
  let ownerFromHere = mode0.mode === 'owner';
  for (const f of todo) {
    if (f.ownerTransition) { warnLexerGucOutsideOwner(f, log); ownerFromHere = true; continue; }   // 🆕 Codex R4 High 1: ⚠️ だけ (本文の前に前提を入れ直す)
    if (f.concurrentIndex) continue;
    if (ownerFromHere) rejectRoleSwitchInOwnerMode(f, log);   // 🆕 Codex R2 High: U&"…"・U&'…'・UESCAPE もここで拒む
    else { warnUnicodeEscapeInLegacy(f, log); warnLexerGucOutsideOwner(f, log); }   // legacy = ⚠️ だけ (今までどおり流す・0001〜0056 には U& も字句の前提の GUC も無い)
  }
  const transitions = todo.filter((f) => f.ownerTransition);
  if (transitions.length && mode0.mode === 'owner') throw Object.assign(new Error(`${transitions[0].file} は owner-transition の migration なのに、もう owner mode (${mode0.role}) = 流さない (持ち主の移しは 1 回だけ)`), { code: 'OWNER_MODE_INVALID', version: transitions[0].version });
  if (transitions.length > 1) throw Object.assign(new Error(`owner-transition の migration が 2 つある (${transitions.map((f) => f.file).join(', ')})`), { code: 'OWNER_MODE_INVALID' });
  // 🆕 設計 13 v3.14 ②: owner の状態なら、接続の役割が禁止の集合に届けば流さない (dry-run も・何も流す前)
  let reachChecked = false;
  if (mode0.mode === 'owner') { await checkRoleAdminReach(db, log, mode0.role); reachChecked = true; }   // 🆕 Codex R5 M3: SET で届く owner の役割の NOLOGIN も
  // 🆕 Codex R5 Low 2: dry-run でも環境の字句の前提 (setting と reset_val) を確かめる (流す道と同じ関数・取引を開いて巻き戻す = 何も変えない)。
  //   流す道で前提を入れる file (取引の中で流す file) があるときだけ = 流す道と同じ範囲。違えば code = LEXER_PREMISE_INVALID (reason = LEXER_PREMISE)
  if (dryRun && todo.some((f) => !f.concurrentIndex || db.supportsConcurrentIndex !== true)) {
    await db.exec('begin');
    try {
      await db.exec(`set local search_path = ${CATALOG_SEARCH_PATH}`);
      await setLexerPremiseLocal(db);
    } catch (e) {
      try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する = 元の誤りを出す */ }
      throw e;
    }
    await db.exec('rollback');   // SET LOCAL を残さない (dry-run は何も変えない)
    log(`dry-run: 字句の前提 (${Object.entries(LEXER_PREMISE).map(([k, v]) => `${k} = ${v}`).join('・')}・reset_val も) を確かめた`);
  }
  // 2 周目 = 流す
  // この回に作った index の「予想 × 3」の和 (Codex R1 H3・後の文と後の file の空きから引く)。🆕 Codex R5 M1: runFiles = この回に流す全部の CIC の file (最初の CIC の file の前に合計で 1 回判定)
  const diskRun = { reservedBytes: 0, runChecked: false, runFiles: todo.filter((f) => f.concurrentIndex) };
  for (const f of todo) {
    if (dryRun) {
      log(f.concurrentIndex ? `dry-run: ${f.file} を流す予定 (concurrent-index・${f.plan.statements.length} 文・${db.supportsConcurrentIndex ? '取引の外' : 'concurrently を外して取引の中'})` : `dry-run: ${f.file} を流す予定${f.ownerTransition ? ' (owner-transition = 接続の役割で流し、記録は印の役割で)' : ''}`);
      result.pending.push(f.version);
      continue;
    }
    // file ごとに mode を読み直す (前の file が owner-transition なら、ここから owner mode)
    const mode = await readOwnerMode(db);
    if (f.ownerTransition && mode.mode === 'owner') throw Object.assign(new Error(`${f.file} は owner-transition なのに、もう owner mode = 流さない`), { code: 'OWNER_MODE_INVALID', version: f.version });
    if (mode.mode === 'owner' && !reachChecked) { await checkRoleAdminReach(db, log, mode.role); reachChecked = true; }   // 同じ回で owner-transition の後
    if (f.concurrentIndex) {
      if (db.supportsConcurrentIndex === true) await applyConcurrentIndexFile(db, f, f.plan, opts, log, appliedBy, mode, diskRun);
      else await applyConcurrentIndexFileInTx(db, f, f.plan, log, appliedBy, lockTimeout, statementTimeout, mode);
      result.applied.push(f.version);
      continue;
    }
    log(`apply ${f.file}${f.ownerTransition ? ' (owner-transition)' : ''} ...`);
    await db.exec('begin');
    try {
      if (mode.mode === 'owner') await db.exec(`set local role ${mode.role}`);   // 取引の終わり (commit / rollback) で戻る = 例外に左右されない
      await db.exec(`set local lock_timeout = '${lockTimeout}'; set local statement_timeout = '${statementTimeout}';`);
      // 🆕 Codex R3 High 2 / R4 High 1: 本文の前に別のクエリで字句の前提 (standard_conforming_strings = on・client_encoding = UTF8) を入れる。
      //   owner の状態の file だけでなく legacy・owner-transition の file も (取引の制御の文の検査が lexSql と同じ境目で Postgres に読まれるように)
      // 🆕 Codex R5 M2: 本文の前の runner の SQL (字句の前提の確かめ・最初の役割の図の hash) も search_path = pg_catalog, pg_temp の中で
      //   (前の file が session に `SET search_path = attacker, pg_catalog` と同じ名前の jsonb_agg などを残しても、最初の hash の解決先を変えさせない)。
      //   固定は本文の前に元の値へ戻す (withCatalogPath の inTx = set_config(…, true)) = 本文の名前の解決は今までどおり
      const graph0 = await withCatalogPath(db, async () => {
        await setLexerPremiseLocal(db);
        // 🆕 設計 13 v3.13 ③ / v3.14 ①: owner の状態のふつうの file と owner-transition の file = 本文の前の役割の図 (役割を作る・membership を変えるのは migration の外)
        return mode.mode === 'owner' || f.ownerTransition ? roleGraphHash(db) : null;
      }, { inTx: true });
      await db.exec(f.text);
      // 本文が search_path を変えていても、runner の後の SQL (mode の読み・記録・役割の図の hash) は pg_catalog の名前で解く (本文が作った同じ名前の関数・型に解かせない)
      await db.exec(`set local search_path = ${CATALOG_SEARCH_PATH}`);
      if (mode.mode === 'legacy') {
        // 印を作ってよいのは owner-transition の file だけ / owner-transition の file は印を作り終えていなければならない
        const after = await readOwnerMode(db, { inTx: true });
        if (f.ownerTransition) {
          if (after.mode !== 'owner') throw Object.assign(new Error(`owner-transition の file が持ち主の印 (${OWNER_MARKER_TABLE}) を作らなかった`), { code: 'OWNER_MODE_INVALID' });
          await db.exec(`set local role ${after.role}`);   // 記録は移した後の持ち主で (接続の役割は記録表の権限を失っているかもしれない)
          log(`持ち主の mode = ${modeText(after)} に移った`);
        } else if (after.mode === 'owner') {
          throw Object.assign(new Error(`持ち主の印 (${OWNER_MARKER_TABLE}) を作ってよいのは 1 行目が「${OWNER_TRANSITION_MARKER}」の file だけ`), { code: 'OWNER_MODE_INVALID' });
        }
      } else {
        // 記録の INSERT の直前にもう一度 (本文が役割を変えていても、記録は印の役割で = 設計 13 v3.10)。🚨 守るのは記録の行だけ (本文の副作用は守らない)
        await db.exec(`set local role ${mode.role}`);
      }
      await db.query('insert into ops.schema_migrations (version, name, checksum, applied_by) values ($1, $2, $3, $4)',
        [f.version, f.name, f.checksum, appliedBy]);
      if (graph0 != null) {
        // 🆕 設計 13 v3.13 ③ ROLE_GRAPH_CHANGED (Codex R-D60-v3-14 M1 の順) = 記録の INSERT の後に deferred の制約の trigger を走らせ (SET CONSTRAINTS ALL IMMEDIATE)、
        //   それから役割の図を比べる (記録の INSERT が起こす trigger も含める)。違えば全部巻き戻す (役割の図を変える migration は無い = 例外なし)
        await db.exec('set constraints all immediate');
        if ((await roleGraphHash(db)) !== graph0) throw Object.assign(ownerErr(`ROLE_GRAPH_CHANGED: ${f.file} の本文 (か本文が作った trigger) が役割の図 (membership・役割の属性・役割の設定) を変えた = 全部巻き戻す (役割の管理は migration に書かない)`), { reason: 'ROLE_GRAPH_CHANGED' });
      }
      await db.exec('commit');
    } catch (e) {
      try { await db.exec('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
      const hint = /lock timeout|canceling statement due to lock timeout/i.test(e.message) ? ` [lock_timeout ${lockTimeout}: 別の接続が表を使っている。少し待ってもう一度流す]` : '';
      throw Object.assign(new Error(`${f.file} で失敗 (このファイルは巻き戻した。前のファイルまでは適用済み): ${e.message}${hint}`), { code: 'MIGRATION_FAILED', version: f.version, reason: e.reason || (e.code === 'OWNER_MODE_INVALID' ? 'OWNER_MODE_INVALID' : undefined), cause: e });
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
  // 持ち主の mode は記録表の有無に依らず読む (catalog だけ・印が壊れていれば OWNER_MODE_INVALID)
  const mode = await readOwnerMode(db);
  // 記録表が無い = 全部 pending。あれば持ち主の mode の役割で読む (読めない誤りは握りつぶさない = 全部 pending と見せない)
  if (await opsTableExists(db, 'schema_migrations')) {
    const rows = await readAppliedRows(db, mode, 'select version, checksum, applied_at, applied_by from ops.schema_migrations');
    applied = new Map(rows.map((r) => [r.version, r]));
  }
  const status = statusRows(files, applied);
  // 🆕 Codex R1 M1: --list も印と owner-transition の適用を両方向で確かめる (配布の確かめで「全部 applied・legacy」と誤って見せない)。
  //   止めるときも一覧は e.statusRows に付ける (CLI は一覧を出してから exit 1)
  //   🆕 設計 13 v3.14 ②: owner の状態なら到達の検査も (届けば OWNER_MODE_INVALID = exit 1)
  try {
    assertOwnerTransitionConsistent(mode, files, applied);
    if (mode.mode === 'owner') await checkRoleAdminReach(db, opts.log || (() => {}), mode.role);
  } catch (e) { e.statusRows = status; throw e; }
  return status;
}
function statusRows(files, applied) {
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
  const { creates } = groupIndexStatements(stmts.map((s) => parseConcurrentIndexStatement(s, f.file)), f.file);
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
    roleAdminReachExempt: true,   // 🆕 試験だけの superuser の 1 つの session = 到達の検査 (ROLE_ADMIN_REACHABLE) は飛ばす (本番の道 = pgAdapter は持たない)
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
      let rows, ownerError = null;
      try { rows = await migrationStatus(db, { dir }); } catch (e) {
        if (e.code !== 'OWNER_MODE_INVALID') throw e;
        ownerError = e; rows = e.statusRows || [];   // 印と記録が食い違う = 一覧 (読めた分) と lock の持ち主は出し、最後に exit 1 (Codex R1 M1)
      }
      for (const s of rows) console.log(`${s.version} ${s.state.padEnd(8)} ${s.name}${s.concurrentIndex ? ' [concurrent-index]' : ''}${s.applied_at ? `  (${new Date(s.applied_at).toISOString()} by ${s.applied_by})` : ''}`);
      try {
        const m = await readOwnerMode(db);
        console.log(`持ち主の mode = ${ownerError ? '不正 (下の FAILED)' : modeText(m)}`);
      } catch (e) { console.log(`持ち主の mode が読めない: ${e.message}`); }
      const h = await describeLockHolder(db).catch(() => []);
      console.log(h.length ? `migrate の lock (${MIGRATE_LOCK_NAME}) を持っている: ${holderText(h)}${h.some((r) => r.minutes >= MIGRATE_LOCK_ALERT_MINUTES) ? ` ⚠️ ${MIGRATE_LOCK_ALERT_MINUTES} 分を超えている` : ''}` : `migrate の lock (${MIGRATE_LOCK_NAME}) を持っている接続は無い`);
      if (ownerError) { console.error(`[company-db] FAILED (OWNER_MODE_INVALID): ${ownerError.message}`); return 1; }
      return 0;
    }
    if (expectOf) {
      const f = listMigrationFiles(dir).find((x) => x.version === expectOf);
      if (!f) { console.error(`${expectOf} のファイルが無い`); return 2; }
      console.log(JSON.stringify(await buildIndexExpect(db, f), null, 2));
      return 0;
    }
    const r = await migrateWithLock(db, cliRunOptions({ dir, to, dryRun: args.includes('--dry-run'), readDiskMetrics: await renderDiskMetricsReader(process.env, url, { currentDatabase: (await client.query('select pg_catalog.current_database()::pg_catalog.text as d')).rows[0].d }), lockLog: (m) => console.log(`[company-db] ${m}`) }));
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
