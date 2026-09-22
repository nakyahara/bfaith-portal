/**
 * create-watch-roles.mjs — 見張り用の 2 つのロールを Company DB に作る (設計 09 §6。1 回だけ・中原さんが miniPC で流す)
 *
 *   watcher       照会用: 対象 schema の select だけ。statement_timeout 10s・default_transaction_read_only (保険であって権限の境界ではない)
 *   watch_writer  記録用: ops.watch_* の insert + 限定 update (watch_runs の終了情報・watch_issues の管理列) + sequence の usage
 *
 * 🚨 Render の「新しい credential」は使わない (default user を差し替える)。ここで SQL で作るロールは Render の管理外
 *    = 資格情報の更新・DB の復元 / 移設のときは、もう一度これを流して .env を更新する (README「AI が見張る」)。
 * 🚨 security definer の関数は public の execute を外す (view から呼べるため)。必要なアプリのロール = migration を流す default user (= このスクリプトを流す current_user) には残る。
 *
 * 使い方:
 *   node -r dotenv/config scripts/company-db/create-watch-roles.mjs            # 作る (無ければ作り・あれば権限だけそろえる。パスワードは新しく発行して表示)
 *   node -r dotenv/config scripts/company-db/create-watch-roles.mjs --dry-run  # 流す SQL を表示するだけ (パスワードは出さない)
 *   node -r dotenv/config scripts/company-db/create-watch-roles.mjs --verify   # .env の COMPANY_DB_WATCH_URL / _WRITER_URL で接続し、読める・書けない を確かめる
 *
 * 表示された 2 行 (COMPANY_DB_WATCH_URL= / COMPANY_DB_WATCH_WRITER_URL=) を miniPC の .env に足す。パスワードはこの画面にしか出ない。
 */
import crypto from 'node:crypto';
import { openPgClient } from './migrate.mjs';

export const WATCH_SCHEMAS = ['core', 'snapshots', 'events', 'ops', 'mart'];
export const WATCH_TABLES = ['watch_runs', 'watch_results', 'watch_issues', 'watch_result_items'];
export const RUNS_UPDATE_COLS = ['finished_at', 'completed_keys', 'summary', 'last_line'];
export const ISSUES_UPDATE_COLS = ['state', 'severity', 'last_seen_at', 'days_seen', 'recovered_at', 'transitions', 'last_result_id', 'summary', 'updated_at'];

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };
const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;
const newPassword = () => crypto.randomBytes(24).toString('base64url');

/** 流す SQL の一覧 (パスワードは引数)。dbName / owner = 実際の DB 名・migration を流すユーザー (= current_user) */
export function roleStatements({ dbName, owner, watcherPw, writerPw, secdefFunctions = [] }) {
  const db = ident(dbName), o = ident(owner);
  const s = [];
  // watcher
  s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = 'watcher') then create role watcher; end if; end $$`);
  s.push(`alter role watcher with login password ${lit(watcherPw)} nosuperuser nocreatedb nocreaterole noinherit connection limit 3`);
  s.push(`alter role watcher set statement_timeout = '10s'`);
  s.push(`alter role watcher set default_transaction_read_only = on`);
  s.push(`grant connect on database ${db} to watcher`);
  for (const sc of WATCH_SCHEMAS) {
    s.push(`grant usage on schema ${sc} to watcher`);
    s.push(`grant select on all tables in schema ${sc} to watcher`);
    s.push(`alter default privileges for role ${o} in schema ${sc} grant select on tables to watcher`);   // 将来の表にも (schema を限定)
  }
  // security definer の関数: public の execute を外す (owner には残る)。将来の関数は作るときに個別に
  for (const f of secdefFunctions) { s.push(`revoke execute on function ${f} from public`); s.push(`grant execute on function ${f} to ${o}`); }
  // watch_writer
  s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = 'watch_writer') then create role watch_writer; end if; end $$`);
  s.push(`alter role watch_writer with login password ${lit(writerPw)} nosuperuser nocreatedb nocreaterole noinherit connection limit 2`);
  s.push(`alter role watch_writer set statement_timeout = '30s'`);
  s.push(`grant connect on database ${db} to watch_writer`);
  s.push(`grant usage on schema ops to watch_writer`);
  for (const t of WATCH_TABLES) s.push(`grant select, insert on ops.${t} to watch_writer`);
  s.push(`grant update (${RUNS_UPDATE_COLS.join(', ')}) on ops.watch_runs to watch_writer`);
  s.push(`grant update (${ISSUES_UPDATE_COLS.join(', ')}) on ops.watch_issues to watch_writer`);
  s.push(`grant usage on all sequences in schema ops to watch_writer`);
  s.push(`alter default privileges for role ${o} in schema ops grant usage on sequences to watch_writer`);
  return s;
}

/** COMPANY_DB_URL のユーザーとパスワードを差し替えた接続文字列 */
export function urlFor(baseUrl, user, password) {
  const u = new URL(baseUrl);
  u.username = user; u.password = password;
  return u.toString();
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run'), verify = args.includes('--verify');
  const base = process.env.COMPANY_DB_URL;
  if (!base) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  if (verify) {
    for (const [name, url, expectWrite] of [['COMPANY_DB_WATCH_URL', process.env.COMPANY_DB_WATCH_URL, false], ['COMPANY_DB_WATCH_WRITER_URL', process.env.COMPANY_DB_WATCH_WRITER_URL, true]]) {
      if (!url) { console.log(`${name}: 未設定`); continue; }
      const c = await openPgClient(url);
      try {
        const who = (await c.query(`select current_user as u, current_setting('statement_timeout') as st`)).rows[0];
        const n = (await c.query(`select count(*)::int as n from ops.watch_runs`)).rows[0].n;
        let core = 'read ok'; try { await c.query(`select 1 from core.orders limit 1`); } catch (e) { core = `read denied (${e.code})`; }
        let write = 'write ok (!)'; try { await c.query('begin'); await c.query(`insert into ops.watch_runs (watch_run_id, company_id, as_of_date, started_at, checks_version, planned_keys) values ('verify', 1, current_date, now(), 'verify', 0)`); await c.query('rollback'); } catch (e) { write = `write denied (${e.code})`; try { await c.query('rollback'); } catch { /* */ } }
        let mutate = 'update core ok (!)'; try { await c.query('begin'); await c.query(`update core.companies set name = name where false`); await c.query('rollback'); } catch (e) { mutate = `update core denied (${e.code})`; try { await c.query('rollback'); } catch { /* */ } }
        console.log(`${name}: role=${who.u} statement_timeout=${who.st} watch_runs=${n} core.orders=${core} / ops.watch_runs insert=${write} (期待 ${expectWrite ? 'ok' : 'denied'}) / core update=${mutate} (期待 denied)`);
      } finally { await c.end(); }
    }
    return;
  }
  const client = await openPgClient(base);
  try {
    const info = (await client.query(`select current_user as owner, current_database() as db`)).rows[0];
    const secdef = (await client.query(`select n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' as sig
      from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.prosecdef and n.nspname = any($1::text[]) order by 1`, [WATCH_SCHEMAS])).rows.map((r) => r.sig);
    const watcherPw = dryRun ? '<watcher の新しいパスワード>' : newPassword(), writerPw = dryRun ? '<watch_writer の新しいパスワード>' : newPassword();
    const stmts = roleStatements({ dbName: info.db, owner: info.owner, watcherPw, writerPw, secdefFunctions: secdef });
    if (dryRun) { console.log(`-- dry-run: owner=${info.owner} db=${info.db} security definer 関数 ${secdef.length} 件`); for (const s of stmts) console.log(s.replace(/password '[^']*'/, "password '***'") + ';'); return; }
    await client.query('begin');
    for (const s of stmts) await client.query(s);
    await client.query('commit');
    console.log(`✅ ロールを作った / 権限をそろえた (owner=${info.owner} db=${info.db} security definer 関数 ${secdef.length} 件の public execute を外した)`);
    console.log('miniPC の .env に足す 2 行 (パスワードはこの画面にしか出ない。Render の画面では管理されない):');
    console.log(`COMPANY_DB_WATCH_URL=${urlFor(base, 'watcher', watcherPw)}`);
    console.log(`COMPANY_DB_WATCH_WRITER_URL=${urlFor(base, 'watch_writer', writerPw)}`);
    console.log('足したら: node -r dotenv/config scripts/company-db/create-watch-roles.mjs --verify');
  } catch (e) { try { await client.query('rollback'); } catch { /* */ } throw e; }
  finally { await client.end(); }
}

const isMain = process.argv[1] && /create-watch-roles\.mjs$/i.test(process.argv[1]);
if (isMain) main().then(() => { process.exitCode = 0; }).catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
