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

/** 1 文を savepoint の中で試す。戻り値 = 'ok' か SQLSTATE (42501 = 権限がない) */
async function attempt(client, sql, params = []) {
  await client.query('savepoint v');
  try { await client.query(sql, params); await client.query('release savepoint v'); return 'ok'; }
  catch (e) { try { await client.query('rollback to savepoint v'); } catch { /* */ } return (e && e.code) || 'error'; }
}

/**
 * ロールの権限を実際の経路で確かめる (全部 rollback = 何も残さない)。戻り値 = { user, findings: string[] } (findings が空 = 期待どおり)
 *   watcher:      読める (ops.watch_runs・core.orders) / 書けない (read only を外して試す = 権限そのものを見る)
 *   watch_writer: 記録の経路 (run → result → item → issue の insert・許した列の update) が通る / 禁止の列の update・core の select・delete は拒まれる
 */
export async function verifyRole(client, kind) {
  const findings = [];
  const expectUser = kind === 'watcher' ? 'watcher' : 'watch_writer';
  const who = (await client.query(`select current_user as u, current_setting('statement_timeout') as st`)).rows[0];
  if (who.u !== expectUser) findings.push(`ロールが ${expectUser} ではない (${who.u})`);
  await client.query(kind === 'watcher' ? 'begin read write' : 'begin');   // watcher は default_transaction_read_only の保険を外して、権限そのもので拒まれることを見る
  try {
    const expect = async (label, sql, want, params = []) => { const got = await attempt(client, sql, params); if (got !== want) findings.push(`${label}: 期待 ${want} / 実際 ${got}`); return got; };
    await expect('ops.watch_runs の select', `select count(*) from ops.watch_runs`, 'ok');
    if (kind === 'watcher') {
      await expect('core.orders の select', `select 1 from core.orders limit 1`, 'ok');
      await expect('ops.watch_runs の insert (書けてはいけない)', `insert into ops.watch_runs (watch_run_id, company_id, as_of_date, started_at, checks_version, planned_keys) values ('verify', 1, current_date, now(), 'verify', 0)`, '42501');
      await expect('core.companies の update (書けてはいけない)', `update core.companies set name = name where false`, '42501');
      await expect('ops.watch_issues の delete (消せてはいけない)', `delete from ops.watch_issues where false`, '42501');
    } else {
      await expect('watch_runs の insert', `insert into ops.watch_runs (watch_run_id, company_id, as_of_date, started_at, checks_version, planned_keys) values ('verify', 1, current_date, now(), 'verify', 0)`, 'ok');
      const rid = await attempt(client, `insert into ops.watch_results (watch_run_id, company_id, check_id, check_version, scope_key, verdict, severity) values ('verify', 1, 'W0', 'v', 's', 'pass', 'info')`);
      if (rid !== 'ok') findings.push(`watch_results の insert: 期待 ok / 実際 ${rid}`);
      const r = (await client.query(`select watch_result_id from ops.watch_results where watch_run_id = 'verify' limit 1`)).rows[0];
      if (r) {
        await expect('watch_result_items の insert', `insert into ops.watch_result_items (watch_result_id, rank, subject_type, subject_key, payload) values ($1, 1, 'day', 'd', '{}'::jsonb)`, 'ok', [r.watch_result_id]);
        await expect('watch_issues の insert', `insert into ops.watch_issues (company_id, check_id, scope_key, state, severity, first_seen_at, last_seen_at, first_result_id, last_result_id) values (1, 'W0', 's', 'open', 'info', now(), now(), $1, $1)`, 'ok', [r.watch_result_id]);
      }
      // 🚨 保存 (engine.mjs persist) が使う列を全部、実際の形 (終了情報・継続・回復) で試す。列の一覧は roleStatements と同じ定数から作る = 列を足したらここも通らなくなる (Codex R2 Low)
      const runSet = { finished_at: 'now()', completed_keys: '0', summary: `'{}'::jsonb`, last_line: `'v'` };
      const issueSet = { state: `'recovered'`, severity: `'warn'`, last_seen_at: 'now()', days_seen: 'days_seen + 1', recovered_at: 'now()', transitions: 'transitions + 1', last_result_id: 'last_result_id', summary: `'v'`, updated_at: 'now()' };
      for (const [name, cols, set] of [['RUNS_UPDATE_COLS', RUNS_UPDATE_COLS, runSet], ['ISSUES_UPDATE_COLS', ISSUES_UPDATE_COLS, issueSet]]) {
        const diff = [...cols.filter((c) => !(c in set)), ...Object.keys(set).filter((c) => !cols.includes(c))];
        if (diff.length) throw new Error(`verify の update の列が ${name} と合っていない (${diff.join(', ')})`);
      }
      await expect('watch_runs の終了情報の update (許した列を全部)', `update ops.watch_runs set ${RUNS_UPDATE_COLS.map((c) => `${c} = ${runSet[c]}`).join(', ')} where watch_run_id = 'verify'`, 'ok');
      await expect('watch_issues の継続・回復の update (許した列を全部)', `update ops.watch_issues set ${ISSUES_UPDATE_COLS.map((c) => `${c} = ${issueSet[c]}`).join(', ')} where check_id = 'W0' and scope_key = 's' and watch_issue_id in (select watch_issue_id from ops.watch_issues where first_result_id = $1)`, 'ok', [r ? r.watch_result_id : null]);
      await expect('watch_issues の禁止の列の update (通ってはいけない)', `update ops.watch_issues set check_id = check_id where false`, '42501');
      await expect('watch_runs の禁止の列の update (通ってはいけない)', `update ops.watch_runs set planned_keys = planned_keys where false`, '42501');
      await expect('core.orders の select (読めてはいけない)', `select 1 from core.orders limit 1`, '42501');
      await expect('watch_result_items の delete (消せてはいけない)', `delete from ops.watch_result_items where false`, '42501');
    }
  } finally { try { await client.query('rollback'); } catch { /* */ } }
  return { user: who.u, statementTimeout: who.st, findings };
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run'), verify = args.includes('--verify');
  const base = process.env.COMPANY_DB_URL;
  if (!base) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  if (verify) {
    let bad = 0;
    for (const [name, url, kind] of [['COMPANY_DB_WATCH_URL', process.env.COMPANY_DB_WATCH_URL, 'watcher'], ['COMPANY_DB_WATCH_WRITER_URL', process.env.COMPANY_DB_WATCH_WRITER_URL, 'writer']]) {
      if (!url) { console.log(`❌ ${name}: 未設定`); bad++; continue; }
      const c = await openPgClient(url);
      try {
        const v = await verifyRole(c, kind);
        if (v.findings.length) { bad++; console.log(`❌ ${name} (role=${v.user} statement_timeout=${v.statementTimeout}):`); for (const f of v.findings) console.log(`   - ${f}`); }
        else console.log(`✅ ${name}: role=${v.user} statement_timeout=${v.statementTimeout} 期待どおり (${kind === 'watcher' ? '読める・書けない' : '記録の経路は通る・それ以外は拒まれる'})`);
      } finally { await c.end(); }
    }
    if (bad) throw new Error(`${bad} 本の接続が期待と違う (create-watch-roles.mjs を流し直す・.env を確かめる)`);
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
