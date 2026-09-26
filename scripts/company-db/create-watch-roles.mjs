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
export const WATCHER_CONN_LIMIT = 3, WRITER_CONN_LIMIT = 2;
export const WATCH_ROLES = ['watcher', 'watch_writer'];

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };
const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;
const newPassword = () => crypto.randomBytes(24).toString('base64url');

/** 流す SQL の一覧 (パスワードは引数)。dbName / owner = 実際の DB 名・migration を流すユーザー (= current_user) */
export function roleStatements({ dbName, owner, watcherPw, writerPw, secdefFunctions = [] }) {
  const db = ident(dbName), o = ident(owner);
  const s = [];
  // watcher
  s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = 'watcher') then create role watcher; end if; end $$`);
  // 🚨 nosuperuser / nocreatedb / nobypassrls / noreplication は書かない: PostgreSQL 16 以降は、その属性を「書くだけ」で実行者に同じ属性が無いと拒まれる
  //    (Render の default user は CREATEROLE だけ → "permission denied to alter role"。9/22 に本番で踏んだ。PGlite 18 でも同じ)。
  //    create role の既定がどれも off なので書く必要は無い。代わりに作った後 (commit の前) に pg_roles で確かめ、違えば rollback する (createRoles)
  s.push(`alter role watcher with login password ${lit(watcherPw)} nocreaterole noinherit connection limit ${WATCHER_CONN_LIMIT}`);
  s.push(`alter role watcher set statement_timeout = '10s'`);
  s.push(`alter role watcher set default_transaction_read_only = on`);
  s.push(`grant connect on database ${db} to watcher`);
  for (const sc of WATCH_SCHEMAS) {
    s.push(`grant usage on schema ${sc} to watcher`);
    s.push(`grant select on all tables in schema ${sc} to watcher`);
    s.push(`alter default privileges for role ${o} in schema ${sc} grant select on tables to watcher`);   // 将来の表にも (schema を限定)
  }
  // security definer の関数: public の execute を外す (owner には残る)。将来の関数は作るときに個別に
  for (const f of secdefFunctions) {
    s.push(`revoke execute on function ${f} from public`); s.push(`grant execute on function ${f} to ${o}`);
  }
  // watch_writer
  s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = 'watch_writer') then create role watch_writer; end if; end $$`);
  s.push(`alter role watch_writer with login password ${lit(writerPw)} nocreaterole noinherit connection limit ${WRITER_CONN_LIMIT}`);
  s.push(`alter role watch_writer set statement_timeout = '30s'`);
  s.push(`grant connect on database ${db} to watch_writer`);
  s.push(`grant usage on schema ops to watch_writer`);
  for (const t of WATCH_TABLES) s.push(`grant select, insert on ops.${t} to watch_writer`);
  s.push(`grant update (${RUNS_UPDATE_COLS.join(', ')}) on ops.watch_runs to watch_writer`);
  s.push(`grant update (${ISSUES_UPDATE_COLS.join(', ')}) on ops.watch_issues to watch_writer`);
  s.push(`grant usage on all sequences in schema ops to watch_writer`);
  s.push(`alter default privileges for role ${o} in schema ops grant usage on sequences to watch_writer`);
  // 照合の判断の台帳 (0032) は watch_writer が関数だけで書く (表へ直接は書けない)。watch_writer を作った後に付ける
  for (const f of secdefFunctions) if (/^ops\.record_decision_(candidates|done)\(/.test(f)) s.push(`grant execute on function ${f} to watch_writer`);
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
    const watcherPw = dryRun ? '<watcher の新しいパスワード>' : newPassword(), writerPw = dryRun ? '<watch_writer の新しいパスワード>' : newPassword();
    const r = await createRoles(client, { watcherPw, writerPw, dryRun });
    if (dryRun) { console.log(`-- dry-run: owner=${r.info.owner} db=${r.info.db} security definer 関数 ${r.secdef.length} 件`); for (const s of r.stmts) console.log(s.replace(/password '[^']*'/, "password '***'") + ';'); return; }
    console.log(`✅ ロールを作った / 権限をそろえた (owner=${r.info.owner} db=${r.info.db} security definer 関数 ${r.secdef.length} 件の public execute を外した。${r.roles.map((x) => `${x.rolname}: superuser=${x.rolsuper} createrole=${x.rolcreaterole} createdb=${x.rolcreatedb} bypassrls=${x.rolbypassrls} login=${x.rolcanlogin} connlimit=${x.rolconnlimit} memberships=${x.memberships}`).join(' / ')})`);
    console.log('miniPC の .env に足す 2 行 (パスワードはこの画面にしか出ない。Render の画面では管理されない):');
    console.log(`COMPANY_DB_WATCH_URL=${urlFor(base, 'watcher', watcherPw)}`);
    console.log(`COMPANY_DB_WATCH_WRITER_URL=${urlFor(base, 'watch_writer', writerPw)}`);
    console.log('足したら: node -r dotenv/config scripts/company-db/create-watch-roles.mjs --verify');
  } finally { await client.end(); }
}

/**
 * ロールを作る / 権限をそろえる (1 取引)。戻り値 = { info, secdef, stmts, roles }。dryRun なら文を作るだけで流さない。
 * 🚨 commit の前に pg_roles で確かめる: superuser / createrole / createdb / bypassrls / replication が無い・login できる・noinherit・connection limit が期待どおり・
 *    ほかのロールのメンバーになっていない (pg_auth_members)。違えば rollback して止める = 「書いて直す」のではなく「検査して止める」
 *    (nosuperuser などを alter に書くと CREATEROLE だけの実行者は拒まれる。既存のロールに危険な属性や membership が付いていたら人が見る)
 */
export async function createRoles(client, { watcherPw, writerPw, dryRun = false }) {
  const info = (await client.query(`select current_user as owner, current_database() as db, r.rolcreaterole, r.rolsuper from pg_roles r where r.rolname = current_user`)).rows[0];
  if (!info.rolcreaterole && !info.rolsuper) throw new Error(`${info.owner} に CREATEROLE が無い = ロールを作れない (Render の default user なら普通はある。別のユーザーで接続していないか .env の COMPANY_DB_URL を確かめる)`);
  const secdef = (await client.query(`select n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' as sig
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.prosecdef and n.nspname = any($1::text[]) order by 1`, [WATCH_SCHEMAS])).rows.map((r) => r.sig);
  const stmts = roleStatements({ dbName: info.db, owner: info.owner, watcherPw, writerPw, secdefFunctions: secdef });
  if (dryRun) return { info, secdef, stmts, roles: [] };
  await client.query('begin');
  try {
    for (const s of stmts) await client.query(s);
    const roles = (await client.query(`select r.rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication, r.rolcanlogin, r.rolinherit, r.rolconnlimit,
        coalesce((select string_agg(g.rolname, ',' order by g.rolname) from pg_auth_members m join pg_roles g on g.oid = m.roleid where m.member = r.oid), '') as memberships
      from pg_roles r where r.rolname = any($1::text[]) order by r.rolname`, [WATCH_ROLES])).rows;
    const limitOf = { watcher: WATCHER_CONN_LIMIT, watch_writer: WRITER_CONN_LIMIT };
    const bad = [];
    for (const r of roles) {
      const why = [];
      if (r.rolsuper) why.push('superuser'); if (r.rolcreaterole) why.push('createrole'); if (r.rolcreatedb) why.push('createdb'); if (r.rolbypassrls) why.push('bypassrls'); if (r.rolreplication) why.push('replication');
      if (!r.rolcanlogin) why.push('login できない'); if (r.rolinherit) why.push('inherit'); if (Number(r.rolconnlimit) !== limitOf[r.rolname]) why.push(`connection limit ${r.rolconnlimit} (期待 ${limitOf[r.rolname]})`);
      if (r.memberships) why.push(`ほかのロールのメンバー (${r.memberships}) = 権限を継ぐ経路。人が revoke してから流し直す`);
      if (why.length) bad.push(`${r.rolname}: ${why.join(' / ')}`);
    }
    if (roles.length !== WATCH_ROLES.length) bad.push(`ロールが ${roles.length} 件しか無い (期待 ${WATCH_ROLES.length})`);
    if (bad.length) throw new Error(`ロールの属性が期待と違う (取り消した): ${bad.join(' ; ')}`);
    await client.query('commit');
    return { info, secdef, stmts, roles };
  } catch (e) { try { await client.query('rollback'); } catch { /* */ } throw e; }
}

const isMain = process.argv[1] && /create-watch-roles\.mjs$/i.test(process.argv[1]);
if (isMain) main().then(() => { process.exitCode = 0; }).catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
