/**
 * create-watch-roles.mjs — 見張り用の 2 つのロールを Company DB に作る (設計 09 §6。1 回だけ・中原さんが miniPC で流す)
 *
 *   watcher       照会用: 対象 schema の select だけ。statement_timeout 10s・default_transaction_read_only (保険であって権限の境界ではない)
 *   watch_writer  記録用: ops.watch_* の insert + 限定 update (watch_runs の終了情報・watch_issues の管理列) + sequence の usage
 *
 * 🚨 Render の「新しい credential」は使わない (default user を差し替える)。ここで SQL で作るロールは Render の管理外
 *    = DB の復元 / 移設でロールが無くなったら、もう一度これを流して (無いロールだけ新しいパスワードで作る) 出た行を .env に入れる。
 *      パスワードを変えたい (漏れた・定期の入れ替え) ときだけ --rotate-passwords (README「AI が見張る」)。
 * 🚨 security definer の関数は public の execute を外す (view から呼べるため)。必要なアプリのロール = migration を流す default user (= このスクリプトを流す current_user) には残る。
 * 🚨 パスワード (2026-10-12 の事故): 既定 (引数なし) では、もうあるロールのパスワードは変えない = migration の後の「ロールを流し直す」は権限をそろえるだけ。
 *    前は流すたびにパスワードを作り直して画面に出すだけだった → 10/12 に migration 0070 の後に素で流し、miniPC の .env が古いまま
 *    11:30 から照合・見張りが password authentication failed で止まった。パスワードを付けるのは「ロールが無くて新しく作るとき」と --rotate-passwords のときだけ。
 *    もうあるロールが login できない (nologin)・パスワードが無い (pg_authid を読める実行者のときだけ分かる) なら、黙って直さず exit 1 (--rotate-passwords が要る)。
 *    既定の実行の最後に、もうあった (パスワードを変えなかった) ロールの .env の URL (COMPANY_DB_WATCH_URL / _WRITER_URL) を必ず確かめる (--verify と同じ):
 *    未設定・接続先 (host・port・database) が COMPANY_DB_URL と違う・入れない・権限が違う = exit 1。新しく作ったロールは .env に足してから --verify。
 *    確かめから作るまでは 1 つの取引 + advisory lock (同時に 2 本流しても、後の方は「もうある」と見てパスワードを変えない)。
 *
 * 使い方:
 *   node -r dotenv/config scripts/company-db/create-watch-roles.mjs                     # 権限だけそろえる (無いロールだけ作って新しいパスワードを表示。あるロールのパスワードは変えない = .env はそのまま)
 *   node -r dotenv/config scripts/company-db/create-watch-roles.mjs --dry-run           # 流す SQL を表示するだけ (パスワードは出さない)
 *   node -r dotenv/config scripts/company-db/create-watch-roles.mjs --verify            # .env の COMPANY_DB_WATCH_URL / _WRITER_URL で接続し、読める・書けない を確かめる
 *   node -r dotenv/config scripts/company-db/create-watch-roles.mjs --rotate-passwords  # 🚨 2 つのパスワードを作り直す = .env を直すまで照合・見張りが止まる (表示された 2 行ですぐ .env を書き換える)
 *
 * 表示された行 (COMPANY_DB_WATCH_URL= / COMPANY_DB_WATCH_WRITER_URL=) を miniPC の .env に入れる。パスワードはこの画面にしか出ない。
 */
import crypto from 'node:crypto';
import { openPgClient } from './migrate.mjs';

export const WATCH_SCHEMAS = ['core', 'snapshots', 'events', 'ops', 'mart'];
/** 🆕 0077: あれば読ませる schema (purchasing = 発注の設定の読むだけの写し・CompanyDB構想 21 C2)。無い DB (0077 の前) では何もしない (流し直すと付く) */
export const WATCH_OPTIONAL_SCHEMAS = ['purchasing'];
export const WATCH_TABLES = ['watch_runs', 'watch_results', 'watch_issues', 'watch_result_items'];
export const RUNS_UPDATE_COLS = ['finished_at', 'completed_keys', 'summary', 'last_line'];
export const ISSUES_UPDATE_COLS = ['state', 'severity', 'last_seen_at', 'days_seen', 'recovered_at', 'transitions', 'last_result_id', 'summary', 'updated_at'];
export const WATCHER_CONN_LIMIT = 3, WRITER_CONN_LIMIT = 2;
export const WATCH_ROLES = ['watcher', 'watch_writer'];
/** 同時に流した 2 本が「無い」と見て両方作る (= 後の方が先の方のパスワードを変える) のを防ぐ advisory lock の名前 (hashtext で数にする) */
export const ROLES_LOCK_NAME = 'company-db:create-watch-roles';

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };
const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;
const newPassword = () => crypto.randomBytes(24).toString('base64url');

/**
 * 流す SQL の一覧。dbName / owner = 実際の DB 名・migration を流すユーザー (= current_user)。
 * watcherPw / writerPw = 付けるパスワード。null / undefined = そのロールのパスワードは変えない (login も書かない = もうあるロールの今の状態のまま。
 * 作った直後のロールに付けなければ login できないまま = createRoles の確かめで止まる)
 * mustCreate = 「無いと確かめたので作る」ロール (createRoles が鍵の中で決める)。そのロールは「無ければ作る」ではなく素の create role にする
 *   = 確かめた後に別の実行が作っていたら 42710 (duplicate_object) で取り消す (もうあるロールのパスワードを ALTER で上書きしない・#1696 Codex R1 L1)
 */
export function roleStatements({ dbName, owner, watcherPw, writerPw, secdefFunctions = [], mustCreate = [] }) {
  const db = ident(dbName), o = ident(owner);
  const loginPw = (pw) => (pw != null ? ` login password ${lit(pw)}` : '');
  const createRole = (role) => (mustCreate.includes(role) ? `create role ${ident(role)}` : `do $$ begin if not exists (select 1 from pg_roles where rolname = '${ident(role)}') then create role ${ident(role)}; end if; end $$`);
  const s = [];
  // watcher
  s.push(createRole('watcher'));
  // 🚨 nosuperuser / nocreatedb / nobypassrls / noreplication は書かない: PostgreSQL 16 以降は、その属性を「書くだけ」で実行者に同じ属性が無いと拒まれる
  //    (Render の default user は CREATEROLE だけ → "permission denied to alter role"。9/22 に本番で踏んだ。PGlite 18 でも同じ)。
  //    create role の既定がどれも off なので書く必要は無い。代わりに作った後 (commit の前) に pg_roles で確かめ、違えば rollback する (createRoles)
  s.push(`alter role watcher with${loginPw(watcherPw)} nocreaterole noinherit connection limit ${WATCHER_CONN_LIMIT}`);
  s.push(`alter role watcher set statement_timeout = '10s'`);
  s.push(`alter role watcher set default_transaction_read_only = on`);
  s.push(`grant connect on database ${db} to watcher`);
  for (const sc of WATCH_SCHEMAS) {
    s.push(`grant usage on schema ${sc} to watcher`);
    s.push(`grant select on all tables in schema ${sc} to watcher`);
    s.push(`alter default privileges for role ${o} in schema ${sc} grant select on tables to watcher`);   // 将来の表にも (schema を限定)
  }
  for (const sc of WATCH_OPTIONAL_SCHEMAS) {
    s.push(`do $$ begin if exists (select 1 from pg_namespace where nspname = '${ident(sc)}') then execute 'grant usage on schema ${ident(sc)} to watcher'; `
      + `execute 'grant select on all tables in schema ${ident(sc)} to watcher'; execute 'alter default privileges for role ${o} in schema ${ident(sc)} grant select on tables to watcher'; end if; end $$`);
  }
  // security definer の関数: public の execute を外す (owner には残る)。将来の関数は作るときに個別に
  for (const f of secdefFunctions) {
    s.push(`revoke execute on function ${f} from public`); s.push(`grant execute on function ${f} to ${o}`);
  }
  // watch_writer
  s.push(createRole('watch_writer'));
  s.push(`alter role watch_writer with${loginPw(writerPw)} nocreaterole noinherit connection limit ${WRITER_CONN_LIMIT}`);
  s.push(`alter role watch_writer set statement_timeout = '30s'`);
  s.push(`grant connect on database ${db} to watch_writer`);
  s.push(`grant usage on schema ops to watch_writer`);
  for (const t of WATCH_TABLES) s.push(`grant select, insert on ops.${t} to watch_writer`);
  s.push(`grant update (${RUNS_UPDATE_COLS.join(', ')}) on ops.watch_runs to watch_writer`);
  s.push(`grant update (${ISSUES_UPDATE_COLS.join(', ')}) on ops.watch_issues to watch_writer`);
  s.push(`grant usage on all sequences in schema ops to watch_writer`);
  s.push(`alter default privileges for role ${o} in schema ops grant usage on sequences to watch_writer`);
  // 照合の判断の台帳 (0032)・最後に一致した値 (0033) は watch_writer が関数だけで書く (表へ直接は書けない)。watch_writer を作った後に付ける
  //   新商品の NE 登録の CSV の確かめ (0053・回の始まりの写し → 観測 → 受け取り → 確かめの 4 つの関数・#1571 R1 High 2・R2 Medium 1) も同じ
  for (const f of secdefFunctions) if (/^ops\.(record_decision_(candidates|done)|record_ne_baseline|record_ne_codes|snapshot_ne_reg_targets|record_ne_registration_observations|seal_ne_registration_run|record_ne_registration_check)\(/.test(f)) s.push(`grant execute on function ${f} to watch_writer`);
  //   0058 (広げる道 PR-1・最小の計画 §3): 照合 ② の新商品のゲートの結果 (ops.record_new_entry_gate) も watch_writer が関数だけで書く。
  //   🚨 開放の許可 (grant / revoke) は渡さない = 専用のログイン new_entry_gate (create-master-edit-roles.mjs)。照合のコード 1 本で開放まで完結しない
  //   照合 ② の始めに入口を閉じる (ops.close_new_entry_for_compare) も同じ
  for (const f of secdefFunctions) if (/^ops\.(record_new_entry_gate|close_new_entry_for_compare)\(/.test(f)) s.push(`grant execute on function ${f} to watch_writer`);
  //   0058: watcher は広げる道の読むだけの判定 (ops.widen_check_readonly)・許可の表示用 (ops.new_entry_lease_valid) を実行できる (判定の本体・apply・許可を出す関数は渡さない)
  for (const f of secdefFunctions) if (/^ops\.(widen_check_readonly|new_entry_lease_valid)\(/.test(f)) s.push(`grant execute on function ${f} to watcher`);
  //   🆕 0071 (新商品の下書きを消す): 消してよいかの確かめ (読むだけ) = watcher も読める (消す関数は渡さない = create-master-edit-roles.mjs の master_edit だけ)
  for (const f of secdefFunctions) if (/^ops\.sku_draft_cancel_check\(/.test(f)) s.push(`grant execute on function ${f} to watcher`);
  //   🆕 0072 (NE から消す CSV): 確かめ・断られたかの確かめ・品目の様子・一覧 (読むだけ・バイト列なし) = watcher も読める (作る・受け取る・取り下げは渡さない = master_edit だけ)
  for (const f of secdefFunctions) if (/^ops\.(ne_del_check|ne_del_check_many|ne_del_refused_check|ne_del_item_status|ne_del_overview)\(/.test(f)) s.push(`grant execute on function ${f} to watcher`);
  //   🆕 0068 (代表の数えと門 PR-6): 照合 ② の代表 (親) の生の数えの記録 (ops.record_parent_gate) = watch_writer が関数だけで書く /
  //   生の数え (ops.parent_raw_gate = drift-list・要約の読み直し)・門の状態 (ops.parent_gate_state) = watcher が読むだけ
  for (const f of secdefFunctions) if (/^ops\.record_parent_gate\(/.test(f)) s.push(`grant execute on function ${f} to watch_writer`);
  for (const f of secdefFunctions) if (/^ops\.(parent_raw_gate|parent_gate_state)\(/.test(f)) s.push(`grant execute on function ${f} to watcher`);
  //   🆕 0070 (NE から消えた商品をやめる): やめてよいかの確かめ (読むだけ) = watcher も読める (動かす関数は渡さない = create-master-edit-roles.mjs の master_edit / master_ops だけ)
  for (const f of secdefFunctions) if (/^ops\.sku_gone_from_ne_(check|list)\(/.test(f)) s.push(`grant execute on function ${f} to watcher`);
  //   🆕 0078 (要確認を確認済みにする): 確認済みにしてよいかの確かめ・一覧 (読むだけ) = watcher も読める (確認の関数は渡さない = create-master-edit-roles.mjs の master_edit だけ)
  for (const f of secdefFunctions) if (/^ops\.quarantine_confirm_(check|list)\(/.test(f)) s.push(`grant execute on function ${f} to watcher`);
  //   0058: 保守の印の表は「ops の全部の表」の select から外す (印の UUID を読ませない・G24)
  s.push(`do $$ begin if to_regclass('ops.master_maintenance_marks') is not null then execute 'revoke all on ops.master_maintenance_marks from watcher, watch_writer'; end if; end $$`);
  //   0058 (§3.9 の 3・R16 H1): 「全部の表に GRANT」の後に、配った CSV の byte 列 (ops.ne_reg_exports.file_bytes) を外して列ごとに付け直す (流し直しても戻らない)
  s.push(`do $$ begin if to_regprocedure('ops.restrict_ne_reg_file_bytes()') is not null then perform ops.restrict_ne_reg_file_bytes(); end if; end $$`);
  //   🆕 0072 (#1690 Codex R1 Medium 1): NE から消す CSV の byte 列 (ops.ne_del_exports.file_bytes) も同じく外す (byte 列は ops.ne_del_download だけ)
  s.push(`do $$ begin if to_regprocedure('ops.restrict_ne_del_file_bytes()') is not null then perform ops.restrict_ne_del_file_bytes(); end if; end $$`);
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

const ENV_OF = { watcher: 'COMPANY_DB_WATCH_URL', watch_writer: 'COMPANY_DB_WATCH_WRITER_URL' };
export const KNOWN_ARGS = ['--dry-run', '--verify', '--rotate-passwords'];
const ROTATE_WARNING = [
  '🚨🚨🚨 --rotate-passwords: watcher / watch_writer のパスワードを作り直す 🚨🚨🚨',
  '🚨 この後 miniPC の .env の COMPANY_DB_WATCH_URL / COMPANY_DB_WATCH_WRITER_URL を下の 2 行に書き換えるまで、',
  '🚨 照合・見張り (daily-sync の CompanyDB見張り・照合 ②) は password authentication failed で止まる。',
  '🚨 書き換えたら: node -r dotenv/config scripts/company-db/create-watch-roles.mjs --verify',
].join('\n');

/** 接続先 (host:port/database) だけを返す (ユーザー・パスワードは入れない = 画面に出してよい)。読めない URL は null */
export function targetOf(url) {
  try {
    const u = new URL(url);
    return `${u.hostname.toLowerCase()}:${u.port || '5432'}/${decodeURIComponent(u.pathname.replace(/^\//, ''))}`;
  } catch { return null; }
}

/**
 * .env の URL (COMPANY_DB_WATCH_URL / _WRITER_URL) で入って権限を確かめる。戻り値 = { bad, authFailed }。
 * roles = 確かめるロール (既定 = 両方)。どれも未設定なら ❌ (数える)。
 * 🚨 #1696 Codex R1 M2: 接続先 (host・port・database) が COMPANY_DB_URL (今そろえた DB) と同じかを先に見る
 *    (復元・移設の後に見張りの URL だけ古い DB を指していても「入れた」で通さない)。違えば接続しない
 */
async function verifyEnvUrls(env, { base, roles = WATCH_ROLES } = {}) {
  let bad = 0, authFailed = 0;
  const want = targetOf(base);
  for (const [name, url, kind, role] of [['COMPANY_DB_WATCH_URL', env.COMPANY_DB_WATCH_URL, 'watcher', 'watcher'], ['COMPANY_DB_WATCH_WRITER_URL', env.COMPANY_DB_WATCH_WRITER_URL, 'writer', 'watch_writer']]) {
    if (!roles.includes(role)) continue;
    if (!url) { console.log(`❌ ${name}: 未設定 (${role} はもうある = .env に要る。値が分からなければ --rotate-passwords で作り直す)`); bad++; continue; }
    const got = targetOf(url);
    if (!got || got !== want) { console.log(`❌ ${name}: 接続先が COMPANY_DB_URL と違う (${name} = ${got ?? '読めない URL'} / COMPANY_DB_URL = ${want}) = 復元・移設の前の DB を指していないか .env を確かめる`); bad++; continue; }
    let c;
    try { c = await openPgClient(url); }
    catch (e) {
      bad++;
      // 28P01 = password authentication failed (パスワードが違う・ロールにパスワードが無い) / 28000 = login できない・ロールが無い
      if (e && (e.code === '28P01' || e.code === '28000')) authFailed++;
      console.log(`❌ ${name}: 接続できない (${e && e.code ? `${e.code} ` : ''}${e && e.message})`);
      continue;
    }
    try {
      const v = await verifyRole(c, kind);
      if (v.findings.length) { bad++; console.log(`❌ ${name} (role=${v.user} statement_timeout=${v.statementTimeout}):`); for (const f of v.findings) console.log(`   - ${f}`); }
      else console.log(`✅ ${name}: role=${v.user} statement_timeout=${v.statementTimeout} 期待どおり (${kind === 'watcher' ? '読める・書けない' : '記録の経路は通る・それ以外は拒まれる'})`);
    } finally { await c.end(); }
  }
  return { bad, authFailed };
}

async function main() {
  const args = process.argv.slice(2);
  const unknown = args.filter((a) => !KNOWN_ARGS.includes(a));
  if (unknown.length) throw new Error(`知らない引数: ${unknown.join(' ')} (使えるのは ${KNOWN_ARGS.join(' / ')})`);
  const dryRun = args.includes('--dry-run'), verify = args.includes('--verify'), rotate = args.includes('--rotate-passwords');
  if (verify && (dryRun || rotate)) throw new Error('--verify は単独で流す (--dry-run / --rotate-passwords と一緒にしない)');
  const base = process.env.COMPANY_DB_URL;
  if (!base) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  if (verify) {
    const { bad, authFailed } = await verifyEnvUrls(process.env, { base });
    if (bad) throw new Error(`${bad} 本の接続が期待と違う (${authFailed ? 'パスワードで入れない = .env が古いか、ロールのパスワードが無い。.env の値を確かめ、分からなければ --rotate-passwords で作り直して .env を書き換える' : 'create-watch-roles.mjs を流し直す・.env を確かめる'})`);
    return;
  }
  if (rotate && !dryRun) console.log(ROTATE_WARNING);
  const client = await openPgClient(base);
  let r;
  try {
    r = await createRoles(client, { rotate, dryRun });
  } finally { await client.end(); }
  const set = Object.keys(r.pw);
  if (dryRun) {
    console.log(`-- dry-run: owner=${r.info.owner} db=${r.info.db} security definer 関数 ${r.secdef.length} 件`);
    for (const s of r.stmts) console.log(s.replace(/password '[^']*'/, "password '***'") + ';');
    console.log(`-- パスワードを付けるロール: ${set.length ? set.map((x) => `${x}${r.created.includes(x) ? ' (新しく作る)' : ' (作り直す)'}`).join(' / ') : 'なし (もうあるロールのパスワードは変えない = .env はそのまま)'}`);
    return;
  }
  console.log(`✅ ${r.created.length ? `ロールを作った (${r.created.join(' / ')}) / ` : ''}権限をそろえた (owner=${r.info.owner} db=${r.info.db} security definer 関数 ${r.secdef.length} 件の public execute を外した。${r.roles.map((x) => `${x.rolname}: superuser=${x.rolsuper} createrole=${x.rolcreaterole} createdb=${x.rolcreatedb} bypassrls=${x.rolbypassrls} login=${x.rolcanlogin} connlimit=${x.rolconnlimit} memberships=${x.memberships}`).join(' / ')})`);
  if (set.length) {
    console.log(`miniPC の .env に${rotate ? '書き換える' : '足す'}行 (パスワードはこの画面にしか出ない。Render の画面では管理されない。ここに出ないロールは今の .env のまま):`);
    for (const role of set) console.log(`${ENV_OF[role]}=${urlFor(base, role, r.pw[role])}      # ${role}${r.created.includes(role) ? ' (新しく作った)' : ' (パスワードを作り直した)'}`);
  }
  if (rotate) { console.log(ROTATE_WARNING); return; }
  const kept = WATCH_ROLES.filter((x) => !set.includes(x));
  if (kept.length) console.log(`パスワードは変えていない (${kept.join(' / ')} は今の .env のまま)。変えるときだけ --rotate-passwords`);
  if (set.length) console.log('新しく作ったロールの行を .env に足したら: node -r dotenv/config scripts/company-db/create-watch-roles.mjs --verify');
  // 既定の実行の最後 (#1696 Codex R1 M1): もうあった (パスワードを変えなかった) ロールは、今の .env の URL で入れて権限が期待どおりかを必ず確かめる。
  //   未設定・接続先が違う・入れない = exit 1 (推測でパスワードを変えない)。新しく作ったロールは .env を直してからの --verify に任せる
  if (!kept.length) return;
  const { bad, authFailed } = await verifyEnvUrls(process.env, { base, roles: kept });
  if (bad) throw new Error(`.env の ${bad} 本が期待と違う (権限はそろえた・パスワードは変えていない)。${authFailed ? 'パスワードで入れない = .env が古いか、ロールのパスワードが無い → --rotate-passwords が要る (作り直して .env を書き換える)' : '上の ❌ を見て .env を直す (値が分からなければ --rotate-passwords で作り直す)'}`);
}

/**
 * もうあるロールの login・パスワードの様子。戻り値 = Map(ロール → { login, noPassword })。
 * noPassword = pg_authid を読める実行者 (superuser) のときだけ分かる (Render の default user は読めない = null = 分からない。そのときは既定の実行の最後の .env の確かめで見つける)
 */
export async function existingWatchRoles(client) {
  const rows = (await client.query(`select rolname, rolcanlogin from pg_roles where rolname = any($1::text[])`, [WATCH_ROLES])).rows;
  const canReadAuthid = (await client.query(`select has_table_privilege('pg_catalog.pg_authid', 'select') as ok`)).rows[0].ok;
  const noPw = canReadAuthid ? new Set((await client.query(`select rolname from pg_authid where rolname = any($1::text[]) and rolpassword is null`, [WATCH_ROLES])).rows.map((r) => r.rolname)) : null;
  return new Map(rows.map((r) => [r.rolname, { login: r.rolcanlogin, noPassword: noPw ? noPw.has(r.rolname) : null }]));
}

/**
 * ロールを作る / 権限をそろえる (1 取引)。戻り値 = { info, secdef, stmts, roles, pw, created }。dryRun なら文を作るだけで流さない。
 * 🚨 パスワード (2026-10-12 の事故): 付けるのは
 *    - watcherPw / writerPw で渡したロール (その値・試験用。今までの呼び方 = 渡せば毎回付ける)
 *    - まだ無いロール (新しいパスワードを発行)
 *    - rotate = true のとき (両方とも新しいパスワードを発行 = CLI の --rotate-passwords)
 *    それ以外のもうあるロールのパスワードは変えない。そのロールが login できない・パスワードが無い (分かるとき) なら黙って直さず止める (--rotate-passwords が要る)。
 *    戻り値の pw = 付けたロールのパスワードだけ ({ watcher?, watch_writer? })・created = 新しく作ったロール
 * 🚨 commit の前に pg_roles で確かめる: superuser / createrole / createdb / bypassrls / replication が無い・login できる・noinherit・connection limit が期待どおり・
 *    ほかのロールのメンバーになっていない (pg_auth_members)。違えば rollback して止める = 「書いて直す」のではなく「検査して止める」
 *    (nosuperuser などを alter に書くと CREATEROLE だけの実行者は拒まれる。既存のロールに危険な属性や membership が付いていたら人が見る)
 */
export async function createRoles(client, { watcherPw, writerPw, rotate = false, dryRun = false } = {}) {
  const info = (await client.query(`select current_user as owner, current_database() as db, r.rolcreaterole, r.rolsuper from pg_roles r where r.rolname = current_user`)).rows[0];
  if (!info.rolcreaterole && !info.rolsuper) throw new Error(`${info.owner} に CREATEROLE が無い = ロールを作れない (Render の default user なら普通はある。別のユーザーで接続していないか .env の COMPANY_DB_URL を確かめる)`);
  const secdef = (await client.query(`select n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' as sig
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.prosecdef and n.nspname = any($1::text[]) order by 1`, [WATCH_SCHEMAS])).rows.map((r) => r.sig);
  // 🚨 #1696 Codex R1 L1: 「あるか」の確かめから作る / そろえるまでを 1 つの取引にし、advisory lock で同時の実行を 1 本ずつにする
  //    (確かめと作るの間に別の実行がロールを作ると、無いと思って ALTER ROLE … PASSWORD を流し、もうあるロールのパスワードを変えてしまう)。
  //    鍵の外で作られた場合 (人の手など) も、作るロールは素の create role = 42710 で取り消す (roleStatements の mustCreate)
  //    🚨 #1696 Codex R2 Medium: begin と鍵の取得も try の中 (鍵の待ちの取り消し・statement_timeout (57014)・権限のエラーでも rollback する =
  //    export の createRoles() の呼び手に失敗中の取引を残さない)。began は begin を送る前に立てる (begin 自体の失敗でも rollback を試す = 取引の外なら警告だけ)
  let began = false;
  try {
    if (!dryRun) { began = true; await client.query('begin'); await client.query('select pg_advisory_xact_lock(hashtext($1))', [ROLES_LOCK_NAME]); }
    const existing = await existingWatchRoles(client);
    const given = { watcher: watcherPw, watch_writer: writerPw };
    const pw = {};
    const needRotate = [];
    for (const role of WATCH_ROLES) {
      if (given[role] != null) pw[role] = given[role];
      else if (!existing.has(role) || rotate) pw[role] = dryRun ? `<${role} の新しいパスワード>` : newPassword();
      else {
        const e = existing.get(role);
        if (!e.login) needRotate.push(`${role} が login できない (nologin)`);
        if (e.noPassword) needRotate.push(`${role} にパスワードが無い`);
      }
    }
    // 推測で直さない: login を付ける / パスワードを付けるのは --rotate-passwords で人が決める (付ければ .env を書き換える日になる)
    if (needRotate.length) throw new Error(`もうあるロールが入れない状態 (${needRotate.join(' / ')})。既定の実行ではパスワードも login も変えない = --rotate-passwords が要る (作り直して表示された行で .env を書き換える)`);
    const created = WATCH_ROLES.filter((r) => !existing.has(r));
    const stmts = roleStatements({ dbName: info.db, owner: info.owner, watcherPw: pw.watcher, writerPw: pw.watch_writer, secdefFunctions: secdef, mustCreate: created });
    if (dryRun) return { info, secdef, stmts, roles: [], pw, created };
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
    return { info, secdef, stmts, roles, pw, created };
  } catch (e) { if (began) { try { await client.query('rollback'); } catch { /* */ } } throw e; }
}

const isMain = process.argv[1] && /create-watch-roles\.mjs$/i.test(process.argv[1]);
if (isMain) main().then(() => { process.exitCode = 0; }).catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
