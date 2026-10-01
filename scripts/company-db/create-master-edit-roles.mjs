#!/usr/bin/env node
/**
 * create-master-edit-roles.mjs — マスタの入力の仕組みのロールを作る / 権限をそろえる (Company DB構想 14 ⑤-1・PR #1563 R1 M8・R2 H2 / M3 / M4・仮レビュー Low 2 / Low 3 / Low 4)
 *   create-watch-roles.mjs と同じ作り = 1 取引・作った後に属性を確かめて違えば取り消す。流し直すと、前に付けた権限を外してから付け直す
 *
 * master_edit     (Render の apps/master-edit が env COMPANY_DB_MASTER_EDIT_URL で使う。無ければ画面は見るだけ):
 *   読む: 画面が読む表だけ (MASTER_EDIT_SELECT) / 書く: 保存の経路で書く表の列だけ (MASTER_EDIT_WRITE。insert も列を絞る)
 *   🚨 書けるのは同じ取引で ops.begin_master_write (実行だけ渡す) を呼んだ後だけ (DB の trigger。段階・持ち主表・誰が を DB で守る・#1563 R3 M2)
 *   🚨 渡さない: events.master_change_events の insert (記録は 0026 の関数 = security definer が書く = 偽れない)・core.skus.version・core.sku_components の書き込み・
 *      切替の段階・門の記録・NE の観測の書き込み。core.sku_costs の削除は渡すが、今日より前の行は DB の trigger が拒む (0050)
 *      core.suppliers の update (仕入先の行の共有の鍵は 0050 の関数 core.lock_suppliers_for_share = security definer の実行だけ)
 *   ロールの設定 = statement_timeout 20s・lock_timeout 10s・idle_in_transaction_session_timeout 60s (画面の接続の設定と同じ。画面が付け忘れても長く持たない)
 * master_ops      (手の操作 scripts/company-db/master-cutover.mjs が env COMPANY_DB_MASTER_OPS_URL で使う): ops.set_master_cutover_phase の実行と、段階・記録を読むだけ
 * master_observer (⑤-2 の夜間ロードが NE のセットの構成の観測を書く env COMPANY_DB_MASTER_OBSERVER_URL): ops.record_ne_set_observations の実行だけ
 * master_gate     (まとめのロール・ログインできない): ops.record_legacy_gate_ack の実行と、段階を読むだけ。ログインは場所ごとのメンバー (#1563 仮レビュー Low 3):
 *   master_gate_render (Render の ⑤-3 の古い入口の門 env COMPANY_DB_MASTER_GATE_RENDER_URL) / master_gate_minipc (miniPC の門 env COMPANY_DB_MASTER_GATE_MINIPC_URL)。
 *   DB の関数がログインのロールと記録の場所 (host) を照らす = Render のログインで minipc を名乗れない
 * 構成の依頼を上げる (promoteComponentRequest) のは夜間ロード = 表の持ち主のロール (COMPANY_DB_URL)
 *
 * 🚨 パスワード (#1563 仮レビュー Low 2): 流し直しても、もうあるロールのパスワードは変えない (門のロールは Render と miniPC の両方が使う =
 *    黙って変えると門の記録が書けなくなり、切替が進められない)。パスワードを付けるのは、ロールを初めて作るときと --rotate-password <ロール> を付けたときだけ
 *    (付けたロールの新しい接続文字列だけを出す。変えたら、そのロールを使う場所の env を同じ日に書き換える)
 * 使い方:
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs --dry-run   # 流す文だけ見る
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs             # 作る / 権限をそろえる (新しいロールのパスワードだけ、この画面に出る)
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs --rotate-password master_gate_render   # そのロールのパスワードだけ変える (何回でも付けられる)
 * 🚨 0050 まで migration を流した後に。migration で表を足したら流し直す (権限は表ごとに付ける)
 */
import crypto from 'node:crypto';
import { openPgClient } from './migrate.mjs';
import { urlFor } from './create-watch-roles.mjs';

/** 場所ごとの門のログイン (DB の関数 ops.record_legacy_gate_ack は session_user = 'master_gate_' || host を求める) */
export const GATE_LOGIN_ROLES = Object.freeze({ render: 'master_gate_render', minipc: 'master_gate_minipc' });
/** ログインできるロール (パスワードを持つ) */
export const MASTER_LOGIN_ROLES = Object.freeze(['master_edit', 'master_gate_minipc', 'master_gate_render', 'master_observer', 'master_ops']);
/** まとめのロール (ログインできない・パスワードなし) */
export const MASTER_GROUP_ROLES = Object.freeze(['master_gate']);
export const MASTER_EDIT_ROLES = Object.freeze([...MASTER_LOGIN_ROLES, ...MASTER_GROUP_ROLES].sort());
const CONN_LIMIT = { master_edit: 5, master_ops: 2, master_observer: 2, master_gate_render: 8, master_gate_minipc: 8 };   // 門: プロセス・CLI が同時に記録を書く (⑤-3)
/** まとめのロールのメンバー (INHERIT = まとめのロールの権限をそのまま使う) */
const MEMBER_OF = { master_gate_render: 'master_gate', master_gate_minipc: 'master_gate' };
/** ロールごとの設定 (master_edit = 画面の接続と同じ値。apps/master-edit/router.mjs の connect) */
const ROLE_SETTINGS = {
  master_edit: { statement_timeout: '20s', lock_timeout: '10s', idle_in_transaction_session_timeout: '60s' },
  master_gate_render: { statement_timeout: '20s' }, master_gate_minipc: { statement_timeout: '20s' },
  master_observer: { statement_timeout: '20s' }, master_ops: { statement_timeout: '20s' },
};
/** 画面が読む表 */
export const MASTER_EDIT_SELECT = [
  'core.skus', 'core.products', 'core.sku_components', 'core.sku_costs', 'core.suppliers', 'core.supplier_skus', 'core.external_ids', 'core.listings', 'core.listing_components',
  'events.master_change_events',
  'ops.master_cutover_state', 'ops.master_edit_requests', 'ops.sku_component_requests', 'ops.sku_component_breaches', 'ops.ne_set_observations', 'ops.ne_set_observation_runs',
  'ops.ne_csv_exports', 'ops.ne_csv_export_rows', 'ops.master_decision_candidates', 'ops.master_decision_observations', 'ops.master_compare_runs',
];
/** 保存の経路で書く表・列 (lib/master-write.mjs の saveSku)。insert も列を絞る */
export const MASTER_EDIT_WRITE = [
  ['update (name, tax_rate, tax_class, handling, standard_price_jpy, shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own)', 'core.skus'],
  ['update (name, status, sales_class, parent_product_id, parent_set_by)', 'core.products'],
  ['insert (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id), update (is_primary)', 'core.supplier_skus'],
  ['insert (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id), update (valid_to), delete', 'core.sku_costs'],
  ['insert (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, error, started_at)', 'ops.master_edit_requests'],
  ['insert (company_id, set_sku_id, rows, rows_hash, base_rows, reason, requested_by, edit_request_id), update (status, closed_at, closed_by, close_reason)', 'ops.sku_component_requests'],
  ['update (status, closed_at, closed_by, close_reason)', 'ops.sku_component_breaches'],
];
export const CUTOVER_FUNCTION = 'ops.set_master_cutover_phase(text, text, jsonb, text)';
export const OBSERVE_FUNCTION = 'ops.record_ne_set_observations(jsonb)';
export const ACK_FUNCTION = 'ops.record_legacy_gate_ack(text, text, text, jsonb, text, text, integer, timestamptz, boolean, text)';
export const LOCK_SUPPLIERS_FUNCTION = 'core.lock_suppliers_for_share(bigint[])';
/** 画面の保存を始める (段階・持ち主表を DB で確かめて、取引の行を書く。これの後でないと画面のロールは書けない・#1563 R3 M2) */
export const BEGIN_WRITE_FUNCTION = 'ops.begin_master_write(uuid, text, text, jsonb)';

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };
const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;
const newPassword = () => crypto.randomBytes(24).toString('base64url');

/**
 * 流す文の一覧。pw = { ロール: パスワード } = パスワードを付けるロールだけ (無いロールのパスワードは変えない)。
 * 🚨 ロールを新しく作る文 (create role) には login を付けない: 付けるのは下の alter role (パスワードがあれば一緒に)。パスワードなしで作ったログインのロールは入れない
 */
export function masterEditRoleStatements({ dbName, pw = {} }) {
  const db = ident(dbName);
  const all = MASTER_EDIT_ROLES.join(', ');
  const s = [];
  for (const role of MASTER_EDIT_ROLES) {
    s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = '${role}') then create role ${role}; end if; end $$`);
  }
  for (const role of MASTER_GROUP_ROLES) {
    // まとめのロール = ログインできない・パスワードなし (前に login で作っていても外す)
    s.push(`alter role ${role} with nologin password null nocreaterole noinherit`);
  }
  for (const role of MASTER_LOGIN_ROLES) {
    // 🚨 nosuperuser などは書かない (create-watch-roles.mjs と同じ理由)。作った後に pg_roles で確かめる
    const inherit = MEMBER_OF[role] ? 'inherit' : 'noinherit';
    s.push(`alter role ${role} with login${pw[role] != null ? ` password ${lit(pw[role])}` : ''} nocreaterole ${inherit} connection limit ${CONN_LIMIT[role]}`);
    for (const [k, v] of Object.entries(ROLE_SETTINGS[role] || {})) s.push(`alter role ${role} set ${ident(k)} = ${lit(v)}`);
    s.push(`grant connect on database ${db} to ${role}`);
  }
  for (const [role, group] of Object.entries(MEMBER_OF)) s.push(`grant ${group} to ${role}`);
  // 前に付けた権限を外してから付け直す (流し直しで広い権限が残らない)
  for (const t of new Set([...MASTER_EDIT_SELECT, ...MASTER_EDIT_WRITE.map(([, t2]) => t2), 'events.master_change_events', 'ops.master_legacy_gate_acks', 'ops.master_legacy_manifests', 'ops.master_cutover_events', 'ops.master_write_sessions', 'ops.master_cutover_prereq_checks'])) {
    s.push(`revoke all on ${t} from ${all}`);
  }
  for (const f of [CUTOVER_FUNCTION, OBSERVE_FUNCTION, ACK_FUNCTION, LOCK_SUPPLIERS_FUNCTION, BEGIN_WRITE_FUNCTION]) s.push(`revoke all on function ${f} from public, ${all}`);
  // master_edit
  for (const sc of ['core', 'ops', 'events']) s.push(`grant usage on schema ${sc} to master_edit`);
  for (const t of MASTER_EDIT_SELECT) s.push(`grant select on ${t} to master_edit`);
  for (const [priv, t] of MASTER_EDIT_WRITE) s.push(`grant ${priv} on ${t} to master_edit`);
  s.push(`grant execute on function ${LOCK_SUPPLIERS_FUNCTION} to master_edit`);
  s.push(`grant execute on function ${BEGIN_WRITE_FUNCTION} to master_edit`);
  s.push('grant usage on sequence core.master_version_seq to master_edit');   // version の既定値・0026 の bump_master_version の nextval (呼び手の権限)
  // master_ops
  s.push('grant usage on schema ops to master_ops');
  for (const t of ['ops.master_cutover_state', 'ops.master_cutover_events', 'ops.master_legacy_gate_acks', 'ops.master_legacy_manifests']) s.push(`grant select on ${t} to master_ops`);
  s.push(`grant execute on function ${CUTOVER_FUNCTION} to master_ops`);
  // master_observer
  s.push('grant usage on schema ops to master_observer');
  s.push(`grant execute on function ${OBSERVE_FUNCTION} to master_observer`);
  // master_gate (まとめ。ログインは master_gate_render / master_gate_minipc が INHERIT で使う)
  s.push('grant usage on schema ops to master_gate');
  s.push('grant select on ops.master_cutover_state to master_gate');
  s.push(`grant execute on function ${ACK_FUNCTION} to master_gate`);
  return s;
}

/**
 * ロールを作る / 権限をそろえる (1 取引)。commit の前に属性を確かめ、違えば取り消す。
 * パスワードを付けるのは: まだ無いログインのロール (新しいパスワード) / rotate に入れたロール (新しいパスワード) / pw で渡したロール (その値・試験用)。
 * それ以外のもうあるロールのパスワードは変えない。戻り値の pw = 付けたロールのパスワードだけ
 */
export async function createMasterEditRoles(client, { pw = {}, rotate = [], dryRun = false } = {}) {
  const info = (await client.query(`select current_user as owner, current_database() as db, r.rolcreaterole, r.rolsuper from pg_roles r where r.rolname = current_user`)).rows[0];
  if (!info.rolcreaterole && !info.rolsuper) throw new Error(`${info.owner} に CREATEROLE が無い = ロールを作れない`);
  for (const r of rotate) if (!MASTER_LOGIN_ROLES.includes(r)) throw new Error(`--rotate-password に知らないロール: ${r} (${MASTER_LOGIN_ROLES.join(' / ')})`);
  const existing = new Set((await client.query('select rolname from pg_roles where rolname = any($1::text[])', [MASTER_LOGIN_ROLES])).rows.map((r) => r.rolname));
  const pws = {};
  for (const role of MASTER_LOGIN_ROLES) {
    if (pw[role] != null) pws[role] = pw[role];
    else if (!existing.has(role) || rotate.includes(role)) pws[role] = dryRun ? `<${role} の新しいパスワード>` : newPassword();
  }
  const stmts = masterEditRoleStatements({ dbName: info.db, pw: pws });
  if (dryRun) return { info, stmts, roles: [], pw: pws, created: MASTER_LOGIN_ROLES.filter((r) => !existing.has(r)) };
  await client.query('begin');
  try {
    for (const s of stmts) await client.query(s);
    const roles = (await client.query(`select r.rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication, r.rolcanlogin, r.rolinherit, r.rolconnlimit,
        coalesce((select string_agg(g.rolname, ',' order by g.rolname) from pg_auth_members m join pg_roles g on g.oid = m.roleid where m.member = r.oid), '') as memberships
      from pg_roles r where r.rolname = any($1::text[]) order by r.rolname`, [MASTER_EDIT_ROLES])).rows;
    const bad = [];
    for (const r of roles) {
      const why = [];
      const login = MASTER_LOGIN_ROLES.includes(r.rolname);
      if (r.rolsuper) why.push('superuser'); if (r.rolcreaterole) why.push('createrole'); if (r.rolcreatedb) why.push('createdb'); if (r.rolbypassrls) why.push('bypassrls'); if (r.rolreplication) why.push('replication');
      if (login && !r.rolcanlogin) why.push('login できない');
      if (!login && r.rolcanlogin) why.push('まとめのロールなのに login できる');
      if (r.rolinherit !== !!MEMBER_OF[r.rolname]) why.push(r.rolinherit ? 'inherit' : 'noinherit (まとめのロールの権限を使えない)');
      if (login && Number(r.rolconnlimit) !== CONN_LIMIT[r.rolname]) why.push(`connection limit ${r.rolconnlimit}`);
      if (r.memberships !== (MEMBER_OF[r.rolname] || '')) why.push(`メンバー = (${r.memberships || 'なし'}) (期待 ${MEMBER_OF[r.rolname] || 'なし'})`);
      if (why.length) bad.push(`${r.rolname}: ${why.join(' / ')}`);
    }
    if (roles.length !== MASTER_EDIT_ROLES.length) bad.push(`ロールが ${roles.length} 件しか無い`);
    if (bad.length) throw new Error(`ロールの属性が期待と違う (取り消した): ${bad.join(' ; ')}`);
    await client.query('commit');
    return { info, stmts, roles, pw: pws, created: MASTER_LOGIN_ROLES.filter((r) => !existing.has(r)) };
  } catch (e) { try { await client.query('rollback'); } catch { /* */ } throw e; }
}

/** --rotate-password <ロール> (何回でも) を読む */
export function parseRotateArgs(argv) {
  const out = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--rotate-password') {
      const v = argv[i + 1];
      if (!v || v.startsWith('--')) throw new Error('--rotate-password の後にロールの名前が要る');
      out.push(v); i++;
    }
  }
  return out;
}

const ENV_OF = {
  master_edit: ['COMPANY_DB_MASTER_EDIT_URL', 'Render (apps/master-edit)'],
  master_ops: ['COMPANY_DB_MASTER_OPS_URL', '切替の段階を進める手の操作'],
  master_observer: ['COMPANY_DB_MASTER_OBSERVER_URL', '⑤-2 の夜間ロード (NE の観測)'],
  master_gate_render: ['COMPANY_DB_MASTER_GATE_RENDER_URL', 'Render の ⑤-3 の古い入口の門 (Render の env)'],
  master_gate_minipc: ['COMPANY_DB_MASTER_GATE_MINIPC_URL', 'miniPC の ⑤-3 の古い入口の門 (miniPC の .env)'],
};

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const rotate = parseRotateArgs(process.argv.slice(2));
  const base = process.env.COMPANY_DB_URL;
  if (!base) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  const client = await openPgClient(base);
  try {
    const r = await createMasterEditRoles(client, { dryRun, rotate });
    if (dryRun) {
      for (const s of r.stmts) console.log(s.replace(/password '[^']*'/, "password '***'") + ';');
      console.log(`-- パスワードを付けるロール: ${Object.keys(r.pw).join(' / ') || 'なし (もうあるロールのパスワードは変えない)'}`);
      return;
    }
    console.log(`✅ ${MASTER_EDIT_ROLES.join(' / ')} を作った / 権限をそろえた (owner=${r.info.owner} db=${r.info.db})`);
    const set = Object.keys(r.pw);
    if (!set.length) { console.log('パスワードは変えていない (もうあるロールは今の env のまま)。変えるときは --rotate-password <ロール>'); return; }
    console.log('環境変数に足す / 書き換える (パスワードはこの画面にしか出ない。ここに出ないロールは今の env のまま):');
    for (const role of set) console.log(`${ENV_OF[role][0]}=${urlFor(base, role, r.pw[role])}      # ${role} = ${ENV_OF[role][1]}${r.created.includes(role) ? ' (新しく作った)' : ' (パスワードを変えた)'}`);
  } finally { await client.end(); }
}

const isMain = process.argv[1] && /create-master-edit-roles\.mjs$/i.test(process.argv[1]);
if (isMain) main().then(() => { process.exitCode = 0; }).catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
