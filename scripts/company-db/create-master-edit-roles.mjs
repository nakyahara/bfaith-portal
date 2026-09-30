#!/usr/bin/env node
/**
 * create-master-edit-roles.mjs — マスタの入力の画面だけのロール master_edit と、切替の段階を進める運用のロール master_ops を作る / 権限をそろえる
 *   (Company DB構想 14 ⑤-1・PR #1563 R1 M8。create-watch-roles.mjs と同じ作り = 1 取引・作った後に属性を確かめて違えば取り消す)
 *
 * master_edit (Render の apps/master-edit が env COMPANY_DB_MASTER_EDIT_URL で使う。無ければ画面は見るだけ):
 *   読む: 画面が読む表だけ (MASTER_EDIT_SELECT)
 *   書く: 保存の経路で書く表・列だけ (MASTER_EDIT_WRITE)。変更の記録 (0026 のトリガーは呼び手の権限で動く) の insert・version の通し番号も要る
 *   🚨 渡さない: ops.set_master_cutover_phase (切替の段階を進める)・ops.record_ne_set_observations (NE の観測を書く)・core.sku_components の書き込み (構成の依頼を上げる)・
 *      ops.master_cutover_state の書き込み・ops.master_legacy_gate_acks の書き込み。構成の依頼を上げる・観測を書くのは夜間ロード (表の持ち主のロール = COMPANY_DB_URL)
 * master_ops (手の操作 scripts/company-db/master-cutover.mjs が env COMPANY_DB_MASTER_OPS_URL で使う):
 *   ops.set_master_cutover_phase の実行 (security definer = 表の書き込みの権限は持たない) と、段階・記録・門の記録を読むだけ
 * 使い方:
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs --dry-run   # 流す文だけ見る
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs             # 作る (パスワードはこの画面にしか出ない)
 * 🚨 0050 まで migration を流した後に。migration で表を足したら流し直す (権限は表ごとに付ける)
 */
import crypto from 'node:crypto';
import { openPgClient } from './migrate.mjs';
import { urlFor } from './create-watch-roles.mjs';

export const MASTER_EDIT_ROLES = ['master_edit', 'master_ops'];
export const MASTER_EDIT_CONN_LIMIT = 5, MASTER_OPS_CONN_LIMIT = 2;
/** 画面が読む表 */
export const MASTER_EDIT_SELECT = [
  'core.skus', 'core.products', 'core.sku_components', 'core.sku_costs', 'core.suppliers', 'core.supplier_skus', 'core.external_ids', 'core.listings', 'core.listing_components',
  'events.master_change_events',
  'ops.master_cutover_state', 'ops.master_edit_requests', 'ops.sku_component_requests', 'ops.sku_component_breaches', 'ops.ne_set_observations', 'ops.ne_set_observation_runs',
  'ops.ne_csv_exports', 'ops.ne_csv_export_rows', 'ops.master_decision_candidates', 'ops.master_decision_observations', 'ops.master_compare_runs',
];
/** 保存の経路で書く表・列 (lib/master-write.mjs の saveSku) */
export const MASTER_EDIT_WRITE = [
  ['update (name, tax_rate, tax_class, handling, standard_price_jpy, shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own, version)', 'core.skus'],
  ['update (name, status, sales_class, parent_product_id, parent_set_by)', 'core.products'],
  ['insert, update (is_primary)', 'core.supplier_skus'],
  ['insert, update (valid_to), delete', 'core.sku_costs'],
  ['insert', 'events.master_change_events'],
  ['insert', 'ops.master_edit_requests'],
  ['insert, update (status, closed_at, closed_by, close_reason)', 'ops.sku_component_requests'],
  ['update (status, closed_at, closed_by, close_reason)', 'ops.sku_component_breaches'],
];
export const MASTER_OPS_SELECT = ['ops.master_cutover_state', 'ops.master_cutover_events', 'ops.master_legacy_gate_acks'];
export const CUTOVER_FUNCTION = 'ops.set_master_cutover_phase(text, text, jsonb, text)';

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };
const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;
const newPassword = () => crypto.randomBytes(24).toString('base64url');

/** 流す文の一覧 (パスワードは引数) */
export function masterEditRoleStatements({ dbName, editPw, opsPw }) {
  const db = ident(dbName);
  const s = [];
  for (const [role, pw, limit] of [['master_edit', editPw, MASTER_EDIT_CONN_LIMIT], ['master_ops', opsPw, MASTER_OPS_CONN_LIMIT]]) {
    s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = '${role}') then create role ${role}; end if; end $$`);
    // 🚨 nosuperuser などは書かない (create-watch-roles.mjs と同じ理由)。作った後に pg_roles で確かめる
    s.push(`alter role ${role} with login password ${lit(pw)} nocreaterole noinherit connection limit ${limit}`);
    s.push(`alter role ${role} set statement_timeout = '20s'`);
    s.push(`grant connect on database ${db} to ${role}`);
  }
  for (const sc of ['core', 'ops', 'events']) s.push(`grant usage on schema ${sc} to master_edit`);
  for (const t of MASTER_EDIT_SELECT) s.push(`grant select on ${t} to master_edit`);
  for (const [priv, t] of MASTER_EDIT_WRITE) s.push(`grant ${priv} on ${t} to master_edit`);
  s.push('grant usage on sequence core.master_version_seq to master_edit');   // version の既定値・0026 のトリガーの nextval (呼び手の権限)
  s.push(`revoke all on function ${CUTOVER_FUNCTION} from public`);
  s.push(`revoke all on function ops.record_ne_set_observations(jsonb) from public`);
  s.push('grant usage on schema ops to master_ops');
  for (const t of MASTER_OPS_SELECT) s.push(`grant select on ${t} to master_ops`);
  s.push(`grant execute on function ${CUTOVER_FUNCTION} to master_ops`);
  return s;
}

/** ロールを作る / 権限をそろえる (1 取引)。commit の前に属性を確かめ、違えば取り消す */
export async function createMasterEditRoles(client, { editPw, opsPw, dryRun = false }) {
  const info = (await client.query(`select current_user as owner, current_database() as db, r.rolcreaterole, r.rolsuper from pg_roles r where r.rolname = current_user`)).rows[0];
  if (!info.rolcreaterole && !info.rolsuper) throw new Error(`${info.owner} に CREATEROLE が無い = ロールを作れない`);
  const stmts = masterEditRoleStatements({ dbName: info.db, editPw, opsPw });
  if (dryRun) return { info, stmts, roles: [] };
  await client.query('begin');
  try {
    for (const s of stmts) await client.query(s);
    const roles = (await client.query(`select r.rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication, r.rolcanlogin, r.rolinherit, r.rolconnlimit,
        coalesce((select string_agg(g.rolname, ',' order by g.rolname) from pg_auth_members m join pg_roles g on g.oid = m.roleid where m.member = r.oid), '') as memberships
      from pg_roles r where r.rolname = any($1::text[]) order by r.rolname`, [MASTER_EDIT_ROLES])).rows;
    const limitOf = { master_edit: MASTER_EDIT_CONN_LIMIT, master_ops: MASTER_OPS_CONN_LIMIT };
    const bad = [];
    for (const r of roles) {
      const why = [];
      if (r.rolsuper) why.push('superuser'); if (r.rolcreaterole) why.push('createrole'); if (r.rolcreatedb) why.push('createdb'); if (r.rolbypassrls) why.push('bypassrls'); if (r.rolreplication) why.push('replication');
      if (!r.rolcanlogin) why.push('login できない'); if (r.rolinherit) why.push('inherit'); if (Number(r.rolconnlimit) !== limitOf[r.rolname]) why.push(`connection limit ${r.rolconnlimit}`);
      if (r.memberships) why.push(`ほかのロールのメンバー (${r.memberships})`);
      if (why.length) bad.push(`${r.rolname}: ${why.join(' / ')}`);
    }
    if (roles.length !== MASTER_EDIT_ROLES.length) bad.push(`ロールが ${roles.length} 件しか無い`);
    if (bad.length) throw new Error(`ロールの属性が期待と違う (取り消した): ${bad.join(' ; ')}`);
    await client.query('commit');
    return { info, stmts, roles };
  } catch (e) { try { await client.query('rollback'); } catch { /* */ } throw e; }
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const base = process.env.COMPANY_DB_URL;
  if (!base) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  const client = await openPgClient(base);
  try {
    const editPw = dryRun ? '<master_edit の新しいパスワード>' : newPassword(), opsPw = dryRun ? '<master_ops の新しいパスワード>' : newPassword();
    const r = await createMasterEditRoles(client, { editPw, opsPw, dryRun });
    if (dryRun) { for (const s of r.stmts) console.log(s.replace(/password '[^']*'/, "password '***'") + ';'); return; }
    console.log(`✅ master_edit / master_ops を作った / 権限をそろえた (owner=${r.info.owner} db=${r.info.db})`);
    console.log('Render の環境変数に足す (パスワードはこの画面にしか出ない):');
    console.log(`COMPANY_DB_MASTER_EDIT_URL=${urlFor(base, 'master_edit', editPw)}`);
    console.log('切替の段階を進める手の操作 (scripts/company-db/master-cutover.mjs) の .env に足す:');
    console.log(`COMPANY_DB_MASTER_OPS_URL=${urlFor(base, 'master_ops', opsPw)}`);
  } finally { await client.end(); }
}

const isMain = process.argv[1] && /create-master-edit-roles\.mjs$/i.test(process.argv[1]);
if (isMain) main().then(() => { process.exitCode = 0; }).catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
