#!/usr/bin/env node
/**
 * create-master-edit-roles.mjs — マスタの入力の仕組みのロールを作る / 権限をそろえる (Company DB構想 14 ⑤-1・PR #1563 R1 M8・R2 H2 / M3 / M4)
 *   create-watch-roles.mjs と同じ作り = 1 取引・作った後に属性を確かめて違えば取り消す。流し直すと、前に付けた権限を外してから付け直す
 *
 * master_edit     (Render の apps/master-edit が env COMPANY_DB_MASTER_EDIT_URL で使う。無ければ画面は見るだけ):
 *   読む: 画面が読む表だけ (MASTER_EDIT_SELECT) / 書く: 保存の経路で書く表の列だけ (MASTER_EDIT_WRITE。insert も列を絞る)
 *   🚨 渡さない: events.master_change_events の insert (記録は 0026 の関数 = security definer が書く = 偽れない)・core.skus.version・core.sku_components の書き込み・
 *      切替の段階・門の記録・NE の観測の書き込み。core.sku_costs の削除は渡すが、今日より前の行は DB の trigger が拒む (0050)
 *      core.suppliers の update (仕入先の行の共有の鍵は 0050 の関数 core.lock_suppliers_for_share = security definer の実行だけ)
 * master_ops      (手の操作 scripts/company-db/master-cutover.mjs が env COMPANY_DB_MASTER_OPS_URL で使う): ops.set_master_cutover_phase の実行と、段階・記録を読むだけ
 * master_observer (⑤-2 の夜間ロードが NE のセットの構成の観測を書く env COMPANY_DB_MASTER_OBSERVER_URL): ops.record_ne_set_observations の実行だけ
 * master_gate     (⑤-3 の古い入口の門が記録を書く env COMPANY_DB_MASTER_GATE_URL): ops.record_legacy_gate_ack の実行と、段階を読むだけ
 * 構成の依頼を上げる (promoteComponentRequest) のは夜間ロード = 表の持ち主のロール (COMPANY_DB_URL)
 * 使い方:
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs --dry-run   # 流す文だけ見る
 *   node -r dotenv/config scripts/company-db/create-master-edit-roles.mjs             # 作る (パスワードはこの画面にしか出ない)
 * 🚨 0050 まで migration を流した後に。migration で表を足したら流し直す (権限は表ごとに付ける)
 */
import crypto from 'node:crypto';
import { openPgClient } from './migrate.mjs';
import { urlFor } from './create-watch-roles.mjs';

export const MASTER_EDIT_ROLES = ['master_edit', 'master_gate', 'master_observer', 'master_ops'];
const CONN_LIMIT = { master_edit: 5, master_ops: 2, master_observer: 2, master_gate: 4 };
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
export const ACK_FUNCTION = 'ops.record_legacy_gate_ack(text, text, text, jsonb, text, text, integer, timestamptz)';
export const LOCK_SUPPLIERS_FUNCTION = 'core.lock_suppliers_for_share(bigint[])';

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };
const lit = (s) => `'${String(s).replace(/'/g, "''")}'`;
const newPassword = () => crypto.randomBytes(24).toString('base64url');

/** 流す文の一覧 (パスワードは引数)。pw = { master_edit, master_ops, master_observer, master_gate } */
export function masterEditRoleStatements({ dbName, pw }) {
  const db = ident(dbName);
  const s = [];
  for (const role of MASTER_EDIT_ROLES) {
    s.push(`do $$ begin if not exists (select 1 from pg_roles where rolname = '${role}') then create role ${role}; end if; end $$`);
    // 🚨 nosuperuser などは書かない (create-watch-roles.mjs と同じ理由)。作った後に pg_roles で確かめる
    s.push(`alter role ${role} with login password ${lit(pw[role])} nocreaterole noinherit connection limit ${CONN_LIMIT[role]}`);
    s.push(`alter role ${role} set statement_timeout = '20s'`);
    s.push(`grant connect on database ${db} to ${role}`);
  }
  // 前に付けた権限を外してから付け直す (流し直しで広い権限が残らない)
  for (const t of new Set([...MASTER_EDIT_SELECT, ...MASTER_EDIT_WRITE.map(([, t2]) => t2), 'events.master_change_events', 'ops.master_legacy_gate_acks', 'ops.master_legacy_manifests', 'ops.master_cutover_events'])) {
    s.push(`revoke all on ${t} from master_edit, master_ops, master_observer, master_gate`);
  }
  for (const f of [CUTOVER_FUNCTION, OBSERVE_FUNCTION, ACK_FUNCTION, LOCK_SUPPLIERS_FUNCTION]) s.push(`revoke all on function ${f} from public, master_edit, master_ops, master_observer, master_gate`);
  // master_edit
  for (const sc of ['core', 'ops', 'events']) s.push(`grant usage on schema ${sc} to master_edit`);
  for (const t of MASTER_EDIT_SELECT) s.push(`grant select on ${t} to master_edit`);
  for (const [priv, t] of MASTER_EDIT_WRITE) s.push(`grant ${priv} on ${t} to master_edit`);
  s.push(`grant execute on function ${LOCK_SUPPLIERS_FUNCTION} to master_edit`);
  s.push('grant usage on sequence core.master_version_seq to master_edit');   // version の既定値・0026 の bump_master_version の nextval (呼び手の権限)
  // master_ops
  s.push('grant usage on schema ops to master_ops');
  for (const t of ['ops.master_cutover_state', 'ops.master_cutover_events', 'ops.master_legacy_gate_acks', 'ops.master_legacy_manifests']) s.push(`grant select on ${t} to master_ops`);
  s.push(`grant execute on function ${CUTOVER_FUNCTION} to master_ops`);
  // master_observer
  s.push('grant usage on schema ops to master_observer');
  s.push(`grant execute on function ${OBSERVE_FUNCTION} to master_observer`);
  // master_gate
  s.push('grant usage on schema ops to master_gate');
  s.push('grant select on ops.master_cutover_state to master_gate');
  s.push(`grant execute on function ${ACK_FUNCTION} to master_gate`);
  return s;
}

/** ロールを作る / 権限をそろえる (1 取引)。commit の前に属性を確かめ、違えば取り消す。pw を省いたロールは新しいパスワード */
export async function createMasterEditRoles(client, { pw = {}, dryRun = false } = {}) {
  const info = (await client.query(`select current_user as owner, current_database() as db, r.rolcreaterole, r.rolsuper from pg_roles r where r.rolname = current_user`)).rows[0];
  if (!info.rolcreaterole && !info.rolsuper) throw new Error(`${info.owner} に CREATEROLE が無い = ロールを作れない`);
  const pws = Object.fromEntries(MASTER_EDIT_ROLES.map((r) => [r, pw[r] ?? (dryRun ? `<${r} の新しいパスワード>` : newPassword())]));
  const stmts = masterEditRoleStatements({ dbName: info.db, pw: pws });
  if (dryRun) return { info, stmts, roles: [], pw: pws };
  await client.query('begin');
  try {
    for (const s of stmts) await client.query(s);
    const roles = (await client.query(`select r.rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication, r.rolcanlogin, r.rolinherit, r.rolconnlimit,
        coalesce((select string_agg(g.rolname, ',' order by g.rolname) from pg_auth_members m join pg_roles g on g.oid = m.roleid where m.member = r.oid), '') as memberships
      from pg_roles r where r.rolname = any($1::text[]) order by r.rolname`, [MASTER_EDIT_ROLES])).rows;
    const bad = [];
    for (const r of roles) {
      const why = [];
      if (r.rolsuper) why.push('superuser'); if (r.rolcreaterole) why.push('createrole'); if (r.rolcreatedb) why.push('createdb'); if (r.rolbypassrls) why.push('bypassrls'); if (r.rolreplication) why.push('replication');
      if (!r.rolcanlogin) why.push('login できない'); if (r.rolinherit) why.push('inherit'); if (Number(r.rolconnlimit) !== CONN_LIMIT[r.rolname]) why.push(`connection limit ${r.rolconnlimit}`);
      if (r.memberships) why.push(`ほかのロールのメンバー (${r.memberships})`);
      if (why.length) bad.push(`${r.rolname}: ${why.join(' / ')}`);
    }
    if (roles.length !== MASTER_EDIT_ROLES.length) bad.push(`ロールが ${roles.length} 件しか無い`);
    if (bad.length) throw new Error(`ロールの属性が期待と違う (取り消した): ${bad.join(' ; ')}`);
    await client.query('commit');
    return { info, stmts, roles, pw: pws };
  } catch (e) { try { await client.query('rollback'); } catch { /* */ } throw e; }
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const base = process.env.COMPANY_DB_URL;
  if (!base) throw new Error('COMPANY_DB_URL が要る (node -r dotenv/config …)');
  const client = await openPgClient(base);
  try {
    const r = await createMasterEditRoles(client, { dryRun });
    if (dryRun) { for (const s of r.stmts) console.log(s.replace(/password '[^']*'/, "password '***'") + ';'); return; }
    console.log(`✅ ${MASTER_EDIT_ROLES.join(' / ')} を作った / 権限をそろえた (owner=${r.info.owner} db=${r.info.db})`);
    console.log('環境変数に足す (パスワードはこの画面にしか出ない):');
    console.log(`COMPANY_DB_MASTER_EDIT_URL=${urlFor(base, 'master_edit', r.pw.master_edit)}      # Render (apps/master-edit)`);
    console.log(`COMPANY_DB_MASTER_OPS_URL=${urlFor(base, 'master_ops', r.pw.master_ops)}        # 切替の段階を進める手の操作`);
    console.log(`COMPANY_DB_MASTER_OBSERVER_URL=${urlFor(base, 'master_observer', r.pw.master_observer)}  # ⑤-2 の夜間ロード (NE の観測)`);
    console.log(`COMPANY_DB_MASTER_GATE_URL=${urlFor(base, 'master_gate', r.pw.master_gate)}      # ⑤-3 の古い入口の門 (Render と miniPC)`);
  } finally { await client.end(); }
}

const isMain = process.argv[1] && /create-master-edit-roles\.mjs$/i.test(process.argv[1]);
if (isMain) main().then(() => { process.exitCode = 0; }).catch((e) => { console.error(`❌ ${e.message}`); process.exitCode = 1; });
