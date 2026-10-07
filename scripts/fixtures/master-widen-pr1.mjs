/**
 * master-widen-pr1.mjs — 試験だけ: 広げる道 PR-2 (画面が DB の active に従う・二層の門) の試験の道具。**本物の 0058 (PR-1 #1644) の上で** 動く
 *
 * PR-2 は PR-1 (migration 0058) の上に載る (#1644 の今の版)。試験の DB は 0058 まで流す = 本物の関数と表を使う:
 *   - ops.master_ownership_active_map() = SECURITY DEFINER・master_edit に EXECUTE (0058 + create-master-edit-roles.mjs)
 *   - 許可 (lease) = ops.master_new_entry_leases の行・有効かは本物の ops._new_entry_lease_ok (取り消しなし・期限の前・一番新しい結果の行・停止の床より新しい・
 *     区分 skus.sku_kind の持ち主が company)。ops.new_entry_lease_valid / ops.acquire_new_entry_locks も本物
 *   - 保存の強制は本物の DB の関数 (register_new_sku・ne_reg_build・ne_reg_issue の初回が _require_new_entry_lease で拒む)
 * この道具がするのは、本番では毎朝の照合 ② (close → record) と daily-sync の段 (grant) が作る「許可の行」を試験の形で置くことだけ:
 *   - setTestLease: 照合 ② の始めの close と結果の記録は本物の関数・許可の行は印 (ops.lease_protocol) で直接置く
 *     (grant は widen の記録・区分の持ち主 company・今日の結果… を全部見る = 試験の場面ごとにそろえない。有効かは本物の判定に任せる)
 *   - 🚨 本物の grant は single だけ = セットの許可は「本番では出ない」。セットの許可は futureSetLease: true を付けた試験だけ (sku_components を C にした将来の形)
 *   - enforceLeaseInDb = lib の門は通った後に DB だけが拒む競合の形 (P0001「new_entry_closed: …」) を、試験だけの trigger で作る
 *     (本物では許可の共有の鍵を取引の終わりまで持つ = 途中で取り消されない。残るのは期限ちょうど (07:00) をまたぐ取引だけ)
 *   - hideLeaseFunctions = 0058 の前の DB (許可の関数が無い) の形を、本物の関数の名前を一時的に変えて作る (restoreLeaseFunctions で戻す = 実行権もそのまま)
 * 🚨 表の持ち主のロール (deploy = migration を流したロール) で呼ぶ。本番では使わない
 */
import crypto from 'node:crypto';
import { ownershipHash } from '../../lib/master-cutover.mjs';
import { ZERO_GATE } from './master-widen.mjs';

const sorted = (o) => Object.fromEntries(Object.keys(o || {}).sort().map((k) => [k, o[k]]));
const one = async (db, sql, p) => (await db.query(sql, p)).rows[0];
export const LEASE_KINDS = Object.freeze(['single', 'set']);
/** 名前を変えて隠す本物の関数 (0058 の前の DB の形) */
const HIDDEN = Object.freeze([['acquire_new_entry_locks', 'text'], ['new_entry_lease_valid', 'text']]);
const HIDDEN_SUFFIX = '__hidden_pr1640';

/** 本物の 0058 が入っているか (許可の表と関数)。無い = 投げる (PR-2 は PR-1 の上だけ) */
export async function assertReal0058(db) {
  const r = await one(db, `select to_regclass('ops.master_new_entry_leases') is not null and to_regprocedure('ops.record_new_entry_gate(text,text,text,jsonb)') is not null as ok`);
  if (r.ok !== true) throw new Error('試験の DB に 0058 (広げる道 PR-1) が無い = PR-2 の試験は PR-1 の上だけで流す');
}

/**
 * PR-2 の試験の準備 (本物の 0058 の上)。leases = 置いておく許可の種類 (既定 = なし = 本番の今)。セットは futureSetLease: true のときだけ。
 * 🚨 照合 ② の始めの close は許可を全部取り消す = この後に seedNewEntryLease (PR-1 の fixture) を呼ぶとセットの許可は消える (withSet: true で置き直す)
 */
export async function useReal0058(db, { leases = [], futureSetLease = false } = {}) {
  await assertReal0058(db);
  if (leases.includes('set') && !futureSetLease) throw new Error('セットの許可は本物 (0058) ではまだ出ない = 将来の形の試験だけ (futureSetLease: true)');
  for (const k of leases) await setTestLease(db, k, true, { futureSetLease });
  return { lease: 'real' };
}

/** 種類ごとに「生きている許可の行」があるか (取り消しなし・期限の前・一番新しい結果の行・停止の床より新しい)。区分の持ち主は見ない (それは本物の判定の残り) */
export async function liveLeaseKinds(db) {
  const rows = (await db.query(`select k.kind,
      exists (select 1 from ops.master_new_entry_leases l
               where l.lease_id = (select max(x.lease_id) from ops.master_new_entry_leases x where x.kind = k.kind)
                 and l.revoked_at is null and clock_timestamp() < l.expires_at
                 and l.result_id = (select max(r.result_id) from ops.new_entry_gate_results r)
                 and l.result_id > coalesce((select max(f.floor_result_id) from ops.master_new_entry_stop_floors f where f.kind = k.kind), 0)) as live
    from unnest($1::text[]) as k(kind)`, [LEASE_KINDS])).rows;
  return rows.filter((r) => r.live === true).map((r) => r.kind);
}

/** 許可の行を直接置く (本物の印 ops.lease_protocol を立てて・1 つの取引)。result = 結果の行 { result_id, compare_run_id } */
async function insertLeases(db, kinds, result, actor) {
  if (!kinds.length) return;
  await db.query('begin');
  try {
    await db.query("select set_config('ops.lease_protocol', '1', true)");
    for (const k of kinds) {
      await db.query(`insert into ops.master_new_entry_leases (kind, result_id, compare_run_id, granted_by, expires_at)
        values ($1, $2::bigint, $3, $4, ops.new_entry_lease_expiry(clock_timestamp()))`, [k, result.result_id, result.compare_run_id, actor]);
    }
    await db.query("select set_config('ops.lease_protocol', '', true)");
    await db.query('commit');
  } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
}

/**
 * 許可を置く / 外す (本物の表と関数)。valid = true:
 *   今の一番新しい結果の行が停止の床より新しい = その行の許可を足す / そうでない = 本番と同じ順 (close → record) で新しい結果の行を作り、ほしい種類全部の許可を置く。
 *   結果の回 = 今の NE のコードの印の回 (まだ結果が無ければ = 配るの「許可の回 = NE のコードの回」が通る)・無ければ試験の回。
 * valid = false: その種類の許可を取り消す (本物の印で)。ほかの種類はそのまま
 */
export async function setTestLease(db, kind, valid, { futureSetLease = false, actor = 'test-pr1640' } = {}) {
  if (!LEASE_KINDS.includes(kind)) throw new Error(`知らない種類: ${kind}`);
  if (kind === 'set' && valid === true && !futureSetLease) throw new Error('セットの許可は本物 (0058) ではまだ出ない = 将来の形の試験だけ (futureSetLease: true)');
  await assertReal0058(db);
  const live = await liveLeaseKinds(db);
  if (valid !== true) {
    if (!live.includes(kind)) return;
    await db.query('begin');
    try {
      await db.query("select set_config('ops.lease_protocol', '1', true)");
      await db.query(`update ops.master_new_entry_leases set revoked_at = clock_timestamp(), revoke_reason = '試験: 許可を外す', revoked_by = $2 where kind = $1 and revoked_at is null`, [kind, actor]);
      await db.query("select set_config('ops.lease_protocol', '', true)");
      await db.query('commit');
    } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
    return;
  }
  if (live.includes(kind)) return;
  const latest = await one(db, `select r.result_id::text as result_id, r.compare_run_id from ops.new_entry_gate_results r order by r.result_id desc limit 1`);
  const floor = latest ? (await one(db, 'select coalesce(max(floor_result_id), 0)::text as f from ops.master_new_entry_stop_floors where kind = $1', [kind])).f : null;
  if (latest && BigInt(latest.result_id) > BigInt(floor)) { await insertLeases(db, [kind], latest, actor); return; }
  // 新しい結果の行 (本番と同じ順: 照合 ② の始めに閉じる → 結果)。close は全部の種類の許可を取り消す = ほしい種類全部を置き直す
  const has = async (run) => (await one(db, 'select exists (select 1 from ops.new_entry_gate_results where compare_run_id = $1) as e', [run])).e;
  const mark = (await one(db, 'select compare_run_id from ops.master_ne_code_mark where id = 1'))?.compare_run_id ?? null;
  const run = mark && !(await has(mark)) ? mark : `fixture_pr1640_${crypto.randomBytes(4).toString('hex')}`;
  const at = new Date(Date.now() - 1000).toISOString();
  await db.query('select ops.close_new_entry_for_compare($1)', [run]);
  const r = (await one(db, 'select ops.record_new_entry_gate($1, $2, $2, $3::jsonb) as r', [run, at, JSON.stringify(ZERO_GATE)])).r;
  await insertLeases(db, [...new Set([...live, kind])], { result_id: r.result_id, compare_run_id: run }, actor);
}

/**
 * DB の強制の形 (試験だけ): 新規開始の書き込みの約束 (ops.master_write_sessions の sku_create・reg_csv_build・reg_csv_issue) を
 * 本物の 0058 と同じ形の誤り (P0001・「new_entry_closed: …」) で拒む。lib の門は通った後に DB だけが拒む形 (本物では期限ちょうどをまたぐ取引)。
 * closed = false で外す (trigger を消す)
 */
export async function enforceLeaseInDb(db, { closed = true } = {}) {
  await db.query('drop trigger if exists trg_test_require_new_entry_lease on ops.master_write_sessions');
  if (!closed) { await db.query('drop function if exists ops.test_require_new_entry_lease()'); return; }
  await db.query(`create or replace function ops.test_require_new_entry_lease() returns trigger language plpgsql as $$
    begin
      if new.operation in ('sku_create', 'reg_csv_build', 'reg_csv_issue') then
        raise exception 'new_entry_closed: 新商品の開放の許可が無い (試験の DB の強制・%)', new.operation using errcode = 'P0001';
      end if;
      return new;
    end $$`);
  await db.query('create trigger trg_test_require_new_entry_lease before insert on ops.master_write_sessions for each row execute function ops.test_require_new_entry_lease()');
}

/** 0058 の前の DB の形 = 許可の関数・新規開始の鍵の関数が無い。本物の関数の名前を変えて隠す (restoreLeaseFunctions で戻す) */
export async function hideLeaseFunctions(db) {
  for (const [name, args] of HIDDEN) {
    if ((await one(db, 'select to_regprocedure($1) is not null as ok', [`ops.${name}(${args})`])).ok) await db.query(`alter function ops.${name}(${args}) rename to ${name}${HIDDEN_SUFFIX}`);
  }
}
export async function restoreLeaseFunctions(db) {
  for (const [name, args] of HIDDEN) {
    if ((await one(db, 'select to_regprocedure($1) is not null as ok', [`ops.${name}${HIDDEN_SUFFIX}(${args})`])).ok) await db.query(`alter function ops.${name}${HIDDEN_SUFFIX}(${args}) rename to ${name}`);
  }
}
export async function revokeActiveMap(db, role = 'master_edit') {
  await db.query(`revoke execute on function ops.master_ownership_active_map() from ${role}`);
}
export async function grantActiveMap(db, role = 'master_edit') {
  await db.query(`grant execute on function ops.master_ownership_active_map() to ${role}`);
}

/**
 * widen の代わり: 段階は new_open のまま、DB の active と段階の記録 (owner_hash) を 1 つの取引で付け替える (PR-1 の ops.widen_master_ownership と同じ結果)。
 * 本物の守り (0051 の段階の守り = 印の GUC・0055 の段階 = active の trigger・0058 の G5 = 印 ops.widen_protocol) を通る形で書く (守りは弱めない)
 */
export async function setActiveOwnershipInDb(db, map) {
  const m = sorted(map);
  const h = ownershipHash(m);
  await db.query('begin');
  try {
    await db.query(`select set_config('ops.widen_protocol', '1', true)`);   // 0058 の G5 (new_open では 0058 の関数でだけ変える) を試験の置き換えとして通す (PR-1 の fixture と同じ)
    await db.query(`update ops.master_ownership_state set active_hash = $1, active_map = $2::jsonb, activated_at = now(), activated_by = 'test-widen', updated_at = now() where id = 1`, [h, JSON.stringify(m)]);
    await db.query(`select set_config('ops.cutover_protocol', '1', true)`);
    await db.query(`update ops.master_cutover_state set owner_hash = $1 where id = 1`, [h]);
    await db.query('commit');
  } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
  return h;
}
/** DB の active の持ち主表だけを書き換える (段階の記録はそのまま = 「配った config と DB が違う」・code_behind の試験) */
export async function setActiveMapOnly(db, map) {
  const m = sorted(map);
  await db.query('begin');
  try {
    await db.query(`select set_config('ops.widen_protocol', '1', true)`);   // 0058 の G5 を試験の置き換えとして通す
    await db.query(`update ops.master_ownership_state set active_hash = $1, active_map = $2::jsonb, updated_at = now() where id = 1`, [ownershipHash(m), JSON.stringify(m)]);
    await db.query('commit');
  } catch (e) { try { await db.query('rollback'); } catch { /* */ } throw e; }
}

/**
 * 画面のロールの ops.ne_reg_exports の SELECT を「file_bytes 以外の列」にする (hide = true = 本物の 0058 の ops.restrict_ne_reg_file_bytes と同じ) /
 * 表ごとに戻す (hide = false = readiness が「file_bytes を直接読める」を見つける試験だけ)
 */
export async function hideRegFileBytes(db, hide = true, role = 'master_edit') {
  if (!(await db.query('select 1 from pg_roles where rolname = $1', [role])).rows.length) return;
  if (hide) { await db.query('select ops.restrict_ne_reg_file_bytes()'); return; }
  await db.query(`grant select on ops.ne_reg_exports to ${role}`);
}
