/**
 * master-epoch.mjs — 試験だけ: ④a の持ち主の epoch (0055 の ops.master_ownership_state の active) を置く
 *
 * なぜ: 0055 から、切替の段階を company_owner・new_open に進めるのは「持ち主の epoch が active で、段階の持ち主表 (owner_hash) と同じ」ときだけ
 *   (⑤-1 の差し込み口の前提・段階の行の trigger)・画面の保存も active と同じ持ち主表だけ (master_write_sessions の trigger)。
 *   本番は切替の日の手順 (readiness → master-ownership-epoch.mjs prepare → frozen → 書きかけ 0 → 最後の active (全部 load) のロード → その run_id の report の成功 + 照合 ② →
 *   --use-prepared のロード → 写し → 作り直し → 確かめ → activate。db/company/README.md・AI_reference 17 §4.2) で active になる。
 *   ⑤ の試験は切替を進めるときにその手順を流さない = ここで「activate 済み」の状態を直接置く (段階を進める直前に。表が無い DB = 何もしない)
 */
import { ownershipHash as hashOf } from '../../lib/master-cutover.mjs';   // 持ち主表のハッシュ = 1 つの式 (load の列は数えない。0055 の ops.ownership_hash と同じ)
const sorted = (o) => Object.fromEntries(Object.keys(o || {}).sort().map((k) => [k, o[k]]));

/** active = ownership (prepared は無し) を置く。db = { query } (pg の adapter / PGlite)。表が無い = false */
export async function seedActiveEpoch(db, ownership) {
  const has = (await db.query(`select to_regclass('ops.master_ownership_state') is not null as ok`)).rows[0].ok === true;
  if (!has) return false;
  const m = sorted(ownership);
  // 0058 (G5): 段階 company_owner / new_open では持ち主の epoch は widen の関数でだけ変わる = 試験の置き換えは同じ印 (ops.widen_protocol) を立てて (表が無い / 0058 の前は効かない)
  const g5 = (await db.query(`select to_regprocedure('ops.guard_master_ownership_widen()') is not null as ok`)).rows[0].ok === true;
  if (g5) await db.query("select set_config('ops.widen_protocol', '1', false)");
  try {
    await db.query(`insert into ops.master_ownership_state (id, active_hash, active_map, activated_by) values (1, $1, $2::jsonb, 'test')
      on conflict (id) do update set active_hash = excluded.active_hash, active_map = excluded.active_map, activated_at = now(), activated_by = 'test',
        prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null, updated_at = now()`, [hashOf(m), JSON.stringify(m)]);
  } finally { if (g5) await db.query("select set_config('ops.widen_protocol', '', false)"); }
  return true;
}

/** 覚えた持ち主表のうち、ハッシュが owner_hash のものを active に置く (段階を進める試験の助け。覚えていない = 何もしない) */
export function epochSeeder(maps = []) {
  const known = new Map(maps.map((m) => [hashOf(m), m]));
  return {
    remember(m) { known.set(hashOf(m), m); return m; },
    async seedFor(db, ownerHash) { const m = known.get(ownerHash); return m ? seedActiveEpoch(db, m) : false; },
  };
}
