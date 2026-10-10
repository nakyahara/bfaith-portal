/**
 * master-widen.mjs — 試験だけ: 広げる道 (0058) の試験の準備 (段階を new_open にする・fake のロードの記録を置く)
 *
 * なぜ: 広げる道は段階 new_open で動く。本番は切替の日の手順 (17 §4.2) で new_open になる = 試験で毎回その手順を流さない。
 *   ここで「new_open・持ち主の epoch が active」を直接置く (0051 の印 ops.cutover_protocol を立てて 1 つの取引で。backfill (0052 の前提) は本物の関数)。
 *   fake のロード = ops.ingest_runs・ops.load_materials・ops.load_decisions・ops.master_load_commits の行だけを置く (widen の判定の材料・(Y) の記録の形)
 * 🚨 試験だけ。本番の DB には使わない
 */
import crypto from 'node:crypto';
import { seedActiveEpoch } from './master-epoch.mjs';
import { ownershipHash } from '../../lib/master-cutover.mjs';

const one = async (db, sql, p) => (await db.query(sql, p)).rows[0];

/** 段階を new_open にする (map = active の持ち主表)。backfill は本物の関数。db = { query } */
export async function forceNewOpen(db, map, { actor = 'test' } = {}) {
  await db.query('begin');
  try {
    await db.query("select set_config('ops.cutover_protocol', '1', true)");
    await db.query("update ops.master_cutover_state set phase = 'frozen', owner_hash = null, changed_by = $1 where id = 1", [actor]);
    await db.query('commit');
  } catch (e) { await db.query('rollback'); throw e; }
  const p = await one(db, 'select * from ops.registration_backfill_plan()');
  if (!(await one(db, 'select exists (select 1 from ops.master_registration_backfill) as e')).e) {
    await db.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, actor]);
  }
  await seedActiveEpoch(db, map);
  await db.query('begin');
  try {
    await db.query("select set_config('ops.cutover_protocol', '1', true)");
    await db.query("update ops.master_cutover_state set phase = 'new_open', owner_hash = $1, changed_by = $2 where id = 1", [ownershipHash(map), actor]);
    await db.query('commit');
  } catch (e) { await db.query('rollback'); throw e; }
}

/** 照合 ② の区分のゲートの 5 つの数が全部 0 (許可が出る形) */
export const ZERO_GATE = Object.freeze({ raw_mismatch: 0, raw_unverifiable_affected_existing_cdb: 0, norm_collision: 0, unknown_kind: 0, integrity_untrusted: 0 });

/**
 * 新商品 (単品) の開放の許可を置く (0058 の後・段階 new_open・区分の持ち主が company の DB)。本番 = widen → 翌朝の照合 ② の結果 → daily-sync の次の段の grant。
 *   試験は「広げた試み」を印 (ops.widen_protocol) で直接置き (無ければ)、本物の関数で照合 ② の結果 (5 つの数が 0) を残し、本物の関数で許可を出す。
 *   結果の回 = 今の NE のコードの印の回 (ops.master_ne_code_mark・まだ結果が無ければ) = 配る (ne_reg_issue) の「許可の回 = NE のコードの回」が通る。runId で指定もできる。
 *   0058 の前の DB = 何もしない (null)。runId の結果がもうある = 何もしない (null)。戻り値 = 許可 ({ lease_id, result_id, ... })。withSet = セットの許可も直接置く (試験だけ)
 */
export async function seedNewEntryLease(db, { actor = 'test', runId = null, kindGate = ZERO_GATE, withSet = false } = {}) {
  if ((await one(db, `select to_regprocedure('ops.grant_new_entry_lease(text, text)') is not null as ok`)).ok !== true) return null;
  if (!(await one(db, "select exists (select 1 from ops.master_widen_attempts where state = 'widened') as e")).e) {
    const mf = JSON.stringify({ entries: [{ id: 'fixture:new-entry-lease', kind: 'code' }] });
    await db.query('begin');
    try {
      await db.query('insert into ops.master_legacy_manifests (manifest_hash, entries) values (ops.legacy_manifest_hash($1::jsonb), $1::jsonb) on conflict do nothing', [mf]);
      await db.query("select set_config('ops.widen_protocol', '1', true)");
      await db.query(`insert into ops.master_widen_attempts (widen_prepare_id, company_id, added_keys, active_hash, prepared_hash, prepared_map, prepared_at, base_commit_seq,
          loader_fingerprint, manifest_hash, prepared_by, state, closed_at, closed_by)
        values (gen_random_uuid(), 1, array['skus.sku_kind'], $1, $1, '{}'::jsonb, clock_timestamp() - interval '2 minutes', 0, $1, ops.legacy_manifest_hash($2::jsonb), $3,
          'widened', clock_timestamp() - interval '1 minute', $3)`, [hex('a'), mf, actor]);
      await db.query("select set_config('ops.widen_protocol', '', true)");
      await db.query('commit');
    } catch (e) { await db.query('rollback'); throw e; }
  }
  const has = async (run) => (await one(db, 'select exists (select 1 from ops.new_entry_gate_results where compare_run_id = $1) as e', [run])).e;
  if (runId && await has(runId)) return null;
  const mark = (await one(db, 'select compare_run_id from ops.master_ne_code_mark where id = 1'))?.compare_run_id ?? null;
  const run = runId || (mark && !(await has(mark)) ? mark : `fixture_${crypto.randomBytes(4).toString('hex')}`);
  const at = new Date(Date.now() - 1000).toISOString();
  await db.query('select ops.close_new_entry_for_compare($1)', [run]);   // 本番と同じ: 照合 ② の始めに閉じる → 結果 → grant (今回の回)
  await db.query('select ops.record_new_entry_gate($1, $2, $2, $3::jsonb)', [run, at, JSON.stringify(kindGate)]);
  const lease = (await one(db, "select ops.grant_new_entry_lease('single', $1) as r", [run])).r;
  // 🆕 0068: 同じ照合の回の代表 (親) の数え (NE = Company DB の観測 = 0) も残す (products.parent が company の DB で新しい NE 登録の CSV の門が開く)
  await seedParentGate(db, { runId: run });
  // withSet = セットの CSV の 0053 の道を試験するためだけに、同じ結果の行のセットの許可を直接置く (本番には出す道が無い = grant は single だけ・#1644 Codex R1 Medium 2)
  if (withSet) {
    await db.query('begin');
    try {
      await db.query("select set_config('ops.lease_protocol', '1', true)");
      await db.query(`insert into ops.master_new_entry_leases (kind, result_id, compare_run_id, granted_by, expires_at)
        select 'set', r.result_id, r.compare_run_id, $1, ops.new_entry_lease_expiry(clock_timestamp()) from ops.new_entry_gate_results r where r.compare_run_id = $2`, [actor, run]);
      await db.query("select set_config('ops.lease_protocol', '', true)");
      await db.query('commit');
    } catch (e) { await db.query('rollback'); throw e; }
  }
  return lease;
}

/**
 * 🆕 0068: 照合 ② の代表 (親) の数えの記録を 1 行置く (本物の関数 ops.record_parent_gate)。0068 の前の DB = 何もしない (null)。同じ回がもうある = 何もしない (null)。
 *   obs = NE の観測 (無ければ「NE = 今の Company DB」= 単品の代表を Company DB の親から作る = 数え 0)。戻り値 = 関数の答え
 *   本番 = 照合 ② (run.mjs) が NE の完全な取得から作った観測を、封をした回 (結果の JSON の sha256) で残す
 */
export async function seedParentGate(db, { runId, obs = null, materialGenerationId = 'mat_fixture', at = null } = {}) {
  if ((await one(db, `select to_regprocedure('ops.record_parent_gate(text, jsonb, text, text, jsonb)') is not null as ok`)).ok !== true) return null;
  if ((await one(db, 'select exists (select 1 from ops.master_parent_gate_results where compare_run_id = $1) as e', [runId])).e) return null;
  const mirror = async () => {
    const rows = (await db.query(`select k.code_norm, k.sku_kind, nullif(core.norm_code(pp.display_code), '') as rep, pp.display_code as raw
        from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
       where k.company_id = 1 and k.sku_kind in ('single', 'set') order by k.code_norm`)).rows;
    return { format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' },
      trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' },   // 🆕 #1676 Codex R4: 照合の許可の一覧 (全部 ok)
      rows: rows.map((r) => (r.sku_kind === 'set' ? [r.code_norm, 'set', null, null, null] : [r.code_norm, 'single', 'ok', r.rep ?? null, r.rep ? r.raw : null])) };
  };
  const when = at || new Date(Date.now() - 1000).toISOString();
  const fetch = { generation_id: `ne_fixture_${crypto.randomBytes(3).toString('hex')}`, raw_hash: hex('c'), products_complete_at: when, setproducts_complete_at: when };
  return (await one(db, 'select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb) as r',
    [runId, JSON.stringify(fetch), materialGenerationId, hex('d'), JSON.stringify(obs || await mirror())])).r;
}

export const hex = (c) => c.repeat(64);
export const nowIso = (offsetMs = 0) => new Date(Date.now() + offsetMs).toISOString().replace(/\.\d{3}Z$/, 'Z');

/**
 * fake のロードを 1 回置く (取引の外で呼ぶ = 自分で commit)。戻り値 = { runId, commitSeq (文字) }
 * opts: epoch ('active' | 'prepared')・hash (持ち主のハッシュ)・gen (世代 ID)・pHash / sHash (中身のハッシュ)・completeAt (取得の完了の時刻の文字)・
 *       fingerprint (規則の指紋)・skuKind (load_decisions の payload.sku_kind・undefined = 正しい空の形・null = 載せない)・status
 */
export async function fakeLoad(db, { epoch, hash, gen = 'mat_20301010T000000000Z_aaaaaaaa_aaaaaa', pHash = hex('a'), sHash = hex('b'), completeAt = nowIso(-60000),
  setCompleteAt = undefined, fingerprint = hex('f'), skuKind = undefined, status = 'matched', runId = `wl_${crypto.randomBytes(5).toString('hex')}` }) {
  await db.query('begin');
  try {
    await db.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, source_tz, format_version)
      values ($1, 'sqlite_initial_load', 'products', 'render', 'test', now(), now(), 'success', true, 'UTC', 'plan-v3')`, [runId]);
    for (const [entity, h, at] of [['products', pHash, completeAt], ['set_components', sHash, setCompleteAt === undefined ? completeAt : setCompleteAt]]) {
      const matched = status === 'matched';
      await db.query(`insert into ops.load_materials (ingest_run_id, entity, status, content_hash, row_count, generation_id, source_complete_at, generation_created_at, rule_version, ownership_hash, rule_fingerprint)
        values ($1, $2, $3, $4, 1, $5, $6, $7, 'v1', $8, $9)`, [runId, entity, status, h, matched ? gen : null, matched ? at : null, matched ? at : null, hash, fingerprint]);
    }
    const sk = skuKind === undefined ? { format: 'sku-kind-v1', held: [], unverifiable: [] } : skuKind;
    await db.query(`insert into ops.load_decisions (ingest_run_id, section, format, payload) values ($1, 'skus', 'ld-v1', $2::jsonb)`,
      [runId, JSON.stringify({ accepted: 1, skipped: [], ...(sk === null ? {} : { sku_kind: sk }) })]);
    const c = await one(db, `insert into ops.master_load_commits (ingest_run_id, epoch, ownership_hash, host) values ($1, $2, $3, 'test') returning commit_seq::text as seq`, [runId, epoch, hash]);
    await db.query('commit');
    return { runId, commitSeq: c.seq };
  } catch (e) { await db.query('rollback'); throw e; }
}
