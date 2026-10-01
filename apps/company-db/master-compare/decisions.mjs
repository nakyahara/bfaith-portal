/**
 * decisions.mjs — 照合 ② の判断の台帳 (Company DB の ops.master_decision_*。migration 0032。Company DB構想 10 §6.1.1「D1 判断の台帳の契約 v3」)
 *
 * 読む (照合の読み取りの取引の中・watcher): 指紋ごとの最新の判断 (approved / rejected / revoked) と、完了した approved の出来事
 * 書く (取引の後・watch_writer): ops.record_decision_candidates (候補と観測) / ops.record_decision_done (直す承認の完了)。表へ直接は書かない
 * 🚨 読めない = 「承認なし」と読まない (state = unreadable → 照合 ② は blocked)。表が無い (0032 の前) = not_applied (今までどおり)
 */
import { openPgClient, pgAdapter } from '../../../scripts/company-db/migrate.mjs';

/**
 * @param {{ query: Function }} db  照合の読み取りの取引の中の接続
 * @returns {{ state: 'ok'|'not_applied'|'unreadable', latest?: Map, done?: Map, reason?: string }}
 */
export async function readDecisionLedger(db) {
  await db.query('savepoint decision_ledger');
  try {
    const exists = (await db.query("select to_regclass('ops.master_decision_events') is not null as ok")).rows[0].ok;
    if (!exists) { await db.query('release savepoint decision_ledger'); return { state: 'not_applied' }; }
    const latest = new Map();
    for (const r of (await db.query(`select distinct on (fingerprint) fingerprint, event_id, kind, resolution, target
        from ops.master_decision_events where kind in ('approved', 'rejected', 'revoked') order by fingerprint, event_id desc`)).rows) {
      latest.set(r.fingerprint, { event_id: Number(r.event_id), kind: r.kind, resolution: r.resolution, target: r.target });
    }
    const done = new Map();
    for (const r of (await db.query(`select approved_event_id, created_at::text as at from ops.master_decision_events where kind = 'action_done'`)).rows) done.set(Number(r.approved_event_id), r.at);
    await db.query('release savepoint decision_ledger');
    return { state: 'ok', latest, done };
  } catch (e) {
    try { await db.query('rollback to savepoint decision_ledger'); } catch { /* */ }
    return { state: 'unreadable', reason: String(e && e.message).slice(0, 200) };
  }
}

/** 候補の形 (全件 JSON の ne.decisions の 1 件 → 関数に渡す形。再計算しない = JSON の中身そのまま) */
export const candidateOf = (d) => ({ fingerprint: d.fingerprint || d.approval_fingerprint, subject_key: d.subject_key, code_norm: d.code_norm || d.norm, col: d.col, child: d.child ?? null,
  cls: d.cls, reason_kind: d.reason_kind, semantic: d.semantic || (d.print && d.print.semantic) || null, print: d.print, resolutions: d.resolutions, proposal: d.proposal ?? null });

/**
 * 候補と完了を書く (writer = watch_writer の接続)。戻り値 = { candidates, done, doneSkipped }
 * @param {{ query: Function }} writer
 * @param {{ compareRunId: string, observedAt: string, decisions: object[], done: object[] }} p
 */
export async function writeDecisions(writer, { compareRunId, observedAt, decisions, done = [] }) {
  const cands = decisions.map(candidateOf).filter((c) => c.fingerprint && c.print && Array.isArray(c.resolutions));
  const n = (await writer.query('select ops.record_decision_candidates($1::jsonb) as n', [JSON.stringify({ compare_run_id: compareRunId, observed_at: observedAt, decisions: cands })])).rows[0].n;
  let written = 0, skipped = 0;
  for (const d of done) {
    const ok = (await writer.query('select ops.record_decision_done($1::bigint, $2::text, $3::jsonb) as ok', [d.approved_event_id, compareRunId, JSON.stringify(d.observed ?? {})])).rows[0].ok;
    if (ok) written++; else skipped++;
  }
  return { candidates: Number(n), done: written, doneSkipped: skipped };
}

/**
 * NE のコードの元の書き方を書く (③b-1b 契約 v3。writer = watch_writer の接続・関数だけ)。1 回 = 1 つの取引で全部を入れ替えて印を進める。
 * 同じ回の再送で中身が同じ = unchanged / 古い回・中身が違う = 関数が拒む (呼び手は失敗として残すだけ)
 */
export async function writeNeCodes(writer, { compareRunId, entries }) {
  const r = (await writer.query('select ops.record_ne_codes($1::jsonb) as r', [JSON.stringify({ compare_run_id: compareRunId, entries })])).rows[0].r;
  return typeof r === 'string' ? JSON.parse(r) : r;
}

/**
 * 新商品の NE 登録の CSV の確かめ待ちの商品 (0052 の ops.v_ne_reg_targets)。照合の読み取りの取引の中 (watcher)。
 * 表が無い (0052 の前) = null (送らない)・読めない = null + reason
 */
export async function readRegTargets(db) {
  await db.query('savepoint reg_targets');
  try {
    const exists = (await db.query("select to_regclass('ops.v_ne_reg_targets') is not null as ok")).rows[0].ok;
    const rows = exists ? (await db.query('select distinct code_norm, sku_kind from ops.v_ne_reg_targets order by code_norm')).rows : null;
    await db.query('release savepoint reg_targets');
    return { state: exists ? 'ok' : 'not_applied', targets: rows };
  } catch (e) {
    try { await db.query('rollback to savepoint reg_targets'); } catch { /* */ }
    return { state: 'unreadable', targets: null, reason: String(e && e.message).slice(0, 200) };
  }
}
const jsonOf = (r) => (typeof r === 'string' ? JSON.parse(r) : r);
/**
 * 新商品の NE 登録の CSV の確かめ (0052・#1571 Codex R1 High 2) = 3 段。どれも writer = watch_writer・関数だけ・1 回 = 1 つの取引。
 *   1. writeRegistrationObservations = この回の NE の観測 (取得の世代・時刻・原本のハッシュ・確かめ待ちの商品・観測) を DB に残す (回ごとに 1 回・後から足せない)
 *   2. sealRegistrationRun = この回が最後まで終わった受け取り (観測のハッシュ・結果の JSON の sha256)。結果の JSON を書けた後だけ
 *   3. runRegistrationCheck = 回の番号だけを渡す。DB の関数が受け取りと残した観測を自分で読んで確かめる (呼び手の観測の JSON は受けない)
 * regObs = compareNe の regObs ({ fetch, products_at, sets_at, absence_trusted, targets, observations })
 */
export async function writeRegistrationObservations(writer, { compareRunId, regObs }) {
  return jsonOf((await writer.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: compareRunId, ...regObs })])).rows[0].r);
}
export async function sealRegistrationRun(writer, { compareRunId, observationHash, evidenceSha256 }) {
  return jsonOf((await writer.query('select ops.seal_ne_registration_run($1, $2, $3) as r', [compareRunId, observationHash, evidenceSha256])).rows[0].r);
}
export async function runRegistrationCheck(writer, { compareRunId }) {
  return jsonOf((await writer.query('select ops.record_ne_registration_check($1) as r', [compareRunId])).rows[0].r);
}

/** 本番の書く接続 (watch_writer。読み取り専用にはしない)。初期設定に失敗したら閉じてから投げる */
export async function connectDecisionWriter(url) {
  const client = await openPgClient(url);
  try { await client.query(`set statement_timeout = '60s'`); } catch (e) { try { await client.end(); } catch { /* */ } throw e; }
  return { db: pgAdapter(client), close: () => client.end() };
}
