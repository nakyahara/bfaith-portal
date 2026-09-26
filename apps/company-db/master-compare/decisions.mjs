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

/** 本番の書く接続 (watch_writer。読み取り専用にはしない)。初期設定に失敗したら閉じてから投げる */
export async function connectDecisionWriter(url) {
  const client = await openPgClient(url);
  try { await client.query(`set statement_timeout = '60s'`); } catch (e) { try { await client.end(); } catch { /* */ } throw e; }
  return { db: pgAdapter(client), close: () => client.end() };
}
