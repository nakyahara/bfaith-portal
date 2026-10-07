/**
 * engine.mjs — 見張りの実行 (評価 → 案件 → 保存 → 1 行の要約)。設計 = 09 §2.2〜§2.3・§7
 *
 *   1. 評価キーを先に確定する (plannedKeys)。結果が 1 件も無い run から「全部正常」を作らない
 *   2. 読み取りは 1 つの snapshot (repeatable read・read only) の中で全部済ませ、閉じてから書く。AI の待ちや HTTP の間に取引を持たない
 *   3. 上流の障害は 1 件にまとめる: depends の前提が pass でない評価キーは blocked (理由に前提の名前) = 同じ障害を何件も出さない
 *   4. 案件 (watch_issues) と通知の状態遷移を分ける: breach が続けば「継続 N 日」で 1 行、pass に戻れば「回復」を 1 回。
 *      blocked の間は案件に触らない (判定保留)。W2 の日付の案件は、窓から外れたら「監視期間外」(回復ではない)
 *   5. 書くのは記録用の接続 (writer)。読むのは照会用 (db)。試験では同じ PGlite でよい
 *   6. 明細は全件で判定した後の抜粋 (上限つき)。案件の管理には使わない
 */
import { plannedKeys, EVALUATORS, addDays, generationOf } from './checks.mjs';

const ICON = { pass: '✅', breach: '⚠️', blocked: '⏸️', execution_error: '❌' };
export const newWatchRunId = (now) => `watch_${now.toISOString().replace(/[-:.TZ]/g, '').slice(0, 17)}_${Math.random().toString(36).slice(2, 8)}`;
const jstDate = (ms) => new Date(ms + 9 * 3600000).toISOString().slice(0, 10);
const byteLen = (o) => Buffer.byteLength(JSON.stringify(o));

/** 明細の抜粋: 影響度順 (payload.weight の大きい順・同順位は subjectKey) で上限まで */
export function pickItems(items, { maxRows, maxBytes }) {
  const sorted = [...items].sort((a, b) => (Number(b.payload && b.payload.weight) || 0) - (Number(a.payload && a.payload.weight) || 0) || String(a.subjectKey).localeCompare(String(b.subjectKey)));
  const out = []; let bytes = 0;
  for (const it of sorted) { const n = byteLen(it); if (out.length >= maxRows || bytes + n > maxBytes) break; out.push(it); bytes += n; }
  return { saved: out, total: items.length, omitted: items.length - out.length, selection: 'weight desc, subject asc' };
}

/**
 * 評価だけ (書かない)。戻り値 = { planned, results, openIssues, deadlineHit }
 * @param {{ query, exec }} db  照会用
 */
export async function evaluateAll({ db, config, asOf, evidence, evidenceHistory = {}, now, log = () => {}, deadlineMs = config.RUN_DEADLINE_MS, syncRunId = null, unbound = false, dataDir = null }) {
  const planned = plannedKeys(config);
  const startMs = Date.now();
  const results = [];
  const byKey = new Map();
  let deadlineHit = false;
  await db.exec('begin isolation level repeatable read read only');
  try {
    // 案件の現状も同じ snapshot で読む (書くのは閉じた後)。0023 が未適用なら案件なしとして評価だけ (書く段で止まる)
    const migrated = (await db.query(`select to_regclass('ops.watch_issues') is not null as ok`)).rows[0].ok;
    const openIssues = !migrated ? [] : (await db.query(`select watch_issue_id, check_id, scope_key, subject_type, subject_key, severity, first_seen_at::text as first_seen_at, last_seen_at::text as last_seen_at, days_seen, transitions
        from ops.watch_issues where company_id = $1::smallint and state = 'open'`, [config.COMPANY_ID])).rows;
    const generation = await generationOf(db, config, asOf, { evidence, dataDir });   // snapshot の中の世代。閉じた後に読み直して比べる (09 §2.1)
    for (const check of config.CHECKS) {
      const keys = planned.filter((k) => k.checkId === check.id);
      if (Date.now() - startMs > deadlineMs) {
        deadlineHit = true;
        for (const k of keys) results.push({ checkId: check.id, checkVersion: check.version, scopeKey: k.scopeKey, subjectType: 'scope', verdict: 'execution_error', severity: check.severity, reason: `全体の期限 (${Math.round(deadlineMs / 1000)} 秒) を超えた`, observed: null, threshold: null, inputGeneration: null, sampleSize: null, items: [], itemTotal: null, periodFrom: null, periodTo: null, durationMs: 0 });
        continue;
      }
      const t0 = Date.now();
      let rs;
      // 🚨 1 つの評価の SQL が失敗しても取引ごと壊さない (Postgres は例外の後、rollback するまで何も受け付けない) → 評価ごとに savepoint
      await db.exec('savepoint chk');
      try { rs = await EVALUATORS[check.id]({ db, config, asOf, evidence, evidenceHistory, now, log, syncRunId, unbound, openIssues, dataDir }, check); await db.exec('release savepoint chk'); }
      catch (e) {
        try { await db.exec('rollback to savepoint chk'); } catch { /* */ }
        log(`${check.id}: 評価に失敗: ${String(e && e.message).slice(0, 200)}`);
        rs = keys.map((k) => ({ checkId: check.id, checkVersion: check.version, scopeKey: k.scopeKey, subjectType: 'scope', verdict: 'execution_error', severity: check.severity, reason: String(e && e.message).slice(0, 300), observed: null, threshold: null, inputGeneration: null, sampleSize: null, items: [], itemTotal: null, periodFrom: null, periodTo: null }));
      }
      const dur = Date.now() - t0;
      const got = new Set(rs.map((r) => r.scopeKey));
      for (const k of keys) if (!got.has(k.scopeKey)) rs.push({ checkId: check.id, checkVersion: check.version, scopeKey: k.scopeKey, subjectType: 'scope', verdict: 'execution_error', severity: check.severity, reason: '評価が結果を返さなかった', observed: null, threshold: null, inputGeneration: null, sampleSize: null, items: [], itemTotal: null, periodFrom: null, periodTo: null });
      for (const r of rs) {
        r.durationMs = r.durationMs ?? Math.round(dur / Math.max(rs.length, 1));
        // 前提 (depends) が pass でなければ blocked = 上流の障害を 1 件にまとめる
        for (const dep of check.depends || []) {
          // 'W1' = 同じ scope の W1 / 'W1:*' = W1 の全部の scope (1 つでも pass でなければ blocked。前提の項目が 1 つも評価されていなければそれも blocked)
          const all = dep.endsWith(':*'), depId = all ? dep.slice(0, -2) : dep;
          const ds = all ? [...byKey.values()].filter((x) => x.checkId === depId) : [byKey.get(`${dep}:${r.scopeKey}`)].filter(Boolean);
          const d = all && !ds.length ? { verdict: 'blocked', reason: '前提の項目が評価されていない', scopeKey: '*' } : ds.find((x) => x.verdict !== 'pass');
          if (d) { r.verdict = 'blocked'; r.reason = `前提 ${depId} (${all ? d.scopeKey : r.scopeKey}) が ${d.verdict}${d.reason ? `: ${d.reason}` : ''}`; r.blockedBy = `${depId}:${all ? d.scopeKey : r.scopeKey}`; break; }
        }
        byKey.set(`${r.checkId}:${r.scopeKey}`, r);
        results.push(r);
      }
    }
    await db.exec('commit');
    return { planned, results, openIssues, deadlineHit, migrated, generation };
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* */ }
    throw e;
  }
}

/**
 * 案件の遷移を決める (書かない)。戻り値 = { inserts: [...], updates: [...], notes: { new, continued, recovered, outOfWindow, held } }
 */
export function reconcileIssues({ config, results, openIssues, asOf, now, holdRecoveries = false }) {
  const nowIso = now.toISOString();
  const open = new Map(openIssues.map((i) => [`${i.check_id}|${i.scope_key}|${i.subject_type}|${i.subject_key}`, i]));
  const touched = new Set();
  const inserts = [], updates = [];
  const notes = { new: [], continued: [], recovered: [], outOfWindow: [], held: [] };
  const checkOf = (id) => config.CHECKS.find((c) => c.id === id);
  for (const r of results) {
    const check = checkOf(r.checkId) || {};
    const subjects = check.issuePerItem ? r.items.map((it) => ({ subjectType: it.subjectType, subjectKey: it.subjectKey, payload: it.payload })) : [{ subjectType: 'scope', subjectKey: '' }];
    if (r.verdict === 'breach') {
      for (const s of subjects) {
        const k = `${r.checkId}|${r.scopeKey}|${s.subjectType}|${s.subjectKey}`;
        touched.add(k);
        const cur = open.get(k);
        const summary = check.issuePerItem ? `${r.checkId} ${r.scopeKey} ${s.subjectKey}: ${s.payload && s.payload.status ? s.payload.status : ''}`.trim() : `${r.checkId} ${r.scopeKey}: ${r.reason || ''}`.trim();
        if (cur) {
          const newDay = jstDate(Date.parse(cur.last_seen_at)) < asOf;
          updates.push({ id: cur.watch_issue_id, set: { last_seen_at: nowIso, days_seen: cur.days_seen + (newDay ? 1 : 0), summary, severity: r.severity }, resultRef: r });
          notes.continued.push({ issueId: cur.watch_issue_id, checkId: r.checkId, scopeKey: r.scopeKey, subjectKey: s.subjectKey, days: cur.days_seen + (newDay ? 1 : 0), summary, transitions: cur.transitions });
        } else {
          inserts.push({ checkId: r.checkId, scopeKey: r.scopeKey, subjectType: s.subjectType, subjectKey: s.subjectKey, severity: r.severity, summary, resultRef: r });
          notes.new.push({ checkId: r.checkId, scopeKey: r.scopeKey, subjectKey: s.subjectKey, summary, severity: r.severity });
        }
      }
    }
    if (r.verdict === 'blocked' || r.verdict === 'execution_error') {
      // 判定保留 = 案件に触らない (回復にも継続にもしない)
      for (const [k, i] of open) if (i.check_id === r.checkId && i.scope_key === r.scopeKey) { touched.add(k); notes.held.push({ issueId: i.watch_issue_id, checkId: r.checkId, scopeKey: r.scopeKey, subjectKey: i.subject_key }); }
      continue;
    }
    if (r.verdict === 'pass' || r.verdict === 'breach') {
      // この評価で breach にならなかった open の案件 = 回復。ただし W2 の日付の案件で窓から外れたものは「監視期間外」
      for (const [k, i] of open) {
        if (i.check_id !== r.checkId || i.scope_key !== r.scopeKey || touched.has(k)) continue;
        touched.add(k);
        // 世代が変わり続けた回 = 「今回 breach に含まれなかった」を回復と読まない (判定保留。Codex R2 #2)
        if (holdRecoveries) { notes.held.push({ issueId: i.watch_issue_id, checkId: r.checkId, scopeKey: r.scopeKey, subjectKey: i.subject_key, reason: 'unstable' }); continue; }
        // 案件ごとの保持 (r.held) と明示の回復 (r.explicitRecovery = recoverable にあるものだけ回復・それ以外は保持。W13:ne。Company DB構想 10 §6.1.1 C2 v5-5)
        //   out_of_scope は下の「監視対象外」で閉じる (本当に対象から外れた案件だけ)
        const inScopeOut = r.outOfScope && Object.hasOwn(r.outOfScope, i.subject_key);
        if (!inScopeOut && ((r.held && Object.hasOwn(r.held, i.subject_key)) || (r.explicitRecovery && !(r.recoverable || []).includes(i.subject_key)))) {
          notes.held.push({ issueId: i.watch_issue_id, checkId: r.checkId, scopeKey: r.scopeKey, subjectKey: i.subject_key, reason: (r.held && r.held[i.subject_key]) || 'not_confirmed' });
          continue;
        }
        // 評価した期間より前の日付の案件 = 監視期間外 (評価した日が 1 つも無い = periodFrom が null のときも、日付の案件は全部期間外 = 回復にしない)。
        // 評価が「監視対象外」と名指しした対象 (r.outOfScope[subject] = 理由。例 W6 の廃番・窓から外れた SKU = 在庫は 0 のまま) も回復にしない (Codex #1406 R1 #5)
        const scopeOut = r.outOfScope && Object.hasOwn(r.outOfScope, i.subject_key) ? String(r.outOfScope[i.subject_key]) : null;
        const outOfWindow = !!scopeOut || (check.issuePerItem && i.subject_type === 'day' && (r.periodFrom == null || i.subject_key < r.periodFrom));
        // 評価の範囲より未来側の案件 (過去の日を評価しているとき) は触らない = 判定保留 (Codex R1 #5)
        if (check.issuePerItem && r.periodTo && i.subject_type === 'day' && i.subject_key > r.periodTo) { notes.held.push({ issueId: i.watch_issue_id, checkId: r.checkId, scopeKey: r.scopeKey, subjectKey: i.subject_key, reason: 'beyond_period' }); continue; }
        const set = outOfWindow ? { state: 'out_of_window', transitions: i.transitions + 1 } : { state: 'recovered', recovered_at: nowIso, transitions: i.transitions + 1 };
        if (scopeOut) set.summary = `${i.check_id} ${i.scope_key} ${i.subject_key}: 監視対象外 (${scopeOut})`;
        updates.push({ id: i.watch_issue_id, set, resultRef: r });
        (outOfWindow ? notes.outOfWindow : notes.recovered).push({ issueId: i.watch_issue_id, checkId: r.checkId, scopeKey: r.scopeKey, subjectKey: i.subject_key, transitions: i.transitions + 1, reason: scopeOut || (outOfWindow ? 'before_period' : null) });
      }
    }
  }
  return { inserts, updates, notes };
}

/** 保存 (writer の 1 取引)。戻り値 = { runId, resultIds } */
export async function persist({ writer, config, runId, asOf, now, host, evidence, planned, results, issues, lastLine, summary }) {
  await writer.exec('begin');
  try {
    await writer.query(`insert into ops.watch_runs (watch_run_id, company_id, as_of_date, started_at, host, checks_version, planned_keys, evidence) values ($1, $2::smallint, $3::date, $4::timestamptz, $5, $6, $7, $8::jsonb)`,
      [runId, config.COMPANY_ID, asOf, now.toISOString(), host, config.CHECKS_VERSION, planned.length, JSON.stringify(evidence || {})]);
    const resultIds = new Map();
    for (const r of results) {
      const pick = pickItems(r.items || [], { maxRows: config.ITEMS_MAX_ROWS, maxBytes: config.ITEMS_MAX_BYTES });
      const row = (await writer.query(`insert into ops.watch_results (watch_run_id, company_id, check_id, check_version, scope_key, subject_type, period_from, period_to, evaluated_at, verdict, severity, observed, threshold, sample_size, input_generation, reason, duration_ms, item_total, item_saved, item_selection)
        values ($1, $2::smallint, $3, $4, $5, $6, $7::date, $8::date, $9::timestamptz, $10, $11, $12::jsonb, $13::jsonb, $14, $15::jsonb, $16, $17, $18, $19, $20) returning watch_result_id`,
        [runId, config.COMPANY_ID, r.checkId, r.checkVersion, r.scopeKey, r.subjectType || 'scope', r.periodFrom, r.periodTo, now.toISOString(), r.verdict, r.severity, JSON.stringify(r.observed ?? null), JSON.stringify(r.threshold ?? null), r.sampleSize ?? null, JSON.stringify(r.inputGeneration ?? null), r.reason ? String(r.reason).slice(0, 500) : null, r.durationMs ?? null, r.itemTotal ?? (r.items ? r.items.length : null), pick.saved.length, pick.saved.length ? pick.selection : null])).rows[0];
      resultIds.set(r, row.watch_result_id);
      let rank = 0;
      for (const it of pick.saved) await writer.query(`insert into ops.watch_result_items (watch_result_id, rank, subject_type, subject_key, payload) values ($1, $2, $3, $4, $5::jsonb)`, [row.watch_result_id, ++rank, it.subjectType, String(it.subjectKey), JSON.stringify(it.payload ?? {})]);
    }
    for (const ins of issues.inserts) {
      const rid = resultIds.get(ins.resultRef) ?? null;
      await writer.query(`insert into ops.watch_issues (company_id, check_id, scope_key, subject_type, subject_key, state, severity, first_seen_at, last_seen_at, first_result_id, last_result_id, summary) values ($1::smallint, $2, $3, $4, $5, 'open', $6, $7::timestamptz, $7::timestamptz, $8, $8, $9)`,
        [config.COMPANY_ID, ins.checkId, ins.scopeKey, ins.subjectType, ins.subjectKey, ins.severity, now.toISOString(), rid, String(ins.summary).slice(0, 300)]);
    }
    for (const up of issues.updates) {
      const rid = resultIds.get(up.resultRef) ?? null;
      const cols = Object.keys(up.set);
      const sets = cols.map((c, i) => `${c} = $${i + 2}${c.endsWith('_at') ? '::timestamptz' : ''}`);
      await writer.query(`update ops.watch_issues set ${sets.join(', ')}, last_result_id = coalesce($${cols.length + 2}, last_result_id) where watch_issue_id = $1`, [up.id, ...cols.map((c) => (c === 'summary' ? String(up.set[c]).slice(0, 300) : up.set[c])), rid]);
    }
    await writer.query(`update ops.watch_runs set finished_at = $2::timestamptz, completed_keys = $3, summary = $4::jsonb, last_line = $5 where watch_run_id = $1`, [runId, new Date().toISOString(), results.length, JSON.stringify(summary), String(lastLine).slice(0, 600)]);
    await writer.exec('commit');
    return { runId, resultIds };
  } catch (e) {
    try { await writer.exec('rollback'); } catch { /* */ }
    throw e;
  }
}

/** 要約と最後の 1 行 */
export function summarize({ asOf, planned, results, notes, deadlineHit, separate = [] }) {
  const n = (v) => results.filter((r) => r.verdict === v).length;
  // 別に数える評価キー (config.SUMMARY_SEPARATE。例 W13:ne = 切替までの NE との差 = 数百件の info)。「新・継続」の件数と明細からは外して、1 つの数にまとめる
  const isSep = (x) => separate.some((s) => s.checkId === x.checkId && s.scopeKey === x.scopeKey);
  const mainNew = notes.new.filter((x) => !isSep(x)), mainCont = notes.continued.filter((x) => !isSep(x));
  const counts = { planned: planned.length, completed: results.length, pass: n('pass'), breach: n('breach'), blocked: n('blocked'), execution_error: n('execution_error'), new: mainNew.length, continued: mainCont.length, recovered: notes.recovered.length, out_of_window: notes.outOfWindow.length, held: notes.held.length };
  const icon = counts.execution_error > 0 || counts.completed < counts.planned || deadlineHit ? '❌' : counts.breach > 0 || counts.blocked > 0 ? '⚠️' : '✅';
  const parts = [`異常 ${counts.breach} (新 ${counts.new} / 継続 ${counts.continued})`, `判定保留 ${counts.blocked}`, `回復 ${counts.recovered}`, `評価 ${counts.completed}/${counts.planned}`];
  for (const s of separate) {
    const sn = notes.new.filter((x) => x.checkId === s.checkId && x.scopeKey === s.scopeKey).length, sc = notes.continued.filter((x) => x.checkId === s.checkId && x.scopeKey === s.scopeKey).length;
    const sr = results.find((r) => r.checkId === s.checkId && r.scopeKey === s.scopeKey);
    if (sr && (sn || sc || sr.verdict !== 'pass')) parts.push(`${s.label} ${sn + sc} 件 (新 ${sn}${sr.verdict === 'blocked' ? '・判定保留' : ''})`);
    counts[`separate_${s.checkId}_${s.scopeKey}`] = sn + sc;
  }
  if (counts.execution_error) parts.push(`評価できず ${counts.execution_error}`);
  if (counts.out_of_window) parts.push(`監視期間外 ${counts.out_of_window}`);
  const details = [];
  for (const x of mainNew.slice(0, 3)) details.push(`${x.summary} (新)`);
  for (const x of mainCont.slice(0, 2)) details.push(`${x.summary} (継続 ${x.days} 日)`);
  const blockedRoots = results.filter((r) => r.verdict === 'blocked' && !r.blockedBy).slice(0, 2).map((r) => `${r.checkId} ${r.scopeKey}: ${r.reason}`);
  for (const b of blockedRoots) details.push(`保留 ${b}`);
  for (const r of results.filter((r) => r.verdict === 'execution_error').slice(0, 2)) details.push(`評価できず ${r.checkId} ${r.scopeKey}: ${r.reason}`);
  const lastLine = `${icon} Company DB 見張り ${asOf}: ${parts.join(' / ')}${details.length ? ' — ' + details.join(' ; ') : ''}`.replace(/\s+/g, ' ').slice(0, 600);
  return { counts, lastLine, icon, exitCode: icon === '❌' ? 1 : 0 };
}

/**
 * 1 回流す。writer = null なら評価だけ (dry-run)。戻り値 = { runId, counts, lastLine, exitCode, results, notes }
 */
export const LOCK_KEY = 'company-db-watch';
export const MAX_GENERATION_RETRIES = 3;

/** snapshot を閉じた後の世代 (短い read only の取引で読む。generationOf は savepoint を使うので取引の中で呼ぶ) */
async function generationAfter(db, config, asOf, opts) {
  await db.exec('begin read only');
  try { const g = await generationOf(db, config, asOf, opts); await db.exec('commit'); return g; }
  catch (e) { try { await db.exec('rollback'); } catch { /* */ } throw e; }
}

/**
 * 1 回流す。writer = null なら評価だけ (dry-run)。戻り値 = { runId, counts, lastLine, exitCode, results, notes }
 *   - 記録する回は as_of = 今日 (JST) だけ (過去の日を評価して、今の案件を回復させない。Codex R1 #5)。過去の日は dry-run で
 *   - 記録する回は会社単位の advisory lock (session) を取る = 2 本が同時に走らない (Codex R1 #4。open の部分 unique が二重目)
 *   - snapshot を閉じた後に世代を読み直し、変わっていれば再評価 (最大 3 回)。変わり続ければ pass を blocked に落とす (Codex R1 #3)
 *   - syncRunId = daily-sync の実行 ID。記録する回は必須 (W7 が同じ ID の証跡だけを採用する)。dry-run で無ければ「結びつけずに」読む (unbound)
 */
/** @param {string|null} [p.dataDir] 証跡・照合の全件 JSON の置き場所 (実行口が決めた値 = --data-dir が先。無ければ env DATA_DIR)。W13 の評価と世代の指紋が同じ値を使う */
export async function runWatch({ db, writer = null, config, asOf, evidence = {}, evidenceHistory = {}, now = new Date(), host = 'minipc', log = () => {}, syncRunId = null, hooks = {}, dataDir = null }) {
  const runId = newWatchRunId(now);
  const todayJst = jstDate(now.getTime());
  if (writer && asOf !== todayJst) throw new Error(`記録する回の as_of は今日 (${todayJst}) だけ (${asOf} を見るなら --dry-run)`);
  if (writer && !syncRunId) throw new Error('記録する回は実行 ID (DAILY_SYNC_RUN_ID) が要る (daily-sync の中で動かす。手で確かめるなら --dry-run)');
  const unbound = !writer && !syncRunId;
  let locked = false;
  try {
    if (writer) {
      locked = (await writer.query(`select pg_try_advisory_lock(hashtext($1)) as got`, [`${LOCK_KEY}:${config.COMPANY_ID}`])).rows[0].got === true;
      if (!locked) throw new Error('別の見張りが走っている (advisory lock が取れない)');
    }
    let ev, attempts = 0, unstable = false;
    for (;;) {
      attempts++;
      ev = await evaluateAll({ db, config, asOf, evidence, evidenceHistory, now, log, syncRunId, unbound, dataDir });
      if (hooks.afterSnapshot) await hooks.afterSnapshot(attempts);
      const after = await generationAfter(db, config, asOf, { evidence, dataDir });   // snapshot を閉じた後に読み直す
      if (after === ev.generation) break;
      log(`世代が変わった (${attempts} 回目) → 再評価`);
      if (attempts >= MAX_GENERATION_RETRIES) {
        // 🚨 変わり続けた = この snapshot の「正常」は信じない: pass は blocked に落とし、案件の回復も保留 (breach の明細に無い案件を回復にしない。Codex R2 #2)。
        //    観測できた breach (新・継続) はそのまま残す
        unstable = true;
        for (const r of ev.results) if (r.verdict === 'pass') { r.verdict = 'blocked'; r.reason = `評価中に世代が変わり続けた (${attempts} 回)`; r.unstable = true; }
        break;
      }
    }
    if (writer && !ev.migrated) throw new Error('0023 (ops.watch_*) が未適用 = 記録できない (migrate を当てる。評価だけなら --dry-run)');
    const issues = reconcileIssues({ config, results: ev.results, openIssues: ev.openIssues, asOf, now, holdRecoveries: unstable });
    const s = summarize({ asOf, planned: ev.planned, results: ev.results, notes: issues.notes, deadlineHit: ev.deadlineHit, separate: config.SUMMARY_SEPARATE || [] });
    if (unstable) s.lastLine = s.lastLine.replace(/^(\S+ Company DB 見張り \S+:)/, `$1 世代が変わり続けた (${attempts} 回・pass と回復は保留に) /`).slice(0, 600);
    for (const r of ev.results) log(`${ICON[r.verdict]} ${r.checkId} ${r.scopeKey}: ${r.verdict}${r.reason ? ` — ${r.reason}` : ''}`);
    if (writer) await persist({ writer, config, runId, asOf, now, host, evidence, planned: ev.planned, results: ev.results, issues, lastLine: s.lastLine, summary: { ...s.counts, attempts, unstable } });
    return { runId, counts: s.counts, lastLine: s.lastLine, exitCode: s.exitCode, results: ev.results, notes: issues.notes, planned: ev.planned, persisted: !!writer, attempts, unstable };
  } finally {
    if (locked) { try { await writer.query(`select pg_advisory_unlock(hashtext($1))`, [`${LOCK_KEY}:${config.COMPANY_ID}`]); } catch { /* 接続が切れれば lock も消える */ } }
  }
}

export { addDays };
