/**
 * ingest/finance-coverage.mjs — 決済のそろい (coverage) の受け口の本体 (Render 側・D7b-1b-2。受け皿 = migration 0051 core.finance_coverage)
 *   設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 §3.1。送り手 = miniPC の coordinator (D7b-1b-3・後の PR)。
 *
 * 受け口 (router.mjs):
 *   POST /apps/company-db/sync/order-finance/coverage  { state: 'updating' | 'complete', mall, scope, source, generation, run_token, manifest? (complete だけ), request_hash? }
 *   GET  /apps/company-db/sync/order-finance/coverage/status?mall&scope&source[&receipts=1]   (送り手が回の始めに Render の今の世代を読む)
 *
 * 状態の移り方 (§3.1・世代ごと。1 取引・**財務の chunk と同じ advisory lock** の中で):
 *   ・古い世代 → stale (何もしない)
 *   ・新しい世代は updating からだけ (新しい世代の直接の complete = 409)。新しい世代の updating は前の complete を無効にする (manifest の列を空に)
 *   ・同じ世代・同じ token の updating の再送 = same / 違う token = 409 / 同じ世代の complete → updating = 409
 *   ・同じ世代・同じ token の updating → complete は 1 回だけ: lock → 今の受領記録から receipt digest を計算 → manifest と一致しなければ 409 (RECEIPT_MISMATCH) → complete
 *   ・同じ世代の complete の再送 = request_hash が同じなら same (応答だけ失われた)・違えば 409
 *   ・無効の印 (invalidated_at = complete の後に token の無い chunk か、ほかの coverage の token の chunk が受領記録を変えた) のある世代は complete に戻れない = 409 (次の世代から)
 *   ・complete の manifest の端: settlements_through > policy の起点・evidence_chain_from ≤ policy の起点 (UTC の瞬間で比べる)。鎖の窓の連続は coordinator (D7b-1b-3) が確かめる
 *   ・complete の manifest.policy_fingerprint = 今の policy の全期間の指紋 (違えば 409 POLICY_MISMATCH)。保存した指紋が後で今の policy と違えば
 *     (policy を後から変えた) core.finance_coverage_state は complete_to を返さない = 次の回の updating → complete でやり直す (#1561 Codex R2 High)
 *
 * 財務の chunk との約束 (ingest/order-finance.mjs が呼ぶ):
 *   ・takeFinanceLock = chunk も coverage の要求も同じ lock (会社 × モール × scope) を取引の最初に取る (待つ・lock_timeout で 503 LOCKED)
 *   ・assertChunkCoverage = chunk に coverage_generation / run_token が付いていれば、その世代・その token の updating のときだけ適用 (違えば 409)。
 *     chunk の行の source と、置き換え・墓石で消える既存の行の source が、その coverage の source と同じこと (違えば何も消す前に 409)
 *   ・invalidateCompleteAfterWrite = chunk が受領記録を 1 つでも変えたら (墓石・置き換えを含む)、その会社 × モール × scope の complete を全部 updating に落とす
 *     (受領記録と receipt digest は source で分かれていない = fail-closed)。token の無い chunk = 全部の source / token 付き = ほかの source (か別の世代) の complete
 *   🚨 coordinator (D7b-1b-3) ができたら token の無い chunk を拒む契約にする (今は今の daily-sync の送り手を止めないために受ける)
 */
import { MALLS, SCOPE_RE } from './orders.mjs';
import { SOURCES } from '../finance/order-finance-checksum.mjs';
import { err } from './stock-daily.mjs';
import { createReceiptDigester, validateCoverageManifest, coverageRequestHash, bigintText, RUN_TOKEN_RE, MANIFEST_FIELDS, COVERAGE_STATES } from '../finance/coverage-manifest.mjs';

export const COMPANY_ID = 1;
export const COVERAGE_MALLS = MALLS.filter((m) => m !== 'other');   // 0043 / 0051 の mall の CHECK
const bad = (m) => err('BAD_REQUEST', m);
const BODY_KEYS = new Set(['state', 'mall', 'scope', 'source', 'generation', 'run_token', 'manifest', 'request_hash']);
export const RECEIPT_FETCH = 10000;

/** 財務の書き込みの lock の名前 (chunk・updating・complete が同じ名前 = §3.1 R23 M5) */
export const financeLockKey = (companyId, mall, scope) => `company-db:order-finance:${companyId}:${mall}:${scope}`;

/** 取引の中で lock を取る (取れるまで待つ・接続の lock_timeout を過ぎたら LOCKED = 503 で送り手がやり直す) */
export async function takeFinanceLock(db, { companyId = COMPANY_ID, mall, scope }) {
  try {
    await db.query(`select pg_advisory_xact_lock(hashtext($1))`, [financeLockKey(companyId, mall, scope)]);
  } catch (e) {
    // 🚨 文言に「canceling statement」を入れない (chunk の受け口がそれを期限切れ = 割って送り直すと読むため)
    if (e && (e.code === '55P03' || /lock timeout/i.test(String(e.message)))) throw err('LOCKED', `財務の書き込みの lock (${mall}/${scope}) が空かない (別の chunk か coverage の要求が走っている)。少し待って送り直す`);
    throw e;
  }
}

/** 0051 が入っているか */
export async function coverageReady(db) {
  return (await db.query(`select to_regclass('core.finance_coverage') is not null as ok`)).rows[0].ok === true;
}

const ts = (col) => `to_char(${col} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as ${col}`;
const SELECT_COLS = [
  'source', 'state', 'generation::text as generation', 'run_token', `to_char(updating_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as updating_at`,
  'complete_to::text as complete_to', ts('settlements_through'), 'source_revision::text as source_revision', 'headers_count', 'headers_checksum',
  'receipt_count', 'receipt_lines::text as receipt_lines', 'receipt_digest', 'inventory_snapshot_id', 'inventory_count', 'inventory_digest', ts('inventory_completed_at'),
  'initial_marker_id', 'initial_marker_digest', 'selected_documents_count', 'selected_documents_digest', ts('evidence_chain_from'), ts('evidence_chain_through'),
  'expected_report_count', 'expected_report_digest', 'inventory_runs_digest', 'policy_fingerprint', 'request_hash',
  `to_char(completed_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as completed_at`,
  `to_char(invalidated_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as invalidated_at`, 'invalidated_reason',
].join(', ');
/** 行を JSON の形に (件数 = 数・receipt_lines = 数・bigint の ID = 10 進の文字列) */
const rowOut = (r) => (r ? { ...r, receipt_lines: r.receipt_lines == null ? null : Number(r.receipt_lines) } : null);

async function readCoverage(db, { companyId, mall, scope, source }, { forUpdate = false } = {}) {
  return (await db.query(`select ${SELECT_COLS} from core.finance_coverage where company_id = $1::smallint and mall = $2 and scope_key = $3 and source = $4${forUpdate ? ' for update' : ''}`,
    [companyId, mall, scope, source])).rows[0] || null;
}

/**
 * 要求の形を確かめて正規化する (例外 = BAD_REQUEST)。戻り = { state, mall, scope, source, generation (10 進の文字列), runToken, manifest (complete だけ), requestHash (complete だけ) }
 */
export function validateCoverageBody(body, { companyId = COMPANY_ID, now = new Date() } = {}) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('body は object');
  for (const k of Object.keys(body)) if (!BODY_KEYS.has(k)) throw bad(`知らない鍵: ${k}`);
  if (!COVERAGE_STATES.includes(body.state)) throw bad(`state は ${COVERAGE_STATES.join(' / ')}: ${String(body.state).slice(0, 20)}`);
  if (!COVERAGE_MALLS.includes(body.mall)) throw bad(`mall が不正: ${String(body.mall).slice(0, 20)}`);
  if (typeof body.scope !== 'string' || !SCOPE_RE.test(body.scope)) throw bad(`scope が不正: ${String(body.scope).slice(0, 40)}`);
  if (!SOURCES.includes(body.source)) throw bad(`source は ${SOURCES.join(' / ')}: ${String(body.source).slice(0, 40)}`);
  let generation;
  try { generation = bigintText(body.generation, 'generation', { min: 1n }); } catch (e) { throw bad(e.message); }
  if (typeof body.run_token !== 'string' || !RUN_TOKEN_RE.test(body.run_token)) throw bad('run_token は英数と ._:- の 16〜100 文字');
  const v = { state: body.state, mall: body.mall, scope: body.scope, source: body.source, generation, runToken: body.run_token, manifest: null, requestHash: null };
  if (body.state === 'updating') {
    if (body.manifest !== undefined || body.request_hash !== undefined) throw bad('updating に manifest / request_hash は付けない (complete で初めて渡す)');
    return v;
  }
  try { v.manifest = validateCoverageManifest(body.manifest, { now }); } catch (e) { throw bad(e.message); }
  v.requestHash = coverageRequestHash({ companyId, mall: v.mall, scopeKey: v.scope, source: v.source, generation, runToken: v.runToken, manifest: v.manifest });
  if (body.request_hash !== undefined && body.request_hash !== v.requestHash) {
    throw bad(`request_hash が受け口の計算と違う (送り手 ${String(body.request_hash).slice(0, 16)}… / 受け口 ${v.requestHash.slice(0, 16)}… = 正規の JSON の作り方がずれている)`);
  }
  return v;
}

/**
 * 今の受領記録から receipt digest を計算する (呼び手の取引の中で・cursor で少しずつ読む = 51 万注文でも手元に全部を持たない)。
 *   対象 = 会社 × モール × scope の lines > 0 の受領記録 (墓石は除く・疑似注文を含む・期間では絞らない)・並び = collate "C" (UTF-8 のバイトの順)
 */
export async function computeReceiptDigest(db, { companyId = COMPANY_ID, mall, scope, fetchSize = RECEIPT_FETCH }) {
  // cursor の宣言に bind の引数を使わない (utility 文) → 値は形を確かめた後に文字として埋める
  if (!Number.isInteger(companyId) || !COVERAGE_MALLS.includes(mall) || typeof scope !== 'string' || !SCOPE_RE.test(scope)) throw new Error('computeReceiptDigest: 会社・モール・scope が不正');
  const cur = 'finance_coverage_receipts';
  await db.exec(`declare ${cur} no scroll cursor for
    select mall_order_no, set_checksum, transform_version, lines from core.order_finance_receipts
     where company_id = ${companyId} and mall = '${mall}' and scope_key = '${scope}' and lines > 0
     order by mall_order_no collate "C"`);
  const d = createReceiptDigester();
  for (;;) {
    const rows = (await db.query(`fetch forward ${Math.max(1, Math.floor(fetchSize))} from ${cur}`)).rows;
    for (const r of rows) d.add({ mall_order_no: r.mall_order_no, set_checksum: r.set_checksum, transform_version: r.transform_version, lines: Number(r.lines) });
    if (rows.length < fetchSize) break;
  }
  await db.exec(`close ${cur}`);
  return d.finish();
}

/** policy の起点 (その source の最初の period_from の JST 00:00)。policy が無ければ NO_POLICY (409) */
async function policyOrigin(db, { companyId, mall, scope, source }) {
  const r = (await db.query(`select count(*)::int as n, min(period_from)::text as f from core.finance_source_policy
     where company_id = $1::smallint and mall = $2 and scope_key = $3 and source = $4`, [companyId, mall, scope, source])).rows[0];
  if (!r || Number(r.n) === 0) throw err('NO_POLICY', `${mall}/${scope} の source ${source} は finance_source_policy に無い (決済のそろいは policy の source ごと)`);
  return `${r.f}T00:00:00+09:00`;
}

/** 今の policy の全期間の指紋 (SQL の core.finance_policy_fingerprint = JS の policyFingerprint) */
export async function currentPolicyFingerprint(db, { companyId = COMPANY_ID, mall, scope }) {
  return (await db.query(`select core.finance_policy_fingerprint($1::smallint, $2, $3) as f`, [companyId, mall, scope])).rows[0].f;
}

const MANIFEST_COLS = Object.keys(MANIFEST_FIELDS);
const NULL_MANIFEST = [...MANIFEST_COLS, 'request_hash', 'completed_at', 'invalidated_at', 'invalidated_reason'].map((c) => `${c} = null`).join(', ');

/**
 * updating / complete を 1 取引で。db = { query, exec } (router の pgAdapter / 試験の PGlite)。
 * @returns {{ status: 'applied'|'same'|'stale', state, generation, current_generation?, complete_to?, receipt? }}
 *   例外: BAD_REQUEST (400) / CONFLICT・RECEIPT_MISMATCH・NO_POLICY・POLICY_MISMATCH・NOT_MIGRATED (409) / LOCKED (503)
 *   hooks.afterLock = 試験の差し込み口 (lock を取った直後)
 */
export async function applyCoverage(db, body, { companyId = COMPANY_ID, now = () => new Date(), log = () => {}, hooks = {} } = {}) {
  const v = validateCoverageBody(body, { companyId, now: now() });
  const key = { companyId, mall: v.mall, scope: v.scope, source: v.source };
  const g = BigInt(v.generation);
  await db.exec('begin');
  try {
    await takeFinanceLock(db, key);
    if (hooks.afterLock) await hooks.afterLock(db);
    if (!(await coverageReady(db))) throw err('NOT_MIGRATED', 'not_migrated: migration 0051 (core.finance_coverage) is not applied');
    const origin = await policyOrigin(db, key);
    const cur = await readCoverage(db, key, { forUpdate: true });
    const cg = cur ? BigInt(cur.generation) : null;
    let out;
    if (cur && g < cg) {
      out = { status: 'stale', state: cur.state, generation: v.generation, current_generation: cur.generation };
    } else if (v.state === 'updating') {
      if (!cur || g > cg) {
        await db.query(`insert into core.finance_coverage (company_id, mall, scope_key, source, state, generation, run_token, updating_at)
            values ($1::smallint, $2, $3, $4, 'updating', $5::bigint, $6, now())
          on conflict (company_id, mall, scope_key, source) do update set state = 'updating', generation = excluded.generation, run_token = excluded.run_token,
            updating_at = excluded.updating_at, ${NULL_MANIFEST}`,
          [companyId, v.mall, v.scope, v.source, v.generation, v.runToken]);
        if (cur) log(`updating: 世代 ${cur.generation} (${cur.state}${cur.invalidated_at ? '・無効の印' : ''}) → ${v.generation}`);
        out = { status: 'applied', state: 'updating', generation: v.generation };
      } else if (cur.state === 'complete') {
        throw err('CONFLICT', `同じ世代 (${v.generation}) は complete 済み = complete → updating はしない (新しい世代の updating を送る)`);
      } else if (cur.run_token !== v.runToken) {
        throw err('CONFLICT', `同じ世代 (${v.generation}) の updating が別の token で始まっている (別の回が同じ世代を採った = 台帳を確かめる)`);
      } else {
        out = { status: 'same', state: 'updating', generation: v.generation };
      }
    } else {   // complete
      if (!cur || g > cg) throw err('CONFLICT', `世代 ${v.generation} は updating を受けていない (新しい世代は updating から。Render の今の世代 = ${cur ? cur.generation : 'なし'})`);
      if (cur.state === 'complete') {
        if (cur.request_hash !== v.requestHash) throw err('CONFLICT', `同じ世代 (${v.generation}) の complete は別の中身で受け取り済み (request_hash ${String(cur.request_hash).slice(0, 12)}… / ${v.requestHash.slice(0, 12)}…)`);
        out = { status: 'same', state: 'complete', generation: v.generation, complete_to: cur.complete_to };
      } else if (cur.run_token !== v.runToken) {
        throw err('CONFLICT', `世代 ${v.generation} の updating の token と違う (別の回の complete は受けない)`);
      } else if (cur.invalidated_at) {
        throw err('CONFLICT', `世代 ${v.generation} は ${cur.invalidated_at} に無効にされた (${cur.invalidated_reason}) = この世代では complete に戻れない。次の世代の updating から回し直す`);
      } else {
        // 🚨 送り手が検査した policy (の指紋) が今の policy と同じこと (#1561 Codex R2 High)。違えば 409 = 回の始めに status で読み直してやり直す。
        //    保存した指紋と今の指紋が後で違えば (policy を後から変えた) core.finance_coverage_state は complete_to を返さない
        const fp = await currentPolicyFingerprint(db, key);
        if (v.manifest.policy_fingerprint !== fp) throw err('POLICY_MISMATCH', `manifest.policy_fingerprint (${v.manifest.policy_fingerprint.slice(0, 12)}…) が今の policy の指紋 (${fp.slice(0, 12)}…) と違う = 送り手が検査した後に policy が変わった。次の回でやり直す`);
        if (Date.parse(v.manifest.settlements_through) <= Date.parse(origin)) throw bad(`settlements_through (${v.manifest.settlements_through}) が policy の起点 (${origin}) より後でない = 起点を覆う決済が無い`);
        // 🚨 証拠の鎖 (初期の印 + 積み上げた一覧) は policy の起点まで届いていること (#1561 Codex R1 High 2)。比べるのは UTC の瞬間 (起点 = period_from の JST 00:00・§3.1 の期間の型)。
        //    Render が確かめられるのは端だけ = 鎖の窓が途切れずにつながること (createdSince / Until の重なり・保持期間の空白) は coordinator (D7b-1b-3) が確かめる
        if (Date.parse(v.manifest.evidence_chain_from) > Date.parse(origin)) throw bad(`evidence_chain_from (${v.manifest.evidence_chain_from}) が policy の起点 (${new Date(Date.parse(origin)).toISOString().replace(/\.000Z$/, 'Z')}) より後 = 起点からの証拠が無い`);
        const rc = await computeReceiptDigest(db, { companyId, mall: v.mall, scope: v.scope });
        const m = v.manifest;
        if (rc.count !== m.receipt_count || rc.lines !== m.receipt_lines || rc.digest !== m.receipt_digest) {
          throw Object.assign(err('RECEIPT_MISMATCH', `受領記録が manifest と違う (Render ${rc.count} 注文・${rc.lines} 行・${rc.digest.slice(0, 12)}… / 送り手 ${m.receipt_count}・${m.receipt_lines}・${m.receipt_digest.slice(0, 12)}…)`), { detail: { render: rc } });
        }
        const sets = MANIFEST_COLS.map((c, i) => `${c} = $${i + 5}`).join(', ');
        await db.query(`update core.finance_coverage set state = 'complete', ${sets}, request_hash = $${MANIFEST_COLS.length + 5}, completed_at = now()
           where company_id = $1::smallint and mall = $2 and scope_key = $3 and source = $4`,
          [companyId, v.mall, v.scope, v.source, ...MANIFEST_COLS.map((c) => m[c]), v.requestHash]);
        log(`complete: 世代 ${v.generation}・complete_to ${m.complete_to}・受領 ${rc.count} 注文 / ${rc.lines} 行`);
        out = { status: 'applied', state: 'complete', generation: v.generation, complete_to: m.complete_to, receipt: rc };
      }
    }
    await db.exec('commit');
    return out;
  } catch (e) {
    try { await db.exec('rollback'); } catch { /* 取引が既に無い */ }
    throw e;
  }
}

/**
 * 財務の chunk に coverage_generation / run_token が付いていたら、その世代・その token の updating のときだけ通す (lock を取った後に呼ぶ)。
 *   sources = chunk の行の source の集合 (その coverage の source と違えば 409)
 */
export async function assertChunkCoverage(db, { companyId = COMPANY_ID, mall, scope, generation, runToken, sources = [], orderNos = [] }) {
  const rows = (await db.query(`select source, state, generation::text as generation, run_token from core.finance_coverage
     where company_id = $1::smallint and mall = $2 and scope_key = $3 order by source`, [companyId, mall, scope])).rows;
  const hit = rows.filter((r) => r.generation === generation && r.run_token === runToken);
  const now = rows.map((r) => `${r.source}=${r.state}@${r.generation}`).join(', ') || 'なし';
  if (hit.length !== 1 || hit[0].state !== 'updating') {
    throw err('COVERAGE_MISMATCH', `coverage の世代 ${generation} (token ${String(runToken).slice(0, 8)}…) の updating が無い (Render の今 = ${now})。complete の後・別の世代・別の token の chunk は適用しない`);
  }
  const other = [...sources].filter((s) => s !== hit[0].source);
  if (other.length) throw err('COVERAGE_MISMATCH', `chunk の行の source (${other.join(', ')}) が coverage の source (${hit[0].source}) と違う`);
  // 🚨 置き換え・墓石で消える **既存の行** の source も確かめる (#1561 Codex R1 High 1: 墓石 lines = [] は chunk の行の source が空 =
  //    ほかの source (例 complete の過去の source A) の既存の行を B の token で消せた)。違えば何も消す前に 409
  if (orderNos.length) {
    const ex =(await db.query(`select distinct source from core.order_finance_daily
       where company_id = $1::smallint and mall = $2 and scope_key = $3 and mall_order_no = any($4::text[]) and source <> $5 order by source`,
      [companyId, mall, scope, orderNos, hit[0].source])).rows.map((r) => r.source);
    if (ex.length) throw err('COVERAGE_MISMATCH', `chunk の注文の既存の行に別の source (${ex.join(', ')}) がある = coverage の source (${hit[0].source}) の token では置き換え・墓石にしない`);
  }
  return hit[0];
}

/** 無効の印の理由 (0051 の CHECK と同じ) */
export const INVALIDATED_REASONS = Object.freeze({ untokened: 'untokened_finance_write', otherCoverage: 'other_coverage_finance_write' });

/**
 * 財務の chunk が受領記録を変えた後: その会社 × モール × scope の **complete を全部** updating に落とす (無効の印つき)。戻り = 落とした行
 *   ・token の無い chunk (今の送り手) = reason untokened_finance_write
 *   ・token 付きの chunk = その token の coverage は updating (確かめ済み) = 落ちるのは **ほかの source (か別の世代) の complete** = reason other_coverage_finance_write
 *     (#1561 Codex R1 High 1: 受領記録と receipt digest は source で分かれていない = ほかの source の complete が今の受領記録と食い違う)
 */
export async function invalidateCompleteAfterWrite(db, { companyId = COMPANY_ID, mall, scope, reason = INVALIDATED_REASONS.untokened, log = () => {} }) {
  if (!Object.values(INVALIDATED_REASONS).includes(reason)) throw new Error(`知らない無効の理由: ${reason}`);
  const rows = (await db.query(`update core.finance_coverage set state = 'updating', invalidated_at = now(), invalidated_reason = $4
     where company_id = $1::smallint and mall = $2 and scope_key = $3 and state = 'complete' returning source, generation::text as generation`, [companyId, mall, scope, reason])).rows;
  for (const r of rows) log(`coverage ${r.source} 世代 ${r.generation}: 財務の書き込み (${reason}) で complete → updating (無効の印)`);
  return rows;
}

/**
 * 状態を読む (送り手が回の始めに Render の今の世代を読む)。withReceipts = 今の受領記録の digest も (読むだけの取引・lock は取らない = 診断)
 * 戻り = { mall, scope, source, coverage: 行 | null, effective: { complete_to, generation, source_revision } (core.finance_coverage_state), receipts? }
 */
export async function coverageStatus(db, { companyId = COMPANY_ID, mall, scope, source, withReceipts = false }) {
  if (!COVERAGE_MALLS.includes(mall) || typeof scope !== 'string' || !SCOPE_RE.test(scope) || !SOURCES.includes(source)) throw bad('mall / scope / source が不正');
  const key = { companyId, mall, scope, source };
  const out = { mall, scope, source, coverage: null, effective: null };
  await db.exec('begin transaction isolation level repeatable read read only');
  try {
    out.coverage = rowOut(await readCoverage(db, key));
    const e = (await db.query(`select complete_to::text as complete_to, generation::text as generation, source_revision::text as source_revision
       from core.finance_coverage_state($1::smallint, $2, $3, $4)`, [companyId, mall, scope, source])).rows[0];
    out.effective = e || { complete_to: null, generation: null, source_revision: null };
    // 送り手は回の始めにこの指紋を読み、検査した policy として manifest.policy_fingerprint に入れる
    const pol = (await db.query(`select period_from::text as period_from, period_to::text as period_to, source from core.finance_source_policy
       where company_id = $1::smallint and mall = $2 and scope_key = $3 order by period_from, source collate "C"`, [companyId, mall, scope])).rows;
    out.policy = { fingerprint: await currentPolicyFingerprint(db, key), rows: pol };
    if (withReceipts) out.receipts = await computeReceiptDigest(db, { companyId, mall, scope });
    await db.exec('commit');
  } catch (err0) {
    try { await db.exec('rollback'); } catch { /* */ }
    throw err0;
  }
  return out;
}
