/**
 * amazon-finance-coverage.js — 決済のそろい (coverage) の判定と manifest (miniPC 側・D7b-1b-3)
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md v26 §3.1・D-65・D-66
 *   Render の受け口 (PR #1561 ingest/finance-coverage.mjs) は manifest の形・receipt digest・policy の指紋・鎖の端しか確かめられない。
 *   一覧・初期の印・文書の版・期待の report の集合はここ (SQLite の 1 つの読み取りの取引の中) で確かめる。
 *
 * evaluateCoverage(db, ctx) = **送り手の走査と同じ読み取りの取引の中で** 呼ぶ (onScanSnapshot)。書かない。
 *   戻り = { ok, reasons: [{ code, detail, human }], manifest (ok のときだけ全部そろう), diag }
 *   reasons の human = 人が Seller Central で印を作り直す・規則を足すまで直らない (⚠️) / それ以外 = 次の回・retry で直りうる (❌)
 *
 * 判定 (fail-closed。どれか 1 つでも欠ければ complete にしない):
 *   ① 採った版 (決済ごとに 1 つ・D-66) の見出しがちょうど 1 行・期間が読める・逆でない・決済 ID が見出しと明細で同じ・通貨 JPY (原文。過去の行 = 印で代える)・
 *      明細の部品の合計 = 見出しの total・終わりが読み取りの時点より後でない・要約が古くない
 *   ② 起点 (policy の period_from の JST 00:00) から途切れずにつながる最後の end = settlements_through (実時刻の半開区間で・重なりは可・隙間で止まる)
 *   ③ 初期の印 (最新の epoch) がある・verified_from ≤ 起点
 *   ④ 一覧の鎖 = その epoch の成功した回 (最後のページまで・一覧 / 取込 / 記録の失敗なし・完了) を積み上げる:
 *      最初の回の窓 [createdSince, createdUntil] に印の captured_at が入る・印の verified_through が最初の回のデータの期間と重なる・
 *      続く回の窓が空白なく重なる (保持期間 85 日以上あかない)・最後の回 = 今回 (同じ世代・token)
 *   ⑤ 期待の集合 = 印の決済 + 鎖の回の report (必須の type = V2・report ID ごとに最新の観測 = 世代 → 回 → 位置):
 *      期間が null・読めない・逆 = 未充足 (対象の外にしない) / 起点より前に終わる = 対象の外 / CANCELLED・FATAL・DONE でない = 未充足 /
 *      DONE = 最新の観測の文書 ID の版がある (file hash も同じ) かつ その決済の採った版がその版 (imported) か
 *      detail_line_count と detail_digest が完全に一致 (satisfied_by_selected_settlement。合計の一致だけでは不可)
 *   ⑥ 印の決済 = 採った見出しと決済 ID・期間・金額・通貨が一致
 * manifest の digest の形 (正規の JSON の SHA-256・ID と bigint は 10 進の文字列・日時は UTC の YYYY-MM-DDTHH:MM:SSZ・並びは UTF-8 のバイトの順):
 *   headers_checksum = {format:'fch-v1', headers:[{settlement_id, start, end, source_layer, total_amount_micro, currency}]}
 *   selected_documents_digest = {format:'fsd-v1', documents:[{settlement_id, document_version_id, report_id, report_document_id, file_hash, header_id, detail_line_count, detail_digest}]}
 *   expected_report_digest = {format:'fer-v1', items:[{kind:'marker_settlement'|'api_report', id, observation_id, result, document_version_id, period_basis, effective_data_from, effective_data_to}]}
 *   inventory_runs_digest = {format:'fir-v1', runs:[{inventory_run_seq, coverage_generation, query_created_since, query_created_until, snapshot_digest, report_count}]}
 *   initial_marker_digest = 印の表の marker_digest (amazon-finance-initial-marker.js)
 */
import { canonicalSha256 } from '../company-db/canonical-hash.mjs';
import { receiptDigest } from '../company-db/finance/coverage-manifest.mjs';
import { selectDocumentVersions, readVersions, REPORT_TYPES, cmpUtf8 } from './amazon-settlement-versions.js';
import { normalizeApiTime, INVENTORY_SCOPE } from './amazon-settlement-inventory.js';

export const REQUIRED_REPORT_TYPE = REPORT_TYPES.v2;   // source policy (amazon_settlement_unified) で complete に要る report type (R18 M5)
export const FUTURE_SKEW_MS = 10 * 60e3;
/** 人が直すまで直らない理由 (⚠️)。ほかは ❌ (次の回・retry で直りうる) */
export const HUMAN_REASONS = new Set([
  'initial_marker_missing', 'marker_verified_from_after_origin', 'marker_captured_outside_first_window', 'marker_through_not_overlapping', 'marker_settlement_missing',
  'marker_settlement_mismatch', 'marker_report_id_unknown', 'evidence_chain_gap', 'report_cancelled', 'report_fatal', 'report_period_unknown', 'report_selected_differs',
  'header_currency_unverified', 'version_without_settlement', 'header_count', 'header_period_unreadable', 'header_period_reversed', 'header_settlement_mismatch',
  'line_settlement_mismatch', 'header_currency', 'line_currency', 'total_mismatch', 'origin_not_covered', 'report_blocked',
]);

const utc = (s) => { const v = normalizeApiTime(s); return v && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(v) ? v : null; };
const ms = (s) => Date.parse(s);
/** 実時刻 → JST の日 (YYYY-MM-DD) */
export const jstDateOfUtc = (iso) => new Date(ms(iso) + 9 * 3600e3).toISOString().slice(0, 10);
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400e3).toISOString().slice(0, 10);
const toUtcSec = (d) => d.toISOString().slice(0, 19) + 'Z';
/** 日付 (JST の日) の始まりの実時刻 */
export const jstDayStartUtc = (d) => toUtcSec(new Date(Date.parse(`${d}T00:00:00+09:00`)));
const big = (v) => (v == null ? null : String(typeof v === 'bigint' ? v : BigInt(Math.trunc(Number(v)))));

/** policy の行 (Render の status.policy.rows) → その source の起点 (JST 00:00 の実時刻)。無ければ null */
export function policyOrigin(rows, source) {
  const f = rows.filter((r) => r.source === source).map((r) => r.period_from).sort()[0];
  return f ? jstDayStartUtc(f) : null;
}

/** 採った版の見出しの検査と期間。戻り = { ok, start, end, reasons } */
function checkSelected(v, { nowMs, markerMatched }) {
  const reasons = [];
  const push = (code, detail) => reasons.push({ code, detail: `決済 ${v.settlement_id}: ${detail}` });
  if (v.detail_stale) push('version_detail_stale', '版の要約が古い (作り直す前)');
  if (v.header_count !== 1) push('header_count', `見出しが ${v.header_count} 行 (1 行だけ)`);
  const start = utc(v.header_start), end = utc(v.header_end);
  if (v.header_count >= 1 && (!start || !end)) push('header_period_unreadable', `見出しの期間が読めない (${v.header_start} 〜 ${v.header_end})`);
  if (start && end && ms(start) >= ms(end)) push('header_period_reversed', `見出しの期間が逆 (${start} 〜 ${end})`);
  if (end && ms(end) > nowMs + FUTURE_SKEW_MS) push('header_end_future', `見出しの終わりが読み取りの時点より後 (${end})`);
  if (v.header_count >= 1 && v.header_settlement_id !== v.settlement_id) push('header_settlement_mismatch', `見出しの決済 ID (${v.header_settlement_id}) が違う`);
  if ((v.line_settlement_count ?? 0) > 1 || (v.line_settlement_count === 1 && v.line_settlement_id !== v.settlement_id)) push('line_settlement_mismatch', `明細の決済 ID が 1 つでない / 違う (${v.line_settlement_count})`);
  if (v.header_count >= 1 && v.header_currency !== 'JPY') push('header_currency', `見出しの通貨が JPY でない (${v.header_currency})`);
  if (v.header_count >= 1 && v.header_currency_raw != null && v.header_currency_raw !== 'JPY') push('header_currency', `見出しの原文の通貨が JPY でない (${v.header_currency_raw})`);
  if (v.header_count >= 1 && v.header_currency_raw == null && !markerMatched) push('header_currency_unverified', '見出しの原文の通貨が無い (過去の行) のに初期の印で確かめていない');
  if ((v.line_currency_bad ?? 0) > 0) push('line_currency', `明細の原文の通貨が JPY でない行 ${v.line_currency_bad}`);
  if (v.header_count >= 1 && String(v.components_sum_micro ?? 0) !== String(v.header_total_micro ?? 'x')) push('total_mismatch', `明細の部品の合計 (${v.components_sum_micro}) ≠ 見出しの total (${v.header_total_micro})`);
  return { ok: reasons.length === 0, start, end, reasons };
}

/** 起点から途切れずにつながる最後の end (実時刻の半開区間・重なりは可・隙間で止まる)。起点を覆う区間が無ければ null */
export function frontierFrom(intervals, originIso) {
  const list = intervals.filter((x) => x.start && x.end).map((x) => ({ s: ms(x.start), e: ms(x.end), end: x.end })).sort((a, b) => a.s - b.s || a.e - b.e);
  const o = ms(originIso);
  let f = o, fIso = null;
  for (const x of list) {
    if (x.s > f) break;
    if (x.e > f) { f = x.e; fIso = x.end; }
  }
  return fIso;
}

/** 印の決済と採った見出しの一致 (期間: time = 実時刻の一致 / jst_date = 見出しの JST の日と一致) */
function markerMatches(m, v, start, end) {
  if (!v || !start || !end) return false;
  const p = m.period_precision === 'time'
    ? m.period_start === start && m.period_end === end
    : m.period_start === jstDateOfUtc(start) && m.period_end === jstDateOfUtc(end);
  return p && String(m.total_amount_micro) === String(v.header_total_micro) && m.currency === v.header_currency;
}

/**
 * 判定と manifest (送り手の走査と同じ読み取りの取引の中で)。
 * ctx = { generation, runToken, policy: { rows, fingerprint }, source, now: Date, receipts: [受領記録 lines > 0], sourceRevision (R) }
 */
export function evaluateCoverage(db, { generation, runToken, policy, source, now = new Date(), receipts, sourceRevision }) {
  const reasons = [];
  const add = (code, detail) => reasons.push({ code, detail, human: HUMAN_REASONS.has(code) });
  const nowMs = now.getTime();
  const origin = policyOrigin(policy.rows, source);
  if (!origin) add('no_policy', `policy に source ${source} が無い`);

  // ── 初期の印 (最新の epoch) ──
  const marker = db.prepare(`SELECT * FROM initial_marker_headers ORDER BY evidence_epoch DESC LIMIT 1`).get() || null;
  const markerSettlements = marker ? db.prepare(`SELECT * FROM initial_marker_settlements WHERE marker_id = ?`).all(marker.marker_id) : [];
  if (!marker) add('initial_marker_missing', '初期の印 (Seller Central の決済の一覧) が無い = 保持期間より前の決済のそろいを言えない (D-65)');
  else if (origin && ms(marker.verified_from) > ms(origin)) add('marker_verified_from_after_origin', `印の verified_from (${marker.verified_from}) が起点 (${origin}) より後`);

  // ── 採った版 ──
  const versions = readVersions(db);
  const selected = selectDocumentVersions(versions);
  const noSettle = versions.filter((v) => v.settlement_id == null && (v.raw_line_count ?? 1) > 0);
  if (noSettle.length) add('version_without_settlement', `決済 ID の決まらない版 ${noSettle.length} (例 #${noSettle[0].seq})`);
  const markerBySettlement = new Map(markerSettlements.map((m) => [m.settlement_id, m]));
  const headerRows = [], docRows = [], intervals = [];
  const perSettlement = new Map();
  for (const [sid, v] of [...selected].sort((a, b) => cmpUtf8(a[0], b[0]))) {
    const m = markerBySettlement.get(sid);
    const start0 = utc(v.header_start), end0 = utc(v.header_end);
    const mm = m ? markerMatches(m, v, start0, end0) : false;
    const c = checkSelected(v, { nowMs, markerMatched: mm });
    for (const r of c.reasons) add(r.code, r.detail);
    perSettlement.set(sid, { v, start: c.start, end: c.end, markerMatched: mm });
    if (c.ok) intervals.push({ start: c.start, end: c.end });
    headerRows.push({ settlement_id: sid, start: c.start ?? v.header_start ?? null, end: c.end ?? v.header_end ?? null, source_layer: v.source_layer, total_amount_micro: big(v.header_total_micro), currency: v.header_currency ?? null });
    docRows.push({ settlement_id: sid, document_version_id: v.document_version_id, report_id: v.report_id ?? null, report_document_id: v.report_document_id ?? null, file_hash: v.file_hash ?? null,
      header_id: big(v.header_id), detail_line_count: v.line_count ?? 0, detail_digest: v.detail_digest ?? null });
  }
  const settlementsThrough = origin ? frontierFrom(intervals, origin) : null;
  if (origin && !settlementsThrough) add('origin_not_covered', `起点 (${origin}) を覆う採った見出しが無い`);

  // ── 一覧の回 (今回 + 鎖) ──
  const runSel = `SELECT * FROM amazon_settlement_report_inventory_runs WHERE company_id = ? AND mall = ? AND scope_key = ? AND report_type = ?`;
  const scope = [INVENTORY_SCOPE.companyId, INVENTORY_SCOPE.mall, INVENTORY_SCOPE.scopeKey, REQUIRED_REPORT_TYPE];
  const good = (r) => r.completed_at && r.last_page_reached === 1 && !r.list_error && !r.ingest_error && !r.record_error && r.snapshot_digest;
  const thisRuns = db.prepare(`${runSel} AND coverage_generation = ? AND run_token = ?`).all(...scope, generation, runToken);
  const thisRun = thisRuns.length === 1 && good(thisRuns[0]) ? thisRuns[0] : null;
  if (!thisRun) add('inventory_this_run', `今回 (世代 ${generation}) の成功した一覧の回が無い (${thisRuns.length} 回${thisRuns[0] ? `・完了 ${thisRuns[0].completed_at || '-'}・最後のページ ${thisRuns[0].last_page_reached}・${thisRuns[0].list_error || thisRuns[0].ingest_error || thisRuns[0].record_error || ''}` : ''})`);
  let chain = [];
  if (marker) {
    chain = db.prepare(`${runSel} AND evidence_epoch = ? ORDER BY inventory_run_seq`).all(...scope, marker.evidence_epoch).filter(good);
    if (thisRun && (thisRun.evidence_epoch !== marker.evidence_epoch)) add('inventory_this_run_epoch', `今回の一覧の回の印の epoch (${thisRun.evidence_epoch}) が最新の印 (${marker.evidence_epoch}) と違う`);
    if (!chain.length) add('evidence_chain_gap', '最新の印の後の成功した一覧の回が無い');
    else {
      const first = chain[0];
      if (!(ms(first.query_created_since) <= ms(marker.captured_at) && ms(marker.captured_at) <= ms(first.query_created_until))) add('marker_captured_outside_first_window', `印の captured_at (${marker.captured_at}) が最初の一覧の窓 (${first.query_created_since} 〜 ${first.query_created_until}) に入らない`);
      const firstRows = db.prepare(`SELECT data_start_time s, data_end_time e FROM amazon_settlement_report_inventory WHERE inventory_run_id = ?`).all(first.id).map((r) => ({ s: utc(r.s), e: utc(r.e) })).filter((r) => r.s && r.e);
      const minS = firstRows.length ? Math.min(...firstRows.map((r) => ms(r.s))) : null, maxE = firstRows.length ? Math.max(...firstRows.map((r) => ms(r.e))) : null;
      if (minS == null || !(ms(marker.verified_from) < maxE && minS < ms(marker.verified_through))) add('marker_through_not_overlapping', `印の [${marker.verified_from}, ${marker.verified_through}) が最初の一覧のデータの期間と重ならない`);
      for (let i = 1; i < chain.length; i++) {
        if (ms(chain[i].query_created_since) > ms(chain[i - 1].query_created_until)) add('evidence_chain_gap', `一覧の回 #${chain[i - 1].inventory_run_seq} と #${chain[i].inventory_run_seq} の窓の間に空白 (${chain[i - 1].query_created_until} 〜 ${chain[i].query_created_since}) = 保持期間の間に出て消えた report を否定できない → Seller Central で印を作り直す`);
      }
      if (thisRun && chain[chain.length - 1].id !== thisRun.id) add('inventory_this_run', '今回の一覧の回が鎖の最後でない');
    }
  }

  // ── 期待の集合 ──
  const items = [];
  const byReport = new Map();   // report ID → 最新の観測 (世代 → 回 → 位置)
  if (chain.length) {
    const ids = chain.map((r) => r.id);
    const rows = db.prepare(`SELECT i.*, r.coverage_generation, r.inventory_run_seq FROM amazon_settlement_report_inventory i JOIN amazon_settlement_report_inventory_runs r ON r.id = i.inventory_run_id
       WHERE i.inventory_run_id IN (${ids.map(() => '?').join(',')})`).all(...ids);
    const key = (x) => [x.coverage_generation == null ? -1 : Number(x.coverage_generation), Number(x.inventory_run_seq), Number(x.last_seen_ordinal)];
    const later = (a, b) => { const ka = key(a), kb = key(b); for (let i = 0; i < 3; i++) if (ka[i] !== kb[i]) return ka[i] > kb[i]; return false; };
    for (const x of rows) { if (x.report_type !== REQUIRED_REPORT_TYPE) continue; const cur = byReport.get(x.report_id); if (!cur || later(x, cur)) byReport.set(x.report_id, x); }
  }
  const versionsByReportDoc = new Map();
  for (const v of versions) if (v.report_id != null) { const k = `${v.report_id}\u0000${v.report_document_id ?? ''}`; if (!versionsByReportDoc.has(k)) versionsByReportDoc.set(k, []); versionsByReportDoc.get(k).push(v); }
  const unsatisfied = { cancelled: 0, fatal: 0, not_done: 0, period: 0, not_imported: 0, differs: 0 };
  for (const [rid, x] of [...byReport].sort((a, b) => cmpUtf8(a[0], b[0]))) {
    const ds = utc(x.data_start_time), de = utc(x.data_end_time);
    const obs = String(x.id);
    if (!ds || !de || ms(ds) >= ms(de)) { unsatisfied.period++; add('report_period_unknown', `report ${rid} の期間が無い・読めない・逆 (${x.data_start_time} 〜 ${x.data_end_time}・${x.processing_status}) = 対象の外にしない`); continue; }
    if (origin && ms(de) <= ms(origin)) continue;   // 起点より前に終わる = 対象の外 (期間が分かるときだけ)
    const item = { kind: 'api_report', id: rid, observation_id: obs, result: null, document_version_id: null, period_basis: 'api', effective_data_from: ds, effective_data_to: de };
    if (x.processing_status === 'CANCELLED') { unsatisfied.cancelled++; add('report_cancelled', `report ${rid} が CANCELLED = 未充足 (当面 satisfied_empty は作らない → Seller Central で印を作り直す)`); continue; }
    if (x.processing_status === 'FATAL') { unsatisfied.fatal++; add('report_fatal', `report ${rid} が FATAL`); continue; }
    if (x.processing_status !== 'DONE' || !x.report_document_id) { unsatisfied.not_done++; add('report_not_done', `report ${rid} が ${x.processing_status} (DONE でない)`); continue; }
    const cands = (versionsByReportDoc.get(`${rid}\u0000${x.report_document_id}`) || []).filter((v) => !x.source_file_hash || v.file_hash === x.source_file_hash);
    const v = cands.sort((a, b) => b.seq - a.seq)[0];
    if (!v) {
      unsatisfied.not_imported++;
      // 並べ直しの規則に無い V2 (blocked) = 規則を足すまで直らない (人) / ほか (一覧にだけ居た・落とせなかった) = 次の回で直りうる
      const blockedRule = x.import_result === 'failed' && /^blocked: /.test(String(x.import_note || ''));
      add(blockedRule ? 'report_blocked' : 'report_not_imported', `report ${rid} (文書 ${x.report_document_id}) の版が無い = 取り込めていない (${x.import_result}${x.import_note ? `: ${String(x.import_note).slice(0, 80)}` : ''})${blockedRule ? ' → amazon-settlement-v2.js に規則を足す' : ''}`);
      continue;
    }
    const sel = v.settlement_id ? selected.get(v.settlement_id) : null;
    if (sel && sel.seq === v.seq) item.result = 'imported';
    else if (sel && !v.detail_stale && !sel.detail_stale && sel.line_count === v.line_count && sel.detail_digest === v.detail_digest) {
      // 採った版 (別の文書) で満たす = 期間は採った版の見出しの期間 (R21 M3)
      const ps = perSettlement.get(v.settlement_id);
      item.result = 'satisfied_by_selected_settlement';
      item.period_basis = 'settlement_header'; item.effective_data_from = ps ? ps.start : null; item.effective_data_to = ps ? ps.end : null;
    } else { unsatisfied.differs++; add('report_selected_differs', `report ${rid} の版 #${v.seq} と決済 ${v.settlement_id} の採った版 #${sel ? sel.seq : '-'} の明細が違う (detail_digest)`); continue; }
    item.document_version_id = v.document_version_id;
    items.push(item);
  }
  for (const m of [...markerSettlements].sort((a, b) => cmpUtf8(a.settlement_id, b.settlement_id))) {
    const ps = perSettlement.get(m.settlement_id);
    if (!ps) { add('marker_settlement_missing', `印の決済 ${m.settlement_id} (${m.period_start} 〜 ${m.period_end}) が SQLite に無い → 手で取り込む (amazon-settlement-manual-file.js)`); continue; }
    if (!ps.markerMatched) { add('marker_settlement_mismatch', `印の決済 ${m.settlement_id} の期間・金額・通貨が採った見出しと違う (印 ${m.period_start}〜${m.period_end} ${m.total_amount_micro} ${m.currency} / 見出し ${ps.start}〜${ps.end} ${ps.v.header_total_micro} ${ps.v.header_currency})`); continue; }
    if (m.report_id != null && !versions.some((v) => v.settlement_id === m.settlement_id && v.report_id === m.report_id)) { add('marker_report_id_unknown', `印の決済 ${m.settlement_id} の report ID ${m.report_id} の版が無い`); continue; }
    items.push({ kind: 'marker_settlement', id: m.settlement_id, observation_id: null, result: 'marker_matched', document_version_id: ps.v.document_version_id, period_basis: 'settlement_header', effective_data_from: ps.start, effective_data_to: ps.end });
  }
  items.sort((a, b) => cmpUtf8(a.kind, b.kind) || cmpUtf8(a.id, b.id));

  // ── receipt digest ──
  let rc = null;
  try { rc = receiptDigest(receipts || []); } catch (e) { add('receipt_digest', `受領記録の要約を作れない: ${e.message}`); }

  const results = {};
  for (const it of items) results[it.result] = (results[it.result] || 0) + 1;
  const diag = { origin, settlementsThrough, selectedSettlements: selected.size, versions: versions.length, chainRuns: chain.length, expectedItems: items.length, results, unsatisfied, markerId: marker ? marker.marker_id : null, thisRunSeq: thisRun ? thisRun.inventory_run_seq : null };
  if (reasons.length) return { ok: false, reasons, manifest: null, diag };
  const manifest = {
    complete_to: addDays(jstDateOfUtc(settlementsThrough), -1),
    settlements_through: settlementsThrough,
    source_revision: String(sourceRevision),
    headers_count: headerRows.length,
    headers_checksum: canonicalSha256({ format: 'fch-v1', headers: headerRows }),
    receipt_count: rc.count, receipt_lines: rc.lines, receipt_digest: rc.digest,
    inventory_snapshot_id: String(thisRun.inventory_run_seq),
    inventory_count: thisRun.report_count,
    inventory_digest: thisRun.snapshot_digest,
    inventory_completed_at: thisRun.completed_at,
    initial_marker_id: marker.marker_id,
    initial_marker_digest: marker.marker_digest,
    selected_documents_count: docRows.length,
    selected_documents_digest: canonicalSha256({ format: 'fsd-v1', documents: docRows }),
    evidence_chain_from: marker.verified_from,
    evidence_chain_through: thisRun.query_created_until,
    expected_report_count: items.length,
    expected_report_digest: canonicalSha256({ format: 'fer-v1', items }),
    inventory_runs_digest: canonicalSha256({ format: 'fir-v1', runs: chain.map((r) => ({ inventory_run_seq: String(r.inventory_run_seq), coverage_generation: big(r.coverage_generation), query_created_since: r.query_created_since, query_created_until: r.query_created_until, snapshot_digest: r.snapshot_digest, report_count: r.report_count })) }),
    policy_fingerprint: policy.fingerprint,
  };
  return { ok: true, reasons, manifest, diag };
}

/**
 * 完成の判定 (pipeline の r.ok は使わない = 鍵の分からない行は runPush の後で r.ok に入る・§3.1)。
 * 入力 = 送り手の回の結果・判定 (evaluateCoverage)・送った後の状態。戻り = 理由の配列 (空 = complete にしてよい)
 */
export function completionBlockers({ r, snapshot, retryLeft, dirtyLeft, sourceRevisionNow, unkeyed, pseudoBlocked }) {
  const out = [];
  const add = (code, detail) => out.push({ code, detail, human: HUMAN_REASONS.has(code) });
  if (!r || r.mode === 'range') add('range', 'range の回は complete にしない (一部の期間だけ)');
  if (r && r.dryRun) add('dry_run', 'dry-run は coverage に触れない');
  if (r && r.failed.length) add('send_failed', `送れなかった注文 ${r.failed.length}`);
  if (r && r.transformErrors.length) add('transform_errors', `整形できない注文 ${r.transformErrors.length}`);
  if (r && r.stale) add('stale', `stale ${r.stale}`);
  if (retryLeft) add('retry_keys', `読み直す鍵 (台帳) ${retryLeft}`);
  if (unkeyed) add('unkeyed', `注文番号も計上日も読めない行 ${unkeyed}`);
  if (pseudoBlocked) add('pseudo_blocked', `送れなかった疑似注文 ${pseudoBlocked}`);
  if (dirtyLeft) add('dirty_left', `読み直す注文 (読み取りの版 R 以下) が ${dirtyLeft} 残った`);
  if (!snapshot) add('no_snapshot', '走査の manifest が無い');
  else {
    for (const x of snapshot.reasons || []) out.push(x);
    if (sourceRevisionNow == null || String(sourceRevisionNow) !== String(snapshot.sourceRevision)) add('source_revision_changed', `決済の生の表の版が読み取りの後に変わった (${snapshot.sourceRevision} → ${sourceRevisionNow}) = 次の回で拾う`);
  }
  return out;
}
