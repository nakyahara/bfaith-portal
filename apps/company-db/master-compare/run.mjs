/**
 * run.mjs — 毎朝のマスタ照合 ①ロードの検証 + ②外との照合 (daily-sync の 1 ステップ。見張りの前。設計 = AI_reference CompanyDB構想/10 §6.1.1 B・C2)
 *   ② (compare-ne.mjs) は ① の後に同じ読み取りの取引で、別の try で流す = ② が落ちても ① の結果・証跡は残る (ne.verdict = error)。
 *   反映待ちの台帳 (pending.mjs) は排他を取ってから読み、② が最後まで走った回だけ新しい版を書いて HEAD を進める
 *   ②b (compare-old-tables.mjs) = 持ち主が C で NE に欄が無い列 (税区分・売上分類・送料・推奨保有月数) の C ↔ 古い表。② の後に同じ取引で、別の try で流す (④a・Codex #1564 R1 H3)。
 *     今は持ち主が全部 load = 比べない (not_applied・要約に出さない)
 *
 * 使い方 (miniPC):
 *   node apps/company-db/master-compare/run.mjs --daily [--data-dir D] [--as-of YYYY-MM-DD] [--json]
 *   (--daily は daily-sync の runScript が引数の無いときに '7' を足すのを避ける印。無くても動く)
 * env: COMPANY_DB_WATCH_URL (ロール watcher = select だけ)。無ければ「⏭️ 未設定」で exit 0。DATA_DIR (控え・証跡・全件 JSON)
 *
 * 証跡 (DATA_DIR/company-db-evidence/<日付>/master-compare.json) の順番 (Codex ③a-2 B-R0 #4 = 同じ実行 ID の古い成功を使わせない):
 *   1. 始めに state = running (この回の compare_run_id) を書いて、前の結果を無効にする。書けなければ exit 1
 *   2. 全件 JSON = DATA_DIR/cdb-master-compare/<日付>/<compare_run_id>.json (不変・35 日)
 *   3. state = complete (JSON の場所・sha256・件数・判定)。書けなければ exit 1。途中で落ちたら state = failed (書ければ)
 * 終わり方: 差がある (breach)・判定できない (blocked) も記録まで済めば exit 0 (⚠️)。exit 1 は照合そのものの失敗だけ (DB に届かない・証跡を書けない)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from '../../../scripts/company-db/migrate.mjs';
import { jstDateStr } from '../../../lib/jst-date.js';
import { writeEvidence } from '../push/evidence.mjs';
import { compareLoad, readCdbMaster, LOAD_CTX } from './compare-load.mjs';
import { compareNe, NE_FORMAT, readRegistrations, regTroubleCounts } from './compare-ne.mjs';
import { readLedger, writeLedger, acquireLock, pendingDir, lockAgeMs, markWriteFailed } from './pending.mjs';
import { readDecisionLedger, writeDecisions, writeNeCodes, connectDecisionWriter, snapshotRegTargets, writeRegistrationObservations, sealRegistrationRun, runRegistrationCheck } from './decisions.mjs';
import { readBaseline, writeBaseline, holdAllDirections } from './baseline.mjs';
import { compareOldTables, oldTablesSummary, oldTablesBad, OLD_FORMAT } from './compare-old-tables.mjs';

export const EVIDENCE_NAME = 'master-compare';
export const RESULT_DIR = 'cdb-master-compare';
export const RESULT_KEEP_DAYS = 35;
export const COMPARE_RUN_ID_RE = /^mc_\d{8}T\d{9}Z_[0-9a-f]{6}$/;

export function makeCompareRunId(now = new Date()) {
  return `mc_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;
}
export const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

/** 全件 JSON を不変で書く (同じ名前があれば書かない)。戻り値 = DATA_DIR からの相対パスと sha256 */
export function writeResultJson(dataDir, asOf, compareRunId, result) {
  const rel = path.join(RESULT_DIR, asOf, `${compareRunId}.json`);
  const file = path.join(dataDir, rel);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const buf = Buffer.from(JSON.stringify(result), 'utf8');
  const tmp = `${file}.${process.pid}.tmp`;
  try {
    fs.writeFileSync(tmp, buf, { flag: 'wx' });
    if (fs.existsSync(file)) throw new Error(`全件 JSON が既にある (上書きしない): ${rel}`);
    fs.renameSync(tmp, file);
  } finally { try { fs.rmSync(tmp, { force: true }); } catch { /* */ } }
  return { rel: rel.replace(/\\/g, '/'), sha256: sha256(buf), bytes: buf.length };
}

/** 35 日より古い日付のフォルダを消す (失敗しても投げない) */
export function pruneResults(dataDir, { now = new Date(), keepDays = RESULT_KEEP_DAYS } = {}) {
  const root = path.join(dataDir, RESULT_DIR);
  let names = [];
  try { names = fs.readdirSync(root); } catch { return; }
  const cutoff = new Date(now.getTime() - keepDays * 86400000).toISOString().slice(0, 10);
  for (const d of names) if (/^\d{4}-\d{2}-\d{2}$/.test(d) && d < cutoff) { try { fs.rmSync(path.join(root, d), { recursive: true, force: true }); } catch { /* */ } }
}

/** 最後の 1 行 (daily-sync の朝の要約に載る)。②b (古い表) は比べた朝だけ足す (⚠️ なら先頭) */
export function summaryLine(r) {
  const base0 = summaryLine12(r);
  const gr = r.ne && r.ne.gate_record, gc = r.ne && r.ne.gate_close;
  // 新商品の入口のゲートの記録 (ops.record_new_entry_gate) を書けなかった・入口を閉じられなかった朝は一言 (照合そのものは失敗にしない)
  const notes = [];
  if (gr && gr.state !== 'ok' && gr.state !== 'skipped_not_closed') notes.push(`ℹ️ 新商品のゲートの記録: ${GATE_RECORD_TEXT[gr.state] || gr.state}${gr.error ? ` (${gr.error.slice(0, 80)})` : ''}`);
  const base = notes.length ? `${base0} / ${notes.join(' / ')}` : base0;
  const old = oldTablesSummary(r.old_tables);
  const line = !old ? base : oldTablesBad(r.old_tables) ? `${old} / ${base}` : `${base} / ${old}`;
  // 入口を閉じられなかった (0058 があるのに・有無が確かめられない) = 前日の許可が残りうる = 重大 = 要約の先頭に ⚠️ (照合そのものの結果は変えない。#1641 Codex R3 High)
  return gateCloseTrouble(gc) ? `⚠️ 新商品の入口を閉じられない (前日の許可が残りうる): ${gateCloseTrouble(gc)} / ${line}` : line;
}
/** 入口を閉じられなかった (成功でも「0058 が無いと確かめた」でもない) 理由。問題なし = null */
export function gateCloseTrouble(gc) {
  if (!gc || gc.state === 'ok' || gc.state === 'not_applied') return null;
  return `${GATE_RECORD_TEXT[gc.state] || gc.state}${gc.error ? ` (${String(gc.error).slice(0, 80)})` : ''}`;
}
const GATE_RECORD_TEXT = { not_applied: '関数が無い (0058 の前)', not_configured: '書く接続が無い', no_kind_gate: '区分のゲートの数が無い', no_fetch_time: '取得の完了の時刻が読めない', failed: '書けない' };
/**
 * 照合 ② の始めに新商品の入口を閉じる (ops.close_new_entry_for_compare = PR-1 (0058) が作る)。返り値 = 状態 (照合そのものは止めない):
 *   ok = 閉じた / not_applied = 関数が無いと **確かめた** (0058 の前 = 今の本番) /
 *   not_configured = 書く接続が無い (関数はある・有無が分からない) / failed = 有無を確かめられない (接続・権限) か、閉じる関数が落ちた。
 *   🚨 not_applied は関数の有無を問い合わせて「無い」と返った時だけ (接続・権限の失敗を「0058 の前」と取り違えない。#1641 Codex R3 High)
 *   ok と not_applied 以外 = 閉じていない = 呼び手は record を呼ばない・要約の先頭に ⚠️
 * @param {(() => Promise<object>)|null} getWriter  書く接続 (watch_writer)
 * @param {{ compareRunId: string, readDb?: object|null }} p  readDb = 書く接続が無いときに関数の有無だけ確かめる読む接続
 */
export async function closeNewEntryForCompare(getWriter, { compareRunId, readDb = null }) {
  const FN = "select to_regprocedure('ops.close_new_entry_for_compare(text)') is not null as ok";
  if (!getWriter) {
    if (!readDb) return { state: 'not_configured', fn: 'unknown' };
    try { return (await readDb.query(FN)).rows[0].ok ? { state: 'not_configured', fn: 'present' } : { state: 'not_applied' }; }
    catch (e) { return { state: 'failed', stage: 'presence', error: String(e && e.message).slice(0, 200) }; }
  }
  let w, has;
  try { w = await getWriter(); has = (await w.query(FN)).rows[0].ok; }
  catch (e) { return { state: 'failed', stage: 'presence', error: String(e && e.message).slice(0, 200) }; }
  if (!has) return { state: 'not_applied' };
  try {
    const r = (await w.query('select ops.close_new_entry_for_compare($1) as r', [compareRunId])).rows[0]?.r ?? null;
    return { state: 'ok', result: r };
  } catch (e) { return { state: 'failed', stage: 'close', error: String(e && e.message).slice(0, 200) }; }
}
/** 取得の件数の記録の完了の時刻 (sync_meta = UTC の 'YYYY-MM-DD HH:MM:SS') → RFC 3339 ('…Z')。読めない = null */
export function fetchTimeRfc3339(t) {
  return typeof t === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(t) ? `${t.replace(' ', 'T')}Z` : null;
}
/**
 * 照合 ② の最後 (結果と証跡を書いた後) に、新商品の入口のゲートの記録を 1 回書く (計画 newentry_min_plan.md §3-1)。
 *   ops.record_new_entry_gate(照合の回, 単品の取得の完了 (RFC 3339), セットの取得の完了, kind_gate の 5 つの数) = PR-1 (0058) が作る。
 *   関数が無い (0058 の前)・書く接続が無い・数が無い・時刻が読めない・書けない = 状態だけ返す (照合は失敗にしない)
 */
export async function recordNewEntryGate(getWriter, { compareRunId, ne }) {
  if (!ne || !ne.kind_gate) return { state: 'no_kind_gate' };
  const fc = ne.fetch_counts || {};
  const pAt = fetchTimeRfc3339(fc.products?.complete_at), sAt = fetchTimeRfc3339(fc.setproducts?.complete_at);
  if (!pAt || !sAt) return { state: 'no_fetch_time' };
  if (!getWriter) return { state: 'not_configured' };
  try {
    const w = await getWriter();
    const has = (await w.query("select to_regprocedure('ops.record_new_entry_gate(text, text, text, jsonb)') is not null as ok")).rows[0].ok;
    if (!has) return { state: 'not_applied' };
    const r = (await w.query('select ops.record_new_entry_gate($1, $2, $3, $4::jsonb) as r', [compareRunId, pAt, sAt, JSON.stringify(ne.kind_gate)])).rows[0]?.r ?? null;
    return { state: 'ok', products_complete_at: pAt, setproducts_complete_at: sAt, result: r };
  } catch (e) { return { state: 'failed', error: String(e && e.message).slice(0, 200) }; }
}
function summaryLine12(r) {
  const one = (() => {
    if (r.verdict === 'blocked') return `⚠️ マスタ照合 ①: 判定できない (${r.blocked_reason})`;
    const c = r.counts || {};
    const parentPart = r.parent_not_compared ? '代表の親子は比べていない (0036 の前のロード)' : `代表の親子 ${c.compared?.parent ?? 0}`;
    if (r.verdict === 'pass') return `✅ マスタ照合 ①: ロード ${r.load?.ingest_run_id} の差 0 (SKU ${c.compared?.value ?? 0}・原価 ${c.compared?.cost ?? 0}・代表の仕入先 ${c.compared?.primary_supplier ?? 0}・構成の親 ${c.compared?.components ?? 0}・${parentPart})`;
    const t = c.by_type || {};
    return `⚠️ マスタ照合 ①: 差 ${c.items} 件 (無い ${t.missing ?? 0} / 値 ${t.value ?? 0} / 原価 ${t.cost ?? 0} / 代表の仕入先 ${t.primary_supplier ?? 0} / 構成 ${t.components ?? 0} / ${r.parent_not_compared ? parentPart : `代表の親子 ${t.parent ?? 0}`})`;
  })();
  if (!r.ne) return one;
  // daily-sync は要約の先頭の ⚠️ で警告を決める (isWarnSummary) → ② が落ちた・判定できない朝は ② を先頭に (① が ✅ でも見出しを ⚠️ に)
  const two = neSummary(r.ne);
  const bad = r.ne.verdict === 'error' || r.ne.verdict === 'blocked' || ['locked', 'untrusted', 'write_failed'].includes(r.ne.pending?.state) || r.ne.decisions_write === 'failed' || r.ne.decisions_write === 'not_configured'
    || baselineTrouble(r.ne) || regTrouble(r.ne) || kindTrouble(r.ne);
  return bad ? `${two} / ${one}` : `${one} / ${two}`;
}
/** 区分の持ち主が C で、完全な NE の取得と Company DB の区分が違う SKU がある (差を残す承認でも消えない) = 要約の先頭に ⚠️ (重大。広げる道 v8・Codex R8) */
export function kindTrouble(ne) {
  const k = ne && ne.sku_kind_raw_mismatch;
  if (!k || !k.alert || !(k.count > 0)) return null;
  return `区分が NE と違う SKU ${k.count} 件 (NE の画面で直す: ${k.codes.slice(0, 10).join(', ')}${k.count > 10 ? ' ほか' : ''})`;
}
/** 最後に一致した値 (D2) を読めない・書けない・拒まれた・書く接続が無い = 要約の先頭に ⚠️ (切替の前でも黙って止めない) */
export function baselineTrouble(ne) {
  const b = ne && ne.baseline;
  if (!b || b.state === 'not_applied') return null;
  if (b.state === 'unreadable') return `基準を読めない (${String(b.held_reason || '').slice(0, 80)})`;
  if (b.write === 'rejected') return `基準を書けない (拒まれた: ${b.write_code}) — 方向は全部保留`;
  if (b.write === 'failed') return `基準を書けない (${String(b.write_error || '').slice(0, 80)})`;
  if (b.write === 'not_configured') return '基準を書けない (書く接続が無い: COMPANY_DB_WATCH_WRITER_URL)';
  if (b.state === 'held' && /^(stale_observation|baseline_without_mark|generation_unreadable)/.test(b.held_reason || '')) return `基準と照らせない (${b.held_reason})`;
  return null;
}
/** ② の要約 (朝の要約の 2 つめ)。切替までは NE との差は全部 info = 「判断待ち・反映待ち」の件数を出すだけ */
export function neSummary(ne) {
  if (ne.verdict === 'error') return `⚠️ ②: 照合が落ちた (${String(ne.error || '').slice(0, 120)})`;
  if (ne.verdict === 'blocked') return `⚠️ ②: 判定できない (${ne.blocked_reason})`;
  // 反映待ちの台帳が使えない朝 = 反映待ちの判定は全部保留。人が確かめる (README の手順)
  // 台帳 (0032) があるのに書く接続が無い = 候補・完了が何も残らない = 書けないのと同じ (Codex #1475 R1 Medium)
  if (ne.decisions_write === 'not_configured') return `⚠️ ②: 判断の台帳を書けない (書く接続が無い: COMPANY_DB_WATCH_WRITER_URL) — 差 ${ne.counts?.items ?? 0} 件`;
  if (ne.decisions_write === 'failed') return `⚠️ ②: 判断の台帳を書けない (${String(ne.decisions_write_error || '').slice(0, 100)}) — 差 ${ne.counts?.items ?? 0} 件・入れ直し = replay-decisions.mjs`;
  if (['locked', 'untrusted', 'write_failed'].includes(ne.pending?.state)) return `⚠️ ②: 反映待ちの台帳が使えない (${ne.pending.state}: ${ne.pending.reason ?? ''}) — 差 ${ne.counts?.items ?? 0} 件・保持 ${ne.counts?.held ?? 0}`;
  const bt = baselineTrouble(ne);
  if (bt) return `⚠️ ②: ${bt} — 差 ${ne.counts?.items ?? 0} 件`;
  // ポータルで登録した新商品の NE 登録の不一致 (区分違い・取り込んだのに NE に無い・NE の中身が違う) = 自動の知らせがほかに無い → 朝の要約の先頭に ⚠️
  const rt = regTrouble(ne);
  // 区分の差 (区分の持ち主が C) と新商品の登録の不一致は両方とも先頭に (片方で片方を隠さない。区分を先に)
  const kt = kindTrouble(ne);
  // 0063 の知らせ (配ってから日がたっても NE に無い・NE にあるが申告が要る) は区分違いの朝も出す (#1659 Codex R1 Medium = rt が無いと消えていた)
  if (rt || kt) { const an = notImportedNote(ne); return `⚠️ ②: ${[kt, rt].filter(Boolean).join(" / ")} — 差 ${ne.counts?.items ?? 0} 件${rt ? regSummary(ne) : an ? `・${an}` : ""}`; }
  const b = ne.counts?.by_class || {};
  const top = Object.entries(b).filter(([k]) => k !== 'match').sort((x, y) => y[1] - x[1]).slice(0, 4).map(([k, v]) => `${k} ${v}`).join(' / ');
  // 基準 (D2) を照らさなかった回 (NE の取得と CDB の読みが 4 時間超) は ⚠️ にしないが、続くと基準が貯まらないので見えるようにする
  const gap = ne.baseline?.held_reason === 'gap' ? '・基準は照らさず (NE の取得と CDB の読みが 4 時間超)' : '';
  const reg = regSummary(ne);
  if (ne.verdict === 'pass') return ((ne.counts?.held ?? 0) > 0 ? `ℹ️ ②: 判明した差 0・比べられない / 判定できない案件 ${ne.counts.held} (保持)` : '✅ ②: NE との差 0') + reg + gap;
  return `ℹ️ ②: NE との差 ${ne.counts?.items ?? 0} 件 (${top})・判断の一覧 ${ne.counts?.decisions ?? 0}・保持 ${ne.counts?.held ?? 0}${reg}${gap}`;
}
/**
 * ポータルで登録した新商品で NE に無いもの (差にしない NE 登録待ち・日がたった reg_stale は差の件数にも入る)。0 件なら何も足さない。
 * 登録の状態を読めない朝 = 待ちを分けていない (差に含む) と書く。
 * 0063: 配ったが申告していない商品は照合の確かめが自動で NE 確認済みにする。配ってから日がたっても NE に無い商品は確かめの答えの not_imported で
 *   「取り込まれていないらしい」を ℹ️ で足す (失敗にはしない)
 */
const STAGE_JA = { before_issue: 'CSV を配る前', issued: '配った', declared: '取り込んだ申告の後', failed: '取り込めていない', rejected: 'NE が全部拒んだ', partial: '中身が違う', verified: '確かめ済み' };
export function regSummary(ne) {
  const ni = notImportedNote(ne);
  const rp = ne && ne.reg_pending;
  if (!rp) return ni ? `・${ni}` : '';
  if (rp.state === 'unreadable') return `・NE 登録待ちを読めない (差に含む)${ni ? `・${ni}` : ''}`;
  const c = ne.counts || {};
  const parts = [];
  if (c.reg_pending) {
    // 段階 (CSV を配る前・配った・申告の後・取り込めていない)
    const st = Object.entries(rp.stages || {}).map(([k, v]) => `${STAGE_JA[k] || k} ${v}`).join('・');
    parts.push(`NE 登録待ち ${c.reg_pending} 件 (差に入れない${st ? `。${st}` : ''})`);
  }
  if (c.reg_stale) parts.push(`登録から ${rp.stale_days} 日以上 NE に無い ${c.reg_stale} 件 (差)`);
  if (ni) parts.push(ni);
  return parts.length ? `・${parts.join('・')}` : '';
}
/**
 * 0063 の確かめの答えの知らせ (知らせるだけ = 照合は失敗にしない)。無ければ null。コードは 5 件まで
 *   not_imported = 配ってから日がたっても NE に無い (申告なし・「無い」を信じてよい取得) = 取り込まれていないらしい
 *   needs_declaration = NE にあるが自動で確かめない (セット・JAN を送った・配った時の印が無い = #1659 Codex R1) = 申告すると確かめる
 */
const AUTO_BLOCK_JA = { jan_not_compared: 'JAN', set_not_compared: 'セット', no_issue_lease: '配った時の印なし', no_row: '行なし', no_item: '品目なし' };
export function notImportedNote(ne) {
  const w = ne && ne.registrations && ne.registrations.written;
  const codesOf = (list, label) => list.slice(0, 5).map((x) => x && x.code && (label ? `${x.code} ${label(x)}` : x.code)).filter(Boolean).join('・') + (list.length > 5 ? ` ほか ${list.length - 5}` : '');
  const parts = [];
  const ni = w && Array.isArray(w.not_imported) ? w.not_imported : [];
  // 0065 (設計 v7 §⑤): NE の取り込みが「N件失敗」だったのに申告しないと、入らなかった商品は待ちのまま (作り直せない) = 申告を促す
  if (ni.length) parts.push(`ℹ️ 配ってから ${w.not_imported_days ?? 3} 日たっても NE に無い (取り込まれていないらしい) ${ni.length} 件 (${codesOf(ni)})。NE の結果が「N件失敗」なら元のファイルで「一部失敗」を申告`);
  const nd = w && Array.isArray(w.needs_declaration) ? w.needs_declaration : [];
  if (nd.length) parts.push(`ℹ️ NE にあるが自動では確かめない (取り込んだと申告すると確かめる) ${nd.length} 件 (${codesOf(nd, (x) => AUTO_BLOCK_JA[x.reason] || x.reason)})`);
  // 0065 3 者一致: NE は配った値と合うが、今の Company DB の値 (代表 (親)) が違う = 起きないはずの事故 (配った後に代表は変えない) = 知らせる
  const cd = w && Array.isArray(w.cdb_drift) ? w.cdb_drift : [];
  if (cd.length) parts.push(`⚠️ NE は配った値と合うが Company DB の代表 (親) が違う (確認済みにしない) ${cd.length} 件 (${codesOf(cd)})`);
  return parts.length ? parts.join('・') : null;
}
/**
 * 確かめ (record_ne_registration_check) の後に登録の段階を読み直す (照合の読む接続・読み取りだけの短い取引)。
 * 区分違いは照合の結果のまま (二重に数えない)。読めない = state unreadable (確かめの前の数に戻さない = 要約・W13 は「読めない」で ⚠️ / blocked。#1635 Codex R3)
 */
export async function regAfterCheck(db, ne, checkCounts) {
  try {
    await db.query('begin transaction read only');
    try {
      const fresh = await readRegistrations(db);
      if (fresh.state !== 'ok') return { state: fresh.state === 'not_applied' ? 'unreadable' : fresh.state, check: checkCounts, reason: fresh.reason ?? fresh.state };
      return { state: 'ok', check: checkCounts, ...regTroubleCounts(fresh, new Set((ne.reg_pending?.kind_mismatch || []).map((e) => e.norm))) };
    } finally { try { await db.query('rollback'); } catch { /* */ } }
  } catch (e) { return { state: 'unreadable', check: checkCounts, reason: String(e && e.message).slice(0, 200) }; }
}
/** 確かめの後の読み直しを読めない (確かめが状態を進めたかもしれない = 確かめの前の数で ✅ にしない)。確かめの内訳は参考に出す */
export function regAfterCheckTrouble(ne) {
  const a = ne && ne.reg_after_check;
  if (!a || a.state === 'ok' || a.state === 'skipped') return null;   // skipped = 確かめが何も変えていない (確かめの前の数のまま正しい)
  const ck = a.check && typeof a.check === 'object' ? Object.entries(a.check).map(([k, v]) => `${k} ${v}`).join('・') : '';
  return `新商品の NE 登録の確かめの後の数を読めない (${String(a.reason || a.state).slice(0, 80)})${ck ? ` — 確かめ: ${ck}` : ''}`;
}
/** ポータルで登録した新商品の NE 登録の不一致 (朝の要約の先頭の ⚠️)。確かめの後に読み直した数があればそちら。読み直せなかった = その旨。無ければ null */
export function regTrouble(ne) {
  const bad = regAfterCheckTrouble(ne);
  if (bad) return bad;
  const a = ne && ne.reg_after_check && ne.reg_after_check.state === 'ok' ? ne.reg_after_check : null;
  const c = { ...((ne && ne.counts) || {}), ...(a ? { reg_failed: a.reg_failed, reg_rejected: a.reg_rejected, reg_partial: a.reg_partial } : {}) };
  const parts = [];
  if (c.reg_kind_mismatch) parts.push(`区分 (単品・セット) 違い ${c.reg_kind_mismatch} 件`);
  if (c.reg_failed) parts.push(`取り込んだと申告したのに NE に無い ${c.reg_failed} 件`);
  if (c.reg_rejected) parts.push(`NE が取り込みを全部拒んだ (CSV を作り直す) ${c.reg_rejected} 件`);
  if (c.reg_partial) parts.push(`NE の中身が登録と違う ${c.reg_partial} 件`);
  return parts.length ? `新商品の NE 登録の不一致 (${parts.join('・')})` : null;
}

/**
 * 1 回の照合 (証跡 → 接続 → 照合 → 全件 JSON → 証跡)。db = { query } (pg の client でも PGlite でも)。
 * 🚨 「実行中」の証跡は**接続より前**に書く (接続・初期設定の失敗でも、同じ実行 ID の前の回の complete を残さない。Codex #1456 R1 High-1)
 * @param {object} p
 * @param {{ query: Function }} [p.db]  もう開いた接続 (試験)
 * @param {() => Promise<{ db: { query: Function }, close?: Function }>} [p.connect]  接続を開く (本番)
 * @returns {{ result: object, evidence: object, line: string }}
 */
export async function runCompare({ db = null, connect = null, dataDir, asOf, now = new Date(), compareRunId = makeCompareRunId(now), compare = compareLoad, write = writeEvidence,
  neCompare = compareNe, syncRunId = process.env.DAILY_SYNC_RUN_ID || null, writerDb = null, connectWriter = null, cdbReadAt = null, oldCompare = compareOldTables }) {
  const startedAt = now.toISOString();
  if (!write(dataDir, EVIDENCE_NAME, { state: 'running', compare_run_id: compareRunId, as_of: asOf, started_at: startedAt })) {
    throw new Error('証跡 (実行中) を書けない = 前の回の結果を無効にできない');
  }
  let result, close = null;
  // 書く接続 (watch_writer) は 1 本を、回の始まりの写し・判断の台帳・基準・新規登録の確かめで使う (完了の証跡の後の確かめまで開いておく)
  let wconn = null;
  const writer = async () => writerDb || (wconn ??= await connectWriter()).db;
  const closeWriter = async () => { const c = wconn; wconn = null; if (c && c.close) { try { await c.close(); } catch { /* */ } } };
  let gateClose = null;
  try {
    if (!db) {
      if (!connect) throw new Error('接続が無い (db か connect が要る)');
      const c = await connect();
      db = c.db; close = c.close || null;
    }
    // 新商品の入口を閉じる (計画 newentry_min_plan.md §3 の 0): 何かを読む前に 1 回 (読む接続を開くだけ = まだ何も読んでいない)。最後の ops.record_new_entry_gate と同じ回の番号。
    //   閉じられなかった (0058 があるのに) = record を呼ばない・要約の先頭に ⚠️。0058 が無いと確かめた = 今までどおり。どちらも照合そのものは止めない
    if (neCompare) gateClose = await closeNewEntryForCompare(writerDb || connectWriter ? writer : null, { compareRunId, readDb: db });
    // ② の台帳は排他を取ってから読む (取れなければ台帳を使う判定は blocked = pending_locked。C2 v6-3)
    const release = neCompare ? (() => { try { return acquireLock(pendingDir(dataDir, RESULT_DIR)); } catch { return null; } })() : null;
    let pendingEntries = null, ledger = null, decisionLedger = null, decisionsDone = [], baselineWrites = [], neCodes = null, regObs = null, regRead = null;
    // 新商品の NE 登録の CSV の確かめ待ち (0053) = 回の始まりに DB が回へ写す (読み取りの取引の前・#1571 Codex R2 Medium 1)。写せない = 送らない (② は続ける)
    if (neCompare) regRead = await snapshotRegTargets(writerDb || connectWriter ? writer : null, { compareRunId });
    try {
      await db.query('begin transaction isolation level repeatable read read only');
      try {
        // CDB の読みの時刻 = 取引の最初の文 (snapshot と同じ時点。D2 契約 v2)。試験は cdbReadAt で固定する
        const readAt = new Date(cdbReadAt ?? (await db.query('select clock_timestamp() as t')).rows[0].t).toISOString();
        result = await compare({ db, dataDir, asOfJst: asOf });
        if (neCompare) {
          try {
            ledger = release ? readLedger(dataDir, RESULT_DIR)
              : { state: 'locked', reason: `pending/.lock がある (${Math.round((lockAgeMs(pendingDir(dataDir, RESULT_DIR)) ?? 0) / 60000)} 分前)`, head: null, entries: new Map() };
            const ctx = result[LOAD_CTX] || null;
            const cdb = ctx?.cdb ?? await readCdbMaster(db);
            // 判断の台帳 (D1) も同じ読み取りの取引で読む (読めない = ② は blocked。表が無い = 今までどおり)
            decisionLedger = await readDecisionLedger(db);
            // 最後に一致した値 (D2) も同じ取引で (読めない = 方向は全部 held・① と ② は続く)
            const baseline = { ...(await readBaseline(db)), cdbReadAt: readAt };
            // ポータルで登録した新商品の登録の状態 (0052) も同じ取引で (読めない = NE 登録待ちを分けない = 今までどおり差に出す・報告に残す)
            const registrations = await readRegistrations(db);
            const r2 = neCompare({ dataDir, asOfJst: asOf, syncRunId, loadCtx: ctx, cdb, ledger, loadVerdict: result.verdict, decisionLedger, baseline,
              regTargets: regRead.state === 'ok' ? regRead.targets : null, registrations });
            result.ne = r2.result; pendingEntries = r2.pendingEntries; decisionsDone = r2.decisionsDone || []; baselineWrites = r2.baselineWrites || []; neCodes = r2.neCodes || null;
            regObs = r2.regObs || null;
            if (pendingEntries) result.ne.pending_entries = pendingEntries;   // 台帳の保存に失敗した回の復旧の元 (restore-pending.mjs)
          } catch (e) {
            result.ne = { format: NE_FORMAT, verdict: 'error', error: String(e && e.message).slice(0, 300) };   // ① は残す
          }
        }
        // ②b 古い表 (同じ取引の C を読む。落ちても ①・② は残す)
        if (oldCompare) {
          try { result.old_tables = await oldCompare({ db, dataDir, asOfJst: asOf, syncRunId, cdbReadAt: readAt, compareRunId, now }); }
          catch (e) { result.old_tables = { format: OLD_FORMAT, verdict: 'error', error: String(e && e.message).slice(0, 300) }; }
        }
      } finally { try { await db.query('rollback'); } catch { /* */ } }
      // 台帳 = ② が最後まで走った回 (判定・blocked) で、台帳が信用できるときだけ新しい版 → HEAD
      if (result.ne && pendingEntries && ledger && (ledger.state === 'ok' || ledger.state === 'initial')) {
        try { const w = writeLedger(dataDir, RESULT_DIR, { compareRunId, ledger, entries: pendingEntries, now }); result.ne.pending = { ...result.ne.pending, written: { compare_run_id: w.compare_run_id, sha256: w.sha256 } }; }
        catch (e) {
          // 保存に失敗 = 今回の新しい期限が残らない → 印を残して次の回を untrusted に (期限を後ろへずらさない)。要約の先頭に ⚠️ (Codex #1464 R4 Medium 4)
          const msg = String(e && e.message).slice(0, 200);
          const marked = markWriteFailed(dataDir, RESULT_DIR, { compare_run_id: compareRunId, error: msg });
          result.ne.pending = { ...result.ne.pending, state: 'write_failed', reason: `台帳を保存できない (${msg})${marked ? '' : '・失敗の印も書けない'}`, write_error: msg };
        }
      }
    } finally { if (release) release(); }
    let j = null;
    let evidence = null;
    try {
    // 判断の台帳に候補と完了を書く (取引の後・別の接続 = watch_writer。表へ直接は書けない = 関数だけ。D1 契約 v3)
    if (result.ne && result.ne.verdict !== 'error') {
      result.ne.decisions_observed = { compare_run_id: compareRunId, observed_at: startedAt };   // 入れ直し (replay-decisions.mjs) の元
      if (!decisionLedger || decisionLedger.state !== 'ok') result.ne.decisions_write = decisionLedger ? decisionLedger.state : 'not_applied';
      else if (result.ne.verdict === 'blocked') result.ne.decisions_write = 'skipped_blocked';
      else if (!writerDb && !connectWriter) result.ne.decisions_write = 'not_configured';
      else {
        try {
          const x = await writeDecisions(await writer(), { compareRunId, observedAt: startedAt, decisions: result.ne.decisions || [], done: decisionsDone });
          Object.assign(result.ne, { decisions_write: 'ok', decisions_written: x });
        } catch (e) {
          Object.assign(result.ne, { decisions_write: 'failed', decisions_write_error: String(e && e.message).slice(0, 200) });   // 翌朝また足す (冪等)・失敗した回は replay-decisions.mjs で入れ直せる
        }
      }
    }
    // NE のコードの元の書き方 (③b-1b 契約 v3): 判断の台帳にこの回が書けたときだけ (照合の回の記録がある = 印の外部キー)。読めなかった回・書けなかった回は書かない = CSV は元の書き方を使わない
    if (result.ne && result.ne.ne_codes) {
      const nc = result.ne.ne_codes;
      if (result.ne.decisions_write !== 'ok') nc.write = `skipped_decisions_${result.ne.decisions_write ?? 'none'}`;
      else if (!neCodes || !neCodes.ok) nc.write = `skipped_${neCodes ? neCodes.reason : 'none'}`;
      else {
        try { nc.written = await writeNeCodes(await writer(), { compareRunId, entries: neCodes.entries }); nc.write = 'ok'; }
        catch (e) { Object.assign(nc, { write: 'failed', write_error: String(e && e.message).slice(0, 200) }); }
      }
    }
    // 新商品の NE 登録の CSV の確かめ (0053・契約 v3 H5・#1571 Codex R1 High 2) の 1 段目 = NE の観測を DB に残すだけ (状態は変えない)。
    //   判断の台帳にこの回が書けたときだけ (照合の回の記録 = 外部キー)。② が最後まで走った回 = NE の完全な取得。
    //   2 段目 (完了の受け取り = receipt) は、この回が最後まで終わって結果の JSON を書いた後。3 段目 (確かめ = 状態を進める) は完了の証跡の後
    if (result.ne && result.ne.verdict !== 'error') {
      const rg = result.ne.registrations || (result.ne.registrations = {});
      if (regRead && regRead.snapshot) rg.snapshot = { state: regRead.snapshot.state, target_hash: regRead.snapshot.target_hash, targets: regRead.targets.length };   // 回の始まりの写し
      if (regRead && regRead.reason) rg.snapshot_error = regRead.reason;
      if (!regRead || regRead.state !== 'ok') rg.write = `skipped_${regRead ? regRead.state : 'none'}`;
      else if (!regObs) rg.write = `skipped_${result.ne.verdict === 'blocked' ? 'blocked' : 'none'}`;
      else if (!regObs.observations.length) rg.write = 'nothing';
      else if (result.ne.decisions_write !== 'ok') rg.write = `skipped_decisions_${result.ne.decisions_write ?? 'none'}`;
      else {
        try { rg.observed = await writeRegistrationObservations(await writer(), { compareRunId, regObs }); rg.write = 'observed'; }
        catch (e) { Object.assign(rg, { write: 'failed', write_error: String(e && e.message).slice(0, 200) }); }
      }
    }
    // 最後に一致した値 (D2) を書く (取引の後・watch_writer・関数だけ・1 回 = 1 取引。変更ゼロでも札を照らして進める)
    const bs = result.ne && result.ne.baseline;
    if (bs && bs.state !== 'not_applied') {
      if (bs.state !== 'ok') bs.write = `skipped_${bs.state}`;
      else if (!writerDb && !connectWriter) bs.write = 'not_configured';   // 読めた基準からの方向は残す (札の検証は通っていない = 区別。D2-R1)
      else {
        try {
          bs.written = await writeBaseline(await writer(), { compareRunId, expectedMark: bs.expected_mark, generation: bs.generation, units: baselineWrites });
          bs.write = 'ok';
        } catch (e) {
          const msg = String(e && e.message).slice(0, 200);
          const code = (msg.match(/^(mark_moved|stale_run|unit_conflict|run_reused|continuation_mismatch|baseline_without_mark|norm_version_rejected|invalid_input)\b/) || [])[1] || null;
          Object.assign(bs, { write: code ? 'rejected' : 'failed', write_code: code, write_error: msg, written: { inserted: 0, updated: 0, units: 0 } });
          if (code) holdAllDirections(result.ne);   // 拒まれた回 = 古い基準で出した方向を使わない (D2-R1 M3)
        }
      }
    }
    Object.assign(result, { compare_run_id: compareRunId, started_at: startedAt, finished_at: new Date().toISOString() });
    j = writeResultJson(dataDir, asOf, compareRunId, result);
    const regEvidence = () => (result.ne && result.ne.registrations ? { targets: result.ne.registrations.targets ?? null, write: result.ne.registrations.write ?? null,
      seal: result.ne.registrations.seal ?? null, counts: result.ne.registrations.written?.counts ?? null,
      not_imported: Array.isArray(result.ne.registrations.written?.not_imported) ? result.ne.registrations.written.not_imported.length : null,   // 0063: 配ってから日がたっても NE に無い (知らせるだけ)
      needs_declaration: Array.isArray(result.ne.registrations.written?.needs_declaration) ? result.ne.registrations.written.needs_declaration.length : null,   // 0063: NE にあるが申告が要る
      cdb_drift: Array.isArray(result.ne.registrations.written?.cdb_drift) ? result.ne.registrations.written.cdb_drift.length : null,   // 0065: NE は合うが Company DB が違う (3 者一致でない)
      write_error: result.ne.registrations.write_error ?? null } : null);
    evidence = {
      state: 'complete', compare_run_id: compareRunId, as_of: asOf, started_at: startedAt, finished_at: result.finished_at,
      json_path: j.rel, sha256: j.sha256, bytes: j.bytes, format: result.format,
      verdict: result.verdict, blocked_reason: result.blocked_reason, counts: result.counts,
      load: result.load ? { ingest_run_id: result.load.ingest_run_id, started_at: result.load.started_at } : null,
      materials: result.materials,
      ne: result.ne ? { verdict: result.ne.verdict, blocked_reason: result.ne.blocked_reason ?? null, error: result.ne.error ?? null, counts: result.ne.counts ?? null,
        decisions_read: result.ne.decisions_read ?? null, decisions_write: result.ne.decisions_write ?? null,
        ne_codes: result.ne.ne_codes ? { state: result.ne.ne_codes.state, reason: result.ne.ne_codes.reason ?? null, counts: result.ne.ne_codes.counts ?? null, write: result.ne.ne_codes.write ?? null } : null,
        registrations: regEvidence(),
        // 新商品の確かめがこの後に走る回 = 確かめの前の完了の証跡には「確かめの後の数はまだ」の印 (fail-closed)。最後の書き直しが成功したときだけ ok / skipped に変わる
        //   (書き直しも failed の書き込みも落ちた = 印が残る = W13:ne は blocked。#1635 Codex R4)
        ...(result.ne.registrations && result.ne.registrations.write === 'observed' ? { reg_after_check: { state: 'pending' } } : {}),
        // 区分の差の生の数 (承認で減らさない。広げる道 v8)
        sku_kind_raw_mismatch: result.ne.sku_kind_raw_mismatch ?? null,
        sku_kind_raw_mismatch_count: result.ne.sku_kind_raw_mismatch_count ?? null,
        kind_gate: result.ne.kind_gate ?? null,
        fetch_counts: result.ne.fetch_counts ?? null,   // 今朝の取得の件数の状態 (区分のゲートの integrity_untrusted の内訳)
        baseline: result.ne.baseline ? { state: result.ne.baseline.state, held_reason: result.ne.baseline.held_reason ?? null, write: result.ne.baseline.write ?? null, write_code: result.ne.baseline.write_code ?? null,
          counts: result.ne.baseline.counts ?? null, written: result.ne.baseline.written ?? null } : null } : null,
      // ②b 古い表 (由来 = 作り直しの ID・写しの世代。比べない朝は not_applied と理由だけ)
      old_tables: result.old_tables ? { verdict: result.old_tables.verdict, reason: result.old_tables.reason ?? null, error: result.old_tables.error ?? null, cols: result.old_tables.cols ?? [],
        counts: result.old_tables.counts ?? null, build: result.old_tables.build ?? null, publish: result.old_tables.publish ?? null, pending: result.old_tables.pending ?? null } : null,
    };
    if (!write(dataDir, EVIDENCE_NAME, evidence)) throw new Error('証跡 (完了) を書けない');
    // 新規登録の確かめ (#1571 Codex R1 High 2) の 2 段目 = 回が最後まで終わった受け取り (receipt)。結果の JSON (j.sha256) と完了の証跡を書けた後だけ。
    //   基準の書き込みが失敗した / 拒まれた回は書かない。3 段目 = 確かめ (状態を進める) は受け取りを書けた回だけ。DB の関数は回の番号だけを受け、受け取りと残した観測を自分で読む。
    //   どちらが落ちても回は完了のまま (NE 確認済みには進めない = 翌朝の回でもう一度)。途中で落ちた回 (受け取りの前) は確かめない
    const rg = result.ne && result.ne.registrations;
    if (rg && rg.write === 'observed') {
      const bsw = result.ne.baseline && result.ne.baseline.write;
      if (bsw === 'failed' || bsw === 'rejected') rg.seal = `skipped_baseline_${bsw}`;
      else {
        try { rg.sealed = await sealRegistrationRun(await writer(), { compareRunId, observationHash: rg.observed.observation_hash, evidenceSha256: j.sha256 }); rg.seal = 'ok'; }
        catch (e) { Object.assign(rg, { seal: 'failed', seal_error: String(e && e.message).slice(0, 200) }); }
      }
      if (rg.seal === 'ok') {
        try { rg.written = await runRegistrationCheck(await writer(), { compareRunId }); rg.write = 'ok'; }
        catch (e) { Object.assign(rg, { write: 'check_failed', write_error: String(e && e.message).slice(0, 200) }); }
      }
      // 確かめで今日 failed / partial になった商品も今日の要約・証跡 (W13 が読む) に出す = 登録の段階を読み直して数え直す (全件 JSON は不変のまま。#1635 Codex R2 Medium)
      //   確かめの関数を呼ばなかった (封が無い・落ちた) = skipped (何も変えていない = 確かめの前の数のまま正しい)
      //   関数を呼んだ後は成功・例外のどちらでも読み直す (commit の後に応答だけ失われた = check_failed でも状態は進んでいるかもしれない。#1635 Codex R5)
      result.ne.reg_after_check = rg.seal === 'ok' ? await regAfterCheck(db, result.ne, rg.written?.counts ?? null)
        : { state: 'skipped', reason: `seal_${rg.seal ?? 'none'}` };
      evidence.ne.registrations = regEvidence();
      evidence.ne.reg_after_check = result.ne.reg_after_check;
      // 確かめの後の証跡を書けない = W13 が確かめの前の数を読む → 回を失敗にする (外の catch が state = failed を書く = W13 は blocked・daily-sync は再試行。#1635 Codex R3)
      let rewritten = null;
      try { rewritten = write(dataDir, EVIDENCE_NAME, evidence); } catch { rewritten = null; }
      if (!rewritten) throw new Error('証跡 (新商品の確かめの後) を書けない = 見張りが確かめの前の数を読む');
    }
    // 新商品の入口のゲートの記録 (計画 newentry_min_plan.md §3-1)。結果と証跡を書いた後に 1 回・失敗しても照合は失敗にしない (要約に一言)
    if (result.ne && gateClose) result.ne.gate_close = gateClose;
    if (result.ne && gateClose && gateClose.state !== 'ok' && gateClose.state !== 'not_applied') {
      result.ne.gate_record = { state: 'skipped_not_closed' };   // 閉じていない回の結果を許可の材料にしない (#1641 Codex R3 High)
    } else if (result.ne && result.ne.verdict !== 'error' && result.ne.verdict !== 'blocked') {
      result.ne.gate_record = await recordNewEntryGate(writerDb || connectWriter ? writer : null, { compareRunId, ne: result.ne });
    }
    } finally { await closeWriter(); }
    pruneResults(dataDir, { now });
    return { result, evidence, line: summaryLine(result) };
  } catch (e) {
    write(dataDir, EVIDENCE_NAME, { state: 'failed', compare_run_id: compareRunId, as_of: asOf, started_at: startedAt, error: String(e && e.message).slice(0, 300) });
    throw e;
  } finally {
    await closeWriter();   // 読み取りの途中で落ちた回も (回の始まりの写しで開いた接続)
    if (close) { try { await close(); } catch { /* */ } }
  }
}

/** 本番の接続 (watcher ロール・60 秒・読むだけ)。初期設定に失敗したら閉じてから投げる */
export async function connectWatcher(url) {
  const client = await openPgClient(url);
  try {
    await client.query(`set statement_timeout = '60s'`);
    await client.query('set default_transaction_read_only = on');
  } catch (e) { try { await client.end(); } catch { /* */ } throw e; }
  return { db: pgAdapter(client), close: () => client.end() };
}

export function parseArgs(argv) {
  const out = { dataDir: null, asOf: null, json: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--as-of') out.asOf = argv[++i];
    else if (a === '--json') out.json = true;
    else if (a === '--daily' || a === '7') { /* daily-sync の印 */ }
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.asOf && !/^\d{4}-\d{2}-\d{2}$/.test(out.asOf)) throw new Error('--as-of は YYYY-MM-DD');
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1, last = '';
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const asOf = a.asOf || jstDateStr(new Date());
    const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) {
      // 未設定でも前の回の結果は無効にする (同じ実行 ID の古い complete を見張りに使わせない)
      writeEvidence(dataDir, EVIDENCE_NAME, { state: 'skipped', as_of: asOf, reason: 'COMPANY_DB_WATCH_URL が無い' });
      last = '⏭️ マスタ照合 ①: 未設定 (COMPANY_DB_WATCH_URL)';
      code = 0;
    } else {
      const wurl = (process.env.COMPANY_DB_WATCH_WRITER_URL || '').trim();   // 判断の台帳を書く (無ければ書かない = not_configured)
      const r = await runCompare({ connect: () => connectWatcher(url), dataDir, asOf, connectWriter: wurl ? () => connectDecisionWriter(wurl) : null });
      if (a.json) console.log(JSON.stringify({ evidence: r.evidence }, null, 1));
      last = r.line;
      code = 0;
    }
  } catch (e) {
    last = `❌ マスタ照合 ①: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`;
    code = 1;
  }
  console.log(String(last).replace(/\s+/g, ' '));
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
