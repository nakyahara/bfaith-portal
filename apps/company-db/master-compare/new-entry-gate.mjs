/**
 * new-entry-gate.mjs — 毎朝の照合 ② の次の 1 段「新商品の許可」(daily-sync の 1 段・新しい定期実行ではない。計画 newentry_min_plan.md §3 の 2・PR-7)
 *
 *   その朝の照合 ② の回 (証跡 master-compare の compare_run_id) で ops.grant_new_entry_lease('single', 回) を呼ぶ (ログイン new_entry_gate)。
 *   DB の関数が全部を確かめる (一番新しいゲートの結果の行の回 = 渡した回・kind_gate の 5 つが 0・最終形が 0・今日・区分の持ち主が company・widen の後・停止の床より新しい)。
 *   許可が出た = 単品の新商品をポータルで開ける (期限 = DB の expires_at = 翌日 07:00 JST = daily-sync の始まり)。
 *
 * 使い方 (miniPC): node apps/company-db/master-compare/new-entry-gate.mjs --daily [--data-dir D]
 *   (--daily は daily-sync の runScript が引数の無いときに '7' を足すのを避ける印)
 * env: COMPANY_DB_NEW_ENTRY_GATE_URL (ログイン new_entry_gate = grant / revoke だけ実行できる)。DATA_DIR (照合の証跡を読む)。DAILY_SYNC_RUN_ID (daily-sync の回)
 *
 * 終わり方 (最後の行 = 朝の要約):
 *   exit 0 = 開いた / 未設定 (接続が無い = この段は飛ばす) / 関数が無い (0058 の前) / 照合 ② が判定できない・落ちた・流れていない (grant を呼ばない) /
 *            widen の前で拒まれた (準備中 = 毎朝 ❌ にしない)。🚨 0058 の前・設定の前に毎朝の処理を止めない
 *   exit 1 = 拒まれた (理由つき) / 接続・関数の失敗 / その回の照合の証跡が無い / 状態不明 (retry に載る。retry は「マスタ照合」からやり直す = gateRetryJobs)
 *   開かなかった回は、いつも新しい接続で revoke_new_entry_lease → new_entry_lease_valid = false を確かめる (closeAndVerify)。
 *   「閉」と出すのは閉じたのを確かめたときだけ。確かめられない (grant の応答が切れた・返り値が壊れた・新しい接続も落ちた) = 「⚠️ 状態不明 (開いている可能性)」・exit 1 (#1645 Codex R1 High)
 *   🚨 0058 で new_entry_gate に ops.new_entry_lease_valid(text) の EXECUTE が要る (無いと毎回「状態不明」)
 *   DATA_DIR が無い・引数の間違い・最上位の例外も、URL があれば同じ (runCli・#1645 Codex R2 High)。URL が無い = 「未設定」exit 0 (今の本番のまま)
 */
import 'dotenv/config';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { jstDateStr } from '../../../lib/jst-date.js';
import { readEvidence } from '../push/evidence.mjs';

export const STEP_NAME = '新商品の許可';
export const EVIDENCE_NAME = 'master-compare';
export const ENV_URL = 'COMPANY_DB_NEW_ENTRY_GATE_URL';
export const KIND = 'single';
/** 拒まれた理由がこれだけ = widen の前 (準備中)。閉じたまま ⏸️ (exit 0) */
export const PREP_CODES = Object.freeze(['sku_kind_not_company', 'not_widened']);
const HEAD = '🆕 新商品の入口:';

/**
 * daily-sync: 照合 (マスタ照合) の結果を見て、この段を流すか。流さない = 結果の行 (流す = null)。
 *   照合が失敗・見送り = 流さない (入口は照合の始めで閉じたまま)。retry で照合が直ったら RERUN_AFTER がこの段も流す = blocked (この段だけを再試行に載せない)
 */
export function skipAfterCompare(compareResult) {
  if (compareResult && compareResult.success) return null;
  return { name: STEP_NAME, success: false, skipped: true, blocked: true, summary: `⏭️ 見送り (マスタ照合が失敗 = この段は流さない・今朝は許可を出していない。照合の再試行が成功したらこの段も流す)` };
}

/**
 * retry に載せる工程の名前 (#1645 Codex R1 Medium)。「新商品の許可」が残っていれば「マスタ照合」も足す。
 *   拒まれた後の revoke は停止の床を今の結果の行まで進める = 同じ照合の回では二度と開かない。
 *   retry は必ず新しい照合の回 (close → record) からやり直し、RERUN_AFTER で許可を出す。daily-sync と retry-failed-jobs の両方が使う
 */
export function gateRetryJobs(names) {
  const out = Array.isArray(names) ? [...names] : [];
  if (out.includes(STEP_NAME) && !out.includes(COMPARE_STEP)) out.push(COMPARE_STEP);
  return out;
}
export const COMPARE_STEP = 'マスタ照合';

/** その朝の照合 ② の回を証跡から読む。{ ok, compareRunId } か { ok: false, kind: 'closed' | 'error', reason } */
export function compareRunOf(ev, { asOf, syncRunId }) {
  if (!ev) return { ok: false, kind: 'error', reason: '照合の証跡が無い' };
  if (ev.error) return { ok: false, kind: 'error', reason: `照合の証跡が読めない (${String(ev.error).slice(0, 80)})` };
  if (syncRunId && ev.sync_run_id !== syncRunId) return { ok: false, kind: 'error', reason: `照合の証跡がこの daily-sync の回のものでない (${ev.sync_run_id ?? 'なし'})` };
  if (ev.as_of !== asOf) return { ok: false, kind: 'error', reason: `照合の証跡が今日のものでない (${ev.as_of ?? 'なし'})` };
  if (ev.state !== 'complete') return { ok: false, kind: 'closed', reason: `照合が完了していない (${ev.state ?? 'なし'}${ev.reason ? `: ${String(ev.reason).slice(0, 60)}` : ''})` };
  if (!ev.compare_run_id) return { ok: false, kind: 'error', reason: '照合の回 (compare_run_id) が無い' };
  const ne = ev.ne;
  if (!ne) return { ok: false, kind: 'closed', reason: '照合 ② が流れていない' };
  if (ne.verdict === 'blocked') return { ok: false, kind: 'closed', reason: `照合 ② が判定できない (${String(ne.blocked_reason ?? '').slice(0, 60)})` };
  if (ne.verdict === 'error') return { ok: false, kind: 'closed', reason: `照合 ② が落ちた (${String(ne.error ?? '').slice(0, 60)})` };
  return { ok: true, compareRunId: ev.compare_run_id };
}

/** 'lease_denied: a: … / b: …' → 理由のコードの配列 */
export function deniedCodes(message) {
  const m = String(message || '').match(/lease_denied:\s*([\s\S]*)$/);
  if (!m) return [];
  return m[1].split(' / ').map((p) => (p.trim().match(/^([a-z_]+)/) || [])[1]).filter(Boolean);
}

/** 期限 (timestamptz の文字) → 'MM/DD HH:MM' (JST)。読めない = そのまま */
export function jstShort(ts) {
  const t = Date.parse(ts);
  if (!Number.isFinite(t)) return String(ts ?? '');
  const d = new Date(t + 9 * 3600000).toISOString();
  return `${d.slice(5, 7)}/${d.slice(8, 10)} ${d.slice(11, 16)}`;
}

/** 接続 (本番 = ログイン new_entry_gate)。{ db: { query }, close } */
export async function connectGate(url) {
  const { openPgClient, pgAdapter } = await import('../../../scripts/company-db/migrate.mjs');
  const client = await openPgClient(url);
  try { await client.query(`set statement_timeout = '60s'`); } catch (e) { try { await client.end(); } catch { /* */ } throw e; }
  return { db: pgAdapter(client), close: () => client.end() };
}

const short = (e) => String(e && e.message ? e.message : e).replace(/\s+/g, ' ').slice(0, 200);

/**
 * 閉じたことを確かめる (#1645 Codex R1 High)。**いつも新しい接続**で revoke → ops.new_entry_lease_valid('single') = false を読む。
 *   grant の後に応答が切れた・返り値が壊れた接続は使わない (DB では許可が開いたままかもしれない)。
 *   結果 = { ok: true } (閉じたのを確かめた) か { ok: false, note } (確かめられない = 開いている可能性)
 */
export async function closeAndVerify(url, reason, { connect = connectGate } = {}) {
  let c;
  try { c = await connect(url); }
  catch (e) { return { ok: false, note: `取り消しの接続もできない (${short(e).slice(0, 80)})` }; }
  try {
    try { await c.db.query('select ops.revoke_new_entry_lease($1, $2) as r', [KIND, String(reason).slice(0, 480)]); }
    catch (e) { return { ok: false, note: `取り消しも失敗 (${short(e).slice(0, 80)})` }; }
    let v;
    try { v = (await c.db.query('select ops.new_entry_lease_valid($1) as v', [KIND])).rows[0]?.v; }
    catch (e) { return { ok: false, note: `取り消したが閉じたかを読めない (${short(e).slice(0, 80)})` }; }
    if (v === false) return { ok: true };
    return { ok: false, note: `取り消した後も有効と読めた (${JSON.stringify(v ?? null)})` };
  } finally {
    try { await c.close(); } catch { /* */ }
  }
}

/** 閉じられたか確かめられない = 「状態不明」(開いている可能性)・exit 1。「閉」と出すのは閉じたのを確かめたときだけ */
const unknownLine = (why, note) => `⚠️ ${HEAD} 状態不明 (開いている可能性・${why} / ${note})`;

/**
 * 1 回ぶん。戻り値 = { code, state, line }
 *   state: opened / not_configured / not_applied / compare_not_ready / prep / denied / error / unknown
 */
export async function runGateStep({ env = process.env, dataDir, now = new Date(), connect = connectGate, readEv = (d, a) => readEvidence(d, a) } = {}) {
  const url = String(env[ENV_URL] || '').trim();
  if (!url) return { code: 0, state: 'not_configured', line: `${HEAD} 未設定 (${ENV_URL} が無い = この段は飛ばした)` };
  /** 開かなかった回の終わり方: 新しい接続で閉じたのを確かめた = 閉 (closedCode) / 確かめられない = 状態不明 (exit 1)。「閉」の語はここ (と runCli) だけ (#1645 Codex R2 High) */
  const closeOut = async (why, revokeReason, { closedCode = 1, state = 'error', line = null } = {}) => {
    const v = await closeAndVerify(url, revokeReason, { connect });
    if (!v.ok) return { code: 1, state: 'unknown', line: unknownLine(why, v.note) };
    return { code: closedCode, state, line: line || `${HEAD} 閉 (${why})` };
  };
  if (!dataDir) return closeOut('DATA_DIR が無い = 照合の証跡を読めない', '新商品の許可の段: DATA_DIR が無い');
  const asOf = jstDateStr(now);
  const syncRunId = String(env.DAILY_SYNC_RUN_ID || '').trim() || null;
  let ev;
  try { const all = readEv(dataDir, asOf) || {}; ev = all[syncRunId ? EVIDENCE_NAME : `${EVIDENCE_NAME}.manual`]; }
  catch (e) { ev = { error: short(e) }; }
  const cr = compareRunOf(ev, { asOf, syncRunId });

  let c;
  try { c = await connect(url); }
  catch (e) { const why = `接続できない: ${short(e).slice(0, 120)}`; return closeOut(why, `新商品の許可の段: ${why}`); }
  let phase = 'check', granted = false, g = null, grantError = null;
  try {
    const db = c.db;
    const has = (await db.query("select to_regprocedure('ops.grant_new_entry_lease(text, text)') is not null as ok")).rows[0]?.ok === true;
    if (!has) return { code: 0, state: 'not_applied', line: `${HEAD} 0058 の前 (許可の関数が無い = 許可そのものが無い・この段は飛ばした)` };
    if (cr.ok) {
      phase = 'grant';
      try { g = (await db.query('select ops.grant_new_entry_lease($1, $2) as r', [KIND, cr.compareRunId])).rows[0]?.r ?? null; granted = true; }
      catch (e) { grantError = e; }
    }
  } catch (e) {
    grantError = grantError || e;
  } finally {
    try { await c.close(); } catch { /* */ }
  }

  if (!cr.ok) {
    // 照合 ② が判定できない・落ちた・その回の証跡が無い = grant を呼ばない (閉じる向きだけ)
    if (grantError) { const why = `確かめられない: ${short(grantError).slice(0, 120)}`; return closeOut(why, `新商品の許可の段: ${why}`); }
    return closeOut(cr.reason, `照合 ② の次の段: ${cr.reason}`, { closedCode: cr.kind === 'error' ? 1 : 0, state: cr.kind === 'error' ? 'error' : 'compare_not_ready' });
  }
  if (grantError) {
    const msg = short(grantError);
    const denied = phase === 'grant' && /lease_denied:/.test(msg);
    const codes = denied ? deniedCodes(msg) : [];
    const why = denied ? msg.replace(/^.*?lease_denied:\s*/, '') : msg;
    const revokeReason = `許可を出せない (${cr.compareRunId}): ${msg}`;
    if (denied && codes.length > 0 && codes.every((k) => PREP_CODES.includes(k))) {
      return closeOut(`widen の前 = 準備中: ${why.slice(0, 160)}`, revokeReason, { closedCode: 0, state: 'prep', line: `⏸️ ${HEAD} 閉 (widen の前 = 準備中: ${why.slice(0, 160)})` });
    }
    // grant の応答が切れた (DB では開いたかもしれない) も、拒まれたのと同じく新しい接続で閉じて確かめる
    return closeOut(`${denied ? '拒まれた' : '許可を出せない'}: ${why.slice(0, 220)}`, revokeReason, { state: denied ? 'denied' : 'error' });
  }
  if (!granted || !g || typeof g !== 'object' || !g.expires_at || !Number.isFinite(Date.parse(g.expires_at))) {
    // grant は返ったが返り値が読めない = 開いたかどうか分からない → 新しい接続で閉じて確かめる
    const why = `許可の返り値が読めない: ${JSON.stringify(g ?? null).slice(0, 80)}`;
    return closeOut(why, `許可を出せない (${cr.compareRunId}): ${why}`);
  }
  return { code: 0, state: 'opened', grant: g, line: `${HEAD} 開 (〜${jstShort(g.expires_at)} JST・照合 ${cr.compareRunId}・許可 #${g.lease_id ?? '?'})` };
}

/**
 * CLI の 1 回 (試験が env・接続を差し替える)。引数の間違い・最上位の例外 = URL があれば新しい接続で閉じて確かめる (#1645 Codex R2 High)。
 *   確かめた = 閉・exit 1 / 確かめられない = 状態不明・exit 1 / URL が無い = 許可は出していない (「閉」とは言わない)・exit 1
 */
export async function runCli(argv, { env = process.env, connect = connectGate, readEv = undefined, now = undefined } = {}) {
  try {
    const a = parseArgs(argv);
    return await runGateStep({ env, dataDir: (a.dataDir || env.DATA_DIR || '').trim(), connect, ...(readEv ? { readEv } : {}), ...(now ? { now } : {}) });
  } catch (e) {
    const why = `この段が落ちた: ${short(e).slice(0, 160)}`;
    const url = String(env[ENV_URL] || '').trim();
    if (!url) return { code: 1, state: 'error', line: `${HEAD} ${why} (${ENV_URL} が無い = 許可は出していない)` };
    const v = await closeAndVerify(url, `新商品の許可の段: ${why}`, { connect });
    return v.ok ? { code: 1, state: 'error', line: `${HEAD} 閉 (${why})` } : { code: 1, state: 'unknown', line: unknownLine(why, v.note) };
  }
}

export function parseArgs(argv) {
  const out = { dataDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--daily' || a === '7') { /* daily-sync の印 */ }
    else throw new Error(`知らない引数: ${a}`);
  }
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1, last = '';
  try {
    const r = await runCli(process.argv.slice(2));
    code = r.code; last = r.line;
  } catch (e) {
    // runCli は投げない作り。投げたら確かめていない = 状態不明
    last = unknownLine(`この段が落ちた: ${short(e)}`, '閉じたかを確かめられない');
    code = 1;
  }
  console.log(String(last).replace(/\s+/g, ' '));
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
