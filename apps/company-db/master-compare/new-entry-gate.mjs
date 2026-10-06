/**
 * new-entry-gate.mjs — 毎朝の照合 ② の次の 1 段「新商品の許可」(daily-sync の 1 段・新しい定期実行ではない。計画 newentry_min_plan.md §3 の 2・PR-7)
 *
 *   その朝の照合 ② の回 (証跡 master-compare の compare_run_id) で ops.grant_new_entry_lease('single', 回) を呼ぶ (ログイン new_entry_gate)。
 *   DB の関数が全部を確かめる (一番新しいゲートの結果の行の回 = 渡した回・kind_gate の 5 つが 0・最終形が 0・今日・区分の持ち主が company・widen の後・停止の床より新しい)。
 *   許可が出た = 単品の新商品をポータルで開ける (期限 = 翌日 10:00 JST)。
 *
 * 使い方 (miniPC): node apps/company-db/master-compare/new-entry-gate.mjs --daily [--data-dir D]
 *   (--daily は daily-sync の runScript が引数の無いときに '7' を足すのを避ける印)
 * env: COMPANY_DB_NEW_ENTRY_GATE_URL (ログイン new_entry_gate = grant / revoke だけ実行できる)。DATA_DIR (照合の証跡を読む)。DAILY_SYNC_RUN_ID (daily-sync の回)
 *
 * 終わり方 (最後の行 = 朝の要約):
 *   exit 0 = 開いた / 未設定 (接続が無い = この段は飛ばす) / 関数が無い (0058 の前) / 照合 ② が判定できない・落ちた・流れていない (grant を呼ばない) /
 *            widen の前で拒まれた (準備中 = 毎朝 ❌ にしない)。🚨 0058 の前・設定の前に毎朝の処理を止めない
 *   exit 1 = 拒まれた (理由つき・revoke してから) / 接続・関数の失敗 / その回の照合の証跡が無い (retry に載る。人が直せば同じ日のうちに開く)
 *   開かなかった回は revoke_new_entry_lease を呼ぶ (閉じる向きだけ・照合 ② の始めの close と同じ向き)
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
  return { name: STEP_NAME, success: false, skipped: true, blocked: true, summary: `⏭️ 見送り (マスタ照合が失敗 = ${HEAD} 閉のまま。照合の再試行が成功したらこの段も流す)` };
}

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

/** 取り消し (閉じる向き)。結果 = { ok, note } */
async function revoke(db, reason) {
  try {
    const r = (await db.query('select ops.revoke_new_entry_lease($1, $2) as r', [KIND, String(reason).slice(0, 480)])).rows[0]?.r ?? null;
    return { ok: true, result: r };
  } catch (e) { return { ok: false, note: `取り消しも失敗 (${short(e).slice(0, 80)})` }; }
}

/**
 * 1 回ぶん。戻り値 = { code, state, line }
 *   state: opened / not_configured / not_applied / compare_not_ready / prep / denied / error
 */
export async function runGateStep({ env = process.env, dataDir, now = new Date(), connect = connectGate, readEv = (d, a) => readEvidence(d, a) } = {}) {
  const url = String(env[ENV_URL] || '').trim();
  if (!url) return { code: 0, state: 'not_configured', line: `${HEAD} 未設定 (${ENV_URL} が無い = この段は飛ばした・閉のまま)` };
  if (!dataDir) return { code: 1, state: 'error', line: `${HEAD} 閉 (DATA_DIR が無い = 照合の証跡を読めない)` };
  const asOf = jstDateStr(now);
  const syncRunId = String(env.DAILY_SYNC_RUN_ID || '').trim() || null;
  let ev;
  try { const all = readEv(dataDir, asOf) || {}; ev = all[syncRunId ? EVIDENCE_NAME : `${EVIDENCE_NAME}.manual`]; }
  catch (e) { ev = { error: short(e) }; }
  const cr = compareRunOf(ev, { asOf, syncRunId });

  let c;
  try { c = await connect(url); }
  catch (e) { return { code: 1, state: 'error', line: `${HEAD} 閉 (接続できない: ${short(e).slice(0, 120)})` }; }
  try {
    const db = c.db;
    const has = (await db.query("select to_regprocedure('ops.grant_new_entry_lease(text, text)') is not null as ok")).rows[0]?.ok === true;
    if (!has) return { code: 0, state: 'not_applied', line: `${HEAD} 閉のまま (許可の関数が無い = 0058 の前・この段は飛ばした)` };

    if (!cr.ok) {
      // 照合 ② が判定できない・落ちた・その回の証跡が無い = grant を呼ばない (閉じる向きだけ)
      const rv = await revoke(db, `照合 ② の次の段: ${cr.reason}`);
      const code = cr.kind === 'error' || !rv.ok ? 1 : 0;
      return { code, state: cr.kind === 'error' ? 'error' : 'compare_not_ready', line: `${HEAD} 閉 (${cr.reason}${rv.ok ? '' : ` / ${rv.note}`})` };
    }

    let g;
    try { g = (await db.query('select ops.grant_new_entry_lease($1, $2) as r', [KIND, cr.compareRunId])).rows[0]?.r ?? null; }
    catch (e) {
      const msg = short(e);
      const codes = deniedCodes(msg);
      const denied = /lease_denied:/.test(msg);
      const rv = await revoke(db, `許可を出せない (${cr.compareRunId}): ${msg}`);
      const prep = denied && codes.length > 0 && codes.every((k) => PREP_CODES.includes(k));
      const why = denied ? msg.replace(/^.*?lease_denied:\s*/, '') : msg;
      if (prep && rv.ok) return { code: 0, state: 'prep', line: `⏸️ ${HEAD} 閉 (widen の前 = 準備中: ${why.slice(0, 160)})` };
      return { code: 1, state: denied ? 'denied' : 'error', line: `${HEAD} 閉 (${denied ? '拒まれた' : '許可を出せない'}: ${why.slice(0, 220)}${rv.ok ? '' : ` / ${rv.note}`})` };
    }
    if (!g || !g.expires_at) return { code: 1, state: 'error', line: `${HEAD} 閉 (許可の返り値が読めない: ${JSON.stringify(g).slice(0, 80)})` };
    return { code: 0, state: 'opened', grant: g, line: `${HEAD} 開 (〜翌日 10:00 = ${jstShort(g.expires_at)} JST・照合 ${cr.compareRunId}・許可 #${g.lease_id ?? '?'})` };
  } catch (e) {
    return { code: 1, state: 'error', line: `${HEAD} 閉 (確かめられない: ${short(e).slice(0, 160)})` };
  } finally {
    try { await c.close(); } catch { /* */ }
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
    const a = parseArgs(process.argv.slice(2));
    const r = await runGateStep({ dataDir: (a.dataDir || process.env.DATA_DIR || '').trim() });
    code = r.code; last = r.line;
  } catch (e) {
    last = `${HEAD} 閉 (${short(e)})`;
    code = 1;
  }
  console.log(String(last).replace(/\s+/g, ' '));
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
