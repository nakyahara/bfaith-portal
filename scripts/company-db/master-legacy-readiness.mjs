#!/usr/bin/env node
/**
 * master-legacy-readiness.mjs — マスタの古い入口の門 (⑤-3) を配る前・配った後に「足りないもの」を出す (読むだけ。何も書かない・何も変えない)
 * (PR #1565 中間レビュー Medium-5: 0051 の前にマージする・env が無い = 古い入口が全部閉じる、を先に気づく)
 *
 * 見るもの:
 *   1. env: 段階を読む接続先 (COMPANY_DB_MASTER_GATE_RENDER_URL / _MINIPC_URL → COMPANY_DB_URL。見張りの watcher は使わない) と、この場所の門のログイン
 *   2. 段階を読める = 0051 が本適用済み・select の権限がある (読めないと古い入口は全部 503)
 *   3. 門のログイン: ログインの役が master_gate_<場所>・記録の関数 (ops.record_legacy_gate_ack) の実行権がある・一覧 (manifest) を DB が受け取れる形
 *   4. build の番号が分かる (Render = RENDER_GIT_COMMIT・miniPC = git の HEAD)
 *   5. (見るだけ) 黙っているプロセス (今までに記録を書いて、15 分以内の記録も「止めた」も無い = 何日前でも) の数。⑤-1 はこれがあると段階を進めない
 * 使い方:
 *   node -r dotenv/config scripts/company-db/master-legacy-readiness.mjs --host minipc     # miniPC で
 *   node scripts/company-db/master-legacy-readiness.mjs --host render                      # Render の Shell で
 * 終了コード: 0 = そろっている / 1 = 足りないものがある (一覧を出す) / 2 = 引数が違う
 * 🚨 書かない: 門の記録も書かない (記録は server.js の起動のときに書く)。一覧の形は ops.legacy_manifest_hash (immutable の関数) で確かめるだけ
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { readCutoverPhase } from '../../lib/master-cutover.mjs';
import { phaseUrlFrom, gateUrlFor, GATE_URL_ENV, legacyManifest, resolveBuildId, ACK_FUNCTION_SIGNATURE } from '../../lib/master-legacy-gate.mjs';

export async function checkReadiness({ host, env = process.env, open = (url) => openPgClient(url, { application_name: 'master-legacy-readiness', connectionTimeoutMillis: 5000, statement_timeout: 5000 }) } = {}) {
  const lines = [];
  const problems = [];
  const ok = (m) => lines.push(`  ✓ ${m}`);
  const ng = (m) => { lines.push(`  ✗ ${m}`); problems.push(m); };
  // 1. env
  const phaseUrl = phaseUrlFrom(env);
  if (phaseUrl) ok('段階を読む接続先がある'); else ng(`段階を読む接続先が無い (${GATE_URL_ENV.render} / ${GATE_URL_ENV.minipc} / COMPANY_DB_URL のどれか。見張りの COMPANY_DB_WATCH_URL は使わない)`);
  const gateUrl = gateUrlFor(host, env);
  if (gateUrl) ok(`この場所の門のログインがある (${GATE_URL_ENV[host]})`); else ng(`この場所の門のログインが無い (${GATE_URL_ENV[host]} = master_gate_${host})`);
  // 2. 段階を読める
  if (phaseUrl) {
    let c = null;
    try {
      c = await open(phaseUrl);
      const s = await readCutoverPhase(pgAdapter(c));
      if (s.readable) ok(`段階を読める (0051 あり・select の権限あり): ${s.phase}`); else ng(`段階を読めない (0051 の前か select の権限が無い): ${s.error}`);
    } catch (e) { ng(`段階を読む接続先につながらない: ${String(e && e.message).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
  }
  // 3. 門のログイン
  if (gateUrl) {
    let c = null;
    try {
      c = await open(gateUrl);
      const who = (await c.query('select session_user::text as u')).rows[0].u;
      if (who === `master_gate_${host}`) ok(`門のログインの役 = ${who}`); else ng(`門のログインの役が ${who} (期待 master_gate_${host}。⑤-1 の記録の関数は場所と役が違えば拒む)`);
      const fn = (await c.query('select to_regprocedure($1)::text as f', [ACK_FUNCTION_SIGNATURE])).rows[0].f;
      if (!fn) ng(`記録の関数 ${ACK_FUNCTION_SIGNATURE} が無い (0051 の前か、⑤-1 の古い版)`);
      else {
        const can = (await c.query(`select has_function_privilege(session_user, $1::regprocedure, 'execute') as ok`, [fn])).rows[0].ok;
        if (can) ok('記録の関数を実行できる'); else ng('記録の関数の実行権が無い (⑤-1 の scripts/company-db/create-master-edit-roles.mjs を流す)');
      }
      try {
        const h = (await c.query('select ops.legacy_manifest_hash($1::jsonb) as h', [JSON.stringify(legacyManifest())])).rows[0].h;
        ok(`古い入口の一覧を DB が受け取れる形 (manifest_hash = ${h})`);
      } catch (e) { ng(`古い入口の一覧の形が DB に合わない: ${String(e && e.message).slice(0, 200)}`); }
    } catch (e) { ng(`門のログインでつながらない: ${String(e && e.message).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
  }
  // 4. build の番号
  const b = resolveBuildId({ env, fresh: true });
  if (b) ok(`build の番号 = ${b}`); else ng('build の番号が分からない (RENDER_GIT_COMMIT も git の HEAD も読めない) = 門の記録を書けない');
  // 5. (見るだけ・足りないにはしない) 黙っているプロセス = 今までに記録を書いて、最後が 15 分より前で「止めた」でもない (何日前でも)。
  //    ⑤-1 の段階を進める関数はこれがあると進めない (#1563 R3: 年齢では外れない)。見る接続先 = COMPANY_DB_MASTER_OPS_URL → COMPANY_DB_WATCH_URL
  const listUrl = String(env.COMPANY_DB_MASTER_OPS_URL || '').trim() || String(env.COMPANY_DB_WATCH_URL || '').trim();
  if (listUrl) {
    let c = null;
    try {
      c = await open(listUrl);
      const { listInstances } = await import('./master-legacy-instance.mjs');
      const silent = (await listInstances(c)).filter((r) => !r.stopped && !r.fresh);
      if (silent.length) lines.push(`  ⚠ 黙っているプロセス ${silent.length} 件 (段階を進められない。止まったのを確かめて master-legacy-instance.mjs --stop): ${silent.slice(0, 5).map((r) => `${r.host}/${r.instance_id}`).join(', ')}`);
      else ok('黙っているプロセスは無い');
    } catch (e) { lines.push(`  ⚠ 黙っているプロセスを見られない: ${String(e && e.message).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
  }
  return { ok: problems.length === 0, problems, lines };
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const host = args[args.indexOf('--host') + 1];
  if (!args.includes('--host') || !['render', 'minipc'].includes(host)) { console.error('--host render か --host minipc を付ける'); process.exitCode = 2; }
  else {
    const r = await checkReadiness({ host });
    console.log(`マスタの古い入口の門 (⑤-3) の準備 (${host}):`);
    for (const l of r.lines) console.log(l);
    console.log(r.ok ? '✅ そろっている' : `❌ 足りない ${r.problems.length} 件 (このまま配ると、古い入口が 503 / 410 になるか、門の記録が書けない)`);
    process.exitCode = r.ok ? 0 : 1;
  }
}
