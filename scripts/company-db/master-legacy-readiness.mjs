#!/usr/bin/env node
/**
 * master-legacy-readiness.mjs — マスタの古い入口の門 (⑤-3) を配る前・配った後に「足りないもの」を出す (読むだけ。何も書かない・何も変えない)
 * (PR #1565 中間レビュー Medium-5: 0051 の前にマージする・env が無い = 古い入口が全部閉じる、を先に気づく)
 *
 * 見るもの:
 *   1. env: 段階を読む接続先 (COMPANY_DB_MASTER_GATE_RENDER_URL / _MINIPC_URL → COMPANY_DB_URL。見張りの watcher は使わない) と、この場所の門のログイン
 *   2. 段階を読める = 0051 が本適用済み・select の権限がある (読めないと古い入口は全部 503)
 *   3. 門のログイン: ログインの役が master_gate_<場所>・記録の関数 (ops.record_legacy_gate_ack) の実行権がある・一覧 (manifest) を DB が受け取れる形
 *      🆕 ⑤-3b (Codex #1610 R1 Medium): 列ごとの持ち主の表 (0055 の ops.master_ownership_state) がある・このログインに SELECT がある・
 *      門と同じ読み方 (readGateOwnership) で読める。legacy_open の間は門は持ち主を読まない = ここで先に確かめないと、frozen にした瞬間に全部 503 になる
 *   4. build の番号が分かる (Render = RENDER_GIT_COMMIT・miniPC = git の HEAD)
 *   5. (見るだけ) 黙っているプロセス (今までに記録を書いて、15 分以内の記録も「止めた」も無い = 何日前でも) の数。⑤-1 はこれがあると段階を進めない
 *   6. 🆕 広げる道 PR-2 (設計 G16・v12 §7.1・Codex #1640 R1 Medium 3)。権限は「実際に呼んで」確かめる (has_function_privilege だけにしない):
 *      a. 門のログイン: 0058 の後 (ops.master_widen_attempts か 2 版の関数がある) は門の記録の 2 版 ops.record_legacy_gate_ack_v2 が要る・実行権がある
 *      b. 画面のロール (COMPANY_DB_MASTER_EDIT_URL = master_edit): DB の active を読める・code_behind でない・ops.new_entry_lease_valid('single') を呼べる
 *         (答えが false = 許可が無い は正常)。🚨 Render (画面のある場所) で接続先が無い = 足りない (miniPC = 注意だけ)
 *      c. 見張り (COMPANY_DB_WATCH_URL = watcher): ops.new_entry_lease_valid('single') を呼べる・ops.widen_check_readonly(uuid, integer) の実行権がある
 *         (試みの番号が無いと呼べない = 実行権を見る)。🚨 miniPC (毎朝のゲート・widen の確かめの場所) で接続先が無い = 足りない (Render = 注意だけ)
 *      d. 許可を出す専用のログイン new_entry_gate が LOGIN・NOINHERIT である (見られる接続のどれかで pg_roles を読む)
 *      e. 許可の出し方の関数の実行権 (newentry_min_plan §3・§7 の 4・PR-1 の確定の形): ops.close_new_entry_for_compare・ops.record_new_entry_gate = watch_writer・
 *         ops.grant_new_entry_lease / ops.revoke_new_entry_lease = new_entry_gate (ロールの名前で has_function_privilege)。
 *         逆向きも見る: watch_writer は grant を呼べない・new_entry_gate は close / record を呼べない (許可を出す権限を分ける)
 *   7. (miniPC だけ・0058 の後) 許可を出す専用のログイン (COMPANY_DB_NEW_ENTRY_GATE_URL) で実際につながり、役が new_entry_gate・grant / revoke / 閉じたかを読む関数を実行できる (PR-1)
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
import { readGateOwnership } from '../../apps/company-db/load/ownership-state.mjs';
import { readActiveOwnership, NEW_ENTRY_LEASE_FN, ACQUIRE_NEW_ENTRY_LOCKS_FN } from '../../lib/master-owner-gate.mjs';
import { ACK_V2_FUNCTION_SIGNATURE } from '../../lib/master-legacy-gate.mjs';
/** watcher が呼ぶ widen の確かめ (PR-1 の 0058・読むだけ) */
export const WIDEN_CHECK_FN = 'ops.widen_check_readonly(uuid,integer)';
/** 0058 が入っているか (試みの表か 2 版の記録の関数がある) */
const HAS_0058_SQL = `select to_regclass('ops.master_widen_attempts') is not null or to_regprocedure('${'ops.record_legacy_gate_ack_v2(text,text,text,jsonb,text,text,integer,timestamptz,text,text,text[],boolean,text)'}') is not null as ok`;
/** 関数を実際に呼んで boolean が返るか (false = 許可が無い = 正常)。読めない = 誤りの文 */
async function callLease(c) {
  try {
    const v = (await c.query(`select ops.new_entry_lease_valid('single') as ok`)).rows[0]?.ok;
    return typeof v === 'boolean' ? { ok: true, value: v } : { ok: false, error: `答えが boolean でない (${JSON.stringify(v)})` };
  } catch (e) { return { ok: false, error: String((e && e.message) || e).slice(0, 200) }; }
}
const canExec = async (c, fn) => (await c.query(`select to_regprocedure($1) is not null and has_function_privilege(current_user, to_regprocedure($1), 'execute') as ok`, [fn])).rows[0].ok === true;
import { capabilityFingerprint } from '../../config/master-capability.mjs';

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
      // 6a. 広げる道: 0058 の後は門の記録の 2 版が要る (1 版の記録では widen が「記録が 1 版」で止まる)
      const has0058 = (await c.query(HAS_0058_SQL)).rows[0].ok === true;
      if (has0058) {
        if (await canExec(c, ACK_V2_FUNCTION_SIGNATURE)) ok('門の記録の 2 版 (ops.record_legacy_gate_ack_v2) を実行できる');
        else ng(`0058 の後なのに門の記録の 2 版 ${ACK_V2_FUNCTION_SIGNATURE} が無いか実行権が無い = widen が「記録が 1 版」で止まる`);
      } else lines.push('  ⚠ 0058 の前 (門の記録は 1 版で書く。広げる道の widen はまだできない)');
      try {
        const h = (await c.query('select ops.legacy_manifest_hash($1::jsonb) as h', [JSON.stringify(legacyManifest())])).rows[0].h;
        ok(`古い入口の一覧を DB が受け取れる形 (manifest_hash = ${h})`);
      } catch (e) { ng(`古い入口の一覧の形が DB に合わない: ${String(e && e.message).slice(0, 200)}`); }
      // ⑤-3b: 列ごとの持ち主 (段階が legacy_open 以外のとき門が読む)。表・SELECT の権限・門と同じ読み方の 3 つ
      const tbl = (await c.query("select to_regclass('ops.master_ownership_state')::text as t")).rows[0].t;
      if (!tbl) ng('列ごとの持ち主の表 ops.master_ownership_state が無い (0055 の前) = frozen にした瞬間に古い入口が全部 503');
      else {
        const sel = (await c.query("select has_table_privilege(session_user, 'ops.master_ownership_state', 'SELECT') as ok")).rows[0].ok;
        if (!sel) ng('門のログインに ops.master_ownership_state の SELECT が無い (この PR の create-master-edit-roles.mjs を流し直す) = frozen にした瞬間に古い入口が全部 503');
        else {
          const o = await readGateOwnership(pgAdapter(c));
          if (o.readable) ok(`列ごとの持ち主を読める (C の列 ${o.company.length} 個${o.prepared_hash ? '・prepared あり' : ''})`);
          else ng(`列ごとの持ち主を読めない: ${String(o.error).slice(0, 200)}`);
        }
      }
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
  // 7. (miniPC だけ) 新商品の開く前のゲートのログイン (0058 の後 = ops.grant_new_entry_lease がある DB だけ見る)。
  //    無い = daily-sync の照合 ② の次の段が許可を出せない = 新商品の入口は閉じたまま (落ちる側だが、本番の手順の 3〜5 の抜けを先に見つける)
  if (host === 'minipc' && phaseUrl) {
    let c = null;
    try {
      c = await open(phaseUrl);
      const has58 = (await c.query("select to_regprocedure('ops.grant_new_entry_lease(text, text)') is not null as ok")).rows[0].ok;
      if (!has58) ok('新商品の開く前のゲート: 0058 の前 (見ない)');
      else if (!String(env.COMPANY_DB_NEW_ENTRY_GATE_URL || '').trim()) ng('新商品の開く前のゲートのログインが無い (COMPANY_DB_NEW_ENTRY_GATE_URL = new_entry_gate。0058 の後に create-master-edit-roles.mjs で作り、miniPC の .env に足す)');
      else {
        let g = null;
        try {
          g = await open(String(env.COMPANY_DB_NEW_ENTRY_GATE_URL).trim());
          const who = (await g.query('select session_user::text as u')).rows[0].u;
          const can = (await g.query(`select has_function_privilege(session_user, 'ops.grant_new_entry_lease(text, text)', 'execute') as g,
              has_function_privilege(session_user, 'ops.revoke_new_entry_lease(text, text)', 'execute') as r,
              has_function_privilege(session_user, 'ops.new_entry_lease_valid(text)', 'execute') as v`)).rows[0];
          if (who !== 'new_entry_gate') ng(`新商品の開く前のゲートのログインの役が ${who} (期待 new_entry_gate)`);
          else if (!can.g || !can.r || !can.v) ng('new_entry_gate に許可を出す / 取り消す / 閉じたかを読む関数の実行権が無い (create-master-edit-roles.mjs を流し直す)');
          else ok('新商品の開く前のゲートのログイン = new_entry_gate (許可を出す / 取り消す / 閉じたかを読むを実行できる)');
        } catch (e) { ng(`新商品の開く前のゲートのログインでつながらない: ${String(e && e.message).slice(0, 200)}`); } finally { if (g) { try { await g.end(); } catch { /* */ } } }
      }
    } catch (e) { ng(`新商品の開く前のゲートを確かめられない: ${String(e && e.message).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
  }
  // 6b. 画面のロール (広げる道 PR-2)
  const editUrl = String(env.COMPANY_DB_MASTER_EDIT_URL || '').trim();
  if (!editUrl) {
    if (host === 'render') ng('画面のロールの接続先 (COMPANY_DB_MASTER_EDIT_URL = master_edit) が無い = 画面の保存の門 (DB の active・許可) を確かめられない (Render は画面のある場所)');
    else lines.push('  ⚠ 画面のロールの接続先 (COMPANY_DB_MASTER_EDIT_URL) が無い = 保存の門の確かめをしない (miniPC に画面は無い)');
  } else {
    let c = null;
    try {
      c = await open(editUrl);
      // 接続先の本当のログイン (Codex #1640 R2 Medium 4): 持ち主の URL・取り違えた URL では、画面のロールの権限を確かめたことにならない
      const who = (await c.query('select session_user::text as u')).rows[0].u;
      if (who !== 'master_edit') { ng(`COMPANY_DB_MASTER_EDIT_URL のログインが ${who} (期待 master_edit) = 画面のロールの権限を確かめられない`); throw Object.assign(new Error('wrong_role'), { skip: true }); }
      ok('画面のロールの接続先のログイン = master_edit');
      const db = pgAdapter(c);
      const a = await readActiveOwnership(db);
      if (!a.readable) ng(`画面のロールで持ち主 (DB の active) を読めない = 保存が全部 503 (PR-1 の 0058 = ops.master_ownership_active_map の SECURITY DEFINER と実行権が先): ${String(a.error).slice(0, 200)}`);
      else if (a.code_behind.length) ng(`このコードが扱えない C のキーが DB の active にある (code_behind: ${a.code_behind.join('・')}) = 保存が全部 409。能力 (config/master-capability.mjs) の足りた build を配る`);
      else ok(`画面のロールで持ち主 (DB の active) を読める (C の列 ${Object.values(a.map).filter((v) => v === 'company').length} 個・能力 protocol ${capabilityFingerprint().protocol})`);
      const fn = (await c.query('select to_regprocedure($1)::text as f', [NEW_ENTRY_LEASE_FN])).rows[0].f;
      if (!fn) ng(`新商品の開放の許可の関数 ${NEW_ENTRY_LEASE_FN} が無い (PR-1 の 0058 の前) = 新商品・NE 登録の CSV の作成は閉じたまま`);
      else {
        const l = await callLease(c);
        if (l.ok) ok(`画面のロールで新商品の開放の許可を呼べる (今 single = ${l.value ? '許可あり' : '許可なし'})`); else ng(`画面のロールで ${NEW_ENTRY_LEASE_FN} を呼べない = 新商品の登録が 503: ${l.error}`);
      }
      // 新規開始の鍵 (許可の共有の鍵を段階の鍵より前に取る関数)・期限つきのファイルの取得・file_bytes の列を直接読めないこと (Codex #1640 R3)
      if (await canExec(c, ACQUIRE_NEW_ENTRY_LOCKS_FN)) ok(`画面のロールが ${ACQUIRE_NEW_ENTRY_LOCKS_FN} を実行できる`);
      else ng(`画面のロールが ${ACQUIRE_NEW_ENTRY_LOCKS_FN} を実行できない (0058 の前・実行権が無い) = 新商品・CSV の作成は閉じたまま`);
      if (await canExec(c, 'ops.ne_reg_file(bigint)')) ok('画面のロールが ops.ne_reg_file(bigint) を実行できる (期限つきのファイルの取得)');
      else ng('画面のロールが ops.ne_reg_file(bigint) を実行できない = 配ったファイルを受け取れない (0058 の前・実行権が無い)');
      const col = (await c.query(`select to_regclass('ops.ne_reg_exports') is not null and has_column_privilege(current_user, 'ops.ne_reg_exports', 'file_bytes', 'SELECT') as can`)).rows[0].can === true;
      if (col) ng('画面のロールが ops.ne_reg_exports.file_bytes を直接読める = 期限つきの取得 (2 時間・取り消し・新しい照合の結果) を迂回しうる (0058 で列の SELECT を外す)');
      else ok('画面のロールは file_bytes の列を直接読めない');
    } catch (e) { if (!e.skip) ng(`画面のロールでつながらない: ${String(e && e.message).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
  }
  // 6c. 見張り (watcher): 毎朝のゲートの読み取り・widen の確かめ
  const watchUrl = String(env.COMPANY_DB_WATCH_URL || '').trim();
  if (!watchUrl) {
    if (host === 'minipc') ng('見張りの接続先 (COMPANY_DB_WATCH_URL = watcher) が無い = 毎朝のゲート・widen の確かめの権限を確かめられない (miniPC で流す)');
    else lines.push('  ⚠ 見張りの接続先 (COMPANY_DB_WATCH_URL) が無い = watcher の権限は miniPC の readiness で確かめる');
  } else {
    let c = null;
    try {
      c = await open(watchUrl);
      const who = (await c.query('select session_user::text as u')).rows[0].u;
      if (who !== 'watcher') { ng(`COMPANY_DB_WATCH_URL のログインが ${who} (期待 watcher) = 見張りの権限を確かめられない`); throw Object.assign(new Error('wrong_role'), { skip: true }); }
      ok('見張りの接続先のログイン = watcher');
      const l = await callLease(c);
      if (l.ok) ok(`見張り (watcher) で新商品の開放の許可を呼べる (今 single = ${l.value ? '許可あり' : '許可なし'})`); else ng(`見張り (watcher) で ${NEW_ENTRY_LEASE_FN} を呼べない: ${l.error}`);
      if (await canExec(c, WIDEN_CHECK_FN)) ok(`見張り (watcher) が ${WIDEN_CHECK_FN} を実行できる`);
      else ng(`見張り (watcher) が ${WIDEN_CHECK_FN} を実行できない (関数が無い = 0058 の前・または実行権が無い)`);
    } catch (e) { if (!e.skip) ng(`見張りの接続先でつながらない: ${String(e && e.message).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
  }
  // 6d. 許可を出す専用のログイン new_entry_gate (LOGIN・NOINHERIT)。見られる接続のどれかで pg_roles を読む
  const roleUrl = watchUrl || editUrl || phaseUrl;
  if (roleUrl) {
    let c = null;
    try {
      c = await open(roleUrl);
      const r = (await c.query(`select rolcanlogin, rolinherit from pg_roles where rolname = 'new_entry_gate'`)).rows[0];
      if (!r) ng('許可を出す専用のログイン new_entry_gate が無い (PR-1 の create-master-edit-roles.mjs を流す)');
      else if (r.rolcanlogin !== true || r.rolinherit !== false) ng(`new_entry_gate の形が違う (LOGIN ${r.rolcanlogin}・INHERIT ${r.rolinherit}。LOGIN・NOINHERIT にする)`);
      else ok('許可を出す専用のログイン new_entry_gate がある (LOGIN・NOINHERIT)');
      // 6e. 許可の出し方の関数の実行権 (ロールの名前で)
      const GATE_FNS = { close: 'ops.close_new_entry_for_compare(text)', record: 'ops.record_new_entry_gate(text,text,text,jsonb)', grant: 'ops.grant_new_entry_lease(text,text)', revoke: 'ops.revoke_new_entry_lease(text,text)' };
      const canRun = async (fn, role) => {
        const x = (await c.query(`select to_regprocedure($1) is not null as has, exists (select 1 from pg_roles where rolname = $2) as role`, [fn, role])).rows[0];
        return { has: x.has, can: x.has && x.role && (await c.query(`select has_function_privilege($2, to_regprocedure($1), 'execute') as ok`, [fn, role])).rows[0].ok === true };
      };
      for (const [k, role] of [['close', 'watch_writer'], ['record', 'watch_writer'], ['grant', 'new_entry_gate'], ['revoke', 'new_entry_gate']]) {
        const r = await canRun(GATE_FNS[k], role);
        if (r.can) ok(`${role} が ${GATE_FNS[k]} を実行できる`); else ng(`${role} が ${GATE_FNS[k]} を実行できない (${r.has ? '実行権が無い' : '関数が無い = 0058 の前'}) = 毎朝の許可が出せない`);
      }
      for (const [k, role] of [['grant', 'watch_writer'], ['close', 'new_entry_gate'], ['record', 'new_entry_gate']]) {
        const r = await canRun(GATE_FNS[k], role);
        if (r.can) ng(`${role} が ${GATE_FNS[k]} を実行できてしまう = 許可を出す権限が分かれていない`);
      }
    } catch (e) { ng(`new_entry_gate を確かめられない: ${String(e && e.message).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
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
