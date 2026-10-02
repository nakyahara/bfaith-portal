#!/usr/bin/env node
/**
 * master-ownership-epoch.mjs — 持ち主の設定の epoch を人が進めるコマンド (0055。マスタ正本切替 ④a・Codex #1564 R1 H1)
 *
 * 切替の日の順番 (⑤-3 の切替の手順の中。🚨 古い書き込み口 (/register など) を閉じるのはその手順 = 持ち主を変える前に閉じる。ここでは閉じない):
 *   1. config/master-ownership.mjs を書き換えてデプロイ (= configured。これだけでは何も変わらない。夜間ロード・写し・作り直しは active のまま)
 *   2. prepare   (miniPC)        : configured を prepared として Company DB に記録する (一緒に切り替える組・写さない列を確かめる)
 *   3. node scripts/company-db/remote-load.mjs load --apply --wait --use-prepared   (prepared の持ち主で 1 回だけロード)
 *   4. node apps/company-db/publish/fetch.mjs → m_products 再構築 → fetch.mjs --verify-apply   (miniPC。prepared の世代を作り直しで入れて確かめる)
 *   5. activate  (miniPC)        : 4 の確かめが通った証拠 (今の作り直しの記録・prepare の後に読んだ世代・証跡の apply) があり、
 *                                   切替の段階 (⑤-1 の ops.master_cutover_state) が frozen のときだけ prepared → active
 *   途中で止める = cancel (prepared を取り消す。active は変わらない = 毎晩の処理は前の持ち主のまま)
 *
 * 使い方 (miniPC):
 *   node scripts/company-db/master-ownership-epoch.mjs status
 *   node scripts/company-db/master-ownership-epoch.mjs prepare  [--actor 名前]
 *   node scripts/company-db/master-ownership-epoch.mjs activate [--actor 名前] [--data-dir D]
 *   node scripts/company-db/master-ownership-epoch.mjs cancel   [--actor 名前]
 * env: COMPANY_DB_URL (書く = DB を作ったユーザー。status は COMPANY_DB_WATCH_URL でも可) / DATA_DIR (activate が warehouse.db と証跡を読む)
 */
import 'dotenv/config';
import fs from 'node:fs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { MASTER_OWNERSHIP, validateOwnership } from '../../config/master-ownership.mjs';
import { readOwnershipState, prepareOwnership, activateOwnership, cancelPrepared, ownershipHashOf } from '../../apps/company-db/load/ownership-state.mjs';
import { checkPublishOwnership, readCurrentPublish, verifyApplied } from '../../apps/warehouse/master-publish.js';
import { latestBuild, publishOfBuild } from '../../apps/warehouse/master-material.js';
import { readEvidence } from '../../apps/company-db/push/evidence.mjs';
import { jstDateStr } from '../../lib/jst-date.js';
import { openPgClient, pgAdapter } from './migrate.mjs';

/**
 * activate してよいかの証拠 (miniPC の warehouse.db と今日の証跡)。そろったときだけ ok:
 *   最新の作り直しが prepared の持ち主の世代を使った / 今日の写しの証跡の世代が prepared の epoch で、入れた後の確かめ (apply) がその作り直しで verified /
 *   今もう一度読み直しても世代の値と同じ (作り直しの後に書き換えられていない)
 */
export function activationEvidence({ sqlite, dataDir, prepared, now = new Date(), read = readEvidence, taxRates }) {
  const problems = [];
  const build = latestBuild(sqlite);
  const bp = publishOfBuild(build);
  if (!build || !bp) problems.push('no_build_with_generation');
  else if (bp.ownership_hash !== prepared.hash) problems.push('build_not_prepared_epoch');
  // その世代が prepare の後に Company DB を読んだ (前の prepare のときの世代・作り直しで active にしない。#1564 の見直し L-1)
  if (bp) {
    const gen = sqlite.prepare('SELECT cdb_read_at, load_run_id FROM cdb_publish_generations WHERE generation_no = ?').get(bp.generation_no) || null;
    if (!gen) problems.push('build_generation_missing');
    else if (!prepared.prepared_at || !(String(gen.cdb_read_at) > String(prepared.prepared_at))) problems.push('generation_before_prepare');
  }
  const all = read(dataDir, jstDateStr(now));
  const ev = ['master-publish', 'master-publish.manual'].map((k) => all[k]).find((e) => e && e.apply && build && e.apply.build_id === build.build_id) ?? null;
  if (!ev) problems.push('no_verified_apply_evidence');
  else {
    if (ev.epochs?.generation?.kind !== 'prepared' || ev.epochs?.generation?.hash !== prepared.hash) problems.push('evidence_not_prepared_epoch');
    if (ev.apply.state !== 'verified') problems.push('apply_not_verified');
    if (bp && ev.apply.generation_no !== bp.generation_no) problems.push('evidence_generation_mismatch');
  }
  // 今もう一度確かめる (証跡の後に書き換えられていない)
  const publication = readCurrentPublish(sqlite);
  const again = verifyApplied(sqlite, { publication, ownership: prepared.map, taxRates });
  if (!again.ok) problems.push('applied_mismatch_now');
  if (bp && again.applied_hash !== bp.applied_hash) problems.push('applied_hash_changed');
  // 証拠の世代が読んだ夜間ロードと、その commit の番号 (0055。activate が「その後にロードが入っていない」を鍵の後に DB の番号で見る。#1564 Codex R3 High 1・R4 Medium 2)
  const genLoad = bp ? (sqlite.prepare('SELECT load_run_id, load_commit_seq FROM cdb_publish_generations WHERE generation_no = ?').get(bp.generation_no) || null) : null;
  const loadCommitSeq = genLoad && genLoad.load_commit_seq != null ? Number(genLoad.load_commit_seq) : null;
  if (bp && !Number.isSafeInteger(loadCommitSeq)) problems.push('generation_without_load_commit');
  return { ok: problems.length === 0, problems, evidence: build && bp ? { build_id: build.build_id, generation_no: bp.generation_no, generation_id: bp.generation_id,
    applied_hash: bp.applied_hash, verified_at: ev?.apply?.checked_at ?? null, load_run_id: genLoad?.load_run_id ?? null, load_commit_seq: loadCommitSeq } : null };
}

export async function cli(argv, { env = process.env, connect = null, openSqlite = null, log = console.log, ownership = MASTER_OWNERSHIP, now = new Date() } = {}) {
  const cmd = argv[0];
  const argAfter = (f) => { const i = argv.indexOf(f); return i >= 0 ? argv[i + 1] : null; };
  const actor = argAfter('--actor') || `${os.userInfo().username}@${os.hostname()}`;
  const url = (cmd === 'status' ? (env.COMPANY_DB_URL || env.COMPANY_DB_WATCH_URL) : env.COMPANY_DB_URL || '').trim();
  if (!['status', 'prepare', 'activate', 'cancel'].includes(cmd)) { log('使い方: status | prepare | activate | cancel'); return 2; }
  if (!url && !connect) { log('COMPANY_DB_URL が無い'); return 2; }
  const c = connect ? await connect() : await (async () => { const client = await openPgClient(url); return { db: pgAdapter(client), close: () => client.end() }; })();
  try {
    const st = await readOwnershipState(c.db);
    const configuredHash = ownershipHashOf(validateOwnership(ownership));
    if (cmd === 'status') {
      log(JSON.stringify({ state: st.state, configured: configuredHash, active: st.active.hash, prepared: st.prepared?.hash ?? null, activated_at: st.active.activated_at, prepared_at: st.prepared?.prepared_at ?? null,
        filled_as_load: { active: st.active.filled, prepared: st.prepared?.filled ?? [] } }, null, 1));   // 記録の後に足した列 = load として足した列
      return 0;
    }
    if (cmd === 'prepare') {
      const p = checkPublishOwnership(ownership);
      if (p.length) { log(`❌ 用意しない: ④a で扱えない持ち主の設定 (${p.join(' / ')})`); return 1; }
      const r = await prepareOwnership(c.db, { map: ownership, actor });
      log(`✅ prepared = ${r.prepared_hash} (active のまま)。次 = remote-load.mjs load --apply --wait --use-prepared → 写し → 作り直し → --verify-apply → activate`);
      return 0;
    }
    if (cmd === 'cancel') { const r = await cancelPrepared(c.db, { actor }); log(r.cancelled ? '✅ prepared を取り消した (active のまま)' : '⏭️ prepared は無い'); return 0; }
    // activate
    if (!st.prepared) { log('❌ prepared が無い'); return 1; }
    const dataDir = (argAfter('--data-dir') || env.DATA_DIR || '').trim();
    const sqlite = openSqlite ? await openSqlite(dataDir) : await (async () => { if (argAfter('--data-dir')) process.env.DATA_DIR = dataDir; const { initDB } = await import('../../apps/warehouse/db.js'); return initDB(); })();
    const { TAX_RATES } = await import('../../apps/warehouse/rebuild-m-products.js');
    const e = activationEvidence({ sqlite, dataDir, prepared: st.prepared, now, taxRates: TAX_RATES });
    if (!e.ok) { log(`❌ active にしない: 確かめがそろっていない (${e.problems.join('・')})`); return 1; }
    // 証拠を集めたときの prepare (ハッシュと時刻) を渡す = 行の鍵の後に比べる (その間に prepare し直されたら断る)
    //   + 証拠の世代が読んだ夜間ロードの commit の番号 = 鍵の後に「その後にロードが入っていない」を DB の番号で見る (入った = 断る)
    const r = await activateOwnership(c.db, { expectHash: st.prepared.hash, expectPreparedAt: st.prepared.prepared_at, expectLoadCommitSeq: e.evidence.load_commit_seq, actor,
      evidence: { ...e.evidence, prepared_at: st.prepared.prepared_at } });
    log(`✅ active = ${r.active_hash} (作り直し ${e.evidence.build_id}・世代 ${e.evidence.generation_no})。今夜から夜間ロードはこの持ち主`);
    return 0;
  } catch (err) {
    log(`❌ ${String(err && err.message).slice(0, 300)}`);
    return 1;
  } finally { if (c.close) { try { await c.close(); } catch { /* */ } } }
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  const code = await cli(process.argv.slice(2));
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
