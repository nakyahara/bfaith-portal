#!/usr/bin/env node
/**
 * master-ownership-epoch.mjs — 持ち主の設定の epoch を人が進めるコマンド (0055。マスタ正本切替 ④a・Codex #1564 R1 H1)
 *
 * 切替の日の順番 (🆕 #1610 = AI_reference 17 §4.2 の表が正。🚨 古い入口の門は active ∪ prepared の C の列の入口だけ閉じる = prepare の後の frozen で閉じる):
 *   1. config/master-ownership.mjs を書き換えてデプロイ (= configured。これだけでは何も変わらない。夜間ロード・写し・作り直しは active のまま)。readiness が両方の環境で 0
 *   2. prepare   (miniPC)        : configured を prepared として Company DB に記録する (一緒に切り替える組・写さない列を確かめる)。段階は legacy_open のまま
 *   2a. master-cutover.mjs --to frozen (この瞬間から prepared の C の列の入口だけ閉じる) → 全部のプロセスの書きかけ 0
 *   2b. 最後の active (全部 load) のロード = remote-load.mjs load --apply --wait (--use-prepared を付けない・prepare をまたいだ古い書き込みの回収)
 *       → そのロードの run_id の report の成功 + 照合 ② (照合 ① は 02:00 のロードだけ)
 *   3. node scripts/company-db/remote-load.mjs load --apply --wait --use-prepared   (prepared の持ち主で 1 回だけロード)
 *   4. node apps/company-db/publish/fetch.mjs → m_products 再構築 → fetch.mjs --verify-apply   (miniPC。prepared の世代を作り直しで入れて確かめる)
 *   5. activate  (miniPC)        : 4 の確かめが通った証拠 (今の作り直しの記録・prepare の後に読んだ世代・証跡の apply) があり、
 *                                   切替の段階 (⑤-1 の ops.master_cutover_state) が frozen のときだけ prepared → active
 *   途中で止める = cancel (prepared を取り消す。active は変わらない = 毎晩の処理は前の持ち主のまま)
 *
 * 🆕 広げる道 (0058・段階 new_open のまま持ち主に company のキーを足す。設計 = 広げる道 v11 §5。PR-8 の前に足せるのは skus.sku_kind だけ):
 *   1. prepare --widen --company 1   : 試み (widen_prepare_id・base_commit_seq・ロードの規則の指紋・この checkout の古い入口の一覧) を作る。
 *                                      🚨 段階 new_open では、ふつうの prepare / activate / cancel は DB が拒む (G5)
 *   2. stop-manual --attempt <id> --entry ne:set-kind --by <名前>   : 足すキーに関係する手の入口を止めた記録 (DB の時刻。miniPC の NE の取得が走っていないことを確かめてから)
 *   3. 全部のプロセスの門の記録 (2 版) が prepared を見て書きかけ 0
 *   4. 回収のロード (active): NE の取得 (停止の後) → 作り直し → Render の同期 → remote-load.mjs load --apply --wait
 *   5. remote-load.mjs load --apply --wait --use-prepared (1 回だけ) → fetch.mjs --daily → 作り直し → --verify-apply
 *   5b. check --attempt <id> --company 1   : widen と同じ判定を読むだけで (COMPANY_DB_WATCH_URL でも可)
 *   6. widen --attempt <id> --company 1    : 写しの証拠 (activate と同じ) を集めて DB の関数 ops.widen_master_ownership (lock_timeout 5 秒)
 *   途中で止める = cancel (開いている試みがあれば試みを cancelled に・prepared を消す。次の prepare は新しい試み・新しい base)
 * 🆕 0059 (Amazon SKU の対応の PR-B): 足すキーに listing_components.amazon がある試みは、check と widen が古い表 (DATA_DIR の warehouse.db の
 *   m_sku_master / m_sku_components) と Company DB (active の対応) の写しの決まった並べ方のハッシュを照らす (違えば check は ok: false・widen はしない)。
 *   widen は 3 つの排他の鍵を取った後に同じ取引で Company DB を読み、ハッシュと行の数を写しの証拠 (amazon_map) に入れる = DB が鍵の後に数え直して照らす。
 *   止める手の入口 = gas:logizard-sheet-and-sku-map (SKU タブ・SKU の CSV・API・cli:import-sku-master.js は門の記録 (ack) で止まる入口)
 *
 * 使い方 (miniPC):
 *   node scripts/company-db/master-ownership-epoch.mjs status
 *   node scripts/company-db/master-ownership-epoch.mjs prepare  [--actor 名前]
 *   node scripts/company-db/master-ownership-epoch.mjs prepare --widen --company 1 [--actor 名前]
 *   node scripts/company-db/master-ownership-epoch.mjs stop-manual --attempt <id> --entry <入口の id> --by <止めた人> [--note メモ]
 *   node scripts/company-db/master-ownership-epoch.mjs check --attempt <id> --company 1 [--data-dir D]   (Amazon を足す試みは DATA_DIR / --data-dir の warehouse.db と fba.db が要る。
 *     fba.db = Sheet にだけある SKU の出品に構成が 1 行も無いかを照らす・#1651。widen も鍵の後に同じく照らす。Amazon を company にする activate は断る)
 *   node scripts/company-db/master-ownership-epoch.mjs widen --attempt <id> --company 1 [--actor 名前] [--data-dir D]
 *   node scripts/company-db/master-ownership-epoch.mjs activate [--actor 名前] [--data-dir D]
 *   node scripts/company-db/master-ownership-epoch.mjs cancel   [--actor 名前] [--attempt <id>]
 * env: COMPANY_DB_URL (書く = DB を作ったユーザー。status・check は COMPANY_DB_WATCH_URL でも可) / DATA_DIR (activate・widen が warehouse.db と証跡を読む)
 * 🚨 commit の番号 (base_commit_seq・ロードの番号) は bigint = 文字のまま出す (JS の Number にしない)
 */
import 'dotenv/config';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MASTER_OWNERSHIP, validateOwnership } from '../../config/master-ownership.mjs';
import { configuredBeyondCapable, capabilityFingerprint, COMPANY_CAPABLE } from '../../config/master-capability.mjs';
import { readOwnershipState, prepareOwnership, activateOwnership, cancelPrepared, ownershipHashOf, isCommitSeqText } from '../../apps/company-db/load/ownership-state.mjs';
import { readOpenWidenAttempt, prepareWiden, recordWidenManualStop, cancelWiden, widenCheck, widenOwnership } from '../../apps/company-db/load/widen-state.mjs';
import { checkPublishOwnership, readCurrentPublish, verifyApplied } from '../../apps/warehouse/master-publish.js';
import { latestBuild, publishOfBuild } from '../../apps/warehouse/master-material.js';
import { readEvidence } from '../../apps/company-db/push/evidence.mjs';
import { jstDateStr } from '../../lib/jst-date.js';
import { openPgClient, pgAdapter } from './migrate.mjs';

/**
 * activate してよいかの証拠 (miniPC の warehouse.db と今日の証跡)。そろったときだけ ok:
 *   最新の作り直しが prepared の持ち主の世代を使った / 今日の写しの証跡の世代が prepared の epoch で、入れた後の確かめ (apply) がその作り直しで verified /
 *   今もう一度読み直しても世代の値と同じ (作り直しの後に書き換えられていない)
 * 🆕 広げる道の widen も同じ証拠を使う (DB は load_commit_seq が試みの prepared のロードと同じかを見る)
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
  //   🚨 番号は bigint = SQLite でも文字で読む (CAST。JS の Number にしない = 2^53 を超えても丸めない。広げる道 PR-1)
  const genLoad = bp ? (sqlite.prepare('SELECT load_run_id, CAST(load_commit_seq AS TEXT) AS load_commit_seq FROM cdb_publish_generations WHERE generation_no = ?').get(bp.generation_no) || null) : null;
  const loadCommitSeq = genLoad && genLoad.load_commit_seq != null ? String(genLoad.load_commit_seq) : null;
  if (bp && !isCommitSeqText(loadCommitSeq)) problems.push('generation_without_load_commit');
  return { ok: problems.length === 0, problems, evidence: build && bp ? { build_id: build.build_id, generation_no: bp.generation_no, generation_id: bp.generation_id,
    applied_hash: bp.applied_hash, verified_at: ev?.apply?.checked_at ?? null, load_run_id: genLoad?.load_run_id ?? null, load_commit_seq: loadCommitSeq } : null };
}

const COMMANDS = ['status', 'prepare', 'activate', 'cancel', 'stop-manual', 'check', 'widen'];
const READ_ONLY = new Set(['status', 'check']);

/** Amazon SKU の対応の持ち主表のキー (lib/amazon-map-migrate.mjs の AMAZON_MAP_WIDEN_KEY と同じ) */
const AMAZON_KEY = 'listing_components.amazon';

/** 古い表 (SKU マスタ) を読む。sqlite (開いた warehouse.db) があればそれから・無ければ dataDir の warehouse.db を読むだけで開く */
async function readLegacyDefault({ dataDir, sqlite }) {
  const M = await import('../../lib/amazon-map-migrate.mjs');
  if (sqlite) return M.readLegacyAmazonMapsFrom(sqlite);
  if (!dataDir) throw new Error('DATA_DIR (--data-dir) が無い = 古い表 (warehouse.db) を読めない');
  return M.readLegacyAmazonMaps(path.join(dataDir, 'warehouse.db'));
}

/**
 * 今の fba.db (Sheet の写し) の Sheet にだけある SKU を読む (#1651)。dataDir が無ければ warehouse/db.js と同じ既定 (cwd/data)。
 * 読めない (ファイル・sku_mapping の表が無い) = 投げる = 照らせない = 広げない
 */
async function readSheetOnlyDefault({ dataDir, legacy }) {
  const M = await import('../../lib/amazon-map-migrate.mjs');
  const file = path.join(dataDir || path.join(process.cwd(), 'data'), 'fba.db');
  if (!fs.existsSync(file)) throw new Error(`fba.db が無い = Sheet にだけある SKU を読めない: ${file}`);
  return M.readSheetOnlySkus(file, legacy);
}

export async function cli(argv, { env = process.env, connect = null, openSqlite = null, log = console.log, ownership = MASTER_OWNERSHIP, now = new Date(),
  capable = COMPANY_CAPABLE, loaderFingerprint = undefined, manifest = undefined, readLegacy = readLegacyDefault, readSheetOnly = readSheetOnlyDefault } = {}) {
  const cmd = argv[0];
  const argAfter = (f) => { const i = argv.indexOf(f); return i >= 0 ? argv[i + 1] : null; };
  const actor = argAfter('--actor') || `${os.userInfo().username}@${os.hostname()}`;
  const url = (READ_ONLY.has(cmd) ? (env.COMPANY_DB_URL || env.COMPANY_DB_WATCH_URL) : env.COMPANY_DB_URL || '').trim();
  if (!COMMANDS.includes(cmd)) { log(`使い方: ${COMMANDS.join(' | ')}`); return 2; }
  if (!url && !connect) { log('COMPANY_DB_URL が無い'); return 2; }
  const companyArg = argAfter('--company');
  const companyId = companyArg == null ? null : (/^[0-9]{1,5}$/.test(companyArg) ? Number(companyArg) : NaN);
  const needCompany = cmd === 'check' || cmd === 'widen' || (cmd === 'prepare' && argv.includes('--widen'));
  if (needCompany && !Number.isInteger(companyId)) { log('❌ --company <番号> が要る (今は 1 だけ)'); return 2; }
  const attemptId = argAfter('--attempt');
  if (['stop-manual', 'check', 'widen'].includes(cmd) && !/^[0-9a-f-]{36}$/.test(String(attemptId || ''))) { log('❌ --attempt <widen_prepare_id (uuid)> が要る'); return 2; }
  const c = connect ? await connect() : await (async () => { const client = await openPgClient(url); return { db: pgAdapter(client), close: () => client.end() }; })();
  try {
    if (cmd === 'check') {
      const r = await widenCheck(c.db, { attemptId, companyId });
      // 🆕 0059: Amazon の対応を足す試み = 古い表と Company DB のハッシュも照らす (読むだけ・鍵は取らない。widen は鍵の後にもう一度)
      if (r && Array.isArray(r.added_keys) && r.added_keys.includes(AMAZON_KEY)) {
        try {
          const M = await import('../../lib/amazon-map-migrate.mjs');
          const dataDirC = (argAfter('--data-dir') || env.DATA_DIR || '').trim();
          const legacy = await readLegacy({ dataDir: dataDirC, sqlite: null });
          r.amazon_map = await M.amazonMapHashEvidence(c.db, legacy);
          if (!r.amazon_map.match) { r.ok = false; r.problems = [...(r.problems || []), `amazon_map_hash: 古い表 ${r.amazon_map.legacy_hash || r.amazon_map.error} と Company DB ${r.amazon_map.company_hash || r.amazon_map.error} のハッシュが違う`]; }
          // 🆕 #1651: 今の fba.db の Sheet にだけある SKU の出品 (対応なし) に構成が 1 行でもある = ok: false (出どころによらない・人が見て決める。widen も鍵の後に同じく照らす)
          try {
            const rows = await M.freshSheetOnlyComponents(c.db, () => readSheetOnly({ dataDir: dataDirC, legacy }));   // widen と同じ読み方 (fba.db を読んで続けて Company DB)
            r.amazon_map.sheet_only_components = rows.length;
            if (rows.length) { r.ok = false; r.problems = [...(r.problems || []), `sheet_only_has_components: Sheet にだけある SKU の出品に構成が ${rows.length} 行ある (${rows.slice(0, 3).map((x) => `${x.listing_code}→${x.sku_code} (${x.source ?? '?'})`).join('・')})。人が見て決めてから`]; }
          } catch (e) {
            r.ok = false; r.problems = [...(r.problems || []), `sheet_only_has_components: 照らせない (${String(e && e.message).slice(0, 200)})`];
          }
        } catch (e) {
          r.ok = false; r.problems = [...(r.problems || []), `amazon_map_hash: 照らせない (${String(e && e.message).slice(0, 200)})`];
        }
      }
      log(JSON.stringify(r, null, 1));
      return r && r.ok === true ? 0 : 1;
    }
    const st = await readOwnershipState(c.db);
    const configuredHash = ownershipHashOf(validateOwnership(ownership));
    if (cmd === 'status') {
      const open = await readOpenWidenAttempt(c.db);
      log(JSON.stringify({ state: st.state, configured: configuredHash, capability: capabilityFingerprint(), active: st.active.hash, prepared: st.prepared?.hash ?? null, activated_at: st.active.activated_at, prepared_at: st.prepared?.prepared_at ?? null,
        filled_as_load: { active: st.active.filled, prepared: st.prepared?.filled ?? [] },   // 記録の後に足した列 = load として足した列
        widen_attempt: open }, null, 1));
      return 0;
    }
    if (cmd === 'prepare' && argv.includes('--widen')) {
      const p = checkPublishOwnership(ownership);
      if (p.length) { log(`❌ 用意しない: ④a で扱えない持ち主の設定 (${p.join(' / ')})`); return 1; }
      // 広げる道 PR-0: このコードが company として扱えないキー (config/master-capability.mjs の COMPANY_CAPABLE の外) は広げない = 能力の PR が先
      const beyondW = configuredBeyondCapable(ownership, capable);
      if (beyondW.length) { log(`❌ 用意しない: このコードが company として扱えないキー (${beyondW.join('・')})。config/master-capability.mjs の COMPANY_CAPABLE に足す PR を全部の場所に配ってから`); return 1; }
      const lf = loaderFingerprint !== undefined ? loaderFingerprint : (await import('../../apps/company-db/load/engine.mjs')).LOAD_RULE_FINGERPRINT;
      const mf = manifest !== undefined ? manifest : (await import('../../lib/master-legacy-gate.mjs')).legacyManifest();
      if (!/^[0-9a-f]{64}$/.test(String(lf || ''))) { log('❌ 夜間ロードの規則の指紋 (LOAD_RULE_FINGERPRINT) を計算できない'); return 1; }
      const r = await prepareWiden(c.db, { companyId, map: validateOwnership(ownership), loaderFingerprint: lf, manifest: mf, actor });
      log(`✅ 試み ${r.widen_prepare_id} (足すキー ${r.added_keys.join(', ')}・base_commit_seq ${r.base_commit_seq})。段階は new_open のまま・active のまま。`
        + `次 = 止める手の入口 (${r.required_manual_entries.join(', ') || 'なし'}) を stop-manual で記録 (miniPC の NE の取得が走っていないことを確かめてから) → 全部のプロセスの 2 版の記録 → `
        + '回収のロード (--use-prepared なし) → --use-prepared のロード 1 回 → 写し → 作り直し → --verify-apply → check → widen (設計 §5)');
      return 0;
    }
    if (cmd === 'prepare') {
      const p = checkPublishOwnership(ownership);
      if (p.length) { log(`❌ 用意しない: ④a で扱えない持ち主の設定 (${p.join(' / ')})`); return 1; }
      // 広げる道 PR-0: このコードが company として扱えないキー (config/master-capability.mjs の COMPANY_CAPABLE の外) は用意しない = 能力の PR が先
      const beyond = configuredBeyondCapable(ownership, capable);
      if (beyond.length) { log(`❌ 用意しない: このコードが company として扱えないキー (${beyond.join('・')})。config/master-capability.mjs の COMPANY_CAPABLE に足す PR を全部の場所に配ってから`); return 1; }
      const r = await prepareOwnership(c.db, { map: ownership, actor });
      log(`✅ prepared = ${r.prepared_hash} (active のまま)。次 = master-cutover.mjs --to frozen → 書きかけ 0 → 最後の active (全部 load) のロード (--use-prepared なし) → その run_id の report + 照合 ② → remote-load.mjs load --apply --wait --use-prepared → 写し → 作り直し → --verify-apply → activate (17 §4.2)。🚨 activate の前に cancel すると、今回 prepared で C にした列の古い入口が再び開く (activate の後は対象外)`);
      return 0;
    }
    if (cmd === 'stop-manual') {
      const entryId = argAfter('--entry'), by = argAfter('--by');
      if (!entryId || !by) { log('❌ --entry <手の入口の id> と --by <止めた人> が要る'); return 2; }
      const r = await recordWidenManualStop(c.db, { attemptId, entryId, stoppedBy: by, note: argAfter('--note') });
      log(`✅ 手の入口 ${r.entry_id} を止めた記録 (DB の時刻 ${r.stopped_at})`);
      return 0;
    }
    if (cmd === 'cancel') {
      const open = await readOpenWidenAttempt(c.db);
      if (open || attemptId) {
        const id = attemptId || open.widen_prepare_id;
        await cancelWiden(c.db, { attemptId: id, actor });
        log(`✅ 試み ${id} を取り消した (prepared を消した・active のまま)。次の prepare --widen は新しい試み (新しい base)`);
        return 0;
      }
      const r = await cancelPrepared(c.db, { actor }); log(r.cancelled ? '✅ prepared を取り消した (active のまま)' : '⏭️ prepared は無い'); return 0;
    }
    // activate / widen = 写しの証拠を集める
    if (!st.prepared) { log('❌ prepared が無い'); return 1; }
    // #1651 Codex R3 Medium: Amazon の構成 (listing_components.amazon) を company にするのは widen の道だけ (Sheet にだけある SKU の出品の構成を鍵の後に照らすのは widen)。
    //   activate の道は断る (今は prepare も not_copied で断る = 二重の守り。⑦-2 で prepare を開けても activate では足さない)
    if (cmd === 'activate' && st.prepared.map[AMAZON_KEY] === 'company' && st.active.map[AMAZON_KEY] !== 'company') {
      log(`❌ active にしない: ${AMAZON_KEY} を company にするのは widen の道 (prepare --widen → check → widen) だけ (#1651)`);
      return 1;
    }
    const dataDir = (argAfter('--data-dir') || env.DATA_DIR || '').trim();
    const sqlite = openSqlite ? await openSqlite(dataDir) : await (async () => { if (argAfter('--data-dir')) process.env.DATA_DIR = dataDir; const { initDB } = await import('../../apps/warehouse/db.js'); return initDB(); })();
    const { TAX_RATES } = await import('../../apps/warehouse/rebuild-m-products.js');
    const e = activationEvidence({ sqlite, dataDir, prepared: st.prepared, now, taxRates: TAX_RATES });
    if (!e.ok) { log(`❌ ${cmd === 'widen' ? '広げない' : 'active にしない'}: 確かめがそろっていない (${e.problems.join('・')})`); return 1; }
    if (cmd === 'widen') {
      // 🆕 0059: Amazon の対応を足す試み = 古い表を読んでおき、鍵の後に同じ取引で Company DB を読んでハッシュを照らす (違えば広げない)
      const open = await readOpenWidenAttempt(c.db);
      const amazon = !!open && open.widen_prepare_id === attemptId && (open.added_keys || []).includes(AMAZON_KEY);
      //   #1651: 鍵の後に今の fba.db を読み直し (鍵の前の一覧は使わない = R4 High)、その出品 (対応なし) に構成が 1 行も無いかも照らす (ある = 広げない・読めない = 広げない)
      let step = null;
      if (amazon) {
        const legacy = await readLegacy({ dataDir, sqlite });
        step = (await import('../../lib/amazon-map-migrate.mjs')).amazonWidenEvidenceStep(legacy, { readSheetOnly: () => readSheetOnly({ dataDir, legacy }) });
      }
      // 判定は DB (ops.widen_master_ownership が鍵の後に本体を呼ぶ)。夜間ロードの最中は 5 秒で諦める (lock_timeout)
      const r = await widenOwnership(c.db, { attemptId, companyId, actor, evidence: { ...e.evidence, prepared_at: st.prepared.prepared_at }, beforeCall: step ? step.beforeCall : null });
      const amz = step ? step.result() : null;
      log(`✅ 広げた: ${r.added_keys.join(', ')} = company (active = ${r.active_hash}・段階は new_open のまま・ロード ${r.loads?.recovery?.commit_seq} → ${r.loads?.prepared?.commit_seq})${amz ? `・Amazon の対応のハッシュ ${amz.company_hash} (対応 ${amz.master_rows}・構成 ${amz.component_rows}) = 古い表と同じ` : ''}。今夜から夜間ロードはこの持ち主`);
      return 0;
    }
    // 証拠を集めたときの prepare (ハッシュと時刻) を渡す = 行の鍵の後に比べる (その間に prepare し直されたら断る)
    //   + 証拠の世代が読んだ夜間ロードの commit の番号 (文字) = 鍵の後に「その後にロードが入っていない」を DB の番号で見る (入った = 断る)
    const r = await activateOwnership(c.db, { expectHash: st.prepared.hash, expectPreparedAt: st.prepared.prepared_at, expectLoadCommitSeq: e.evidence.load_commit_seq, actor,
      evidence: { ...e.evidence, prepared_at: st.prepared.prepared_at } });
    log(`✅ active = ${r.active_hash} (作り直し ${e.evidence.build_id}・世代 ${e.evidence.generation_no})。今夜から夜間ロードはこの持ち主`);
    return 0;
  } catch (err) {
    log(`❌ ${String(err && err.message).slice(0, 600)}`);
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
