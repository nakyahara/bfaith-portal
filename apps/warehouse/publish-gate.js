/**
 * publish-gate.js — Company DB の写しの反映が世代と違う朝に、daily-sync の後の工程を止める (マスタ正本切替 ④a・Codex #1564 R1 H4)
 *
 * いつ: fetch.mjs --verify-apply が exit 4 (applied_broken = m_products・上書き表の値が世代と違う。持ち主が C の列があるときだけ起きる)。
 * 何を止めるか: m_products・m_set_components・上書き表 (exception_genka・product_shipping・m_reorder_setting・product_tax_rate・product_sales_class)・
 *   m_products_history と、それらから作る表 (f_sales・各モールの財務の日次) を読む工程 = 古い表の違う値を先 (Render・Company DB・商品管理リスト) へ配らない。
 *   止めた工程は「⚠️ 見送り」(success: false・blocked: true = 再試行に載せない。翌朝の作り直しで直ったら普通に流れる)。
 *   自分で ping を打つ工程 (台帳の項目がある) は、止めた朝に daily-sync が代わりに fail の ping を打つ (ok が進まない = 締切を待たずに見える)。
 * 止めない工程 (UNGATED): 読まない工程と、壊れを見つけて知らせる工程 (照合・見張り・バックアップ)。理由つき。
 * 🚨 写しの反映より後の工程は GATED か UNGATED のどちらかに必ず載せる (試験 = scripts/test-master-publish.mjs が daily-sync を読んで確かめる。
 *   載っていない工程を足すと試験が落ちる = 止めるかどうかを決めてから足す)
 * 🚨 止めるかどうかの正 = warehouse.db の門の 1 行 (cdb_publish_gate。safe / broken / unknown。#1564 Codex R2 High 2)。readPublishGate
 *   daily-sync (exit 4 と両方)・自動再試行 (retry-failed-jobs.js)・商品管理リストの手の更新 (fba-service.js → pml-fba-refresh.js) が同じ読み手を使う
 *   (daily-sync のプロセスの中だけの印にしない = 別のプロセスの再試行・手の更新が抜け道にならない。#1564 の見直し M-2)。
 *   書くのは入れた後の確かめ (fetch.mjs --verify-apply) だけ: 違う = broken (証跡より先に書く) / 通った = safe (broken を戻せるのはこれだけ) /
 *   遅れ・確かめられない = 前の値のまま (行が無く持ち主が C = unknown)。証跡 master-publish の apply.broken は人が読む控え (日付で消える・読めない日がある = 正にしない)
 *   行が無い = 持ち主が全部 load と分かる (確かめた今の世代と、世代を使った最新の作り直しが両方あり、どちらの持ち主も全部 load) ときだけ流してよい。
 *   読めない (表はあるが SELECT が落ちる・ファイルが開けない) = unknown = 止める (「行が無い」と同じにしない。#1564 Codex R3 High 2)
 * 🚨 safe の行は「確かめた作り直し・世代・入れた値のハッシュ・持ち主」を持つ。読み手は最新の作り直し・その世代・今の古い表から作り直したハッシュと比べ、
 *   違えば (確かめた後に作り直した・書き換えられた・broken を書けなかった) その safe は使わない = 持ち主が全部 load と分かるときだけ流す・それ以外は unknown
 * 🚨 確かめが通らなかった朝 (落ちた・exit 1。#1564 Codex R5 Medium): 確かめた safe の行が今も合う (遅れの朝) ときだけ流す。それ以外は
 *   確かめの側が unknown を残す (markGateUnknown。broken はそのまま) = 自動再試行・手の更新も止まる / daily-sync もその回を止める (gateAfterVerify)
 *   遅れだけ (今朝の写しが取れない・作り直しが前の世代) で古い表が作り直しの世代と合う朝は、確かめの側がその作り直しの safe を書く (#1564 Codex R6 Medium)
 * 🚨 止めの印 (#1564 Codex R6 Low) = DATA_DIR の cdb-publish-gate.stop.json (warehouse.db の隣・書くときは別名で書いて名前を替える = 書きかけを残さない)。
 *   warehouse.db を開けない・門を書けない (SQLITE_BUSY など) 回に、確かめの側が残す。印がある間は readPublishGate が止める (行・暗黙の safe より先)。
 *   消すのは確かめの側が safe の行を書けた回だけ (= 故障が直った後も、通った確かめまで止まったまま)
 */
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { readCurrentPublish, readPublishGeneration, verifyApplied, ownershipHash } from './master-publish.js';
import { latestBuild, publishOfBuild } from './master-material.js';
import { ALL_LOAD } from '../company-db/load/ownership-state.mjs';
import { TAX_RATES } from '../../lib/master-set-rules.js';

export const GATE_STATES = Object.freeze(['safe', 'broken', 'unknown']);
/** 止めの印のファイルの名前 (DATA_DIR の中・warehouse.db の隣) */
export const GATE_STOP_MARK = 'cdb-publish-gate.stop.json';
/** 印のファイルの場所 (dataDir か、開いた warehouse.db の場所から)。分からない = null */
export function stopMarkPath({ db = null, dataDir = null } = {}) {
  if (dataDir) return path.join(dataDir, GATE_STOP_MARK);
  const name = db && typeof db.name === 'string' ? db.name : '';
  return name && name !== ':memory:' ? path.join(path.dirname(name), GATE_STOP_MARK) : null;
}
/** 止めの印を書く (別名で書いてから名前を替える = 書きかけを読ませない)。@returns {{ ok: boolean, file: string|null, error: string|null }} */
export function writeStopMark(target, { state = 'unknown', reason, now = new Date() }) {
  const file = stopMarkPath(target);
  if (!file) return { ok: false, file: null, error: 'no_data_dir' };
  const tmpFile = `${file}.${process.pid}.${Date.now()}.tmp`;
  try {
    fs.writeFileSync(tmpFile, JSON.stringify({ state: state === 'broken' ? 'broken' : 'unknown', reason: String(reason ?? '').slice(0, 400), at: now.toISOString() }));
    fs.renameSync(tmpFile, file);
    return { ok: true, file, error: null };
  } catch (e) {
    try { fs.rmSync(tmpFile, { force: true }); } catch { /* */ }
    return { ok: false, file, error: String(e && e.message).slice(0, 200) };
  }
}
/** 止めの印を読む。無い = null / 読めない・壊れた = 止める (unknown) */
export function readStopMark(target) {
  const file = stopMarkPath(target);
  if (!file || !fs.existsSync(file)) return null;
  try {
    const m = JSON.parse(fs.readFileSync(file, 'utf8'));
    return { state: m && m.state === 'broken' ? 'broken' : 'unknown', reason: String((m && m.reason) || 'stop_mark'), at: (m && m.at) || null };
  } catch (e) { return { state: 'unknown', reason: `stop_mark_unreadable: ${String(e && e.message).slice(0, 120)}`, at: null }; }
}
/** 止めの印を消す (確かめの側が safe の行を書けた回だけ)。@returns {{ ok: boolean, removed: boolean, error: string|null }} */
export function clearStopMark(target) {
  const file = stopMarkPath(target);
  if (!file || !fs.existsSync(file)) return { ok: true, removed: false, error: null };
  try { fs.rmSync(file); return { ok: true, removed: true, error: null }; } catch (e) { return { ok: false, removed: false, error: String(e && e.message).slice(0, 200) }; }
}
const GATE_COLS = ['state', 'reason', 'build_id', 'generation_no', 'applied_hash', 'ownership_hash', 'checked_at', 'updated_at'];
/**
 * 門の行。表が無い・行が無い = null。🚨 表はあるが読めない (SELECT が落ちる・列が無い) = 投げる = 呼び手が unknown (止める) にする
 *   (読めないを「行が無い」= 持ち主が全部 load なら流す、と同じにしない。#1564 Codex R3 High 2)
 */
export function readGateRow(db) {
  const has = db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = 'cdb_publish_gate'").get();
  if (!has) return null;
  return db.prepare(`SELECT ${GATE_COLS.join(', ')} FROM cdb_publish_gate WHERE id = 1`).get() || null;
}
/** 門を書く (入れた後の確かめだけが呼ぶ)。safe は確かめた作り直し・世代・入れた値のハッシュ・持ち主のハッシュと一緒に書く (読み手が今と比べる) */
export function writePublishGate(db, { state, reason = null, buildId = null, generationNo = null, appliedHash = null, ownershipHash: ownHash = null, checkedAt, now = new Date() }) {
  if (!GATE_STATES.includes(state)) throw new Error(`門の値が不正: ${state}`);
  if (state === 'safe' && (!buildId || generationNo == null || !appliedHash || !ownHash)) throw new Error('safe は確かめた作り直し・世代・入れた値のハッシュ・持ち主のハッシュと一緒に書く');
  db.prepare(`INSERT INTO cdb_publish_gate (id, state, reason, build_id, generation_no, applied_hash, ownership_hash, checked_at, updated_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO UPDATE SET state = excluded.state, reason = excluded.reason, build_id = excluded.build_id, generation_no = excluded.generation_no,
      applied_hash = excluded.applied_hash, ownership_hash = excluded.ownership_hash, checked_at = excluded.checked_at, updated_at = excluded.updated_at`)
    .run(state, reason == null ? null : String(reason).slice(0, 400), buildId, generationNo, appliedHash, ownHash, checkedAt, now.toISOString());
  return state;
}
const ALL_LOAD_HASH = ownershipHash(ALL_LOAD);
/** 最新の作り直しと、それが使った世代 (読めない = 投げる) */
function latestBuildState(db) {
  const build = latestBuild(db);
  return { build, bp: publishOfBuild(build) };
}
/**
 * 持ち主が全部 load と分かるか (行が無い・古い safe の行のときに流してよいか。#1564 Codex R3 High 2)。
 * 分かる = 確かめた今の世代がある・その持ち主が全部 load・最新の作り直しがあり **今の世代** を使った (番号・ID・中身のハッシュが同じ。#1564 Codex R4 Medium 1)・
 *   その持ち主も全部 load。どれか欠ける・読めない・作り直しが前の世代のまま (写しの後に作り直しが失敗した・飛ばした) = 分からない
 * @returns {{ ok: boolean, why: string|null }}
 */
export function knownAllLoad(db, st = latestBuildState(db)) {
  const head = readCurrentPublish(db, { values: false });
  if (head.problem || !head.generation) return { ok: false, why: `no_current_generation:${head.problem || 'none'}` };
  let own;
  try { own = JSON.parse(head.generation.ownership); } catch { return { ok: false, why: 'generation_ownership_unreadable' }; }
  if (!own || typeof own !== 'object' || ownershipHash(own) !== ALL_LOAD_HASH) return { ok: false, why: 'company_owner' };
  if (!st.build) return { ok: false, why: 'no_build' };
  if (!st.bp) return { ok: false, why: 'build_without_generation' };
  if (st.bp.ownership_hash !== ALL_LOAD_HASH) return { ok: false, why: 'company_owner' };
  const g = head.generation;
  if (st.bp.generation_no !== g.generation_no || (st.bp.generation_id ?? null) !== (g.generation_id ?? null) || (st.bp.content_hash ?? null) !== (g.content_hash ?? null)) {
    return { ok: false, why: 'build_not_current_generation' };
  }
  return { ok: true, why: null };
}
/**
 * safe の行が今の状態を確かめたものか。違う理由 (null = 同じ):
 *   最新の作り直し・その世代・持ち主・入れた値のハッシュ (記録) が行と同じ + 今の古い表から作り直したハッシュも同じ (作り直しの後に書き換えられていない)
 */
export function staleSafeRow(db, row, st = latestBuildState(db), { taxRates = TAX_RATES } = {}) {
  if (!st.build) return 'no_build';
  if (row.build_id !== st.build.build_id) return 'build_changed';
  if (!st.bp) return 'build_without_generation';
  if (row.generation_no !== st.bp.generation_no) return 'generation_changed';
  if (!row.applied_hash || row.applied_hash !== st.bp.applied_hash) return 'applied_hash_changed';
  if (!row.ownership_hash || row.ownership_hash !== st.bp.ownership_hash) return 'ownership_changed';
  const publication = readPublishGeneration(db, st.bp.generation_no);
  if (publication.problem) return `generation_${publication.problem}`;
  let ownership;
  try { ownership = JSON.parse(publication.generation.ownership); } catch { return 'generation_ownership_unreadable'; }
  const now = verifyApplied(db, { publication, ownership, taxRates });
  if (!now.ok || now.applied_hash !== row.applied_hash) return 'applied_changed_now';
  return null;
}
const closed = (state, reason, extra = {}) => ({ state, open: false, broken: true, reason, checked_at: null, build_id: null, generation_no: null, source: 'none', ...extra });
/** 門の今の値 (開いている = open)。読めない = 投げる (readPublishGate が unknown にする) */
export function gateOfDb(db) {
  const row = readGateRow(db);
  const fromRow = row ? { checked_at: row.checked_at, build_id: row.build_id ?? null, generation_no: row.generation_no ?? null } : {};
  if (row && row.state !== 'safe') return closed(row.state, row.reason ?? row.state, { ...fromRow, source: 'row' });
  const st = latestBuildState(db);
  if (row) {
    const stale = staleSafeRow(db, row, st);
    if (!stale) return { state: 'safe', open: true, broken: false, reason: row.reason ?? 'safe', ...fromRow, source: 'row' };
    // 確かめた後に変わった safe = 使わない。持ち主が全部 load と分かるときだけ流す (写す値が無い = 古い表は NE の値のまま)
    const all = knownAllLoad(db, st);
    if (all.ok) return { state: 'safe', open: true, broken: false, reason: `all_load_safe_row_stale:${stale}`, ...fromRow, source: 'implicit' };
    return closed('unknown', `safe_row_stale:${stale}`, { ...fromRow, source: 'row' });
  }
  const all = knownAllLoad(db, st);
  if (all.ok) return { state: 'safe', open: true, broken: false, reason: 'all_load_no_gate_row', checked_at: null, build_id: null, generation_no: null, source: 'implicit' };
  // 持ち主が C なのに一度も確かめていない / 全部 load と分からない (世代・作り直しが無い・読めない) = 止める
  return closed('unknown', all.why === 'company_owner' ? 'no_gate_row_with_company_owner' : `no_gate_row_unverified:${all.why}`);
}
/** 門が「確かめた safe の行」(今の作り直し・世代・古い表と合う) か。読めない = いいえ */
export function validSafeRow(db) {
  try { const g = gateOfDb(db); return g.open === true && g.source === 'row'; } catch { return false; }
}
/**
 * 確かめ (fetch.mjs --verify-apply) が決める前に落ちた・確かめが通らなかった回に、門を unknown にする (#1564 Codex R5 Medium) =
 *   自動再試行・商品管理リストの手の更新も止まる (暗黙の safe = 「行が無く全部 load」で流さない)。
 *   書かない: broken の行 (戻せるのは通った確かめだけ・unknown に替えない) / 確かめた safe の行が今も合う (遅れの朝 = 前の確かめのまま流す)
 * @returns {{ wrote: boolean, kept: string|null, error: string|null }}
 */
export function markGateUnknown(db, { reason, now = new Date(), dataDir = null }) {
  let row = null;
  try { row = readGateRow(db); } catch { row = null; }
  if (row && row.state === 'broken') return { wrote: false, kept: 'broken', error: null, mark: null };
  if (validSafeRow(db)) return { wrote: false, kept: 'safe', error: null, mark: null };
  try {
    writePublishGate(db, { state: 'unknown', reason, buildId: row?.build_id ?? null, generationNo: row?.generation_no ?? null, checkedAt: now.toISOString(), now });
    return { wrote: true, kept: null, error: null, mark: null };
  } catch (e) {
    // 門を書けない (SQLITE_BUSY など) = 止めの印を残す (故障が直った後も、通った確かめまで止まったまま。#1564 Codex R6 Low)
    const mark = writeStopMark({ db, dataDir }, { state: 'unknown', reason: `${reason} / gate_write_failed: ${String(e && e.message).slice(0, 120)}`, now });
    return { wrote: false, kept: null, error: String(e && e.message).slice(0, 200), mark };
  }
}
/**
 * daily-sync: 「写しの反映の確かめ」の後に、m_products・上書き表を読む工程を止めるか (#1564 Codex R1 H4・R2 High 2・R5 Medium)。
 *   exit 4 = broken / 門が閉じている = その値 / 確かめが通らなかった (exit 1・落ちた) のに門が「確かめた safe の行」でない
 *   (行が無く全部 load の暗黙の safe・合わなくなった safe) = unknown = この回は止める。確かめた safe の行が今も合う遅れの朝 = 流す
 * @param {{ success: boolean, exitCode?: number|null }} apply  runScript の結果
 * @param {{ open: boolean, state: string, reason: string, source: string }} gate  readPublishGate の結果 (確かめの後に読む)
 */
export function gateAfterVerify({ apply, gate }) {
  if (apply && !apply.success && apply.exitCode === 4) return { broken: true, state: 'broken', reason: 'verify_apply_exit_4' };
  if (!gate || !gate.open) return { broken: true, state: gate ? gate.state : 'unknown', reason: gate ? gate.reason : 'no_gate' };
  if (apply && !apply.success && gate.source !== 'row') return { broken: true, state: 'unknown', reason: `verify_apply_failed_without_safe_row (${gate.reason})` };
  return { broken: false, state: gate.state, reason: gate.reason };
}
/**
 * 門を読む (db = 開いた warehouse.db / dataDir = DATA_DIR から読み取り専用で開く)。読めない = unknown = 止める
 * @returns {{ state: 'safe'|'broken'|'unknown', open: boolean, broken: boolean, reason: string, checked_at: string|null, build_id: string|null, generation_no: number|null, source: string }}
 */
export function readPublishGate({ db = null, dataDir = null } = {}) {
  const g = gateFromDbOrDir({ db, dataDir });
  // 止めの印 (門を書けなかった・warehouse.db を開けなかった回) = 行・暗黙の safe より先に止める (通った確かめが消すまで)
  const mark = readStopMark({ db, dataDir });
  if (mark) return closed(mark.state === 'broken' || g.state === 'broken' ? 'broken' : 'unknown', `stop_mark: ${mark.reason}`, { checked_at: mark.at, source: 'stop_mark' });
  return g;
}
function gateFromDbOrDir({ db = null, dataDir = null } = {}) {
  if (db) { try { return gateOfDb(db); } catch (e) { return closed('unknown', `gate_unreadable: ${String(e && e.message).slice(0, 120)}`); } }
  const file = dataDir ? path.join(dataDir, 'warehouse.db') : null;
  if (!file || !fs.existsSync(file)) return closed('unknown', 'no_warehouse_db');
  let d = null;
  try { d = new Database(file, { readonly: true, fileMustExist: true }); return gateOfDb(d); }
  catch (e) { return closed('unknown', `gate_unreadable: ${String(e && e.message).slice(0, 120)}`); }
  finally { try { if (d) d.close(); } catch { /* */ } }
}

/** 止める工程 (daily-sync の runScript の最初の引数 = PROJECT_DIR からの相対パス) → 読むもの */
export const PUBLISH_GATED_SCRIPTS = Object.freeze({
  'apps/warehouse/record-m-products-history.js': 'm_products を読む (履歴)',
  'apps/company-db/push/sku-cost-observed.mjs': 'm_products_history から原価の期間を作って Company DB へ送る',
  'apps/warehouse/rebuild-f-sales.js': 'm_products・m_set_components を読む (販売集計)',
  'apps/warehouse/rebuild-sales-velocity.js': 'm_products・m_set_components を読む (販売速度)',
  'apps/warehouse/build-product-management-snapshot.js': 'm_products・m_reorder_setting を読む (商品管理リスト)',
  'scripts/amazon-finance/build-daily-fact.js': '原価 (m_products・exception_genka) を読む (Amazon 財務の日次)',
  'apps/warehouse/sync-amazon-finance-daily.js': 'Amazon 財務の日次 (原価入り) を Render へ送る',
  'apps/company-db/push/amazon-finance.mjs': 'Amazon 財務の日次 (原価入り) を Company DB へ送る・突き合わせる',
  'scripts/rakuten-finance/build-rakuten-daily-fact.js': '原価を読む (楽天 財務の日次)',
  'apps/warehouse/run-rakuten-finance-dq.js': '楽天 財務の日次の品質',
  'apps/warehouse/sync-rakuten-finance-daily.js': '楽天 財務の日次を Render へ送る',
  'apps/warehouse/sync-sku-maps.js': 'm_products を読む (コードの確かめ・書き方)',
  'scripts/yahoo-finance/build-yahoo-daily-fact.js': '原価を読む (Yahoo 財務の日次)',
  'apps/warehouse/run-yahoo-finance-dq.js': 'm_products を読む (Yahoo 財務の日次の品質)',
  'apps/warehouse/sync-yahoo-finance-daily.js': 'Yahoo 財務の日次を Render へ送る',
  'scripts/aupay-finance/build-aupay-daily-fact.js': '原価を読む (au PAY 財務の日次)',
  'apps/warehouse/run-aupay-finance-dq.js': 'm_products を読む (au PAY 財務の日次の品質)',
  'apps/warehouse/sync-aupay-finance-daily.js': 'au PAY 財務の日次を Render へ送る',
  'scripts/linegift-finance/build-linegift-daily-fact.js': '原価を読む (LINE ギフト 財務の日次)',
  'apps/warehouse/run-linegift-finance-dq.js': 'm_products を読む (LINE ギフト 財務の日次の品質)',
  'apps/warehouse/sync-linegift-finance-daily.js': 'LINE ギフト 財務の日次を Render へ送る',
  'scripts/qoo10-finance/build-qoo10-daily-fact.js': '原価を読む (Qoo10 財務の日次)',
  'apps/warehouse/run-qoo10-finance-dq.js': 'm_products を読む (Qoo10 財務の日次の品質)',
  'apps/warehouse/sync-qoo10-finance-daily.js': 'Qoo10 財務の日次を Render へ送る',
  'apps/warehouse/sync-f-sales-by-listing.js': 'f_sales (m_products から作る) を Render へ送る',
  'apps/warehouse/rebuild-rakuten-sku-map.js': 'm_products を読む (楽天のコードの対応)',
  'apps/warehouse/sync-to-render.js': 'm_products・m_set_components を Render へ送る',
  'scripts/company-db/lz-daily.mjs': 'm_products の材料からロジザードの商品マスタ (影) を作る',
});

/** 止めない工程 → 理由 */
export const PUBLISH_UNGATED_SCRIPTS = Object.freeze({
  'apps/company-db/publish/fetch.mjs': 'この確かめそのもの',
  'apps/company-db/publish/amazon-map.mjs': 'Company DB の Amazon SKU の対応を m_sku_master・m_sku_components に写すだけ (⑦-2 PR-A。商品マスタ・上書き表は読まない)',
  'apps/warehouse/rebuild-amazon-settlement-mart.js': '決済の明細だけを読む',
  'apps/warehouse/sync-amazon-ads-daily.js': '広告費だけ',
  'apps/warehouse/sync-amazon-price-snapshot.js': 'カート価格だけ',
  'apps/warehouse/rebuild-amazon-account-fees.js': '決済の明細だけを読む',
  'apps/warehouse/sync-amazon-account-fees.js': 'アカウントの手数料だけ',
  'apps/warehouse/import-rakuten-ads-rpp.js': 'モールの広告の数値だけ',
  'apps/warehouse/sync-rakuten-ads-daily.js': 'モールの広告の数値だけ',
  'apps/warehouse/import-rakuten-data.js': 'モールの数値だけ',
  'apps/warehouse/sync-rakuten-data-daily.js': 'モールの数値だけ',
  'apps/warehouse/import-rakuten-review.js': 'レビューだけ',
  'apps/warehouse/sync-rakuten-review-daily.js': 'レビューだけ',
  'apps/warehouse/import-yahoo-review.js': 'レビューだけ',
  'apps/warehouse/plan-rakuten-review-campaigns.js': 'レビューの依頼だけ',
  'apps/warehouse/fetch-rakuten-review-contacts.js': 'レビューの依頼先だけ',
  'apps/warehouse/import-yahoo-data.js': 'モールの数値だけ',
  'apps/warehouse/sync-yahoo-data-daily.js': 'モールの数値だけ',
  'apps/warehouse/import-aupay-data.js': 'モールの数値だけ',
  'apps/warehouse/sync-aupay-data-daily.js': 'モールの数値だけ',
  'apps/warehouse/import-qoo10-data.js': 'モールの数値だけ',
  'apps/warehouse/sync-qoo10-data-daily.js': 'モールの数値だけ',
  'apps/warehouse/monitor-fee-coverage.js': '注文と手数料だけ',
  'apps/warehouse/notify-yahoo-token-expiry.js': '期限の知らせだけ',
  'apps/warehouse/notify-rakuten-license-expiry.js': '期限の知らせだけ',
  'apps/rakuten-unshipped/notify-job.js': '注文の知らせだけ',
  'apps/yahoo-unshipped/notify-job.js': '注文の知らせだけ',
  'apps/aupay-unshipped/notify-job.js': '注文の知らせだけ',
  'apps/qoo10-unshipped/notify-job.js': '注文の知らせだけ',
  'apps/yahoo-inquiry-alert/notify-job.js': '問い合わせの知らせだけ',
  'apps/warehouse/backup-warehouse.js': '写しを残す (壊れた朝の証拠も残す)',
  'apps/company-db/master-compare/run.mjs': '比べて知らせる側 (② と ②b が壊れを朝の要約に出す)',
  'apps/company-db/master-compare/new-entry-gate.mjs': 'Company DB の新商品の許可だけ (照合の証跡と DB の関数を読む)',
  'apps/company-db/watch/run.mjs': 'Company DB だけを見る',
});

/** 止めた朝に fail の ping を代わりに打つ工程 (自分で ping を打つ = 台帳に項目がある) */
export const GATED_OWN_PING = Object.freeze({
  'scripts/company-db/lz-daily.mjs': 'lz-daily-build',
});

/** runScript の最初の引数 → 工程のファイル */
export const scriptFileOf = (scriptCmd) => String(scriptCmd || '').split(' ').filter(Boolean)[0] || '';

/**
 * この工程を止めるか (daily-sync の runScript が最初に呼ぶ)
 * @param {string} scriptCmd  runScript の最初の引数 (パス + 引数)
 * @param {{ broken: boolean }} gate  写しの反映が世代と違う (fetch.mjs --verify-apply の exit 4)
 * @returns {{ skip: false } | { skip: true, file, reason, pingJobId, summary }}
 */
export function publishGateDecision(scriptCmd, gate) {
  if (!gate || !gate.broken) return { skip: false };
  const file = scriptFileOf(scriptCmd);
  if (!Object.hasOwn(PUBLISH_GATED_SCRIPTS, file)) return { skip: false };
  const reason = PUBLISH_GATED_SCRIPTS[file];
  return { skip: true, file, reason, pingJobId: Object.hasOwn(GATED_OWN_PING, file) ? GATED_OWN_PING[file] : null,
    summary: `⚠️ 見送り: Company DB の写しの反映が${gate.state === 'unknown' ? '確かめられていない' : '世代と違う'} (${reason} = 違う値を先へ配らない。門 cdb_publish_gate = ${gate.state ?? 'broken'}・master-publish の証跡の apply を見る)` };
}
