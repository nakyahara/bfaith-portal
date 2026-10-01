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
 *   行が無い = 持ち主が全部 load (今の世代・最新の作り直しのどちらも C の列なし) のときだけ流してよい (今と同じ)。読めない = unknown = 止める
 */
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { readCurrentPublish, publishCols, ownershipHash } from './master-publish.js';
import { latestBuild, publishOfBuild } from './master-material.js';
import { ALL_LOAD } from '../company-db/load/ownership-state.mjs';

export const GATE_STATES = Object.freeze(['safe', 'broken', 'unknown']);
/** 門の行 (表が無い・行が無い = null) */
export function readGateRow(db) {
  try { return db.prepare('SELECT state, reason, build_id, generation_no, checked_at, updated_at FROM cdb_publish_gate WHERE id = 1').get() || null; } catch { return null; }
}
/** 門を書く (入れた後の確かめだけが呼ぶ) */
export function writePublishGate(db, { state, reason = null, buildId = null, generationNo = null, checkedAt, now = new Date() }) {
  if (!GATE_STATES.includes(state)) throw new Error(`門の値が不正: ${state}`);
  db.prepare(`INSERT INTO cdb_publish_gate (id, state, reason, build_id, generation_no, checked_at, updated_at) VALUES (1, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO UPDATE SET state = excluded.state, reason = excluded.reason, build_id = excluded.build_id, generation_no = excluded.generation_no,
      checked_at = excluded.checked_at, updated_at = excluded.updated_at`)
    .run(state, reason == null ? null : String(reason).slice(0, 400), buildId, generationNo, checkedAt, now.toISOString());
  return state;
}
/** 持ち主が C の列を使っているか (今の世代の持ち主・最新の作り直しが使った持ち主のどちらか)。読めない = 使っているとみなす */
function usesCompanyOwner(db) {
  const head = readCurrentPublish(db, { values: false });
  if (head.generation) { try { if (publishCols(JSON.parse(head.generation.ownership)).length) return true; } catch { return true; } }
  let lb = null;
  try { lb = publishOfBuild(latestBuild(db)); } catch { lb = null; }
  return !!(lb && lb.ownership_hash && lb.ownership_hash !== ownershipHash(ALL_LOAD));
}
const closed = (state, reason, extra = {}) => ({ state, open: false, broken: true, reason, checked_at: null, build_id: null, generation_no: null, source: 'none', ...extra });
/** 門の今の値 (開いている = open) */
export function gateOfDb(db) {
  const row = readGateRow(db);
  if (row) {
    const open = row.state === 'safe';
    return { state: row.state, open, broken: !open, reason: row.reason ?? row.state, checked_at: row.checked_at, build_id: row.build_id ?? null, generation_no: row.generation_no ?? null, source: 'row' };
  }
  if (usesCompanyOwner(db)) return closed('unknown', 'no_gate_row_with_company_owner');   // 持ち主が C なのに一度も確かめていない
  return { state: 'safe', open: true, broken: false, reason: 'all_load_no_gate_row', checked_at: null, build_id: null, generation_no: null, source: 'implicit' };
}
/**
 * 門を読む (db = 開いた warehouse.db / dataDir = DATA_DIR から読み取り専用で開く)。読めない = unknown = 止める
 * @returns {{ state: 'safe'|'broken'|'unknown', open: boolean, broken: boolean, reason: string, checked_at: string|null, build_id: string|null, generation_no: number|null, source: string }}
 */
export function readPublishGate({ db = null, dataDir = null } = {}) {
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
