/**
 * retry-failed-jobs.js — daily-sync.js の失敗ジョブを自動再試行
 *
 * 朝7:00 の daily-sync で失敗した f_sales / 楽天sku_map / Render同期 を、
 * 8:30 / 10:00 / 11:30 JST (固定) に自動再実行する (Task Scheduler から3回起動)。
 *
 * 注: daily-sync の最悪完了時刻は 8:15 (Yahoo 60分タイムアウト想定)。8:30 が最早安全タイミング。
 *
 * 動作:
 *   1. data/daily-sync-retry-state.json があれば読む。なければ no-op で終了
 *   2. state.run_date が今日(JST)でなければ古い state とみなしてクリーンアップ
 *   3. remaining_jobs を依存順 (f_sales → 楽天sku_map → Render同期) で再実行
 *      - Render同期: 今回 f_sales / 楽天sku_map のどちらかを再実行して失敗した場合スキップ
 *   4. 全部成功 → state削除 + ✅復旧通知
 *      最大試行 (3回) 到達 → state削除 + 🔴最終失敗通知
 *      まだ残る → state更新 (次回 Task Scheduler が拾う)
 *
 * Task Scheduler 設定 (3つ):
 *   WarehouseDailySyncRetry1: 08:30 JST 毎日
 *   WarehouseDailySyncRetry2: 10:00 JST 毎日
 *   WarehouseDailySyncRetry3: 11:30 JST 毎日
 *   いずれも `node apps/warehouse/retry-failed-jobs.js` を実行。
 *   state ファイルが無ければ即時 no-op で終了するので、空振り起動は無害。
 *
 * 🚨 回は 1 つずつ (2026-09-29・retry-lock.js): 前の回がまだ動いている・朝の daily-sync がまだ動いている間は見送る (retry-state には触らない)。
 *   1 回の中の工程の上限の合計は 90 分の間隔を超えうる (Amazon決済と財務 90 分 …) = 並ぶと同じ工程が重なり、
 *   片方が消した state をもう片方が書き戻す。見送った回は、retry-state があれば ⏸️ を通知する (最後の 11:30 を見送ると次の回が無い = 人が見る)
 */
import 'dotenv/config';
import fs from 'fs';
import { execFileSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';
import { isWarnSummary } from './amazon-fees-outcome.js';
import { acquireRetryLock, releaseRetryLock } from './retry-lock.js';
import { financeCoordinatorEnabled, legacyGateCheck, FINANCE_COORDINATOR_ENV } from './finance-coordinator-switch.js';
import { publishGateDecision, readPublishGate } from './publish-gate.js';
import { gateRetryJobs } from '../company-db/master-compare/new-entry-gate.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');
const RETRY_STATE_FILE = path.join(PROJECT_DIR, 'data', 'daily-sync-retry-state.json');
const RETRY_LOCK_FILE = path.join(PROJECT_DIR, 'data', 'retry-failed-jobs.lock.json');
const DAILY_SYNC_LOCK_FILE = path.join(PROJECT_DIR, 'data', 'daily-sync.lock.json');   // daily-sync.js の LOCK_FILE と同じ

const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

const MAX_RETRY_COUNT = 3;

// ジョブ定義 (daily-sync.js と一致させる)
//   f_sales のみ retry 時は 30分 (初回 10分でタイムアウトした場合の余裕)
//   args 省略時は '7' (rebuild系の日数引数の既存挙動を維持)
export const JOB_DEFINITIONS = {
  // Company DB の Amazon SKU の対応 → m_sku_master・m_sku_components (⑦-2 PR-A)。毎回 Company DB の今の対応にまるごと合わせる = 再実行安全
  //   (同じ中身なら変わった行 0)。書くのは回の鍵を持つ親 (daily-sync・この retry・手の口 --amazon-map-chain) の子だけ・写しの鍵 (job_locks) もスクリプトが取る。持ち主が load = 何もしない (exit 0)。
  //   f_sales の上流 (UPSTREAM_OF)。写しが retry に載った日 = 写しの鎖 (AMAZON_MAP_CHAIN): 直ったら f_sales → sales_velocity → pml_snapshot → Render同期 を一段ずつ流す
  'CompanyDB写し(Amazon SKU)': { script: 'apps/company-db/publish/amazon-map.mjs', args: ['--daily'], timeoutMs: 300000 },
  'f_sales':        { script: 'apps/warehouse/rebuild-f-sales.js',                timeoutMs: 1800000 },
  'sales_velocity': { script: 'apps/warehouse/rebuild-sales-velocity.js',         timeoutMs: 900000  },
  'pml_snapshot':   { script: 'apps/warehouse/build-product-management-snapshot.js', timeoutMs: 600000 },
  '楽天sku_map':    { script: 'apps/warehouse/rebuild-rakuten-sku-map.js',        timeoutMs: 600000  },
  'Render同期':     { script: 'apps/warehouse/sync-to-render.js',                 timeoutMs: 600000  },
  // Company DB へ NE 伝票を送る (D5a)。冪等 (台帳の指紋で差分だけ・Render 側は世代で判定) なので再実行安全。Render が落ちていた朝の自動復旧用
  'CompanyDB出荷':  { script: 'apps/company-db/push/ne-shipments.mjs',            args: ['--incremental'], timeoutMs: 1800000 },
  // Company DB へ NE の在庫の日次を送る (D2b-1)。送り済みの日は Render に聞いて飛ばす・先に確定した日は書き換えない = 再実行安全
  'CompanyDB在庫(NE)': { script: 'apps/company-db/push/stock-daily.mjs',          args: ['--source', 'ne', '--days', '14'], timeoutMs: 600000 },
  // Company DB へ FBA の在庫の日次を送る (D2b-2)。NE と同じ送り手 = 再実行安全
  'CompanyDB在庫(FBA)': { script: 'apps/company-db/push/stock-daily.mjs',         args: ['--source', 'fba_jp', '--days', '14'], timeoutMs: 600000 },
  'CompanyDB見張り':    { script: 'apps/company-db/watch/run.mjs',                 args: [], timeoutMs: 300000 },
  // マスタ照合 ①ロードの検証 (Company DB構想 10 §6.1.1 B)。読むだけ・証跡と全件 JSON は実行ごとに新しく書く = 再実行安全。
  //   照合そのものの失敗 (DB に届かない・証跡を書けない) だけ ❌ で retry に載る。差がある・判定できないは ⚠️ (exit 0)
  'マスタ照合':        { script: 'apps/company-db/master-compare/run.mjs',          args: ['--daily'], timeoutMs: 300000 },
  // 新商品の許可 (照合 ② の次の 1 段・PR-7)。その朝の照合の回 (証跡 master-compare) で ops.grant_new_entry_lease を呼ぶ = 再実行安全 (DB が一番新しい結果の行の回と同じかを確かめる)。
  //   拒まれた・接続できない = ❌ (人が直せば同じ日の再試行で開く)。照合が retry で直ったら走らせ直す (RERUN_AFTER)・照合をこの回で再試行して失敗したら見送る (UPSTREAM_OF)
  '新商品の許可':      { script: 'apps/company-db/master-compare/new-entry-gate.mjs', args: ['--daily'], timeoutMs: 120000 },
  // ロジザードの毎日の商品マスタ (影。③c-1a)。読むだけ・出すものは実行ごとに新しい実行 ID で作る = 再実行安全。材料が欠ける (exit 3) も失敗 = 次の回にまた試す
  'ロジザード毎日の商品マスタ(影)': { script: 'scripts/company-db/lz-daily.mjs', args: ['--daily'], timeoutMs: 300000 },   // 見張り自身の失敗 (❌) だけが retry に載る (業務の異常は ⚠️ で exit 0)
  'CompanyDB在庫(FBA US)': { script: 'apps/company-db/push/stock-daily.mjs',      args: ['--source', 'fba_us', '--days', '14'], timeoutMs: 600000 },
  // Company DB へ楽天の注文を送る (D5b-1)。同じく台帳の指紋 + Render の世代で冪等。送った後に伝票との結び直しも回る
  'CompanyDB注文(楽天)': { script: 'apps/company-db/push/mall-orders.mjs',        args: ['--mall', 'rakuten', '--incremental'], timeoutMs: 1800000 },
  // Company DB へ Amazon の注文を送る (D5b-2)。同上。初回のバックフィル前は送らない (--require-backfilled)
  'CompanyDB注文(Amazon)': { script: 'apps/company-db/push/mall-orders.mjs',      args: ['--mall', 'amazon', '--incremental', '--require-backfilled'], timeoutMs: 1800000 },
  // Company DB へ au PAY / LINE ギフトの注文を送る (D5b-3)。同上
  'CompanyDB注文(auPAY)': { script: 'apps/company-db/push/mall-orders.mjs',       args: ['--mall', 'aupay', '--incremental', '--require-backfilled'], timeoutMs: 1800000 },
  'CompanyDB注文(LINEギフト)': { script: 'apps/company-db/push/mall-orders.mjs',  args: ['--mall', 'linegift', '--incremental', '--require-backfilled'], timeoutMs: 1800000 },
  // Company DB へ Qoo10 の注文を送る (D5b-4)。同上
  'CompanyDB注文(Qoo10)': { script: 'apps/company-db/push/mall-orders.mjs',       args: ['--mall', 'qoo10', '--incremental', '--require-backfilled'], timeoutMs: 1800000 },
  // Company DB へ Yahoo の注文を送る (D5b-5)。同上
  'CompanyDB注文(Yahoo)': { script: 'apps/company-db/push/mall-orders.mjs',       args: ['--mall', 'yahoo', '--incremental', '--require-backfilled'], timeoutMs: 1800000 },
  // Amazon決済と財務 (2026-10-01・D7b-1b-3): 決済の取込 + Company DB の Amazon 財務 + 決済のそろい (coverage) を 1 回として回す coordinator。
  //   retry の単位も coverage の回の全体 (送り手だけの retry は、その世代の一覧が無いので complete にできない。設計 13 §3.1 R18 M4)。
  //   前の 'Amazon Settlement' (取込) と 'CompanyDB財務(Amazon)' (送り手) の 2 つをまとめた。冪等 (取込は INSERT OR IGNORE・送り手は台帳の指紋・新しい世代で回し直す)
  //   Settlement の下流 (アカウントフィー build/sync) は翌朝 daily-sync が再集計する冪等設計のため、ここでは coordinator だけ再試行すれば十分。
  // Amazon Ads: 一過性の SP-API fetch failed で落ちた際の自動復旧 (2026-07-13 に Settlement が「JOB_DEFINITIONS 未登録のため未実行」→手動対応になった実績)。
  // ⚠️'Amazon finance build' は --month 引数が動的 (当月) なため未登録のまま (unhandled 通知で顕在化)
  //   🚨 env CDB_FINANCE_COORDINATOR=1 のときだけ (daily-sync と同じスイッチ・finance-coordinator-switch.js)。無いとき = 下の今までの 2 つ (Amazon Settlement → CompanyDB財務(Amazon))
  'Amazon決済と財務':     { script: 'apps/warehouse/amazon-finance-coverage-run.js', args: ['--source', 'v2'], timeoutMs: 5400000 },
  // スイッチが無いとき (今までどおり・master と同じ): 決済の取込 (fetch-amazon-settlements.js は今までどおり書く・coverage の lease を取る)
  'Amazon Settlement':     { script: 'apps/warehouse/fetch-amazon-settlements.js', args: ['--days', '14'], timeoutMs: 3600000 },
  'Amazon Ads (campaign)': { script: 'apps/warehouse/fetch-amazon-ads-campaign.js', args: [], timeoutMs: 1800000 },
  'Amazon Ads (SKU)':      { script: 'apps/warehouse/fetch-amazon-ads.js',          args: [], timeoutMs: 1800000 },
  // Company DB へ Amazon SP の広告費の日次を送る (Company DB構想 11 の ②)。Render と同じ日は送らない・古い世代は受け口が拒む = 再実行安全。上流 = Amazon Ads (SKU) (UPSTREAM_OF)
  'CompanyDB広告費(Amazon)': { script: 'apps/company-db/push/ad-spend.mjs',     args: ['--mall', 'amazon', '--days', '35'], timeoutMs: 600000 },
  // スイッチが無いとき (今までどおり・master と同じ): Company DB へ Amazon の財務を送る (F2b-3)。台帳の指紋 + 読み直す鍵 + Render の世代で冪等。retry は曜日に依らず --full。
  //   スイッチがあるとき = 'Amazon決済と財務' (coordinator) にまとめる。突き合わせ (CompanyDB財務突合(Amazon)) はどちらでも retry に載せない (差の続いた回数を数えている)
  'CompanyDB財務(Amazon)': { script: 'apps/company-db/push/amazon-finance.mjs', args: ['--full', '--require-backfilled'], timeoutMs: 1800000 },
  // m_products の変化を履歴に記録 (差分 = 今の m_products と最後の履歴を比べて違う分だけ足す = 再実行安全。--baseline は付けない)。観測の原価の上流 (Codex #1549 R3 M2)
  'm_products_history':  { script: 'apps/warehouse/record-m-products-history.js', args: [], timeoutMs: 600000 },
  // Company DB へ観測の原価を送る (D7b-2)。毎回 m_products_history から全部作り直し、Render と同じ中身なら送らない・応答が失われた回は同じ世代で再送 (same) = 再実行安全。上流 = m_products_history (UPSTREAM_OF)
  'CompanyDB観測原価':  { script: 'apps/company-db/push/sku-cost-observed.mjs',   args: ['--send'], timeoutMs: 600000 },
  // 'Amazon手数料' (2026-07-16 障害対応、incident_amazon_fee_coverage_no_retry):
  //   daily-sync の RETRYABLE_JOBS に入れるだけでは「未対応」🔴 になるため、ここにも定義必須。
  //   retry は 08:30/10:00/11:30 の空き枠で走るので daily(07:00, 10分) より timeout を 20分に延ばして余裕を取る。
  //   INSERT OR REPLACE + TTL/差分フィルタで再実行安全 (成功済み SKU は skip されるので負荷は自然縮小)。
  'Amazon手数料':          { script: 'apps/warehouse/fetch-amazon-fees.js',        args: ['--recent', '30'], timeoutMs: 1200000 },
  // ABA検索ワード: 冪等 (aba_weeks 台帳で取込済み週は即skip、未公開週は正常skip。
  //   INSERT OR REPLACE なので中断後の再実行も安全)。SP-API throttle 等の一過性失敗を拾う
  'ABA検索ワード':         { script: 'apps/aba-keywords/fetch-aba-search-terms.js',  args: [], timeoutMs: 3600000 },
  // DBバックアップ: 冪等 (当日分完成済み+元DB変化なしなら再利用し offsite 以降だけやり直す。
  // 先行ジョブの retry で DB が更新されていれば src_sig 不一致で自動的に作り直す)。月初最悪 ~5.5h
  'DBバックアップ':        { script: 'apps/warehouse/backup-warehouse.js',          args: [], timeoutMs: 21600000 },
  // 楽天未発送アラート: RMS API を読んで GChat へ通知するだけ (DBに書かない) ので再実行安全。
  // 朝の便が RMS の一時障害で落ちても、当日中に出荷漏れの通知が届くようにする
  '楽天未発送アラート':    { script: 'apps/rakuten-unshipped/notify-job.js',        args: ['--once'], timeoutMs: 600000 },
  // Yahoo未発送アラート: warehouse.db の候補を Yahoo受注API で最新確認して通知するだけ (DBに書かない)
  'Yahoo未発送アラート':   { script: 'apps/yahoo-unshipped/notify-job.js',          args: ['--once'], timeoutMs: 600000 },
  // auPAY未発送アラート: warehouse.db の候補を auPAY受注API で最新確認して通知するだけ (DBに書かない)
  'auPAY未発送アラート':   { script: 'apps/aupay-unshipped/notify-job.js',          args: ['--once'], timeoutMs: 900000 },
  // Qoo10受注同期: 直近90日を毎回取り直す冪等な UPSERT なので再実行安全。
  // ⚠️Qoo10未発送アラートは「今日の同期で確認できた注文」だけを見るため、
  //   同期が復旧しないと判定できない。必ずこちらも retry 対象にすること (Codexレビュー High)
  'Qoo10':                 { script: 'apps/warehouse/qoo10-orders.js',              args: ['90'], timeoutMs: 1800000 },
  // Qoo10未発送アラート: DBだけで判定して通知する (API再確認なし)。上の Qoo10 同期の後に走らせる
  'Qoo10未発送アラート':   { script: 'apps/qoo10-unshipped/notify-job.js',          args: ['--once'], timeoutMs: 300000 },
  // Yahoo問い合わせ対応漏れ: 問い合わせ管理API を読んで通知するだけ (DBに書かない)。
  // 通知は at-least-once = 送信後・終了判定前に落ちた場合 retry で同じ通知が重複しうる
  // (見逃しより重複を取る割り切り。未発送アラート群と同じ)。
  // 該当ゼロ=無通知 の仕様なので、朝の便が落ちたら当日中の retry で必ず結果を出す
  'Yahoo問い合わせ対応漏れ': { script: 'apps/yahoo-inquiry-alert/notify-job.js',     args: ['--once'], timeoutMs: 300000 },
};

// 実行順序 (依存関係順)。sales_velocity → pml_snapshot は f_sales と同じ raw + マスタ依存なので直後。
// Amazon系は他ジョブと独立なので先頭 (長時間ジョブを先に開始)
// DBバックアップは最後 (f_sales 等が同時に失敗していた場合、復旧後の最新状態を保存するため)
// 楽天未発送アラートは先頭 (出荷漏れの通知は早いほど価値があり、他ジョブに依存しない)
export const RETRY_ORDER = ['楽天未発送アラート', 'Yahoo未発送アラート', 'auPAY未発送アラート', 'Yahoo問い合わせ対応漏れ', 'Qoo10', 'Qoo10未発送アラート', 'CompanyDB出荷', 'CompanyDB在庫(NE)', 'CompanyDB在庫(FBA)', 'CompanyDB在庫(FBA US)', 'CompanyDB注文(楽天)', 'CompanyDB注文(Amazon)', 'CompanyDB注文(auPAY)', 'CompanyDB注文(LINEギフト)', 'CompanyDB注文(Qoo10)', 'CompanyDB注文(Yahoo)', 'm_products_history', 'CompanyDB観測原価', 'Amazon決済と財務', 'Amazon Settlement', 'CompanyDB財務(Amazon)', 'Amazon Ads (campaign)', 'Amazon Ads (SKU)', 'CompanyDB広告費(Amazon)', 'Amazon手数料', 'ABA検索ワード', 'CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot', '楽天sku_map', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'DBバックアップ', 'CompanyDB見張り'];

/**
 * 上流 (取込) → 下流 (その取込の結果を使うジョブ)。下流は、**同じ回で上流を再試行して失敗したら走らせない** (古い・途中の raw を送らない)。
 *   daily-sync は上流が失敗した朝、下流を「⏭️ skipped」の失敗として retry-state に載せる → 上流の再試行が成功した回に下流も走る。
 *   上流が remaining_jobs に無い (= 朝は成功していて下流だけ失敗した・前の回で復旧済み) なら、下流はそのまま走らせる。
 *   🚨 ここに載せてよいのは、上流そのものが retry の対象 (JOB_DEFINITIONS にある) の組だけ (Qoo10・Amazon Ads (SKU)・Amazon Settlement (スイッチが無いとき)・m_products_history)。上流が retry されない取込 (楽天・Amazon の注文・au PAY・LINE ギフト・NE) は、
 *      朝に見送った送信を retry に載せない (= 翌朝の daily-sync が台帳の指紋で追いつく)。載せると、取込が失敗したままの raw を送ってしまう
 *   RETRY_ORDER では上流を下流より前に置く (scripts/test-retry-upstream.mjs が確かめる)
 */
export const UPSTREAM_OF = {
  'CompanyDB注文(Qoo10)': 'Qoo10',
  'CompanyDB広告費(Amazon)': 'Amazon Ads (SKU)',
  'CompanyDB財務(Amazon)': 'Amazon Settlement',   // 決済の取込 → Company DB の Amazon 財務 (F2b-3。#1536 Codex R1)。スイッチが無いときだけ走る (あるときは coordinator の 1 工程 = 組は無い)
  'CompanyDB観測原価': 'm_products_history',       // 原価の履歴の記録 → Company DB の観測の原価 (D7b-2。#1549 Codex R3 M2)
  '新商品の許可': 'マスタ照合',                     // 照合 ② → 新商品の許可 (PR-7)。照合をこの回で再試行して失敗 = 許可を出さない (入口は照合の始めで閉じたまま)
  // Amazon SKU の対応の写し → f_sales (⑦-2 PR-A・Codex 計画 R2 High 2)。写しをこの回で再試行して失敗 = f_sales を作り直さない
  //   (朝の f_sales は前の対応のまま = 古い対応と新しい対応の混ざった回を Render へ送らない。写しが直った回に f_sales から作り直す = RERUN_AFTER)
  //   今の本番 (持ち主 load) は写しがいつも exit 0 = retry に載らない = この組は効かない (今の f_sales の retry は変わらない)
  //   🆕 #1652 Codex R1 Medium / R2 High: 写しが落ちた回は、写しの前後の写しの記録 (warehouse.db の sync_meta cdb_amazon_map_publish) を比べて決める (runRetryRound の mapOutcome):
  //   前のまま (確かめられた) = 古い表は前のまま = f_sales がもとの remaining にあれば見送らない (config が company・active が load の朝に watcher が読めず、
  //   f_sales も落ちた日に f_sales・Render同期 が戻らない穴を塞ぐ) / 変わった (commit の後の終了処理で落ちた) = 写せた扱いで鎖を f_sales から /
  //   鍵待ち (73)・分からない = f_sales〜Render同期 をこの回は流さない
  'f_sales': 'CompanyDB写し(Amazon SKU)',
};
/**
 * Amazon SKU の対応の写しの鎖 (⑦-2 PR-A・#1649 Codex R1 Medium 2 / R2 Medium)。**写しが retry に載った日 (state.amazon_map_chain・remaining に写し) だけ**効く
 *   = 今の本番 (持ち主 load = 写しは retry に載らない) の retry の動きは今までどおり (sales_velocity・pml_snapshot が落ちても Render同期 は流れる)。
 *   鎖の中: 1 つが成功したら次だけを足す (走らせ直し) / 前がこの回で落ちた (見送りも) = 流さない / 途中で落ちた = その先を「⏸️ 見送り」の失敗として残し、
 *   次の回も鎖のまま落ちたところから一段ずつ流す (amazon_map_chain を retry-state に残す)。
 *   daily-sync は写しが鍵待ち (exit 73) の朝、f_sales〜Render同期 を retry-state に載せない (blocked) = まず写しだけが残り、直った回に鎖で流れる
 */
export const AMAZON_MAP_CHAIN = Object.freeze(['CompanyDB写し(Amazon SKU)', 'f_sales', 'sales_velocity', 'pml_snapshot', 'Render同期']);
const chainPrev = (j) => { const i = AMAZON_MAP_CHAIN.indexOf(j); return i > 0 ? AMAZON_MAP_CHAIN[i - 1] : null; };
const chainNext = (j) => { const i = AMAZON_MAP_CHAIN.indexOf(j); return i >= 0 && i < AMAZON_MAP_CHAIN.length - 1 ? AMAZON_MAP_CHAIN[i + 1] : null; };
/**
 * 写しが「持ち主 load = 何も書いていない」(amazon-map.mjs の not_applied = ⏭️ の行) で終わったか (⑦-2 PR-C)。
 *   config (configured) が company・DB の active が load の間 (配ってから widen まで) は、持ち主を読めない朝の写しが config を手がかりに ❌ = retry に載り、
 *   写しの鎖が効く。retry で持ち主を読めて ⏭️ で直った回は古い表が前のまま = その後は鎖の回でない (今の本番の retry と同じ: f_sales 以降を走らせ直さない・
 *   鎖の見送りもしない・retry-state の鎖の印も消える。朝の f_sales・Render同期 は同じ古い表で作った)。
 *   鎖が要るのは持ち主が company の朝 (鍵待ち exit 73 で f_sales 以降を見送った朝) だけ。その後に load に戻る道は無い (widen は戻せない) = これに当たらない。
 *   行の形は試験 (test-retry-rerun.mjs [4f]) が amazon-map.mjs の本物の行で確かめる
 */
export const MAP_NOT_APPLIED_RE = /^⏭️ CompanyDB写し\(Amazon SKU\): 持ち主が load/;
/** 写しの記録の鍵 (amazon-map.mjs の META_KEY と同じ。試験が確かめる)。写しは古い表と同じ SQLite の 1 取引で書く = 記録が変わった ⇔ 古い表が commit された */
export const MAP_META_KEY = 'cdb_amazon_map_publish';
/**
 * 写しの記録を読む (warehouse.db を読むだけで開く)。{ value: 文字 | null (行が無い) } / 読めない = { unreadable: true }。
 *   毎回の写しの commit は published_at の新しい記録を書く = 変わらない中身の写しでも記録は変わる
 */
export function readMapPublishMeta({ dataDir = process.env.DATA_DIR || path.join(PROJECT_DIR, 'data') } = {}) {
  try {
    const Database = createRequire(import.meta.url)('better-sqlite3');
    const db = new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true });
    try {
      const row = db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(MAP_META_KEY);
      return { value: row === undefined ? null : String(row.value) };
    } finally { db.close(); }
  } catch (e) { return { unreadable: true, error: String(e && e.message).slice(0, 120) }; }
}
/**
 * 写しが落ちた回の結果 (#1652 Codex R2 High): 写しの前後の記録を比べる。
 *   'unchanged' = 前のままと確かめられた (古い表は前のまま) / 'committed' = 記録が変わった (commit の後の終了処理 = PG の切断の待ちなどで timeout・異常終了) /
 *   'unknown' = どちらかが読めない (推測しない)
 */
export function mapOutcome(before, after) {
  if (!before || before.unreadable || !after || after.unreadable) return 'unknown';
  return before.value === after.value ? 'unchanged' : 'committed';
}
export const mapWroteNothing = (r) => !!r && r.success === true && MAP_NOT_APPLIED_RE.test(String(r.summary || '').trim());
/** この回が写しの鎖の回か (前の回が鎖の途中で終わった = amazon_map_chain / 写しが remaining にある) */
export function amazonChainActive(state) {
  return !!state && (state.amazon_map_chain === true || (Array.isArray(state.remaining_jobs) && state.remaining_jobs.includes(AMAZON_MAP_CHAIN[0])));
}
/**
 * 走らせ直しの依存 (Company DB構想 10 §6.1.1 B4。Codex ③a-2 R1 H5・B-R0 #3): 上流が**この回の retry で成功**したら、朝に成功していた下流も走らせ直す。
 *   Render同期 が直った = 照合の材料・到達の証跡が新しくなった → マスタ照合 → 見張り (照合が blocked で exit 0 でも、新しい結果なので見張りは走らせ直す)。
 *   マスタ照合 が直った = ロジザードの毎日の商品マスタ (影) も新しい照合の回で作り直す (③c-1a)。
 *   マスタ照合 が直った = 新商品の許可もその照合の回で出し直す (PR-7。照合の始めで入口を閉じるので、走らせ直さないとその日は閉じたまま)。
 *   上流が直っても判定できるとは限らない (夜間ロードの材料が mismatch・規則の指紋違いなどは blocked のまま)。
 *   足した下流の失敗も結果に入る = 次の回の remaining_jobs に残る。下流は RETRY_ORDER で上流より後 (試験 test-retry-rerun.mjs が確かめる)
 */
export const RERUN_AFTER = {
  // 🚨 Amazon SKU の写し → f_sales → … の走らせ直しはここに載せない (写しの鎖の回だけ = AMAZON_MAP_CHAIN。今の本番の retry を変えない)
  'Render同期': ['マスタ照合'],
  'マスタ照合': ['新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り'],
};
/** RERUN_AFTER の決まり (定義がある・RETRY_ORDER にある・下流は上流より後 = 循環しない)。違えば理由の配列 */
export function rerunAfterProblems(rerun = RERUN_AFTER, order = RETRY_ORDER, defs = JOB_DEFINITIONS) {
  const out = [];
  for (const [up, downs] of Object.entries(rerun)) {
    for (const j of [up, ...downs]) {
      if (!Object.hasOwn(defs, j)) out.push(`${j}: JOB_DEFINITIONS に無い`);
      if (!order.includes(j)) out.push(`${j}: RETRY_ORDER に無い`);
    }
    for (const d of downs) if (order.indexOf(d) <= order.indexOf(up)) out.push(`${d} は ${up} より後でなければならない (RETRY_ORDER)`);
  }
  return out;
}

/** この回で上流を再試行して失敗していれば、見送りの理由 (文字列)。走らせてよければ null */
export function upstreamBlock(jobName, results) {
  if (!Object.hasOwn(UPSTREAM_OF, jobName)) return null;
  const up = UPSTREAM_OF[jobName];
  const attempt = results.find((r) => r.name === up);
  return attempt && !attempt.success ? `${up} 再失敗` : null;
}

/**
 * 成功したが要約が「警告つき」(⚠️ で始まる) のジョブの行。復旧の通知はジョブ名しか載せないので、警告つきで復旧したジョブの中身 (どの SKU が取れていないか等) が消える
 * → 成功・部分復旧・最終失敗のどの通知にも足す (Codex #1371 R1 #2)
 */
export function warnLines(results) {
  return (results || []).filter((r) => r && r.success && isWarnSummary(r.summary)).map((r) => `${r.name}: ${String(r.summary).trim()}`);
}

async function notify(text) {
  if (!GCHAT_WEBHOOK) {
    console.warn('[Retry] [NOTIFY:status=skipped] GCHAT_WEBHOOK未設定のため通知スキップ');
    return;
  }
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    if (!res.ok) {
      console.error(`[Retry] [NOTIFY:status=failed] GChat HTTP ${res.status}`);
      return;
    }
    console.log('[Retry] [NOTIFY:status=sent]');
  } catch (e) {
    console.error('[Retry] [NOTIFY:status=failed] 通知エラー:', e.message);
  }
}

// execFileSync(shell:false): timeout時のWAL lock残留と引数のshell解釈を避ける
// (feedback_execfile_vs_execsync — daily-sync.js と同方針)
export function runScript(scriptPath, label, timeoutMs, args = ['7']) {
  const filePath = path.join(PROJECT_DIR, scriptPath);
  console.log(`\n=== ${label} ===`);
  try {
    const output = execFileSync(process.execPath, [filePath, ...args], {
      cwd: PROJECT_DIR,
      timeout: timeoutMs,
      encoding: 'utf-8',
      // daily-sync.js と同じ 64MB (デフォルト 1MB は Settlement 系の stdout で超え、
      // 本体成功でも ENOBUFS で「失敗」誤記録になる — 構造監査 H-2)
      maxBuffer: 64 * 1024 * 1024,
      env: {
        ...process.env,
        PATH: process.env.PATH,
        // バッチ用の長め busy_timeout (db.js の initDB が参照)
        WAREHOUSE_DB_BUSY_TIMEOUT_MS: process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS || '60000',
      },
    });
    console.log(output);
    const lines = output.trim().split('\n');
    return { success: true, summary: lines[lines.length - 1] || '' };
  } catch (e) {
    // exit 2 = 「通知は送れたが結果が不完全」(未発送アラート系の規約。daily-sync 側では blocked)。
    // 通知そのものは届いているので、再試行を続けても同じ内容が重複するだけ → ここで打ち切る
    if (e.status === 2) {
      const lastLine = String(e.stdout ?? '').trim().split('\n').slice(-1)[0] || '';
      console.log(`[${label}] 結果は不完全だが通知は送信済み (exit 2) — retry を打ち切ります`);
      return { success: true, summary: `⚠不完全だが通知済み | ${lastLine}`.slice(0, 200) };
    }
    console.error(`[${label}] エラー:`, e.message);
    // 失敗の理由は子の最後の行にあることが多い (❌ …) = 残す。子の行 120 字・失敗の内容 (timeout・起動の失敗など) 77 字を別々に残す (Codex #1540 R1 Low・R2 Low)
    const summary = failSummary(e);
    console.error(`[${label}] 失敗の要約: ${summary}`);   // 試行ごとのログにも残す
    // exitCode = 子の exit (写しの鍵待ち 73 を見分ける。#1652 Codex R1 Medium)。timeout・起動の失敗は null
    return { success: false, summary, exitCode: Number.isInteger(e?.status) ? e.status : null };
  }
}

/** 失敗の要約 = 子の最後の行 (120 字まで) + 失敗の内容 (77 字まで)。どちらかが長くても、もう片方は消えない */
export function failSummary(e) {
  const tail = String((e && e.stdout) ?? '').trim().split('\n').slice(-1)[0].trim().slice(0, 120);
  const msg = String((e && e.message) || 'error').replace(/\s+/g, ' ').slice(0, 77);
  return (tail ? `${tail} | ${msg}` : msg).slice(0, 200);
}

/** Date を JST (UTC+9) の YYYY-MM-DD に変換 */
function toJstDate(d) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

/**
 * state ファイル読み込み。
 * 戻り値:
 *   { found: false }                              ファイル無し (no-op で終了)
 *   { found: true, state }                        正常読み込み
 *   { found: true, state: null, parseError }      破損 (呼び出し側で deleteState を試みる)
 */
/**
 * 工程の名前の読み替え (#1567 R1 L4・スイッチ)。朝の daily-sync と retry の間にスイッチ (env CDB_FINANCE_COORDINATOR) が変わっても、今のスイッチの工程で走らせる
 *   スイッチがある = 'Amazon Settlement' / 'CompanyDB財務(Amazon)' → coordinator 'Amazon決済と財務' の 1 工程
 *   スイッチが無い = 'Amazon決済と財務' → 今までの 2 工程 'Amazon Settlement'・'CompanyDB財務(Amazon)' (順もこの順 = 上流が先)。
 *     🚨 ただし coordinator に切り替え済み (switched = coverage の世代がある) か分からない (null) なら読み替えない = 一方向 (#1567 Codex R6 High)。
 *     その 'Amazon決済と財務' はスイッチが無いので走らせず ❌ (runJobs) = .env を直す
 */
export const RENAMED_JOBS = Object.freeze({ 'Amazon Settlement': 'Amazon決済と財務', 'CompanyDB財務(Amazon)': 'Amazon決済と財務' });
export const LEGACY_JOBS_OF = Object.freeze({ 'Amazon決済と財務': Object.freeze(['Amazon Settlement', 'CompanyDB財務(Amazon)']) });
/** remaining_jobs の名前を今のスイッチの工程に (重複は 1 つに・順は最初に出た位置) */
export function renameRetryJobs(jobs, { coordinator = financeCoordinatorEnabled(), switched = null } = {}) {
  if (!Array.isArray(jobs)) return jobs;
  const out = [];
  const add = (n) => { if (!out.includes(n)) out.push(n); };
  for (const j of jobs) {
    if (coordinator && Object.hasOwn(RENAMED_JOBS, j)) add(RENAMED_JOBS[j]);
    else if (!coordinator && switched === false && Object.hasOwn(LEGACY_JOBS_OF, j)) LEGACY_JOBS_OF[j].forEach(add);
    else add(j);
  }
  return out;
}

function loadState() {
  if (!fs.existsSync(RETRY_STATE_FILE)) return { found: false };
  try {
    const json = fs.readFileSync(RETRY_STATE_FILE, 'utf-8');
    const state = JSON.parse(json);
    return { found: true, state };
  } catch (e) {
    console.error('[Retry] state file 読み込み失敗:', e.message);
    return { found: true, state: null, parseError: e.message };
  }
}

/**
 * state ファイル書き込み。成否を返す。
 * 失敗時は呼び出し側で「state 不整合の恐れあり」として通知すべき。
 */
function saveState(state) {
  try {
    fs.writeFileSync(RETRY_STATE_FILE, JSON.stringify(state, null, 2));
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

/**
 * state ファイル削除。成否を返す。
 * - ファイル無し: ok (成功扱い)
 * - 削除成功: ok
 * - 削除失敗: ok=false + error (呼び出し側で通知)
 *   stale state が残ると後続 retry が誤実行するため、失敗は明示する。
 */
function deleteState() {
  try {
    fs.unlinkSync(RETRY_STATE_FILE);
    return { ok: true };
  } catch (e) {
    if (e.code === 'ENOENT') return { ok: true };
    return { ok: false, error: e.message };
  }
}

/**
 * 1 回ぶんの再試行 (実行ループ)。remaining_jobs のうち RETRY_ORDER にあるものを順に走らせ、結果 [{name, success, summary}] を返す。
 * main() から切り出しただけで動きは同じ (試験が run を差し替えて、上流の規則が実際のループで効いていることを確かめられるように。Codex #1369 R1 #2)
 */
export function runRetryRound(remainingJobs0, { run = runScript, log = console.log, rerunAfter = RERUN_AFTER, publishGate = { broken: false }, amazonChain = null,
  readMapMeta = () => readMapPublishMeta() } = {}) {
  // 「新商品の許可」は同じ照合の回では開かない (拒まれた後の revoke で停止の床が進む) = 必ず「マスタ照合」からやり直す (#1645 Codex R1 Medium)
  const remainingJobs = gateRetryJobs(remainingJobs0);
  const results = []; // {name, success, summary}
  const rerun = new Set();   // この回で上流が成功したので走らせ直す下流 (RERUN_AFTER)
  // 写しの鎖の回か (呼び手が retry-state から決める。渡されない = remaining に写しがあるとき)
  //   🆕 PR-C: この回の写しが ⏭️ (持ち主 load = 何も書いていない) で終わったら、その後は鎖の回ではない (今の本番の retry と同じ = mapWroteNothing)
  let chain = amazonChain ?? remainingJobs.includes(AMAZON_MAP_CHAIN[0]);
  //   🆕 #1652 Codex R1 Medium / R2 High: この回の写しが落ちたら、写しの前後の写しの記録 (sync_meta) を比べる (mapOutcome。timeout・異常終了を「前のまま」と推測しない):
  //   前のままと確かめられた (mapFailedOldTables) = もとの remaining にある f_sales 以降は古い表のまま普通の retry で流す (上流で見送らない・鎖の見送りもしない)・
  //     写しだけを鎖の未完として残す (写しが後で本当に写せた回に、鎖が f_sales から全部を流し直す)
  //   記録が変わった = commit の後の終了処理で落ちた = 写せた扱い (⚠️) にして鎖で f_sales から流す
  //   鍵待ち (73)・分からない (mapHold) = この回は f_sales・sales_velocity・pml_snapshot・Render同期 を流さない (新しい mirror_sku_* と古い対応の f_sales を一緒に送らない)
  let mapFailedOldTables = false;
  let mapHold = null;

  for (const jobName of RETRY_ORDER) {
    if (!remainingJobs.includes(jobName) && !rerun.has(jobName)) continue;
    if (!remainingJobs.includes(jobName)) log(`[Retry] ${jobName} を走らせ直す (上流がこの回で成功)`);

    // Company DB の写しの反映が世代と違う (証跡 master-publish の apply.broken) = m_products・上書き表を読む工程は再試行でも動かさない
    //   (daily-sync と同じ一覧 = publish-gate.js。RERUN_AFTER の走らせ直しも同じ。ほかの見送りの理由より先に見る。#1564 の見直し M-2)
    const gate = Object.hasOwn(JOB_DEFINITIONS, jobName) ? publishGateDecision(JOB_DEFINITIONS[jobName].script, publishGate) : { skip: false };
    if (gate.skip) {
      log(`[Retry] ${jobName} ${gate.summary}`);
      results.push({ name: jobName, success: false, blocked: true, gated: true, pingJobId: gate.pingJobId, summary: gate.summary });
      continue;
    }

    // 写しが鍵待ち (73)・結果が分からない回 = 鎖の後ろ (f_sales・sales_velocity・pml_snapshot・Render同期) はこの回は流さない (#1652 Codex R2 High)
    if (mapHold && AMAZON_MAP_CHAIN.indexOf(jobName) > 0) {
      log(`[Retry] ${jobName} スキップ (${mapHold}・Amazon SKU の写しの鎖、次回再試行)`);
      results.push({ name: jobName, success: false, summary: `⏸️ skipped (${mapHold}・Amazon SKU の写しの鎖)` });
      continue;
    }

    // Render同期 fail-fast: 今回 f_sales / 楽天sku_map を試行して失敗した場合スキップ。
    //   どちらかが remaining_jobs に無い (= 既に成功済み) なら同方向はクリア扱い、
    //   今回試行して失敗していたら Render は古い表を押し付けないようスキップ。
    if (jobName === 'Render同期') {
      const fSalesAttempt = results.find(r => r.name === 'f_sales');
      const skuMapAttempt = results.find(r => r.name === '楽天sku_map');
      const fSalesFailed = fSalesAttempt && !fSalesAttempt.success;
      const skuMapFailed = skuMapAttempt && !skuMapAttempt.success;
      if (fSalesFailed || skuMapFailed) {
        const reasons = [];
        if (fSalesFailed) reasons.push('f_sales 再失敗');
        if (skuMapFailed) reasons.push('楽天sku_map 再失敗');
        log(`[Retry] Render同期 スキップ (${reasons.join(', ')}、次回再試行)`);
        results.push({ name: 'Render同期', success: false, summary: `⏸️ skipped (${reasons.join(', ')})` });
        continue;
      }
    }

    // 上流 (取込) をこの回で再試行して失敗したら、下流は見送る (次の回へ)
    const blocked = upstreamBlock(jobName, results);
    if (blocked && !(mapFailedOldTables && jobName === 'f_sales' && UPSTREAM_OF[jobName] === AMAZON_MAP_CHAIN[0] && remainingJobs.includes(jobName))) {
      log(`[Retry] ${jobName} スキップ (${blocked}、次回再試行)`);
      results.push({ name: jobName, success: false, summary: `⏸️ skipped (${blocked})` });
      continue;
    }

    // 写しの鎖の回: 鎖の前の工程がこの回で落ちた (見送りも) = 流さない (次の回に残す)
    const prev = chain ? chainPrev(jobName) : null;
    if (prev) {
      const pa = results.find((r) => r.name === prev);
      if (pa && !pa.success) {
        log(`[Retry] ${jobName} スキップ (${prev} 再失敗・Amazon SKU の写しの鎖、次回再試行)`);
        results.push({ name: jobName, success: false, summary: `⏸️ skipped (${prev} 再失敗・Amazon SKU の写しの鎖)` });
        continue;
      }
    }

    // coordinator の工程は env CDB_FINANCE_COORDINATOR=1 のときだけ (勝手に起動しない。切り替え済みでスイッチが消えた = .env を直す・#1567 Codex R6 High)
    if (jobName === 'Amazon決済と財務' && !financeCoordinatorEnabled()) {
      log(`[Retry] ${jobName} スキップ (${FINANCE_COORDINATOR_ENV} が無い)`);
      results.push({ name: jobName, success: false, summary: `❌ ${FINANCE_COORDINATOR_ENV} が無い = coordinator を走らせない (coordinator に切り替え済みなら .env の ${FINANCE_COORDINATOR_ENV}=1 が消えた疑い = 今までの 2 工程にも戻さない。.env を確かめる)` });
      continue;
    }
    const def = JOB_DEFINITIONS[jobName];
    const isMap = jobName === AMAZON_MAP_CHAIN[0];
    const metaBefore = isMap ? readMapMeta() : null;   // 写しの前の写しの記録 (落ちたときに前後を比べる)
    let result = run(def.script, jobName, def.timeoutMs, def.args);
    if (isMap && !(result && result.success)) {
      if (result?.exitCode === 73) {
        mapHold = '写しが鍵待ち (73)';
      } else {
        const outcome = mapOutcome(metaBefore, readMapMeta());
        if (outcome === 'committed') {
          // commit の後の終了処理 (PG の切断の待ちなど) で落ちた = 古い表はもう Company DB の対応 = 写せた扱い (⚠️) で鎖を f_sales から流す (#1652 Codex R2 High)
          result = { ...result, success: true, committed_after_failure: true,
            summary: `⚠️ ${jobName}: 写しの記録が変わっていた = 古い表は写し終わっていた (commit の後の終了処理で失敗) = 鎖で f_sales から流す | ${String(result?.summary || '')}`.slice(0, 300) };
          chain = true;
          log(`[Retry] ${jobName} は commit の後に落ちた (写しの記録が変わった) = 写せた扱いで f_sales から鎖を流す`);
        } else if (outcome === 'unchanged') {
          mapFailedOldTables = true;
          if (chain) log(`[Retry] ${jobName} 再失敗 (写しの記録が前のまま = 古い表は前のまま) = この回の f_sales 以降は古い表で普通に再試行・写しは鎖の未完で残す`);
          chain = false;
        } else {
          mapHold = '写しの結果が分からない (写しの記録を読めない)';
        }
      }
      if (mapHold) log(`[Retry] ${jobName} 再失敗 (${mapHold}) = この回は f_sales〜Render同期 を流さない`);
    }
    results.push({ name: jobName, ...result });
    if (result && result.success) for (const d of (Object.hasOwn(rerunAfter, jobName) ? rerunAfter[jobName] : [])) rerun.add(d);
    // 写しが ⏭️ (持ち主 load = 古い表に何も書いていない) = この回は鎖の回でなくなる (f_sales 以降を走らせ直さない・鎖の見送りもしない・retry-state の鎖の印も消える。PR-C)
    if (chain && jobName === AMAZON_MAP_CHAIN[0] && mapWroteNothing(result)) {
      chain = false;
      log(`[Retry] ${jobName} は持ち主 load (何も書いていない) = 写しの鎖を外す (f_sales 以降は走らせ直さない)`);
    }
    if (chain && result && result.success && chainNext(jobName)) rerun.add(chainNext(jobName));   // 写しの鎖: 成功したら次の一段だけ
  }
  // 写しの鎖の途中 (写しの後) で落ちた = その先の工程を「見送り」の失敗として残す (次の回に鎖のまま落ちたところから流す)
  if (chain) {
    const i = AMAZON_MAP_CHAIN.findIndex((j, k) => k > 0 && results.some((r) => r.name === j && !r.success));
    const mapFailed = results.some((r) => r.name === AMAZON_MAP_CHAIN[0] && !r.success);   // 写しそのものが落ちた = 次の回も写しから (鎖を最初から流す)
    if (i > 0 && !mapFailed) for (const j of AMAZON_MAP_CHAIN.slice(i + 1)) if (!results.some((r) => r.name === j)) results.push({ name: j, success: false, summary: `⏸️ skipped (${AMAZON_MAP_CHAIN[i]} 失敗・Amazon SKU の写しの鎖)` });
  }
  results.amazonChainPending = (chain || mapFailedOldTables || !!mapHold) && results.some((r) => AMAZON_MAP_CHAIN.includes(r.name) && !r.success);
  return results;
}

/** 持ち主が company の肯定の手がかりがあるか (amazon-map.mjs の amazonMapHint)。読み込めない = 鎖の印 (retry-state) に従う側 = true */
async function amazonHintNow() {
  try { const M = await import('../company-db/publish/amazon-map.mjs'); return !!M.amazonMapHint({ dataDir: process.env.DATA_DIR || path.join(PROJECT_DIR, 'data') }); }
  catch (e) { console.warn(`[Retry] 写しの手がかりを読めない (${e.message}) = retry-state の印に従う`); return true; }
}

// ─── 手の口: Amazon SKU の写しの鎖だけを流す (⑦-2 PR-A・#1649 Codex R3 High / Medium 3) ───
/**
 * 手で写す入口はこれ 1 つ: 再試行と同じ回の鍵 (retry-lock.js・daily-sync の生存も確かめる) を取り、持ったまま
 *   写し → f_sales → sales_velocity → pml_snapshot → Render同期 を一続きで流す。途中で落ちたらその先は流さない (exit 1)。
 *   = daily-sync・自動再試行・手の 3 つは同じ回の鍵でどれか 1 つだけ (写しの子は「回の鍵を持つ親」からしか書かない = amazon-map.mjs の runLockHeldByParent)。
 *   回の鍵を取れない (daily-sync / 再試行が動いている) = 待たずに断る (retry-state にも触らない)。
 *   04:30〜07:30 (JST) は断る (#1649 Codex R4 High): 鎖が 07:00 の daily-sync と重なると、daily-sync は再試行の鍵を 60 秒待って起動をやめる (その朝の全部が抜ける)。
 *     始めの時刻ではなく「鎖の最長 (各段の timeout の和 = 70 分) + 余裕」を 07:00 から引いた時刻から、daily-sync が自分の鍵を取り終える 07:30 までを断る
 * 使い方: node -r dotenv/config apps/warehouse/retry-failed-jobs.js --amazon-map-chain [--allow-shrink | --accept-restore] [--expect-hash <H>]
 *   (--allow-shrink / --accept-restore は --expect-hash <H> と一緒。H = apps/company-db/publish/amazon-map.mjs --dry-run が出す Company DB のハッシュ)
 */
export const MANUAL_CHAIN_FLAG = '--amazon-map-chain';
/** 鎖の最長 = 各段の timeout の和 (写し 5 分 + f_sales 30 分 + 速度 15 分 + リスト 10 分 + Render 10 分 = 70 分) */
export const MANUAL_CHAIN_MAX_MS = AMAZON_MAP_CHAIN.reduce((n, j) => n + JOB_DEFINITIONS[j].timeoutMs, 0);
/** 07:00 の daily-sync (Task Scheduler) と、その起動の 60 秒の待ちに重ならないための余裕 (鍵・門を読む時間・timeout の後の後始末・時計のずれ) */
export const MANUAL_CHAIN_MARGIN_MS = 80 * 60 * 1000;
export const DAILY_SYNC_START_JST = '07:00';
/** daily-sync の起動が遅れても自分の鍵を取り終える時刻 (この後は daily-sync の鍵が生きていれば手の口は鍵で断られる) */
export const DAILY_SYNC_SETTLED_JST = '07:30';
const hmToMin = (hm) => Number(hm.slice(0, 2)) * 60 + Number(hm.slice(3, 5));
const minToHm = (m) => `${String(Math.floor(((m % 1440) + 1440) % 1440 / 60)).padStart(2, '0')}:${String((((m % 1440) + 1440) % 1440) % 60).padStart(2, '0')}`;
/** 手の口を断る時間 (JST・[始め, 終わり)) = 07:00 − (鎖の最長 + 余裕) 〜 07:30 = 04:30〜07:30。1 か所の定数から計算する */
export const MANUAL_CHAIN_QUIET_JST = Object.freeze([minToHm(hmToMin(DAILY_SYNC_START_JST) - Math.ceil((MANUAL_CHAIN_MAX_MS + MANUAL_CHAIN_MARGIN_MS) / 60000)), DAILY_SYNC_SETTLED_JST]);
export function parseManualChainArgs(argv) {
  const mapArgs = ['--chain'];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === MANUAL_CHAIN_FLAG) continue;
    if (a === '--allow-shrink' || a === '--accept-restore') mapArgs.push(a);
    else if (a === '--expect-hash') { mapArgs.push(a, String(argv[++i] ?? '')); }
    else throw new Error(`知らない引数: ${a} (使えるのは --allow-shrink / --accept-restore / --expect-hash <H>)`);
  }
  return { mapArgs };
}
/** 手の口を断る時間 (JST の HH:MM が MANUAL_CHAIN_QUIET_JST の間) = 理由 / 流してよい = null */
export function manualChainQuietReason(now = new Date()) {
  const hm = new Date(now.getTime() + 9 * 3600 * 1000).toISOString().slice(11, 16);
  return hm >= MANUAL_CHAIN_QUIET_JST[0] && hm < MANUAL_CHAIN_QUIET_JST[1] ? `いま ${hm} (JST) は 07:00 の daily-sync と重なりうる (${MANUAL_CHAIN_QUIET_JST.join('〜')}・鎖は最長 ${Math.round(MANUAL_CHAIN_MAX_MS / 60000)} 分) = 流さない (daily-sync が鍵を待って起動をやめるので)` : null;
}
/** 写しの鎖を一続きで流す (呼び手が回の鍵を持つ)。途中で落ちた (見送りも) = その先は流さない。写しの反映の門が壊れている = 門の一覧の工程は止まる */
export function runAmazonChainSteps({ mapArgs = ['--chain'], run = runScript, log = console.log, publishGate = { broken: false } } = {}) {
  const results = [];
  for (const job of AMAZON_MAP_CHAIN) {
    const def = JOB_DEFINITIONS[job];
    const gate = publishGateDecision(def.script, publishGate);
    if (gate.skip) { log(`[AmazonChain] ${job} ${gate.summary}`); results.push({ name: job, success: false, blocked: true, gated: true, summary: gate.summary }); break; }
    const r = run(def.script, job, def.timeoutMs, job === AMAZON_MAP_CHAIN[0] ? mapArgs : def.args);
    results.push({ name: job, ...r });
    if (!r || !r.success) { log(`[AmazonChain] ${job} が失敗 = この先 (${AMAZON_MAP_CHAIN.slice(AMAZON_MAP_CHAIN.indexOf(job) + 1).join(' → ') || 'なし'}) は流さない`); break; }
  }
  return results;
}
/** 手の口の 1 回分 (試験は鍵の場所・時計・起動を差し替える)。戻り値 = { code, results, reason? } */
export async function manualAmazonChain(argv, { lockFile = RETRY_LOCK_FILE, dailySyncLockFile = DAILY_SYNC_LOCK_FILE, isAlive = undefined, now = new Date(), run = runScript, log = console.log,
  publishGate = null, acquire = acquireRetryLock, release = releaseRetryLock } = {}) {
  let parsed;
  try { parsed = parseManualChainArgs(argv); } catch (e) { log(`❌ Amazon SKU の写しの鎖: ${e.message}`); return { code: 1, results: [], reason: 'args' }; }
  const quiet = manualChainQuietReason(now);
  if (quiet) { log(`❌ Amazon SKU の写しの鎖: ${quiet}`); return { code: 1, results: [], reason: 'quiet_window' }; }
  const lock = acquire({ lockFile, dailySyncLockFile, ...(isAlive ? { isAlive } : {}), now });
  if (!lock.ok) { log(`❌ Amazon SKU の写しの鎖: 回の鍵を取れない = 流さない (${lock.reason})。daily-sync / 自動再試行が終わってから`); return { code: 1, results: [], reason: lock.dailySync ? 'daily_sync' : 'locked' }; }
  try {
    process.env.WAREHOUSE_BUSINESS_DATE = toJstDate(now);
    const gate = publishGate || readPublishGate({ dataDir: process.env.DATA_DIR || path.join(PROJECT_DIR, 'data') });
    const results = runAmazonChainSteps({ mapArgs: parsed.mapArgs, run, log, publishGate: gate });
    const ok = results.length === AMAZON_MAP_CHAIN.length && results.every((r) => r.success);
    log(`${ok ? '✅' : '❌'} Amazon SKU の写しの鎖: ${results.map((r) => `${r.success ? '✅' : '❌'} ${r.name}`).join(' → ')}${ok ? '' : ' (落ちたところから先は流していない)'}`);
    return { code: ok ? 0 : 1, results };
  } finally { release(lock); }
}

async function main() {
  if (process.argv.includes(MANUAL_CHAIN_FLAG)) {
    const r = await manualAmazonChain(process.argv.slice(2));
    process.exitCode = r.code;
    return;
  }
  const lock = acquireRetryLock({ lockFile: RETRY_LOCK_FILE, dailySyncLockFile: DAILY_SYNC_LOCK_FILE });
  if (!lock.ok) {
    console.log(`[Retry] 見送り: ${lock.reason}`);
    // やることがある (retry-state がある)・lock を書けない・朝の daily-sync のために見送った (daily-sync が後で state を書く) ときは知らせる (空振りの見送りは静かに)
    if (lock.error || lock.dailySync || fs.existsSync(RETRY_STATE_FILE)) await notify(`⏸️ *Warehouse自動再試行 見送り*\n${lock.reason}\nretry-state はそのまま (動いている回が結果を書く・次の回が拾う。最後の 11:30 を見送った日は残りを手で確かめる)`);
    if (lock.error) process.exitCode = 1;
    return;
  }
  if (lock.recovered) console.warn(`[Retry] 前の回の lock の残骸を回収した (${lock.recovered}) = 前の回は途中で止まった`);
  try {
    await runLocked();
  } finally {
    releaseRetryLock(lock);
  }
}

async function runLocked() {
  const loadResult = loadState();
  if (!loadResult.found) {
    console.log('[Retry] retry-state 無し → no-op');
    return;
  }
  if (loadResult.parseError) {
    // 破損 state → 削除試行 + 失敗時は通知
    console.warn('[Retry] 破損した retry-state を削除します');
    const del = deleteState();
    if (!del.ok) {
      const msg = `🔴 *Warehouse自動再試行 cleanup 失敗*\n破損した retry-state を削除できず (${del.error})。手動削除を: ${RETRY_STATE_FILE}\nparse error: ${loadResult.parseError}`;
      console.error(msg);
      await notify(msg);
    }
    return;
  }
  const state = loadResult.state;
  // 工程の名前を今のスイッチの工程に読み替える (#1567)。coordinator の名前 → 今までの 2 工程は、一度も切り替えていないと確かに分かったときだけ
  //   (ローカル + Render の門 = legacyGateCheck。切り替え済み・判定できない = 読み替えない = その工程はスイッチが無いので走らせず ❌・Codex R6 High / R7 High 1)
  if (state && Array.isArray(state.remaining_jobs)) {
    const coordinator = financeCoordinatorEnabled();
    let switched = null;
    if (!coordinator && state.remaining_jobs.some((j) => Object.hasOwn(LEGACY_JOBS_OF, j))) {
      const gate = await legacyGateCheck({ dataDir: process.env.DATA_DIR || path.join(PROJECT_DIR, 'data') });
      switched = !gate.allowed;
      if (!gate.allowed) console.warn(`[Retry] ${gate.message}`);
    }
    state.remaining_jobs = renameRetryJobs(state.remaining_jobs, { coordinator, switched });
  }

  const today = toJstDate(new Date());
  if (state.run_date !== today) {
    console.log(`[Retry] state は別日 (state.run_date=${state.run_date}, today=${today}) → クリーンアップして終了`);
    const del = deleteState();
    if (!del.ok) {
      const msg = `🔴 *Warehouse自動再試行 cleanup 失敗*\n別日 retry-state を削除できず (${del.error})。手動でファイルを削除してください: ${RETRY_STATE_FILE}`;
      console.error(msg);
      await notify(msg);
    }
    return;
  }

  if (!Array.isArray(state.remaining_jobs) || state.remaining_jobs.length === 0) {
    console.log('[Retry] remaining_jobs 空 → クリーンアップして終了');
    const del = deleteState();
    if (!del.ok) {
      const msg = `🔴 *Warehouse自動再試行 cleanup 失敗*\nremaining_jobs 空の retry-state を削除できず (${del.error})。手動でファイルを削除してください: ${RETRY_STATE_FILE}`;
      console.error(msg);
      await notify(msg);
    }
    return;
  }

  // JST 業務日付を子プロセスに伝える
  process.env.WAREHOUSE_BUSINESS_DATE = today;
  // 朝の daily-sync の実行 ID を引き継ぐ (送り手の証跡と見張りが同じ ID で結びつく。無い state = 古い版の daily-sync が書いた → 見張りは blocked になるだけ)
  if (state.daily_sync_run_id) process.env.DAILY_SYNC_RUN_ID = String(state.daily_sync_run_id);

  const retryCount = (state.retry_count || 0) + 1;
  const startedAt = new Date();
  console.log(`[Retry] 試行 ${retryCount}/${MAX_RETRY_COUNT}: ${state.remaining_jobs.join(', ')}`);

  const publishGate = readPublishGate({ dataDir: process.env.DATA_DIR || path.join(PROJECT_DIR, 'data') });
  if (publishGate.broken) console.log(`[Retry] ⚠️ Company DB の写しの反映の門 = ${publishGate.state} (${publishGate.reason}) → m_products・上書き表を読む工程は動かさない`);
  // 写しの鎖は「持ち主が company の肯定の手がかり」(config が company・有効な写しの記録) がある日だけ (#1649 Codex R3 Medium 2。今の本番 = 手がかりなし = master と同じ)
  const results = runRetryRound(state.remaining_jobs, { publishGate, amazonChain: amazonChainActive(state) && await amazonHintNow() });
  // 止めた工程のうち自分で ping を打つもの = fail の ping (送れなくても再試行は続ける)
  for (const r of results.filter((x) => x.gated && x.pingJobId)) {
    try { const { sendPing } = await import('../../scripts/company-db/lz-daily.mjs'); await sendPing(r.pingJobId, { status: 'fail', note: String(r.summary).slice(0, 180) }); }
    catch (e) { console.warn(`[Retry] 止めた工程の ping を送れない (${r.pingJobId}): ${e.message}`); }
  }

  // fail-closed: remaining_jobs のうち runner に定義が無いジョブ (daily-sync の RETRYABLE_JOBS には
  // あるが JOB_DEFINITIONS/RETRY_ORDER 未登録、例: Amazon finance build) は上の
  // ループで実行されない。results に載らないと stillFailed=0 となり state削除→誤「復旧成功」で
  // サイレントに落ちる。未対応ジョブは失敗扱いで残し、最終的に 🔴 通知で顕在化させる。
  // Codex R2 High #2: JOB_DEFINITIONS だけでなく RETRY_ORDER 漏れも未対応扱いにする。
  //   定義はあるが RETRY_ORDER に無いと実行ループ (RETRY_ORDER を回す) に入らず、
  //   かつ従来判定では unhandled にも載らず、誤「✅復旧成功」でサイレントに落ちるため。
  const unhandled = state.remaining_jobs.filter(j => !JOB_DEFINITIONS[j] || !RETRY_ORDER.includes(j));
  for (const j of unhandled) {
    const reason = !JOB_DEFINITIONS[j] ? 'JOB_DEFINITIONS 未登録' : 'RETRY_ORDER 未登録';
    results.push({ name: j, success: false, summary: `⚠️ retry runner 未対応 (${reason}) のため未実行` });
  }

  const stillFailed = results.filter(r => !r.success).map(r => r.name);
  const justSucceeded = results.filter(r => r.success).map(r => r.name);
  const duration = Math.round((Date.now() - startedAt.getTime()) / 1000);

  if (stillFailed.length === 0) {
    // 全部成功 → state削除 + ✅復旧通知
    const del = deleteState();
    let msg = `🔄 *Warehouse自動再試行 ${retryCount}回目: ✅ 復旧成功* (${duration}秒)\n`;
    msg += `復旧したジョブ: ${justSucceeded.join(', ')}\n`;
    for (const w of warnLines(results)) msg += `${w}\n`;
    if (!del.ok) {
      msg += `\n🔴 retry-state クリーンアップ失敗 (${del.error})。後続 retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}\n`;
    }
    console.log('\n' + msg);
    await notify(msg);
  } else if (retryCount >= MAX_RETRY_COUNT) {
    // 最終失敗 → state削除 + 🔴最終通知 (手動対応必要)
    const del = deleteState();
    let msg = `🔴 *Warehouse自動再試行 失敗* (${retryCount}回試行)\n`;
    msg += `手動対応が必要なジョブ: ${stillFailed.join(', ')}\n`;
    if (justSucceeded.length > 0) msg += `今回復旧: ${justSucceeded.join(', ')}\n`;
    for (const w of warnLines(results)) msg += `${w}\n`;
    for (const r of results.filter(r => !r.success)) {
      msg += `❌ ${r.name}: ${r.summary}\n`;
    }
    if (!del.ok) {
      msg += `\n🔴 retry-state クリーンアップ失敗 (${del.error})。後続 retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}\n`;
    }
    console.log('\n' + msg);
    await notify(msg);
  } else {
    // 次回も試行 → state更新
    const sav = saveState({
      ...state,
      remaining_jobs: stillFailed,
      retry_count: retryCount,
      last_attempt_at: startedAt.toISOString(),
      amazon_map_chain: results.amazonChainPending === true,   // 写しの鎖の途中 = 次の回も鎖で流す
    });
    if (justSucceeded.length > 0) {
      let msg = `🔄 *Warehouse自動再試行 ${retryCount}回目: 部分復旧* (${duration}秒)\n`;
      msg += `復旧: ${justSucceeded.join(', ')}\n`;
      for (const w of warnLines(results)) msg += `${w}\n`;
      msg += `残り: ${stillFailed.join(', ')}（次回再試行予定）\n`;
      if (!sav.ok) {
        msg += `\n🔴 retry-state 書き込み失敗 (${sav.error})、次回 retry の retry_count / remaining_jobs が古いままになる恐れあり。手動確認を\n`;
      }
      console.log('\n' + msg);
      await notify(msg);
    } else if (!sav.ok) {
      // 全失敗だが state 更新失敗 → 通知
      const msg = `🔴 *Warehouse自動再試行 ${retryCount}回目: 全失敗 + state更新失敗*\n書き込み失敗 (${sav.error})、次回 retry が誤動作する恐れあり`;
      console.error(msg);
      await notify(msg);
    } else {
      // 何も復旧しなかったが state 更新は成功 → 連続失敗で煩くならないよう通知抑制
      console.log(`[Retry] 試行 ${retryCount} 全失敗、通知抑制 (state更新成功)`);
    }
  }
}

// 試験から import しても走らないように (直接起動のときだけ main)
// 🚨 実体パスで比べる: Node は import.meta.url をリンクの先 (実体) にするが、argv[1] はリンクのまま → junction やシンボリックリンク経由で起動すると不一致になり、
//    main() が走らず exit 0 で無言終了する (state も通知も触らない。Codex #1369 R1 #1)。realpath が取れなければ素のパスで比べる
const realPath = (p) => { try { return fs.realpathSync.native(p); } catch { return path.resolve(p); } };
const foldCase = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);   // パスの大文字小文字を区別しないのは Windows だけ (Codex #1369 R2)
export const isDirectRun = (argv1, selfUrl) => !!argv1 && foldCase(realPath(argv1)) === foldCase(realPath(fileURLToPath(selfUrl)));
const isMain = isDirectRun(process.argv[1], import.meta.url);
if (isMain) main().catch(async (e) => {
  console.error('[Retry] 致命的エラー:', e.message);
  await notify(`❌ *Warehouse自動再試行 実行エラー*\n${e.message}`);
  process.exit(1);
});
