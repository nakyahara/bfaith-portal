/**
 * lib/amazon-finance-read.js — Amazon の財務を読む所の共通の読み口 (F4-1・2026-10-03)
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/19_Amazon財務の利用側の切替_設計_20261003.md (§6.3・D-72・§10 F4-1)。
 * Codex R-F4-1 の着手の条件:
 *   1. 既定は必ず legacy (今の写し)。F4-1 では Company DB に切り替えない
 *      → このファイルの「どこから読むか (source)」は legacy の 1 つだけ。cdb の道は作っていない (SOURCE_TABLES に無い = 選べない・選ぶと起動で止まる)
 *   2. 使う所 (consumer) ごとの profile を先に固定する
 *      → PROFILE_SETS.legacy (LEGACY_PROFILE) = 閉じた一覧。consumer × 読むもの (dataset) ごとに source を持つ。一覧に無い consumer・宣言していない dataset は例外
 *   3. 全部の API の before/after の JSON が一致する → scripts/test-amazon-finance-read-parity.mjs
 *   4. site-products の asinFinance の除去は別 PR (ここでは読み口を通すだけ)
 *      → 2026-10-03 に外した: ASIN は SKU → ASIN の専用のマップ (lib/amazon-sku-asin-map.js) から引く = site-products は財務を読まない
 *        (consumer 'site-products' は一覧から消した。dashboard・supplier-sales の ASIN も同じマップ・財務の asin_norm は常に空)
 *
 * 読み手は自分の SQL を持ったまま、表の名前だけをここからもらう (SQL の中身・列・並び・丸めは変えない)。
 *   例: `FROM ${financeDailyTable('amazon-dashboard:trend')}`
 *
 * D-72 の env の値の不統一 (`legacy` / `cdb` と `cdb:finance_only` / `cdb:all` が混ざっていた) の解き方:
 *   - 切り替えの単位 = **profile の組 (PROFILE_SETS の名前)**。組 = 全部の consumer × dataset の source の表 (閉じた一覧)。
 *     段ごとに組を 1 つ足す (F4-3 で例えば `stage3_finance_only`・F4-4 で `stage4_finance_columns`)。
 *   - env は将来 `AMAZON_FINANCE_READ_PROFILE` = 組の名前 1 つだけ (値は PROFILE_SETS のキーに限る・それ以外は起動で止まる)。
 *     🚨 F4-1 では env を **読まない** = 組は legacy に固定 (台帳の temporary_asset も F4-3 で env を足すときに登録する)。
 *
 * 間接の読み手 (統合の view を通して読む所) は INDIRECT_CONSUMERS に書く。view の表の名前は
 * 'mall-finance-unified-view' の profile で決まる = view の読み手 (purchase-orders・ai-insights) は同じ source を読む。
 *
 * 読み口の外 (表の読みではないもの・今のまま):
 *   - 写しへの書き込み (apps/warehouse-mirror/router.js の受け口・db.js の表の DDL)・miniPC の送り手 (apps/warehouse/*)
 *   - ai-insights の「取得完了の印」= sync_run_chunks の entity 'amazon_finance_sku_daily' (送信の台帳。F4-3 で写しの記録に読み替える・19 §7 段 3)
 */

/** source ごとの表の名前。🚨 F4-1 は legacy だけ (今の写し = miniPC の SQLite の build → Render の mirror) */
const SOURCE_TABLES = Object.freeze({
  legacy: Object.freeze({
    finance_daily: 'mirror_amazon_finance_sku_daily',     // 日 × SKU の財務 (売上・数量・手数料・返金・補てん・原価・利益)
    account_fees: 'mirror_amazon_account_fees_monthly',   // 月 × 手数料の種類 (SKU に付かない手数料)
  }),
});

export const SOURCES = Object.freeze(Object.keys(SOURCE_TABLES));
export const DATASETS = Object.freeze(['finance_daily', 'account_fees']);

/**
 * consumer ごとの profile (legacy の組)。キー = 使う所・値 = { dataset: source }。
 * 1 つの API の応答の中で読む所が分かれるもの (dashboard の「確定の境」と「中身」) は consumer を分けてある
 * (19 §7: 段 3 で dashboard の月の手数料と確定の境だけを先に替える)。
 */
const LEGACY_PROFILE = Object.freeze({
  // amazon-dashboard (apps/amazon-dashboard/queries.js)
  'amazon-dashboard:settled-boundary': Object.freeze({ finance_daily: 'legacy' }),   // lastSettledDate / settledWindow / settledCompleteDate (全部のタブと margin-alert が使う「決済のそろった日」)
  'amazon-dashboard:overview': Object.freeze({ finance_daily: 'legacy' }),           // 概要のタイル (settledSummary)
  'amazon-dashboard:account-fees': Object.freeze({ account_fees: 'legacy' }),        // 月の手数料の表 (getAccountFees) と月のタイルの最終利益 (accountFeesCostForMonth)
  'amazon-dashboard:trend': Object.freeze({ finance_daily: 'legacy' }),              // 傾向 (getTrend)
  'amazon-dashboard:waterfall': Object.freeze({ finance_daily: 'legacy' }),          // 利益の滝 (getWaterfall)
  'amazon-dashboard:sku-profit': Object.freeze({ finance_daily: 'legacy' }),         // SKU の利益の表・CSV (getSkuProfit)
  'amazon-dashboard:ads': Object.freeze({ finance_daily: 'legacy' }),                // 広告 (getAdsAnalysis・月の TACoS)
  'amazon-dashboard:bestsellers': Object.freeze({ finance_daily: 'legacy' }),        // 売れ筋 (getBestsellers)
  'amazon-dashboard:diagnosis': Object.freeze({ finance_daily: 'legacy' }),          // 診断 (getDiagnosis)
  // 夜の処理・通知
  'margin-alert': Object.freeze({ finance_daily: 'legacy' }),                        // apps/profit-analysis/margin-alert-job.js (getSkuProfit を consumer 'margin-alert' で呼ぶ・境は settled-boundary)
  // ほかのアプリ
  'amazon-pricing': Object.freeze({ finance_daily: 'legacy' }),                      // apps/amazon-pricing (360 行の view・指紋・鮮度・判定の入力の出どころ)
  'supplier-sales': Object.freeze({ finance_daily: 'legacy' }),                      // apps/supplier-sales/aggregate.js (社内と未認証の公開の口が同じ関数)
  'mall-finance-unified-view': Object.freeze({ finance_daily: 'legacy' }),           // apps/warehouse-mirror/db.js が作る v_mall_finance_daily_unified の Amazon の枝
});

/** profile の組。🚨 F4-1 は legacy だけ。組を足すのは F4-3 以降 (段ごとに 1 つ・Codex レビューを通してから) */
export const PROFILE_SETS = Object.freeze({ legacy: LEGACY_PROFILE });

/** F4-1 で使う組 (env を読まない = 固定) */
export const ACTIVE_PROFILE_SET = 'legacy';

/** 統合の view を通して読む所 (自分では表の名前を持たない)。値 = 読み口の consumer */
export const INDIRECT_CONSUMERS = Object.freeze({
  'purchase-orders': 'mall-finance-unified-view',   // apps/purchase-orders/router.js の /api/products/:code/mall-sales (Amazon の数量・FBA/FBM)
  'ai-insights': 'mall-finance-unified-view',       // apps/ai-insights/facts.js (週次の粗利・ワーストの SKU)
});

/**
 * 組の形を確かめる (閉じた一覧)。違反があれば例外。
 * - 全部の consumer が組にある・余計な consumer が無い (基準 = legacy の組の consumer の一覧)
 * - dataset は DATASETS のどれか・source は SOURCES のどれか (F4-1 では legacy だけ)
 * - consumer ごとの dataset の集合が legacy の組と完全に同じ (組が変えてよいのは source だけ)
 * - 間接の読み手の行き先が consumer の一覧にある
 * @returns {string[]} consumer の一覧
 */
export function validateProfileSet(name, sets = PROFILE_SETS) {
  if (!Object.hasOwn(sets, name)) throw new Error(`amazon-finance-read: profile の組 '${name}' は無い (あるのは ${Object.keys(sets).join(', ')})`);
  const set = sets[name];
  const expected = Object.keys(LEGACY_PROFILE).sort();
  const got = Object.keys(set).sort();
  const missing = expected.filter((c) => !got.includes(c));
  const extra = got.filter((c) => !expected.includes(c));
  if (missing.length || extra.length) {
    throw new Error(`amazon-finance-read: 組 '${name}' の consumer が閉じた一覧と違う (足りない: ${missing.join(', ') || 'なし'} / 余計: ${extra.join(', ') || 'なし'})`);
  }
  for (const c of got) {
    const p = set[c];
    const ds = Object.keys(p || {});
    if (ds.length === 0) throw new Error(`amazon-finance-read: 組 '${name}' の consumer '${c}' が何も読まない`);
    for (const d of ds) {
      if (!DATASETS.includes(d)) throw new Error(`amazon-finance-read: 組 '${name}' の '${c}' に知らない dataset '${d}'`);
      if (!SOURCES.includes(p[d])) throw new Error(`amazon-finance-read: 組 '${name}' の '${c}'.${d} の source '${p[d]}' は選べない (F4-1 で選べるのは ${SOURCES.join(', ')} だけ)`);
    }
    // consumer が読むもの (dataset の集合) も閉じた一覧 = legacy の組と完全に同じ (差し替え・足し・抜けを拒む。Codex #1599 R1 M1)。
    // 組が変えてよいのは source だけ
    const want = Object.keys(LEGACY_PROFILE[c]).sort();
    const have = [...ds].sort();
    if (want.length !== have.length || want.some((d, i) => d !== have[i])) {
      throw new Error(`amazon-finance-read: 組 '${name}' の '${c}' の dataset が閉じた一覧と違う (決まり: ${want.join(', ')} / 組: ${have.join(', ')})`);
    }
  }
  for (const [ic, target] of Object.entries(INDIRECT_CONSUMERS)) {
    if (!Object.hasOwn(set, target)) throw new Error(`amazon-finance-read: 間接の読み手 '${ic}' の行き先 '${target}' が組 '${name}' に無い`);
  }
  return got;
}

// 起動のときに確かめる (import した時点で壊れた profile は止まる)
for (const name of Object.keys(PROFILE_SETS)) validateProfileSet(name);
validateProfileSet(ACTIVE_PROFILE_SET);

/**
 * consumer が dataset を読む表の名前。
 * 一覧に無い consumer・profile で宣言していない dataset は例外 (読み手を足すときは LEGACY_PROFILE と試験の EXPECTED に足す = レビューで見える)。
 */
export function amazonFinanceTable(consumer, dataset) {
  const set = PROFILE_SETS[ACTIVE_PROFILE_SET];
  if (!Object.hasOwn(set, consumer)) throw new Error(`amazon-finance-read: consumer '${consumer}' は profile の一覧に無い`);
  const profile = set[consumer];
  if (!Object.hasOwn(profile, dataset)) throw new Error(`amazon-finance-read: consumer '${consumer}' は dataset '${dataset}' を宣言していない`);
  return SOURCE_TABLES[profile[dataset]][dataset];
}

/** 日 × SKU の財務の表 */
export const financeDailyTable = (consumer) => amazonFinanceTable(consumer, 'finance_daily');
/** 月 × 手数料の種類の表 */
export const accountFeesTable = (consumer) => amazonFinanceTable(consumer, 'account_fees');

/** consumer → { dataset: { source, table } } の表 (試験・画面に出す用) */
export function describeProfile(name = ACTIVE_PROFILE_SET) {
  validateProfileSet(name);
  const out = {};
  for (const [c, p] of Object.entries(PROFILE_SETS[name])) {
    out[c] = {};
    for (const [d, s] of Object.entries(p)) out[c][d] = { source: s, table: SOURCE_TABLES[s][d] };
  }
  return out;
}
