/**
 * rakuten-dd-columns.js — 楽天データダウンロードCSVの列定義 (mall-csv-fetcher P1-R3)
 *
 * miniPC側 (apps/warehouse/rakuten-dd-lib.js) と Render mirror側
 * (apps/warehouse-mirror/db.js, router.js) で列名を共有するための定数。
 * 実測ヘッダ (2026-07-10) と1:1対応。ここを変えたら両側のスキーマ影響を確認すること。
 */

// 店舗データのベンチマーク列 (指標×グループの直積。ヘッダ順=指標ごとにグループが並ぶ)
export const BENCH_METRICS = [
  ['売上金額', 'sales_yen'], ['売上件数', 'orders'], ['アクセス人数', 'access'],
  ['転換率', 'cvr_pct'], ['客単価', 'aov_yen'],
];
export const BENCH_GROUPS = [
  ['サブジャンルTOP10平均', 'bench_top10'],
  ['月商別平均値（月商1億以上）', 'bench_cls1'],
  ['月商別平均値（月商3,000万～9,999万）', 'bench_cls2'],
  ['月商別平均値（月商1,000万～2,999万）', 'bench_cls3'],
  ['月商別平均値（月商100万～999万）', 'bench_cls4'],
  ['月商別平均値（月商50万～99万）', 'bench_cls5'],
  ['月商別平均値（月商50万未満）', 'bench_cls6'],
];
export const STORE_BENCH_COLS = BENCH_GROUPS.flatMap(([, g]) => BENCH_METRICS.map(([, m]) => `${g}_${m}`));

// 店舗データの自店・オプション列 (ベンチ以外)
export const STORE_DEVICE_BASE_COLS = [
  'sales_yen', 'orders', 'access_users', 'cvr_pct', 'aov_yen',
  'unique_users', 'buyers_member', 'buyers_guest', 'buyers_new', 'buyers_repeat',
  'tax_out_yen', 'shipping_yen', 'coupon_store_yen', 'coupon_rakuten_yen',
  'free_ship_coupon_yen', 'wrapping_yen', 'settlement_fee_yen',
];
export const STORE_DEVICE_OPT_COLS = [
  'deal_sales_yen', 'deal_orders', 'deal_access', 'deal_cvr_pct', 'deal_aov_yen',
  'deal_unique_users', 'deal_buyers_member', 'deal_buyers_guest', 'deal_buyers_new', 'deal_buyers_repeat',
  'point_boost_sales_yen', 'point_boost_orders', 'point_boost_grant_fee_yen',
  'social_gift_sales_yen', 'social_gift_orders',
];
// REAL で保持する列 (率・平均・ベンチ)。それ以外の数値列は INTEGER
export const STORE_DEVICE_REAL_COLS = new Set(['cvr_pct', 'deal_cvr_pct', ...STORE_BENCH_COLS]);

// カテゴリページデータの属性・地域・会員ランク列 (日本語ヘッダ→列名)
export const CATEGORY_DEMO_DEFS = [
  ['男性 年齢すべて', 'demo_m_all'], ['女性 年齢すべて', 'demo_f_all'], ['性別 不明 年齢すべて', 'demo_u_all'],
  ['男性 20代以下', 'demo_m_u29'], ['男性 30代', 'demo_m_30s'], ['男性 40代', 'demo_m_40s'],
  ['男性 50代以上', 'demo_m_50s'], ['男性 年齢不明', 'demo_m_unk'],
  ['女性 20代以下', 'demo_f_u29'], ['女性 30代', 'demo_f_30s'], ['女性 40代', 'demo_f_40s'],
  ['女性 50代以上', 'demo_f_50s'], ['女性 年齢不明', 'demo_f_unk'],
  ['性別不明 20代以下', 'demo_u_u29'], ['性別不明 30代', 'demo_u_30s'], ['性別不明 40代', 'demo_u_40s'],
  ['性別不明 50代以上', 'demo_u_50s'], ['性別不明 年齢不明', 'demo_u_unk'],
  ['北海道', 'reg_hokkaido'], ['北東北', 'reg_kita_tohoku'], ['南東北', 'reg_minami_tohoku'],
  ['関東', 'reg_kanto'], ['北陸', 'reg_hokuriku'], ['信越', 'reg_shinetsu'], ['東海', 'reg_tokai'],
  ['関西', 'reg_kansai'], ['中国', 'reg_chugoku'], ['四国', 'reg_shikoku'],
  ['北九州', 'reg_kita_kyushu'], ['南九州', 'reg_minami_kyushu'], ['沖縄', 'reg_okinawa'],
  ['海外', 'reg_overseas'], ['不明', 'reg_unknown'],
  ['レギュラー', 'rank_regular'], ['シルバー', 'rank_silver'], ['ゴールド', 'rank_gold'],
  ['プラチナ', 'rank_platinum'], ['ダイヤモンド', 'rank_diamond'], ['ランク不明', 'rank_unknown'],
];
export const CATEGORY_DEMO_COLS = CATEGORY_DEMO_DEFS.map(([, c]) => c);
