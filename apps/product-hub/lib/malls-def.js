/**
 * 展開先モールの定義 (2026-08-23)。
 *
 * **定義だけを持つファイル**。workflow-progress.js (工程) と mall-status.js (モール) の
 * 双方が参照するため、循環 import を避けてここに切り出している。
 *
 * Amazon は FBA 側の別管理 (SKU は Pricetar 発行の pr_xxx) なので含めない (中原さん 2026-08-23)。
 * hint は運用の目安を画面に出すためのもので、実際の進み方は人が state で管理する。
 */
export const MALLS = [
  { code: 'rakuten', label: '楽天', sort: 10, hint: 'このアプリから出品できます (出品すると自動で完了になります)' },
  { code: 'yahoo', label: 'Yahoo!', sort: 20, hint: 'RakutenYahooSync が楽天から同期します' },
  { code: 'aupay', label: 'au PAY', sort: 30, hint: '' },
  { code: 'mercari', label: 'メルカリShops', sort: 40, hint: 'mercari-sync が楽天から同期します' },
  { code: 'qoo10', label: 'Qoo10', sort: 50, hint: '' },
  { code: 'linegift', label: 'LINEギフト', sort: 60, hint: 'linegift-sync が楽天から同期します' },
];

export const MALL_CODES = new Set(MALLS.map((m) => m.code));
export const MALL_LABELS = new Map(MALLS.map((m) => [m.code, m.label]));

/** 工程「出品・展開」のコード。この工程の完了はモール状況が正 */
export const LISTING_STEP_CODE = 'listing';
