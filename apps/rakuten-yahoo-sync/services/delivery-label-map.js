/**
 * 楽天の配送方法 → 移行アプリの配送方法ラベル (delivery_mapping の主キー) の対応 (2026-10-22)。
 *
 * 旧 Notion の「配送方法」は delivery_mapping のラベル (ヤマト50サイズ / ヤマト宅急便 …) をそのまま持っていた。
 * 商品登録ハブは楽天の配送方法グループ ID ('1'〜'9'、名前の正本 = apps/price-update/shipping-labels.js) と
 * 「ヤフーだけ別配送」(draft_yahoo.shipping_override + delivery_label = 楽天の配送方法名) で持つので、ここで読み替える。
 *
 * 🚨 対応を決めていない配送方法 (定形外の単独・飛脚宅配便・ゆうパック・使用不可のクリックポスト) は **null を返す**。
 *    推測で近いものを当てると Yahoo の送料設定を誤る → 呼び出し側は「配送方法を選んでください」で止める。
 *    対応を足すときは delivery_mapping (Yahoo の delivery / postage_set) と一緒に決める。
 */

import { RAKUTEN_SHIPPING_METHODS } from '../../price-update/shipping-labels.js';

/** 楽天の配送方法名 → delivery_mapping のラベル (同じ送料グループで Yahoo に出す) */
const BY_RAKUTEN_NAME = Object.freeze({
  'ネコポス': 'ネコポス',
  'クリックポスト': 'クリックポスト',
  'ヤマト運輸宅急便': 'ヤマト宅急便',
  '宅急便50サイズ以下': 'ヤマト50サイズ',
  'ゆうパケットパフ': 'ゆうパケットパフ',
});

/** 楽天=定形外 + ヤフーだけ別配送 (商品登録ハブの複合選択肢 1y8 / 1y5 と同じ組み合わせ) */
const TEIKEIGAI_YAHOO_OVERRIDE = Object.freeze({
  '宅急便50サイズ以下': '定形外（ヤフーのみ宅急便50サイズ）',
  'ネコポス': '定形外（ヤフーのみネコポス）',
});

const TEIKEIGAI_GROUP = '1';

/** 楽天の配送方法グループ ID → ラベル (対応が無ければ null) */
export function labelFromRakutenGroup(groupId) {
  const name = RAKUTEN_SHIPPING_METHODS[String(groupId ?? '').trim()];
  return name ? (BY_RAKUTEN_NAME[name] ?? null) : null;
}

/**
 * 商品登録ハブの値 → ラベル。
 * @param {{ rakutenGroup: string|null, shippingOverride: boolean, yahooDeliveryName: string|null }} p
 * @returns {{ label: string|null, reason?: string }}
 */
export function labelFromPortal({ rakutenGroup, shippingOverride, yahooDeliveryName }) {
  const g = String(rakutenGroup ?? '').trim();
  if (shippingOverride) {
    const y = String(yahooDeliveryName ?? '').trim();
    if (!y) return { label: null, reason: 'ヤフーだけ別配送になっていますが、Yahoo!の配送方法が空です' };
    if (g === TEIKEIGAI_GROUP && TEIKEIGAI_YAHOO_OVERRIDE[y]) return { label: TEIKEIGAI_YAHOO_OVERRIDE[y] };
    const label = BY_RAKUTEN_NAME[y] ?? null;
    return label ? { label } : { label: null, reason: `Yahoo!の配送方法「${y}」は移行アプリの配送方法に対応がありません` };
  }
  if (!g) return { label: null, reason: '楽天の配送方法が空です' };
  const label = labelFromRakutenGroup(g);
  if (label) return { label };
  const name = RAKUTEN_SHIPPING_METHODS[g] || `ID ${g}`;
  return { label: null, reason: `楽天の配送方法「${name}」は Yahoo!の配送方法への対応を決めていません (定形外なら「定形外（ヤフーのみ…）」を選んでください)` };
}

/**
 * 楽天 RMS の商品 (Items API 2.0) → 候補のラベル。表示している SKU の配送方法グループが 1 つにそろっているときだけ。
 * 候補を出すだけで確定はしない (確定は人が押す — D-2 と同じ考え方)。
 */
export function suggestDeliveryLabelFromRakutenItem(rakutenItem) {
  const v = rakutenItem?.variants;
  const list = Array.isArray(v) ? v : (v && typeof v === 'object' ? Object.values(v) : []);
  const groups = new Set();
  for (const sku of list) {
    if (!sku || sku.hidden) continue;
    const g = sku.shipping?.shippingMethodGroup;
    if (g !== undefined && g !== null && String(g).trim() !== '') groups.add(String(g).trim());
  }
  if (groups.size !== 1) return { label: null, rakutenGroup: null, rakutenGroupName: null, mixed: groups.size > 1 };
  const [g] = groups;
  return { label: labelFromRakutenGroup(g), rakutenGroup: g, rakutenGroupName: RAKUTEN_SHIPPING_METHODS[g] || null, mixed: false };
}
