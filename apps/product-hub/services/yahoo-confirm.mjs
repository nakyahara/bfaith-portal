/**
 * 商品登録ハブ → 楽天→Yahoo!移行 (RYS) への確定値の引き継ぎ (2026-10-22、方針案 20260914 D-5 / PR 3)。
 *
 * 旧: RYS は Notion 商品マスターから Yahoo!タイトル・売価・配送・税率を取り込んでいた (2026-09-14 廃止)。
 * 新: ハブの商品詳細で「Yahoo!移行に確定」を押したとき、**画面で見せた値そのもの**を RYS の確定値の表へ書く
 *     (RYS の services/confirmed-values.js が唯一の書き込み口。ドロワーの手入力と同じ検査・履歴・楽観ロック)。
 *     - ドラフトがあるだけでは何もしない (押したときだけ。D-5)
 *     - 確定後にハブの値を変えても RYS は確定した値のまま (承認した金額で出す — D-2)。画面に「差あり」を出して確定し直してもらう
 *     - ハブで確定した商品は RYS では読み取り専用。ハブの商品を消しても RYS の値は消さない
 *     - headline・caption・JAN はハブから送らない (RYS に旧 Notion の値があればそのまま残る)
 *
 * 値の決め方 (ハブ → RYS):
 *   タイトル = AI 出力の yahoo_title (人が直した値も同じ行)。空なら止める
 *   配送方法 = ヤフーだけ別配送なら draft_yahoo の配送方法、そうでなければ楽天の配送方法グループ → RYS のラベル
 *              (対応を決めていない配送方法は止める — services/delivery-label-map.js)
 *   売価     = ヤフーだけ別配送なら draft_yahoo の Yahoo!売価、そうでなければ基本情報の売価 (= 楽天と同じ)
 *              SKU ごとに売価が違う商品は Yahoo!へ 1 つの売価でしか出せないので止める
 *   佐川売価 = 佐川売価の対象の配送方法 (定形外（ヤフーのみ…）) のときだけ。空なら「通常売価と同額」を画面で明示的に選ぶ
 *   税率     = 楽天の出品と同じ決め方 (listing-tax.mjs: 切替前は draft_yahoo.tax_rate / 切替後は Company DB)
 *   カテゴリ = draft_yahoo の Yahoo!カテゴリID + path (両方空なら RYS の紐付け辞書)
 */
import { getDB as getRysDB } from '../../rakuten-yahoo-sync/db.js';
import { confirmValues, loadConfirmedRow, validateConfirmInput, normalizeTaxRate, ConfirmError } from '../../rakuten-yahoo-sync/services/confirmed-values.js';
import { labelFromPortal } from '../../rakuten-yahoo-sync/services/delivery-label-map.js';
import { resolveListingTax } from './listing-tax.mjs';
import { existingPageOfDraft } from '../lib/existing-page.js';

export { ConfirmError };

/** RYS の商品管理番号 = 楽天の商品管理番号 = NE の代表コードの小文字 (rakutenItemPageUrl と同じ) */
export function manageNumberOfDraft(draft) {
  return String(draft?.ne_code ?? '').trim().toLowerCase();
}

/**
 * ハブの今の値から、RYS へ送る確定値を組み立てる (書かない)。
 * @returns {Promise<{ ok: boolean, manageNumber: string, values: object|null, reasons: string[],
 *                     needsSagawaChoice: boolean, rys: object|null }>}
 */
export async function buildYahooConfirmPreview(db, draft, { sagawaSame = false, tax = null } = {}) {
  const reasons = [];
  const manageNumber = manageNumberOfDraft(draft);
  if (!manageNumber) reasons.push('商品コードがありません');
  if (draft.provisional_code === 1) reasons.push(`商品コードが仮のままです (${draft.ne_code})。本コードに差し替えてから確定してください`);
  const ep = existingPageOfDraft(db, draft.id);
  if (ep.existingPage || draft.added_to_draft_id != null) {
    reasons.push('既存の楽天ページに追加する商品です。Yahoo!側も既存ページの扱いになるので、移行アプリからは出しません');
  }

  const ai = db.prepare(`SELECT content FROM draft_ai_outputs WHERE draft_id = ? AND kind = 'yahoo_title'`).get(draft.id);
  const title = String(ai?.content ?? '').replace(/[\r\n]+/g, ' ').trim();
  if (!title) reasons.push('Yahoo!タイトルが空です (商品説明タブの「Yahoo!タイトル」)');

  const yahoo = db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(draft.id) || {};
  const rakuten = db.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(draft.id) || {};
  const override = !!yahoo.shipping_override;
  const d = labelFromPortal({
    rakutenGroup: rakuten.shipping_method_group,
    shippingOverride: override,
    yahooDeliveryName: yahoo.delivery_label,
  });
  if (!d.label) reasons.push(d.reason || '配送方法を決められません');

  let price = null;
  if (override) {
    price = Number(yahoo.yahoo_price) > 0 ? Number(yahoo.yahoo_price) : null;
    if (!price) reasons.push('ヤフーだけ別配送の商品です。Yahoo!売価を入れてください');
  } else {
    price = Number(draft.price) > 0 ? Number(draft.price) : null;
    if (!price) reasons.push('売価が空です (基本情報の売価)');
    const skuPrices = db.prepare('SELECT DISTINCT price FROM draft_sku_prices WHERE draft_id = ? AND price IS NOT NULL').all(draft.id)
      .map((r) => Number(r.price)).filter((n) => n > 0);
    const distinct = new Set([...(price ? [price] : []), ...skuPrices]);
    if (distinct.size > 1) {
      reasons.push(`SKU ごとに売価が違います (${[...distinct].sort((a, b) => a - b).map((n) => '¥' + n.toLocaleString()).join(' / ')})。Yahoo!へは 1 つの売価でしか出せないので、移行アプリの商品詳細で決めてください`);
    }
  }

  // 佐川売価 (RYS の delivery_mapping で applies_sagawa_price=1 の配送方法だけ)
  const rys = getRysDB();
  let sagawaMode = null;
  let sagawaPrice = null;
  let needsSagawaChoice = false;
  const dm = d.label ? rys.prepare('SELECT applies_sagawa_price FROM delivery_mapping WHERE notion_delivery_label = ?').get(d.label) : null;
  if (d.label && !dm) reasons.push(`配送方法「${d.label}」が移行アプリの対応表にありません`);
  if (dm && dm.applies_sagawa_price === 1) {
    if (Number(yahoo.yahoo_price_sagawa) > 0) {
      sagawaMode = 'price';
      sagawaPrice = Number(yahoo.yahoo_price_sagawa);
    } else if (sagawaSame) {
      sagawaMode = 'same';
    } else {
      needsSagawaChoice = true;
    }
  }

  // 税率 (楽天の出品と同じ決め方。切替の状態を読めない・決められない = 止める)
  const t = tax || await resolveListingTax(db, draft);
  let taxRate = null;
  if (t.mode === 'blocked') reasons.push(t.reason || '税率を決められません');
  else if (t.mode === 'cdb') taxRate = normalizeTaxRate(t.percent);
  else taxRate = normalizeTaxRate(yahoo.tax_rate);
  if (t.mode !== 'blocked' && !taxRate) reasons.push('税率が空か想定外の値です (基本情報の税率で 10% / 8% を選んでください)');

  const catId = yahoo.yahoo_category_id ?? null;
  const path = String(yahoo.yahoo_path ?? '').trim() || null;
  if ((catId == null) !== (path == null)) {
    reasons.push('Yahoo!カテゴリID と path は両方入れるか、両方空 (移行アプリの紐付け辞書で決める) にしてください');
  }

  const values = {
    yahoo_title: title,
    yahoo_price: price,
    delivery_label: d.label,
    sagawa_mode: sagawaMode,
    yahoo_price_sagawa: sagawaPrice,
    tax_rate: taxRate,
    product_category: catId,
    path,
  };
  // 書き込み口と同じ検査も通す (ここで通れば確定でも通る)
  if (reasons.length === 0 && !needsSagawaChoice) {
    const v = validateConfirmInput(rys, values);
    if (!v.ok) reasons.push(...v.errors);
  }
  const current = manageNumber ? loadConfirmedRow(rys, manageNumber) : null;
  return {
    ok: reasons.length === 0 && !needsSagawaChoice,
    manageNumber,
    values,
    reasons,
    needsSagawaChoice,
    rys: current ? {
      source: current.source, version: current.version, confirmedAt: current.confirmed_at, confirmedBy: current.confirmed_by,
      portalDraftId: current.portal_draft_id,
      values: {
        yahoo_title: current.yahoo_title, yahoo_price: current.yahoo_price, delivery_label: current.delivery_label,
        sagawa_mode: current.sagawa_mode, yahoo_price_sagawa: current.yahoo_price_sagawa, tax_rate: current.tax_rate,
        product_category: current.product_category, path: current.path,
      },
    } : null,
  };
}

const KEYS = ['yahoo_title', 'yahoo_price', 'delivery_label', 'sagawa_mode', 'yahoo_price_sagawa', 'tax_rate', 'product_category', 'path'];
export function sameValues(a, b) {
  if (!a || !b) return false;
  return KEYS.every((k) => (a[k] ?? null) === (b[k] ?? null));
}

/**
 * 確定する。画面で見せた値 (expectedValues) と今組み立てた値が違えば 409 (確認した内容と違う値を確定しない)。
 */
export async function confirmYahooFromPortal(db, draft, { actor, expectedValues, expectedVersion = null, sagawaSame = false } = {}) {
  const p = await buildYahooConfirmPreview(db, draft, { sagawaSame });
  if (!p.ok) {
    throw new ConfirmError(p.needsSagawaChoice && p.reasons.length === 0
      ? '佐川売価が空です。「通常売価と同額」で出すかを選んでください'
      : p.reasons.join(' / '), { statusCode: 400, code: 'not_ready', errors: p.reasons });
  }
  if (!sameValues(p.values, expectedValues)) {
    throw new ConfirmError('確認したあとに値が変わりました。もう一度「送る内容を確認」してから確定してください',
      { statusCode: 409, code: 'values_changed' });
  }
  const r = confirmValues(getRysDB(), {
    manageNumber: p.manageNumber,
    input: p.values,
    source: 'portal',
    actor,
    expectedVersion,
    portalDraftId: draft.id,
  });
  return { ...r, manageNumber: p.manageNumber, values: p.values };
}
