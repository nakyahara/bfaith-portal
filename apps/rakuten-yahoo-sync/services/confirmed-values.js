/**
 * 確定値 (Yahoo!タイトル・売価・配送方法・税率・カテゴリ) の読み書き — Notion 廃止後の唯一の入口 (2026-10-22)。
 *
 * 正本 = AI_reference『システム設計/RakutenYahooSync_Notion廃止後の方針案_20260914.md』D-2〜D-5。
 *
 * 出品で使う値の決まり方 (loadConfirmedValues):
 *   1. yahoo_confirmed_values に行がある → その値 (source = manual: このアプリのドロワーで確定 / portal: 商品登録ハブで確定)
 *   2. 無い → 旧 notion_overrides の値 (source = legacy_notion「旧運用から継承」。D-3: 揃っていて書式に問題が無ければ使う)
 *   3. どちらも無い → null (出品前チェックで「確定値なし」で止まる)
 *   headline・caption・JAN は確定値の表に持たないので、1 のときも旧 notion_overrides の値を引き継ぐ (空で上書きしない)。
 *
 * 返す形は旧 notion_overrides の行と同じ列名 (yahoo_title / notion_delivery_label / notion_tax_rate …)。
 * field-mapper・readiness-check・category-resolver は列名で読んでいるので、そのまま使える。
 *
 * 書き込み (confirmValues):
 *   - 売価は人が確定した金額だけ (楽天の値は画面に候補として出すだけ — D-2)
 *   - 佐川売価の対象の配送方法では「佐川売価を確定」か「通常売価と同額」を明示 (D-2)
 *   - source='portal' の商品はこのアプリからは変えられない (商品登録ハブで確定し直す — D-5)
 *   - 同時に 2 人が確定しても後勝ちで黙って上書きしない (expectedVersion の楽観ロック)
 *   - 直す前の値は yahoo_confirmed_value_history に残す。最初の確定では旧 Notion の値も残す
 */

export const CONFIRMED_SOURCES = Object.freeze(['manual', 'portal']);
export const SOURCE_LABELS = Object.freeze({
  manual: 'このアプリで確定',
  portal: '商品登録ハブで確定',
  legacy_notion: '旧運用 (Notion) から継承',
});

const TITLE_MAX = 200;   // 送るときに 65 字へ切り詰める (field-mapper)。確定時は異常な長さだけ弾く
const PATH_MAX = 500;

function hasTable(db, name) {
  return !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?`).get(name);
}

/** 税率の書式をそろえる。'10' / '10%' / '８％' / 0.1 → '10'、'8' / '8%' / 0.08 → '8'。それ以外は null */
export function normalizeTaxRate(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') {
    if (Math.abs(v - 0.1) < 1e-9 || v === 10) return '10';
    if (Math.abs(v - 0.08) < 1e-9 || v === 8) return '8';
    return null;
  }
  const s = String(v).normalize('NFKC').trim().replace(/\s+/g, '');
  const m = s.match(/^(\d+)%?$/);
  if (!m) return null;
  if (m[1] === '10') return '10';
  if (m[1] === '8') return '8';
  return null;
}

function toPositiveInt(v) {
  if (v === null || v === undefined || String(v).trim() === '') return null;
  const s = String(v).normalize('NFKC').trim().replace(/[,，円¥￥]/g, '');
  if (!/^\d+$/.test(s)) return NaN;
  const n = Number(s);
  return Number.isSafeInteger(n) && n > 0 ? n : NaN;
}

/** 旧 notion_overrides の行 (無ければ null) */
function loadLegacyRow(db, manageNumber) {
  if (!hasTable(db, 'notion_overrides')) return null;
  return db.prepare('SELECT * FROM notion_overrides WHERE rakuten_manage_number = ?').get(manageNumber) || null;
}

/** 確定値の行 (無ければ null)。migration 023 未適用でも落ちない */
export function loadConfirmedRow(db, manageNumber) {
  if (!manageNumber || !hasTable(db, 'yahoo_confirmed_values')) return null;
  // 確定値の表は小文字で持つ (confirmValues)。楽天の管理番号は小文字運用だが、大文字で渡されても引けるように
  return db.prepare('SELECT * FROM yahoo_confirmed_values WHERE rakuten_manage_number = ?')
    .get(String(manageNumber).trim().toLowerCase()) || null;
}

/**
 * 確定値の行 + 旧 notion_overrides の行 → 出品で使う値 (旧 notion_overrides と同じ列名)。
 * 一覧画面 (JOIN で両方を読んだ行) と出品 (1 商品ずつ読む) で同じ合成をするために切り出す。
 */
export function mergeConfirmedValues(confirmed, legacy) {
  if (!confirmed && !legacy) return null;
  if (!confirmed) {
    // 旧 Notion で「カテゴリ ID と path の片方だけ」入っていた商品: 以前は人の意図として
    // そこで止めていたが、Notion が無くなって直す場所が消えた (紐付け画面で保存しても動かず「完了」と出ていた)。
    // 入力途中の値とみなして両方捨て、紐付け画面の辞書で解決させる
    const lc = legacy.notion_product_category;
    const lp = legacy.notion_path;
    const hasCat = lc !== null && lc !== undefined && lc !== '';
    const hasPath = typeof lp === 'string' && lp.trim() !== '';
    const partial = hasCat !== hasPath;
    return {
      ...legacy,
      ...(partial ? { notion_product_category: null, notion_path: null, legacy_category_partial_ignored: true } : {}),
      value_source: 'legacy_notion',
      sagawa_mode: null,
      confirmed_by: null,
      confirmed_at: null,           // 承認日時は作らない (D-3)。最終同期日時は synced_at のまま
      version: null,
      portal_draft_id: null,
    };
  }
  return {
    rakuten_manage_number: confirmed.rakuten_manage_number,
    yahoo_title: confirmed.yahoo_title,
    yahoo_price: confirmed.yahoo_price,
    yahoo_price_sagawa: confirmed.yahoo_price_sagawa,
    sagawa_mode: confirmed.sagawa_mode,
    notion_delivery_label: confirmed.delivery_label,
    notion_tax_rate: confirmed.tax_rate,
    notion_product_category: confirmed.product_category,
    notion_path: confirmed.path,
    // 旧 Notion の「バリエーション有無」は照合用だった。人が確定し直した商品では古い印で止めない
    notion_has_variation: null,
    // ハブ・ドロワーが持たない項目は旧値を引き継ぐ (空で上書きしない)
    yahoo_headline: legacy?.yahoo_headline ?? null,
    yahoo_caption_text: legacy?.yahoo_caption_text ?? null,
    yahoo_caption_html: legacy?.yahoo_caption_html ?? null,
    yahoo_jan: legacy?.yahoo_jan ?? null,
    value_source: confirmed.source,
    confirmed_by: confirmed.confirmed_by,
    confirmed_at: confirmed.confirmed_at,
    version: confirmed.version,
    portal_draft_id: confirmed.portal_draft_id,
    synced_at: confirmed.confirmed_at,
  };
}

/** 一覧画面用: 確定値の表を全部 (小文字の管理番号 → 行) */
export function loadAllConfirmedRows(db) {
  const map = new Map();
  if (!hasTable(db, 'yahoo_confirmed_values')) return map;
  for (const r of db.prepare('SELECT * FROM yahoo_confirmed_values').all()) map.set(r.rakuten_manage_number, r);
  return map;
}

/** 出品・出品前チェックで使う値 (無ければ null) */
export function loadConfirmedValues(db, manageNumber) {
  if (!manageNumber) return null;
  return mergeConfirmedValues(loadConfirmedRow(db, manageNumber), loadLegacyRow(db, manageNumber));
}

function deliveryRowOf(db, label) {
  return db.prepare(`
    SELECT notion_delivery_label, yahoo_delivery, yahoo_postage_set, applies_sagawa_price
      FROM delivery_mapping WHERE notion_delivery_label = ?
  `).get(label) || null;
}

/** 配送方法の選択肢 (画面のプルダウン用) */
export function listDeliveryOptions(db) {
  return db.prepare(`
    SELECT notion_delivery_label AS label, yahoo_delivery, yahoo_postage_set, applies_sagawa_price, note
      FROM delivery_mapping ORDER BY rowid
  `).all();
}

/**
 * 入力値の検査と正規化。
 * @returns {{ ok: true, values } | { ok: false, errors: string[] }}
 */
export function validateConfirmInput(db, input = {}) {
  const errors = [];
  const title = String(input.yahoo_title ?? '').replace(/[\r\n]+/g, ' ').trim();
  if (!title) errors.push('Yahoo!タイトルが空です');
  else if (title.length > TITLE_MAX) errors.push(`Yahoo!タイトルが長すぎます (${title.length} 字。送るときは 65 字に切り詰めます)`);

  const price = toPositiveInt(input.yahoo_price);
  if (price === null) errors.push('Yahoo!売価が空です');
  else if (Number.isNaN(price)) errors.push('Yahoo!売価は 1 以上の整数 (円) で入れてください');

  const label = String(input.delivery_label ?? '').trim();
  const deliveryRow = label ? deliveryRowOf(db, label) : null;
  if (!label) errors.push('配送方法が空です');
  else if (!deliveryRow) errors.push(`配送方法「${label}」は対応表 (delivery_mapping) にありません`);

  let sagawaMode = null;
  let sagawaPrice = null;
  if (deliveryRow && deliveryRow.applies_sagawa_price === 1) {
    const mode = String(input.sagawa_mode ?? '').trim();
    if (mode === 'same') {
      sagawaMode = 'same';
    } else if (mode === 'price') {
      sagawaPrice = toPositiveInt(input.yahoo_price_sagawa);
      if (sagawaPrice === null || Number.isNaN(sagawaPrice)) errors.push('佐川売価を 1 以上の整数 (円) で入れてください');
      else sagawaMode = 'price';
    } else {
      errors.push(`配送方法「${label}」は佐川売価の対象です。「佐川売価を入れる」か「通常売価と同額」を選んでください`);
    }
  }

  const tax = normalizeTaxRate(input.tax_rate);
  if (tax === null) errors.push('税率は 10% か 8% を選んでください');

  let category = null;
  let path = null;
  const rawCat = input.product_category;
  const rawPath = String(input.path ?? '').trim();
  const hasCat = rawCat !== null && rawCat !== undefined && String(rawCat).trim() !== '';
  if (hasCat) {
    const s = String(rawCat).normalize('NFKC').trim();
    if (!/^\d+$/.test(s)) errors.push('Yahoo!カテゴリID は数字で入れてください');
    else category = Number(s);
  }
  if (rawPath) {
    if (rawPath.length > PATH_MAX) errors.push('店の棚 (path) が長すぎます');
    else path = rawPath;
  }
  if (hasCat !== !!rawPath) {
    errors.push('Yahoo!カテゴリID と店の棚 (path) は両方入れるか、両方空 (紐付け画面の辞書で解決) にしてください');
  }

  if (errors.length) return { ok: false, errors };
  return {
    ok: true,
    values: {
      yahoo_title: title,
      yahoo_price: price,
      sagawa_mode: sagawaMode,
      yahoo_price_sagawa: sagawaPrice,
      delivery_label: label,
      tax_rate: tax,
      product_category: category,
      path,
    },
  };
}

const VALUE_KEYS = ['yahoo_title', 'yahoo_price', 'sagawa_mode', 'yahoo_price_sagawa', 'delivery_label', 'tax_rate', 'product_category', 'path'];
function pickValues(row) {
  if (!row) return null;
  const out = {};
  for (const k of VALUE_KEYS) out[k] = row[k] ?? null;
  return out;
}
function legacyValues(legacy) {
  if (!legacy) return null;
  return {
    yahoo_title: legacy.yahoo_title ?? null,
    yahoo_price: legacy.yahoo_price ?? null,
    yahoo_price_sagawa: legacy.yahoo_price_sagawa ?? null,
    delivery_label: legacy.notion_delivery_label ?? null,
    tax_rate: legacy.notion_tax_rate ?? null,
    product_category: legacy.notion_product_category ?? null,
    path: legacy.notion_path ?? null,
    synced_at: legacy.synced_at ?? null,
  };
}

export class ConfirmError extends Error {
  constructor(message, { statusCode = 400, code = 'invalid', errors = null, current = null } = {}) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
    this.errors = errors;
    this.current = current;
  }
}

/**
 * 確定値を保存する (ドロワーの「この値で確定」と商品登録ハブの「Yahoo! 移行に確定」の共通の入口)。
 *
 * @param {object} opts
 * @param {string} opts.manageNumber   楽天の商品管理番号 (小文字にそろえる)
 * @param {object} opts.input          画面の値 (validateConfirmInput 参照)
 * @param {'manual'|'portal'} opts.source
 * @param {string} opts.actor          確定した人 (メール等)
 * @param {number|null} [opts.expectedVersion]  画面が見ていた version (行が無かった = null)。違えば 409
 * @param {number|null} [opts.portalDraftId]
 * @returns {{ row, previousSource, changed: boolean }}
 */
export function confirmValues(db, { manageNumber, input, source, actor, expectedVersion = null, portalDraftId = null } = {}) {
  const mn = String(manageNumber ?? '').trim().toLowerCase();
  if (!mn) throw new ConfirmError('商品管理番号がありません');
  if (!CONFIRMED_SOURCES.includes(source)) throw new ConfirmError(`source が不正です: ${source}`);
  if (!actor) throw new ConfirmError('確定した人が分かりません (ログインし直してください)', { statusCode: 401 });
  if (source === 'portal' && !(Number.isInteger(portalDraftId) && portalDraftId > 0)) {
    throw new ConfirmError('商品登録ハブの商品 ID がありません');
  }
  const v = validateConfirmInput(db, input);
  if (!v.ok) throw new ConfirmError(v.errors.join(' / '), { errors: v.errors });
  const values = v.values;

  const tx = db.transaction(() => {
    const cur = loadConfirmedRow(db, mn);
    const curVersion = cur ? cur.version : null;
    if ((expectedVersion ?? null) !== curVersion) {
      throw new ConfirmError('ほかの人 (または別の画面) が先に確定しています。画面を読み直してからもう一度確定してください',
        { statusCode: 409, code: 'version_conflict', current: cur });
    }
    // D-5: ハブで確定した商品は、このアプリでは読み取り専用 (ハブで確定し直す)
    if (cur && cur.source === 'portal' && source === 'manual') {
      throw new ConfirmError('この商品は商品登録ハブで確定しています。値はハブの商品詳細で直して確定し直してください',
        { statusCode: 409, code: 'portal_owned', current: cur });
    }
    // 同じ値・同じ出所なら版を上げない (確定し直しても何も変わらない)
    const same = cur && cur.source === source
      && VALUE_KEYS.every((k) => (cur[k] ?? null) === (values[k] ?? null))
      && (source !== 'portal' || cur.portal_draft_id === portalDraftId);
    if (same) return { row: cur, previousSource: cur.source, changed: false };

    const legacy = cur ? null : loadLegacyRow(db, mn);
    const nextVersion = (curVersion || 0) + 1;
    db.prepare(`
      INSERT INTO yahoo_confirmed_values (
        rakuten_manage_number, source, yahoo_title, yahoo_price, sagawa_mode, yahoo_price_sagawa,
        delivery_label, tax_rate, product_category, path, portal_draft_id, confirmed_by, confirmed_at, version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?)
      ON CONFLICT(rakuten_manage_number) DO UPDATE SET
        source = excluded.source, yahoo_title = excluded.yahoo_title, yahoo_price = excluded.yahoo_price,
        sagawa_mode = excluded.sagawa_mode, yahoo_price_sagawa = excluded.yahoo_price_sagawa,
        delivery_label = excluded.delivery_label, tax_rate = excluded.tax_rate,
        product_category = excluded.product_category, path = excluded.path,
        portal_draft_id = excluded.portal_draft_id, confirmed_by = excluded.confirmed_by,
        confirmed_at = excluded.confirmed_at, version = excluded.version
    `).run(
      mn, source, values.yahoo_title, values.yahoo_price, values.sagawa_mode, values.yahoo_price_sagawa,
      values.delivery_label, values.tax_rate, values.product_category, values.path,
      source === 'portal' ? portalDraftId : null, String(actor), nextVersion,
    );
    db.prepare(`
      INSERT INTO yahoo_confirmed_value_history (
        rakuten_manage_number, source_before, version_before, values_before_json,
        source_after, version_after, values_after_json, changed_by
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      mn,
      cur ? cur.source : (legacy ? 'legacy_notion' : 'none'),
      curVersion,
      cur ? JSON.stringify(pickValues(cur)) : (legacy ? JSON.stringify(legacyValues(legacy)) : null),
      source, nextVersion, JSON.stringify(values), String(actor),
    );
    return { row: loadConfirmedRow(db, mn), previousSource: cur ? cur.source : (legacy ? 'legacy_notion' : null), changed: true };
  });
  return tx();
}

/** 確定し直しの履歴 (新しい順) */
export function listConfirmHistory(db, manageNumber, { limit = 20 } = {}) {
  if (!hasTable(db, 'yahoo_confirmed_value_history')) return [];
  const mn = String(manageNumber ?? '').trim().toLowerCase();
  return db.prepare(`
    SELECT id, source_before, version_before, values_before_json, source_after, version_after,
           values_after_json, changed_by, changed_at
      FROM yahoo_confirmed_value_history WHERE rakuten_manage_number = ?
     ORDER BY id DESC LIMIT ?
  `).all(mn, limit).map((r) => ({
    ...r,
    values_before: r.values_before_json ? JSON.parse(r.values_before_json) : null,
    values_after: JSON.parse(r.values_after_json),
  }));
}
