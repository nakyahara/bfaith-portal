/**
 * 商品ページ表記 (化粧品・食品) — 旧「商品ページ詳細ページ作成.xlsm」の移植 (2026-07-29)。
 *
 * 楽天の必須記載事項 (医薬品・医薬部外品・化粧品・特保・栄養機能食品・機能性表示食品・
 * 健康食品・ダイエット食品が対象。xlsm 内の楽天ルール引用 + 2026-07-29 Web確認):
 *   1) 広告文責 (社名・連絡先電話番号)
 *   2) メーカー名または販売業者名 (輸入品はメーカー名と輸入者名の両記載)
 *   3) 日本製か海外製か (海外製は原則原産国名。健康食品は原産国名が必須。
 *      原産国 = 製造事業所の所在国。原材料の産地やパッキング国ではない)
 *   4) 商品区分 (化粧品/医薬部外品/健康食品/…)
 *   ※ テキストで記載必須 (画像化は不可) → この HTML 生成がそのまま要件を満たす
 *
 * 食品 (一般) は食品表示法ベースの項目 (名称/原材料名/賞味期限/保存方法/販売者) を載せる。
 */
import { SHIPPING_METHOD_GROUPS } from '../services/rakuten-listing.js';

export const PRODUCT_TYPES = {
  general: '雑貨・その他',
  cosmetics: '化粧品',
  health_food: '健康食品',
  food: '食品 (一般)',
};

/** 商品分類区分の選択肢 (楽天ルール 4) の区分名) */
export const CATEGORY_LABELS = [
  '化粧品', '医薬部外品', '健康食品', 'ダイエット食品',
  '栄養機能食品', '機能性表示食品', '特定保健用食品', '食品', '雑貨',
];

/**
 * 広告文責 (固定。xlsm の固定値。env で差し替え可)。
 * env 値は許可タグ <br> 以外を全部エスケープして返す (Codex R1 medium:
 * 生の env を信頼済みHTMLとして流すと管理画面と商品説明の両方に効くため)
 */
export function adResponsibility() {
  const raw = (process.env.PH_AD_RESPONSIBILITY || 'B-Faith株式会社<br>（TEL:066334858）').trim();
  return esc(raw).replace(/&lt;br\s*\/?&gt;/gi, '<br>');
}

/** 注意事項 (xlsm の固定文) */
export const FIXED_NOTES =
  '※モニター画面の状況によって実際のお色と見え方が異なる場合がございます。予めご了承くださいませ。<br>'
  + '※予告なくパッケージラベル・外観等変更になる場合がございます。予めご了承お願いいたします。';

/** 楽天必須記載 (化粧品・健康食品等) の対象タイプか */
export function isRegulatedType(productType) {
  return productType === 'cosmetics' || productType === 'health_food';
}

/**
 * 楽天必須記載事項の不足チェック。reasons (空=OK) を返す。
 * general/food は楽天の必須記載対象外なので空 (food は食品表示系を推奨表示に留める)。
 */
export function validatePageInfo(info) {
  const reasons = [];
  if (!info || !isRegulatedType(info.product_type)) return reasons;
  const label = PRODUCT_TYPES[info.product_type] || info.product_type;
  if (!s(info.seller_name)) {
    reasons.push(`${label}は「発売元 (メーカー名/販売業者名)」の記載が楽天必須です`);
  }
  if (!s(info.origin_type)) {
    reasons.push(`${label}は「日本製か海外製か」の記載が楽天必須です`);
  } else if (info.origin_type === '海外製') {
    if (info.product_type === 'health_food' && !s(info.origin_country)) {
      reasons.push('健康食品の海外製は「原産国名」の記載が楽天必須です (製造事業所の所在国)');
    }
    if (!s(info.importer_name)) {
      reasons.push('海外製 (輸入品) は「輸入者名」の記載が楽天必須です (メーカー名との両記載)');
    }
  }
  const cat = s(info.category_label);
  if (!cat) {
    reasons.push(`${label}は「商品区分」の記載が楽天必須です`);
  } else if (!CATEGORY_LABELS_BY_TYPE[info.product_type].includes(cat)) {
    reasons.push(`商品タイプ「${label}」に商品区分「${cat}」は選べません (整合しない組み合わせ)`);
  }
  // 健康食品は食品なので加工食品の必須記載 (名称/原材料名/内容量/賞味期限/保存方法) も楽天必須
  // (2026-07-29 Web再確認: 楽天ガイドラインの加工食品ルール。食品(一般) は推奨に留める従来判断を維持)
  if (info.product_type === 'health_food') {
    if (!s(info.food_name)) reasons.push('健康食品は「名称 (食品表示)」の記載が楽天必須です');
    if (!s(info.food_ingredients)) reasons.push('健康食品は「原材料名」の記載が楽天必須です');
    if (!s(info.content_volume)) reasons.push('健康食品は「内容量」の記載が楽天必須です');
    if (!s(info.food_expiry)) reasons.push('健康食品は「賞味期限」の記載が楽天必須です');
    if (!s(info.food_storage)) reasons.push('健康食品は「保存方法」の記載が楽天必須です');
  }
  return reasons;
}

/** 商品タイプごとに整合する商品区分 (Codex R1 medium: 不整合の組み合わせをブロック) */
export const CATEGORY_LABELS_BY_TYPE = {
  cosmetics: ['化粧品', '医薬部外品'],
  health_food: ['健康食品', 'ダイエット食品', '栄養機能食品', '機能性表示食品', '特定保健用食品'],
};

function s(v) {
  return v != null && String(v).trim() !== '' ? String(v).trim() : null;
}

function esc(v) {
  return String(v).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

/** 改行を <br> に変換しつつ HTML エスケープ */
function nl2br(v) {
  return esc(v).replace(/\r?\n/g, '<br>');
}

/**
 * xlsm と同じ表形式の掲載用 HTML を作る。空欄の行は出さない。
 *
 * 店舗の従来フォーマット (2026-08-01 中原さん提示の実例) では PC用商品説明文が
 * この表1枚で、先頭が「説明」行 (商品名+説明文)・2行目が「注意事項」行。
 * descriptionText を渡すとそのフォーマットになる (渡さなければ従来どおり
 * 「商品名」行から始まる = 商品情報タブの掲載プレビュー互換)。
 *
 * @param {object} p
 * @param {string} p.productName  商品名
 * @param {object|null} p.info    draft_page_info 行
 * @param {string|null} p.shippingLabel 発送方法の表示名 (楽天配送方法グループ名)
 * @param {string|null} [p.descriptionText] 「説明」行に入れる平文 (商品名は呼び出し側が文頭に含める)
 * @param {string|null} [p.notesText] AI注意書き (平文)。「注意事項」行の固定文の前に載せる
 * @param {Array<{spec_key:string, spec_value:string|null}>|null} [p.specs] 仕様表 → 1項目1行
 */
export function buildPageInfoHtml({ productName, info, shippingLabel, descriptionText = null, notesText = null, specs = null }) {
  const rows = [];
  const add = (label, valueHtml) => {
    if (!valueHtml) return;
    rows.push(
      `<tr><td bgcolor="#eeeeee" width="30%"><b>${esc(label)}</b></td><td>${valueHtml}</td></tr>`
    );
  };
  if (s(descriptionText)) {
    add('説明', nl2br(descriptionText));
    // 注意事項は店舗フォーマットに合わせて説明の直後 (AI注意書き → 固定文の順)
    add('注意事項', (s(notesText) ? nl2br(notesText) + '<br>' : '') + FIXED_NOTES);
  } else {
    add('商品名', s(productName) ? esc(productName) : null);
  }
  for (const sp of Array.isArray(specs) ? specs : []) {
    if (s(sp?.spec_key)) add(sp.spec_key, s(sp.spec_value) ? nl2br(sp.spec_value) : null);
  }
  const i = info || {};
  add('サイズ', s(i.size_text) ? nl2br(i.size_text) : null);
  add('内容量', s(i.content_volume) ? nl2br(i.content_volume) : null);
  add(i.product_type === 'food' || i.product_type === 'health_food' ? '原材料名' : '成分・素材',
    s(i.product_type === 'food' || i.product_type === 'health_food' ? i.food_ingredients : i.ingredients)
      ? nl2br(i.product_type === 'food' || i.product_type === 'health_food' ? i.food_ingredients : i.ingredients)
      : (s(i.ingredients) ? nl2br(i.ingredients) : null));
  if (i.product_type === 'food' || i.product_type === 'health_food') {
    add('名称', s(i.food_name) ? nl2br(i.food_name) : null);
    add('賞味期限', s(i.food_expiry) ? nl2br(i.food_expiry) : null);
    add('保存方法', s(i.food_storage) ? nl2br(i.food_storage) : null);
  }
  add('使用上の注意', s(i.usage_notes) ? nl2br(i.usage_notes) : null);
  // 製造国: 「日本製」 or 「海外製（フランス製）」の形
  const origin = s(i.origin_type)
    ? (i.origin_type === '海外製' && s(i.origin_country) ? `海外製（${esc(i.origin_country)}）` : esc(i.origin_type))
    : null;
  add('製造国', origin);
  add('商品区分', s(i.category_label) ? esc(i.category_label) : null);
  const seller = s(i.seller_name)
    ? esc(i.seller_name) + (s(i.importer_name) ? `<br>輸入者: ${esc(i.importer_name)}` : '')
    : null;
  add('発売元', seller);
  add('発送方法', s(shippingLabel) ? esc(shippingLabel) : null);
  add('広告文責', adResponsibility()); // 固定値 (env 由来。<br> を含む信頼済み文字列)
  if (!s(descriptionText)) add('注意事項', FIXED_NOTES); // 説明行ありのときは先頭側で追加済み

  if (rows.length === 0) return '';
  return '<table width="100%" cellpadding="4" cellspacing="2" border="1" bordercolor="#000000">'
    + rows.join('') + '</table>';
}

/**
 * NE 配送方法 → 楽天配送方法グループ。ph_shipping_method_map を引き、
 * 未登録なら名前一致 (完全一致→部分一致) で推測して返す (推測は保存しない)。
 * @returns {{group: string|null, label: string|null, guessed: boolean}}
 */
export function mapNeShippingToRakuten(db, neLabel) {
  const label = s(neLabel);
  if (!label) return { group: null, label: null, guessed: false };
  const row = db.prepare('SELECT rakuten_group FROM ph_shipping_method_map WHERE ne_label = ?').get(label);
  if (row && row.rakuten_group && SHIPPING_METHOD_GROUPS[row.rakuten_group]) {
    return { group: row.rakuten_group, label: SHIPPING_METHOD_GROUPS[row.rakuten_group], guessed: false };
  }
  // 名前の完全一致だけ推測する (例: NE「ネコポス」= 楽天グループ5「ネコポス」)。
  // 部分一致は誤マッピングの温床 (Codex R1 medium: 宅急便コンパクト→宅急便 等の
  // 別サービス誤判定が掲載HTMLにまで載る) なので、曖昧なものは admin の割当に任せる
  for (const [gid, gname] of Object.entries(SHIPPING_METHOD_GROUPS)) {
    if (gname === label) return { group: gid, label: gname, guessed: true };
  }
  return { group: null, label: null, guessed: false };
}

/**
 * マッピング一覧 (admin 画面用): NE の配送方法 distinct + 現在の割当 + 推測。
 */
export function listShippingMethodMap(db) {
  let neLabels = [];
  try {
    neLabels = db.prepare(`
      SELECT 配送方法 AS label, COUNT(*) AS cnt FROM mirror_products
      WHERE 配送方法 IS NOT NULL AND TRIM(配送方法) != ''
      GROUP BY 配送方法 ORDER BY cnt DESC
    `).all();
  } catch (_) { /* mirror 未同期でも画面は出す */ }
  const saved = new Map(
    db.prepare('SELECT ne_label, rakuten_group FROM ph_shipping_method_map').all()
      .map((r) => [r.ne_label, r.rakuten_group])
  );
  const rows = neLabels.map((r) => {
    const savedGroup = saved.get(r.label) || null;
    const guess = savedGroup ? null : mapNeShippingToRakuten(db, r.label);
    return {
      neLabel: r.label,
      count: r.cnt,
      rakutenGroup: savedGroup || (guess?.guessed ? guess.group : null),
      saved: !!savedGroup,
    };
  });
  // 保存済みだが NE に現存しないラベルも見せる (棚卸し用)
  for (const [label, group] of saved) {
    if (!rows.some((r) => r.neLabel === label)) {
      rows.push({ neLabel: label, count: 0, rakutenGroup: group, saved: true });
    }
  }
  return rows;
}

/** マッピングの保存 (upsert。group は '1'〜'9' か空=未割当) */
export function saveShippingMethodMap(db, entries) {
  const up = db.prepare(`
    INSERT INTO ph_shipping_method_map (ne_label, rakuten_group)
    VALUES (?, ?)
    ON CONFLICT(ne_label) DO UPDATE SET
      rakuten_group = excluded.rakuten_group,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `);
  let savedCount = 0;
  const tx = db.transaction(() => {
    for (const e of entries) {
      const label = s(e?.ne_label);
      if (!label || label.length > 100) continue;
      const group = s(e?.rakuten_group);
      if (group && !SHIPPING_METHOD_GROUPS[group]) continue; // 不正なグループIDは保存しない
      up.run(label, group);
      savedCount += 1;
    }
  });
  tx();
  return savedCount;
}
